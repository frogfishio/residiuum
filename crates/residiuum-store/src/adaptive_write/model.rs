//! Pure executable selector model for AWO-0 golden vectors.
//!
//! Arithmetic is locked to `scripts/verify-awo-contract.sh` `decide()` and
//! `spec/performance/awo/golden-decisions-v1.json`. Do not invent alternate
//! formulas without amending the contract and plan.

use super::types::AwoPlan;

/// Inputs for one closed golden-decision vector / pure selector call.
///
/// Field units match the golden JSON schema (nanoseconds, ppm, counts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GoldenDecisionInput {
    /// Compatible requests already queued at decision time.
    pub queue_count: u64,
    /// Lower uncertainty bound on natural per-request service time (ns).
    pub natural_service_lower_ns: u64,
    /// Upper uncertainty bound on batch completion for the candidate (ns).
    ///
    /// In the AWO-0 golden model this is treated as the full batch completion
    /// cost for the objective's batch side (mean and tail share this upper).
    pub batch_completion_upper_ns: u64,
    /// Decision margin in parts-per-million (e.g. 100_000 = 10%).
    pub decision_margin_ppm: u32,
    /// Estimator has met the warm-sample floor.
    pub evidence_warm: bool,
    /// Estimator sample is stale by monotonic-clock policy.
    pub evidence_stale: bool,
    /// Candidate members share a V1 lane (layout/durability/shard).
    pub compatible: bool,
    /// Queue credit / memory bound permits a batch of this size.
    pub memory_ok: bool,
    /// Earliest absolute completion deadline among candidates (ns from now).
    pub earliest_deadline_ns: u64,
}

/// Result of pure natural-vs-batch selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decision {
    /// Selected plan class.
    pub plan: AwoPlan,
    /// Closed decision-reason id (`decision-reasons-v1.json`).
    pub reason: &'static str,
}

/// Natural mean + tail completion for `q` equal service times `service`.
///
/// ```text
/// mean_natural(q) = service * (q + 1) / 2
/// tail_natural(q) = service * q
/// J_natural = mean + tail
/// ```
///
/// Integer division truncates toward zero (matches the contract verifier).
pub fn natural_objective_ns(queue_count: u64, service_lower_ns: u64) -> Option<(u64, u64, u64)> {
    let q = queue_count;
    let service = service_lower_ns;
    let mean = service.checked_mul(q.checked_add(1)?)?.checked_div(2)?;
    let tail = service.checked_mul(q)?;
    let j = mean.checked_add(tail)?;
    Some((mean, tail, j))
}

/// Pure natural-versus-batch decision used by golden vectors.
///
/// Order of closed reasons matches `scripts/verify-awo-contract.sh`:
/// single request → incompatible → memory → cold → stale → deadline branch →
/// margin comparison (batch / tie / no gain).
pub fn decide(input: &GoldenDecisionInput) -> Decision {
    let q = input.queue_count;
    if q == 1 {
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_single_request",
        };
    }
    if !input.compatible {
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_incompatible",
        };
    }
    if !input.memory_ok {
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_memory_bound",
        };
    }
    if !input.evidence_warm {
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_insufficient_evidence",
        };
    }
    if input.evidence_stale {
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_stale_model",
        };
    }

    let Some((_mean, tail_natural, j_natural)) =
        natural_objective_ns(q, input.natural_service_lower_ns)
    else {
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_arithmetic_overflow",
        };
    };

    let batch = input.batch_completion_upper_ns;
    let deadline = input.earliest_deadline_ns;

    // Deadline branch: batch upper exceeds earliest deadline.
    if batch > deadline {
        if tail_natural <= deadline {
            return Decision {
                plan: AwoPlan::Natural,
                reason: "natural_deadline",
            };
        }
        // Both plans predicted late: pick smaller upper tail (batch upper vs natural tail).
        if batch < tail_natural {
            return Decision {
                plan: AwoPlan::Batch,
                reason: "batch_deadline_mitigation",
            };
        }
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_deadline_mitigation",
        };
    }

    // J_batch uses the upper completion twice (mean + tail share the upper bound)
    // matching the contract verifier: j_batch = batch * 2.
    let Some(j_batch) = batch.checked_mul(2) else {
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_arithmetic_overflow",
        };
    };

    // Batch wins only when:
    //   J_batch * 1_000_000 < J_natural * (1_000_000 - decision_margin_ppm)
    let margin = u64::from(input.decision_margin_ppm);
    if margin > 1_000_000 {
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_arithmetic_overflow",
        };
    }
    let Some(lhs) = j_batch.checked_mul(1_000_000) else {
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_arithmetic_overflow",
        };
    };
    let Some(rhs) = j_natural.checked_mul(1_000_000 - margin) else {
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_arithmetic_overflow",
        };
    };

    if lhs < rhs {
        return Decision {
            plan: AwoPlan::Batch,
            reason: "batch_existing_backlog",
        };
    }
    if lhs == rhs {
        return Decision {
            plan: AwoPlan::Natural,
            reason: "natural_tie",
        };
    }
    Decision {
        plan: AwoPlan::Natural,
        reason: "natural_no_positive_gain",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adaptive_write::types::{decision_reason_ids, AWO_PROFILE};
    use serde::Deserialize;
    use std::path::PathBuf;

    #[derive(Debug, Deserialize)]
    struct GoldenFile {
        profile: String,
        items: Vec<GoldenItem>,
    }

    #[derive(Debug, Deserialize)]
    struct GoldenItem {
        id: String,
        queue_count: u64,
        natural_service_lower_ns: u64,
        batch_completion_upper_ns: u64,
        decision_margin_ppm: u32,
        evidence_warm: bool,
        evidence_stale: bool,
        compatible: bool,
        memory_ok: bool,
        earliest_deadline_ns: u64,
        expected_plan: String,
        expected_reason: String,
    }

    fn golden_path() -> PathBuf {
        // crates/residiuum-store -> repo root
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../spec/performance/awo/golden-decisions-v1.json")
    }

    fn load_goldens() -> GoldenFile {
        let path = golden_path();
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        serde_json::from_str(&text).expect("parse golden-decisions-v1.json")
    }

    #[test]
    fn profile_constant_matches_registry() {
        assert_eq!(AWO_PROFILE, "residiuum-adaptive-write-v1");
        let g = load_goldens();
        assert_eq!(g.profile, AWO_PROFILE);
    }

    #[test]
    fn closed_reason_ids_include_all_golden_reasons() {
        let closed: std::collections::HashSet<&str> =
            decision_reason_ids().iter().copied().collect();
        let g = load_goldens();
        for item in &g.items {
            assert!(
                closed.contains(item.expected_reason.as_str()),
                "golden {} reason {} not in closed set",
                item.id,
                item.expected_reason
            );
        }
    }

    #[test]
    fn all_golden_vectors_match_decide() {
        let g = load_goldens();
        assert!(
            g.items.len() >= 12,
            "expected at least 12 golden vectors, got {}",
            g.items.len()
        );
        for item in &g.items {
            let input = GoldenDecisionInput {
                queue_count: item.queue_count,
                natural_service_lower_ns: item.natural_service_lower_ns,
                batch_completion_upper_ns: item.batch_completion_upper_ns,
                decision_margin_ppm: item.decision_margin_ppm,
                evidence_warm: item.evidence_warm,
                evidence_stale: item.evidence_stale,
                compatible: item.compatible,
                memory_ok: item.memory_ok,
                earliest_deadline_ns: item.earliest_deadline_ns,
            };
            let d = decide(&input);
            assert_eq!(
                d.plan.as_str(),
                item.expected_plan.as_str(),
                "vector {} plan mismatch",
                item.id
            );
            assert_eq!(
                d.reason,
                item.expected_reason.as_str(),
                "vector {} reason mismatch",
                item.id
            );
        }
    }

    #[test]
    fn exact_tie_uses_integer_margin_equality() {
        // Mirrors exact-tie-natural golden: q=3, service=100000, batch=225000, margin=10%.
        let input = GoldenDecisionInput {
            queue_count: 3,
            natural_service_lower_ns: 100_000,
            batch_completion_upper_ns: 225_000,
            decision_margin_ppm: 100_000,
            evidence_warm: true,
            evidence_stale: false,
            compatible: true,
            memory_ok: true,
            earliest_deadline_ns: 10_000_000,
        };
        // mean=200000, tail=300000, J_n=500000; J_b=450000
        // lhs=450000*1e6=4.5e11; rhs=500000*900000=4.5e11 → tie
        let d = decide(&input);
        assert_eq!(d.plan, AwoPlan::Natural);
        assert_eq!(d.reason, "natural_tie");
    }

    #[test]
    fn overflow_service_returns_natural_arithmetic_overflow() {
        let input = GoldenDecisionInput {
            queue_count: u64::MAX,
            natural_service_lower_ns: u64::MAX,
            batch_completion_upper_ns: 1,
            decision_margin_ppm: 100_000,
            evidence_warm: true,
            evidence_stale: false,
            compatible: true,
            memory_ok: true,
            earliest_deadline_ns: u64::MAX,
        };
        let d = decide(&input);
        assert_eq!(d.plan, AwoPlan::Natural);
        assert_eq!(d.reason, "natural_arithmetic_overflow");
    }
}
