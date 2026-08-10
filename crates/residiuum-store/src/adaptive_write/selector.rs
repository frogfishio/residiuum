//! Candidate plan generation and selection (plan §11.3–11.5, AWO-5).
//!
//! Reuses pure [`super::model::decide`] for the natural-vs-batch margin test so
//! golden vectors remain the single arithmetic authority.

use super::model::{decide, GoldenDecisionInput};
use super::types::{AwoPlan, DECISION_MARGIN_PPM_DEFAULT};

/// Closed power-of-two candidates plus optional queued count (policy-v1).
pub const CANDIDATE_POW2: &[u64] = &[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];

/// Inputs for building the candidate set.
#[derive(Debug, Clone, Copy)]
pub struct CandidateBounds {
    /// Requests currently queued.
    pub queued_count: u64,
    /// Entry credit remaining.
    pub entries_available: u64,
    /// Max batch entries (policy).
    pub maximum_batch_entries: u64,
    /// Approx bytes per entry for byte clip (0 = skip byte clip).
    pub bytes_per_entry: u64,
    /// Byte credit remaining.
    pub bytes_available: u64,
    /// Max batch bytes (policy).
    pub maximum_batch_bytes: u64,
    /// Segment room in entries (conservative).
    pub segment_room_entries: u64,
}

/// Build clipped candidate entry counts (includes `1` = natural).
pub fn candidate_entry_counts(b: &CandidateBounds) -> Vec<u64> {
    let mut out = Vec::with_capacity(CANDIDATE_POW2.len() + 1);
    let hard = b
        .queued_count
        .min(b.entries_available)
        .min(b.maximum_batch_entries)
        .min(b.segment_room_entries);
    if hard == 0 {
        return vec![1];
    }
    for &c in CANDIDATE_POW2 {
        if c > hard {
            break;
        }
        if b.bytes_per_entry > 0 {
            let need = b.bytes_per_entry.saturating_mul(c);
            if need > b.bytes_available || need > b.maximum_batch_bytes {
                continue;
            }
        }
        out.push(c);
    }
    // Always include natural 1 if missing and hard >= 1.
    if !out.contains(&1) && hard >= 1 {
        out.insert(0, 1);
    }
    // Include current queued count when not already a power of two and fits.
    let q = b.queued_count.min(hard);
    if q > 1 && !out.contains(&q) {
        if b.bytes_per_entry == 0
            || b.bytes_per_entry.saturating_mul(q) <= b.bytes_available.min(b.maximum_batch_bytes)
        {
            out.push(q);
            out.sort_unstable();
        }
    }
    if out.is_empty() {
        out.push(1);
    }
    out
}

/// Selection among candidates using pure decide on each batch size > 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    /// Chosen plan class.
    pub plan: AwoPlan,
    /// Batch size (1 for natural).
    pub entries: u64,
    /// Closed reason id.
    pub reason: &'static str,
}

/// Select natural vs best batch among candidates.
///
/// `natural_service_lower_ns` and `batch_completion_upper_for_n` supply bounds.
/// `batch_completion_upper_for_n(n)` returns upper completion for a batch of `n`.
pub fn select_plan<F>(
    bounds: &CandidateBounds,
    natural_service_lower_ns: u64,
    decision_margin_ppm: u32,
    evidence_warm: bool,
    evidence_stale: bool,
    compatible: bool,
    earliest_deadline_ns: u64,
    mut batch_completion_upper_for_n: F,
) -> Selection
where
    F: FnMut(u64) -> u64,
{
    let margin = if decision_margin_ppm == 0 {
        DECISION_MARGIN_PPM_DEFAULT
    } else {
        decision_margin_ppm
    };
    let candidates = candidate_entry_counts(bounds);
    // Natural for n=1 is mandatory path.
    if bounds.queued_count <= 1 {
        return Selection {
            plan: AwoPlan::Natural,
            entries: 1,
            reason: "natural_single_request",
        };
    }

    let mut best_batch: Option<(u64, u64, &'static str)> = None; // (n, J_batch, reason)

    for &n in &candidates {
        if n <= 1 {
            continue;
        }
        // Memory: entries already clipped; still pass memory_ok true.
        let batch_upper = batch_completion_upper_for_n(n);
        let input = GoldenDecisionInput {
            queue_count: n,
            natural_service_lower_ns,
            batch_completion_upper_ns: batch_upper,
            decision_margin_ppm: margin,
            evidence_warm,
            evidence_stale,
            compatible,
            memory_ok: true,
            earliest_deadline_ns,
        };
        let d = decide(&input);
        if d.plan == AwoPlan::Batch {
            match best_batch {
                None => best_batch = Some((n, batch_upper, d.reason)),
                Some((bn, bj, _)) => {
                    // Among winning batches: lowest J; ties fewer bytes≈entries.
                    if batch_upper < bj || (batch_upper == bj && n < bn) {
                        best_batch = Some((n, batch_upper, d.reason));
                    }
                }
            }
        }
    }

    if let Some((n, _, reason)) = best_batch {
        return Selection {
            plan: AwoPlan::Batch,
            entries: n,
            reason,
        };
    }

    // No batch won — re-run decide on full queue for a specific natural reason.
    let n = bounds.queued_count.min(bounds.maximum_batch_entries).max(1);
    let batch_upper = batch_completion_upper_for_n(n.max(2));
    let input = GoldenDecisionInput {
        queue_count: n.max(2),
        natural_service_lower_ns,
        batch_completion_upper_ns: batch_upper,
        decision_margin_ppm: margin,
        evidence_warm,
        evidence_stale,
        compatible,
        memory_ok: true,
        earliest_deadline_ns,
    };
    let d = decide(&input);
    Selection {
        plan: AwoPlan::Natural,
        entries: 1,
        reason: d.reason,
    }
}

/// Collection delay decision (plan §11.5).
///
/// Returns delay ns to wait (0 = evaluate/release now). Cap is policy max.
pub fn collection_delay_ns(
    queued_count: u64,
    evidence_warm: bool,
    predicts_winning_batch: bool,
    max_collection_delay_ns: u64,
) -> u64 {
    if queued_count >= 2 {
        return 0;
    }
    if queued_count == 1 && evidence_warm && predicts_winning_batch {
        return max_collection_delay_ns;
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidates_clip_and_include_queued() {
        let b = CandidateBounds {
            queued_count: 10,
            entries_available: 100,
            maximum_batch_entries: 1024,
            bytes_per_entry: 0,
            bytes_available: 0,
            maximum_batch_bytes: 0,
            segment_room_entries: 1000,
        };
        let c = candidate_entry_counts(&b);
        assert!(c.contains(&1));
        assert!(c.contains(&8));
        assert!(c.contains(&10));
        assert!(!c.contains(&16)); // 16 > queued 10
    }

    #[test]
    fn select_natural_when_cold() {
        let b = CandidateBounds {
            queued_count: 8,
            entries_available: 100,
            maximum_batch_entries: 1024,
            bytes_per_entry: 0,
            bytes_available: u64::MAX,
            maximum_batch_bytes: u64::MAX,
            segment_room_entries: 1000,
        };
        let s = select_plan(&b, 1000, 100_000, false, false, true, u64::MAX, |_| 10);
        assert_eq!(s.plan, AwoPlan::Natural);
        assert_eq!(s.reason, "natural_insufficient_evidence");
    }

    #[test]
    fn select_batch_when_gain() {
        let b = CandidateBounds {
            queued_count: 8,
            entries_available: 100,
            maximum_batch_entries: 1024,
            bytes_per_entry: 0,
            bytes_available: u64::MAX,
            maximum_batch_bytes: u64::MAX,
            segment_room_entries: 1000,
        };
        // natural J for q=8 service=1000: mean=4500 tail=8000 J=12500
        // batch upper must be < J * 0.9 = 11250
        let s = select_plan(&b, 1000, 100_000, true, false, true, u64::MAX, |_| 5_000);
        assert_eq!(s.plan, AwoPlan::Batch);
        assert!(s.entries >= 2);
    }

    #[test]
    fn collection_delay_rules() {
        assert_eq!(collection_delay_ns(2, true, true, 250_000), 0);
        assert_eq!(collection_delay_ns(1, true, true, 250_000), 250_000);
        assert_eq!(collection_delay_ns(1, false, true, 250_000), 0);
        assert_eq!(collection_delay_ns(1, true, false, 250_000), 0);
    }
}
