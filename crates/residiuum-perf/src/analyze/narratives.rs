//! False-narrative detectors (plan §12 fixtures + SPEC §17 mutation spirit).
//!
//! Each detector returns a finding when a claim would mislead; mutation tests
//! prove the analyzer rejects these narratives.

use serde::{Deserialize, Serialize};

use super::match_run::{causal_ok, validate_matched_runs, RunEvidence};

/// Closed set of false-narrative ids required by the implementation plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FalseNarrative {
    IdleAggregateHidesSaturatedCore,
    PageCacheBurstAsSustained,
    PerOpSyncHiddenByAggregateBw,
    FailedOpsExcludedFromDenominator,
    HigherThroughputFromWeakerDurability,
    MultiStoreAsSingleStoreScaling,
    QueueStarvationCalledDiskSaturation,
    LifecycleSpikesHiddenByMeans,
    ObserverOverheadAsDbCost,
    TwoChangedVariablesAsCausal,
}

impl FalseNarrative {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IdleAggregateHidesSaturatedCore => "idle_aggregate_hides_saturated_core",
            Self::PageCacheBurstAsSustained => "page_cache_burst_as_sustained",
            Self::PerOpSyncHiddenByAggregateBw => "per_op_sync_hidden_by_aggregate_bw",
            Self::FailedOpsExcludedFromDenominator => "failed_ops_excluded_from_denominator",
            Self::HigherThroughputFromWeakerDurability => {
                "higher_throughput_from_weaker_durability"
            }
            Self::MultiStoreAsSingleStoreScaling => "multi_store_as_single_store_scaling",
            Self::QueueStarvationCalledDiskSaturation => "queue_starvation_called_disk_saturation",
            Self::LifecycleSpikesHiddenByMeans => "lifecycle_spikes_hidden_by_means",
            Self::ObserverOverheadAsDbCost => "observer_overhead_as_db_cost",
            Self::TwoChangedVariablesAsCausal => "two_changed_variables_as_causal",
        }
    }

    pub fn all() -> &'static [FalseNarrative] {
        &ALL_NARRATIVES
    }
}

const ALL_NARRATIVES: [FalseNarrative; 10] = [
    FalseNarrative::IdleAggregateHidesSaturatedCore,
    FalseNarrative::PageCacheBurstAsSustained,
    FalseNarrative::PerOpSyncHiddenByAggregateBw,
    FalseNarrative::FailedOpsExcludedFromDenominator,
    FalseNarrative::HigherThroughputFromWeakerDurability,
    FalseNarrative::MultiStoreAsSingleStoreScaling,
    FalseNarrative::QueueStarvationCalledDiskSaturation,
    FalseNarrative::LifecycleSpikesHiddenByMeans,
    FalseNarrative::ObserverOverheadAsDbCost,
    FalseNarrative::TwoChangedVariablesAsCausal,
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NarrativeFinding {
    pub narrative: FalseNarrative,
    pub narrative_id: String,
    pub detected: bool,
    pub detail: String,
}

/// Bundle of signals used to evaluate false narratives on a claim.
#[derive(Debug, Clone, Default)]
pub struct NarrativeContext {
    pub subject: Option<RunEvidence>,
    pub matched: Option<RunEvidence>,
    /// Claimed verdict the author would like to assert.
    pub claimed_verdict: Option<String>,
    /// Claimed throughput that may exclude failures.
    pub claimed_throughput_ops: Option<f64>,
    pub single_core_saturated: bool,
    pub multi_store: bool,
    pub page_cache_warm_only: bool,
    pub lifecycle_spikes_present: bool,
    /// p99 / mean ratio or similar spike indicator.
    pub latency_mean_ns: Option<f64>,
    pub latency_p99_ns: Option<f64>,
    /// Observer budget (fraction of e2e); default 0.02 if unset.
    pub observer_budget: Option<f64>,
}

/// Detect all plan-required false narratives present in `ctx`.
pub fn detect_false_narratives(ctx: &NarrativeContext) -> Vec<NarrativeFinding> {
    FalseNarrative::all()
        .iter()
        .copied()
        .map(|n| check_one(n, ctx))
        .collect()
}

/// True if every required narrative id was evaluated (detected or clean).
pub fn coverage_complete(findings: &[NarrativeFinding]) -> bool {
    let ids: std::collections::HashSet<_> =
        findings.iter().map(|f| f.narrative_id.as_str()).collect();
    FalseNarrative::all()
        .iter()
        .all(|n| ids.contains(n.as_str()))
}

fn check_one(n: FalseNarrative, ctx: &NarrativeContext) -> NarrativeFinding {
    let (detected, detail) = match n {
        FalseNarrative::IdleAggregateHidesSaturatedCore => idle_aggregate_hides_core(ctx),
        FalseNarrative::PageCacheBurstAsSustained => page_cache_burst(ctx),
        FalseNarrative::PerOpSyncHiddenByAggregateBw => per_op_sync_hidden(ctx),
        FalseNarrative::FailedOpsExcludedFromDenominator => failed_ops_excluded(ctx),
        FalseNarrative::HigherThroughputFromWeakerDurability => weaker_durability(ctx),
        FalseNarrative::MultiStoreAsSingleStoreScaling => multi_store_scaling(ctx),
        FalseNarrative::QueueStarvationCalledDiskSaturation => queue_as_disk(ctx),
        FalseNarrative::LifecycleSpikesHiddenByMeans => lifecycle_means(ctx),
        FalseNarrative::ObserverOverheadAsDbCost => observer_as_db(ctx),
        FalseNarrative::TwoChangedVariablesAsCausal => two_vars_causal(ctx),
    };
    NarrativeFinding {
        narrative: n,
        narrative_id: n.as_str().into(),
        detected,
        detail,
    }
}

fn idle_aggregate_hides_core(ctx: &NarrativeContext) -> (bool, String) {
    let Some(s) = &ctx.subject else {
        return (false, "no subject".into());
    };
    let idle = s.aggregate_cpu_idle.unwrap_or(0.0);
    if ctx.single_core_saturated && idle > 0.3 {
        (
            true,
            format!(
                "aggregate_cpu_idle={idle:.2} while one core is saturated; refuse idle-system claim"
            ),
        )
    } else {
        (false, "no saturated-core / idle-aggregate conflict".into())
    }
}

fn page_cache_burst(ctx: &NarrativeContext) -> (bool, String) {
    let Some(s) = &ctx.subject else {
        return (false, "no subject".into());
    };
    let claims_io = ctx
        .claimed_verdict
        .as_deref()
        .map(|v| v == "io_bandwidth_bound")
        .unwrap_or(false);
    if ctx.page_cache_warm_only
        && (claims_io || s.window_class == "burst" || s.window_class == "page_cache_warm")
    {
        (
            true,
            "page-cache warm / burst window cannot support sustained device throughput claim"
                .into(),
        )
    } else if ctx.page_cache_warm_only && s.device_util.unwrap_or(0.0) > 0.8 {
        (
            true,
            "high device_util during page-cache-only window is not sustained device BW".into(),
        )
    } else {
        (false, "no page-cache burst mislabel".into())
    }
}

fn per_op_sync_hidden(ctx: &NarrativeContext) -> (bool, String) {
    let Some(s) = &ctx.subject else {
        return (false, "no subject".into());
    };
    if s.sync_per_op {
        // High aggregate throughput claim while sync_per_op is on is suspicious
        // without a buffered matched control.
        let has_buffered_match = ctx
            .matched
            .as_ref()
            .map(|m| !m.sync_per_op)
            .unwrap_or(false);
        if !has_buffered_match {
            (
                true,
                "per-op sync present without buffered match; aggregate BW may hide durability cost"
                    .into(),
            )
        } else {
            (
                false,
                "buffered match available for sync attribution".into(),
            )
        }
    } else {
        (false, "no per-op sync".into())
    }
}

fn failed_ops_excluded(ctx: &NarrativeContext) -> (bool, String) {
    let Some(s) = &ctx.subject else {
        return (false, "no subject".into());
    };
    if s.failed_ops > 0 {
        // Denominator must include failures: acknowledged + failed should relate to attempted
        let accounted = s.acknowledged_ops.saturating_add(s.failed_ops);
        if accounted < s.attempted_ops {
            return (
                true,
                format!(
                    "failed_ops={} not fully reflected in denominator (acked+failed={}, attempted={})",
                    s.failed_ops, accounted, s.attempted_ops
                ),
            );
        }
        // Claimed throughput ops ignoring failures
        if let Some(claimed) = ctx.claimed_throughput_ops {
            let honest = s.acknowledged_ops as f64;
            if claimed > honest * 1.01 && s.failed_ops > 0 {
                return (
                    true,
                    format!(
                        "claimed throughput ops {claimed} exceeds honest acked {honest} with failures"
                    ),
                );
            }
        }
        // If validity is valid but failed_ops > 0 and throughput uses only successes implicitly
        if s.validity == "valid" && s.failed_ops > 0 {
            // Not automatically false — report when claimed_verdict ignores failures
            if ctx.claimed_verdict.is_some() {
                return (
                    true,
                    format!(
                        "run has failed_ops={}; any throughput claim must keep failures in denominator",
                        s.failed_ops
                    ),
                );
            }
        }
    }
    (false, "failures accounted or none".into())
}

fn weaker_durability(ctx: &NarrativeContext) -> (bool, String) {
    let (Some(s), Some(m)) = (&ctx.subject, &ctx.matched) else {
        return (false, "need subject+matched".into());
    };
    if s.durability == m.durability {
        return (false, "same durability".into());
    }
    // Rank durability strength (higher = stronger)
    let rank = |d: &str| -> i32 {
        match d {
            "memory" | "none" | "buffered" => 0,
            "flush" | "os_flush" => 1,
            "durable" | "fsync" | "group_commit" => 2,
            _ => 1,
        }
    };
    let rs = rank(&s.durability);
    let rm = rank(&m.durability);
    if rs < rm && s.throughput_bytes_per_sec > m.throughput_bytes_per_sec {
        let claims_win = ctx
            .claimed_verdict
            .as_deref()
            .map(|v| v != "mixed_or_unknown" && v != "durability_barrier_bound")
            .unwrap_or(true);
        if claims_win {
            return (
                true,
                format!(
                    "higher throughput on weaker durability ({} vs {}); not a same-surface win",
                    s.durability, m.durability
                ),
            );
        }
    }
    (false, "no weaker-durability throughput celebration".into())
}

fn multi_store_scaling(ctx: &NarrativeContext) -> (bool, String) {
    if ctx.multi_store {
        let claims_scale = ctx
            .claimed_verdict
            .as_deref()
            .map(|v| v.contains("scale") || v == "io_bandwidth_bound" || v == "serialized_writer")
            .unwrap_or(false);
        // Always flag multi-store when present for single-store scaling claims
        (
            true,
            if claims_scale {
                "multi-store capacity must not be reported as single-store scaling".into()
            } else {
                "multi-store configuration present; refuse single-store scaling attribution".into()
            },
        )
    } else {
        (false, "single-store".into())
    }
}

fn queue_as_disk(ctx: &NarrativeContext) -> (bool, String) {
    let Some(s) = &ctx.subject else {
        return (false, "no subject".into());
    };
    let claims_disk = ctx
        .claimed_verdict
        .as_deref()
        .map(|v| v == "io_bandwidth_bound")
        .unwrap_or(false);
    let low_q = s.outstanding_depth < 4 || s.outstanding < 4;
    let device_idle = s.device_util.unwrap_or(0.0) < 0.5;
    if claims_disk && low_q && device_idle {
        (
            true,
            "claimed disk saturation while device idle and queue underdriven".into(),
        )
    } else if low_q && device_idle && s.device_util.is_some() {
        // Even without explicit claim, surface the hazard when someone might call it disk
        if claims_disk {
            (
                true,
                "queue starvation mislabeled as disk saturation".into(),
            )
        } else {
            (false, "underdriven queue without disk claim".into())
        }
    } else {
        (false, "no queue/disk confusion".into())
    }
}

fn lifecycle_means(ctx: &NarrativeContext) -> (bool, String) {
    let Some(s) = &ctx.subject else {
        return (false, "no subject".into());
    };
    if !ctx.lifecycle_spikes_present {
        return (false, "no lifecycle spikes".into());
    }
    let mean = ctx
        .latency_mean_ns
        .or_else(|| s.p50_latency_ns.map(|x| x as f64));
    let p99 = ctx
        .latency_p99_ns
        .or_else(|| s.p99_latency_ns.map(|x| x as f64));
    if let (Some(m), Some(p)) = (mean, p99) {
        if m > 0.0 && p / m >= 3.0 {
            return (
                true,
                format!(
                    "lifecycle spikes present; p99/mean={:.1} — means hide tail/lifecycle cost",
                    p / m
                ),
            );
        }
    }
    // Spikes flagged by harness even without ratio
    (
        true,
        "lifecycle spikes present; mean-only summary would hide them".into(),
    )
}

fn observer_as_db(ctx: &NarrativeContext) -> (bool, String) {
    let Some(s) = &ctx.subject else {
        return (false, "no subject".into());
    };
    let budget = ctx.observer_budget.unwrap_or(0.02);
    if let Some(oh) = s.observer_overhead_fraction {
        if oh > budget {
            return (
                true,
                format!(
                    "observer_overhead_fraction={oh:.4} exceeds budget={budget:.4}; not database cost"
                ),
            );
        }
    }
    let claims_db = ctx
        .claimed_verdict
        .as_deref()
        .map(|v| {
            matches!(
                v,
                "cpu_transform_bound" | "io_bandwidth_bound" | "serialized_writer"
            )
        })
        .unwrap_or(false);
    if claims_db {
        if let Some(oh) = s.observer_overhead_fraction {
            if oh > budget * 0.5 {
                return (
                    true,
                    format!(
                        "material observer overhead ({oh:.4}) must not be folded into DB bottleneck claim"
                    ),
                );
            }
        }
    }
    (false, "observer within budget".into())
}

fn two_vars_causal(ctx: &NarrativeContext) -> (bool, String) {
    let (Some(a), Some(b)) = (&ctx.subject, &ctx.matched) else {
        return (false, "need pair".into());
    };
    match validate_matched_runs(a, b) {
        Ok(m) => match causal_ok(&m) {
            Ok(()) => (
                false,
                "single control variable (or layer ladder only)".into(),
            ),
            Err(_) => (
                true,
                format!(
                    "multiple control variables changed: {:?}; refuse causal claim",
                    m.changed_keys
                ),
            ),
        },
        Err(e) => (true, format!("match invalid: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(id: &str) -> RunEvidence {
        RunEvidence {
            run_id: id.into(),
            layer: "L4".into(),
            durability: "durable".into(),
            payload_size: 4096,
            concurrency: 1,
            outstanding: 1,
            config_hash: "abc".into(),
            throughput_bytes_per_sec: 1e8,
            p50_latency_ns: Some(1000),
            p99_latency_ns: Some(5000),
            residual_fraction: Some(0.02),
            validity: "valid".into(),
            failed_ops: 0,
            attempted_ops: 100,
            acknowledged_ops: 100,
            device_util: Some(0.3),
            outstanding_depth: 1,
            sync_per_op: false,
            observer_overhead_fraction: Some(0.001),
            cpu_cores_busy: Some(0.2),
            aggregate_cpu_idle: Some(0.7),
            window_class: "sustained".into(),
            features: "l4_minimal".into(),
        }
    }

    #[test]
    fn all_ten_narratives_evaluated() {
        let ctx = NarrativeContext {
            subject: Some(base("a")),
            matched: Some(base("b")),
            ..Default::default()
        };
        let f = detect_false_narratives(&ctx);
        assert_eq!(f.len(), 10);
        assert!(coverage_complete(&f));
    }

    #[test]
    fn detects_idle_aggregate() {
        let mut s = base("a");
        s.aggregate_cpu_idle = Some(0.8);
        let ctx = NarrativeContext {
            subject: Some(s),
            single_core_saturated: true,
            ..Default::default()
        };
        let f = detect_false_narratives(&ctx);
        let hit = f
            .iter()
            .find(|x| x.narrative == FalseNarrative::IdleAggregateHidesSaturatedCore)
            .unwrap();
        assert!(hit.detected);
    }

    #[test]
    fn detects_two_variables() {
        let a = base("a");
        let mut b = base("b");
        b.concurrency = 8;
        b.durability = "memory".into();
        let ctx = NarrativeContext {
            subject: Some(a),
            matched: Some(b),
            ..Default::default()
        };
        let f = detect_false_narratives(&ctx);
        let hit = f
            .iter()
            .find(|x| x.narrative == FalseNarrative::TwoChangedVariablesAsCausal)
            .unwrap();
        assert!(hit.detected);
    }

    #[test]
    fn detects_weaker_durability() {
        let mut s = base("fast");
        s.durability = "memory".into();
        s.throughput_bytes_per_sec = 2e8;
        let mut m = base("slow");
        m.durability = "durable".into();
        m.throughput_bytes_per_sec = 1e8;
        let ctx = NarrativeContext {
            subject: Some(s),
            matched: Some(m),
            claimed_verdict: Some("io_bandwidth_bound".into()),
            ..Default::default()
        };
        let f = detect_false_narratives(&ctx);
        let hit = f
            .iter()
            .find(|x| x.narrative == FalseNarrative::HigherThroughputFromWeakerDurability)
            .unwrap();
        assert!(hit.detected);
    }

    #[test]
    fn detects_failed_ops() {
        let mut s = base("a");
        s.failed_ops = 10;
        s.acknowledged_ops = 90;
        s.attempted_ops = 100;
        let ctx = NarrativeContext {
            subject: Some(s),
            claimed_throughput_ops: Some(100.0),
            claimed_verdict: Some("io_bandwidth_bound".into()),
            ..Default::default()
        };
        let f = detect_false_narratives(&ctx);
        let hit = f
            .iter()
            .find(|x| x.narrative == FalseNarrative::FailedOpsExcludedFromDenominator)
            .unwrap();
        assert!(hit.detected);
    }

    #[test]
    fn detects_queue_as_disk() {
        let mut s = base("a");
        s.device_util = Some(0.1);
        s.outstanding_depth = 1;
        let ctx = NarrativeContext {
            subject: Some(s),
            claimed_verdict: Some("io_bandwidth_bound".into()),
            ..Default::default()
        };
        let f = detect_false_narratives(&ctx);
        let hit = f
            .iter()
            .find(|x| x.narrative == FalseNarrative::QueueStarvationCalledDiskSaturation)
            .unwrap();
        assert!(hit.detected);
    }
}
