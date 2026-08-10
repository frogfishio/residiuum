//! Ranked falsification experiments for a bottleneck verdict (SPEC §11).

use serde::{Deserialize, Serialize};

use super::classify::AttributionResult;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FalsificationExperiment {
    pub rank: u32,
    pub id: String,
    pub title: String,
    pub changes_one_variable: String,
    pub expected_if_verdict_true: String,
    pub expected_if_verdict_false: String,
    pub related_run_ids: Vec<String>,
}

/// Generate ranked follow-up experiments that could falsify `result.verdict`.
pub fn falsification_experiments(result: &AttributionResult) -> Vec<FalsificationExperiment> {
    let runs = result.run_ids.clone();
    let mut out = match result.verdict.as_str() {
        "generator_bound" => vec![exp(
            1,
            "raise_l0_cost",
            "Increase generator cost / payload entropy",
            "generator_cpu_or_entropy",
            "L0 and all upper layers drop proportionally",
            "upper layers stay flat while L0 drops → not generator-bound",
            &runs,
        )],
        "cpu_transform_bound" => vec![
            exp(
                1,
                "disable_transform_stage",
                "Null out the dominant L3 transform stage",
                "transform_stage_enabled",
                "throughput rises toward L2 envelope",
                "no gain → transform not the ceiling",
                &runs,
            ),
            exp(
                2,
                "add_cores_or_shards",
                "Increase worker shards with affinity held constant",
                "writer_shards",
                "near-linear gain until other bound appears",
                "no gain with free cores → not CPU transform",
                &runs,
            ),
        ],
        "io_bandwidth_bound" => vec![
            exp(
                1,
                "increase_outstanding",
                "Raise outstanding I/O depth",
                "outstanding_limit",
                "little further gain (already at device envelope)",
                "large gain → was queue underdriven, not BW",
                &runs,
            ),
            exp(
                2,
                "cold_cache_rerun",
                "Rerun with cold page cache / larger than RAM set",
                "dataset_state",
                "throughput stays near prior sustained rate",
                "collapse → prior result was page-cache burst",
                &runs,
            ),
        ],
        "io_queue_underdriven" => vec![exp(
            1,
            "raise_queue_depth",
            "Increase outstanding depth and concurrency",
            "outstanding_limit",
            "throughput rises and device util climbs",
            "no rise → different bound (CPU/lock/durability)",
            &runs,
        )],
        "serialized_writer" => vec![exp(
            1,
            "shard_writers",
            "Increase writer shard count",
            "writer_shards",
            "throughput scales until next bottleneck",
            "no scale with idle CPU/device → not single-writer",
            &runs,
        )],
        "durability_barrier_bound" => vec![exp(
            1,
            "buffered_match",
            "Matched run with buffered/group-commit durability",
            "durability",
            "loss largely disappears vs sync-per-op",
            "loss remains → not durability barrier",
            &runs,
        )],
        "lifecycle_bound" => vec![exp(
            1,
            "stretch_rotation",
            "Increase seal/rotation threshold to reduce lifecycle rate",
            "segment_rotation_threshold",
            "tails and mean loss improve",
            "no change → not lifecycle-bound",
            &runs,
        )],
        "index_bound" => vec![exp(
            1,
            "index_off_match",
            "Matched run without additive index features",
            "features",
            "matched loss recovers",
            "loss remains → not index-bound",
            &runs,
        )],
        "memory_pressure_bound" => vec![exp(
            1,
            "raise_memory_budget",
            "Controlled increase of process/cache memory budget",
            "cache_budget",
            "faults drop and throughput rises",
            "no change → not memory pressure",
            &runs,
        )],
        "telemetry_bound" => vec![exp(
            1,
            "telemetry_off",
            "Matched run with telemetry disabled",
            "instrumentation_mode",
            "overhead disappears within budget",
            "overhead remains → not telemetry",
            &runs,
        )],
        _ => vec![exp(
            1,
            "isolate_one_variable",
            "Re-run with a single control variable change and ≥5 reps",
            "one_registered_parameter",
            "isolates a registered verdict with CI excluding zero effect",
            "still ambiguous → remain mixed_or_unknown (no tuning advice)",
            &runs,
        )],
    };

    // Always attach residual-closure check when residual incomplete
    if result.residual_incomplete {
        out.push(exp(
            99,
            "close_stage_residual",
            "Improve stage instrumentation until |e2e-sum|/e2e ≤ 5%",
            "instrumentation_mode",
            "residual within bound; stage verdicts become eligible",
            "residual stays high → keep mixed_or_unknown for stage claims",
            &runs,
        ));
    }

    out.sort_by_key(|e| e.rank);
    out
}

fn exp(
    rank: u32,
    id: &str,
    title: &str,
    var: &str,
    if_true: &str,
    if_false: &str,
    runs: &[String],
) -> FalsificationExperiment {
    FalsificationExperiment {
        rank,
        id: id.into(),
        title: title.into(),
        changes_one_variable: var.into(),
        expected_if_verdict_true: if_true.into(),
        expected_if_verdict_false: if_false.into(),
        related_run_ids: runs.to_vec(),
    }
}

/// Candidate tuning comparison record (report only — never auto-applied).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TuningCandidateComparison {
    pub parameter: String,
    pub baseline_throughput: f64,
    pub candidate_throughput: f64,
    pub delta: f64,
    pub ci_excludes_zero: bool,
    pub durability_unchanged: bool,
    pub correctness_green: bool,
    pub accepted_for_recommendation: bool,
    pub reason: String,
}

/// Compare baseline vs candidate throughputs for a single registered parameter.
/// V1 emits recommendations only; never applies them.
pub fn compare_tuning_candidate(
    parameter: &str,
    baseline_samples: &[f64],
    candidate_samples: &[f64],
    durability_unchanged: bool,
    correctness_green: bool,
) -> TuningCandidateComparison {
    let b = mean(baseline_samples);
    let c = mean(candidate_samples);
    let delta = c - b;
    // Simple sign-stable CI proxy: all paired direction or bootstrap overlap
    let ci_excludes_zero =
        delta > 0.0 && baseline_samples.len() >= 3 && candidate_samples.len() >= 3 && {
            let bci = super::stats::bootstrap_ci_mean(baseline_samples, 100, 1, 0.95);
            let cci = super::stats::bootstrap_ci_mean(candidate_samples, 100, 2, 0.95);
            match (bci, cci) {
                (Some(b0), Some(c0)) => c0.low > b0.high,
                _ => false,
            }
        };
    let accepted = delta > 0.0 && ci_excludes_zero && durability_unchanged && correctness_green;
    TuningCandidateComparison {
        parameter: parameter.into(),
        baseline_throughput: b,
        candidate_throughput: c,
        delta,
        ci_excludes_zero,
        durability_unchanged,
        correctness_green,
        accepted_for_recommendation: accepted,
        reason: if accepted {
            "ΔT>0 with CI excluding zero; durability+correctness held".into()
        } else if !durability_unchanged {
            "rejected: durability changed".into()
        } else if !correctness_green {
            "rejected: correctness not green".into()
        } else if !ci_excludes_zero {
            "rejected: confidence interval does not exclude zero".into()
        } else {
            "rejected: non-positive delta".into()
        },
    }
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyze::classify::AttributionResult;

    fn sample_result(verdict: &str) -> AttributionResult {
        AttributionResult {
            verdict: verdict.into(),
            confidence: "high".into(),
            run_ids: vec!["r1".into(), "r2".into()],
            effect_size: Some(-0.2),
            throughput_stats: None,
            throughput_ci: None,
            residual_fraction: Some(0.02),
            residual_incomplete: false,
            retention: None,
            efficiency: None,
            contradicted_alternatives: vec![],
            supporting_evidence: vec![],
            allows_tuning_advice: true,
            notes: vec![],
        }
    }

    #[test]
    fn falsify_io_bw_has_queue_and_cold() {
        let e = falsification_experiments(&sample_result("io_bandwidth_bound"));
        assert!(e.iter().any(|x| x.id == "increase_outstanding"));
        assert!(e.iter().any(|x| x.id == "cold_cache_rerun"));
        assert!(e.iter().all(|x| !x.related_run_ids.is_empty()));
    }

    #[test]
    fn mixed_gets_isolation_experiment() {
        let mut r = sample_result("mixed_or_unknown");
        r.allows_tuning_advice = false;
        r.residual_incomplete = true;
        let e = falsification_experiments(&r);
        assert!(e.iter().any(|x| x.id == "isolate_one_variable"));
        assert!(e.iter().any(|x| x.id == "close_stage_residual"));
    }

    #[test]
    fn tuning_rejects_durability_change() {
        let c = compare_tuning_candidate(
            "outstanding_limit",
            &[100.0, 101.0, 99.0],
            &[150.0, 151.0, 149.0],
            false,
            true,
        );
        assert!(!c.accepted_for_recommendation);
        assert!(c.reason.contains("durability"));
    }
}
