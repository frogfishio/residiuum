//! Registered bottleneck classifier (SPEC §11).
//!
//! Emits only closed-set verdicts. When evidence cannot isolate one cause,
//! returns `mixed_or_unknown` and forbids tuning advice.

use serde::{Deserialize, Serialize};

use super::calc::{efficiency, latency_residual_fraction, throughput_retention};
use super::match_run::{causal_ok, validate_matched_runs, RunEvidence};
use super::stats::{bootstrap_ci_mean, mean_mad, BootstrapCi, SampleStats};

/// Closed verdict set (must match `crate::VERDICTS`).
pub const REGISTERED_VERDICTS: &[&str] = &[
    "generator_bound",
    "cpu_transform_bound",
    "io_bandwidth_bound",
    "io_queue_underdriven",
    "serialized_writer",
    "durability_barrier_bound",
    "lifecycle_bound",
    "index_bound",
    "memory_pressure_bound",
    "telemetry_bound",
    "mixed_or_unknown",
];

/// Median residual > 5% of e2e blocks stage-level primary-bottleneck claims.
pub const RESIDUAL_INCOMPLETE_FRACTION: f64 = 0.05;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributionInput {
    /// Primary run under investigation.
    pub subject: RunEvidence,
    /// Optional matched comparison run (single control variable preferred).
    pub matched: Option<RunEvidence>,
    /// Repeated throughput samples for the subject cell (bytes/s).
    pub throughput_samples: Vec<f64>,
    /// Optional L0/L1/L2 envelope throughputs for retention ladder.
    pub t_l0: Option<f64>,
    pub t_l2: Option<f64>,
    pub t_l3: Option<f64>,
    /// Stage sum vs e2e for residual closure (nanoseconds).
    pub e2e_latency_ns: Option<f64>,
    pub stage_sum_ns: Option<f64>,
    /// Flags from harness / platform probes.
    pub single_core_saturated: bool,
    pub multi_store: bool,
    pub page_cache_warm_only: bool,
    pub lifecycle_spikes_present: bool,
    pub telemetry_delta_exceeds_budget: bool,
    pub memory_faults_align: bool,
    pub lock_residence_dominates: bool,
    pub index_stage_explains_loss: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AttributionResult {
    pub verdict: String,
    pub confidence: String,
    pub run_ids: Vec<String>,
    pub effect_size: Option<f64>,
    pub throughput_stats: Option<SampleStats>,
    pub throughput_ci: Option<BootstrapCi>,
    pub residual_fraction: Option<f64>,
    pub residual_incomplete: bool,
    pub retention: Option<f64>,
    pub efficiency: Option<f64>,
    pub contradicted_alternatives: Vec<String>,
    pub supporting_evidence: Vec<String>,
    pub allows_tuning_advice: bool,
    pub notes: Vec<String>,
}

/// Classify bottleneck from evidence. Never invents unregistered verdicts.
pub fn classify_bottleneck(input: &AttributionInput) -> AttributionResult {
    let mut notes = Vec::new();
    let mut run_ids = vec![input.subject.run_id.clone()];
    if let Some(m) = &input.matched {
        run_ids.push(m.run_id.clone());
    }

    // Correctness gate
    if input.subject.validity != "valid" {
        return mixed(
            run_ids,
            None,
            None,
            None,
            vec![],
            vec!["subject run is not validity=valid".into()],
            notes,
        );
    }

    let residual_fraction = match (input.e2e_latency_ns, input.stage_sum_ns) {
        (Some(e2e), Some(sum)) => latency_residual_fraction(e2e, sum),
        _ => input.subject.residual_fraction,
    };
    let residual_incomplete = residual_fraction
        .map(|r| r > RESIDUAL_INCOMPLETE_FRACTION)
        .unwrap_or(true);

    let throughput_stats = mean_mad(&input.throughput_samples);
    let throughput_ci = if input.throughput_samples.len() >= 2 {
        bootstrap_ci_mean(&input.throughput_samples, 200, 0x50_48_38_53_4545_44, 0.95)
    } else {
        None
    };

    // Single-run gate: a single sample cannot support a bottleneck verdict (SPEC §9).
    if input.throughput_samples.len() < 2 {
        notes.push("fewer than two repetitions; cannot support a bottleneck verdict".into());
        return mixed(
            run_ids,
            residual_fraction,
            throughput_stats,
            throughput_ci,
            vec![],
            vec!["insufficient repetitions".into()],
            notes,
        )
        .with_metrics(
            match (input.t_l3, input.t_l2) {
                (Some(a), Some(b)) => throughput_retention(a, b, "L3", "L2").retention,
                _ => None,
            },
            match (input.subject.throughput_bytes_per_sec, input.t_l2) {
                (t, Some(l2)) => efficiency(t, l2),
                _ => None,
            },
            None,
        );
    }

    // Matched-run causal hygiene
    let mut multi_var = false;
    if let Some(m) = &input.matched {
        match validate_matched_runs(&input.subject, m) {
            Ok(pair) => {
                if causal_ok(&pair).is_err() {
                    multi_var = true;
                    notes.push("multiple control variables changed; causal claim blocked".into());
                }
            }
            Err(e) => {
                notes.push(format!("matched-run validation failed: {e}"));
                return mixed(
                    run_ids,
                    residual_fraction,
                    throughput_stats,
                    throughput_ci,
                    vec![],
                    vec![format!("match error: {e}")],
                    notes,
                );
            }
        }
    }

    if multi_var {
        return mixed(
            run_ids,
            residual_fraction,
            throughput_stats,
            throughput_ci,
            vec!["two_changed_variables_called_causal".into()],
            vec!["multi-variable comparison cannot isolate a bottleneck".into()],
            notes,
        );
    }

    let efficiency = match (input.subject.throughput_bytes_per_sec, input.t_l2) {
        (t, Some(l2)) => efficiency(t, l2),
        _ => None,
    };

    let retention = if let (Some(t_l3), Some(t_l2)) = (input.t_l3, input.t_l2) {
        throughput_retention(t_l3, t_l2, "L3", "L2").retention
    } else {
        None
    };

    // --- Rule evaluation (most specific first) ---
    let s = &input.subject;
    let mut candidates: Vec<(String, f64, Vec<String>, Vec<String>)> = Vec::new();

    // telemetry_bound
    if input.telemetry_delta_exceeds_budget {
        candidates.push((
            "telemetry_bound".into(),
            0.9,
            vec!["telemetry-on/off matched pair exceeds declared budget".into()],
            vec!["database_cost_from_observer".into()],
        ));
    }

    // durability_barrier_bound
    if s.sync_per_op {
        if let Some(m) = &input.matched {
            if !m.sync_per_op && m.throughput_bytes_per_sec > s.throughput_bytes_per_sec * 1.1 {
                candidates.push((
                    "durability_barrier_bound".into(),
                    0.85,
                    vec![
                        "per-op sync on subject".into(),
                        "matched buffered run removes loss".into(),
                    ],
                    vec!["aggregate_bandwidth_hides_sync".into()],
                ));
            } else {
                candidates.push((
                    "durability_barrier_bound".into(),
                    0.6,
                    vec!["per-op sync present".into()],
                    vec![],
                ));
            }
        } else {
            candidates.push((
                "durability_barrier_bound".into(),
                0.55,
                vec!["per-op sync present; need buffered match to confirm".into()],
                vec![],
            ));
        }
    }

    // durability weakening false claim (subject weaker durability, higher T)
    if let Some(m) = &input.matched {
        if s.durability != m.durability {
            // weaker durability must not be celebrated as a win for stronger claim surface
            notes.push(format!(
                "durability differs: subject={} matched={}",
                s.durability, m.durability
            ));
        }
    }

    // lifecycle_bound
    if input.lifecycle_spikes_present {
        candidates.push((
            "lifecycle_bound".into(),
            0.8,
            vec!["seal/checkpoint/rotation spikes present in window".into()],
            vec!["means_hide_lifecycle_spikes".into()],
        ));
    }

    // memory_pressure_bound
    if input.memory_faults_align {
        candidates.push((
            "memory_pressure_bound".into(),
            0.75,
            vec!["faults/reclaim/RSS/dirty throttling align with loss".into()],
            vec![],
        ));
    }

    // index_bound
    if input.index_stage_explains_loss {
        candidates.push((
            "index_bound".into(),
            0.75,
            vec!["additive index layer and index stage time explain matched loss".into()],
            vec![],
        ));
    }

    // io_queue_underdriven vs io_bandwidth_bound
    let device_busy = s.device_util.unwrap_or(0.0) >= 0.85;
    let low_outstanding = s.outstanding_depth < 4 || s.outstanding < 4;
    if !device_busy && low_outstanding {
        candidates.push((
            "io_queue_underdriven".into(),
            0.8,
            vec!["device not busy".into(), "low outstanding I/O depth".into()],
            vec!["queue_starvation_called_disk_saturation".into()],
        ));
    } else if device_busy && s.outstanding_depth >= 4 {
        candidates.push((
            "io_bandwidth_bound".into(),
            0.8,
            vec![
                "device utilization near envelope".into(),
                "outstanding work present".into(),
            ],
            vec![],
        ));
    }

    // page-cache burst is not sustained device bandwidth
    if input.page_cache_warm_only && device_busy {
        notes.push("page-cache warm window; not sustained device throughput".into());
        // demote io_bandwidth_bound confidence if present
        for c in &mut candidates {
            if c.0 == "io_bandwidth_bound" {
                c.1 *= 0.3;
                c.3.push("page_cache_burst_as_sustained".into());
            }
        }
    }

    // serialized_writer
    if input.lock_residence_dominates {
        let cpu_idle = s.aggregate_cpu_idle.unwrap_or(0.0) > 0.2;
        let device_headroom = s.device_util.unwrap_or(1.0) < 0.7;
        if cpu_idle || device_headroom {
            candidates.push((
                "serialized_writer".into(),
                0.85,
                vec![
                    "lock/single-stage residence dominates".into(),
                    "CPU or device headroom remains".into(),
                ],
                vec![],
            ));
        }
    }

    // cpu_transform_bound (L3)
    if s.layer == "L3" || input.t_l3.is_some() {
        let cpu_busy = s.cpu_cores_busy.unwrap_or(0.0) >= 0.85 || input.single_core_saturated;
        if cpu_busy {
            candidates.push((
                "cpu_transform_bound".into(),
                0.8,
                vec!["L3 saturates available CPU".into()],
                if input.single_core_saturated && s.aggregate_cpu_idle.unwrap_or(0.0) > 0.3 {
                    vec!["idle_aggregate_hides_saturated_core".into()]
                } else {
                    vec![]
                },
            ));
        }
    }

    // generator_bound (L0 dominant; storage layers don't explain)
    if s.layer == "L0" {
        candidates.push((
            "generator_bound".into(),
            0.7,
            vec!["L0 consumes dominant capacity".into()],
            vec![],
        ));
    } else if let (Some(t0), Some(t2)) = (input.t_l0, input.t_l2) {
        if t0 > 0.0 && (t2 / t0) > 0.95 {
            candidates.push((
                "generator_bound".into(),
                0.65,
                vec!["storage layers do not explain loss vs L0".into()],
                vec![],
            ));
        }
    }

    // multi-store capacity ≠ single-store scaling
    if input.multi_store {
        notes.push("multi-store configuration; refuse single-store scaling claim".into());
        for c in &mut candidates {
            c.1 *= 0.5;
            c.3.push("multi_store_as_single_store_scaling".into());
        }
    }

    // residual incomplete blocks stage-level primary claims (SPEC §9).
    // Without closed accounting, refuse any primary bottleneck verdict.
    if residual_incomplete {
        notes.push(format!(
            "residual fraction {:?} exceeds {} — primary bottleneck blocked",
            residual_fraction, RESIDUAL_INCOMPLETE_FRACTION
        ));
        return mixed(
            run_ids,
            residual_fraction,
            throughput_stats,
            throughput_ci,
            candidates.into_iter().flat_map(|c| c.3).collect(),
            vec!["stage accounting residual incomplete".into()],
            notes,
        )
        .with_metrics(retention, efficiency, effect_size_preview(input));
    }

    // Pick best candidate above threshold
    candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let effect_size = effect_size_preview(input);

    if let Some((verdict, score, support, contradicted)) = candidates.first() {
        if *score >= 0.55 && verdict.as_str() != "mixed_or_unknown" {
            // single clear winner? require margin over second
            let clear = candidates
                .get(1)
                .map(|c| score - c.1 >= 0.1)
                .unwrap_or(true);
            if clear || *score >= 0.8 {
                let allows = input.throughput_samples.len() >= 2
                    && !residual_incomplete
                    && verdict.as_str() != "mixed_or_unknown";
                return AttributionResult {
                    verdict: verdict.clone(),
                    confidence: if *score >= 0.8 {
                        "high".into()
                    } else {
                        "medium".into()
                    },
                    run_ids,
                    effect_size,
                    throughput_stats,
                    throughput_ci,
                    residual_fraction,
                    residual_incomplete,
                    retention,
                    efficiency,
                    contradicted_alternatives: contradicted.clone(),
                    supporting_evidence: support.clone(),
                    allows_tuning_advice: allows,
                    notes,
                };
            }
        }
    }

    mixed(
        run_ids,
        residual_fraction,
        throughput_stats,
        throughput_ci,
        candidates.into_iter().flat_map(|c| c.3).collect::<Vec<_>>(),
        vec!["evidence cannot isolate one registered cause".into()],
        notes,
    )
    .with_metrics(retention, efficiency, effect_size)
}

fn mixed(
    run_ids: Vec<String>,
    residual_fraction: Option<f64>,
    throughput_stats: Option<SampleStats>,
    throughput_ci: Option<BootstrapCi>,
    contradicted: Vec<String>,
    support: Vec<String>,
    notes: Vec<String>,
) -> AttributionResult {
    AttributionResult {
        verdict: "mixed_or_unknown".into(),
        confidence: "low".into(),
        run_ids,
        effect_size: None,
        throughput_stats,
        throughput_ci,
        residual_fraction,
        residual_incomplete: residual_fraction
            .map(|r| r > RESIDUAL_INCOMPLETE_FRACTION)
            .unwrap_or(true),
        retention: None,
        efficiency: None,
        contradicted_alternatives: contradicted,
        supporting_evidence: support,
        allows_tuning_advice: false,
        notes,
    }
}

impl AttributionResult {
    fn with_metrics(
        mut self,
        retention: Option<f64>,
        efficiency: Option<f64>,
        effect_size: Option<f64>,
    ) -> Self {
        self.retention = retention;
        self.efficiency = efficiency;
        self.effect_size = effect_size;
        self
    }
}

/// True iff `v` is in the closed registered set.
pub fn is_registered_verdict(v: &str) -> bool {
    REGISTERED_VERDICTS.contains(&v)
}

fn effect_size_preview(input: &AttributionInput) -> Option<f64> {
    input.matched.as_ref().map(|m| {
        let ta = input.subject.throughput_bytes_per_sec;
        let tb = m.throughput_bytes_per_sec;
        if tb > 0.0 {
            (ta - tb) / tb
        } else {
            0.0
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(id: &str, layer: &str) -> RunEvidence {
        RunEvidence {
            run_id: id.into(),
            layer: layer.into(),
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
            device_util: Some(0.9),
            outstanding_depth: 8,
            sync_per_op: false,
            observer_overhead_fraction: Some(0.001),
            cpu_cores_busy: Some(0.5),
            aggregate_cpu_idle: Some(0.5),
            window_class: "sustained".into(),
            features: "l4_minimal".into(),
        }
    }

    #[test]
    fn registered_set_matches_crate() {
        for v in REGISTERED_VERDICTS {
            assert!(crate::VERDICTS.contains(v), "missing {v}");
        }
        assert_eq!(REGISTERED_VERDICTS.len(), crate::VERDICTS.len());
    }

    #[test]
    fn io_bandwidth_when_device_busy() {
        let mut s = base("s", "L2");
        s.device_util = Some(0.95);
        s.outstanding_depth = 16;
        let input = AttributionInput {
            subject: s,
            matched: None,
            throughput_samples: vec![1e8, 1.01e8, 0.99e8, 1.02e8, 1e8],
            t_l0: None,
            t_l2: Some(1.1e8),
            t_l3: None,
            e2e_latency_ns: Some(10_000.0),
            stage_sum_ns: Some(9_800.0),
            single_core_saturated: false,
            multi_store: false,
            page_cache_warm_only: false,
            lifecycle_spikes_present: false,
            telemetry_delta_exceeds_budget: false,
            memory_faults_align: false,
            lock_residence_dominates: false,
            index_stage_explains_loss: false,
        };
        let r = classify_bottleneck(&input);
        assert_eq!(r.verdict, "io_bandwidth_bound");
        assert!(is_registered_verdict(&r.verdict));
    }

    #[test]
    fn underdriven_queue() {
        let mut s = base("s", "L2");
        s.device_util = Some(0.2);
        s.outstanding_depth = 1;
        s.outstanding = 1;
        let input = AttributionInput {
            subject: s,
            matched: None,
            throughput_samples: vec![5e7, 5.1e7, 4.9e7],
            t_l0: None,
            t_l2: Some(1e8),
            t_l3: None,
            e2e_latency_ns: Some(10_000.0),
            stage_sum_ns: Some(9_700.0),
            single_core_saturated: false,
            multi_store: false,
            page_cache_warm_only: false,
            lifecycle_spikes_present: false,
            telemetry_delta_exceeds_budget: false,
            memory_faults_align: false,
            lock_residence_dominates: false,
            index_stage_explains_loss: false,
        };
        let r = classify_bottleneck(&input);
        assert_eq!(r.verdict, "io_queue_underdriven");
    }

    #[test]
    fn mixed_when_insufficient() {
        let s = base("s", "L4");
        let input = AttributionInput {
            subject: s,
            matched: None,
            throughput_samples: vec![1e8], // single sample
            t_l0: None,
            t_l2: None,
            t_l3: None,
            e2e_latency_ns: Some(10_000.0),
            stage_sum_ns: Some(5_000.0), // residual incomplete
            single_core_saturated: false,
            multi_store: false,
            page_cache_warm_only: false,
            lifecycle_spikes_present: false,
            telemetry_delta_exceeds_budget: false,
            memory_faults_align: false,
            lock_residence_dominates: false,
            index_stage_explains_loss: false,
        };
        let r = classify_bottleneck(&input);
        assert_eq!(r.verdict, "mixed_or_unknown");
        assert!(!r.allows_tuning_advice);
    }

    #[test]
    fn multi_var_forces_mixed() {
        let a = base("a", "L4");
        let mut b = base("b", "L4");
        b.concurrency = 8;
        b.durability = "memory".into();
        let input = AttributionInput {
            subject: a,
            matched: Some(b),
            throughput_samples: vec![1e8, 1e8, 1e8],
            t_l0: None,
            t_l2: Some(1e8),
            t_l3: None,
            e2e_latency_ns: Some(10_000.0),
            stage_sum_ns: Some(9_800.0),
            single_core_saturated: false,
            multi_store: false,
            page_cache_warm_only: false,
            lifecycle_spikes_present: false,
            telemetry_delta_exceeds_budget: false,
            memory_faults_align: false,
            lock_residence_dominates: false,
            index_stage_explains_loss: false,
        };
        let r = classify_bottleneck(&input);
        assert_eq!(r.verdict, "mixed_or_unknown");
    }
}
