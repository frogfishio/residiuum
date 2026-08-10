//! PQH-8: attribution analyzer, bottleneck verdicts, false-narrative detectors.
//!
//! Consumes matched-run evidence and emits only registered verdicts. When
//! evidence is insufficient, returns `mixed_or_unknown` with no tuning advice.

mod calc;
mod classify;
mod falsify;
mod match_run;
mod narratives;
mod report;
mod stats;

pub use calc::{
    amplification, efficiency, latency_residual_fraction, scaling_efficiency, throughput_retention,
    RetentionReport,
};
pub use classify::{
    classify_bottleneck, is_registered_verdict, AttributionInput, AttributionResult,
    REGISTERED_VERDICTS, RESIDUAL_INCOMPLETE_FRACTION,
};
pub use falsify::{
    compare_tuning_candidate, falsification_experiments, FalsificationExperiment,
    TuningCandidateComparison,
};
pub use match_run::{causal_ok, validate_matched_runs, MatchError, MatchedRun, RunEvidence};
pub use narratives::{
    coverage_complete, detect_false_narratives, FalseNarrative, NarrativeContext, NarrativeFinding,
};
pub use report::{render_markdown, AttributionReport, REPORT_PROFILE};
pub use stats::{bootstrap_ci_mean, mean_mad, BootstrapCi, SampleStats};

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AnalyzeError {
    #[error("analyze: {0}")]
    Msg(String),
    #[error("match: {0}")]
    Match(#[from] MatchError),
}

/// End-to-end helper: classify + scan narratives + build report.
pub fn analyze_attribution(
    input: &AttributionInput,
    narrative_ctx: &NarrativeContext,
    tuning: Vec<TuningCandidateComparison>,
) -> AttributionReport {
    let attribution = classify_bottleneck(input);
    let mut ctx = NarrativeContext {
        subject: Some(input.subject.clone()),
        matched: input.matched.clone(),
        claimed_verdict: Some(attribution.verdict.clone()),
        claimed_throughput_ops: narrative_ctx.claimed_throughput_ops,
        single_core_saturated: input.single_core_saturated || narrative_ctx.single_core_saturated,
        multi_store: input.multi_store || narrative_ctx.multi_store,
        page_cache_warm_only: input.page_cache_warm_only || narrative_ctx.page_cache_warm_only,
        lifecycle_spikes_present: input.lifecycle_spikes_present
            || narrative_ctx.lifecycle_spikes_present,
        latency_mean_ns: narrative_ctx.latency_mean_ns,
        latency_p99_ns: narrative_ctx.latency_p99_ns,
        observer_budget: narrative_ctx.observer_budget,
    };
    // Prefer explicit narrative_ctx subject/matched if provided
    if narrative_ctx.subject.is_some() {
        ctx.subject = narrative_ctx.subject.clone();
    }
    if narrative_ctx.matched.is_some() {
        ctx.matched = narrative_ctx.matched.clone();
    }
    let false_narratives = detect_false_narratives(&ctx);
    // mixed_or_unknown must never carry tuning advice
    let tuning = if attribution.allows_tuning_advice {
        tuning
    } else {
        vec![]
    };
    AttributionReport::build(attribution, false_narratives, tuning)
}

#[cfg(test)]
mod suite_tests {
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
            p50_latency_ns: Some(1_000),
            p99_latency_ns: Some(5_000),
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

    /// Mutation suite: every plan §12 false narrative is detected when injected.
    #[test]
    fn all_plan_false_narratives_detected_when_injected() {
        // 1. idle aggregate CPU hiding saturated core
        {
            let mut s = base("idle", "L3");
            s.aggregate_cpu_idle = Some(0.85);
            s.cpu_cores_busy = Some(0.15);
            let f = detect_false_narratives(&NarrativeContext {
                subject: Some(s),
                single_core_saturated: true,
                ..Default::default()
            });
            assert!(f.iter().any(|x| {
                x.narrative == FalseNarrative::IdleAggregateHidesSaturatedCore && x.detected
            }));
        }

        // 2. page-cache burst as sustained device
        {
            let mut s = base("pc", "L2");
            s.device_util = Some(0.95);
            s.window_class = "page_cache_warm".into();
            let f = detect_false_narratives(&NarrativeContext {
                subject: Some(s),
                page_cache_warm_only: true,
                claimed_verdict: Some("io_bandwidth_bound".into()),
                ..Default::default()
            });
            assert!(f.iter().any(|x| {
                x.narrative == FalseNarrative::PageCacheBurstAsSustained && x.detected
            }));
        }

        // 3. per-op sync hidden by aggregate BW
        {
            let mut s = base("sync", "L6");
            s.sync_per_op = true;
            s.throughput_bytes_per_sec = 2e8;
            let f = detect_false_narratives(&NarrativeContext {
                subject: Some(s),
                ..Default::default()
            });
            assert!(f.iter().any(|x| {
                x.narrative == FalseNarrative::PerOpSyncHiddenByAggregateBw && x.detected
            }));
        }

        // 4. failed ops excluded from denominator
        {
            let mut s = base("fail", "L4");
            s.failed_ops = 20;
            s.acknowledged_ops = 80;
            s.attempted_ops = 100;
            let f = detect_false_narratives(&NarrativeContext {
                subject: Some(s),
                claimed_throughput_ops: Some(100.0),
                claimed_verdict: Some("io_bandwidth_bound".into()),
                ..Default::default()
            });
            assert!(f.iter().any(|x| {
                x.narrative == FalseNarrative::FailedOpsExcludedFromDenominator && x.detected
            }));
        }

        // 5. higher throughput from weaker durability
        {
            let mut s = base("weak", "L4");
            s.durability = "memory".into();
            s.throughput_bytes_per_sec = 3e8;
            let mut m = base("strong", "L4");
            m.durability = "durable".into();
            m.throughput_bytes_per_sec = 1e8;
            let f = detect_false_narratives(&NarrativeContext {
                subject: Some(s),
                matched: Some(m),
                claimed_verdict: Some("io_bandwidth_bound".into()),
                ..Default::default()
            });
            assert!(f.iter().any(|x| {
                x.narrative == FalseNarrative::HigherThroughputFromWeakerDurability && x.detected
            }));
        }

        // 6. multi-store as single-store scaling
        {
            let f = detect_false_narratives(&NarrativeContext {
                subject: Some(base("ms", "L4")),
                multi_store: true,
                claimed_verdict: Some("io_bandwidth_bound".into()),
                ..Default::default()
            });
            assert!(f.iter().any(|x| {
                x.narrative == FalseNarrative::MultiStoreAsSingleStoreScaling && x.detected
            }));
        }

        // 7. queue starvation called disk saturation
        {
            let mut s = base("q", "L2");
            s.device_util = Some(0.1);
            s.outstanding_depth = 1;
            let f = detect_false_narratives(&NarrativeContext {
                subject: Some(s),
                claimed_verdict: Some("io_bandwidth_bound".into()),
                ..Default::default()
            });
            assert!(f.iter().any(|x| {
                x.narrative == FalseNarrative::QueueStarvationCalledDiskSaturation && x.detected
            }));
        }

        // 8. lifecycle spikes hidden by means
        {
            let mut s = base("lc", "L6");
            s.p50_latency_ns = Some(1_000);
            s.p99_latency_ns = Some(50_000);
            let f = detect_false_narratives(&NarrativeContext {
                subject: Some(s),
                lifecycle_spikes_present: true,
                latency_mean_ns: Some(1_000.0),
                latency_p99_ns: Some(50_000.0),
                ..Default::default()
            });
            assert!(f.iter().any(|x| {
                x.narrative == FalseNarrative::LifecycleSpikesHiddenByMeans && x.detected
            }));
        }

        // 9. observer overhead as DB cost
        {
            let mut s = base("obs", "L4");
            s.observer_overhead_fraction = Some(0.08);
            let f = detect_false_narratives(&NarrativeContext {
                subject: Some(s),
                claimed_verdict: Some("cpu_transform_bound".into()),
                observer_budget: Some(0.02),
                ..Default::default()
            });
            assert!(f.iter().any(|x| {
                x.narrative == FalseNarrative::ObserverOverheadAsDbCost && x.detected
            }));
        }

        // 10. two changed variables as causal
        {
            let a = base("a", "L4");
            let mut b = base("b", "L4");
            b.concurrency = 16;
            b.outstanding = 64;
            let f = detect_false_narratives(&NarrativeContext {
                subject: Some(a),
                matched: Some(b),
                ..Default::default()
            });
            assert!(f.iter().any(|x| {
                x.narrative == FalseNarrative::TwoChangedVariablesAsCausal && x.detected
            }));
        }
    }

    #[test]
    fn analyze_helper_mixed_strips_tuning() {
        let s = base("s", "L4");
        let input = AttributionInput {
            subject: s,
            matched: None,
            throughput_samples: vec![1.0],
            t_l0: None,
            t_l2: None,
            t_l3: None,
            e2e_latency_ns: Some(1000.0),
            stage_sum_ns: Some(100.0),
            single_core_saturated: false,
            multi_store: false,
            page_cache_warm_only: false,
            lifecycle_spikes_present: false,
            telemetry_delta_exceeds_budget: false,
            memory_faults_align: false,
            lock_residence_dominates: false,
            index_stage_explains_loss: false,
        };
        let tuning = vec![compare_tuning_candidate(
            "batch_size",
            &[1.0, 1.0, 1.0],
            &[2.0, 2.0, 2.0],
            true,
            true,
        )];
        let report = analyze_attribution(&input, &NarrativeContext::default(), tuning);
        assert_eq!(report.attribution.verdict, "mixed_or_unknown");
        assert!(!report.attribution.allows_tuning_advice);
        assert!(report.tuning_candidates.is_empty());
        assert!(coverage_complete(&report.false_narratives));
        let md = render_markdown(&report);
        assert!(md.contains("No tuning advice"));
    }
}
