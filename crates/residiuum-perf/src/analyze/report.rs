//! Human Markdown + canonical JSON attribution reports.

use serde::{Deserialize, Serialize};

use super::classify::AttributionResult;
use super::falsify::{
    falsification_experiments, FalsificationExperiment, TuningCandidateComparison,
};
use super::narratives::NarrativeFinding;

pub const REPORT_PROFILE: &str = "residiuum-performance-attribution-v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AttributionReport {
    pub profile: String,
    pub attribution: AttributionResult,
    pub false_narratives: Vec<NarrativeFinding>,
    pub falsification_experiments: Vec<FalsificationExperiment>,
    pub tuning_candidates: Vec<TuningCandidateComparison>,
}

impl AttributionReport {
    pub fn build(
        attribution: AttributionResult,
        false_narratives: Vec<NarrativeFinding>,
        tuning_candidates: Vec<TuningCandidateComparison>,
    ) -> Self {
        let falsification_experiments = falsification_experiments(&attribution);
        Self {
            profile: REPORT_PROFILE.into(),
            attribution,
            false_narratives,
            falsification_experiments,
            tuning_candidates,
        }
    }

    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// Render a human-readable Markdown report. No tuning advice when mixed_or_unknown.
pub fn render_markdown(report: &AttributionReport) -> String {
    let a = &report.attribution;
    let mut md = String::new();
    md.push_str("# Attribution report\n\n");
    md.push_str(&format!("Profile: `{}`\n\n", report.profile));
    md.push_str(&format!(
        "## Verdict\n\n**{}** ({})\n\n",
        a.verdict, a.confidence
    ));
    md.push_str(&format!("- Run IDs: {}\n", a.run_ids.join(", ")));
    if let Some(e) = a.effect_size {
        md.push_str(&format!("- Effect size (relative throughput): {e:.4}\n"));
    }
    if let Some(r) = a.residual_fraction {
        md.push_str(&format!(
            "- Residual fraction: {r:.4}{}\n",
            if a.residual_incomplete {
                " (**incomplete** — stage primary blocked)"
            } else {
                ""
            }
        ));
    }
    if let Some(ret) = a.retention {
        md.push_str(&format!("- Retention: {ret:.4}\n"));
    }
    if let Some(eff) = a.efficiency {
        md.push_str(&format!("- Efficiency E=T_L6/T_L2: {eff:.4}\n"));
    }
    if let Some(st) = &a.throughput_stats {
        md.push_str(&format!(
            "- Throughput samples: n={} mean={:.3} median={:.3} mad={:.3}\n",
            st.n, st.mean, st.median, st.mad
        ));
    }
    if let Some(ci) = &a.throughput_ci {
        md.push_str(&format!(
            "- Bootstrap CI ({:.0}%, {} reps): [{:.3}, {:.3}]\n",
            ci.level * 100.0,
            ci.replicates,
            ci.low,
            ci.high
        ));
    }
    md.push('\n');

    md.push_str("## Supporting evidence\n\n");
    if a.supporting_evidence.is_empty() {
        md.push_str("- (none)\n");
    } else {
        for e in &a.supporting_evidence {
            md.push_str(&format!("- {e}\n"));
        }
    }
    md.push('\n');

    md.push_str("## Contradicted alternatives\n\n");
    if a.contradicted_alternatives.is_empty() {
        md.push_str("- (none recorded)\n");
    } else {
        for e in &a.contradicted_alternatives {
            md.push_str(&format!("- `{e}`\n"));
        }
    }
    md.push('\n');

    md.push_str("## False-narrative scan\n\n");
    let detected: Vec<_> = report
        .false_narratives
        .iter()
        .filter(|f| f.detected)
        .collect();
    if detected.is_empty() {
        md.push_str("No plan-required false narratives detected.\n\n");
    } else {
        for f in detected {
            md.push_str(&format!("- **{}**: {}\n", f.narrative_id, f.detail));
        }
        md.push('\n');
    }

    md.push_str("## Falsification experiments\n\n");
    for e in &report.falsification_experiments {
        md.push_str(&format!("### {}. {} (`{}`)\n\n", e.rank, e.title, e.id));
        md.push_str(&format!(
            "- Change one variable: `{}`\n",
            e.changes_one_variable
        ));
        md.push_str(&format!(
            "- If verdict true: {}\n",
            e.expected_if_verdict_true
        ));
        md.push_str(&format!(
            "- If verdict false: {}\n\n",
            e.expected_if_verdict_false
        ));
    }

    md.push_str("## Tuning candidates\n\n");
    if a.verdict == "mixed_or_unknown" || !a.allows_tuning_advice {
        md.push_str(
            "**No tuning advice** — verdict is `mixed_or_unknown` or evidence is insufficient.\n\n",
        );
    } else if report.tuning_candidates.is_empty() {
        md.push_str("No candidate comparisons supplied.\n\n");
    } else {
        for t in &report.tuning_candidates {
            md.push_str(&format!(
                "- `{}`: Δ={:.3} accepted={} — {}\n",
                t.parameter, t.delta, t.accepted_for_recommendation, t.reason
            ));
        }
        md.push('\n');
    }

    if !a.notes.is_empty() {
        md.push_str("## Notes\n\n");
        for n in &a.notes {
            md.push_str(&format!("- {n}\n"));
        }
        md.push('\n');
    }

    md
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyze::classify::{classify_bottleneck, AttributionInput};
    use crate::analyze::match_run::RunEvidence;
    use crate::analyze::narratives::{detect_false_narratives, NarrativeContext};

    fn base(id: &str) -> RunEvidence {
        RunEvidence {
            run_id: id.into(),
            layer: "L2".into(),
            durability: "durable".into(),
            payload_size: 4096,
            concurrency: 1,
            outstanding: 8,
            config_hash: "abc".into(),
            throughput_bytes_per_sec: 1e8,
            p50_latency_ns: Some(1000),
            p99_latency_ns: Some(5000),
            residual_fraction: Some(0.02),
            validity: "valid".into(),
            failed_ops: 0,
            attempted_ops: 100,
            acknowledged_ops: 100,
            device_util: Some(0.92),
            outstanding_depth: 16,
            sync_per_op: false,
            observer_overhead_fraction: Some(0.001),
            cpu_cores_busy: Some(0.4),
            aggregate_cpu_idle: Some(0.5),
            window_class: "sustained".into(),
            features: "l2".into(),
        }
    }

    #[test]
    fn report_roundtrip_json_and_markdown() {
        let s = base("s1");
        let attr = classify_bottleneck(&AttributionInput {
            subject: s.clone(),
            matched: None,
            throughput_samples: vec![1e8, 1.01e8, 0.99e8, 1e8, 1.02e8],
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
        });
        let narratives = detect_false_narratives(&NarrativeContext {
            subject: Some(s),
            claimed_verdict: Some(attr.verdict.clone()),
            ..Default::default()
        });
        let report = AttributionReport::build(attr, narratives, vec![]);
        let json = report.to_json_pretty().unwrap();
        assert!(json.contains(REPORT_PROFILE));
        assert!(json.contains("verdict"));
        let md = render_markdown(&report);
        assert!(md.contains("# Attribution report"));
        assert!(md.contains("## Verdict"));
        if report.attribution.verdict == "mixed_or_unknown" {
            assert!(md.contains("No tuning advice") || md.contains("mixed_or_unknown"));
        }
        // re-parse JSON
        let back: AttributionReport = serde_json::from_str(&json).unwrap();
        assert_eq!(back.profile, REPORT_PROFILE);
    }

    #[test]
    fn mixed_markdown_forbids_tuning() {
        let s = base("s");
        let attr = classify_bottleneck(&AttributionInput {
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
        });
        assert_eq!(attr.verdict, "mixed_or_unknown");
        let report = AttributionReport::build(attr, vec![], vec![]);
        let md = render_markdown(&report);
        assert!(md.contains("No tuning advice"));
    }
}
