//! Result kernel: metrics map, stage residual accounting, validity.

use super::aggregate::ThreadAggregate;
use super::histogram::Percentiles;
use super::probe::{InstrumentationBudget, ProbeMode};
use crate::runner::MetricObservation;
use crate::STAGES;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Named metric observations for a finished run (post-timing).
pub type MetricMap = BTreeMap<String, MetricObservation>;

/// Residual accounting: e2e vs sum of stage medians (SPEC residual ε).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageResiduals {
    pub e2e_median_ns: Option<u64>,
    pub stages_sum_median_ns: Option<u64>,
    pub residual_ns: Option<i64>,
    /// residual / e2e; if > 0.05 attribution is incomplete.
    pub residual_fraction: Option<f64>,
    pub attribution_complete: bool,
}

impl StageResiduals {
    pub fn from_aggregate(agg: &ThreadAggregate) -> Self {
        let e2e = agg.e2e_latency.percentile_ns(50.0);
        let mut sum = 0u64;
        let mut any = false;
        for h in &agg.stage_latency {
            if let Some(p) = h.percentile_ns(50.0) {
                sum = sum.saturating_add(p);
                any = true;
            }
        }
        let stages_sum = if any { Some(sum) } else { None };
        let residual_ns = match (e2e, stages_sum) {
            (Some(e), Some(s)) => Some(e as i64 - s as i64),
            _ => None,
        };
        let residual_fraction = match (e2e, residual_ns) {
            (Some(e), Some(r)) if e > 0 => Some((r as f64).abs() / e as f64),
            _ => None,
        };
        let attribution_complete = residual_fraction.map(|f| f <= 0.05).unwrap_or(false);
        Self {
            e2e_median_ns: e2e,
            stages_sum_median_ns: stages_sum,
            residual_ns,
            residual_fraction,
            attribution_complete,
        }
    }
}

/// Assembled result kernel after timing ends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultKernel {
    pub schema: String,
    pub profile: String,
    pub run_id: String,
    pub validity: String,
    pub probe_mode: String,
    pub metrics: MetricMap,
    pub e2e_percentiles: Percentiles,
    pub residuals: StageResiduals,
    pub counters_saturated: bool,
    pub histogram_saturated: bool,
    pub messages: Vec<String>,
}

impl ResultKernel {
    pub fn from_aggregate(
        run_id: impl Into<String>,
        agg: &ThreadAggregate,
        mode: ProbeMode,
        budget: &InstrumentationBudget,
        measured_loss: Option<f64>,
    ) -> Self {
        let mut messages = Vec::new();
        let mut validity = "valid".to_string();

        if let Some(loss) = measured_loss {
            if let Some(v) = budget.validity_if_exceeded(mode, loss) {
                validity = v.into();
                messages.push(format!(
                    "instrumentation loss {loss:.4} exceeds budget for mode {}",
                    mode.as_str()
                ));
            }
        }

        if agg.counters.is_saturated() || agg.e2e_latency.saturated {
            messages.push("counter or histogram saturation".into());
        }

        let residuals = StageResiduals::from_aggregate(agg);
        if !residuals.attribution_complete && mode != ProbeMode::Off {
            messages.push("stage residual >5% or incomplete stage samples".into());
        }

        let mut metrics = MetricMap::new();
        for (name, val) in agg.counters.as_map() {
            let unit = unit_for_counter(&name);
            metrics.insert(name, MetricObservation::available(val as f64, unit));
        }
        let p = agg.e2e_latency.percentiles();
        insert_latency(&mut metrics, "latency_e2e_p50_ns", p.p50_ns);
        insert_latency(&mut metrics, "latency_e2e_p90_ns", p.p90_ns);
        insert_latency(&mut metrics, "latency_e2e_p95_ns", p.p95_ns);
        insert_latency(&mut metrics, "latency_e2e_p99_ns", p.p99_ns);
        insert_latency(&mut metrics, "latency_e2e_p999_ns", p.p999_ns);
        insert_latency(&mut metrics, "latency_e2e_max_ns", p.max_ns);

        // Stage residual as explicit metric when available.
        if let Some(r) = residuals.residual_fraction {
            metrics.insert(
                "stage_residual_fraction".into(),
                MetricObservation::available(r, "fraction"),
            );
        } else {
            metrics.insert(
                "stage_residual_fraction".into(),
                MetricObservation::unavailable("not_sampled"),
            );
        }

        // Ensure STAGES are referenced so residual accounting stays aligned.
        let _ = STAGES;

        Self {
            schema: "residiuum-performance-result-v1".into(),
            profile: crate::PROFILE_ID.into(),
            run_id: run_id.into(),
            validity,
            probe_mode: mode.as_str().into(),
            metrics,
            e2e_percentiles: p,
            residuals,
            counters_saturated: agg.counters.is_saturated(),
            histogram_saturated: agg.e2e_latency.saturated,
            messages,
        }
    }
}

fn unit_for_counter(name: &str) -> &'static str {
    if name.contains("bytes") {
        "bytes"
    } else {
        "count"
    }
}

fn insert_latency(map: &mut MetricMap, name: &str, v: Option<u64>) {
    match v {
        Some(n) => {
            map.insert(name.into(), MetricObservation::available(n as f64, "ns"));
        }
        None => {
            map.insert(name.into(), MetricObservation::unavailable("not_sampled"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::counters::CounterId;

    #[test]
    fn residual_complete_when_stages_match_e2e() {
        let mut agg = ThreadAggregate::new(0);
        // e2e 1000; one stage 1000 → residual 0
        for _ in 0..100 {
            agg.record_e2e(1000);
            agg.record_stage(0, 1000);
        }
        let r = StageResiduals::from_aggregate(&agg);
        assert!(r.attribution_complete);
    }

    #[test]
    fn invalid_instrumentation_on_budget() {
        let mut agg = ThreadAggregate::new(0);
        agg.counters.inc(CounterId::OpsAcknowledged);
        agg.record_e2e(100);
        let k = ResultKernel::from_aggregate(
            "r1",
            &agg,
            ProbeMode::Aggregate,
            &InstrumentationBudget::default(),
            Some(0.05),
        );
        assert_eq!(k.validity, "invalid_instrumentation");
    }

    #[test]
    fn zero_counter_is_available() {
        let agg = ThreadAggregate::new(0);
        let k = ResultKernel::from_aggregate(
            "r0",
            &agg,
            ProbeMode::Aggregate,
            &InstrumentationBudget::default(),
            None,
        );
        match k.metrics.get("ops_attempted") {
            Some(MetricObservation::Available { value, .. }) => assert_eq!(*value, 0.0),
            other => panic!("expected available zero, got {other:?}"),
        }
    }
}
