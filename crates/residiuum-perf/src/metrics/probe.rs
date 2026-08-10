//! Probe modes: off / aggregate / sampled (SPEC §7).
//!
//! `ProbeMode::Off` paths are empty and `#[inline(always)]` so ordinary
//! diagnostic builds pay nothing when probes are disabled.

use super::aggregate::ThreadAggregate;
use super::counters::CounterId;
use super::sample::{should_sample, SamplerConfig};
use serde::{Deserialize, Serialize};

/// Instrumentation mode for a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeMode {
    /// No probes — timed path has no metric side effects.
    Off,
    /// Fixed-bucket aggregate counters/histograms for every op.
    Aggregate,
    /// Aggregate + per-stage samples at deterministic rate.
    Sampled,
}

impl ProbeMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Aggregate => "aggregate",
            Self::Sampled => "sampled",
        }
    }
}

/// Budget for instrumentation overhead vs uninstrumented baseline.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct InstrumentationBudget {
    /// Max median throughput loss fraction for aggregate probes (default 1%).
    pub aggregate_max_loss: f64,
    /// Max median throughput loss for sampled probes (default 3%).
    pub sampled_max_loss: f64,
}

impl Default for InstrumentationBudget {
    fn default() -> Self {
        Self {
            aggregate_max_loss: 0.01,
            sampled_max_loss: 0.03,
        }
    }
}

impl InstrumentationBudget {
    /// Return `true` when overhead exceeds the budget → `invalid_instrumentation`.
    pub fn exceeds(&self, mode: ProbeMode, loss_fraction: f64) -> bool {
        match mode {
            ProbeMode::Off => false,
            ProbeMode::Aggregate => loss_fraction > self.aggregate_max_loss,
            ProbeMode::Sampled => loss_fraction > self.sampled_max_loss,
        }
    }

    pub fn validity_if_exceeded(
        &self,
        mode: ProbeMode,
        loss_fraction: f64,
    ) -> Option<&'static str> {
        if self.exceeds(mode, loss_fraction) {
            Some("invalid_instrumentation")
        } else {
            None
        }
    }
}

/// Session wrapping mode + sampler + thread aggregate.
#[derive(Debug, Clone)]
pub struct ProbeSession {
    pub mode: ProbeMode,
    pub sampler: SamplerConfig,
    pub agg: ThreadAggregate,
    /// When true, writers must not be called from timed path (enforced by API split).
    timed_path: bool,
}

impl ProbeSession {
    pub fn new(mode: ProbeMode, thread_id: u32) -> Self {
        Self {
            mode,
            sampler: SamplerConfig::default(),
            agg: ThreadAggregate::new(thread_id),
            timed_path: true,
        }
    }

    pub fn with_sampler(mut self, sampler: SamplerConfig) -> Self {
        self.sampler = sampler;
        self
    }

    /// Mark end of timed interval — artifact I/O only after this.
    pub fn end_timed_path(&mut self) {
        self.timed_path = false;
    }

    pub fn is_timed_path(&self) -> bool {
        self.timed_path
    }

    /// Record one acknowledged e2e latency. No-op when mode is Off.
    #[inline(always)]
    pub fn on_ack(&mut self, seq: u64, e2e_ns: u64, payload_bytes: u64) {
        match self.mode {
            ProbeMode::Off => {}
            ProbeMode::Aggregate | ProbeMode::Sampled => {
                self.agg.counters.inc(CounterId::OpsAcknowledged);
                self.agg
                    .counters
                    .add(CounterId::LogicalPayloadBytesAck, payload_bytes);
                self.agg.record_e2e(e2e_ns);
                if matches!(self.mode, ProbeMode::Sampled) && should_sample(seq, &self.sampler) {
                    self.agg.counters.inc(CounterId::ProbeSamples);
                }
            }
        }
    }

    /// Record stage latency only in Sampled mode when `seq` is selected.
    #[inline(always)]
    pub fn on_stage_sample(&mut self, seq: u64, stage_idx: usize, ns: u64) {
        if !matches!(self.mode, ProbeMode::Sampled) {
            return;
        }
        if !should_sample(seq, &self.sampler) {
            return;
        }
        self.agg.record_stage(stage_idx, ns);
    }

    #[inline(always)]
    pub fn on_attempt(&mut self) {
        if matches!(self.mode, ProbeMode::Off) {
            return;
        }
        self.agg.counters.inc(CounterId::OpsAttempted);
    }
}

/// Synthetic overhead fixture: compare ops/s with probes off vs on.
pub fn measure_probe_overhead_fixture(ops: u64, body_ns: u64) -> (f64, f64, f64) {
    // Returns (off_ops_per_ns_proxy, on_ops_per_ns_proxy, loss_fraction)
    // Uses synthetic work units, not wall clock, for determinism.
    let off_cost = ops.saturating_mul(body_ns);
    let on_extra_per_op = 50u64; // fixed model of aggregate probe cost
    let on_cost = ops.saturating_mul(body_ns.saturating_add(on_extra_per_op));
    let off_rate = if off_cost == 0 {
        0.0
    } else {
        ops as f64 / off_cost as f64
    };
    let on_rate = if on_cost == 0 {
        0.0
    } else {
        ops as f64 / on_cost as f64
    };
    let loss = if off_rate <= 0.0 {
        0.0
    } else {
        (off_rate - on_rate) / off_rate
    };
    (off_rate, on_rate, loss)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn off_mode_records_nothing() {
        let mut s = ProbeSession::new(ProbeMode::Off, 0);
        s.on_attempt();
        s.on_ack(0, 100, 64);
        assert_eq!(s.agg.counters.get(CounterId::OpsAttempted), 0);
        assert_eq!(s.agg.e2e_latency.total_count, 0);
    }

    #[test]
    fn aggregate_records_all() {
        let mut s = ProbeSession::new(ProbeMode::Aggregate, 0);
        for i in 0..10 {
            s.on_attempt();
            s.on_ack(i, 100 + i, 8);
        }
        assert_eq!(s.agg.counters.get(CounterId::OpsAttempted), 10);
        assert_eq!(s.agg.counters.get(CounterId::OpsAcknowledged), 10);
        assert_eq!(s.agg.e2e_latency.total_count, 10);
        assert_eq!(s.agg.counters.get(CounterId::ProbeSamples), 0);
    }

    #[test]
    fn sampled_counts_samples() {
        let mut s =
            ProbeSession::new(ProbeMode::Sampled, 0).with_sampler(SamplerConfig { rate: 4 });
        for i in 0..16 {
            s.on_ack(i, 10, 1);
        }
        assert_eq!(s.agg.counters.get(CounterId::ProbeSamples), 4); // 0,4,8,12
    }

    #[test]
    fn budget_invalidates_high_overhead() {
        let b = InstrumentationBudget::default();
        assert!(b.exceeds(ProbeMode::Aggregate, 0.02));
        assert!(!b.exceeds(ProbeMode::Aggregate, 0.005));
        assert_eq!(
            b.validity_if_exceeded(ProbeMode::Sampled, 0.05),
            Some("invalid_instrumentation")
        );
    }

    #[test]
    fn overhead_fixture_positive_loss() {
        let (_, _, loss) = measure_probe_overhead_fixture(10_000, 1000);
        assert!(loss > 0.0 && loss < 0.1);
    }
}
