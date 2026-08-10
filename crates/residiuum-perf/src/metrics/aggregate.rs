//! Bounded per-thread aggregation; merge after the timed interval.

use super::counters::{CounterId, CounterSet};
use super::histogram::LatencyHistogram;
use crate::STAGES;
use serde::{Deserialize, Serialize};

/// Per-thread measurement state — fixed size, no per-op heap maps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadAggregate {
    pub thread_id: u32,
    pub counters: CounterSet,
    pub e2e_latency: LatencyHistogram,
    /// Per-stage latency histograms aligned with [`crate::STAGES`].
    pub stage_latency: Vec<LatencyHistogram>,
    /// Wall ns spent inside probe record paths (for overhead accounting).
    pub probe_overhead_ns: u64,
}

impl ThreadAggregate {
    pub fn new(thread_id: u32) -> Self {
        Self {
            thread_id,
            counters: CounterSet::new(),
            e2e_latency: LatencyHistogram::new(),
            stage_latency: STAGES.iter().map(|_| LatencyHistogram::new()).collect(),
            probe_overhead_ns: 0,
        }
    }

    pub fn record_e2e(&mut self, ns: u64) {
        self.e2e_latency.record(ns);
    }

    pub fn record_stage(&mut self, stage_idx: usize, ns: u64) {
        if let Some(h) = self.stage_latency.get_mut(stage_idx) {
            h.record(ns);
        }
    }

    pub fn stage_index(name: &str) -> Option<usize> {
        STAGES.iter().position(|s| *s == name)
    }

    pub fn merge_from(&mut self, other: &Self) {
        self.counters.merge_from(&other.counters);
        self.e2e_latency.merge_from(&other.e2e_latency);
        let n = self.stage_latency.len().min(other.stage_latency.len());
        for i in 0..n {
            self.stage_latency[i].merge_from(&other.stage_latency[i]);
        }
        self.probe_overhead_ns = self
            .probe_overhead_ns
            .saturating_add(other.probe_overhead_ns);
    }
}

/// Merge many thread aggregates into one (post-timing only).
pub fn merge_thread_aggregates(parts: &[ThreadAggregate]) -> ThreadAggregate {
    let mut acc = ThreadAggregate::new(0);
    for p in parts {
        acc.merge_from(p);
    }
    acc
}

/// Approximate memory footprint of one aggregate (bounded, for tests).
pub fn approx_bytes(agg: &ThreadAggregate) -> usize {
    std::mem::size_of_val(agg) + agg.stage_latency.len() * std::mem::size_of::<LatencyHistogram>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_no_lost_latency_counts() {
        let mut a = ThreadAggregate::new(0);
        let mut b = ThreadAggregate::new(1);
        for _ in 0..100 {
            a.record_e2e(50);
            b.record_e2e(50);
        }
        a.counters.add(CounterId::OpsAcknowledged, 100);
        b.counters.add(CounterId::OpsAcknowledged, 100);
        let m = merge_thread_aggregates(&[a, b]);
        assert_eq!(m.e2e_latency.total_count, 200);
        assert_eq!(m.counters.get(CounterId::OpsAcknowledged), 200);
    }

    #[test]
    fn bounded_memory_independent_of_op_count() {
        let mut a = ThreadAggregate::new(0);
        let base = approx_bytes(&a);
        for i in 0..10_000 {
            a.record_e2e(i % 1000 + 1);
        }
        // Histogram is fixed-bucket: size class stays the same.
        assert_eq!(approx_bytes(&a), base);
        assert_eq!(a.e2e_latency.total_count, 10_000);
    }

    #[test]
    fn stage_names_align_with_registry() {
        assert_eq!(STAGES.len(), 9);
        assert_eq!(ThreadAggregate::stage_index("residual"), Some(8));
        assert_eq!(ThreadAggregate::stage_index("magic"), None);
    }
}
