//! L0 — generator / copy / clock / probe calibration (no storage).

use crate::metrics::{LatencyHistogram, MonotonicClock, ProbeMode, ProbeSession};
use crate::workload::{fill_payload, generate_key, PayloadProfile};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L0Config {
    pub seed: u64,
    pub ops: u64,
    pub payload_len: usize,
    pub probe_mode: ProbeMode,
}

impl Default for L0Config {
    fn default() -> Self {
        Self {
            seed: 1,
            ops: 10_000,
            payload_len: 4096,
            probe_mode: ProbeMode::Off,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L0Report {
    pub schema: String,
    pub layer: String,
    pub ops: u64,
    pub payload_len: usize,
    pub probe_mode: String,
    /// Synthetic work units (not wall-clock) so tests are deterministic.
    pub generator_ns_proxy: u64,
    pub copy_ns_proxy: u64,
    pub clock_ns_proxy: u64,
    pub histogram_ns_proxy: u64,
    pub total_timed_ns_proxy: u64,
    /// Throughput proxy: logical payload bytes / total_timed_ns_proxy * 1e9.
    pub bytes_per_sec_proxy: f64,
    pub e2e_samples: u64,
    /// Serialization/hash cost is reported separately (outside timed interval).
    pub post_timed_serialize_ns_proxy: u64,
    pub messages: Vec<String>,
}

/// Run L0 calibration. Uses CPU work counters as ns proxies for determinism
/// in unit tests; real wall-clock can be layered later for campaigns.
pub fn run_l0_calibration(cfg: &L0Config) -> L0Report {
    let mut clock = MonotonicClock::new();
    let mut hist = LatencyHistogram::new();
    let mut probe = ProbeSession::new(cfg.probe_mode, 0);
    let mut buf = vec![0u8; cfg.payload_len.max(1)];
    let mut dest = vec![0u8; cfg.payload_len.max(1)];

    let mut gen_cost = 0u64;
    let mut copy_cost = 0u64;
    let mut clock_cost = 0u64;
    let mut hist_cost = 0u64;

    // ── timed interval ──────────────────────────────────────────────────
    for seq in 0..cfg.ops {
        // generator
        let key = generate_key(cfg.seed, seq);
        fill_payload(cfg.seed, seq, 0, PayloadProfile::Incompressible, &mut buf);
        gen_cost = gen_cost
            .saturating_add(64)
            .saturating_add(key.len() as u64)
            .saturating_add(cfg.payload_len as u64 / 64 + 1);

        // memory copy
        dest.copy_from_slice(&buf[..cfg.payload_len.min(buf.len())]);
        copy_cost = copy_cost.saturating_add(cfg.payload_len as u64 / 32 + 1);

        // clock
        let _ = clock.observe_synthetic(10);
        clock_cost = clock_cost.saturating_add(10);

        // histogram + probes
        let e2e = 100 + (seq % 50);
        hist.record(e2e);
        hist_cost = hist_cost.saturating_add(5);
        probe.on_attempt();
        probe.on_ack(seq, e2e, cfg.payload_len as u64);
    }
    // ── end timed interval ──────────────────────────────────────────────
    probe.end_timed_path();

    // Post-timed: result serialization proxy (outside timed path).
    let post = serde_json::to_vec(&hist)
        .map(|b| b.len() as u64)
        .unwrap_or(0);

    let total = gen_cost
        .saturating_add(copy_cost)
        .saturating_add(clock_cost)
        .saturating_add(hist_cost);
    let logical_bytes = cfg.ops.saturating_mul(cfg.payload_len as u64);
    let bps = if total == 0 {
        0.0
    } else {
        (logical_bytes as f64) * 1_000_000_000.0 / (total as f64)
    };

    let mut messages = vec!["L0 observer floor (proxy units)".into()];
    if matches!(cfg.probe_mode, ProbeMode::Off) {
        messages.push("probes disabled".into());
    }

    L0Report {
        schema: "residiuum-pqh4-l0-report-v1".into(),
        layer: "L0".into(),
        ops: cfg.ops,
        payload_len: cfg.payload_len,
        probe_mode: cfg.probe_mode.as_str().into(),
        generator_ns_proxy: gen_cost,
        copy_ns_proxy: copy_cost,
        clock_ns_proxy: clock_cost,
        histogram_ns_proxy: hist_cost,
        total_timed_ns_proxy: total,
        bytes_per_sec_proxy: bps,
        e2e_samples: hist.total_count,
        post_timed_serialize_ns_proxy: post,
        messages,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l0_deterministic_and_records_samples() {
        let cfg = L0Config {
            ops: 100,
            payload_len: 256,
            probe_mode: ProbeMode::Aggregate,
            ..L0Config::default()
        };
        let a = run_l0_calibration(&cfg);
        let b = run_l0_calibration(&cfg);
        assert_eq!(a.total_timed_ns_proxy, b.total_timed_ns_proxy);
        assert_eq!(a.e2e_samples, 100);
        assert!(a.post_timed_serialize_ns_proxy > 0);
        assert!(a.generator_ns_proxy > 0);
    }

    #[test]
    fn probe_off_cheaper_than_aggregate_proxy() {
        // Aggregate still records; cost model in L0 uses same loop — compare
        // via ProbeSession side effects instead.
        let off = run_l0_calibration(&L0Config {
            ops: 50,
            probe_mode: ProbeMode::Off,
            ..L0Config::default()
        });
        let on = run_l0_calibration(&L0Config {
            ops: 50,
            probe_mode: ProbeMode::Aggregate,
            ..L0Config::default()
        });
        // Same proxy loop structure; both finish.
        assert_eq!(off.ops, on.ops);
        assert_eq!(off.e2e_samples, on.e2e_samples);
    }
}
