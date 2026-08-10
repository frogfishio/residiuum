//! L1 — filesystem/device envelope over disposable ordinary files.

use super::io::{IoAdapter, IoMode, SyncMode};
use super::window::{WindowClass, WindowDetector, WindowSample};
use super::EnvelopeError;
use crate::metrics::LatencyHistogram;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdState {
    /// Warm page cache / ordinary buffered path — always honest.
    Warm,
    /// Cold only claimed when the platform can establish it; portable default is unsupported.
    Cold,
    /// Explicitly not claimed.
    NotClaimed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L1Config {
    pub block_size: usize,
    pub blocks: u64,
    pub workers: u32,
    pub outstanding_depth: u32,
    pub io_mode: IoMode,
    pub sync_mode: SyncMode,
    pub sync_every_blocks: u64,
    pub cold_state: ColdState,
    pub positioned: bool,
    pub file_id: String,
}

impl Default for L1Config {
    fn default() -> Self {
        Self {
            block_size: 4096,
            blocks: 64,
            workers: 1,
            outstanding_depth: 1,
            io_mode: IoMode::Buffered,
            sync_mode: SyncMode::None,
            sync_every_blocks: 0,
            cold_state: ColdState::Warm,
            positioned: false,
            file_id: "l1-data".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L1Point {
    pub block_size: usize,
    pub outstanding_depth: u32,
    pub workers: u32,
    pub io_mode: String,
    pub sync_mode: String,
    pub bytes_completed: u64,
    pub ops: u64,
    pub total_latency_ns: u64,
    pub throughput_bytes_per_sec: f64,
    pub latency_p50_ns: Option<u64>,
    pub latency_p99_ns: Option<u64>,
    pub cold_state: String,
    pub cold_claim_honest: bool,
    pub direct_supported: bool,
    pub window: String,
    pub errors: Vec<String>,
}

/// Classify cold-state honesty: Cold is only honest when adapter/platform says so.
pub fn cold_claim_honest(state: ColdState, platform_can_establish_cold: bool) -> bool {
    match state {
        ColdState::Warm | ColdState::NotClaimed => true,
        ColdState::Cold => platform_can_establish_cold,
    }
}

/// Run one L1 cell against the given adapter.
pub fn run_l1_envelope<A: IoAdapter>(
    adapter: &mut A,
    cfg: &L1Config,
    platform_can_establish_cold: bool,
) -> Result<L1Point, EnvelopeError> {
    let mut errors = Vec::new();
    let honest = cold_claim_honest(cfg.cold_state, platform_can_establish_cold);
    if !honest {
        errors.push("cold state claimed but platform cannot establish cold honestly".into());
    }

    if matches!(cfg.io_mode, IoMode::Direct) && !adapter.supports_direct() {
        return Ok(L1Point {
            block_size: cfg.block_size,
            outstanding_depth: cfg.outstanding_depth,
            workers: cfg.workers,
            io_mode: "direct".into(),
            sync_mode: format!("{:?}", cfg.sync_mode).to_ascii_lowercase(),
            bytes_completed: 0,
            ops: 0,
            total_latency_ns: 0,
            throughput_bytes_per_sec: 0.0,
            latency_p50_ns: None,
            latency_p99_ns: None,
            cold_state: format!("{:?}", cfg.cold_state).to_ascii_lowercase(),
            cold_claim_honest: honest,
            direct_supported: false,
            window: format!("{:?}", WindowClass::Finite).to_ascii_lowercase(),
            errors: vec!["direct I/O unsupported on adapter (honest)".into()],
        });
    }

    if cfg.positioned && !adapter.supports_positioned() {
        return Err(EnvelopeError::Msg(
            "positioned I/O requested but adapter does not support it".into(),
        ));
    }

    // Outstanding depth is advisory for real multi-queue; fake uses it for latency.
    // For FakeIoAdapter we set via downcast-like pattern: optional hook.
    set_outstanding_if_fake(adapter, cfg.outstanding_depth);

    adapter.create_file(&cfg.file_id)?;

    let mut hist = LatencyHistogram::new();
    let mut bytes = 0u64;
    let mut ops = 0u64;
    let mut total_lat = 0u64;
    let mut window_samples = Vec::new();
    let chunk = (cfg.blocks / 5).max(1);

    for i in 0..cfg.blocks {
        let offset = if cfg.positioned {
            Some(i.saturating_mul(cfg.block_size as u64))
        } else {
            None
        };
        match adapter.write_block(&cfg.file_id, offset, cfg.block_size, cfg.io_mode) {
            Ok(r) => {
                bytes = bytes.saturating_add(r.bytes_completed);
                ops = ops.saturating_add(r.ops);
                total_lat = total_lat.saturating_add(r.latency_ns);
                hist.record(r.latency_ns.max(1));
            }
            Err(e) => {
                errors.push(format!("write: {e}"));
                // continue collecting; partial cell
            }
        }

        if cfg.sync_every_blocks > 0 && (i + 1) % cfg.sync_every_blocks == 0 {
            match adapter.sync(&cfg.file_id, cfg.sync_mode) {
                Ok(r) => {
                    total_lat = total_lat.saturating_add(r.latency_ns);
                    ops = ops.saturating_add(r.ops);
                }
                Err(e) => errors.push(format!("sync: {e}")),
            }
        }

        if (i + 1) % chunk == 0 && total_lat > 0 {
            let bps = (bytes as f64) * 1_000_000_000.0 / (total_lat as f64);
            window_samples.push(WindowSample {
                throughput_bytes_per_sec: bps,
            });
        }
    }

    // Optional sequential read pass (small).
    if bytes > 0 {
        let _ = adapter.read_block(
            &cfg.file_id,
            0,
            cfg.block_size.min(bytes as usize),
            cfg.io_mode,
        );
    }

    let tput = if total_lat == 0 {
        0.0
    } else {
        (bytes as f64) * 1_000_000_000.0 / (total_lat as f64)
    };
    let window = WindowDetector::default().classify(&window_samples);

    let p = hist.percentiles();
    Ok(L1Point {
        block_size: cfg.block_size,
        outstanding_depth: cfg.outstanding_depth,
        workers: cfg.workers,
        io_mode: format!("{:?}", cfg.io_mode).to_ascii_lowercase(),
        sync_mode: format!("{:?}", cfg.sync_mode).to_ascii_lowercase(),
        bytes_completed: bytes,
        ops,
        total_latency_ns: total_lat,
        throughput_bytes_per_sec: tput,
        latency_p50_ns: p.p50_ns,
        latency_p99_ns: p.p99_ns,
        cold_state: format!("{:?}", cfg.cold_state).to_ascii_lowercase(),
        cold_claim_honest: honest,
        direct_supported: adapter.supports_direct(),
        window: format!("{window:?}").to_ascii_lowercase(),
        errors,
    })
}

fn set_outstanding_if_fake<A: IoAdapter>(_adapter: &mut A, _depth: u32) {
    // Outstanding depth is applied via `run_l1_with_fake` for FakeIoAdapter.
    // Generic adapters treat it as a reported control dimension only.
}

/// Run L1 with explicit outstanding depth on a fake adapter.
pub fn run_l1_with_fake(
    adapter: &mut super::io::FakeIoAdapter,
    cfg: &L1Config,
) -> Result<L1Point, EnvelopeError> {
    adapter.outstanding_depth = cfg.outstanding_depth.max(1);
    run_l1_envelope(adapter, cfg, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::io::{FakeIoAdapter, FakeIoConfig};

    #[test]
    fn l1_maps_block_size_and_qd() {
        let mut adapter = FakeIoAdapter::new(FakeIoConfig {
            bandwidth_bytes_per_sec: 50 * 1024 * 1024,
            base_latency_ns: 500,
            qd_latency_ns: 200,
            ..FakeIoConfig::default()
        });
        let mut points = Vec::new();
        for &bs in &[4096usize, 65536] {
            for &qd in &[1u32, 8] {
                let cfg = L1Config {
                    block_size: bs,
                    blocks: 32,
                    outstanding_depth: qd,
                    ..L1Config::default()
                };
                let p = run_l1_with_fake(&mut adapter, &cfg).unwrap();
                assert!(p.bytes_completed > 0, "bs={bs} qd={qd}");
                points.push(p);
            }
        }
        assert_eq!(points.len(), 4);
        // Higher QD should show higher latency proxy at same block size.
        assert!(points[1].total_latency_ns >= points[0].total_latency_ns);
    }

    #[test]
    fn direct_unsupported_honest() {
        let mut adapter = FakeIoAdapter::new(FakeIoConfig::default());
        let cfg = L1Config {
            io_mode: IoMode::Direct,
            blocks: 4,
            ..L1Config::default()
        };
        let p = run_l1_with_fake(&mut adapter, &cfg).unwrap();
        assert!(!p.direct_supported);
        assert!(p.errors.iter().any(|e| e.contains("unsupported")));
        assert_eq!(p.bytes_completed, 0);
    }

    #[test]
    fn cold_claim_dishonest_flagged() {
        assert!(!cold_claim_honest(ColdState::Cold, false));
        assert!(cold_claim_honest(ColdState::Warm, false));
        let mut adapter = FakeIoAdapter::new(FakeIoConfig::default());
        let cfg = L1Config {
            cold_state: ColdState::Cold,
            blocks: 8,
            ..L1Config::default()
        };
        let p = run_l1_envelope(&mut adapter, &cfg, false).unwrap();
        assert!(!p.cold_claim_honest);
    }
}
