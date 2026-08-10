//! L1 campaign report: curve across block size × queue depth.

use super::io::IoAdapter;
use super::io::{FakeIoAdapter, FakeIoConfig, IoMode, SyncMode};
use super::l0::{run_l0_calibration, L0Config, L0Report};
use super::l1::{run_l1_with_fake, ColdState, L1Config, L1Point};
use super::EnvelopeError;
use crate::metrics::ProbeMode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L1Report {
    pub schema: String,
    pub layer: String,
    pub adapter: String,
    pub l0: L0Report,
    pub points: Vec<L1Point>,
    pub block_sizes: Vec<usize>,
    pub outstanding_depths: Vec<u32>,
    pub no_raw_device: bool,
    pub messages: Vec<String>,
}

/// Build an L1 envelope map using the fake adapter (CI-safe).
pub fn run_l1_matrix_fake(
    block_sizes: &[usize],
    outstanding_depths: &[u32],
    cfg_base: FakeIoConfig,
) -> Result<L1Report, EnvelopeError> {
    let l0 = run_l0_calibration(&L0Config {
        ops: 1_000,
        payload_len: 1024,
        probe_mode: ProbeMode::Off,
        seed: 42,
    });

    let mut adapter = FakeIoAdapter::new(cfg_base);
    let mut points = Vec::new();
    for &bs in block_sizes {
        for &qd in outstanding_depths {
            let cfg = L1Config {
                block_size: bs,
                blocks: 24,
                outstanding_depth: qd,
                workers: 1,
                io_mode: IoMode::Buffered,
                sync_mode: SyncMode::None,
                sync_every_blocks: 0,
                cold_state: ColdState::Warm,
                positioned: false,
                file_id: format!("l1-{bs}-{qd}"),
            };
            let p = run_l1_with_fake(&mut adapter, &cfg)?;
            let _ = adapter.remove_file(&cfg.file_id);
            points.push(p);
        }
    }

    Ok(L1Report {
        schema: "residiuum-pqh4-l1-report-v1".into(),
        layer: "L1".into(),
        adapter: "fake".into(),
        l0,
        points,
        block_sizes: block_sizes.to_vec(),
        outstanding_depths: outstanding_depths.to_vec(),
        no_raw_device: true,
        messages: vec![
            "L1 device envelope (fake adapter); page cache/FS disclosed as part of path".into(),
            "direct I/O and cold state unsupported claims are honest".into(),
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_covers_curve() {
        let report = run_l1_matrix_fake(&[4096, 16384], &[1, 4], FakeIoConfig::default()).unwrap();
        assert_eq!(report.points.len(), 4);
        assert!(report.no_raw_device);
        assert_eq!(report.l0.layer, "L0");
        for p in &report.points {
            assert!(p.bytes_completed > 0);
            assert!(p.throughput_bytes_per_sec > 0.0);
        }
    }
}
