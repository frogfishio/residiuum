//! Minimal authoritative / L4–L6 path drivers (harness simulation).
//!
//! Models product path stages without linking residiuum-store. Correctness is
//! enforced via AckLedger; a durability-weakening mutant is rejected.

use super::ledger::AckLedger;
use super::profiles::FeatureProfile;
use super::scheduler::MatrixCell;
use super::MatrixError;
use crate::envelope::{FakeIoAdapter, FakeIoConfig, WindowDetector, WindowSample};
use crate::pipeline::{run_l3_pipeline, InjectedDelays, L3Config, PipelineError, StageSet};
use crate::shadow::{execute_shadow, PlanBuilder, ShadowError, ShapeConfig};
use crate::workload::SizeSampler;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayerProfile {
    L4,
    L5,
    L6,
}

impl LayerProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::L4 => "L4",
            Self::L5 => "L5",
            Self::L6 => "L6",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityMode {
    Memory,
    Buffered,
    Durable,
}

impl DurabilityMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Buffered => "buffered",
            Self::Durable => "durable",
        }
    }

    /// Cost proxy for durability barrier (ns work units).
    pub fn barrier_ns(self) -> u64 {
        match self {
            Self::Memory => 0,
            Self::Buffered => 50,
            Self::Durable => 500,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseState {
    Empty,
    SteadyPopulated,
    RewriteHeavy,
    Fragmented,
}

impl DatabaseState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::SteadyPopulated => "steady_populated",
            Self::RewriteHeavy => "rewrite_heavy",
            Self::Fragmented => "fragmented",
        }
    }

    pub fn setup_cost_ns(self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::SteadyPopulated => 100,
            Self::RewriteHeavy => 200,
            Self::Fragmented => 300,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunCellConfig {
    pub cell: MatrixCell,
    pub seed: u64,
    /// If true, weaken durable → memory without relabeling (must be rejected).
    pub durability_mutant: bool,
    /// If true, poison ack digest (must invalidate).
    pub digest_mutant: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellRunReport {
    pub cell_id: String,
    pub layer: String,
    pub durability: String,
    pub validity: String,
    pub attempted: u64,
    pub admitted: u64,
    pub acknowledged: u64,
    pub failed: u64,
    pub logical_bytes_ack: u64,
    pub e2e_ns_proxy: u64,
    pub throughput_bytes_per_sec_proxy: f64,
    pub window: String,
    pub shadow_planned_bytes: u64,
    pub shadow_completed_bytes: u64,
    pub features: String,
    pub interference: String,
    pub messages: Vec<String>,
    pub reopen_ok: bool,
}

/// Execute one matrix cell through L3 CPU + L2 shadow + ack ledger.
pub fn run_cell(cfg: &RunCellConfig) -> Result<CellRunReport, MatrixError> {
    let cell = &cfg.cell;
    let mut messages = Vec::new();

    // Durability mutant: claim durable but run memory barrier.
    let effective_dur = if cfg.durability_mutant {
        if matches!(cell.durability, DurabilityMode::Durable) {
            return Err(MatrixError::DurabilityMutant(
                "durable label with memory barrier is a different product profile".into(),
            ));
        }
        cell.durability
    } else {
        cell.durability
    };

    let sampler = match cell.distribution {
        Some(d) => SizeSampler::distribution(d),
        None => SizeSampler::fixed(cell.payload_size),
    };

    let mut ledger = AckLedger::new();
    let mut records = Vec::new();
    let mut e2e = cell.db_state.setup_cost_ns();
    let mut logical_bytes = 0u64;
    let mut window_samples = Vec::new();

    // L3 stage set scales with features.
    let stages = stage_set_for(&cell.features);
    let l3 = run_l3_pipeline(&L3Config {
        seed: cfg.seed,
        ops: cell.op_count.min(64), // bound for unit speed
        payload_len: cell.payload_size.min(64 * 1024) as usize,
        producers: cell.concurrency.max(1),
        stages,
        delays: InjectedDelays {
            queue_ns: 1,
            lock_ns: if cell.features.features.len() > 2 {
                2
            } else {
                0
            },
            cpu_burn_ns: 0,
            stage_extra: vec![],
        },
        sink_cap: 2048,
    })
    .map_err(|e: PipelineError| MatrixError::Msg(e.to_string()))?;
    e2e = e2e.saturating_add(l3.e2e_ns);
    messages.push(format!(
        "L3 digest {}",
        &l3.output_digest_hex[..16.min(l3.output_digest_hex.len())]
    ));

    // Operation stream with ledger.
    let ops = cell.op_count.min(64);
    for seq in 0..ops {
        ledger.record_attempt();
        let plen = if cell.distribution.is_some() {
            sampler.size_at(seq)
        } else {
            cell.payload_size
        };
        // Fail every 17th op explicitly (stays in denominator).
        if seq > 0 && seq % 17 == 0 {
            ledger.record_fail();
            e2e = e2e.saturating_add(10);
            continue;
        }
        ledger.record_admit();
        if cfg.digest_mutant && seq == 0 {
            ledger.record_ack_mutant_wrong_digest(seq, seq, plen, 0);
        } else {
            ledger.record_ack(seq, seq, plen, 0);
        }
        records.push((seq, seq, plen, 0u32));
        logical_bytes = logical_bytes.saturating_add(plen);
        e2e = e2e
            .saturating_add(effective_dur.barrier_ns())
            .saturating_add(cell.interference.cost_ns_per_op)
            .saturating_add(plen / 64 + 1);

        if (seq + 1) % 8 == 0 && e2e > 0 {
            let bps = (logical_bytes as f64) * 1e9 / (e2e as f64);
            window_samples.push(WindowSample {
                throughput_bytes_per_sec: bps,
            });
        }
    }

    // Correctness interlock.
    if let Err(e) = ledger.verify_correctness() {
        return Err(MatrixError::InvalidCorrectness(e.to_string()));
    }
    let reopen_ok = ledger.verify_reopen(&records).is_ok();
    if !reopen_ok {
        return Err(MatrixError::InvalidCorrectness(
            "reopen digest mismatch".into(),
        ));
    }

    // L2 shadow for physical shape matching.
    let sizes: Vec<u64> = (0..ops.min(16))
        .map(|i| {
            if cell.distribution.is_some() {
                sampler.size_at(i).min(8192)
            } else {
                cell.payload_size.min(8192)
            }
        })
        .collect();
    let plan = PlanBuilder::build(&ShapeConfig {
        plan_id: cell.cell_id.clone(),
        write_sizes: sizes,
        segment_threshold: 32 * 1024,
        chunk_threshold: 1024 * 1024,
        batch_size: cell.batch_size.max(1),
        sync_every_ops: if matches!(effective_dur, DurabilityMode::Durable) {
            4
        } else {
            0
        },
        alignment: 4096,
        final_sync: matches!(effective_dur, DurabilityMode::Durable),
    });
    let mut adapter = FakeIoAdapter::new(FakeIoConfig::default());
    let shadow = execute_shadow(&mut adapter, &plan)
        .map_err(|e: ShadowError| MatrixError::Msg(format!("{e}")))?;

    let window = WindowDetector::default().classify(&window_samples);
    let tput = if e2e == 0 {
        0.0
    } else {
        (logical_bytes as f64) * 1e9 / (e2e as f64)
    };

    Ok(CellRunReport {
        cell_id: cell.cell_id.clone(),
        layer: cell.layer.as_str().into(),
        durability: effective_dur.as_str().into(),
        validity: "valid".into(),
        attempted: ledger.attempted,
        admitted: ledger.admitted,
        acknowledged: ledger.acknowledged,
        failed: ledger.failed,
        logical_bytes_ack: logical_bytes,
        e2e_ns_proxy: e2e,
        throughput_bytes_per_sec_proxy: tput,
        window: format!("{window:?}").to_ascii_lowercase(),
        shadow_planned_bytes: plan.planned_bytes,
        shadow_completed_bytes: shadow.stats.bytes_completed,
        features: cell.features.name.clone(),
        interference: cell.interference.kind.as_str().into(),
        messages,
        reopen_ok,
    })
}

fn stage_set_for(features: &FeatureProfile) -> StageSet {
    use super::profiles::AdditiveFeature;
    let mut s = StageSet {
        validation: true,
        encoding: true,
        integrity: features
            .features
            .contains(&AdditiveFeature::IntegrityVerify)
            || features.name.starts_with("l4")
            || features.name.starts_with("l6"),
        chunking: features.features.contains(&AdditiveFeature::ChunkedValues),
        manifest: features.name.starts_with("l5") || features.name.starts_with("l6"),
        index_prep: features.features.contains(&AdditiveFeature::PrimaryIndex)
            || features.features.contains(&AdditiveFeature::SecondaryIndex),
    };
    // L4 always has integrity light path
    if features.name == "l4_minimal" {
        s.integrity = true;
        s.index_prep = true;
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matrix::profiles::{FeatureProfile, InterferenceProfile};
    use crate::matrix::scheduler::MatrixCell;

    fn sample_cell() -> MatrixCell {
        MatrixCell {
            cell_id: "t".into(),
            layer: LayerProfile::L4,
            durability: DurabilityMode::Durable,
            payload_size: 256,
            distribution: None,
            concurrency: 1,
            outstanding: 1,
            batch_size: 1,
            shards: 1,
            db_state: DatabaseState::Empty,
            features: FeatureProfile::l4_minimal(),
            interference: InterferenceProfile::absent(),
            op_count: 20,
            order_rank: 0,
        }
    }

    #[test]
    fn honest_cell_valid() {
        let r = run_cell(&RunCellConfig {
            cell: sample_cell(),
            seed: 1,
            durability_mutant: false,
            digest_mutant: false,
        })
        .unwrap();
        assert_eq!(r.validity, "valid");
        assert!(r.reopen_ok);
        assert!(r.acknowledged > 0);
        assert_eq!(r.attempted, r.acknowledged + r.failed);
    }

    #[test]
    fn durability_mutant_rejected() {
        let mut c = sample_cell();
        c.durability = DurabilityMode::Durable;
        let err = run_cell(&RunCellConfig {
            cell: c,
            seed: 1,
            durability_mutant: true,
            digest_mutant: false,
        });
        assert!(matches!(err, Err(MatrixError::DurabilityMutant(_))));
    }

    #[test]
    fn digest_mutant_invalidates() {
        let err = run_cell(&RunCellConfig {
            cell: sample_cell(),
            seed: 1,
            durability_mutant: false,
            digest_mutant: true,
        });
        assert!(matches!(err, Err(MatrixError::InvalidCorrectness(_))));
    }

    #[test]
    fn failures_in_denominator() {
        let mut c = sample_cell();
        c.op_count = 34; // includes seq 17
        let r = run_cell(&RunCellConfig {
            cell: c,
            seed: 1,
            durability_mutant: false,
            digest_mutant: false,
        })
        .unwrap();
        assert!(r.failed >= 1);
        assert_eq!(r.attempted, r.acknowledged + r.failed);
    }
}
