//! Exact boundary aggregate summaries — separate from lossless plan/replay.
//!
//! Counters, latency histograms, chain digests, and coverage are always valid
//! when the store probe ran. PhysicalWritePlan + L2 replay claims require a
//! **lossless** sample set (zero dropped samples).

use serde::{Deserialize, Serialize};

/// Exact probe aggregates for evidence (never claim lossless plan from these alone).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BoundaryAggregateSummary {
    /// Blake3 hex of the full event chain (all observations, including dropped samples).
    pub event_chain_digest: String,
    /// Total observations while probe enabled.
    pub total_observed: u64,
    /// Samples retained in the vector.
    pub samples_retained: u64,
    /// Observations not retained (capacity full).
    pub samples_dropped: u64,
    /// Sample vector capacity.
    pub sample_capacity: u64,
    /// True when any sample was dropped from the vector.
    pub sample_vector_capped: bool,
    /// Explicit drop reason when capped.
    pub drop_reason: Option<String>,
    /// Per-kind counts (append, file_write, file_sync, …) as name→count map.
    pub counts_by_kind: Vec<(String, u64)>,
    /// Outcome totals.
    pub outcome_ok: u64,
    pub outcome_short_write: u64,
    pub outcome_io_error: u64,
    pub total_requested_bytes: u64,
    pub total_completed_bytes: u64,
    /// Latency sample counts (histograms remain in store snapshot; counts only here).
    pub write_latency_samples: u64,
    pub sync_latency_samples: u64,
    pub append_latency_samples: u64,
    pub write_latency_mean_ns: f64,
    pub sync_latency_mean_ns: f64,
    /// True only when sample set is complete — plan/replay may be claimed.
    pub lossless_plan_eligible: bool,
    /// Why plan/replay is invalid when not eligible.
    pub plan_replay_invalidate_reason: Option<String>,
}

impl BoundaryAggregateSummary {
    /// Build from store snapshot fields (feature `store-driver`).
    #[cfg(feature = "store-driver")]
    pub fn from_store_snapshot(snap: &residiuum_store::BoundarySnapshot) -> Self {
        use residiuum_store::BoundaryKind;
        let cov = &snap.coverage;
        let lossless = !cov.sample_vector_capped && cov.samples_dropped == 0;
        let counts_by_kind = BoundaryKind::ALL
            .iter()
            .map(|k| (k.as_str().to_string(), snap.counters.count(*k)))
            .collect();
        Self {
            event_chain_digest: snap.event_chain_digest.clone(),
            total_observed: cov.total_observed,
            samples_retained: cov.samples_retained,
            samples_dropped: cov.samples_dropped,
            sample_capacity: cov.sample_capacity,
            sample_vector_capped: cov.sample_vector_capped,
            drop_reason: cov.drop_reason.clone(),
            counts_by_kind,
            outcome_ok: snap.counters.outcome_ok,
            outcome_short_write: snap.counters.outcome_short_write,
            outcome_io_error: snap.counters.outcome_io_error,
            total_requested_bytes: snap.counters.total_requested_bytes,
            total_completed_bytes: snap.counters.total_completed_bytes,
            write_latency_samples: snap.write_latency.samples,
            sync_latency_samples: snap.sync_latency.samples,
            append_latency_samples: snap.append_latency.samples,
            write_latency_mean_ns: snap.write_latency.mean_ns(),
            sync_latency_mean_ns: snap.sync_latency.mean_ns(),
            lossless_plan_eligible: lossless,
            plan_replay_invalidate_reason: if lossless {
                None
            } else {
                Some(format!(
                    "sample drops invalidate lossless plan/replay: dropped={} capped={} capacity={}",
                    cov.samples_dropped, cov.sample_vector_capped, cov.sample_capacity
                ))
            },
        }
    }
}

/// Default observer overhead budget (fraction of probe-off wall time).
/// Matches analyze narratives default (`observer_budget` 0.02).
pub const OBSERVER_OVERHEAD_BUDGET: f64 = 0.02;

/// Minimum order-balanced off/on pairs for non-smoke qualification overhead.
pub const OVERHEAD_MIN_PAIRS_QUAL: u32 = 4;

/// Minimum pairs for smoke/CI overhead (still order-balanced).
pub const OVERHEAD_MIN_PAIRS_SMOKE: u32 = 2;

/// Qualification workload contract mirrored by overhead legs (not op_count alone).
///
/// Overhead probe-off/on legs must execute the **same** stop conditions and cell
/// parameters as the measured cell driver: duration/byte floors (non-smoke),
/// smoke op cap when applicable, payload distribution, durability, concurrency,
/// outstanding, shards, batch, db_state, features, and interference.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkloadContract {
    pub cell_id: String,
    pub run_class: String,
    /// How the leg stops: `smoke_op_cap` or `duration_and_byte_floors`.
    pub stop_policy: String,
    pub min_duration_secs: u64,
    pub min_logical_bytes: u64,
    /// Smoke-only op ceiling when `stop_policy=smoke_op_cap`; else None.
    pub smoke_ops_limit: Option<u64>,
    /// Planned cell op_count (not a silent helper cap for non-smoke).
    pub cell_op_count: u64,
    pub payload_size: u64,
    pub distribution: Option<String>,
    pub durability: String,
    pub concurrency: u32,
    pub outstanding: u32,
    pub batch_size: u32,
    pub shards: u32,
    pub db_state: String,
    pub features: String,
    pub interference: String,
    pub layer: String,
    /// How matrix dimensions are applied as behaviour.
    pub application_note: String,
}

impl WorkloadContract {
    /// Build contract from a matrix cell + run class floors.
    pub fn from_cell_and_class(
        cell: &crate::matrix::MatrixCell,
        run_class: crate::campaign::RunClass,
    ) -> Self {
        let smoke = run_class.allows_smoke_op_cap();
        let stop_policy = if smoke {
            "smoke_op_cap"
        } else {
            "duration_and_byte_floors"
        };
        let smoke_ops_limit = if smoke {
            Some(
                cell.op_count
                    .min(crate::campaign::RunClass::SMOKE_MAX_OPS)
                    .max(1),
            )
        } else {
            None
        };
        Self {
            cell_id: cell.cell_id.clone(),
            run_class: run_class.as_str().into(),
            stop_policy: stop_policy.into(),
            min_duration_secs: run_class.min_duration_secs(),
            min_logical_bytes: run_class.min_logical_bytes(),
            smoke_ops_limit,
            cell_op_count: cell.op_count,
            payload_size: cell.payload_size,
            distribution: cell.distribution.map(|d| d.as_str().to_string()),
            durability: cell.durability.as_str().into(),
            concurrency: cell.concurrency,
            outstanding: cell.outstanding,
            batch_size: cell.batch_size,
            shards: cell.shards,
            db_state: cell.db_state.as_str().into(),
            features: cell.features.name.clone(),
            interference: cell.interference.kind.as_str().into(),
            layer: cell.layer.as_str().into(),
            application_note:
                "shared execute_workload_puts: create_with_shards; concurrent preparers; \
                 outstanding-bounded put_many (put_many_parallel when multi-shard, probe-instrumented); \
                 same floors as measured cell"
                    .into(),
        }
    }

    /// True when stop policy is floors (qualification/diagnostic/soak), not op_count.
    pub fn uses_duration_byte_floors(&self) -> bool {
        self.stop_policy == "duration_and_byte_floors"
    }
}

/// How probe cost is derived for a pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverheadCostBasis {
    /// Smoke/op-cap: (on_wall - off_wall) / off_wall.
    WallTime,
    /// Duration-floor legs: (tput_off - tput_on) / tput_off from sustained throughput.
    SustainedThroughput,
}

impl OverheadCostBasis {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WallTime => "wall_time",
            Self::SustainedThroughput => "sustained_throughput",
        }
    }
}

/// One matched probe-off / probe-on sample (one leg order).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OverheadPairSample {
    pub pair_index: u32,
    /// `off_first` (AB) or `on_first` (BA) — order-balanced across pairs.
    pub order: String,
    pub probe_off_e2e_ns: u64,
    pub probe_on_e2e_ns: u64,
    pub probe_off_logical_bytes: u64,
    pub probe_on_logical_bytes: u64,
    /// Ops completed on each leg (should match contract stop policy).
    #[serde(default)]
    pub probe_off_ops: u64,
    #[serde(default)]
    pub probe_on_ops: u64,
    /// Sustained throughput (bytes/s) for duration-controlled cost basis.
    #[serde(default)]
    pub probe_off_sustained_tput_bps: f64,
    #[serde(default)]
    pub probe_on_sustained_tput_bps: f64,
    /// Cost basis used for `overhead_fraction`.
    #[serde(default = "default_cost_basis")]
    pub cost_basis: OverheadCostBasis,
    /// Both legs met floors + validity for this pair.
    #[serde(default)]
    pub legs_valid: bool,
    /// Probe cost fraction (see `cost_basis`).
    pub overhead_fraction: f64,
}

fn default_cost_basis() -> OverheadCostBasis {
    OverheadCostBasis::WallTime
}

/// Matched probe-off vs probe-on observer overhead for **one** qualified cell.
///
/// Legs execute the same [`WorkloadContract`] as the measured cell — duration/
/// byte floors (or smoke op cap), not a fixed op_count toy helper.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObserverOverheadReport {
    pub cell_id: String,
    pub seed: u64,
    /// Full workload contract (floors, distribution, durability, concurrency, …).
    #[serde(default)]
    pub workload_contract: Option<WorkloadContract>,
    /// Ops completed per leg (mean); not the stop policy for non-smoke.
    #[serde(default)]
    pub ops_per_leg: u64,
    #[serde(default)]
    pub payload_size: u64,
    #[serde(default)]
    pub run_class: String,
    /// Mean of pair wall times (off legs).
    pub probe_off_e2e_ns: u64,
    /// Mean of pair wall times (on legs).
    pub probe_on_e2e_ns: u64,
    pub probe_off_logical_bytes: u64,
    pub probe_on_logical_bytes: u64,
    /// Mean (on - off) / off across pairs; primary measured value for attribution.
    pub overhead_fraction: f64,
    /// Median pair overhead (robust to single-leg noise).
    #[serde(default)]
    pub median_overhead_fraction: f64,
    /// Budget used for `within_budget` (default [`OBSERVER_OVERHEAD_BUDGET`]).
    #[serde(default = "default_overhead_budget")]
    pub overhead_budget: f64,
    /// True when median overhead ≤ budget.
    #[serde(default)]
    pub within_budget: bool,
    #[serde(default)]
    pub pair_count: u32,
    #[serde(default)]
    pub pairs: Vec<OverheadPairSample>,
    pub notes: Vec<String>,
}

fn default_overhead_budget() -> f64 {
    OBSERVER_OVERHEAD_BUDGET
}

impl OverheadPairSample {
    pub fn from_times(
        pair_index: u32,
        order: &str,
        off_e2e_ns: u64,
        on_e2e_ns: u64,
        off_bytes: u64,
        on_bytes: u64,
    ) -> Self {
        Self::from_leg_stats(
            pair_index,
            order,
            off_e2e_ns,
            on_e2e_ns,
            off_bytes,
            on_bytes,
            0,
            0,
            0.0,
            0.0,
            OverheadCostBasis::WallTime,
            true,
        )
    }

    pub fn from_leg_stats(
        pair_index: u32,
        order: &str,
        off_e2e_ns: u64,
        on_e2e_ns: u64,
        off_bytes: u64,
        on_bytes: u64,
        off_ops: u64,
        on_ops: u64,
        off_tput_bps: f64,
        on_tput_bps: f64,
        cost_basis: OverheadCostBasis,
        legs_valid: bool,
    ) -> Self {
        let overhead_fraction = match cost_basis {
            OverheadCostBasis::WallTime => {
                if off_e2e_ns > 0 {
                    (on_e2e_ns as f64 - off_e2e_ns as f64) / off_e2e_ns as f64
                } else {
                    0.0
                }
            }
            OverheadCostBasis::SustainedThroughput => {
                // Relative throughput loss when probe is on (not wall-time delta).
                if off_tput_bps > 0.0 && off_tput_bps.is_finite() {
                    (off_tput_bps - on_tput_bps) / off_tput_bps
                } else {
                    0.0
                }
            }
        };
        Self {
            pair_index,
            order: order.into(),
            probe_off_e2e_ns: off_e2e_ns,
            probe_on_e2e_ns: on_e2e_ns,
            probe_off_logical_bytes: off_bytes,
            probe_on_logical_bytes: on_bytes,
            probe_off_ops: off_ops,
            probe_on_ops: on_ops,
            probe_off_sustained_tput_bps: off_tput_bps,
            probe_on_sustained_tput_bps: on_tput_bps,
            cost_basis,
            legs_valid,
            overhead_fraction,
        }
    }
}

impl ObserverOverheadReport {
    /// Single pair (smoke convenience / unit tests).
    pub fn from_pair(
        cell_id: &str,
        seed: u64,
        off_e2e_ns: u64,
        on_e2e_ns: u64,
        off_bytes: u64,
        on_bytes: u64,
    ) -> Self {
        let pair = OverheadPairSample::from_times(
            0,
            "off_first",
            off_e2e_ns,
            on_e2e_ns,
            off_bytes,
            on_bytes,
        );
        Self::from_pairs(
            cell_id,
            seed,
            None,
            0,
            0,
            "smoke",
            OBSERVER_OVERHEAD_BUDGET,
            vec![pair],
        )
    }

    /// Build report from order-balanced multi-pair samples; enforces budget.
    pub fn from_pairs(
        cell_id: &str,
        seed: u64,
        workload_contract: Option<WorkloadContract>,
        ops_per_leg: u64,
        payload_size: u64,
        run_class: &str,
        overhead_budget: f64,
        pairs: Vec<OverheadPairSample>,
    ) -> Self {
        let pair_count = pairs.len() as u32;
        let mut notes = vec![
            format!(
                "order-balanced probe-off/on on matched workload contract: pairs={pair_count} ops_per_leg={ops_per_leg} run_class={run_class}"
            ),
        ];
        if let Some(c) = workload_contract.as_ref() {
            notes.push(format!(
                "workload_contract stop={} floors_s={} floors_bytes={} dur={} conc={} out={} shards={} dist={:?} features={} interference={} db_state={}",
                c.stop_policy,
                c.min_duration_secs,
                c.min_logical_bytes,
                c.durability,
                c.concurrency,
                c.outstanding,
                c.shards,
                c.distribution,
                c.features,
                c.interference,
                c.db_state
            ));
            if c.uses_duration_byte_floors() {
                notes.push(
                    "stop_policy=duration_and_byte_floors (not cell.op_count); same as measured cell"
                        .into(),
                );
            } else {
                notes.push(format!(
                    "stop_policy=smoke_op_cap limit={:?}",
                    c.smoke_ops_limit
                ));
            }
        }
        if pairs.is_empty() {
            notes.push("no pairs measured".into());
            return Self {
                cell_id: cell_id.into(),
                seed,
                workload_contract,
                ops_per_leg,
                payload_size,
                run_class: run_class.into(),
                probe_off_e2e_ns: 0,
                probe_on_e2e_ns: 0,
                probe_off_logical_bytes: 0,
                probe_on_logical_bytes: 0,
                overhead_fraction: 0.0,
                median_overhead_fraction: 0.0,
                overhead_budget,
                within_budget: false,
                pair_count: 0,
                pairs,
                notes,
            };
        }

        let n = pairs.len() as f64;
        let sum_off: u128 = pairs.iter().map(|p| p.probe_off_e2e_ns as u128).sum();
        let sum_on: u128 = pairs.iter().map(|p| p.probe_on_e2e_ns as u128).sum();
        let mean_off = (sum_off / pairs.len() as u128) as u64;
        let mean_on = (sum_on / pairs.len() as u128) as u64;
        let mean_off_bytes =
            pairs.iter().map(|p| p.probe_off_logical_bytes).sum::<u64>() / pairs.len() as u64;
        let mean_on_bytes =
            pairs.iter().map(|p| p.probe_on_logical_bytes).sum::<u64>() / pairs.len() as u64;
        let mean_ops = pairs
            .iter()
            .map(|p| p.probe_off_ops.saturating_add(p.probe_on_ops) / 2)
            .sum::<u64>()
            / pairs.len() as u64;
        let ops_per_leg = if ops_per_leg > 0 {
            ops_per_leg
        } else {
            mean_ops
        };

        let mean_frac: f64 = pairs.iter().map(|p| p.overhead_fraction).sum::<f64>() / n;
        let mut fracs: Vec<f64> = pairs.iter().map(|p| p.overhead_fraction).collect();
        fracs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = if fracs.len() % 2 == 1 {
            fracs[fracs.len() / 2]
        } else {
            (fracs[fracs.len() / 2 - 1] + fracs[fracs.len() / 2]) / 2.0
        };

        let within_budget = median <= overhead_budget;
        notes.push(format!(
            "mean_overhead={mean_frac:.6} median_overhead={median:.6} budget={overhead_budget:.4} within_budget={within_budget}"
        ));
        for p in &pairs {
            notes.push(format!(
                "pair[{}] order={} basis={} overhead={:.6} off_ops={} on_ops={} off_tput={:.3} on_tput={:.3} valid={}",
                p.pair_index,
                p.order,
                p.cost_basis.as_str(),
                p.overhead_fraction,
                p.probe_off_ops,
                p.probe_on_ops,
                p.probe_off_sustained_tput_bps,
                p.probe_on_sustained_tput_bps,
                p.legs_valid
            ));
        }
        if pairs.iter().any(|p| !p.legs_valid) {
            notes.push("LEG_INVALID: at least one overhead leg failed floors/validity".into());
        }
        if pairs
            .iter()
            .any(|p| p.cost_basis == OverheadCostBasis::SustainedThroughput)
        {
            notes.push(
                "duration-controlled pairs use sustained_throughput probe cost (not wall-time)"
                    .into(),
            );
        }
        if mean_off_bytes != mean_on_bytes {
            notes.push(format!(
                "logical_bytes differ off={mean_off_bytes} on={mean_on_bytes}; overhead wall-time only"
            ));
        }
        if mean_frac < 0.0 || median < 0.0 {
            notes.push(
                "probe-on faster than probe-off on average (noise); negative overhead reported"
                    .into(),
            );
        }
        if !within_budget {
            notes.push(format!(
                "OVERHEAD_BUDGET_EXCEEDED: median {median:.6} > budget {overhead_budget:.4}; product claims must not treat probe cost as DB cost"
            ));
        }
        // Order balance note
        let off_first = pairs.iter().filter(|p| p.order == "off_first").count();
        let on_first = pairs.iter().filter(|p| p.order == "on_first").count();
        notes.push(format!(
            "order_balance off_first={off_first} on_first={on_first}"
        ));

        Self {
            cell_id: cell_id.into(),
            seed,
            workload_contract,
            ops_per_leg,
            payload_size,
            run_class: run_class.into(),
            probe_off_e2e_ns: mean_off,
            probe_on_e2e_ns: mean_on,
            probe_off_logical_bytes: mean_off_bytes,
            probe_on_logical_bytes: mean_on_bytes,
            overhead_fraction: mean_frac,
            median_overhead_fraction: median,
            overhead_budget,
            within_budget,
            pair_count,
            pairs,
            notes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overhead_fraction_basic() {
        let r = ObserverOverheadReport::from_pair("c1", 1, 1000, 1100, 100, 100);
        assert!((r.overhead_fraction - 0.1).abs() < 1e-9);
        // 10% exceeds default 2% budget
        assert!(!r.within_budget);
        assert!((r.median_overhead_fraction - 0.1).abs() < 1e-9);
    }

    #[test]
    fn sustained_throughput_cost_basis() {
        // off 100 MB/s, on 95 MB/s → 5% overhead from throughput loss
        let p = OverheadPairSample::from_leg_stats(
            0,
            "off_first",
            10_000,
            10_500, // wall times ignored for tput basis
            1_000_000,
            1_000_000,
            100,
            100,
            100e6,
            95e6,
            OverheadCostBasis::SustainedThroughput,
            true,
        );
        assert!((p.overhead_fraction - 0.05).abs() < 1e-9);
        assert_eq!(p.cost_basis, OverheadCostBasis::SustainedThroughput);
    }

    #[test]
    fn multi_pair_order_balance_and_budget() {
        let pairs = vec![
            OverheadPairSample::from_times(0, "off_first", 1000, 1005, 100, 100),
            OverheadPairSample::from_times(1, "on_first", 1000, 1008, 100, 100),
            OverheadPairSample::from_times(2, "off_first", 1000, 1010, 100, 100),
            OverheadPairSample::from_times(3, "on_first", 1000, 1006, 100, 100),
        ];
        let r = ObserverOverheadReport::from_pairs(
            "c1",
            1,
            None,
            32,
            256,
            "qualification",
            OBSERVER_OVERHEAD_BUDGET,
            pairs,
        );
        assert_eq!(r.pair_count, 4);
        assert!(r.median_overhead_fraction < 0.02);
        assert!(r.within_budget);
        assert_eq!(r.pairs.iter().filter(|p| p.order == "off_first").count(), 2);
        assert_eq!(r.pairs.iter().filter(|p| p.order == "on_first").count(), 2);
    }

    #[test]
    fn budget_exceeded_flag() {
        let pairs = vec![
            OverheadPairSample::from_times(0, "off_first", 1000, 1200, 50, 50),
            OverheadPairSample::from_times(1, "on_first", 1000, 1250, 50, 50),
        ];
        let r = ObserverOverheadReport::from_pairs(
            "c1",
            2,
            None,
            8,
            64,
            "smoke",
            OBSERVER_OVERHEAD_BUDGET,
            pairs,
        );
        assert!(!r.within_budget);
        assert!(r
            .notes
            .iter()
            .any(|n| n.contains("OVERHEAD_BUDGET_EXCEEDED")));
    }
}
