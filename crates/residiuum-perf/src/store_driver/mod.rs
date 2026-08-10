//! PQH-10: L4/L5/L6 drivers and store-native PhysicalWritePlan emission.
//!
//! - **Synthetic** (always available): harness proxy; **non-product**.
//! - **Real store** (`store-driver` feature): `residiuum-store` with
//!   `legacy-raw-store`; measurements may support product baselines only when
//!   the campaign platform allows it and disclosure is complete.
//!
//! Plans never contain payloads, keys, or heap identities.

mod aggregates;
mod emitter;
mod kinds;
mod synthetic;

#[cfg(feature = "store-driver")]
mod real;

pub use aggregates::{
    BoundaryAggregateSummary, ObserverOverheadReport, OverheadCostBasis, OverheadPairSample,
    WorkloadContract, OBSERVER_OVERHEAD_BUDGET, OVERHEAD_MIN_PAIRS_QUAL, OVERHEAD_MIN_PAIRS_SMOKE,
};
pub use emitter::{
    emit_plan_from_boundary_events, emit_plan_from_receipts, facts_from_boundary_events,
    StoreBoundaryEvent, StoreBoundaryKind, WriteReceiptFact, STORE_SEAM_EMITTER_FROM_RECEIPTS,
};
#[cfg(feature = "store-driver")]
pub use emitter::{emit_plan_from_store_boundary_events, facts_from_store_boundary_events};
pub use kinds::{AwoMode, DriverKind, MeasurementSurface, DRIVER_KIND_SYNTHETIC};
#[cfg(feature = "store-driver")]
pub use real::measure_probe_observer_overhead;

use crate::matrix::{CellRunReport, DurabilityMode, MatrixCell, MatrixError};
use crate::shadow::PhysicalWritePlan;
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DriverError {
    #[error("driver: {0}")]
    Msg(String),
    #[error("matrix: {0}")]
    Matrix(#[from] MatrixError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("store feature disabled; rebuild with --features store-driver")]
    StoreFeatureDisabled,
    #[cfg(feature = "store-driver")]
    #[error("store: {0}")]
    Store(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverCellReport {
    pub cell: CellRunReport,
    pub driver_kind: DriverKind,
    pub measurement_surface: MeasurementSurface,
    /// True only when this run may contribute to a product baseline claim.
    pub product_claim_eligible: bool,
    /// Lossless PhysicalWritePlan for L2 replay — **None** when samples dropped.
    pub plan: Option<PhysicalWritePlan>,
    pub plan_source: String,
    /// True only when plan is complete (zero sample drops) for replay claims.
    #[serde(default)]
    pub lossless_plan_eligible: bool,
    /// Exact boundary aggregates (always when probe ran); separate from plan.
    #[serde(default)]
    pub boundary_aggregates: Option<BoundaryAggregateSummary>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DriverRunConfig {
    pub cell: MatrixCell,
    pub seed: u64,
    pub kind: DriverKind,
    /// Work root for real store paths (must already be path-guarded by caller).
    pub work_root: Option<std::path::PathBuf>,
    pub durability_mutant: bool,
    pub digest_mutant: bool,
    /// `smoke` | `diagnostic` | `qualification` | `soak` (SPEC §6.4).
    /// Default smoke when empty.
    pub run_class: String,
    /// Adaptive write mode for real_store (ignored by synthetic). Default disabled.
    pub awo_mode: AwoMode,
}

impl DriverRunConfig {
    pub fn run_class_parsed(&self) -> crate::campaign::RunClass {
        crate::campaign::RunClass::parse(&self.run_class)
            .unwrap_or(crate::campaign::RunClass::Smoke)
    }
}

/// Run one cell with the selected driver.
pub fn run_driver_cell(cfg: &DriverRunConfig) -> Result<DriverCellReport, DriverError> {
    match cfg.kind {
        DriverKind::Synthetic => synthetic::run_synthetic(cfg),
        DriverKind::RealStore => {
            #[cfg(feature = "store-driver")]
            {
                real::run_real_store(cfg)
            }
            #[cfg(not(feature = "store-driver"))]
            {
                let _ = cfg;
                Err(DriverError::StoreFeatureDisabled)
            }
        }
    }
}

/// Whether the `store-driver` feature is compiled in.
pub fn store_driver_compiled() -> bool {
    cfg!(feature = "store-driver")
}

/// Map matrix durability to a stable name (shared).
pub fn durability_name(d: DurabilityMode) -> &'static str {
    d.as_str()
}

/// Ensure a subdirectory under work_root for a cell store.
pub fn cell_store_path(work_root: &Path, cell_id: &str, process_tag: &str) -> std::path::PathBuf {
    work_root
        .join("stores")
        .join(process_tag)
        .join(sanitize_id(cell_id))
}

fn sanitize_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
