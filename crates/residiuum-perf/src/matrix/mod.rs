//! PQH-7: L4/L5/L6 database matrix runner.
//!
//! Harness-side drivers model the authoritative path with a correctness
//! interlock (ack ledger + independent digest). Real store attachment lives in
//! [`crate::store_driver`] (feature `store-driver`); this matrix module remains
//! the synthetic/proxy path and is always **non-product**.

mod compare;
mod driver;
mod ledger;
mod profiles;
mod scheduler;

pub use compare::{select_matched_pairs, CellResult, MatchedPair};
pub use driver::{
    run_cell, CellRunReport, DatabaseState, DurabilityMode, LayerProfile, RunCellConfig,
};
pub use ledger::{AckLedger, LedgerError};
pub use profiles::{AdditiveFeature, BackgroundInterference, FeatureProfile, InterferenceProfile};
pub use scheduler::{build_matrix_cells, MatrixCell, MatrixManifest, ScheduleSeed};

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MatrixError {
    #[error("matrix: {0}")]
    Msg(String),
    #[error("ledger: {0}")]
    Ledger(#[from] LedgerError),
    #[error("invalid_correctness: {0}")]
    InvalidCorrectness(String),
    #[error("durability_mutant_rejected: {0}")]
    DurabilityMutant(String),
}
