//! Lane errors.

use residiuum_atomics::{AtomicsError, StagingFailpoint};
use std::io;

/// Durable-lane failure.
#[derive(Debug, thiserror::Error)]
pub enum LaneError {
    /// Armed failpoint fired after the prefix writes were synced.
    #[error("injected failpoint {0:?}")]
    Injected(StagingFailpoint),
    /// In-memory kernel refused the operation.
    #[error(transparent)]
    Kernel(#[from] AtomicsError),
    /// Filesystem error.
    #[error("lane io: {0}")]
    Io(#[from] io::Error),
    /// Frame or envelope encode/decode failed.
    #[error("lane format: {0}")]
    Format(String),
    /// On-disk image is missing a required sidecar or identity.
    #[error("corrupt lane: {0}")]
    Corrupt(&'static str),
    /// Acknowledged log coverage is damaged (not a torn unacked tail).
    #[error("lane coverage {start}..{end}: {reason}")]
    Coverage {
        /// Hole start offset.
        start: u64,
        /// Hole end offset.
        end: u64,
        /// Scan hole classification.
        reason: &'static str,
    },
    /// A diagnostic recovery ceiling was hit. Coverage is incomplete, not empty.
    #[error("lane recovery incomplete: {what} ({observed} > {limit})")]
    Incomplete {
        /// Which ceiling was exceeded.
        what: &'static str,
        /// Observed quantity.
        observed: u64,
        /// Configured ceiling.
        limit: u64,
    },
    /// Another process or in-process instance already holds this directory.
    #[error("lane writer lock held")]
    WriterHeld,
}
