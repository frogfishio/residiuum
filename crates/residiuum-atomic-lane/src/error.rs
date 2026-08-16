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
}
