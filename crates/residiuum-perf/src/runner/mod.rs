//! PQH-1: safe runner, path guard, preflight, platform fingerprint, artifacts.
//!
//! Safety boundary (SPEC §4 / plan §5): operate only inside an explicitly
//! supplied dedicated work directory; reject destructive targets; enforce
//! budgets; leave classifiable artifacts on every interruption.

mod artifacts;
mod budget;
mod fingerprint;
mod marker;
mod metric;
mod path_guard;
mod platform;
mod preflight;

pub use artifacts::{write_cancel_partial, write_run_artifacts, RunArtifactLayout};
pub use budget::{BudgetCheck, RunBudgets};
pub use fingerprint::{environment_fingerprint, EnvironmentFingerprint};
pub use marker::{
    ensure_work_root_marker, marker_path, read_marker, verify_marker, WorkRootMarker,
    MARKER_FILE_NAME, MARKER_SCHEMA,
};
pub use metric::{MetricObservation, UnavailableMetric};
pub use path_guard::{classify_work_path, PathRejectReason, WorkPathClass};
pub use platform::{
    detect_adapter, free_space_bytes, free_space_inodes, mount_points, PlatformAdapter,
};
pub use preflight::{
    is_debug_build, preflight_work_root, PreflightConfig, PreflightOutcome, PreflightReport,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Build / profile mode for preflight and fingerprints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildMode {
    /// Developer smoke; debug builds allowed.
    Diagnostic,
    /// Qualification evidence; debug builds rejected.
    Qualification,
}

impl BuildMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Diagnostic => "diagnostic",
            Self::Qualification => "qualification",
        }
    }

    pub fn allows_debug(self) -> bool {
        matches!(self, Self::Diagnostic)
    }
}

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("invalid marker: {0}")]
    InvalidMarker(String),
    #[error("budget: {0}")]
    Budget(String),
    #[error("preflight: {0}")]
    Preflight(String),
    #[error("debug build rejected for qualification")]
    DebugBuildRejected,
}
