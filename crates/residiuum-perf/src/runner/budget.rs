//! Byte / time / free-space budgets (SPEC §4, plan §5).

use super::platform::{free_space_bytes, free_space_inodes};
use super::RunnerError;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;

/// Default free-space floor for developer-safe profile: 512 MiB.
pub const DEFAULT_FREE_SPACE_FLOOR_BYTES: u64 = 512 * 1024 * 1024;
/// Default free-inode floor (when observable).
pub const DEFAULT_FREE_INODE_FLOOR: u64 = 10_000;
/// Default max wall time for a single cell (diagnostic-safe): 15 minutes.
pub const DEFAULT_MAX_DURATION_SECS: u64 = 15 * 60;
/// Default max logical payload budget for diagnostic: 256 MiB.
pub const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunBudgets {
    /// Maximum logical payload / written bytes for the run.
    pub max_bytes: u64,
    /// Maximum wall-clock duration.
    pub max_duration_secs: u64,
    /// Must keep at least this many free bytes on the work filesystem.
    pub free_space_floor_bytes: u64,
    /// Must keep at least this many free inodes when observable.
    pub free_inode_floor: u64,
}

impl Default for RunBudgets {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_BYTES,
            max_duration_secs: DEFAULT_MAX_DURATION_SECS,
            free_space_floor_bytes: DEFAULT_FREE_SPACE_FLOOR_BYTES,
            free_inode_floor: DEFAULT_FREE_INODE_FLOOR,
        }
    }
}

impl RunBudgets {
    pub fn max_duration(&self) -> Duration {
        Duration::from_secs(self.max_duration_secs)
    }

    /// True when planned write would exceed max_bytes or free-space floor.
    pub fn check_planned_write(
        &self,
        free_bytes: u64,
        planned_write_bytes: u64,
        bytes_written_so_far: u64,
    ) -> BudgetCheck {
        if bytes_written_so_far.saturating_add(planned_write_bytes) > self.max_bytes {
            return BudgetCheck::Exceeded {
                kind: BudgetKind::MaxBytes,
                detail: format!(
                    "planned {} + written {} exceeds max_bytes {}",
                    planned_write_bytes, bytes_written_so_far, self.max_bytes
                ),
            };
        }
        // Stop before crossing free-space floor: after write, free must remain >= floor.
        if free_bytes.saturating_sub(planned_write_bytes) < self.free_space_floor_bytes {
            return BudgetCheck::Exceeded {
                kind: BudgetKind::FreeSpaceFloor,
                detail: format!(
                    "free {free_bytes} - planned {planned_write_bytes} would breach floor {}",
                    self.free_space_floor_bytes
                ),
            };
        }
        BudgetCheck::Ok
    }

    pub fn check_duration(&self, elapsed_secs: u64) -> BudgetCheck {
        if elapsed_secs > self.max_duration_secs {
            BudgetCheck::Exceeded {
                kind: BudgetKind::MaxDuration,
                detail: format!(
                    "elapsed {elapsed_secs}s exceeds max_duration_secs {}",
                    self.max_duration_secs
                ),
            }
        } else {
            BudgetCheck::Ok
        }
    }

    /// Probe live free space/inodes at `path` and enforce floors.
    pub fn check_live_free_space(&self, path: &Path) -> Result<BudgetCheck, RunnerError> {
        let free = free_space_bytes(path)?;
        if free < self.free_space_floor_bytes {
            return Ok(BudgetCheck::Exceeded {
                kind: BudgetKind::FreeSpaceFloor,
                detail: format!(
                    "free bytes {free} below floor {}",
                    self.free_space_floor_bytes
                ),
            });
        }
        if let Some(inodes) = free_space_inodes(path)? {
            if inodes < self.free_inode_floor {
                return Ok(BudgetCheck::Exceeded {
                    kind: BudgetKind::FreeInodeFloor,
                    detail: format!("free inodes {inodes} below floor {}", self.free_inode_floor),
                });
            }
        }
        Ok(BudgetCheck::Ok)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetKind {
    MaxBytes,
    MaxDuration,
    FreeSpaceFloor,
    FreeInodeFloor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetCheck {
    Ok,
    Exceeded { kind: BudgetKind, detail: String },
}

impl BudgetCheck {
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }

    pub fn into_result(self) -> Result<(), RunnerError> {
        match self {
            Self::Ok => Ok(()),
            Self::Exceeded { detail, .. } => Err(RunnerError::Budget(detail)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planned_write_respects_max_bytes() {
        let b = RunBudgets {
            max_bytes: 1000,
            free_space_floor_bytes: 100,
            ..RunBudgets::default()
        };
        assert!(b.check_planned_write(10_000, 500, 400).is_ok());
        assert!(!b.check_planned_write(10_000, 700, 400).is_ok());
    }

    #[test]
    fn planned_write_respects_free_floor() {
        let b = RunBudgets {
            max_bytes: 1_000_000,
            free_space_floor_bytes: 1_000,
            ..RunBudgets::default()
        };
        // free 1500, plan 600 → remaining 900 < 1000 floor
        assert!(!b.check_planned_write(1500, 600, 0).is_ok());
        assert!(b.check_planned_write(1500, 400, 0).is_ok());
    }

    #[test]
    fn duration_budget() {
        let b = RunBudgets {
            max_duration_secs: 10,
            ..RunBudgets::default()
        };
        assert!(b.check_duration(10).is_ok());
        assert!(!b.check_duration(11).is_ok());
    }
}
