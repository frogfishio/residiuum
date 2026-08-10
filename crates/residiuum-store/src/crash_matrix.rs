//! Machine-readable crash matrix loader (DEF-022).
//!
//! The canonical matrix lives at `crash_matrix.v1.json` next to this crate's
//! sources and is embedded at compile time. Tests validate structure and run a
//! bounded CI subset; nightly may request the full set via env.

use serde::Deserialize;

/// Embedded crash matrix document (version 1).
pub const CRASH_MATRIX_JSON: &str = include_str!("../crash_matrix.v1.json");

/// Top-level matrix document.
#[derive(Debug, Clone, Deserialize)]
pub struct CrashMatrix {
    /// Schema version (must be 1).
    pub version: u32,
    /// Human title.
    pub title: String,
    /// Global reopen invariants every cell must respect.
    pub invariants: Vec<String>,
    /// Operations under test with ordered persistence steps and failpoints.
    pub operations: Vec<MatrixOperation>,
}

/// One store operation and its crash surface.
#[derive(Debug, Clone, Deserialize)]
pub struct MatrixOperation {
    /// Stable id (e.g. `put_durable`).
    pub id: String,
    /// Human description.
    pub description: String,
    /// Ordered persistence steps (documentation / planning).
    pub persistence_order: Vec<String>,
    /// Failpoints instrumented for this operation.
    pub failpoints: Vec<MatrixFailpoint>,
}

/// One failpoint cell in the matrix.
#[derive(Debug, Clone, Deserialize)]
pub struct MatrixFailpoint {
    /// Failpoint name (must match [`crate::failpoint::hit`] argument).
    pub name: String,
    /// Whether CI runs this cell on every PR.
    #[serde(default)]
    pub ci_subset: bool,
    /// Optional fault class beyond the default `Error` action.
    ///
    /// Recognized values: `enospc`, `permission`, `short_write`, `process_abort`,
    /// `panic`, `error` (default when omitted).
    #[serde(default)]
    pub fault: Option<String>,
    /// Expected state after drop + reopen when the failpoint fires mid-op.
    pub expected_on_reopen: ExpectedReopen,
}

/// Assertions after simulated crash at a failpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct ExpectedReopen {
    /// If the operation was not acknowledged, the new value must not appear.
    #[serde(default)]
    pub acknowledged_visible: Option<bool>,
    /// Must never invent a committed event that was not on durable media.
    #[serde(default = "default_true")]
    pub no_fabricated_commit: bool,
    /// Surviving frames remain salvageable.
    #[serde(default = "default_true")]
    pub salvageable: bool,
    /// Prior durable keys remain visible after reopen.
    #[serde(default = "default_true")]
    pub prior_durable_retained: bool,
    /// Free-form notes for humans / full matrix drivers.
    #[serde(default)]
    pub notes: String,
}

fn default_true() -> bool {
    true
}

/// Parse the embedded matrix.
pub fn load_embedded() -> Result<CrashMatrix, String> {
    serde_json::from_str(CRASH_MATRIX_JSON).map_err(|e| format!("crash matrix parse: {e}"))
}

/// Validate structural rules for the matrix (used by CI).
pub fn validate(matrix: &CrashMatrix) -> Result<(), String> {
    if matrix.version != 1 {
        return Err(format!(
            "unsupported crash matrix version {}",
            matrix.version
        ));
    }
    if matrix.operations.is_empty() {
        return Err("crash matrix has no operations".into());
    }
    if matrix.invariants.is_empty() {
        return Err("crash matrix has no invariants".into());
    }
    let mut names = std::collections::HashSet::new();
    let mut ci_count = 0usize;
    for op in &matrix.operations {
        if op.id.is_empty() {
            return Err("operation with empty id".into());
        }
        if op.persistence_order.is_empty() {
            return Err(format!("operation {} missing persistence_order", op.id));
        }
        if op.failpoints.is_empty() {
            return Err(format!("operation {} has no failpoints", op.id));
        }
        for fp in &op.failpoints {
            if fp.name.is_empty() {
                return Err(format!("empty failpoint name under {}", op.id));
            }
            // Unique per (op, name, fault); same name may appear with different faults.
            let cell_key = format!(
                "{}|{}|{}",
                op.id,
                fp.name,
                fp.fault.as_deref().unwrap_or("")
            );
            if !names.insert(cell_key) {
                return Err(format!(
                    "duplicate matrix cell {} / {} / {:?}",
                    op.id, fp.name, fp.fault
                ));
            }
            if let Some(ref fault) = fp.fault {
                match fault.as_str() {
                    "enospc" | "permission" | "short_write" | "process_abort" | "panic"
                    | "error" => {}
                    other => {
                        return Err(format!(
                            "unknown fault class {other:?} on {} / {}",
                            op.id, fp.name
                        ));
                    }
                }
            }
            if fp.ci_subset {
                ci_count += 1;
            }
            if !fp.expected_on_reopen.no_fabricated_commit {
                return Err(format!(
                    "failpoint {} must keep no_fabricated_commit=true",
                    fp.name
                ));
            }
        }
    }
    if ci_count == 0 {
        return Err("crash matrix CI subset is empty".into());
    }
    Ok(())
}

/// All failpoints marked `ci_subset`.
pub fn ci_subset_cells(matrix: &CrashMatrix) -> Vec<(&MatrixOperation, &MatrixFailpoint)> {
    let mut out = Vec::new();
    for op in &matrix.operations {
        for fp in &op.failpoints {
            if fp.ci_subset {
                out.push((op, fp));
            }
        }
    }
    out
}

/// Every failpoint cell (nightly full matrix).
pub fn all_cells(matrix: &CrashMatrix) -> Vec<(&MatrixOperation, &MatrixFailpoint)> {
    let mut out = Vec::new();
    for op in &matrix.operations {
        for fp in &op.failpoints {
            out.push((op, fp));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_matrix_loads_and_validates() {
        let m = load_embedded().expect("load");
        validate(&m).expect("validate");
        assert!(!ci_subset_cells(&m).is_empty());
    }
}
