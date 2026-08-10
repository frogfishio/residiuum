//! Dedicated work-root marker protocol (SPEC §4).

use super::RunnerError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const MARKER_FILE_NAME: &str = ".residiuum-perf-work-root";
pub const MARKER_SCHEMA: &str = "residiuum-perf-work-root-v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkRootMarker {
    pub schema: String,
    /// Stable id for this dedicated work root (not a run id).
    pub work_root_id: String,
    /// Optional last run id that claimed the root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub created_unix: u64,
    pub updated_unix: u64,
}

pub fn marker_path(work_root: &Path) -> PathBuf {
    work_root.join(MARKER_FILE_NAME)
}

pub fn read_marker(work_root: &Path) -> Result<WorkRootMarker, RunnerError> {
    let path = marker_path(work_root);
    let raw = fs::read_to_string(&path)
        .map_err(|e| RunnerError::InvalidMarker(format!("read {}: {e}", path.display())))?;
    let m: WorkRootMarker = serde_json::from_str(&raw)?;
    if m.schema != MARKER_SCHEMA {
        return Err(RunnerError::InvalidMarker(format!(
            "unsupported marker schema {}",
            m.schema
        )));
    }
    if m.work_root_id.is_empty() {
        return Err(RunnerError::InvalidMarker(
            "marker missing work_root_id".into(),
        ));
    }
    Ok(m)
}

pub fn verify_marker(
    work_root: &Path,
    expected_work_root_id: Option<&str>,
) -> Result<WorkRootMarker, RunnerError> {
    let m = read_marker(work_root)?;
    if let Some(exp) = expected_work_root_id {
        if m.work_root_id != exp {
            return Err(RunnerError::InvalidMarker(format!(
                "stale or wrong marker id: got {}, expected {exp}",
                m.work_root_id
            )));
        }
    }
    Ok(m)
}

/// Ensure `work_root` is a dedicated root: empty (create marker) or already
/// marked with a valid marker. Foreign non-empty directories are rejected.
pub fn ensure_work_root_marker(
    work_root: &Path,
    work_root_id: &str,
    run_id: Option<&str>,
) -> Result<WorkRootMarker, RunnerError> {
    if work_root_id.is_empty() {
        return Err(RunnerError::InvalidMarker(
            "work_root_id must be non-empty".into(),
        ));
    }
    fs::create_dir_all(work_root)?;
    let marker = marker_path(work_root);
    if marker.exists() {
        let existing = verify_marker(work_root, Some(work_root_id))?;
        // Touch run_id / updated_unix.
        let now = unix_now();
        let updated = WorkRootMarker {
            schema: existing.schema,
            work_root_id: existing.work_root_id,
            run_id: run_id.map(|s| s.to_string()).or(existing.run_id),
            created_unix: existing.created_unix,
            updated_unix: now,
        };
        write_marker_atomic(work_root, &updated)?;
        return Ok(updated);
    }

    // No marker: directory must be empty (ignore `.` / `..` only).
    let mut entries = fs::read_dir(work_root)?;
    if entries.next().is_some() {
        return Err(RunnerError::InvalidMarker(format!(
            "non-empty foreign directory without marker: {}",
            work_root.display()
        )));
    }

    let now = unix_now();
    let m = WorkRootMarker {
        schema: MARKER_SCHEMA.to_string(),
        work_root_id: work_root_id.to_string(),
        run_id: run_id.map(|s| s.to_string()),
        created_unix: now,
        updated_unix: now,
    };
    write_marker_atomic(work_root, &m)?;
    Ok(m)
}

/// Delete only a child path under `work_root` when the root still carries the
/// expected marker. Prevents filling/deleting unowned trees.
pub fn safe_remove_under(
    work_root: &Path,
    relative: &Path,
    expected_work_root_id: &str,
) -> Result<(), RunnerError> {
    verify_marker(work_root, Some(expected_work_root_id))?;
    if relative.is_absolute() {
        return Err(RunnerError::InvalidPath(
            "safe_remove_under requires relative child path".into(),
        ));
    }
    if relative
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(RunnerError::InvalidPath(
            "safe_remove_under rejects .. components".into(),
        ));
    }
    let target = work_root.join(relative);
    let target_resolved = if target.exists() {
        fs::canonicalize(&target)?
    } else {
        return Ok(());
    };
    let root_resolved = fs::canonicalize(work_root)?;
    if !target_resolved.starts_with(&root_resolved) {
        return Err(RunnerError::InvalidPath(format!(
            "refusing to delete outside work root: {}",
            target_resolved.display()
        )));
    }
    // Never delete the marker itself via this API.
    if target_resolved == root_resolved.join(MARKER_FILE_NAME) {
        return Err(RunnerError::InvalidPath(
            "refusing to delete work-root marker via safe_remove_under".into(),
        ));
    }
    if target_resolved.is_dir() {
        fs::remove_dir_all(&target_resolved)?;
    } else {
        fs::remove_file(&target_resolved)?;
    }
    Ok(())
}

fn write_marker_atomic(work_root: &Path, marker: &WorkRootMarker) -> Result<(), RunnerError> {
    let path = marker_path(work_root);
    let tmp = work_root.join(format!("{MARKER_FILE_NAME}.tmp"));
    let body = serde_json::to_string_pretty(marker)?;
    fs::write(&tmp, body)?;
    fs::rename(&tmp, &path)?;
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn creates_marker_on_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("work");
        fs::create_dir_all(&root).unwrap();
        let m = ensure_work_root_marker(&root, "wr-1", Some("run-a")).unwrap();
        assert_eq!(m.work_root_id, "wr-1");
        assert_eq!(m.run_id.as_deref(), Some("run-a"));
        let again = verify_marker(&root, Some("wr-1")).unwrap();
        assert_eq!(again.work_root_id, "wr-1");
    }

    #[test]
    fn rejects_foreign_non_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("work");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("foreign.txt"), b"nope").unwrap();
        let err = ensure_work_root_marker(&root, "wr-1", None).unwrap_err();
        assert!(matches!(err, RunnerError::InvalidMarker(_)));
    }

    #[test]
    fn rejects_wrong_marker_id() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("work");
        fs::create_dir_all(&root).unwrap();
        ensure_work_root_marker(&root, "wr-1", None).unwrap();
        let err = ensure_work_root_marker(&root, "wr-OTHER", None).unwrap_err();
        assert!(matches!(err, RunnerError::InvalidMarker(_)));
    }

    #[test]
    fn safe_remove_requires_marker_and_relative() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("work");
        fs::create_dir_all(&root).unwrap();
        ensure_work_root_marker(&root, "wr-1", None).unwrap();
        let child = root.join("runs").join("r1");
        fs::create_dir_all(&child).unwrap();
        fs::write(child.join("x"), b"1").unwrap();
        safe_remove_under(&root, Path::new("runs/r1"), "wr-1").unwrap();
        assert!(!child.exists());
        // Absolute child rejected.
        assert!(safe_remove_under(&root, Path::new("/tmp"), "wr-1").is_err());
    }
}
