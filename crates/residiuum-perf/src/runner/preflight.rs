//! Preflight and environment validity classifier (plan §5).

use super::budget::{BudgetCheck, RunBudgets};
use super::fingerprint::{environment_fingerprint, EnvironmentFingerprint};
use super::marker::{ensure_work_root_marker, WorkRootMarker};
use super::path_guard::{classify_work_path, WorkPathClass};
use super::platform::detect_adapter;
use super::{BuildMode, RunnerError};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct PreflightConfig {
    pub work_root: PathBuf,
    pub work_root_id: String,
    pub run_id: String,
    pub repository_root: Option<PathBuf>,
    pub budgets: RunBudgets,
    pub build_mode: BuildMode,
    /// When true, treat `cfg!(debug_assertions)` as debug build.
    pub enforce_release_for_qualification: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PreflightOutcome {
    Ready,
    InvalidPath,
    InvalidMarker,
    InvalidBudget,
    InvalidDebugBuild,
    InvalidPreflight,
}

impl PreflightOutcome {
    pub fn validity_id(&self) -> &'static str {
        match self {
            Self::Ready => "valid", // preflight only; run validity assigned later
            Self::InvalidPath => "invalid_path",
            Self::InvalidMarker => "invalid_path",
            Self::InvalidBudget => "invalid_budget",
            Self::InvalidDebugBuild => "invalid_debug_build",
            Self::InvalidPreflight => "invalid_preflight",
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightReport {
    pub outcome: PreflightOutcome,
    pub validity_id: String,
    pub work_root: String,
    pub work_root_id: String,
    pub run_id: String,
    pub platform_adapter: String,
    pub build_mode: String,
    pub debug_assertions: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub marker: Option<WorkRootMarker>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<EnvironmentFingerprint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
    pub messages: Vec<String>,
}

/// Run full preflight: path guard → budgets → debug policy → marker → fingerprint.
pub fn preflight_work_root(cfg: &PreflightConfig) -> Result<PreflightReport, RunnerError> {
    let adapter = detect_adapter();
    let debug = cfg!(debug_assertions);

    let finish = |outcome: PreflightOutcome,
                  work_root: String,
                  reject: Option<String>,
                  messages: Vec<String>,
                  marker: Option<WorkRootMarker>,
                  environment: Option<EnvironmentFingerprint>| {
        PreflightReport {
            validity_id: outcome.validity_id().into(),
            outcome,
            work_root,
            work_root_id: cfg.work_root_id.clone(),
            run_id: cfg.run_id.clone(),
            platform_adapter: adapter.as_str().into(),
            build_mode: cfg.build_mode.as_str().into(),
            debug_assertions: debug,
            marker,
            environment,
            reject_reason: reject,
            messages,
        }
    };

    // Qualification must not use debug builds.
    if cfg.enforce_release_for_qualification
        && matches!(cfg.build_mode, BuildMode::Qualification)
        && debug
        && !cfg.build_mode.allows_debug()
    {
        return Ok(finish(
            PreflightOutcome::InvalidDebugBuild,
            cfg.work_root.display().to_string(),
            Some("debug_assertions".into()),
            vec!["debug build rejected for qualification".into()],
            None,
            None,
        ));
    }

    let class = classify_work_path(&cfg.work_root, cfg.repository_root.as_deref())?;
    let resolved = match class {
        WorkPathClass::Allowed { resolved } => resolved,
        WorkPathClass::Rejected { reason, path } => {
            return Ok(finish(
                PreflightOutcome::InvalidPath,
                path.display().to_string(),
                Some(reason.to_string()),
                vec![format!("path rejected: {reason} ({})", path.display())],
                None,
                None,
            ));
        }
    };

    // Live free-space floor.
    match cfg.budgets.check_live_free_space(&resolved)? {
        BudgetCheck::Ok => {}
        BudgetCheck::Exceeded { detail, .. } => {
            return Ok(finish(
                PreflightOutcome::InvalidBudget,
                resolved.display().to_string(),
                Some(detail.clone()),
                vec![detail],
                None,
                None,
            ));
        }
    }

    let marker = match ensure_work_root_marker(&resolved, &cfg.work_root_id, Some(&cfg.run_id)) {
        Ok(m) => m,
        Err(RunnerError::InvalidMarker(detail)) => {
            return Ok(finish(
                PreflightOutcome::InvalidMarker,
                resolved.display().to_string(),
                Some(detail.clone()),
                vec![detail],
                None,
                None,
            ));
        }
        Err(e) => return Err(e),
    };

    let env = environment_fingerprint(Some(&resolved), cfg.build_mode)?;
    Ok(finish(
        PreflightOutcome::Ready,
        resolved.display().to_string(),
        None,
        vec!["preflight ready".into()],
        Some(marker),
        Some(env),
    ))
}

/// Convenience: current process is a debug binary.
pub fn is_debug_build() -> bool {
    cfg!(debug_assertions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn diag_cfg(work: PathBuf) -> PreflightConfig {
        PreflightConfig {
            work_root: work,
            work_root_id: "test-wr".into(),
            run_id: "run-1".into(),
            repository_root: None,
            budgets: RunBudgets {
                // Low floor so CI/dev disks pass; dedicated floor tests elsewhere.
                free_space_floor_bytes: 1024,
                free_inode_floor: 1,
                ..RunBudgets::default()
            },
            build_mode: BuildMode::Diagnostic,
            enforce_release_for_qualification: true,
        }
    }

    #[test]
    fn preflight_ready_on_empty_temp() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("pqh");
        fs::create_dir_all(&work).unwrap();
        let report = preflight_work_root(&diag_cfg(work)).unwrap();
        assert!(report.outcome.is_ready());
        assert!(report.marker.is_some());
        assert!(report.environment.is_some());
    }

    #[test]
    fn preflight_rejects_root() {
        let report = preflight_work_root(&diag_cfg(PathBuf::from("/"))).unwrap();
        assert_eq!(report.outcome, PreflightOutcome::InvalidPath);
        assert_eq!(report.validity_id, "invalid_path");
    }

    #[test]
    fn preflight_rejects_foreign_non_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("pqh");
        fs::create_dir_all(&work).unwrap();
        fs::write(work.join("noise"), b"x").unwrap();
        let report = preflight_work_root(&diag_cfg(work)).unwrap();
        assert_eq!(report.outcome, PreflightOutcome::InvalidMarker);
    }

    #[test]
    fn qualification_debug_rejected_when_enforced() {
        if !cfg!(debug_assertions) {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("pqh");
        fs::create_dir_all(&work).unwrap();
        let mut cfg = diag_cfg(work);
        cfg.build_mode = BuildMode::Qualification;
        let report = preflight_work_root(&cfg).unwrap();
        assert_eq!(report.outcome, PreflightOutcome::InvalidDebugBuild);
        assert_eq!(report.validity_id, "invalid_debug_build");
    }
}
