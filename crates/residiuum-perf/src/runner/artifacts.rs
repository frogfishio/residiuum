//! Run directory / artifact writer and cancel → invalid partial (plan §5).

use super::fingerprint::EnvironmentFingerprint;
use super::marker::{safe_remove_under, verify_marker};
use super::preflight::PreflightReport;
use super::RunnerError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

/// On-disk layout under `{work_root}/runs/{run_id}/`.
#[derive(Debug, Clone)]
pub struct RunArtifactLayout {
    pub run_dir: PathBuf,
    pub manifest: PathBuf,
    pub result: PathBuf,
    pub environment: PathBuf,
    pub hashes: PathBuf,
    pub attachments: PathBuf,
}

impl RunArtifactLayout {
    pub fn for_run(work_root: &Path, run_id: &str) -> Self {
        let run_dir = work_root.join("runs").join(run_id);
        Self {
            manifest: run_dir.join("manifest.json"),
            result: run_dir.join("result.json"),
            environment: run_dir.join("environment.json"),
            hashes: run_dir.join("hashes.json"),
            attachments: run_dir.join("attachments"),
            run_dir,
        }
    }

    pub fn ensure_dirs(&self) -> Result<(), RunnerError> {
        fs::create_dir_all(&self.run_dir)?;
        fs::create_dir_all(&self.attachments)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialResult {
    pub schema: String,
    pub profile: String,
    pub run_id: String,
    pub validity: String,
    pub messages: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preflight_outcome: Option<String>,
}

/// Write core run artifacts after a successful preflight (scaffold for later PQH packages).
pub fn write_run_artifacts(
    work_root: &Path,
    work_root_id: &str,
    run_id: &str,
    preflight: &PreflightReport,
    environment: Option<&EnvironmentFingerprint>,
) -> Result<RunArtifactLayout, RunnerError> {
    verify_marker(work_root, Some(work_root_id))?;
    let layout = RunArtifactLayout::for_run(work_root, run_id);
    layout.ensure_dirs()?;

    let manifest = json!({
        "schema": "residiuum-performance-manifest-v1",
        "profile": crate::PROFILE_ID,
        "run_id": run_id,
        "work_root_id": work_root_id,
        "platform_adapter": preflight.platform_adapter,
        "build_mode": preflight.build_mode,
        "preflight_outcome": format!("{:?}", preflight.outcome).to_ascii_lowercase(),
        "validity_hint": preflight.validity_id,
    });
    write_json_atomic(&layout.manifest, &manifest)?;

    if let Some(env) = environment.or(preflight.environment.as_ref()) {
        write_json_atomic(&layout.environment, env)?;
    }

    let result = json!({
        "schema": "residiuum-performance-result-v1",
        "profile": crate::PROFILE_ID,
        "run_id": run_id,
        "validity": "valid",
        "messages": ["artifact scaffold; measurement layers land in later PQH packages"],
    });
    write_json_atomic(&layout.result, &result)?;

    write_hashes(&layout)?;
    Ok(layout)
}

/// Cancellation / crash residue: classifiable `invalid_partial_cancelled` artifact.
pub fn write_cancel_partial(
    work_root: &Path,
    work_root_id: &str,
    run_id: &str,
    messages: Vec<String>,
) -> Result<RunArtifactLayout, RunnerError> {
    verify_marker(work_root, Some(work_root_id))?;
    let layout = RunArtifactLayout::for_run(work_root, run_id);
    layout.ensure_dirs()?;

    let partial = PartialResult {
        schema: "residiuum-performance-result-v1".into(),
        profile: crate::PROFILE_ID.into(),
        run_id: run_id.into(),
        validity: "invalid_partial_cancelled".into(),
        messages,
        preflight_outcome: None,
    };
    write_json_atomic(&layout.result, &partial)?;

    let manifest = json!({
        "schema": "residiuum-performance-manifest-v1",
        "profile": crate::PROFILE_ID,
        "run_id": run_id,
        "work_root_id": work_root_id,
        "cancelled": true,
    });
    write_json_atomic(&layout.manifest, &manifest)?;
    write_hashes(&layout)?;
    Ok(layout)
}

/// Remove a prior run directory only when the work-root marker matches.
pub fn remove_run_dir(
    work_root: &Path,
    work_root_id: &str,
    run_id: &str,
) -> Result<(), RunnerError> {
    let rel = Path::new("runs").join(run_id);
    safe_remove_under(work_root, &rel, work_root_id)
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), RunnerError> {
    let parent = path.parent().ok_or_else(|| {
        RunnerError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "artifact path has no parent",
        ))
    })?;
    fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(
        ".{}.tmp",
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("artifact")
    ));
    let body = serde_json::to_vec_pretty(value)?;
    fs::write(&tmp, body)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn write_hashes(layout: &RunArtifactLayout) -> Result<(), RunnerError> {
    let mut map = serde_json::Map::new();
    for (name, path) in [
        ("manifest.json", &layout.manifest),
        ("result.json", &layout.result),
        ("environment.json", &layout.environment),
    ] {
        if path.exists() {
            let bytes = fs::read(path)?;
            let digest = Sha256::digest(&bytes);
            map.insert(name.to_string(), json!(hex::encode(digest)));
        }
    }
    write_json_atomic(&layout.hashes, &map)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::budget::RunBudgets;
    use crate::runner::marker::ensure_work_root_marker;
    use crate::runner::preflight::{preflight_work_root, PreflightConfig};
    use crate::runner::BuildMode;

    #[test]
    fn cancel_leaves_classifiable_artifact() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("pqh");
        fs::create_dir_all(&work).unwrap();
        ensure_work_root_marker(&work, "wr", Some("run-x")).unwrap();
        let layout = write_cancel_partial(
            &work,
            "wr",
            "run-x",
            vec!["SIGINT".into(), "operator cancel".into()],
        )
        .unwrap();
        assert!(layout.result.exists());
        let raw = fs::read_to_string(&layout.result).unwrap();
        assert!(raw.contains("invalid_partial_cancelled"));
        assert!(layout.hashes.exists());
    }

    #[test]
    fn write_artifacts_after_preflight() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("pqh");
        fs::create_dir_all(&work).unwrap();
        let cfg = PreflightConfig {
            work_root: work.clone(),
            work_root_id: "wr".into(),
            run_id: "run-ok".into(),
            repository_root: None,
            budgets: RunBudgets {
                free_space_floor_bytes: 1024,
                free_inode_floor: 1,
                ..RunBudgets::default()
            },
            build_mode: BuildMode::Diagnostic,
            enforce_release_for_qualification: true,
        };
        let report = preflight_work_root(&cfg).unwrap();
        assert!(report.outcome.is_ready());
        let layout = write_run_artifacts(&work, "wr", "run-ok", &report, None).unwrap();
        assert!(layout.manifest.exists());
        assert!(layout.result.exists());
        assert!(layout.environment.exists());
        assert!(layout.hashes.exists());
    }

    #[test]
    fn cannot_remove_run_without_matching_marker() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("pqh");
        fs::create_dir_all(&work).unwrap();
        ensure_work_root_marker(&work, "wr", None).unwrap();
        let layout = RunArtifactLayout::for_run(&work, "r1");
        layout.ensure_dirs().unwrap();
        let err = remove_run_dir(&work, "WRONG", "r1").unwrap_err();
        assert!(matches!(err, RunnerError::InvalidMarker(_)));
        assert!(layout.run_dir.exists());
    }
}
