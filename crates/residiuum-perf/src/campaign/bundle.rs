//! Hashed evidence bundle for independent verification.

use super::disclosure::DisclosureSummary;
use super::reports::CampaignReports;
use super::run::CampaignResult;
use super::CampaignError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

pub const BUNDLE_SCHEMA: &str = "residiuum-performance-evidence-bundle-v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceBundle {
    pub schema: String,
    pub campaign_id: String,
    pub profile: String,
    pub platform: String,
    pub allows_product_baseline: bool,
    pub result: CampaignResult,
    pub reports: CampaignReports,
    pub disclosure: DisclosureSummary,
    /// SHA-256 over ordered `relative_path\\0sha256\\n` of file_hashes.
    pub content_hash: String,
    pub file_hashes: Vec<FileHash>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileHash {
    pub relative_path: String,
    pub sha256_hex: String,
}

/// Write campaign evidence under `campaign_dir/` and return the bundle.
pub fn write_evidence_bundle(
    campaign_dir: &Path,
    result: &CampaignResult,
    reports: &CampaignReports,
    disclosure: &DisclosureSummary,
) -> Result<EvidenceBundle, CampaignError> {
    fs::create_dir_all(campaign_dir)?;
    fs::create_dir_all(campaign_dir.join("runs"))?;

    let plan_path = campaign_dir.join("plan.json");
    write_json(&plan_path, &result.plan)?;

    let result_path = campaign_dir.join("campaign_result.json");
    write_json(&result_path, result)?;

    let reports_path = campaign_dir.join("reports.json");
    write_json(&reports_path, reports)?;

    let disclosure_path = campaign_dir.join("disclosure.json");
    write_json(&disclosure_path, disclosure)?;

    let disclosure_md = super::disclosure::render_disclosure_markdown(disclosure, reports);
    fs::write(campaign_dir.join("DISCLOSURE.md"), disclosure_md)?;

    // Per-run summaries (bounded; no payloads)
    for rep in &result.repetitions {
        let run_dir = campaign_dir.join("runs").join(&rep.run_id);
        fs::create_dir_all(&run_dir)?;
        write_json(&run_dir.join("result.json"), &rep.report)?;
    }

    // Bind preflight + environment fingerprint into hashed evidence when present.
    let mut optional_hash_paths: Vec<&str> = Vec::new();
    if let Some(pf) = result.preflight_report.as_ref() {
        write_json(&campaign_dir.join("preflight.json"), pf)?;
        optional_hash_paths.push("preflight.json");
    }
    if let Some(env) = result.environment_fingerprint.as_ref() {
        write_json(&campaign_dir.join("environment.json"), env)?;
        optional_hash_paths.push("environment.json");
    }
    // Observer-overhead and boundary-aggregate markers from withdrawals/notes.
    if let Some(h) = result.environment_hash.as_ref() {
        write_json(
            &campaign_dir.join("environment_hash.json"),
            &serde_json::json!({
                "environment_hash": h,
                "preflight_outcome": result.preflight_outcome,
                "preflight_validity_id": result.preflight_validity_id,
            }),
        )?;
        optional_hash_paths.push("environment_hash.json");
    }
    if !result.observer_overhead_reports.is_empty() {
        // Per-cell overhead evidence (full array) — primary hashed artifact.
        write_json(
            &campaign_dir.join("observer_overhead.json"),
            &serde_json::json!({
                "schema": "residiuum-observer-overhead-v1",
                "cells": result.observer_overhead_reports,
                "all_within_budget": result.observer_overhead_reports.iter().all(|r| r.within_budget),
            }),
        )?;
        optional_hash_paths.push("observer_overhead.json");
    } else if let Some(oh) = result.observer_overhead_report.as_ref() {
        write_json(&campaign_dir.join("observer_overhead.json"), oh)?;
        optional_hash_paths.push("observer_overhead.json");
    }

    // Hash on-disk bytes (stable independent check — no re-serialize drift).
    let mut file_hashes = Vec::new();
    let mut hash_rels: Vec<&str> = vec![
        "plan.json",
        "campaign_result.json",
        "reports.json",
        "disclosure.json",
        "DISCLOSURE.md",
    ];
    hash_rels.extend(optional_hash_paths.iter().copied());
    for rel in hash_rels {
        let p = campaign_dir.join(rel);
        if !p.exists() {
            continue;
        }
        file_hashes.push(FileHash {
            relative_path: rel.into(),
            sha256_hex: hash_file(&p)?,
        });
    }
    let content_hash = hash_file_list(&file_hashes);

    let bundle = EvidenceBundle {
        schema: BUNDLE_SCHEMA.into(),
        campaign_id: result.plan.campaign_id.clone(),
        profile: result.plan.profile.clone(),
        platform: result.plan.platform.as_str().into(),
        allows_product_baseline: result.plan.platform.allows_product_baseline(),
        result: result.clone(),
        reports: reports.clone(),
        disclosure: disclosure.clone(),
        content_hash,
        file_hashes,
    };

    write_json(&campaign_dir.join("bundle.json"), &bundle)?;
    // hashes manifest
    write_json(
        &campaign_dir.join("hashes.json"),
        &serde_json::json!({
            "schema": "residiuum-performance-bundle-hashes-v1",
            "content_hash": bundle.content_hash,
            "files": bundle.file_hashes,
        }),
    )?;

    Ok(bundle)
}

/// Re-hash on-disk files and compare to bundle.file_hashes (independent check).
pub fn verify_bundle_hashes(campaign_dir: &Path) -> Result<(), CampaignError> {
    let raw = fs::read_to_string(campaign_dir.join("bundle.json"))
        .map_err(|e| CampaignError::Bundle(e.to_string()))?;
    let bundle: EvidenceBundle =
        serde_json::from_str(&raw).map_err(|e| CampaignError::Bundle(e.to_string()))?;

    if bundle.schema != BUNDLE_SCHEMA {
        return Err(CampaignError::Bundle(format!(
            "unexpected schema {}",
            bundle.schema
        )));
    }

    let mut recomputed_files = Vec::new();
    for fh in &bundle.file_hashes {
        let p = campaign_dir.join(&fh.relative_path);
        let got = hash_file(&p)?;
        if got != fh.sha256_hex {
            return Err(CampaignError::Bundle(format!(
                "hash mismatch for {}: expected {} got {}",
                fh.relative_path, fh.sha256_hex, got
            )));
        }
        recomputed_files.push(FileHash {
            relative_path: fh.relative_path.clone(),
            sha256_hex: got,
        });
    }

    let recomputed = hash_file_list(&recomputed_files);
    if recomputed != bundle.content_hash {
        return Err(CampaignError::Bundle(format!(
            "content_hash mismatch: expected {} got {}",
            bundle.content_hash, recomputed
        )));
    }
    Ok(())
}

fn hash_file_list(files: &[FileHash]) -> String {
    let mut hasher = Sha256::new();
    for fh in files {
        hasher.update(fh.relative_path.as_bytes());
        hasher.update(b"\0");
        hasher.update(fh.sha256_hex.as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

fn hash_file(path: &Path) -> Result<String, CampaignError> {
    let bytes = fs::read(path).map_err(|e| CampaignError::Io(e.to_string()))?;
    Ok(hex::encode(Sha256::digest(&bytes)))
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), CampaignError> {
    let s =
        serde_json::to_string_pretty(value).map_err(|e| CampaignError::Bundle(e.to_string()))?;
    fs::write(path, s)?;
    let _ = PathBuf::from(path);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::campaign::disclosure::build_disclosure;
    use crate::campaign::plan::campaign_plan_synthetic;
    use crate::campaign::reports::build_campaign_reports;
    use crate::campaign::run::{run_campaign, CampaignConfig};

    #[test]
    fn bundle_roundtrip_verifies() {
        let dir = tempfile::tempdir().unwrap();
        let plan = campaign_plan_synthetic(7, 2);
        let result = run_campaign(&CampaignConfig::synthetic(plan)).unwrap();
        let reports = build_campaign_reports(&result);
        let disclosure = build_disclosure(&result, &reports);
        let bundle = write_evidence_bundle(dir.path(), &result, &reports, &disclosure).unwrap();
        assert_eq!(bundle.schema, BUNDLE_SCHEMA);
        assert!(!bundle.content_hash.is_empty());
        verify_bundle_hashes(dir.path()).unwrap();
        // tamper
        fs::write(dir.path().join("plan.json"), b"{}").unwrap();
        assert!(verify_bundle_hashes(dir.path()).is_err());
    }

    #[test]
    fn bundle_hashes_include_environment_when_present() {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        fs::create_dir_all(&work).unwrap();
        let plan = campaign_plan_synthetic(11, 1);
        let result = run_campaign(&CampaignConfig {
            plan,
            driver: crate::store_driver::DriverKind::Synthetic,
            work_root: Some(work),
            declare_controlled_runner: false,
            run_class: crate::campaign::RunClass::Smoke,
            spawn_workers: false,
            worker_bin: None,
            require_qualification_preflight: false,
            awo_mode: crate::store_driver::AwoMode::Disabled,
            presentation: Default::default(),
        })
        .unwrap();
        assert!(result.environment_hash.is_some());
        let reports = build_campaign_reports(&result);
        let disclosure = build_disclosure(&result, &reports);
        let camp = dir.path().join("campaign");
        let bundle = write_evidence_bundle(&camp, &result, &reports, &disclosure).unwrap();
        assert!(
            bundle
                .file_hashes
                .iter()
                .any(|f| f.relative_path == "environment.json"
                    || f.relative_path == "environment_hash.json"),
            "environment must be bound into hashed evidence"
        );
        verify_bundle_hashes(&camp).unwrap();
    }

    /// Synthetic campaigns do not measure store probe overhead; report absent.
    #[test]
    fn synthetic_campaign_has_no_observer_overhead_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let plan = campaign_plan_synthetic(17, 1);
        let result = run_campaign(&CampaignConfig::synthetic(plan)).unwrap();
        assert!(result.observer_overhead_reports.is_empty());
        assert!(result.observer_overhead_report.is_none());
        let reports = build_campaign_reports(&result);
        let disclosure = build_disclosure(&result, &reports);
        let camp = dir.path().join("campaign");
        let bundle = write_evidence_bundle(&camp, &result, &reports, &disclosure).unwrap();
        assert!(bundle
            .file_hashes
            .iter()
            .all(|f| f.relative_path != "observer_overhead.json"));
    }
}
