//! Environment fingerprint + evidence bundle writer (Q0 env + programme §7.4).
//!
//! Competitive campaigns must not run on a dirty tree without principal waiver.

use crate::canonicalize::CanonicalResult;
use crate::engine::EngineRunOutcome;
use crate::lane::{EngineId, LaneId, LanePairing};
use crate::metrics::CellMetrics;
use crate::{ENV_FINGERPRINT_SCHEMA, EVIDENCE_BUNDLE_SCHEMA, HARNESS_PROFILE};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum EvidenceError {
    #[error("io: {0}")]
    Io(String),
    #[error("json: {0}")]
    Json(String),
    #[error("fingerprint: {0}")]
    Fingerprint(String),
}

/// Evidence intent. Scaffold bundles can exercise structure but can never be
/// accepted as competitive campaign evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignClass {
    Scaffold,
    Qualification,
}

/// Frozen execution protocol carried by every evidence bundle (F14).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignProtocol {
    pub class: CampaignClass,
    pub seed: u64,
    pub warmup_operations: u64,
    pub minimum_repetitions: u32,
    pub minimum_duration_ms: u64,
    pub minimum_operations: u64,
    pub engine_order_policy: String,
}

impl CampaignProtocol {
    pub fn scaffold(seed: u64) -> Self {
        Self {
            class: CampaignClass::Scaffold,
            seed,
            warmup_operations: 0,
            minimum_repetitions: 1,
            minimum_duration_ms: 0,
            minimum_operations: 1,
            engine_order_policy: "fixed_scaffold_not_competitive".into(),
        }
    }

    pub fn q5(seed: u64, minimum_duration_ms: u64, minimum_operations: u64) -> Self {
        Self {
            class: CampaignClass::Qualification,
            seed,
            warmup_operations: minimum_operations,
            minimum_repetitions: 7,
            minimum_duration_ms,
            minimum_operations,
            engine_order_policy: "alternating_seeded_randomized".into(),
        }
    }
}

/// One raw engine repetition. Aggregates never replace these records.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawRepetition {
    pub repetition: u32,
    pub engine: EngineId,
    pub operations: u64,
    pub duration_ns: u64,
    pub valid: bool,
    pub result_digest: String,
    pub query_hash: String,
    pub qvm_hash: Option<String>,
    pub index_config_hash: String,
    pub cache_state: String,
}

/// Q0-aligned environment fingerprint for evidence bundles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvFingerprint {
    pub format: String,
    pub harness_profile: String,
    pub git_sha: String,
    pub dirty: bool,
    pub residiuum_version: String,
    pub rustc: Option<String>,
    pub os: String,
    pub arch: String,
    pub hostname_class: String,
    pub recorded_unix_s: u64,
    /// Comparator pins (from Q0 manifest — recorded as strings, not claimed installed).
    pub mongo_pin: String,
    pub cbl_pin: String,
    pub cbl_full_sync: bool,
    /// Named Residiuum query defaults (Q0.A13).
    pub query_defaults: QueryDefaultsPin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDefaultsPin {
    pub consistency_mode: String,
    pub coverage_policy: String,
    pub page_size: u32,
}

impl Default for QueryDefaultsPin {
    fn default() -> Self {
        Self {
            consistency_mode: "Available".into(),
            coverage_policy: "Complete".into(),
            page_size: 64,
        }
    }
}

impl EnvFingerprint {
    /// Capture host fingerprint. `dirty` from `git status --porcelain`.
    pub fn capture(workspace_root: &Path) -> Result<Self, EvidenceError> {
        let git_sha = git_stdout(workspace_root, &["rev-parse", "HEAD"])
            .unwrap_or_else(|_| "0000000000000000000000000000000000000000".into());
        let porcelain = git_stdout(workspace_root, &["status", "--porcelain"]).unwrap_or_default();
        let dirty = !porcelain.trim().is_empty();
        let rustc = Command::new("rustc")
            .arg("--version")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string());
        let version = fs::read_to_string(workspace_root.join("VERSION"))
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|_| "0.2.2".into());
        let unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Ok(Self {
            format: ENV_FINGERPRINT_SCHEMA.into(),
            harness_profile: HARNESS_PROFILE.into(),
            git_sha: git_sha.trim().into(),
            dirty,
            residiuum_version: version,
            rustc,
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            hostname_class: "controlled_host_unspecified".into(),
            recorded_unix_s: unix,
            mongo_pin: "8.2.12".into(),
            cbl_pin: "4.1.0".into(),
            cbl_full_sync: true,
            query_defaults: QueryDefaultsPin::default(),
        })
    }

    /// Qualification campaigns refuse dirty trees unless waiver.
    pub fn assert_clean_for_campaign(&self, dirty_waiver: bool) -> Result<(), EvidenceError> {
        if self.dirty && !dirty_waiver {
            return Err(EvidenceError::Fingerprint(
                "dirty tree forbidden for qualification campaigns (set dirty_waiver or commit)"
                    .into(),
            ));
        }
        Ok(())
    }
}

fn git_stdout(root: &Path, args: &[&str]) -> Result<String, EvidenceError> {
    let out = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .map_err(|e| EvidenceError::Io(e.to_string()))?;
    if !out.status.success() {
        return Err(EvidenceError::Io(format!(
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        )));
    }
    String::from_utf8(out.stdout).map_err(|e| EvidenceError::Io(e.to_string()))
}

/// One cell / case measurement record inside a bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellEvidence {
    pub cell_id: String,
    pub case_id: Option<String>,
    pub lane: LaneId,
    pub pairing: LanePairing,
    /// Plan-requested concurrency (§7.2 matrix).
    pub concurrency: u32,
    /// Peak simultaneous workers observed at execution (F10). Equals `concurrency`
    /// when real multi-worker path ran; must not stay at 1 when plan asks for >1.
    #[serde(default = "default_achieved_concurrency")]
    pub achieved_concurrency: u32,
    pub side_a: EngineRunOutcome,
    pub side_b: EngineRunOutcome,
    pub equivalent: Option<bool>,
    pub equivalence_detail: Option<String>,
    pub metrics_a: Option<CellMetrics>,
    pub metrics_b: Option<CellMetrics>,
    /// Raw repetitions from which aggregate metrics were derived (F14/Q5).
    #[serde(default)]
    pub raw_repetitions: Vec<RawRepetition>,
    /// True when this record is structural scaffold only (not competitive).
    pub scaffold_only: bool,
}

fn default_achieved_concurrency() -> u32 {
    1
}

/// Top-level evidence bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceBundle {
    pub format: String,
    pub harness_profile: String,
    pub env: EnvFingerprint,
    pub campaign_id: String,
    pub protocol: CampaignProtocol,
    pub notes: Vec<String>,
    pub cells: Vec<CellEvidence>,
    /// Content hash of the bundle body without this field (filled on write).
    pub content_hash: Option<String>,
}

impl EvidenceBundle {
    pub fn new(env: EnvFingerprint, campaign_id: impl Into<String>) -> Self {
        Self {
            format: EVIDENCE_BUNDLE_SCHEMA.into(),
            harness_profile: HARNESS_PROFILE.into(),
            env,
            campaign_id: campaign_id.into(),
            protocol: CampaignProtocol::scaffold(0),
            notes: vec![
                "Q4.1 scaffold — not Gate-1; not competitive".into(),
                "Mongo/CBL adapters not configured until Q4.3".into(),
            ],
            cells: Vec::new(),
            content_hash: None,
        }
    }

    pub fn push_cell(&mut self, cell: CellEvidence) {
        self.cells.push(cell);
    }

    /// Fail closed unless the bundle has the campaign fields and raw evidence
    /// required by programme §8. Scaffold bundles always fail this check.
    pub fn validate_qualification_ready(&self) -> Result<(), EvidenceError> {
        if self.protocol.class != CampaignClass::Qualification {
            return Err(EvidenceError::Fingerprint(
                "scaffold bundle is not qualification evidence".into(),
            ));
        }
        if self.protocol.minimum_repetitions < 7
            || self.protocol.minimum_duration_ms == 0
            || self.protocol.minimum_operations == 0
            || self.protocol.warmup_operations == 0
            || self.protocol.engine_order_policy != "alternating_seeded_randomized"
        {
            return Err(EvidenceError::Fingerprint(
                "qualification protocol below Q5 floors".into(),
            ));
        }
        if self.cells.is_empty() {
            return Err(EvidenceError::Fingerprint(
                "qualification bundle has no cells".into(),
            ));
        }
        for cell in &self.cells {
            if cell.scaffold_only {
                return Err(EvidenceError::Fingerprint(format!(
                    "cell {} is scaffold-only",
                    cell.cell_id
                )));
            }
            let valid = cell.raw_repetitions.iter().filter(|r| r.valid).count();
            if valid < self.protocol.minimum_repetitions as usize {
                return Err(EvidenceError::Fingerprint(format!(
                    "cell {} has {valid} valid repetitions; need {}",
                    cell.cell_id, self.protocol.minimum_repetitions
                )));
            }
            if cell.raw_repetitions.iter().any(|r| {
                r.operations < self.protocol.minimum_operations
                    || r.duration_ns < self.protocol.minimum_duration_ms.saturating_mul(1_000_000)
                    || r.result_digest.is_empty()
                    || r.query_hash.is_empty()
                    || r.index_config_hash.is_empty()
                    || r.cache_state.is_empty()
            }) {
                return Err(EvidenceError::Fingerprint(format!(
                    "cell {} has incomplete/below-floor raw repetition",
                    cell.cell_id
                )));
            }
        }
        Ok(())
    }

    /// Serialize, hash, write JSON to path.
    pub fn write_json(&mut self, path: impl AsRef<Path>) -> Result<(), EvidenceError> {
        self.content_hash = None;
        let mut body =
            serde_json::to_value(&*self).map_err(|e| EvidenceError::Json(e.to_string()))?;
        if let Some(obj) = body.as_object_mut() {
            obj.remove("content_hash");
        }
        let bytes = serde_json::to_vec(&body).map_err(|e| EvidenceError::Json(e.to_string()))?;
        let mut h = Sha256::new();
        h.update(&bytes);
        let hash = hex::encode(h.finalize());
        self.content_hash = Some(hash);
        let pretty =
            serde_json::to_string_pretty(self).map_err(|e| EvidenceError::Json(e.to_string()))?;
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).map_err(|e| EvidenceError::Io(e.to_string()))?;
        }
        fs::write(path, pretty).map_err(|e| EvidenceError::Io(e.to_string()))?;
        Ok(())
    }
}

/// Compare two ready outcomes under Q0 dimensions (scaffold helper).
pub fn compare_ready_outcomes(
    a: &EngineRunOutcome,
    b: &EngineRunOutcome,
) -> (Option<bool>, Option<String>) {
    match (&a.result, &b.result) {
        (Some(ra), Some(rb)) => match crate::canonicalize::results_equivalent(ra, rb) {
            Ok(()) => (Some(true), None),
            Err(e) => (Some(false), Some(e)),
        },
        _ => (
            None,
            Some("one or both sides missing canonical result".into()),
        ),
    }
}

/// Build a scaffold cell evidence row (not competitive). Concurrency defaults to 1.
pub fn scaffold_cell(
    cell_id: &str,
    pairing: LanePairing,
    side_a: EngineRunOutcome,
    side_b: EngineRunOutcome,
) -> CellEvidence {
    scaffold_cell_with_concurrency(cell_id, pairing, side_a, side_b, 1, 1)
}

/// Scaffold cell with explicit requested + achieved concurrency (F10).
pub fn scaffold_cell_with_concurrency(
    cell_id: &str,
    pairing: LanePairing,
    side_a: EngineRunOutcome,
    side_b: EngineRunOutcome,
    requested_concurrency: u32,
    achieved_concurrency: u32,
) -> CellEvidence {
    let (equivalent, equivalence_detail) = compare_ready_outcomes(&side_a, &side_b);
    CellEvidence {
        cell_id: cell_id.into(),
        case_id: None,
        lane: pairing.lane,
        pairing,
        concurrency: requested_concurrency.max(1),
        achieved_concurrency: achieved_concurrency.max(1),
        side_a,
        side_b,
        equivalent,
        equivalence_detail,
        metrics_a: None,
        metrics_b: None,
        raw_repetitions: Vec::new(),
        scaffold_only: true,
    }
}

/// Env flag: when truthy, also write checked-in `spec/` evidence snapshots (F8).
/// Default is off — tests/verify write under `target/rql-q4/` only.
pub fn write_spec_evidence_enabled() -> bool {
    match std::env::var("RESIDIUUM_WRITE_SPEC_EVIDENCE") {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => false,
    }
}

/// Always write `body` to `target_path`. Optionally also to `spec_path` when
/// [`write_spec_evidence_enabled`].
pub fn write_evidence_artifact(
    target_path: impl AsRef<Path>,
    spec_path: impl AsRef<Path>,
    body: &str,
) -> Result<PathBuf, EvidenceError> {
    let target_path = target_path.as_ref();
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent).map_err(|e| EvidenceError::Io(e.to_string()))?;
    }
    fs::write(target_path, body).map_err(|e| EvidenceError::Io(e.to_string()))?;
    if write_spec_evidence_enabled() {
        let spec_path = spec_path.as_ref();
        if let Some(parent) = spec_path.parent() {
            fs::create_dir_all(parent).map_err(|e| EvidenceError::Io(e.to_string()))?;
        }
        fs::write(spec_path, body).map_err(|e| EvidenceError::Io(e.to_string()))?;
    }
    Ok(target_path.to_path_buf())
}

/// Minimal machine-readable architecture report for verify script.
pub fn write_architecture_report(path: impl AsRef<Path>) -> Result<Value, EvidenceError> {
    let report = json!({
        "format": "residiuum-rql-q4-1-architecture-report-v1",
        "harness_profile": HARNESS_PROFILE,
        "evidence_bundle_schema": EVIDENCE_BUNDLE_SCHEMA,
        "env_fingerprint_schema": ENV_FINGERPRINT_SCHEMA,
        "lanes": ["embedded", "local_client_server"],
        "engines": [
            EngineId::ResidiuumEmbedded.as_str(),
            EngineId::ResidiuumServer.as_str(),
            EngineId::MongoLocal.as_str(),
            EngineId::CouchbaseLiteEmbedded.as_str(),
            EngineId::LogicalHarness.as_str(),
        ],
        "modules": [
            "lane", "fixture", "engine", "canonicalize", "metrics", "evidence", "cells"
        ],
        "mandatory_cells": 12,
        "crate": "residiuum-rql-qual",
        "non_claims": [
            "not_gate1",
            "not_competitive",
            "mongo_cbl_stubs",
            "q4_package_not_accepted"
        ],
        "authority": "doc/todo/rql/RQL_Q4_1_HARNESS_ARCHITECTURE.md",
    });
    if let Some(parent) = path.as_ref().parent() {
        fs::create_dir_all(parent).map_err(|e| EvidenceError::Io(e.to_string()))?;
    }
    let pretty =
        serde_json::to_string_pretty(&report).map_err(|e| EvidenceError::Json(e.to_string()))?;
    fs::write(path, pretty).map_err(|e| EvidenceError::Io(e.to_string()))?;
    Ok(report)
}

// re-export for callers that only need the type name in docs
pub type _CanonicalResultAlias = CanonicalResult;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{synthetic_ready_outcome, EngineAdapter};
    use crate::fixture::CorpusCaseHandle;
    use crate::lane::LanePairing;
    use std::path::PathBuf;

    fn workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap()
    }

    #[test]
    fn fingerprint_capture_and_bundle_write() {
        let root = workspace_root();
        let env = EnvFingerprint::capture(&root).expect("fingerprint");
        assert_eq!(env.format, ENV_FINGERPRINT_SCHEMA);
        assert_eq!(env.mongo_pin, "8.2.12");
        assert_eq!(env.cbl_pin, "4.1.0");
        assert!(env.cbl_full_sync);
        assert_eq!(env.query_defaults.page_size, 64);

        let mut bundle = EvidenceBundle::new(env, "q4-1-scaffold");
        let mut mongo = crate::engine::MongoLocalAdapter::default();
        let case = CorpusCaseHandle {
            case_id: "x".into(),
            tier: "A".into(),
            domain: "t".into(),
            plain_english_intent: None,
            generator_id: None,
            seed: None,
            rql_source: None,
            server_lane_ineligible: false,
            lane_hint: None,
        };
        let side_b = mongo.execute_case(&case).unwrap();
        let side_a = synthetic_ready_outcome(crate::lane::EngineId::ResidiuumEmbedded, "aaa");
        bundle.push_cell(scaffold_cell(
            "cell_key_get",
            LanePairing::EMBEDDED,
            side_a,
            side_b,
        ));

        let out = root.join("target/rql-q4/q4_1_scaffold_bundle.json");
        bundle.write_json(&out).expect("write");
        assert!(out.is_file());
        assert!(bundle.content_hash.is_some());

        // F8: default → target/; RESIDIUUM_WRITE_SPEC_EVIDENCE=1 also writes spec/.
        let target = root.join("target/rql-q4/q4_1_architecture_report.json");
        let spec = root.join("spec/rql/qualification/harness-v1/q4_1_architecture_report.json");
        let report = write_architecture_report(&target).expect("arch report");
        assert!(target.is_file());
        assert_eq!(
            report["format"],
            "residiuum-rql-q4-1-architecture-report-v1"
        );
        if write_spec_evidence_enabled() {
            write_architecture_report(&spec).expect("arch report spec");
            assert!(spec.is_file());
        }
    }

    #[test]
    fn dirty_campaign_guard() {
        let mut env = EnvFingerprint::capture(&workspace_root()).unwrap();
        env.dirty = true;
        assert!(env.assert_clean_for_campaign(false).is_err());
        assert!(env.assert_clean_for_campaign(true).is_ok());
    }

    #[test]
    fn scaffold_bundle_cannot_validate_as_q5_evidence() {
        let env = EnvFingerprint::capture(&workspace_root()).unwrap();
        let bundle = EvidenceBundle::new(env, "scaffold");
        assert!(bundle.validate_qualification_ready().is_err());
    }

    #[test]
    fn q5_protocol_requires_cells_and_raw_repetitions() {
        let env = EnvFingerprint::capture(&workspace_root()).unwrap();
        let mut bundle = EvidenceBundle::new(env, "q5-empty");
        bundle.protocol = CampaignProtocol::q5(7, 1_000, 100);
        assert!(bundle.validate_qualification_ready().is_err());

        let side_a = synthetic_ready_outcome(EngineId::ResidiuumEmbedded, "a");
        let side_b = synthetic_ready_outcome(EngineId::CouchbaseLiteEmbedded, "a");
        let mut cell = scaffold_cell("cell_key_get", LanePairing::EMBEDDED, side_a, side_b);
        cell.scaffold_only = false;
        bundle.push_cell(cell);
        assert!(bundle.validate_qualification_ready().is_err());
    }
}
