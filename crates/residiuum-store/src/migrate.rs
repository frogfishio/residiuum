//! Format migration with explicit phases (DEF-052).
//!
//! Migration is always **source-preserving**: segments are never rewritten
//! in place. Work writes into a new destination store (append/copy), records
//! a durable job under `recovery/migration/`, and only marks success after
//! verify. Failed or abandoned apply leaves the source fully readable and
//! allows [`rollback_migration`] to remove an incomplete destination.
//!
//! Profile: [`MIGRATE_PROFILE`].
//!
//! Phases:
//! 1. **preflight** — version matrix, segment classification, dest empty
//! 2. **plan** — durable file actions (copy / preserve unsupported / unreadable)
//! 3. **apply** — evidence-preserving copy into destination
//! 4. **verify** — open dest, check live parity / content hashes
//! 5. **rollback** — delete incomplete dest; source untouched

use crate::error::StoreError;
use crate::ids::{hex16, random_id};
use crate::layout::StorePaths;
use crate::tier::{classify_segment_bytes, FormatClassification};
use residiuum_format::{
    wire_compat_matrix, wire_reader_supports, wire_support_summary, WIRE_PROFILE_LABEL,
    WRITER_WIRE_MAJOR, WRITER_WIRE_MINOR,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Profile tag for migration job documents (DEF-052).
pub const MIGRATE_PROFILE: &str = "residiuum-migrate-v1";

/// Directory under `recovery/` for migration job control (source-side audit).
pub const MIGRATE_DIR: &str = "migration";

/// Durable migration job filename.
pub const MIGRATE_JOB_FILE: &str = "job.v1.json";

/// Authoritative trees copied during format migration (same as backup).
const AUTHORITATIVE_TOP_LEVEL: &[&str] = &[
    "store-info",
    "active",
    "segments",
    "chunks",
    "recovery",
    "tiers",
];

/// Filenames under `store-info/` that must not be copied (runtime locks).
const SKIP_STORE_INFO_NAMES: &[&str] = &["writer.lock", "writer.lock.pid"];

/// Schema version for migration job documents.
const DOC_VERSION: u32 = 1;

/// Declared network protocol major this product line speaks (mirrors
/// `residiuum-client::PROTOCOL_MAJOR`; kept here so the store crate does not
/// depend on the client).
pub const PROTOCOL_MAJOR_DECLARED: u16 = 1;

/// Declared network protocol minor (mirrors `residiuum-client::PROTOCOL_MINOR`).
pub const PROTOCOL_MINOR_DECLARED: u16 = 0;

/// Declared RPC profile tag (mirrors `residiuum-client::PROTOCOL_PROFILE`).
pub const PROTOCOL_PROFILE_DECLARED: &str = "residiuum-rpc-v1";

/// Draft RPC interoperability label (mirrors `residiuum-client::RPC_WIRE_LABEL`).
pub const RPC_WIRE_LABEL_DECLARED: &str = "1.0-draft";

/// Migration job phase (monotonic except rollback / failed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigratePhase {
    /// Compatibility and destination checks only.
    Preflight,
    /// Durable plan written; no destination bytes yet (or only job file).
    Plan,
    /// Destination bytes materializing or complete pending verify.
    Apply,
    /// Destination opened and checked.
    Verify,
    /// Successful end state.
    Done,
    /// Destination removed / abandoned after failure.
    RolledBack,
    /// Terminal failure (source still intact).
    Failed,
}

impl MigratePhase {
    /// Stable ASCII name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preflight => "preflight",
            Self::Plan => "plan",
            Self::Apply => "apply",
            Self::Verify => "verify",
            Self::Done => "done",
            Self::RolledBack => "rolled_back",
            Self::Failed => "failed",
        }
    }
}

/// Per-file plan action (evidence-preserving; never in-place rewrite).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrateFileAction {
    /// Supported wire major: byte-identical copy into destination.
    CopyAsIs,
    /// Unsupported wire major: preserve opaque bytes + provenance.
    PreserveUnsupported,
    /// No verified frames: preserve bytes as unreadable evidence.
    PreserveUnreadable,
    /// Non-segment authoritative file (meta, chunks, recovery, …).
    CopyMeta,
}

impl MigrateFileAction {
    /// Stable ASCII name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CopyAsIs => "copy_as_is",
            Self::PreserveUnsupported => "preserve_unsupported",
            Self::PreserveUnreadable => "preserve_unreadable",
            Self::CopyMeta => "copy_meta",
        }
    }
}

/// One planned file transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrateFilePlan {
    /// Path relative to store root (POSIX `/`).
    pub relative_path: String,
    /// Planned action.
    pub action: MigrateFileAction,
    /// Source size in bytes.
    pub size: u64,
    /// Source BLAKE3-256 hex.
    pub source_blake3_hex: String,
    /// Wire major when classified from segment bytes.
    pub wire_major: Option<u8>,
    /// Wire minor when classified.
    pub wire_minor: Option<u8>,
    /// Free-form classification note.
    pub note: Option<String>,
}

/// One row of the wire reader/writer matrix (serializable snapshot).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireMatrixRow {
    /// Wire major.
    pub major: u8,
    /// Min minor (inclusive).
    pub min_minor: u8,
    /// Max minor (inclusive), if any.
    pub max_minor: Option<u8>,
    /// Can fully read.
    pub can_read: bool,
    /// Can write.
    pub can_write: bool,
    /// Status string: `current` / `read_only` / `deprecated` / `unsupported`.
    pub status: String,
}

/// Declared protocol support window (operational policy; DEF-052).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolCompatSnapshot {
    /// Profile tag.
    pub profile: String,
    /// Draft interoperability label.
    pub rpc_wire_label: String,
    /// Protocol major this build speaks.
    pub major: u16,
    /// Protocol minor this build speaks.
    pub minor: u16,
    /// Whether mixed majors require an explicit upgrade path (always true).
    pub mixed_major_requires_policy: bool,
    /// Notes for operators.
    pub notes: Vec<String>,
}

/// Preflight report (no durable job required).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigratePreflight {
    /// Source store root.
    pub source_root: String,
    /// Proposed destination root.
    pub dest_root: String,
    /// Source store id hex.
    pub source_store_id_hex: String,
    /// Source meta version line.
    pub meta_version: String,
    /// Writer wire profile of this build.
    pub writer_wire_profile: String,
    /// Writer major.minor.
    pub writer_wire: String,
    /// Wire support matrix snapshot.
    pub wire_matrix: Vec<WireMatrixRow>,
    /// Protocol support snapshot.
    pub protocol: ProtocolCompatSnapshot,
    /// Human summary of wire support.
    pub wire_support_summary: String,
    /// Segment / file classifications counted.
    pub files_classified: usize,
    /// Supported segment files.
    pub supported_segments: usize,
    /// Unsupported-major segment files (will be preserved opaque).
    pub unsupported_segments: usize,
    /// Unreadable segment files (preserved as evidence).
    pub unreadable_segments: usize,
    /// Whether destination is acceptable (empty / absent).
    pub dest_ok: bool,
    /// Blocking problems (empty = may proceed to plan).
    pub blockers: Vec<String>,
    /// Non-blocking warnings.
    pub warnings: Vec<String>,
}

/// Durable migration job document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationJob {
    /// Document schema version.
    pub format_version: u32,
    /// Profile tag.
    pub profile: String,
    /// Unique job id (hex).
    pub job_id_hex: String,
    /// Current phase.
    pub phase: MigratePhase,
    /// Source store root.
    pub source_root: String,
    /// Destination store root.
    pub dest_root: String,
    /// Source store id hex.
    pub source_store_id_hex: String,
    /// Target writer wire profile.
    pub target_wire_profile: String,
    /// Target writer major.
    pub target_wire_major: u8,
    /// Target writer minor.
    pub target_wire_minor: u8,
    /// Wire matrix snapshot at plan time.
    pub wire_matrix: Vec<WireMatrixRow>,
    /// Protocol snapshot at plan time.
    pub protocol: ProtocolCompatSnapshot,
    /// Planned file actions.
    pub files: Vec<MigrateFilePlan>,
    /// Wall-clock created (unix ns).
    pub created_unix_ns: u64,
    /// Wall-clock last update (unix ns).
    pub updated_unix_ns: u64,
    /// Files applied so far.
    pub files_applied: usize,
    /// Bytes applied so far.
    pub bytes_applied: u64,
    /// Live subjects observed after verify (if done).
    pub verified_live_subjects: Option<usize>,
    /// Failure detail when phase is Failed.
    pub error: Option<String>,
    /// Content hash of canonical job body without this field.
    pub content_hash_hex: String,
}

/// Result of a full or phased migration run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateReport {
    /// Job id.
    pub job_id: [u8; 16],
    /// Final phase.
    pub phase: MigratePhase,
    /// Source root.
    pub source: PathBuf,
    /// Destination root.
    pub destination: PathBuf,
    /// Path of the durable job document (source recovery and/or dest).
    pub job_path: PathBuf,
    /// Files planned.
    pub files_planned: usize,
    /// Files applied.
    pub files_applied: usize,
    /// Bytes applied.
    pub bytes_applied: u64,
    /// Live subjects after verify (if any).
    pub verified_live_subjects: Option<usize>,
    /// Unsupported segment files preserved.
    pub unsupported_preserved: usize,
    /// Unreadable segment files preserved.
    pub unreadable_preserved: usize,
}

/// Options for migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MigrateOptions {
    /// Stop after writing the plan (no destination bytes).
    pub plan_only: bool,
    /// Stop after apply without verify (operator may verify later).
    pub skip_verify: bool,
}

/// Path of the migration control directory under a store root.
pub fn migrate_dir(paths: &StorePaths) -> PathBuf {
    paths.recovery_dir().join(MIGRATE_DIR)
}

/// Path of the migration job file under a store root.
pub fn migrate_job_path(paths: &StorePaths) -> PathBuf {
    migrate_dir(paths).join(MIGRATE_JOB_FILE)
}

/// Snapshot the wire compatibility matrix for job documents.
pub fn snapshot_wire_matrix() -> Vec<WireMatrixRow> {
    wire_compat_matrix()
        .iter()
        .map(|e| WireMatrixRow {
            major: e.major,
            min_minor: e.min_minor,
            max_minor: e.max_minor,
            can_read: e.can_read,
            can_write: e.can_write,
            status: match e.status {
                residiuum_format::WireSupportStatus::Current => "current".into(),
                residiuum_format::WireSupportStatus::ReadOnly => "read_only".into(),
                residiuum_format::WireSupportStatus::Deprecated => "deprecated".into(),
                residiuum_format::WireSupportStatus::Unsupported => "unsupported".into(),
            },
        })
        .collect()
}

/// Snapshot the declared protocol compatibility policy.
pub fn snapshot_protocol_compat() -> ProtocolCompatSnapshot {
    ProtocolCompatSnapshot {
        profile: PROTOCOL_PROFILE_DECLARED.into(),
        rpc_wire_label: RPC_WIRE_LABEL_DECLARED.into(),
        major: PROTOCOL_MAJOR_DECLARED,
        minor: PROTOCOL_MINOR_DECLARED,
        mixed_major_requires_policy: true,
        notes: vec![
            "handshake negotiates protocol major; mismatch rejects connect".into(),
            "rolling upgrade: bring adjacent readers online before changing writers".into(),
            "RPC_WIRE_LABEL is draft until DEF-053 freeze".into(),
        ],
    }
}

/// Run preflight checks without writing a durable job.
pub fn migrate_preflight(
    source_root: &Path,
    dest_root: &Path,
    source_store_id: [u8; 16],
) -> Result<MigratePreflight, StoreError> {
    let source_paths = StorePaths::new(source_root);
    if !source_paths.looks_like_store() {
        return Err(StoreError::NotAStore(source_root.to_path_buf()));
    }

    let meta_version = fs::read_to_string(source_paths.meta_file())
        .unwrap_or_default()
        .trim()
        .to_string();

    let mut blockers = Vec::new();
    let mut warnings = Vec::new();

    let dest_ok = dest_is_acceptable(dest_root)?;
    if !dest_ok {
        blockers.push(format!(
            "destination not empty or already a store: {}",
            dest_root.display()
        ));
    }

    // Classify segment-like files for the plan preview.
    let plans = classify_authoritative_files(source_root)?;
    let mut supported_segments = 0usize;
    let mut unsupported_segments = 0usize;
    let mut unreadable_segments = 0usize;
    for p in &plans {
        match p.action {
            MigrateFileAction::CopyAsIs => supported_segments += 1,
            MigrateFileAction::PreserveUnsupported => {
                unsupported_segments += 1;
                warnings.push(format!(
                    "unsupported wire major on {}: preserved as opaque evidence",
                    p.relative_path
                ));
            }
            MigrateFileAction::PreserveUnreadable => {
                unreadable_segments += 1;
                warnings.push(format!(
                    "unreadable segment bytes on {}: preserved as evidence",
                    p.relative_path
                ));
            }
            MigrateFileAction::CopyMeta => {}
        }
        if let Some(m) = p.wire_major {
            if !wire_reader_supports(m) {
                // already counted; matrix honesty
            }
        }
    }

    // Policy: writer must appear in matrix as current.
    if !wire_reader_supports(WRITER_WIRE_MAJOR) {
        blockers.push("writer major is not in the reader support matrix".into());
    }

    Ok(MigratePreflight {
        source_root: source_root.display().to_string(),
        dest_root: dest_root.display().to_string(),
        source_store_id_hex: hex16(&source_store_id),
        meta_version,
        writer_wire_profile: WIRE_PROFILE_LABEL.into(),
        writer_wire: format!("{WRITER_WIRE_MAJOR}.{WRITER_WIRE_MINOR}"),
        wire_matrix: snapshot_wire_matrix(),
        protocol: snapshot_protocol_compat(),
        wire_support_summary: wire_support_summary(),
        files_classified: plans.len(),
        supported_segments,
        unsupported_segments,
        unreadable_segments,
        dest_ok,
        blockers,
        warnings,
    })
}

/// Build a durable plan (phase = Plan). Does not create destination store bytes
/// beyond ensuring destination root is empty-ready.
pub fn migrate_plan(
    source_root: &Path,
    dest_root: &Path,
    source_store_id: [u8; 16],
) -> Result<MigrationJob, StoreError> {
    let pre = migrate_preflight(source_root, dest_root, source_store_id)?;
    if !pre.blockers.is_empty() {
        return Err(StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("migration preflight blocked: {}", pre.blockers.join("; ")),
        )));
    }

    let files = classify_authoritative_files(source_root)?;
    let job_id = random_id()?;
    let now = now_ns();
    let mut job = MigrationJob {
        format_version: DOC_VERSION,
        profile: MIGRATE_PROFILE.into(),
        job_id_hex: hex16(&job_id),
        phase: MigratePhase::Plan,
        source_root: source_root.display().to_string(),
        dest_root: dest_root.display().to_string(),
        source_store_id_hex: hex16(&source_store_id),
        target_wire_profile: WIRE_PROFILE_LABEL.into(),
        target_wire_major: WRITER_WIRE_MAJOR,
        target_wire_minor: WRITER_WIRE_MINOR,
        wire_matrix: pre.wire_matrix,
        protocol: pre.protocol,
        files,
        created_unix_ns: now,
        updated_unix_ns: now,
        files_applied: 0,
        bytes_applied: 0,
        verified_live_subjects: None,
        error: None,
        content_hash_hex: String::new(),
    };
    job.content_hash_hex = hash_job(&job)?;
    persist_job_on_source(source_root, &job)?;
    Ok(job)
}

/// Apply a planned migration: copy files into destination (never mutates source
/// segment bytes). Updates job phase to Apply (or Failed).
pub fn migrate_apply(job: &MigrationJob) -> Result<MigrationJob, StoreError> {
    if !matches!(job.phase, MigratePhase::Plan | MigratePhase::Apply) {
        return Err(StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "migrate_apply requires phase plan|apply, got {}",
                job.phase.as_str()
            ),
        )));
    }

    let source_root = PathBuf::from(&job.source_root);
    let dest_root = PathBuf::from(&job.dest_root);
    let source_paths = StorePaths::new(&source_root);
    if !source_paths.looks_like_store() {
        return Err(StoreError::NotAStore(source_root));
    }

    // Re-check dest is still acceptable.
    if !dest_is_acceptable(&dest_root)? && !StorePaths::new(&dest_root).looks_like_store() {
        // Allow resume if dest already partially populated by this job.
        let existing = load_job_from_dest(&dest_root).ok().flatten();
        let same_job = existing
            .as_ref()
            .map(|j| j.job_id_hex == job.job_id_hex)
            .unwrap_or(false);
        if !same_job {
            return Err(StoreError::AlreadyExists(dest_root));
        }
    }

    prepare_dest_root(&dest_root)?;

    let mut files_applied = 0usize;
    let mut bytes_applied = 0u64;

    for plan in &job.files {
        let rel = plan
            .relative_path
            .replace('/', std::path::MAIN_SEPARATOR_STR);
        let from = source_root.join(&rel);
        let to = dest_root.join(&rel);
        if !from.is_file() {
            let mut failed = job.clone();
            failed.phase = MigratePhase::Failed;
            failed.error = Some(format!(
                "source file missing during apply: {}",
                plan.relative_path
            ));
            failed.updated_unix_ns = now_ns();
            failed.content_hash_hex = hash_job(&failed)?;
            let _ = persist_job_on_source(&source_root, &failed);
            return Err(StoreError::CorruptControl {
                path: plan.relative_path.clone(),
                detail: "source file missing during migration apply".into(),
                recovery: "fix source; re-run preflight/plan".into(),
            });
        }
        if let Some(parent) = to.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(&from, &to)?;
        let hex = blake3_file_hex(&to)?;
        if hex != plan.source_blake3_hex {
            let mut failed = job.clone();
            failed.phase = MigratePhase::Failed;
            failed.error = Some(format!(
                "post-copy blake3 mismatch on {}",
                plan.relative_path
            ));
            failed.updated_unix_ns = now_ns();
            failed.content_hash_hex = hash_job(&failed)?;
            let _ = persist_job_on_source(&source_root, &failed);
            let _ = persist_job_on_dest(&dest_root, &failed);
            return Err(StoreError::CorruptControl {
                path: to.display().to_string(),
                detail: "post-copy blake3 mismatch".into(),
                recovery: "rollback destination; re-run migration".into(),
            });
        }
        files_applied += 1;
        bytes_applied = bytes_applied.saturating_add(plan.size);
    }

    // Ensure empty derived dirs for a normal layout.
    let dest_paths = StorePaths::new(&dest_root);
    dest_paths.create_dirs()?;

    let mut updated = job.clone();
    updated.phase = MigratePhase::Apply;
    updated.files_applied = files_applied;
    updated.bytes_applied = bytes_applied;
    updated.updated_unix_ns = now_ns();
    updated.error = None;
    updated.content_hash_hex = hash_job(&updated)?;
    persist_job_on_source(&source_root, &updated)?;
    persist_job_on_dest(&dest_root, &updated)?;
    crate::atomic_file::sync_dir(&dest_root)?;
    Ok(updated)
}

/// Verify destination opens and live subject count is available.
///
/// Does not require byte-identical live maps when source had incomplete
/// coverage; records observed live count and marks Done on success.
pub fn migrate_verify(job: &MigrationJob) -> Result<MigrationJob, StoreError> {
    if !matches!(job.phase, MigratePhase::Apply | MigratePhase::Verify) {
        return Err(StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "migrate_verify requires phase apply|verify, got {}",
                job.phase.as_str()
            ),
        )));
    }

    let source_root = PathBuf::from(&job.source_root);
    let dest_root = PathBuf::from(&job.dest_root);

    // Verify every planned file still matches.
    for plan in &job.files {
        let rel = plan
            .relative_path
            .replace('/', std::path::MAIN_SEPARATOR_STR);
        let to = dest_root.join(&rel);
        if !to.is_file() {
            return fail_job(
                job,
                &source_root,
                Some(&dest_root),
                format!("verify: missing {}", plan.relative_path),
            );
        }
        let hex = blake3_file_hex(&to)?;
        if hex != plan.source_blake3_hex {
            return fail_job(
                job,
                &source_root,
                Some(&dest_root),
                format!("verify: blake3 mismatch on {}", plan.relative_path),
            );
        }
    }

    let live = {
        let mut opened = crate::store::Store::open(&dest_root)?;
        let _ = opened.persist_index_cache();
        let _ = opened.rebuild_catalogs();
        opened.live_logical_entries()?.len()
    };

    let mut updated = job.clone();
    updated.phase = MigratePhase::Done;
    updated.verified_live_subjects = Some(live);
    updated.updated_unix_ns = now_ns();
    updated.error = None;
    updated.content_hash_hex = hash_job(&updated)?;
    persist_job_on_source(&source_root, &updated)?;
    persist_job_on_dest(&dest_root, &updated)?;
    Ok(updated)
}

/// Remove an incomplete destination and mark the job rolled back.
///
/// Refuses to delete a destination whose job is already [`MigratePhase::Done`]
/// (successful migrations are not silently undone).
pub fn migrate_rollback(job: &MigrationJob) -> Result<MigrationJob, StoreError> {
    if matches!(job.phase, MigratePhase::Done) {
        return Err(StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "cannot rollback a completed migration (destination is verified)",
        )));
    }

    let source_root = PathBuf::from(&job.source_root);
    let dest_root = PathBuf::from(&job.dest_root);

    if dest_root.exists() {
        // Safety: only remove if it looks like our migration target (has our
        // job file) or is not yet a complete store, or is empty-ish.
        let job_on_dest = load_job_from_dest(&dest_root).ok().flatten();
        let our_job = job_on_dest
            .as_ref()
            .map(|j| j.job_id_hex == job.job_id_hex)
            .unwrap_or(false);
        let looks = StorePaths::new(&dest_root).looks_like_store();
        if our_job || !looks {
            // Remove contents carefully: only if job matches or not a store.
            if our_job || dest_is_empty_dir(&dest_root)? || !looks {
                if dest_root.is_dir() {
                    fs::remove_dir_all(&dest_root)?;
                }
            } else {
                return Err(StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "refusing to rollback: destination has no matching job document",
                )));
            }
        } else {
            return Err(StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "refusing to rollback: destination is a store without matching job",
            )));
        }
    }

    let mut updated = job.clone();
    updated.phase = MigratePhase::RolledBack;
    updated.updated_unix_ns = now_ns();
    updated.error = None;
    updated.content_hash_hex = hash_job(&updated)?;
    persist_job_on_source(&source_root, &updated)?;
    Ok(updated)
}

/// Full migration: preflight → plan → apply → verify (unless options stop early).
pub fn migrate_store(
    source_root: &Path,
    dest_root: &Path,
    source_store_id: [u8; 16],
    opts: MigrateOptions,
) -> Result<MigrateReport, StoreError> {
    let plan = migrate_plan(source_root, dest_root, source_store_id)?;
    let unsupported = plan
        .files
        .iter()
        .filter(|f| f.action == MigrateFileAction::PreserveUnsupported)
        .count();
    let unreadable = plan
        .files
        .iter()
        .filter(|f| f.action == MigrateFileAction::PreserveUnreadable)
        .count();

    if opts.plan_only {
        let job_path = migrate_job_path(&StorePaths::new(source_root));
        return Ok(MigrateReport {
            job_id: parse_job_id(&plan.job_id_hex)?,
            phase: plan.phase,
            source: source_root.to_path_buf(),
            destination: dest_root.to_path_buf(),
            job_path,
            files_planned: plan.files.len(),
            files_applied: 0,
            bytes_applied: 0,
            verified_live_subjects: None,
            unsupported_preserved: unsupported,
            unreadable_preserved: unreadable,
        });
    }

    let applied = migrate_apply(&plan)?;
    let final_job = if opts.skip_verify {
        applied
    } else {
        migrate_verify(&applied)?
    };

    let job_path = migrate_job_path(&StorePaths::new(source_root));
    Ok(MigrateReport {
        job_id: parse_job_id(&final_job.job_id_hex)?,
        phase: final_job.phase,
        source: source_root.to_path_buf(),
        destination: dest_root.to_path_buf(),
        job_path,
        files_planned: final_job.files.len(),
        files_applied: final_job.files_applied,
        bytes_applied: final_job.bytes_applied,
        verified_live_subjects: final_job.verified_live_subjects,
        unsupported_preserved: unsupported,
        unreadable_preserved: unreadable,
    })
}

/// Load the durable job from a store's recovery directory.
pub fn load_migration_job(store_root: &Path) -> Result<Option<MigrationJob>, StoreError> {
    let path = migrate_job_path(&StorePaths::new(store_root));
    if !path.is_file() {
        return Ok(None);
    }
    Ok(Some(load_job_file(&path)?))
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn classify_authoritative_files(source_root: &Path) -> Result<Vec<MigrateFilePlan>, StoreError> {
    let mut plans = Vec::new();
    for top in AUTHORITATIVE_TOP_LEVEL {
        let src_top = source_root.join(top);
        if !src_top.exists() {
            continue;
        }
        walk_classify(source_root, &src_top, *top == "store-info", &mut plans)?;
    }
    plans.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(plans)
}

fn walk_classify(
    source_root: &Path,
    current: &Path,
    under_store_info: bool,
    plans: &mut Vec<MigrateFilePlan>,
) -> Result<(), StoreError> {
    if current.is_file() {
        classify_one(source_root, current, under_store_info, plans)?;
        return Ok(());
    }
    if !current.is_dir() {
        return Ok(());
    }
    let mut entries: Vec<_> = fs::read_dir(current)?.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if under_store_info && SKIP_STORE_INFO_NAMES.iter().any(|s| *s == name_str) {
            continue;
        }
        if name_str.ends_with(".tmp") || name_str.ends_with(".lock") {
            continue;
        }
        // Skip nested migration job on source recovery to avoid copying stale
        // control into dest mid-plan (we write a fresh job after copy).
        if name_str == MIGRATE_JOB_FILE {
            continue;
        }
        let next_under_info = under_store_info
            || path
                .strip_prefix(source_root)
                .ok()
                .and_then(|r| r.components().next())
                .map(|c| c.as_os_str() == "store-info")
                .unwrap_or(false);
        if path.is_dir() {
            // Skip source recovery/migration directory contents except we still
            // copy other recovery artifacts; migration/ is re-created on dest.
            if path.file_name().and_then(|s| s.to_str()) == Some(MIGRATE_DIR)
                && path
                    .parent()
                    .and_then(|p| p.file_name())
                    .and_then(|s| s.to_str())
                    == Some("recovery")
            {
                continue;
            }
            walk_classify(source_root, &path, next_under_info, plans)?;
        } else if path.is_file() {
            classify_one(source_root, &path, next_under_info, plans)?;
        }
    }
    Ok(())
}

fn classify_one(
    source_root: &Path,
    src_file: &Path,
    under_store_info: bool,
    plans: &mut Vec<MigrateFilePlan>,
) -> Result<(), StoreError> {
    let rel = src_file
        .strip_prefix(source_root)
        .map_err(|_| {
            StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "migration path not under source root",
            ))
        })?
        .to_path_buf();
    let rel_str = path_to_posix(&rel);
    if under_store_info {
        let base = src_file.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if SKIP_STORE_INFO_NAMES.contains(&base) {
            return Ok(());
        }
    }

    let size = fs::metadata(src_file)?.len();
    let source_blake3_hex = blake3_file_hex(src_file)?;

    // Classify .residiuum segment-like files under active/ and segments/.
    let is_segmentish = rel_str.ends_with(".residiuum")
        && (rel_str.starts_with("active/")
            || rel_str.starts_with("segments/")
            || rel_str.starts_with("store-info/descriptor"));

    if is_segmentish {
        let bytes = fs::read(src_file)?;
        let class = classify_segment_bytes(&bytes);
        let (action, wire_major, wire_minor, note) = match class {
            FormatClassification::Supported {
                wire_major,
                wire_minor,
            } => (
                MigrateFileAction::CopyAsIs,
                wire_major,
                wire_minor,
                Some("supported wire generation; byte-identical copy".into()),
            ),
            FormatClassification::FormatUnsupported {
                wire_major,
                byte_len: _,
                content_hash: _,
            } => (
                MigrateFileAction::PreserveUnsupported,
                Some(wire_major),
                None,
                Some("unsupported wire major; preserve opaque bytes (no in-place rewrite)".into()),
            ),
            FormatClassification::Unreadable {
                byte_len: _,
                content_hash: _,
            } => (
                MigrateFileAction::PreserveUnreadable,
                None,
                None,
                Some("no verified frames; preserve evidence bytes".into()),
            ),
        };
        plans.push(MigrateFilePlan {
            relative_path: rel_str,
            action,
            size,
            source_blake3_hex,
            wire_major,
            wire_minor,
            note,
        });
    } else {
        plans.push(MigrateFilePlan {
            relative_path: rel_str,
            action: MigrateFileAction::CopyMeta,
            size,
            source_blake3_hex,
            wire_major: None,
            wire_minor: None,
            note: None,
        });
    }
    Ok(())
}

fn dest_is_acceptable(dest_root: &Path) -> Result<bool, StoreError> {
    if !dest_root.exists() {
        return Ok(true);
    }
    if !dest_root.is_dir() {
        return Ok(false);
    }
    if StorePaths::new(dest_root).looks_like_store() {
        return Ok(false);
    }
    dest_is_empty_dir(dest_root)
}

fn dest_is_empty_dir(dest_root: &Path) -> Result<bool, StoreError> {
    if !dest_root.is_dir() {
        return Ok(false);
    }
    Ok(fs::read_dir(dest_root)?.next().is_none())
}

fn prepare_dest_root(dest_root: &Path) -> Result<(), StoreError> {
    if !dest_root.exists() {
        fs::create_dir_all(dest_root)?;
    } else if dest_root.is_dir() && dest_is_empty_dir(dest_root)? {
        // ok
    } else if StorePaths::new(dest_root).looks_like_store() {
        // resume path handled by caller
    } else if fs::read_dir(dest_root)?.next().is_some() {
        // may already have partial files from this job
    }
    Ok(())
}

fn persist_job_on_source(source_root: &Path, job: &MigrationJob) -> Result<(), StoreError> {
    let paths = StorePaths::new(source_root);
    let dir = migrate_dir(&paths);
    fs::create_dir_all(&dir)?;
    write_job_file(&migrate_job_path(&paths), job)?;
    crate::atomic_file::sync_dir(&dir)?;
    Ok(())
}

fn persist_job_on_dest(dest_root: &Path, job: &MigrationJob) -> Result<(), StoreError> {
    let paths = StorePaths::new(dest_root);
    let dir = migrate_dir(&paths);
    fs::create_dir_all(&dir)?;
    write_job_file(&migrate_job_path(&paths), job)?;
    crate::atomic_file::sync_dir(&dir)?;
    Ok(())
}

fn load_job_from_dest(dest_root: &Path) -> Result<Option<MigrationJob>, StoreError> {
    load_migration_job(dest_root)
}

fn write_job_file(path: &Path, job: &MigrationJob) -> Result<(), StoreError> {
    let mut body = serde_json::to_vec_pretty(job).map_err(|e| {
        StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("serialize migration job: {e}"),
        ))
    })?;
    body.push(b'\n');
    crate::atomic_file::write_atomic_keep_previous(path, &body)
}

fn load_job_file(path: &Path) -> Result<MigrationJob, StoreError> {
    let bytes = fs::read(path)?;
    let job: MigrationJob = serde_json::from_slice(&bytes).map_err(|e| {
        StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("parse migration job: {e}"),
        ))
    })?;
    if job.profile != MIGRATE_PROFILE {
        return Err(StoreError::CorruptMeta("unsupported migration profile"));
    }
    if job.format_version != DOC_VERSION {
        return Err(StoreError::CorruptMeta(
            "unsupported migration format_version",
        ));
    }
    let expected = hash_job(&job)?;
    if expected != job.content_hash_hex {
        return Err(StoreError::CorruptControl {
            path: path.display().to_string(),
            detail: "migration job content_hash_hex mismatch".into(),
            recovery: "discard job; re-run preflight/plan from source".into(),
        });
    }
    Ok(job)
}

fn hash_job(job: &MigrationJob) -> Result<String, StoreError> {
    let mut for_hash = job.clone();
    for_hash.content_hash_hex.clear();
    let body = serde_json::to_vec(&for_hash).map_err(|e| {
        StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("serialize migration job for hash: {e}"),
        ))
    })?;
    Ok(hex32(blake3::hash(&body).as_bytes()))
}

fn fail_job(
    job: &MigrationJob,
    source_root: &Path,
    dest_root: Option<&Path>,
    detail: String,
) -> Result<MigrationJob, StoreError> {
    let mut failed = job.clone();
    failed.phase = MigratePhase::Failed;
    failed.error = Some(detail.clone());
    failed.updated_unix_ns = now_ns();
    failed.content_hash_hex = hash_job(&failed)?;
    let _ = persist_job_on_source(source_root, &failed);
    if let Some(d) = dest_root {
        let _ = persist_job_on_dest(d, &failed);
    }
    Err(StoreError::CorruptControl {
        path: job.dest_root.clone(),
        detail,
        recovery: "rollback destination; re-run migration from source".into(),
    })
}

fn parse_job_id(s: &str) -> Result<[u8; 16], StoreError> {
    crate::layout::unhex16(s).ok_or(StoreError::CorruptMeta("invalid job_id hex"))
}

fn path_to_posix(p: &Path) -> String {
    p.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn blake3_file_hex(path: &Path) -> Result<String, StoreError> {
    let mut f = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex32(hasher.finalize().as_bytes()))
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::DurabilityMode;
    use crate::store::Store;
    use tempfile::tempdir;

    #[test]
    fn matrix_snapshot_non_empty() {
        let m = snapshot_wire_matrix();
        assert!(!m.is_empty());
        assert!(m.iter().any(|r| r.status == "current"));
        let p = snapshot_protocol_compat();
        assert_eq!(p.profile, PROTOCOL_PROFILE_DECLARED);
        assert!(p.mixed_major_requires_policy);
    }

    #[test]
    fn full_migrate_preserves_live_data_and_source() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");

        let mut store = Store::create(&src).unwrap();
        store
            .put("col/a", br#"{"v":1}"#, DurabilityMode::Durable)
            .unwrap();
        store
            .put("col/b", br#"{"v":2}"#, DurabilityMode::Durable)
            .unwrap();
        store.delete("col/a", DurabilityMode::Durable).unwrap();
        let sid = store.store_id();
        drop(store);

        // Re-open exclusive for flush path consistency.
        let store = Store::open(&src).unwrap();
        let report =
            migrate_store(&src, &dst, store.store_id(), MigrateOptions::default()).unwrap();
        assert_eq!(report.phase, MigratePhase::Done);
        assert!(report.files_applied > 0);
        assert_eq!(report.verified_live_subjects, Some(1));
        drop(store);

        // Source still readable with same data.
        let src_open = Store::open(&src).unwrap();
        assert_eq!(src_open.store_id(), sid);
        assert_eq!(
            src_open.get("col/b").unwrap().as_deref(),
            Some(br#"{"v":2}"#.as_slice())
        );
        drop(src_open);

        let dst_open = Store::open(&dst).unwrap();
        assert_eq!(dst_open.store_id(), sid);
        assert!(dst_open.get("col/a").unwrap().is_none());
        assert_eq!(
            dst_open.get("col/b").unwrap().as_deref(),
            Some(br#"{"v":2}"#.as_slice())
        );

        let job = load_migration_job(&src).unwrap().expect("job on source");
        assert_eq!(job.phase, MigratePhase::Done);
        assert_eq!(job.profile, MIGRATE_PROFILE);
    }

    #[test]
    fn plan_only_does_not_create_dest_store() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        let mut store = Store::create(&src).unwrap();
        store.put("k", b"v", DurabilityMode::Durable).unwrap();
        let report = migrate_store(
            &src,
            &dst,
            store.store_id(),
            MigrateOptions {
                plan_only: true,
                skip_verify: false,
            },
        )
        .unwrap();
        assert_eq!(report.phase, MigratePhase::Plan);
        assert!(!StorePaths::new(&dst).looks_like_store());
        assert!(load_migration_job(&src).unwrap().is_some());
    }

    #[test]
    fn preflight_blocks_non_empty_dest() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        fs::create_dir_all(&dst).unwrap();
        fs::write(dst.join("stale"), b"x").unwrap();
        let store = Store::create(&src).unwrap();
        let pre = migrate_preflight(&src, &dst, store.store_id()).unwrap();
        assert!(!pre.dest_ok);
        assert!(!pre.blockers.is_empty());
    }

    #[test]
    fn rollback_removes_incomplete_dest() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        let mut store = Store::create(&src).unwrap();
        store.put("k", b"v", DurabilityMode::Durable).unwrap();
        let sid = store.store_id();
        drop(store);

        let plan = migrate_plan(&src, &dst, sid).unwrap();
        let applied = migrate_apply(&plan).unwrap();
        assert!(StorePaths::new(&dst).looks_like_store());
        let rolled = migrate_rollback(&applied).unwrap();
        assert_eq!(rolled.phase, MigratePhase::RolledBack);
        assert!(!dst.exists() || !StorePaths::new(&dst).looks_like_store());
        // Source intact.
        let src_open = Store::open(&src).unwrap();
        assert_eq!(src_open.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
    }

    #[test]
    fn rollback_refuses_completed() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        let mut store = Store::create(&src).unwrap();
        store.put("k", b"v", DurabilityMode::Durable).unwrap();
        let sid = store.store_id();
        drop(store);
        let report = migrate_store(&src, &dst, sid, MigrateOptions::default()).unwrap();
        assert_eq!(report.phase, MigratePhase::Done);
        let job = load_migration_job(&src).unwrap().unwrap();
        assert!(migrate_rollback(&job).is_err());
    }
}
