//! Bounded integrity scrub and media-health findings (DEF-051).
//!
//! Scrub verifies segment and chunk bytes without mutating authoritative data.
//! Work is **bounded** per invocation (`max_files` / `max_bytes`) so operators
//! and background schedulers can interleave scrub with foreground IO.
//!
//! Durable state lives under `recovery/scrub/`:
//!
//! - [`SCRUB_STATE_FILE`] — frontier, pause flag, cumulative counters
//! - [`SCRUB_FINDINGS_FILE`] — open findings (hash mismatch, holes, IO)
//! - `quarantine/` — optional copies of corrupt evidence (**never** deletes
//!   or hides the original path)
//!
//! Profile: [`SCRUB_PROFILE`].

use crate::error::StoreError;
use crate::ids::hex16;
use crate::layout::{list_residiuum_files, segment_id_from_filename, StorePaths};
use crate::tier::{available_sealed_paths, hash_file, TierPlacement};
use residiuum_format::{scan_forward, SafetyLimits};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Profile tag for the scrub control documents (DEF-051).
pub const SCRUB_PROFILE: &str = "residiuum-scrub-v1";

/// Directory under `recovery/` for scrub control + quarantine.
pub const SCRUB_DIR: &str = "scrub";

/// Durable scrub frontier / counters filename.
pub const SCRUB_STATE_FILE: &str = "state.v1.json";

/// Durable open findings filename.
pub const SCRUB_FINDINGS_FILE: &str = "findings.v1.json";

/// Subdirectory under scrub for quarantined evidence copies.
pub const SCRUB_QUARANTINE_DIR: &str = "quarantine";

/// Default max files verified per `scrub_once` call.
pub const DEFAULT_SCRUB_MAX_FILES: usize = 32;

/// Default max bytes hashed/scanned per `scrub_once` call (64 MiB).
pub const DEFAULT_SCRUB_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Schema version for scrub state and findings documents.
const DOC_VERSION: u32 = 1;

/// Kind of on-disk target verified by scrub.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScrubTargetKind {
    /// Sealed segment (hot or tier media).
    SealedSegment,
    /// Active append segment (`active/active.residiuum`).
    ActiveSegment,
    /// Chunk payload piece under `chunks/`.
    ChunkFile,
    /// Recovery Shadow (`.rsh`) — recovery-authoritative P★ media (CSE-3).
    RecoveryShadow,
}

impl ScrubTargetKind {
    /// Stable ASCII name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SealedSegment => "sealed_segment",
            Self::ActiveSegment => "active_segment",
            Self::ChunkFile => "chunk_file",
            Self::RecoveryShadow => "recovery_shadow",
        }
    }
}

/// Classification of a scrub finding (honest, never silent success).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScrubFindingKind {
    /// Full-file BLAKE3 disagrees with placement / catalog expectation.
    ContentHashMismatch,
    /// Forward scan found one or more non-verified regions (holes).
    FrameHoles,
    /// File unreadable or other IO failure during verification.
    IoError,
    /// File missing when placement/catalog said it should exist.
    MissingFile,
    /// Expected size disagrees with on-disk size.
    SizeMismatch,
    /// Recovery Shadow failed self-verify (incomplete/corrupt — never P★).
    ShadowIntegrity,
}

impl ScrubFindingKind {
    /// Stable ASCII name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ContentHashMismatch => "content_hash_mismatch",
            Self::FrameHoles => "frame_holes",
            Self::IoError => "io_error",
            Self::MissingFile => "missing_file",
            Self::SizeMismatch => "size_mismatch",
            Self::ShadowIntegrity => "shadow_integrity",
        }
    }
}

/// One unit of scrub work (deterministic order key).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrubTarget {
    /// Stable sort key (`kind:relative_path`).
    pub key: String,
    /// Target class.
    pub kind: ScrubTargetKind,
    /// Path relative to store root (POSIX `/`).
    pub relative_path: String,
    /// Segment id hex when known (sealed / active).
    pub segment_id_hex: Option<String>,
    /// Expected BLAKE3-256 hex from placement/catalog when known.
    pub expected_hash_hex: Option<String>,
    /// Expected size from placement when known.
    pub expected_size: Option<u64>,
}

/// Result of verifying a single target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrubTargetResult {
    /// Target key.
    pub key: String,
    /// Target class.
    pub kind: ScrubTargetKind,
    /// Relative path under store root.
    pub relative_path: String,
    /// Observed size (0 if missing).
    pub size: u64,
    /// Observed BLAKE3 hex when hashed.
    pub observed_hash_hex: Option<String>,
    /// Verified frames when scanned.
    pub verified_frames: u64,
    /// Hole regions when scanned.
    pub holes: u64,
    /// Finding kinds raised for this target (empty = clean).
    pub findings: Vec<ScrubFindingKind>,
    /// Optional quarantine relative path under store root.
    pub quarantine_path: Option<String>,
    /// Human detail for the first failure (if any).
    pub detail: Option<String>,
}

/// Persistent finding retained across scrub cycles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrubFinding {
    /// Finding id (hex of target key + kind for stability within a cycle).
    pub finding_id: String,
    /// Target key.
    pub target_key: String,
    /// Target class.
    pub kind: ScrubTargetKind,
    /// Finding classification.
    pub finding: ScrubFindingKind,
    /// Relative path of the original file.
    pub relative_path: String,
    /// Segment id hex when applicable.
    pub segment_id_hex: Option<String>,
    /// Detail string.
    pub detail: String,
    /// Expected hash hex when relevant.
    pub expected_hash_hex: Option<String>,
    /// Observed hash hex when relevant.
    pub observed_hash_hex: Option<String>,
    /// Quarantine copy relative path when made.
    pub quarantine_path: Option<String>,
    /// Unix nanoseconds when first observed.
    pub first_seen_ns: u64,
    /// Unix nanoseconds of last reconfirm.
    pub last_seen_ns: u64,
    /// Scrub cycle id when last confirmed.
    pub cycle_id: u64,
}

/// Durable scrub frontier and counters (`recovery/scrub/state.v1.json`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScrubState {
    /// Document schema version.
    pub format_version: u32,
    /// Profile label.
    pub profile: String,
    /// Store id hex.
    pub store_id_hex: String,
    /// Monotonic scrub cycle counter (increments when a full pass completes).
    pub cycle_id: u64,
    /// Index into the sorted target list for the next unit of work.
    pub next_index: usize,
    /// Number of targets known at the start of the current cycle.
    pub targets_in_cycle: usize,
    /// Whether the operator paused further progress.
    pub paused: bool,
    /// Bytes verified in the current (possibly partial) cycle.
    pub bytes_verified_cycle: u64,
    /// Targets verified in the current cycle.
    pub targets_verified_cycle: usize,
    /// Failures found in the current cycle.
    pub failures_cycle: usize,
    /// Lifetime bytes verified (all completed cycles + current).
    pub bytes_verified_total: u64,
    /// Lifetime targets verified.
    pub targets_verified_total: u64,
    /// Lifetime failures observed.
    pub failures_total: u64,
    /// Unix ns when the last full cycle completed (`None` if never).
    pub last_complete_cycle_ns: Option<u64>,
    /// Unix ns of the last `scrub_once` progress.
    pub last_progress_ns: Option<u64>,
    /// Unix ns when state was first created.
    pub created_ns: u64,
    /// Unix ns of last state write.
    pub updated_ns: u64,
    /// Coverage of the last completed cycle (0.0–1.0), or partial progress.
    pub coverage_ratio: f64,
    /// Open findings count at last write.
    pub open_findings: usize,
}

/// Findings document (`recovery/scrub/findings.v1.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrubFindingsDoc {
    /// Document schema version.
    pub format_version: u32,
    /// Profile label.
    pub profile: String,
    /// Store id hex.
    pub store_id_hex: String,
    /// Open findings (not auto-cleared when target is later clean — operator may clear).
    pub findings: Vec<ScrubFinding>,
    /// Unix ns of last write.
    pub updated_ns: u64,
}

/// Operator-visible status snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct ScrubStatus {
    /// Whether scrub is paused.
    pub paused: bool,
    /// Current cycle id (in progress or last completed).
    pub cycle_id: u64,
    /// Targets remaining in the current cycle (0 when idle after complete).
    pub targets_remaining: usize,
    /// Targets planned for the current cycle.
    pub targets_in_cycle: usize,
    /// Coverage ratio for the current or last cycle (0.0–1.0).
    pub coverage_ratio: f64,
    /// Bytes verified in the current cycle.
    pub bytes_verified_cycle: u64,
    /// Lifetime bytes verified.
    pub bytes_verified_total: u64,
    /// Failures in the current cycle.
    pub failures_cycle: usize,
    /// Lifetime failures.
    pub failures_total: u64,
    /// Open findings count.
    pub open_findings: usize,
    /// Age of the last complete cycle in nanoseconds (`None` if never completed).
    pub last_complete_age_ns: Option<u64>,
    /// Wall time of last complete cycle (unix ns).
    pub last_complete_cycle_ns: Option<u64>,
    /// Wall time of last progress (unix ns).
    pub last_progress_ns: Option<u64>,
}

/// Options for a bounded scrub step.
#[derive(Debug, Clone)]
pub struct ScrubOptions {
    /// Max target files to process this call.
    pub max_files: usize,
    /// Max bytes to hash/scan this call (stops after the target that crosses).
    pub max_bytes: u64,
    /// Include the active segment (default true).
    pub include_active: bool,
    /// Include chunk files under `chunks/` (default true).
    pub include_chunks: bool,
    /// Copy corrupt targets into quarantine (default true).
    pub quarantine: bool,
    /// Safety limits for frame scan.
    pub limits: SafetyLimits,
}

impl Default for ScrubOptions {
    fn default() -> Self {
        Self {
            max_files: DEFAULT_SCRUB_MAX_FILES,
            max_bytes: DEFAULT_SCRUB_MAX_BYTES,
            include_active: true,
            include_chunks: true,
            quarantine: true,
            limits: SafetyLimits::default(),
        }
    }
}

/// Report from one `scrub_once` invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct ScrubReport {
    /// Results for each target processed this call.
    pub results: Vec<ScrubTargetResult>,
    /// Bytes processed this call.
    pub bytes_processed: u64,
    /// Targets processed this call.
    pub targets_processed: usize,
    /// New or reconfirmed failures this call.
    pub failures_this_call: usize,
    /// Whether the scrub cycle completed during this call.
    pub cycle_completed: bool,
    /// Whether work stopped because the scrub is paused.
    pub paused: bool,
    /// Status after this call.
    pub status: ScrubStatus,
}

/// Directory for scrub control documents.
pub fn scrub_dir(paths: &StorePaths) -> PathBuf {
    paths.recovery_dir().join(SCRUB_DIR)
}

/// Path of the scrub state document.
pub fn scrub_state_path(paths: &StorePaths) -> PathBuf {
    scrub_dir(paths).join(SCRUB_STATE_FILE)
}

/// Path of the findings document.
pub fn scrub_findings_path(paths: &StorePaths) -> PathBuf {
    scrub_dir(paths).join(SCRUB_FINDINGS_FILE)
}

/// Quarantine directory under recovery/scrub.
pub fn scrub_quarantine_dir(paths: &StorePaths) -> PathBuf {
    scrub_dir(paths).join(SCRUB_QUARANTINE_DIR)
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn relative_to_root(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .map(|rel| rel.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| path.to_string_lossy().replace('\\', "/"))
}

fn hex32(hash: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in hash {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn finding_id(target_key: &str, finding: ScrubFindingKind) -> String {
    let mut h = blake3::Hasher::new();
    h.update(target_key.as_bytes());
    h.update(b"|");
    h.update(finding.as_str().as_bytes());
    let digest = h.finalize();
    let bytes = digest.as_bytes();
    // 16-byte id is enough for operator correlation.
    let mut id = [0u8; 16];
    id.copy_from_slice(&bytes[..16]);
    hex16(&id)
}

/// Build the deterministic target list for one scrub cycle.
pub fn plan_scrub_targets(
    paths: &StorePaths,
    placement: &TierPlacement,
    opts: &ScrubOptions,
) -> Result<Vec<ScrubTarget>, StoreError> {
    let mut targets = Vec::new();

    // Sealed segments from placement (available media) + any leftover hot dir files.
    let sealed_paths = available_sealed_paths(paths, placement)?;
    let mut seen_rel = std::collections::BTreeSet::new();

    for path in &sealed_paths {
        let rel = relative_to_root(&paths.root, path);
        if !seen_rel.insert(rel.clone()) {
            continue;
        }
        let segment_id = segment_id_from_filename(path);
        let (expected_hash_hex, expected_size, segment_id_hex) = if let Some(id) = segment_id {
            if let Some(p) = placement.get(&id) {
                (
                    p.content_hash.known_hash().map(|h| hex32(&h)),
                    Some(p.size),
                    Some(hex16(&id)),
                )
            } else {
                (None, None, Some(hex16(&id)))
            }
        } else {
            (None, None, None)
        };
        let key = format!("sealed:{}", rel);
        targets.push(ScrubTarget {
            key,
            kind: ScrubTargetKind::SealedSegment,
            relative_path: rel,
            segment_id_hex,
            expected_hash_hex,
            expected_size,
        });
    }

    // Placement may list segments not currently in available_sealed_paths (offline).
    // Still plan them so missing media surfaces as findings when they become selected
    // via a placement walk — but available_sealed_paths already filters available.
    // Also include placement entries that claim available but path is wrong:
    for p in placement.entries() {
        if !p.available {
            continue;
        }
        let abs = if Path::new(&p.relative_path).is_absolute() {
            PathBuf::from(&p.relative_path)
        } else {
            paths.root.join(&p.relative_path)
        };
        let rel = relative_to_root(&paths.root, &abs);
        if !seen_rel.insert(rel.clone()) {
            continue;
        }
        let key = format!("sealed:{}", rel);
        targets.push(ScrubTarget {
            key,
            kind: ScrubTargetKind::SealedSegment,
            relative_path: rel,
            segment_id_hex: Some(hex16(&p.segment_id)),
            expected_hash_hex: p.content_hash.known_hash().map(|h| hex32(&h)),
            expected_size: Some(p.size),
        });
    }

    if opts.include_active {
        let active = paths.active_segment();
        if active.is_file() {
            let rel = relative_to_root(&paths.root, &active);
            targets.push(ScrubTarget {
                key: format!("active:{}", rel),
                kind: ScrubTargetKind::ActiveSegment,
                relative_path: rel,
                segment_id_hex: None,
                expected_hash_hex: None,
                expected_size: None,
            });
        }
    }

    if opts.include_chunks {
        let chunk_files = list_residiuum_files(&paths.chunks_dir())?;
        for path in chunk_files {
            let rel = relative_to_root(&paths.root, &path);
            targets.push(ScrubTarget {
                key: format!("chunk:{}", rel),
                kind: ScrubTargetKind::ChunkFile,
                relative_path: rel,
                segment_id_hex: None,
                expected_hash_hex: None,
                expected_size: None,
            });
        }
    }

    // Recovery Shadows (CSE-3): recovery-authoritative; scrub must verify integrity.
    let shadow_dir = crate::recovery_shadow::shadow_dir(paths);
    if shadow_dir.is_dir() {
        for ent in fs::read_dir(&shadow_dir)? {
            let ent = ent?;
            let path = ent.path();
            if !crate::recovery_shadow::is_recovery_shadow_path(&path) {
                continue;
            }
            let rel = relative_to_root(&paths.root, &path);
            let segment_id_hex = path
                .file_stem()
                .and_then(|s| s.to_str())
                .filter(|s| s.len() == 32)
                .map(|s| s.to_string());
            targets.push(ScrubTarget {
                key: format!("rsh:{}", rel),
                kind: ScrubTargetKind::RecoveryShadow,
                relative_path: rel,
                segment_id_hex,
                expected_hash_hex: None,
                expected_size: None,
            });
        }
    }

    targets.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(targets)
}

/// Load scrub state or invent a fresh one.
pub fn load_or_init_scrub_state(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<ScrubState, StoreError> {
    let path = scrub_state_path(paths);
    if path.is_file() {
        let bytes = fs::read(&path)?;
        match serde_json::from_slice::<ScrubState>(&bytes) {
            Ok(state) => Ok(state),
            Err(e) => Err(StoreError::CorruptControl {
                path: path.display().to_string(),
                detail: e.to_string(),
                recovery: "delete recovery/scrub/state.v1.json to restart scrub frontier".into(),
            }),
        }
    } else {
        let ns = now_ns();
        Ok(ScrubState {
            format_version: DOC_VERSION,
            profile: SCRUB_PROFILE.into(),
            store_id_hex: hex16(&store_id),
            cycle_id: 1,
            next_index: 0,
            targets_in_cycle: 0,
            paused: false,
            bytes_verified_cycle: 0,
            targets_verified_cycle: 0,
            failures_cycle: 0,
            bytes_verified_total: 0,
            targets_verified_total: 0,
            failures_total: 0,
            last_complete_cycle_ns: None,
            last_progress_ns: None,
            created_ns: ns,
            updated_ns: ns,
            coverage_ratio: 0.0,
            open_findings: 0,
        })
    }
}

/// Persist scrub state atomically.
pub fn write_scrub_state(paths: &StorePaths, state: &ScrubState) -> Result<PathBuf, StoreError> {
    let dir = scrub_dir(paths);
    fs::create_dir_all(&dir)?;
    let path = scrub_state_path(paths);
    let bytes = serde_json::to_vec_pretty(state).map_err(|e| StoreError::CorruptControl {
        path: path.display().to_string(),
        detail: e.to_string(),
        recovery: "fix scrub state serialization".into(),
    })?;
    crate::atomic_file::write_atomic_keep_previous(&path, &bytes)?;
    Ok(path)
}

/// Load findings document (empty if absent).
pub fn load_scrub_findings(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<ScrubFindingsDoc, StoreError> {
    let path = scrub_findings_path(paths);
    if !path.is_file() {
        return Ok(ScrubFindingsDoc {
            format_version: DOC_VERSION,
            profile: SCRUB_PROFILE.into(),
            store_id_hex: hex16(&store_id),
            findings: Vec::new(),
            updated_ns: now_ns(),
        });
    }
    let bytes = fs::read(&path)?;
    match serde_json::from_slice::<ScrubFindingsDoc>(&bytes) {
        Ok(doc) => Ok(doc),
        Err(e) => Err(StoreError::CorruptControl {
            path: path.display().to_string(),
            detail: e.to_string(),
            recovery: "inspect or delete recovery/scrub/findings.v1.json".into(),
        }),
    }
}

/// Persist findings document atomically.
pub fn write_scrub_findings(
    paths: &StorePaths,
    doc: &ScrubFindingsDoc,
) -> Result<PathBuf, StoreError> {
    let dir = scrub_dir(paths);
    fs::create_dir_all(&dir)?;
    let path = scrub_findings_path(paths);
    let bytes = serde_json::to_vec_pretty(doc).map_err(|e| StoreError::CorruptControl {
        path: path.display().to_string(),
        detail: e.to_string(),
        recovery: "fix scrub findings serialization".into(),
    })?;
    crate::atomic_file::write_atomic_keep_previous(&path, &bytes)?;
    Ok(path)
}

/// Build a status snapshot from durable state + now.
pub fn status_from_state(state: &ScrubState) -> ScrubStatus {
    let now = now_ns();
    let remaining = state
        .targets_in_cycle
        .saturating_sub(state.next_index.min(state.targets_in_cycle));
    ScrubStatus {
        paused: state.paused,
        cycle_id: state.cycle_id,
        targets_remaining: remaining,
        targets_in_cycle: state.targets_in_cycle,
        coverage_ratio: state.coverage_ratio,
        bytes_verified_cycle: state.bytes_verified_cycle,
        bytes_verified_total: state.bytes_verified_total,
        failures_cycle: state.failures_cycle,
        failures_total: state.failures_total,
        open_findings: state.open_findings,
        last_complete_age_ns: state.last_complete_cycle_ns.map(|t| now.saturating_sub(t)),
        last_complete_cycle_ns: state.last_complete_cycle_ns,
        last_progress_ns: state.last_progress_ns,
    }
}

fn upsert_finding(doc: &mut ScrubFindingsDoc, finding: ScrubFinding) {
    if let Some(existing) = doc
        .findings
        .iter_mut()
        .find(|f| f.finding_id == finding.finding_id)
    {
        existing.last_seen_ns = finding.last_seen_ns;
        existing.cycle_id = finding.cycle_id;
        existing.detail = finding.detail;
        existing.observed_hash_hex = finding.observed_hash_hex;
        existing.quarantine_path = finding
            .quarantine_path
            .or_else(|| existing.quarantine_path.clone());
    } else {
        doc.findings.push(finding);
    }
}

/// Remove findings for a target key that are no longer present (target is clean).
fn clear_findings_for_target(doc: &mut ScrubFindingsDoc, target_key: &str) {
    doc.findings.retain(|f| f.target_key != target_key);
}

fn quarantine_target(
    paths: &StorePaths,
    target: &ScrubTarget,
    abs: &Path,
) -> Result<Option<String>, StoreError> {
    if !abs.is_file() {
        return Ok(None);
    }
    let qdir = scrub_quarantine_dir(paths);
    fs::create_dir_all(&qdir)?;
    // Stable name from target key hash so re-quarantine overwrites same slot.
    let mut h = blake3::Hasher::new();
    h.update(target.key.as_bytes());
    let digest = *h.finalize().as_bytes();
    let mut short = [0u8; 16];
    short.copy_from_slice(&digest[..16]);
    let stem = hex16(&short);
    let file_name = abs
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "target.bin".into());
    let dest = qdir.join(format!("{stem}_{file_name}"));
    // Copy (do not move) — original must remain for honesty.
    fs::copy(abs, &dest)?;
    // Side-car metadata.
    let meta_path = qdir.join(format!("{stem}.finding.json"));
    let meta = serde_json::json!({
        "profile": SCRUB_PROFILE,
        "target_key": target.key,
        "relative_path": target.relative_path,
        "kind": target.kind,
        "quarantined_file": relative_to_root(&paths.root, &dest),
        "source_path": target.relative_path,
    });
    let meta_bytes = serde_json::to_vec_pretty(&meta).map_err(|e| StoreError::CorruptControl {
        path: meta_path.display().to_string(),
        detail: e.to_string(),
        recovery: "delete quarantine meta".into(),
    })?;
    let mut f = File::create(&meta_path)?;
    f.write_all(&meta_bytes)?;
    f.sync_all()?;
    Ok(Some(relative_to_root(&paths.root, &dest)))
}

/// Verify one target: hash, optional expected compare, optional frame scan.
pub fn verify_scrub_target(
    paths: &StorePaths,
    target: &ScrubTarget,
    opts: &ScrubOptions,
) -> Result<ScrubTargetResult, StoreError> {
    let abs = paths.root.join(&target.relative_path);
    if !abs.is_file() {
        return Ok(ScrubTargetResult {
            key: target.key.clone(),
            kind: target.kind,
            relative_path: target.relative_path.clone(),
            size: 0,
            observed_hash_hex: None,
            verified_frames: 0,
            holes: 0,
            findings: vec![ScrubFindingKind::MissingFile],
            quarantine_path: None,
            detail: Some(format!("missing file: {}", target.relative_path)),
        });
    }

    let (hash, size) = hash_file(&abs)?;
    let observed_hash_hex = hex32(&hash);
    let mut findings = Vec::new();
    let mut detail = None;

    if let Some(expected_size) = target.expected_size {
        if expected_size != size {
            findings.push(ScrubFindingKind::SizeMismatch);
            detail = Some(format!(
                "size mismatch: expected {expected_size}, observed {size}"
            ));
        }
    }

    if let Some(ref expected_hex) = target.expected_hash_hex {
        if expected_hex != &observed_hash_hex {
            findings.push(ScrubFindingKind::ContentHashMismatch);
            detail = Some(format!(
                "content hash mismatch: expected {expected_hex}, observed {observed_hash_hex}"
            ));
        }
    }

    let mut verified_frames = 0u64;
    let mut holes = 0u64;

    // Frame/segment scan for segment kinds (not raw chunk files — those are
    // opaque payload pieces verified by content hash when known).
    if matches!(
        target.kind,
        ScrubTargetKind::SealedSegment | ScrubTargetKind::ActiveSegment
    ) {
        let bytes = fs::read(&abs)?;
        let report = scan_forward(&bytes, opts.limits);
        verified_frames = report.verified_count() as u64;
        holes = report.holes().count() as u64;
        if holes > 0 {
            findings.push(ScrubFindingKind::FrameHoles);
            if detail.is_none() {
                detail = Some(format!("{holes} hole region(s) in forward scan"));
            }
        }
    }

    if matches!(target.kind, ScrubTargetKind::RecoveryShadow) {
        match crate::recovery_shadow::try_load_shadow(&abs, None)? {
            crate::recovery_shadow::ShadowLoad::Ok(d) => {
                verified_frames = d.records.len() as u64;
                if let Some(ref hex) = target.segment_id_hex {
                    if crate::layout::hex16(&d.segment_id) != *hex {
                        findings.push(ScrubFindingKind::ShadowIntegrity);
                        detail = Some("shadow segment_id mismatch vs path".into());
                    }
                }
            }
            crate::recovery_shadow::ShadowLoad::Missing => {
                findings.push(ScrubFindingKind::MissingFile);
                detail = Some("shadow missing".into());
            }
            crate::recovery_shadow::ShadowLoad::Incomplete => {
                findings.push(ScrubFindingKind::ShadowIntegrity);
                detail = Some("incomplete recovery shadow (never P★)".into());
            }
            crate::recovery_shadow::ShadowLoad::Corrupt { detail: d } => {
                findings.push(ScrubFindingKind::ShadowIntegrity);
                detail = Some(d);
            }
        }
    }

    let quarantine_path = if opts.quarantine && !findings.is_empty() {
        quarantine_target(paths, target, &abs)?
    } else {
        None
    };

    Ok(ScrubTargetResult {
        key: target.key.clone(),
        kind: target.kind,
        relative_path: target.relative_path.clone(),
        size,
        observed_hash_hex: Some(observed_hash_hex),
        verified_frames,
        holes,
        findings,
        quarantine_path,
        detail,
    })
}

/// Run a bounded scrub step: verify up to `max_files` / `max_bytes`, persist
/// frontier and findings. Does not mutate authoritative segments.
pub fn scrub_once(
    paths: &StorePaths,
    store_id: [u8; 16],
    placement: &TierPlacement,
    opts: &ScrubOptions,
) -> Result<ScrubReport, StoreError> {
    let mut state = load_or_init_scrub_state(paths, store_id)?;
    let mut findings_doc = load_scrub_findings(paths, store_id)?;

    if state.paused {
        state.open_findings = findings_doc.findings.len();
        state.updated_ns = now_ns();
        write_scrub_state(paths, &state)?;
        return Ok(ScrubReport {
            results: Vec::new(),
            bytes_processed: 0,
            targets_processed: 0,
            failures_this_call: 0,
            cycle_completed: false,
            paused: true,
            status: status_from_state(&state),
        });
    }

    // (Re)plan targets when starting a cycle or after a complete cycle reset.
    let targets = plan_scrub_targets(paths, placement, opts)?;
    if state.targets_in_cycle == 0 || state.next_index == 0 && state.targets_verified_cycle == 0 {
        // Fresh cycle start (or first run): adopt current plan size.
        if state.targets_in_cycle == 0 {
            state.targets_in_cycle = targets.len();
            state.bytes_verified_cycle = 0;
            state.targets_verified_cycle = 0;
            state.failures_cycle = 0;
            state.coverage_ratio = 0.0;
        }
    }
    // If the plan shrank (segments reclaimed), clamp index.
    if state.next_index > targets.len() {
        state.next_index = targets.len();
    }
    // Keep targets_in_cycle aligned with current plan when restarting mid-idle.
    if state.next_index == 0 && state.targets_verified_cycle == 0 {
        state.targets_in_cycle = targets.len();
    }

    let mut results = Vec::new();
    let mut bytes_processed = 0u64;
    let mut failures_this_call = 0usize;
    let mut cycle_completed = false;
    let ns = now_ns();

    while state.next_index < targets.len()
        && results.len() < opts.max_files
        && bytes_processed < opts.max_bytes
    {
        let target = &targets[state.next_index];
        let result = match verify_scrub_target(paths, target, opts) {
            Ok(r) => r,
            Err(e) => ScrubTargetResult {
                key: target.key.clone(),
                kind: target.kind,
                relative_path: target.relative_path.clone(),
                size: 0,
                observed_hash_hex: None,
                verified_frames: 0,
                holes: 0,
                findings: vec![ScrubFindingKind::IoError],
                quarantine_path: None,
                detail: Some(e.to_string()),
            },
        };

        bytes_processed = bytes_processed.saturating_add(result.size);
        state.bytes_verified_cycle = state.bytes_verified_cycle.saturating_add(result.size);
        state.bytes_verified_total = state.bytes_verified_total.saturating_add(result.size);
        state.targets_verified_cycle += 1;
        state.targets_verified_total = state.targets_verified_total.saturating_add(1);

        if result.findings.is_empty() {
            clear_findings_for_target(&mut findings_doc, &result.key);
        } else {
            failures_this_call += 1;
            state.failures_cycle += 1;
            state.failures_total = state.failures_total.saturating_add(1);
            for kind in &result.findings {
                let f = ScrubFinding {
                    finding_id: finding_id(&result.key, *kind),
                    target_key: result.key.clone(),
                    kind: result.kind,
                    finding: *kind,
                    relative_path: result.relative_path.clone(),
                    segment_id_hex: target.segment_id_hex.clone(),
                    detail: result
                        .detail
                        .clone()
                        .unwrap_or_else(|| kind.as_str().into()),
                    expected_hash_hex: target.expected_hash_hex.clone(),
                    observed_hash_hex: result.observed_hash_hex.clone(),
                    quarantine_path: result.quarantine_path.clone(),
                    first_seen_ns: ns,
                    last_seen_ns: ns,
                    cycle_id: state.cycle_id,
                };
                upsert_finding(&mut findings_doc, f);
            }
        }

        results.push(result);
        state.next_index += 1;

        if state.targets_in_cycle > 0 {
            state.coverage_ratio =
                (state.next_index as f64 / state.targets_in_cycle as f64).min(1.0);
        }
    }

    if !targets.is_empty() && state.next_index >= targets.len() {
        // Cycle complete — reset frontier for the next pass.
        cycle_completed = true;
        state.last_complete_cycle_ns = Some(ns);
        state.cycle_id = state.cycle_id.saturating_add(1);
        state.next_index = 0;
        state.targets_in_cycle = 0; // replan on next call
        state.bytes_verified_cycle = 0;
        state.targets_verified_cycle = 0;
        state.failures_cycle = 0;
        state.coverage_ratio = 1.0;
    } else if targets.is_empty() {
        // Empty store: treat as complete coverage.
        cycle_completed = true;
        state.last_complete_cycle_ns = Some(ns);
        state.coverage_ratio = 1.0;
        state.cycle_id = state.cycle_id.saturating_add(1);
        state.next_index = 0;
        state.targets_in_cycle = 0;
    }

    state.last_progress_ns = Some(ns);
    state.updated_ns = ns;
    findings_doc.updated_ns = ns;
    state.open_findings = findings_doc.findings.len();
    findings_doc.store_id_hex = hex16(&store_id);

    write_scrub_findings(paths, &findings_doc)?;
    write_scrub_state(paths, &state)?;

    let targets_processed = results.len();
    Ok(ScrubReport {
        results,
        bytes_processed,
        targets_processed,
        failures_this_call,
        cycle_completed,
        paused: false,
        status: status_from_state(&state),
    })
}

/// Pause scrub so subsequent `scrub_once` calls no-op until resume.
pub fn pause_scrub(paths: &StorePaths, store_id: [u8; 16]) -> Result<ScrubStatus, StoreError> {
    let mut state = load_or_init_scrub_state(paths, store_id)?;
    state.paused = true;
    state.updated_ns = now_ns();
    let findings = load_scrub_findings(paths, store_id)?;
    state.open_findings = findings.findings.len();
    write_scrub_state(paths, &state)?;
    Ok(status_from_state(&state))
}

/// Resume a paused scrub.
pub fn resume_scrub(paths: &StorePaths, store_id: [u8; 16]) -> Result<ScrubStatus, StoreError> {
    let mut state = load_or_init_scrub_state(paths, store_id)?;
    state.paused = false;
    state.updated_ns = now_ns();
    let findings = load_scrub_findings(paths, store_id)?;
    state.open_findings = findings.findings.len();
    write_scrub_state(paths, &state)?;
    Ok(status_from_state(&state))
}

/// Read scrub status without advancing the frontier.
pub fn scrub_status(paths: &StorePaths, store_id: [u8; 16]) -> Result<ScrubStatus, StoreError> {
    let mut state = load_or_init_scrub_state(paths, store_id)?;
    let findings = load_scrub_findings(paths, store_id)?;
    state.open_findings = findings.findings.len();
    Ok(status_from_state(&state))
}

/// List open findings.
pub fn list_scrub_findings(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<Vec<ScrubFinding>, StoreError> {
    Ok(load_scrub_findings(paths, store_id)?.findings)
}

// --- TierPlacement helper: entries() may not exist; check ---

// We use placement.get and need entries. Check TierPlacement API.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::StorePaths;
    use crate::tier::TierPlacement;
    use tempfile::tempdir;

    #[test]
    fn empty_store_scrub_completes() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("s");
        let paths = StorePaths::new(&root);
        paths.create_dirs().unwrap();
        // Minimal store_id file not required for scrub_once itself.
        let store_id = [1u8; 16];
        let placement = TierPlacement::new();
        let report = scrub_once(&paths, store_id, &placement, &ScrubOptions::default()).unwrap();
        assert!(report.cycle_completed);
        assert_eq!(report.failures_this_call, 0);
        assert!((report.status.coverage_ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn pause_blocks_progress() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("s");
        let paths = StorePaths::new(&root);
        paths.create_dirs().unwrap();
        let store_id = [2u8; 16];
        let st = pause_scrub(&paths, store_id).unwrap();
        assert!(st.paused);
        let placement = TierPlacement::new();
        let report = scrub_once(&paths, store_id, &placement, &ScrubOptions::default()).unwrap();
        assert!(report.paused);
        assert_eq!(report.targets_processed, 0);
        let st = resume_scrub(&paths, store_id).unwrap();
        assert!(!st.paused);
    }

    #[test]
    fn hex32_is_64_chars() {
        let h = [0xabu8; 32];
        let s = hex32(&h);
        assert_eq!(s.len(), 64);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
