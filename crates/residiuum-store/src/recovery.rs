//! Evidence-preserving salvage and recovery manifests (DEF-011 / DX_SPEC §13.4).
//!
//! Salvage copies **verified frame bytes** without re-encoding. Holes, corrupt
//! candidates, and scan parameters are recorded in a destination recovery
//! manifest. Live-state materialization (re-put) lives separately as
//! [`crate::Store::export_live_state`].

use crate::error::StoreError;
use crate::layout::{hex16, StorePaths};
use residiuum_format::{
    scan_forward, ByteRange, FrameKind, HoleReason, SafetyLimits, ScanRegion, WIRE_PROFILE_LABEL,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Filename of the hashed recovery manifest under `recovery/`.
pub const SALVAGE_MANIFEST_FILE: &str = "salvage-manifest.v1.json";

/// How non-destructive recovery materializes a destination store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SalvageMode {
    /// Copy verified frames (and record holes) without re-encoding.
    Evidence,
    /// Re-append live logical payloads with new store/event lineage.
    LiveStateExport,
}

/// One verified frame copied from source to destination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameEvidence {
    /// Half-open byte range in the source file.
    pub source_start: u64,
    /// Exclusive end offset in the source file.
    pub source_end: u64,
    /// Offset of the same bytes in the destination segment file.
    pub dest_offset: u64,
    /// Encoded frame length.
    pub frame_len: u64,
    /// Frame kind byte.
    pub frame_kind: u8,
    /// Known core kind name when assigned.
    pub frame_kind_name: Option<String>,
    /// Event / object id from the frame header (hex).
    pub event_id_hex: String,
    /// BLAKE3-256 of the stored body (hex).
    pub body_hash_hex: String,
}

/// One non-verified region observed during the source scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoleEvidence {
    /// Inclusive start offset in the source file.
    pub start: u64,
    /// Exclusive end offset in the source file.
    pub end: u64,
    /// Classification of the hole.
    pub reason: String,
}

/// Per-source-file scan and mapping evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFileEvidence {
    /// Path relative to the source store root (`segments/….residiuum`, `active/active.residiuum`).
    pub source_relative: String,
    /// Source file length in bytes.
    pub source_len: u64,
    /// Destination relative path that received verified frames, if any.
    pub dest_relative: Option<String>,
    /// Verified frames copied (source ranges → dest offsets).
    pub verified_frames: Vec<FrameEvidence>,
    /// Holes / corrupt candidates left in the source (not rewritten into dest).
    pub holes: Vec<HoleEvidence>,
}

/// Hashed recovery manifest written beside the destination store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryManifest {
    /// Manifest schema version.
    pub format_version: u32,
    /// Recovery mode used.
    pub mode: SalvageMode,
    /// Absolute or operator-supplied source root (display form).
    pub source_root: String,
    /// Destination store root (display form).
    pub destination_root: String,
    /// Source `store_id` (hex).
    pub source_store_id_hex: String,
    /// Destination `store_id` (hex).
    pub dest_store_id_hex: String,
    /// Wire profile label of this build.
    pub wire_profile: String,
    /// Tool identity that produced the manifest.
    pub tool: String,
    /// Safety limit snapshot applied during the scan.
    pub limits: LimitsSnapshot,
    /// Per-file evidence.
    pub files: Vec<SourceFileEvidence>,
    /// Total verified frames copied.
    pub frames_copied: u64,
    /// Total holes recorded.
    pub holes_recorded: u64,
    /// Live subjects after destination index rebuild (when available).
    pub live_subjects: usize,
    /// BLAKE3-256 hex of the canonical manifest body without this field.
    pub content_hash_hex: String,
}

/// Safety limits included in the recovery manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitsSnapshot {
    /// Maximum envelope length.
    pub max_envelope_len: u32,
    /// Maximum body length.
    pub max_body_len: u64,
    /// Maximum total frame length.
    pub max_frame_len: u64,
}

impl LimitsSnapshot {
    /// Capture current [`SafetyLimits`].
    pub fn from_limits(limits: SafetyLimits) -> Self {
        Self {
            max_envelope_len: limits.max_envelope_len,
            max_body_len: limits.max_body_len,
            max_frame_len: limits.max_frame_len,
        }
    }
}

/// Path of the salvage recovery manifest under a store root.
pub fn salvage_manifest_path(paths: &StorePaths) -> PathBuf {
    paths.recovery_dir().join(SALVAGE_MANIFEST_FILE)
}

/// Write verified frames from `source_paths` into a prepared destination store.
///
/// Destination must already have `store-info/` and empty segment trees from
/// [`crate::Store::create`]. Frames land only under `segments/` so a later
/// open does not re-encode them through the active-segment rebuild path.
///
/// Returns the manifest (with `content_hash_hex` filled) and aggregate counts.
pub fn copy_verified_frames(
    source_root: &Path,
    source_store_id: [u8; 16],
    dest_paths: &StorePaths,
    dest_store_id: [u8; 16],
    source_segment_files: &[(String, PathBuf)],
    limits: SafetyLimits,
) -> Result<(RecoveryManifest, u64, u64), StoreError> {
    let mut files = Vec::new();
    let mut frames_copied = 0u64;
    let mut holes_recorded = 0u64;

    for (rel, src_path) in source_segment_files {
        let bytes = fs::read(src_path)?;
        let report = scan_forward(&bytes, limits);
        let mut frame_ev = Vec::new();
        let mut hole_ev = Vec::new();
        let mut out_buf = Vec::new();

        for region in &report.regions {
            match region {
                ScanRegion::VerifiedFrame { range, frame } => {
                    let start = range.start as usize;
                    let end = range.end as usize;
                    if end > bytes.len() || start > end {
                        return Err(StoreError::CorruptMeta(
                            "verified frame range outside source buffer",
                        ));
                    }
                    let slice = &bytes[start..end];
                    let dest_offset = out_buf.len() as u64;
                    out_buf.extend_from_slice(slice);
                    frames_copied += 1;
                    let kind_name = frame.header.known_kind().map(|k| format!("{k:?}"));
                    frame_ev.push(FrameEvidence {
                        source_start: range.start,
                        source_end: range.end,
                        dest_offset,
                        frame_len: range.len(),
                        frame_kind: frame.header.frame_kind,
                        frame_kind_name: kind_name,
                        event_id_hex: hex16(&frame.header.event_id),
                        body_hash_hex: hex32(&frame.body_hash),
                    });
                }
                ScanRegion::Hole { range, reason } => {
                    holes_recorded += 1;
                    hole_ev.push(HoleEvidence {
                        start: range.start,
                        end: range.end,
                        reason: hole_reason_label(reason),
                    });
                }
            }
        }

        let dest_relative = if out_buf.is_empty() {
            None
        } else {
            let dest_name = dest_segment_relative(rel, &out_buf);
            let dest_path = dest_paths.root.join(&dest_name);
            if let Some(parent) = dest_path.parent() {
                fs::create_dir_all(parent)?;
            }
            // Do not overwrite an existing sealed segment with different content.
            if dest_path.exists() {
                let existing = fs::read(&dest_path)?;
                if existing != out_buf {
                    return Err(StoreError::AlreadyExists(dest_path));
                }
            } else {
                let mut f = fs::File::create(&dest_path)?;
                f.write_all(&out_buf)?;
                f.sync_all()?;
            }
            Some(dest_name)
        };

        files.push(SourceFileEvidence {
            source_relative: rel.clone(),
            source_len: bytes.len() as u64,
            dest_relative,
            verified_frames: frame_ev,
            holes: hole_ev,
        });
    }

    let mut manifest = RecoveryManifest {
        format_version: 1,
        mode: SalvageMode::Evidence,
        source_root: source_root.display().to_string(),
        destination_root: dest_paths.root.display().to_string(),
        source_store_id_hex: hex16(&source_store_id),
        dest_store_id_hex: hex16(&dest_store_id),
        wire_profile: WIRE_PROFILE_LABEL.to_string(),
        tool: "residiuum-store".to_string(),
        limits: LimitsSnapshot::from_limits(limits),
        files,
        frames_copied,
        holes_recorded,
        live_subjects: 0,
        content_hash_hex: String::new(),
    };
    manifest.content_hash_hex = hash_manifest(&manifest)?;
    Ok((manifest, frames_copied, holes_recorded))
}

/// Persist the recovery manifest under `recovery/` (atomic durable, DEF-021).
pub fn write_recovery_manifest(
    paths: &StorePaths,
    manifest: &RecoveryManifest,
) -> Result<PathBuf, StoreError> {
    fs::create_dir_all(paths.recovery_dir())?;
    let path = salvage_manifest_path(paths);
    let mut body = serde_json::to_vec_pretty(manifest).map_err(|e| {
        StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("serialize recovery manifest: {e}"),
        ))
    })?;
    body.push(b'\n');
    crate::atomic_file::write_atomic_keep_previous(&path, &body)?;
    Ok(path)
}

/// Load a recovery manifest if present.
pub fn try_load_recovery_manifest(
    paths: &StorePaths,
) -> Result<Option<RecoveryManifest>, StoreError> {
    let path = salvage_manifest_path(paths);
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(&path)?;
    let m: RecoveryManifest = serde_json::from_slice(&bytes).map_err(|e| {
        StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("parse recovery manifest: {e}"),
        ))
    })?;
    Ok(Some(m))
}

fn dest_segment_relative(source_relative: &str, frame_bytes: &[u8]) -> String {
    // Preserve sealed segment filenames so identity remains operator-visible.
    if let Some(name) = source_relative.strip_prefix("segments/") {
        if name.ends_with(".residiuum") && !name.contains('/') {
            return format!("segments/{name}");
        }
    }
    // Active (or other) sources: deterministic sealed name from content hash.
    let hash = blake3::hash(frame_bytes);
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    format!("segments/{}.residiuum", hex16(&id))
}

fn hole_reason_label(reason: &HoleReason) -> String {
    match reason {
        HoleReason::UnclassifiedGarbage => "unclassified_garbage".into(),
        HoleReason::CorruptCandidate {
            candidate_offset,
            error,
        } => format!("corrupt_candidate@{candidate_offset}:{error}"),
        HoleReason::DamagedCandidate {
            candidate_offset,
            error,
        } => format!("damaged_candidate@{candidate_offset}:{error}"),
    }
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Hash the manifest with `content_hash_hex` cleared for stability.
fn hash_manifest(manifest: &RecoveryManifest) -> Result<String, StoreError> {
    let mut for_hash = manifest.clone();
    for_hash.content_hash_hex.clear();
    let body = serde_json::to_vec(&for_hash).map_err(|e| {
        StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("serialize recovery manifest for hash: {e}"),
        ))
    })?;
    Ok(hex32(blake3::hash(&body).as_bytes()))
}

/// Re-encode a [`ByteRange`] for callers that need the format type.
#[allow(dead_code)]
pub fn byte_range(start: u64, end: u64) -> ByteRange {
    ByteRange::new(start, end)
}

/// Known kind helper for tests / diagnostics.
#[allow(dead_code)]
pub fn is_item_event_kind(kind: u8) -> bool {
    FrameKind::from_u8(kind).ok() == Some(FrameKind::ItemEvent)
}
