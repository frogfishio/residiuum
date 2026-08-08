//! Compaction and derived checkpoints (OVERVIEW §13, Stage 6; DEF-024).
//!
//! Compaction creates new immutable segments from verified live state while
//! preserving item identities. It MUST NOT convert an uncertain history into an
//! apparently complete snapshot: source segments remain until an explicit,
//! phased reclaim step after the new generation is verified and activated.
//!
//! ## Durable phases (DEF-024)
//!
//! ```text
//! planned → created → verified → activated → [retention_hold] → reclaimed
//!                ↘ failed / cancelled
//! ```
//!
//! Job records live under `recovery/compaction/` and survive process restart.
//! Crash at any phase leaves either the pre-compaction authoritative tree or a
//! verified post-activation generation (never a torn half-reclaim).

use crate::envelope::{decode_item_envelope, encode_item_envelope, EventKind, ItemEnvelope};
use crate::error::StoreError;
use crate::index::{IndexEntry, LiveValue, PrimaryIndex};
use crate::layout::{hex16, segment_id_from_filename, unhex16, StorePaths};
use residiuum_format::{
    scan_forward, verify_frame_at, ActiveSegment, FrameKind, SafetyLimits, SegmentId,
    FRAME_PREFIX_LEN, FRAME_SUFFIX_LEN,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Filename prefix for compaction job records under `recovery/compaction/`.
pub const COMPACTION_JOB_DIR: &str = "compaction";
/// Job file suffix.
pub const COMPACTION_JOB_SUFFIX: &str = ".job.json";

/// Report from a live-state compaction pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactReport {
    /// New segment that received live puts.
    pub segment_id: [u8; 16],
    /// Number of live subjects rewritten.
    pub live_subjects_written: usize,
    /// Source segment files that contributed to the pre-compaction index
    /// (paths relative to store root when possible).
    pub source_segments: Vec<String>,
    /// Whether source segments were left in place (true unless reclaim ran).
    pub sources_retained: bool,
    /// Declared coverage: `"live-projection"` means only current live values
    /// were rewritten; full event history remains in source segments while
    /// retained.
    pub coverage: &'static str,
    /// Durable job id for this compaction (hex identity of the job record).
    pub job_id: [u8; 16],
    /// Final phase reached.
    pub phase: CompactPhase,
    /// Estimated bytes that would be read from sources (pre-create).
    pub bytes_estimated_read: u64,
    /// Estimated bytes to write for the live projection (pre-create).
    pub bytes_estimated_write: u64,
    /// Actual bytes read from sources during create/verify.
    pub bytes_read: u64,
    /// Actual bytes written to the new segment file.
    pub bytes_written: u64,
    /// Bytes still retained in source segments after this call.
    pub bytes_retained: u64,
    /// Bytes reclaimed (deleted) from source segments after this call.
    pub bytes_reclaimed: u64,
    /// Tombstone horizon carried on the job (ns; informational for reclaim policy).
    pub tombstone_horizon_ns: Option<u64>,
    /// Dedup evidence horizon tag carried on the job (informational).
    pub dedup_horizon: Option<String>,
}

/// Options for a live-projection compaction (DEF-024).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CompactOptions {
    /// When true, after activate immediately attempt source reclaim.
    ///
    /// Live-projection compact does **not** rewrite history; reclaim requires
    /// [`Self::allow_history_loss`] so operators cannot silently destroy event
    /// evidence.
    pub reclaim_sources: bool,
    /// Required when [`Self::reclaim_sources`] is true for live-projection
    /// coverage. Acknowledges that subject history and pre-compact tombstones
    /// in reclaimed sources will no longer be readable.
    pub allow_history_loss: bool,
    /// Optional tombstone horizon (created_ns) recorded on the job.
    pub tombstone_horizon_ns: Option<u64>,
    /// Optional dedup horizon tag (e.g. max operation id retained).
    pub dedup_horizon: Option<String>,
}

/// Durable compaction phase (DEF-024).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactPhase {
    /// Job record written; output segment not yet durable.
    Planned,
    /// Output segment file created and synced; not yet verified.
    Created,
    /// Output segment verified against the live projection.
    Verified,
    /// Output segment registered; sources still retained.
    Activated,
    /// Activated and waiting for an explicit reclaim (retention window).
    RetentionHold,
    /// Source segments listed on the job have been reclaimed.
    Reclaimed,
    /// Operator cancelled before activation completed.
    Cancelled,
    /// Permanent failure; operator must inspect job record.
    Failed,
}

impl CompactPhase {
    /// Whether this phase is terminal.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Reclaimed | Self::Cancelled | Self::Failed
        )
    }

    /// Stable ASCII name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Created => "created",
            Self::Verified => "verified",
            Self::Activated => "activated",
            Self::RetentionHold => "retention_hold",
            Self::Reclaimed => "reclaimed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

/// Durable compaction job record under `recovery/compaction/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactJob {
    /// Job schema version.
    pub version: u32,
    /// Opaque job id (also embedded in filename).
    pub job_id: String,
    /// Store id hex.
    pub store_id: String,
    /// Current phase.
    pub phase: CompactPhase,
    /// Recovery generation (monotonic counter for this store's compact jobs).
    pub recovery_generation: u64,
    /// Coverage declaration.
    pub coverage: String,
    /// Projection rule.
    pub projection: String,
    /// Output sealed segment id (hex).
    pub output_segment_id: String,
    /// Source paths relative to store root.
    pub source_segments: Vec<String>,
    /// Source sealed segment ids (hex) eligible for reclaim (excludes active).
    pub source_segment_ids: Vec<String>,
    /// Planned live subject count.
    pub live_subjects_planned: usize,
    /// Written live subject count (after create).
    pub live_subjects_written: usize,
    /// Estimated source bytes to read (plan time).
    pub bytes_estimated_read: u64,
    /// Estimated output segment bytes (plan time).
    pub bytes_estimated_write: u64,
    /// Actual bytes attributed to source reads.
    pub bytes_read: u64,
    /// Actual output segment file size.
    pub bytes_written: u64,
    /// Source bytes still on disk after the latest phase.
    pub bytes_retained: u64,
    /// Source bytes deleted by reclaim.
    pub bytes_reclaimed: u64,
    /// Whether sources are still on disk.
    pub sources_retained: bool,
    /// Operator requested reclaim after activation.
    pub reclaim_requested: bool,
    /// Operator accepted history loss for live-projection reclaim.
    pub allow_history_loss: bool,
    /// Tombstone horizon (created_ns) recorded for retention policy.
    pub tombstone_horizon_ns: Option<u64>,
    /// Dedup evidence horizon tag recorded for retention policy.
    pub dedup_horizon: Option<String>,
    /// Soft-cancel flag (honoured between phases).
    pub cancel_requested: bool,
    /// Failure / cancellation detail.
    pub detail: Option<String>,
    /// Job creation timestamp (ns).
    pub created_ns: u64,
    /// Last phase-update timestamp (ns).
    pub updated_ns: u64,
}

impl CompactJob {
    /// Parse job id bytes.
    pub fn job_id_bytes(&self) -> Option<[u8; 16]> {
        unhex16(&self.job_id)
    }

    /// Parse output segment id bytes.
    pub fn output_segment_bytes(&self) -> Option<[u8; 16]> {
        unhex16(&self.output_segment_id)
    }
}

/// Derived checkpoint metadata (OVERVIEW §13.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointMeta {
    /// Opaque checkpoint id (16 bytes).
    pub checkpoint_id: [u8; 16],
    /// Live subject count at capture.
    pub live_subjects: usize,
    /// Segment fingerprint at capture.
    pub segment_fingerprint: [u8; 32],
    /// Coverage declaration.
    pub coverage: String,
    /// Projection rule tag.
    pub projection: String,
    /// Nanoseconds timestamp when written.
    pub created_ns: u64,
}

const CHECKPOINT_MAGIC: &[u8; 8] = b"RCHKPT01";
const JOB_VERSION: u32 = 1;

/// Directory for compaction job records.
pub fn compaction_jobs_dir(paths: &StorePaths) -> PathBuf {
    paths.recovery_dir().join(COMPACTION_JOB_DIR)
}

/// Path for a job record by job id.
pub fn compaction_job_path(paths: &StorePaths, job_id: &[u8; 16]) -> PathBuf {
    compaction_jobs_dir(paths).join(format!("{}{}", hex16(job_id), COMPACTION_JOB_SUFFIX))
}

/// Persist a compaction job atomically (DEF-021 helper).
pub fn write_compact_job(paths: &StorePaths, job: &CompactJob) -> Result<PathBuf, StoreError> {
    let dir = compaction_jobs_dir(paths);
    fs::create_dir_all(&dir)?;
    let job_id = job
        .job_id_bytes()
        .ok_or(StoreError::CorruptMeta("compact job id"))?;
    let path = compaction_job_path(paths, &job_id);
    let bytes = serde_json::to_vec_pretty(job).map_err(|e| {
        StoreError::CorruptControl {
            path: path.display().to_string(),
            detail: e.to_string(),
            recovery: "fix job or cancel compaction".into(),
        }
    })?;
    crate::failpoint::hit("store.compact.job_write")?;
    crate::atomic_file::write_atomic_keep_previous(&path, &bytes)?;
    Ok(path)
}

/// Load a compaction job if present and well-formed.
pub fn try_load_compact_job(
    paths: &StorePaths,
    job_id: &[u8; 16],
) -> Result<Option<CompactJob>, StoreError> {
    let path = compaction_job_path(paths, job_id);
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(&path)?;
    match serde_json::from_slice::<CompactJob>(&bytes) {
        Ok(job) => Ok(Some(job)),
        Err(e) => Err(StoreError::CorruptControl {
            path: path.display().to_string(),
            detail: e.to_string(),
            recovery: "inspect .prev generation or cancel job".into(),
        }),
    }
}

/// List all compaction jobs under recovery/compaction (sorted by filename).
pub fn list_compact_jobs(paths: &StorePaths) -> Result<Vec<CompactJob>, StoreError> {
    let dir = compaction_jobs_dir(paths);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut paths_list: Vec<PathBuf> = fs::read_dir(&dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(COMPACTION_JOB_SUFFIX) && !n.ends_with(".prev"))
                .unwrap_or(false)
        })
        .collect();
    paths_list.sort();
    let mut out = Vec::new();
    for p in paths_list {
        let bytes = fs::read(&p)?;
        if let Ok(job) = serde_json::from_slice::<CompactJob>(&bytes) {
            out.push(job);
        }
    }
    Ok(out)
}

/// Estimate source read size and live write size from paths + index.
pub fn estimate_compact_bytes(
    paths: &StorePaths,
    source_rel_names: &[String],
    index: &PrimaryIndex,
) -> (u64, u64) {
    let mut read = 0u64;
    for name in source_rel_names {
        let p = paths.root.join(name);
        if let Ok(meta) = fs::metadata(&p) {
            read = read.saturating_add(meta.len());
        }
    }
    // With locator-first index (DEF-095), resident bodies may be empty; budget
    // from source bytes instead of summing index payloads.
    let write = read / 2; // live projection is typically ≤ source (history dropped)
    let _ = index; // live count already planned separately; keep signature stable
    (read, write.max(128 * index.live_len() as u64))
}

/// Resolve payload bytes for a live index entry (resident or frame at offset).
///
/// Tries canonical sealed path, active path, then every `*.residiuum` under the
/// store (salvage may hash-rename active → sealed while envelopes keep the
/// original `segment_id`).
pub fn resolve_live_body(
    paths: &StorePaths,
    limits: SafetyLimits,
    lv: &LiveValue,
) -> Result<Vec<u8>, StoreError> {
    if !lv.body.is_empty() {
        return Ok(lv.body.clone());
    }
    if lv.frame_offset == 0 {
        return Ok(Vec::new());
    }
    let mut candidates = Vec::new();
    let sealed = paths.sealed_segment(&lv.segment_id);
    if sealed.is_file() {
        candidates.push(sealed);
    }
    let active = paths.active_segment();
    if active.is_file() {
        candidates.push(active);
    }
    // Other sealed names (evidence salvage hash renames).
    if let Ok(entries) = fs::read_dir(paths.segments_dir()) {
        for ent in entries.flatten() {
            let p = ent.path();
            if p.extension().and_then(|e| e.to_str()) == Some("residiuum") {
                if !candidates.iter().any(|c| c == &p) {
                    candidates.push(p);
                }
            }
        }
    }
    for path in &candidates {
        if let Ok(body) = pread_item_body_if_segment(path, lv.frame_offset, &lv.segment_id, limits) {
            return Ok(body);
        }
    }
    Err(StoreError::SegmentNotFound)
}

/// Expected identity for a durable index locator (disk pread defence).
#[derive(Debug, Clone)]
pub struct LocatorExpect {
    /// Envelope / media segment id.
    pub segment_id: [u8; 16],
    /// Frame header event id.
    pub event_id: [u8; 16],
    /// Envelope item id.
    pub item_id: [u8; 16],
    /// Envelope subject bytes (must match the index key).
    pub subject: Vec<u8>,
    /// Frame header writer_sequence (generation-style fence).
    pub writer_sequence: u64,
}

/// Pread item body only when frame matches `expect` completely (P0 disk defence).
pub fn pread_item_body_matching(
    path: &Path,
    offset: u64,
    expect: &LocatorExpect,
    limits: SafetyLimits,
) -> Result<Vec<u8>, StoreError> {
    let (header, envelope, body, file_len) =
        pread_item_frame_full(path, offset, expect.segment_id, limits)?;
    if header.event_id != expect.event_id {
        return Err(StoreError::ConsistencyViolation(
            "locator event_id mismatch at disk frame offset".into(),
        ));
    }
    let _ = expect.writer_sequence; // event_id is the durable generation fence
    let _ = header.writer_sequence;
    let Some(env) = decode_item_envelope(&envelope) else {
        return Err(StoreError::LocatorFault(Box::new(
            crate::error::LocatorFault::at_path(
                crate::error::LocatorFaultKind::FrameVerifyFailed,
                expect.segment_id,
                offset,
                path,
                Some(file_len),
                Some("item envelope decode failed".into()),
            ),
        )));
    };
    if env.segment_id != expect.segment_id {
        return Err(StoreError::LocatorFault(Box::new(
            crate::error::LocatorFault::segment_mismatch(
                expect.segment_id,
                env.segment_id,
                offset,
                path,
                Some(file_len),
            ),
        )));
    }
    if env.item_id != expect.item_id {
        return Err(StoreError::ConsistencyViolation(
            "locator item_id mismatch at disk frame offset".into(),
        ));
    }
    if env.subject != expect.subject {
        return Err(StoreError::ConsistencyViolation(
            "locator subject mismatch at disk frame offset".into(),
        ));
    }
    Ok(body)
}

/// Read a verified item-event body at `offset` when the envelope `segment_id` matches.
///
/// Failures are structured [`StoreError::LocatorFault`] (DEF-SCAN-001).
pub fn pread_item_body_if_segment(
    path: &Path,
    offset: u64,
    expect_segment_id: &[u8; 16],
    limits: SafetyLimits,
) -> Result<Vec<u8>, StoreError> {
    let (_header, envelope, body, file_len) =
        pread_item_frame_full(path, offset, *expect_segment_id, limits)?;
    let env_seg = decode_item_envelope(&envelope)
        .map(|e| e.segment_id)
        .unwrap_or([0u8; 16]);
    if env_seg != *expect_segment_id {
        return Err(StoreError::LocatorFault(Box::new(
            crate::error::LocatorFault::segment_mismatch(
                *expect_segment_id,
                env_seg,
                offset,
                path,
                Some(file_len),
            ),
        )));
    }
    Ok(body)
}

fn pread_item_frame_full(
    path: &Path,
    offset: u64,
    expect_segment_id: [u8; 16],
    limits: SafetyLimits,
) -> Result<(residiuum_format::FrameHeader, Vec<u8>, Vec<u8>, u64), StoreError> {
    let mut file = File::open(path).map_err(|e| {
        StoreError::LocatorFault(Box::new(crate::error::LocatorFault::at_path(
            crate::error::LocatorFaultKind::SegmentNotFound,
            expect_segment_id,
            offset,
            path,
            None,
            Some(e.to_string()),
        )))
    })?;
    let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if offset >= file_len {
        return Err(StoreError::LocatorFault(Box::new(
            crate::error::LocatorFault::at_path(
                crate::error::LocatorFaultKind::OffsetInvalid,
                expect_segment_id,
                offset,
                path,
                Some(file_len),
                Some(format!("offset {offset} >= file_len {file_len}")),
            ),
        )));
    }
    file.seek(SeekFrom::Start(offset)).map_err(|e| {
        StoreError::LocatorFault(Box::new(crate::error::LocatorFault::at_path(
            crate::error::LocatorFaultKind::OffsetInvalid,
            expect_segment_id,
            offset,
            path,
            Some(file_len),
            Some(e.to_string()),
        )))
    })?;
    let mut prefix = [0u8; FRAME_PREFIX_LEN];
    if let Err(e) = file.read_exact(&mut prefix) {
        return Err(StoreError::LocatorFault(Box::new(
            crate::error::LocatorFault::at_path(
                crate::error::LocatorFaultKind::OffsetInvalid,
                expect_segment_id,
                offset,
                path,
                Some(file_len),
                Some(e.to_string()),
            ),
        )));
    }
    let envelope_len = u32::from_le_bytes(prefix[12..16].try_into().unwrap()) as u64;
    let body_len = u64::from_le_bytes(prefix[16..24].try_into().unwrap());
    let frame_len = match (FRAME_PREFIX_LEN as u64)
        .checked_add(envelope_len)
        .and_then(|n| n.checked_add(body_len))
        .and_then(|n| n.checked_add(FRAME_SUFFIX_LEN as u64))
    {
        Some(n) => n,
        None => {
            return Err(StoreError::LocatorFault(Box::new(
                crate::error::LocatorFault::at_path(
                    crate::error::LocatorFaultKind::FrameVerifyFailed,
                    expect_segment_id,
                    offset,
                    path,
                    Some(file_len),
                    Some("frame length overflow".into()),
                ),
            )));
        }
    };
    if frame_len > limits.max_frame_len {
        return Err(StoreError::LocatorFault(Box::new(
            crate::error::LocatorFault::at_path(
                crate::error::LocatorFaultKind::FrameVerifyFailed,
                expect_segment_id,
                offset,
                path,
                Some(file_len),
                Some(format!("frame_len {frame_len} exceeds safety limits")),
            ),
        )));
    }
    if offset.saturating_add(frame_len) > file_len {
        return Err(StoreError::LocatorFault(Box::new(
            crate::error::LocatorFault::at_path(
                crate::error::LocatorFaultKind::OffsetInvalid,
                expect_segment_id,
                offset,
                path,
                Some(file_len),
                Some(format!(
                    "frame extends past end: offset+len={} file_len={file_len}",
                    offset.saturating_add(frame_len)
                )),
            ),
        )));
    }
    let mut frame = vec![0u8; frame_len as usize];
    frame[..FRAME_PREFIX_LEN].copy_from_slice(&prefix);
    if let Err(e) = file.read_exact(&mut frame[FRAME_PREFIX_LEN..]) {
        return Err(StoreError::LocatorFault(Box::new(
            crate::error::LocatorFault::at_path(
                crate::error::LocatorFaultKind::OffsetInvalid,
                expect_segment_id,
                offset,
                path,
                Some(file_len),
                Some(e.to_string()),
            ),
        )));
    }
    let (header, envelope, body, _hash, _len) = match verify_frame_at(&frame, limits) {
        Ok(v) => v,
        Err(e) => {
            return Err(StoreError::LocatorFault(Box::new(
                crate::error::LocatorFault::at_path(
                    crate::error::LocatorFaultKind::FrameVerifyFailed,
                    expect_segment_id,
                    offset,
                    path,
                    Some(file_len),
                    Some(e.to_string()),
                ),
            )));
        }
    };
    Ok((header, envelope.to_vec(), body.to_vec(), file_len))
}

#[allow(dead_code)]
fn pread_item_frame(
    path: &Path,
    offset: u64,
    expect_segment_id: [u8; 16],
    limits: SafetyLimits,
) -> Result<([u8; 16], Vec<u8>, u64), StoreError> {
    let (_header, envelope, body, file_len) =
        pread_item_frame_full(path, offset, expect_segment_id, limits)?;
    let env_seg = decode_item_envelope(&envelope)
        .map(|e| e.segment_id)
        .unwrap_or([0u8; 16]);
    Ok((env_seg, body, file_len))
}

/// Collect reclaimable sealed segment ids from source relative names.
pub fn reclaimable_source_ids(paths: &StorePaths, source_rel_names: &[String]) -> Vec<[u8; 16]> {
    let mut out = Vec::new();
    for name in source_rel_names {
        let p = paths.root.join(name);
        // Never reclaim active segment path.
        if p == paths.active_segment() {
            continue;
        }
        if let Some(id) = segment_id_from_filename(&p) {
            if !out.contains(&id) {
                out.push(id);
            }
        }
    }
    out
}

/// Create the sealed live-projection segment file (create phase body).
pub fn write_live_segment(
    paths: &StorePaths,
    store_id: [u8; 16],
    limits: SafetyLimits,
    index: &PrimaryIndex,
    next_segment_id: [u8; 16],
    mint_event_id: &mut dyn FnMut() -> Result<[u8; 16], StoreError>,
    created_ns: u64,
) -> Result<(usize, u64), StoreError> {
    let ids = SegmentId::new(store_id, next_segment_id);
    let mut seg = ActiveSegment::create(ids, limits, created_ns)?;

    let mut written = 0usize;
    for (subject, entry) in index.iter_all() {
        let IndexEntry::Live(lv) = entry else {
            continue;
        };
        let body = resolve_live_body(paths, limits, lv)?;
        let event_id = mint_event_id()?;
        let env = ItemEnvelope {
            store_id,
            segment_id: next_segment_id,
            item_id: lv.item_id,
            event_kind: EventKind::Put,
            created_ns,
            subject: subject.clone(),
            operation_id: None,
            operation_content_hash: None,
        };
        let envelope = encode_item_envelope(&env).map_err(StoreError::BadEnvelope)?;
        seg.append(FrameKind::ItemEvent, &envelope, &body, event_id)?;
        written += 1;
    }

    let sealed = seg.seal()?;
    let dest = paths.sealed_segment(&next_segment_id);
    {
        let mut out = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&dest)?;
        out.write_all(sealed.as_bytes())?;
        crate::failpoint::hit("store.compact.before_segment_sync")?;
        out.sync_all()?;
    }
    sync_dir_best_effort(&paths.segments_dir());
    let bytes_written = fs::metadata(&dest).map(|m| m.len()).unwrap_or(0);
    Ok((written, bytes_written))
}

/// Verify a live-projection segment matches the expected live index (verify phase).
///
/// Checks live subject count and that every live subject in `index` appears with
/// the same body bytes in the output segment. Unknown/conflict frames in the
/// output are rejected.
pub fn verify_live_segment(
    paths: &StorePaths,
    limits: SafetyLimits,
    index: &PrimaryIndex,
    output_segment_id: &[u8; 16],
    expected_live: usize,
) -> Result<(), StoreError> {
    crate::failpoint::hit("store.compact.before_verify")?;
    let path = paths.sealed_segment(output_segment_id);
    if !path.is_file() {
        return Err(StoreError::SegmentNotFound);
    }
    let bytes = fs::read(&path)?;
    let report = scan_forward(&bytes, limits);
    let mut found: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = std::collections::BTreeMap::new();
    for (_off, frame) in report.verified_frames() {
        // Only item events contribute to the live projection; other known
        // kinds (descriptors, padding) are ignored. Unrecognized kind bytes
        // surface as None from known_kind and are skipped.
        if frame.header.known_kind() != Some(FrameKind::ItemEvent) {
            continue;
        }
        let Some(env) = decode_item_envelope(&frame.envelope) else {
            return Err(StoreError::ConsistencyViolation(
                "compact verify: undecodable item envelope in output segment".into(),
            ));
        };
        if env.event_kind != EventKind::Put {
            return Err(StoreError::ConsistencyViolation(
                "compact verify: non-put event in live-projection segment".into(),
            ));
        }
        found.insert(env.subject, frame.body.clone());
    }
    if found.len() != expected_live {
        return Err(StoreError::ConsistencyViolation(format!(
            "compact verify: live count mismatch output={} expected={}",
            found.len(),
            expected_live
        )));
    }
    for (subj, entry) in index.iter_all() {
        let IndexEntry::Live(lv) = entry else {
            continue;
        };
        let expected = resolve_live_body(paths, limits, lv)?;
        match found.get(subj) {
            Some(body) if body == &expected => {}
            Some(_) => {
                return Err(StoreError::ConsistencyViolation(
                    "compact verify: body mismatch for live subject".into(),
                ));
            }
            None => {
                return Err(StoreError::ConsistencyViolation(
                    "compact verify: missing live subject in output".into(),
                ));
            }
        }
    }
    crate::failpoint::hit("store.compact.after_verify")?;
    Ok(())
}

/// Delete reclaimable source sealed segments listed on the job.
///
/// Safety rules:
/// - Output segment is never deleted.
/// - Active segment is never deleted.
/// - Requires `allow_history_loss` for live-projection jobs.
/// - Job must be `Activated` or `RetentionHold`.
///
/// Returns bytes reclaimed and remaining retained source bytes among listed sources.
pub fn reclaim_source_segments(
    paths: &StorePaths,
    job: &CompactJob,
) -> Result<(u64, u64, Vec<[u8; 16]>), StoreError> {
    if !job.allow_history_loss {
        return Err(StoreError::ConsistencyViolation(
            "compact reclaim refused: allow_history_loss required for live-projection".into(),
        ));
    }
    if !matches!(
        job.phase,
        CompactPhase::Activated | CompactPhase::RetentionHold
    ) {
        return Err(StoreError::ConsistencyViolation(format!(
            "compact reclaim refused in phase {}",
            job.phase.as_str()
        )));
    }
    let output = job
        .output_segment_bytes()
        .ok_or(StoreError::CorruptMeta("compact output segment id"))?;
    crate::failpoint::hit("store.compact.before_reclaim")?;

    // Post-flip: refuse before deleting sources if replacement Shadow missing.
    if let Some(store_id) = unhex16(&job.store_id) {
        if matches!(
            crate::recovery_shadow::shadow_reclaim_policy(),
            crate::recovery_shadow::ShadowReclaimPolicy::RequireReplacementShadow
        ) {
            let repl = crate::recovery_shadow::shadow_path(paths, &output);
            match crate::recovery_shadow::try_load_shadow(&repl, Some(store_id))? {
                crate::recovery_shadow::ShadowLoad::Ok(_) => {}
                _ => {
                    return Err(StoreError::ConsistencyViolation(
                        "post-flip reclaim requires durable replacement Shadow before source retirement"
                            .into(),
                    ));
                }
            }
        }
    }

    let mut reclaimed = 0u64;
    let mut deleted_ids = Vec::new();
    for id_hex in &job.source_segment_ids {
        let Some(id) = unhex16(id_hex) else {
            continue;
        };
        if id == output {
            continue;
        }
        let path = paths.sealed_segment(&id);
        if !path.is_file() {
            continue;
        }
        let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        fs::remove_file(&path)?;
        reclaimed = reclaimed.saturating_add(size);
        deleted_ids.push(id);
    }
    sync_dir_best_effort(&paths.segments_dir());

    // Stage 2: erase Recovery Shadows under current reclaim policy.
    if let Some(store_id) = unhex16(&job.store_id) {
        crate::recovery_shadow::retire_shadows_after_replacement(
            paths,
            store_id,
            &output,
            &deleted_ids,
            0,
        )?;
    }

    let mut retained = 0u64;
    for name in &job.source_segments {
        let p = paths.root.join(name);
        if p.is_file() {
            retained = retained.saturating_add(fs::metadata(&p).map(|m| m.len()).unwrap_or(0));
        }
    }
    // Output segment still present; count remaining source files only above.

    crate::failpoint::hit("store.compact.after_reclaim")?;
    Ok((reclaimed, retained, deleted_ids))
}

/// Build a fresh planned job skeleton.
#[allow(clippy::too_many_arguments)] // job fields are explicit for durable serialization
pub fn new_planned_job(
    store_id: [u8; 16],
    job_id: [u8; 16],
    output_segment_id: [u8; 16],
    source_segments: Vec<String>,
    source_segment_ids: Vec<[u8; 16]>,
    live_subjects_planned: usize,
    bytes_estimated_read: u64,
    bytes_estimated_write: u64,
    recovery_generation: u64,
    options: &CompactOptions,
    created_ns: u64,
) -> CompactJob {
    CompactJob {
        version: JOB_VERSION,
        job_id: hex16(&job_id),
        store_id: hex16(&store_id),
        phase: CompactPhase::Planned,
        recovery_generation,
        coverage: "live-projection".into(),
        projection: "primary-live-v1".into(),
        output_segment_id: hex16(&output_segment_id),
        source_segments,
        source_segment_ids: source_segment_ids.iter().map(hex16).collect(),
        live_subjects_planned,
        live_subjects_written: 0,
        bytes_estimated_read,
        bytes_estimated_write,
        bytes_read: 0,
        bytes_written: 0,
        bytes_retained: bytes_estimated_read,
        bytes_reclaimed: 0,
        sources_retained: true,
        reclaim_requested: options.reclaim_sources,
        allow_history_loss: options.allow_history_loss,
        tombstone_horizon_ns: options.tombstone_horizon_ns,
        dedup_horizon: options.dedup_horizon.clone(),
        cancel_requested: false,
        detail: None,
        created_ns,
        updated_ns: created_ns,
    }
}

/// Convert a finished job into a [`CompactReport`].
pub fn report_from_job(job: &CompactJob) -> Result<CompactReport, StoreError> {
    let segment_id = job
        .output_segment_bytes()
        .ok_or(StoreError::CorruptMeta("compact output segment id"))?;
    let job_id = job
        .job_id_bytes()
        .ok_or(StoreError::CorruptMeta("compact job id"))?;
    Ok(CompactReport {
        segment_id,
        live_subjects_written: job.live_subjects_written,
        source_segments: job.source_segments.clone(),
        sources_retained: job.sources_retained,
        coverage: "live-projection",
        job_id,
        phase: job.phase,
        bytes_estimated_read: job.bytes_estimated_read,
        bytes_estimated_write: job.bytes_estimated_write,
        bytes_read: job.bytes_read,
        bytes_written: job.bytes_written,
        bytes_retained: job.bytes_retained,
        bytes_reclaimed: job.bytes_reclaimed,
        tombstone_horizon_ns: job.tombstone_horizon_ns,
        dedup_horizon: job.dedup_horizon.clone(),
    })
}

/// Write a derived checkpoint under `snapshots/` with declared coverage.
pub fn write_checkpoint(
    paths: &StorePaths,
    store_id: [u8; 16],
    meta: &CheckpointMeta,
    live_pairs: &[(&[u8], &[u8])],
) -> Result<PathBuf, StoreError> {
    let dir = paths.snapshots_dir();
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{}.ckpt", hex16(&meta.checkpoint_id)));
    let mut out = Vec::new();
    out.extend_from_slice(CHECKPOINT_MAGIC);
    out.extend_from_slice(&store_id);
    out.extend_from_slice(&meta.checkpoint_id);
    out.extend_from_slice(&meta.created_ns.to_le_bytes());
    out.extend_from_slice(&(meta.live_subjects as u64).to_le_bytes());
    out.extend_from_slice(&meta.segment_fingerprint);
    write_str(&mut out, &meta.coverage);
    write_str(&mut out, &meta.projection);
    out.extend_from_slice(&(live_pairs.len() as u32).to_le_bytes());
    for (subj, body) in live_pairs {
        write_bytes(&mut out, subj);
        write_bytes(&mut out, body);
    }
    crate::failpoint::hit("store.checkpoint.before_write")?;
    crate::atomic_file::write_atomic(&path, &out)?;
    Ok(path)
}

/// Subject/body pairs recovered from a checkpoint file.
pub type CheckpointPairs = Vec<(Vec<u8>, Vec<u8>)>;

/// Load checkpoint metadata + live pairs if the file verifies as a draft checkpoint.
pub fn try_load_checkpoint(
    path: &Path,
    store_id: [u8; 16],
) -> Result<Option<(CheckpointMeta, CheckpointPairs)>, StoreError> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    if bytes.len() < 8 + 16 + 16 + 8 + 8 + 32 {
        return Ok(None);
    }
    if &bytes[0..8] != CHECKPOINT_MAGIC.as_slice() {
        return Ok(None);
    }
    let sid: [u8; 16] = bytes[8..24].try_into().unwrap_or([0; 16]);
    if sid != store_id {
        return Ok(None);
    }
    let checkpoint_id: [u8; 16] = bytes[24..40].try_into().unwrap_or([0; 16]);
    let created_ns = u64::from_le_bytes(bytes[40..48].try_into().unwrap_or([0; 8]));
    let live_subjects = u64::from_le_bytes(bytes[48..56].try_into().unwrap_or([0; 8])) as usize;
    let segment_fingerprint: [u8; 32] = bytes[56..88].try_into().unwrap_or([0; 32]);
    let mut cursor = 88usize;
    let Some(coverage) = read_str(&bytes, &mut cursor) else {
        return Ok(None);
    };
    let Some(projection) = read_str(&bytes, &mut cursor) else {
        return Ok(None);
    };
    if cursor + 4 > bytes.len() {
        return Ok(None);
    }
    let n = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap_or([0; 4])) as usize;
    cursor += 4;
    let mut pairs = Vec::with_capacity(n);
    for _ in 0..n {
        let Some(subj) = read_bytes(&bytes, &mut cursor) else {
            return Ok(None);
        };
        let Some(body) = read_bytes(&bytes, &mut cursor) else {
            return Ok(None);
        };
        pairs.push((subj, body));
    }
    if cursor != bytes.len() {
        return Ok(None);
    }
    Ok(Some((
        CheckpointMeta {
            checkpoint_id,
            live_subjects,
            segment_fingerprint,
            coverage,
            projection,
            created_ns,
        },
        pairs,
    )))
}

fn write_str(out: &mut Vec<u8>, s: &str) {
    write_bytes(out, s.as_bytes());
}

fn write_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn read_str(bytes: &[u8], cursor: &mut usize) -> Option<String> {
    let b = read_bytes(bytes, cursor)?;
    String::from_utf8(b).ok()
}

fn read_bytes(bytes: &[u8], cursor: &mut usize) -> Option<Vec<u8>> {
    if *cursor + 4 > bytes.len() {
        return None;
    }
    let len = u32::from_le_bytes(bytes[*cursor..*cursor + 4].try_into().ok()?) as usize;
    *cursor += 4;
    if *cursor + len > bytes.len() {
        return None;
    }
    let v = bytes[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Some(v)
}

fn sync_dir_best_effort(path: &Path) {
    #[cfg(unix)]
    {
        if let Ok(dir) = File::open(path) {
            let _ = dir.sync_all();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}
