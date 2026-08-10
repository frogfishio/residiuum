//! Optional on-disk primary index cache (OVERVIEW §10 / Stage 3c, DEF-023).
//!
//! Derived only. Missing, corrupt, or frontier-mismatched caches MUST NOT
//! prevent recovery: the store rebuilds from segments.
//!
//! ## Frontier checkpoint (DEF-023 / DEF-095)
//!
//! Version **4** caches record a durable frontier, per-entry **frame offsets**,
//! optional slim bodies (chunk manifests / legacy inline only), and verified
//! payload-chunk locators. Full payload bodies are not written. Loading a legacy
//! v1/v2 fat cache that still embeds full dataset bodies is refused so open
//! cannot re-inflate O(dataset) RSS; the store falls back to a slim segment
//! rebuild instead. Valid v2/v3 checkpoints remain readable but require a
//! one-time chunk-locator scan before the next v4 checkpoint is written.
//!
//! Open validation is O(number of segments) for metadata, then O(active tail)
//! to apply frames beyond `active_covered_len`. Full segment rescans remain the
//! recovery path when the sealed set changes or the cache is absent.

use crate::error::StoreError;
use crate::index::{IndexEntry, LiveValue, PrimaryIndex};
use blake3::Hasher;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Filename under `indexes/` for the primary current-state cache.
pub const PRIMARY_CACHE_FILE: &str = "primary.idx";
/// Sealed-media membership bound to the exact primary checkpoint bytes.
pub const PRIMARY_SEALED_MANIFEST_FILE: &str = "primary.sealed.v1";

const MAGIC_V1: &[u8; 8] = b"RIDX0001";
const MAGIC_V2: &[u8; 8] = b"RIDX0002";
const MAGIC_V3: &[u8; 8] = b"RIDX0003";
const MAGIC_V4: &[u8; 8] = b"RIDX0004";
const VERSION_V1: u32 = 1;
const VERSION_V2: u32 = 2;
const VERSION_V3: u32 = 3;
const VERSION_V4: u32 = 4;
const SEALED_MANIFEST_MAGIC: &[u8; 8] = b"RIDXSM01";
const SEALED_MANIFEST_VERSION: u32 = 1;
const MAX_SEALED_MANIFEST_ENTRIES: usize = 1_000_000;

const CHUNK_LOCATOR_ENCODED_LEN: usize = 16 + 16 + 8 + 16 + 4 + 4 + 8 + 32;

const KIND_LIVE: u8 = 1;
const KIND_DELETED: u8 = 2;

/// Refuse to load a legacy fat cache whose resident bodies exceed this budget.
///
/// Forces a slim rebuild instead of pinning multi-GB payload copies in RSS.
const FAT_CACHE_BODY_BUDGET: u64 = 64 * 1024 * 1024;

/// Durable frontier recorded with a v2/v3 primary index checkpoint (DEF-023).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexFrontier {
    /// Fingerprint of **sealed** segment files only (name + length metadata).
    pub sealed_fingerprint: [u8; 32],
    /// Active segment id covered by the checkpoint (`[0;16]` if none).
    pub active_segment_id: [u8; 16],
    /// Exclusive end offset in the active segment included in the checkpoint.
    pub active_covered_len: u64,
}

/// One derived physical locator for an authenticated payload-chunk frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChunkFrameLocator {
    pub(crate) segment_id: [u8; 16],
    pub(crate) frame_offset: u64,
    pub(crate) item_id: [u8; 16],
    pub(crate) chunk_index: u32,
    pub(crate) chunk_total: u32,
    pub(crate) logical_len: u64,
    pub(crate) verified_body_hash: [u8; 32],
}

pub(crate) type ChunkLocatorMap = HashMap<[u8; 16], Vec<ChunkFrameLocator>>;

/// One immutable sealed object covered by an exact primary checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SealedCheckpointEntry {
    pub(crate) segment_id: [u8; 16],
    pub(crate) byte_len: u64,
}

/// Successfully decoded frontier checkpoint and optional locator section.
pub(crate) struct LoadedIndexFrontier {
    pub(crate) index: PrimaryIndex,
    pub(crate) frontier: IndexFrontier,
    pub(crate) format_version: u32,
    /// `None` for legacy v2/v3 checkpoints, which require one compatibility scan.
    pub(crate) chunk_locators: Option<ChunkLocatorMap>,
}

/// Absolute path of the primary index cache file.
pub fn primary_cache_path(indexes_dir: &Path) -> PathBuf {
    indexes_dir.join(PRIMARY_CACHE_FILE)
}

pub(crate) fn primary_sealed_manifest_path(indexes_dir: &Path) -> PathBuf {
    indexes_dir.join(PRIMARY_SEALED_MANIFEST_FILE)
}

/// Publish sealed membership for one exact primary checkpoint.
pub(crate) fn write_primary_sealed_manifest(
    path: &Path,
    store_id: [u8; 16],
    primary_bytes: &[u8],
    entries: &[SealedCheckpointEntry],
) -> Result<(), StoreError> {
    let mut entries = entries.to_vec();
    entries.sort_by_key(|entry| entry.segment_id);
    let mut out = Vec::with_capacity(8 + 4 + 16 + 32 + 8 + entries.len() * 24 + 32);
    out.extend_from_slice(SEALED_MANIFEST_MAGIC);
    out.extend_from_slice(&SEALED_MANIFEST_VERSION.to_le_bytes());
    out.extend_from_slice(&store_id);
    out.extend_from_slice(blake3::hash(primary_bytes).as_bytes());
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        out.extend_from_slice(&entry.segment_id);
        out.extend_from_slice(&entry.byte_len.to_le_bytes());
    }
    let checksum = blake3::hash(&out);
    out.extend_from_slice(checksum.as_bytes());
    crate::atomic_file::write_atomic(path, &out)
}

/// Load membership only when it is checksummed and bound to `primary_bytes`.
pub(crate) fn try_load_primary_sealed_manifest(
    path: &Path,
    store_id: [u8; 16],
    primary_bytes: &[u8],
) -> Option<Vec<SealedCheckpointEntry>> {
    let bytes = fs::read(path).ok()?;
    if bytes.len() < 8 + 4 + 16 + 32 + 8 + 32 || &bytes[..8] != SEALED_MANIFEST_MAGIC.as_slice() {
        return None;
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    let file_store: [u8; 16] = bytes[12..28].try_into().ok()?;
    let primary_hash: [u8; 32] = bytes[28..60].try_into().ok()?;
    if version != SEALED_MANIFEST_VERSION
        || file_store != store_id
        || primary_hash != *blake3::hash(primary_bytes).as_bytes()
    {
        return None;
    }
    let count = u64::from_le_bytes(bytes[60..68].try_into().ok()?);
    let count = usize::try_from(count).ok()?;
    if count > MAX_SEALED_MANIFEST_ENTRIES {
        return None;
    }
    let records_len = count.checked_mul(24)?;
    let checksum_offset = 68usize.checked_add(records_len)?;
    if checksum_offset.checked_add(32)? != bytes.len()
        || blake3::hash(&bytes[..checksum_offset]).as_bytes() != &bytes[checksum_offset..]
    {
        return None;
    }
    let mut entries = Vec::with_capacity(count);
    let mut offset = 68;
    for _ in 0..count {
        entries.push(SealedCheckpointEntry {
            segment_id: bytes[offset..offset + 16].try_into().ok()?,
            byte_len: u64::from_le_bytes(bytes[offset + 16..offset + 24].try_into().ok()?),
        });
        offset += 24;
    }
    Some(entries)
}

/// Fingerprint of authoritative segment files used to invalidate a cache.
///
/// Covers every path in `segment_paths`: relative name + byte length, sorted,
/// then BLAKE3-256. Callers pass sealed-only or sealed+active as needed.
/// Cost is O(number of paths) metadata, not O(total segment bytes).
pub fn segment_fingerprint(segment_paths: &[PathBuf]) -> Result<[u8; 32], StoreError> {
    let mut pairs: Vec<(String, u64)> = Vec::with_capacity(segment_paths.len());
    for path in segment_paths {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?")
            .to_string();
        let len = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        // Include parent role so active vs sealed of same name cannot collide.
        let parent = path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .unwrap_or("");
        pairs.push((format!("{parent}/{name}"), len));
    }
    pairs.sort();
    let mut hasher = Hasher::new();
    for (name, len) in pairs {
        hasher.update(name.as_bytes());
        hasher.update(&[0]);
        hasher.update(&len.to_le_bytes());
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Load a primary index cache if magic, version, store_id, and fingerprint match.
///
/// Supports legacy v1 only when resident bodies stay under [`FAT_CACHE_BODY_BUDGET`].
/// Prefer [`try_load_primary_index_frontier`] for DEF-023 open paths.
///
/// Returns `Ok(None)` when the file is absent or not usable (never an error for
/// ordinary salvage; callers fall back to rebuild).
pub fn try_load_primary_index(
    path: &Path,
    store_id: [u8; 16],
    expected_fp: [u8; 32],
) -> Result<Option<PrimaryIndex>, StoreError> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };
    // Frontier files are not loadable via the v1 full-fingerprint API.
    if bytes.len() >= 8
        && (&bytes[..8] == MAGIC_V2.as_slice()
            || &bytes[..8] == MAGIC_V3.as_slice()
            || &bytes[..8] == MAGIC_V4.as_slice())
    {
        return Ok(None);
    }
    match decode_cache_v1(&bytes, store_id, expected_fp) {
        Some(index) if index.resident_body_bytes() <= FAT_CACHE_BODY_BUDGET => Ok(Some(index)),
        _ => Ok(None),
    }
}

/// Load a v4 (or compatible legacy v2/v3) frontier checkpoint.
///
/// Returns `Ok(None)` for absent/corrupt/fat-legacy files.
pub fn try_load_primary_index_frontier(
    path: &Path,
    store_id: [u8; 16],
) -> Result<Option<LoadedIndexFrontier>, StoreError> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };
    if let Some((index, frontier, chunk_locators)) = decode_cache_v4(&bytes, store_id) {
        return Ok(Some(LoadedIndexFrontier {
            index,
            frontier,
            format_version: VERSION_V4,
            chunk_locators: Some(chunk_locators),
        }));
    }
    if let Some((index, frontier)) = decode_cache_v3(&bytes, store_id) {
        return Ok(Some(LoadedIndexFrontier {
            index,
            frontier,
            format_version: VERSION_V3,
            chunk_locators: None,
        }));
    }
    // Legacy v2: accept only when bodies are small (dev fixtures / tiny stores).
    if let Some((index, frontier)) = decode_cache_v2(&bytes, store_id) {
        if index.resident_body_bytes() <= FAT_CACHE_BODY_BUDGET {
            return Ok(Some(LoadedIndexFrontier {
                index,
                frontier,
                format_version: VERSION_V2,
                chunk_locators: None,
            }));
        }
        // Fat v2 would re-inflate multi-GB RSS — force slim rebuild.
        return Ok(None);
    }
    Ok(None)
}

/// Persist the primary index as a v1 full-fingerprint cache (tests / compat).
#[allow(dead_code)] // retained for v1 fixtures and external tooling
pub fn write_primary_index(
    path: &Path,
    store_id: [u8; 16],
    fingerprint: [u8; 32],
    index: &PrimaryIndex,
) -> Result<(), StoreError> {
    let bytes = encode_cache_v1(store_id, fingerprint, index);
    crate::failpoint::hit("store.index_cache.before_write")?;
    crate::atomic_file::write_atomic(path, &bytes)?;
    crate::failpoint::hit("store.index_cache.after_write")?;
    Ok(())
}

/// Persist a v4 frontier checkpoint (DEF-023 + DEF-095 + DEF-098).
///
/// The file is an atomic, durable, derived replacement. Chunk payload bodies
/// remain in authoritative segments; only their verified physical locators are
/// checkpointed.
pub fn write_primary_index_frontier(
    path: &Path,
    store_id: [u8; 16],
    frontier: &IndexFrontier,
    index: &PrimaryIndex,
    chunk_locators: &ChunkLocatorMap,
) -> Result<(), StoreError> {
    let bytes = encode_cache_v4(store_id, frontier, index, chunk_locators);
    crate::failpoint::hit("store.index_cache.before_write")?;
    crate::atomic_file::write_atomic(path, &bytes)?;
    crate::failpoint::hit("store.index_cache.after_write")?;
    Ok(())
}

/// Why a primary index cache is accepted or rejected (DEF-102).
///
/// `primary.idx` is **never** authority. Byte size alone is never a health signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimaryCacheValidation {
    /// Cache present and matches store + sealed frontier.
    Accepted,
    /// No cache file on disk.
    Absent,
    /// Sealed fingerprint does not match current sealed set.
    Stale,
    /// Truncated / hash-mismatch / undecodable payload.
    Corrupt,
    /// Store id in file does not match this store.
    Foreign,
    /// Magic/version not recognized by this build.
    Unsupported,
}

impl PrimaryCacheValidation {
    /// Stable snake_case label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Absent => "absent",
            Self::Stale => "stale",
            Self::Corrupt => "corrupt",
            Self::Foreign => "foreign",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Diagnostic view of the derived primary index cache (DEF-102).
///
/// Always reports `authoritative: false`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryCacheDiag {
    /// Cache file exists.
    pub present: bool,
    /// Detected format version (1/2/3/4) when parseable.
    pub format_version: Option<u32>,
    /// On-disk byte length (0 when absent).
    pub byte_len: u64,
    /// Acceptance class.
    pub validation: PrimaryCacheValidation,
    /// Sealed fingerprint recorded in the cache (if decoded).
    pub sealed_fingerprint: Option<[u8; 32]>,
    /// Current sealed fingerprint of the store (if provided).
    pub current_sealed_fingerprint: Option<[u8; 32]>,
    /// Active segment id from frontier.
    pub active_segment_id: Option<[u8; 16]>,
    /// Active covered length from frontier.
    pub active_covered_len: Option<u64>,
    /// Current active file length when known (for replay sizing).
    pub active_actual_len: Option<u64>,
    /// Bytes still to replay from active tail (`max(0, actual - covered)`).
    pub replay_bytes: Option<u64>,
    /// Resident entries if cache loaded successfully into a throwaway index.
    pub resident_entries: Option<usize>,
    /// Resident body bytes if loaded.
    pub resident_body_bytes: Option<u64>,
    /// Always false — cache is derived only.
    pub authoritative: bool,
    /// Operator-safe detail.
    pub detail: String,
}

/// Classify the on-disk primary cache without elevating it to authority (DEF-102).
///
/// `expected_sealed_fp` and `active_actual_len` are optional context from the open store.
pub fn diagnose_primary_cache(
    path: &Path,
    store_id: [u8; 16],
    expected_sealed_fp: Option<[u8; 32]>,
    active_actual_len: Option<u64>,
) -> PrimaryCacheDiag {
    let absent = PrimaryCacheDiag {
        present: false,
        format_version: None,
        byte_len: 0,
        validation: PrimaryCacheValidation::Absent,
        sealed_fingerprint: None,
        current_sealed_fingerprint: expected_sealed_fp,
        active_segment_id: None,
        active_covered_len: None,
        active_actual_len,
        replay_bytes: active_actual_len,
        resident_entries: None,
        resident_body_bytes: None,
        authoritative: false,
        detail: "primary.idx absent — store recovers from authoritative segments".into(),
    };
    if !path.is_file() {
        return absent;
    }
    let byte_len = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => {
            return PrimaryCacheDiag {
                present: true,
                format_version: None,
                byte_len,
                validation: PrimaryCacheValidation::Corrupt,
                sealed_fingerprint: None,
                current_sealed_fingerprint: expected_sealed_fp,
                active_segment_id: None,
                active_covered_len: None,
                active_actual_len,
                replay_bytes: None,
                resident_entries: None,
                resident_body_bytes: None,
                authoritative: false,
                detail: "primary.idx unreadable — rebuild from segments".into(),
            };
        }
    };
    if bytes.len() < 12 {
        return PrimaryCacheDiag {
            present: true,
            format_version: None,
            byte_len,
            validation: PrimaryCacheValidation::Corrupt,
            sealed_fingerprint: None,
            current_sealed_fingerprint: expected_sealed_fp,
            active_segment_id: None,
            active_covered_len: None,
            active_actual_len,
            replay_bytes: None,
            resident_entries: None,
            resident_body_bytes: None,
            authoritative: false,
            detail: format!(
                "primary.idx truncated ({byte_len} bytes) — not a health signal; rebuild from segments"
            ),
        };
    }
    let magic = &bytes[..8];
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap_or([0; 4]));

    if magic == MAGIC_V4.as_slice() {
        return classify_frontier_cache(
            &bytes,
            store_id,
            expected_sealed_fp,
            active_actual_len,
            byte_len,
            VERSION_V4,
            decode_cache_v4_without_locators,
        );
    }
    if magic == MAGIC_V3.as_slice() {
        return classify_frontier_cache(
            &bytes,
            store_id,
            expected_sealed_fp,
            active_actual_len,
            byte_len,
            VERSION_V3,
            decode_cache_v3,
        );
    }
    if magic == MAGIC_V2.as_slice() {
        return classify_frontier_cache(
            &bytes,
            store_id,
            expected_sealed_fp,
            active_actual_len,
            byte_len,
            VERSION_V2,
            decode_cache_v2,
        );
    }
    if magic == MAGIC_V1.as_slice() {
        // v1 has full fingerprint, not sealed-only frontier.
        if bytes.len() < 8 + 4 + 16 + 32 {
            return PrimaryCacheDiag {
                present: true,
                format_version: Some(VERSION_V1),
                byte_len,
                validation: PrimaryCacheValidation::Corrupt,
                sealed_fingerprint: None,
                current_sealed_fingerprint: expected_sealed_fp,
                active_segment_id: None,
                active_covered_len: None,
                active_actual_len,
                replay_bytes: None,
                resident_entries: None,
                resident_body_bytes: None,
                authoritative: false,
                detail: "primary.idx v1 truncated".into(),
            };
        }
        let file_store: [u8; 16] = bytes[12..28].try_into().unwrap_or([0; 16]);
        if file_store != store_id {
            return PrimaryCacheDiag {
                present: true,
                format_version: Some(VERSION_V1),
                byte_len,
                validation: PrimaryCacheValidation::Foreign,
                sealed_fingerprint: None,
                current_sealed_fingerprint: expected_sealed_fp,
                active_segment_id: None,
                active_covered_len: None,
                active_actual_len,
                replay_bytes: None,
                resident_entries: None,
                resident_body_bytes: None,
                authoritative: false,
                detail: "primary.idx store_id mismatch (foreign cache)".into(),
            };
        }
        return PrimaryCacheDiag {
            present: true,
            format_version: Some(VERSION_V1),
            byte_len,
            validation: PrimaryCacheValidation::Unsupported,
            sealed_fingerprint: None,
            current_sealed_fingerprint: expected_sealed_fp,
            active_segment_id: None,
            active_covered_len: None,
            active_actual_len,
            replay_bytes: None,
            resident_entries: None,
            resident_body_bytes: None,
            authoritative: false,
            detail: format!(
                "primary.idx v1 present ({byte_len} bytes) — derived only; open prefers v4 frontier \
                 and rebuilds if needed (size is not stored-data size)"
            ),
        };
    }

    PrimaryCacheDiag {
        present: true,
        format_version: Some(version),
        byte_len,
        validation: PrimaryCacheValidation::Unsupported,
        sealed_fingerprint: None,
        current_sealed_fingerprint: expected_sealed_fp,
        active_segment_id: None,
        active_covered_len: None,
        active_actual_len,
        replay_bytes: None,
        resident_entries: None,
        resident_body_bytes: None,
        authoritative: false,
        detail: format!("primary.idx unrecognized magic/version (byte_len={byte_len})"),
    }
}

fn classify_frontier_cache(
    bytes: &[u8],
    store_id: [u8; 16],
    expected_sealed_fp: Option<[u8; 32]>,
    active_actual_len: Option<u64>,
    byte_len: u64,
    version: u32,
    decode: fn(&[u8], [u8; 16]) -> Option<(PrimaryIndex, IndexFrontier)>,
) -> PrimaryCacheDiag {
    // Peek store id before full decode for foreign classification.
    if bytes.len() >= 28 {
        let file_store: [u8; 16] = bytes[12..28].try_into().unwrap_or([0; 16]);
        if file_store != store_id {
            return PrimaryCacheDiag {
                present: true,
                format_version: Some(version),
                byte_len,
                validation: PrimaryCacheValidation::Foreign,
                sealed_fingerprint: None,
                current_sealed_fingerprint: expected_sealed_fp,
                active_segment_id: None,
                active_covered_len: None,
                active_actual_len,
                replay_bytes: None,
                resident_entries: None,
                resident_body_bytes: None,
                authoritative: false,
                detail: "primary.idx store_id mismatch (foreign cache)".into(),
            };
        }
    }
    match decode(bytes, store_id) {
        None => PrimaryCacheDiag {
            present: true,
            format_version: Some(version),
            byte_len,
            validation: PrimaryCacheValidation::Corrupt,
            sealed_fingerprint: None,
            current_sealed_fingerprint: expected_sealed_fp,
            active_segment_id: None,
            active_covered_len: None,
            active_actual_len,
            replay_bytes: None,
            resident_entries: None,
            resident_body_bytes: None,
            authoritative: false,
            detail: format!(
                "primary.idx v{version} corrupt or fat-legacy refused ({byte_len} bytes) — \
                 rebuild from segments; size is not a health signal"
            ),
        },
        Some((index, frontier)) => {
            let stale = expected_sealed_fp
                .map(|fp| fp != frontier.sealed_fingerprint)
                .unwrap_or(false);
            // Covered length past the active file is an ahead/invalid frontier.
            let ahead = active_actual_len
                .map(|actual| frontier.active_covered_len > actual)
                .unwrap_or(false);
            let replay =
                active_actual_len.map(|actual| actual.saturating_sub(frontier.active_covered_len));
            let (validation, detail) = if ahead {
                (
                    PrimaryCacheValidation::Corrupt,
                    format!(
                        "primary.idx v{version} frontier ahead of active file \
                         (covered={} actual={:?}, {byte_len} bytes) — rebuild from segments; \
                         cache is not authority",
                        frontier.active_covered_len, active_actual_len
                    ),
                )
            } else if stale {
                (
                    PrimaryCacheValidation::Stale,
                    format!(
                        "primary.idx v{version} frontier sealed fingerprint stale ({byte_len} bytes) — \
                         open replays/rebuilds; cache is not authority"
                    ),
                )
            } else {
                (
                    PrimaryCacheValidation::Accepted,
                    format!(
                        "primary.idx v{version} accepted as derived checkpoint ({byte_len} bytes, \
                         {} resident entries) — not authority; active tail replay_bytes={replay:?}",
                        index.len()
                    ),
                )
            };
            PrimaryCacheDiag {
                present: true,
                format_version: Some(version),
                byte_len,
                validation,
                sealed_fingerprint: Some(frontier.sealed_fingerprint),
                current_sealed_fingerprint: expected_sealed_fp,
                active_segment_id: Some(frontier.active_segment_id),
                active_covered_len: Some(frontier.active_covered_len),
                active_actual_len,
                replay_bytes: replay,
                resident_entries: Some(index.len()),
                resident_body_bytes: Some(index.resident_body_bytes()),
                authoritative: false,
                detail,
            }
        }
    }
}

/// Lifecycle snapshot for active log + derived checkpoints (DEF-102).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleDiag {
    /// Writer shard count.
    pub active_shards: usize,
    /// Pending seal files awaiting finalize.
    pub pending_seals: usize,
    /// Sealed segment file count (available paths).
    pub sealed_segments: usize,
    /// Why the last derived checkpoint was written (best-effort label).
    pub checkpoint_reason: String,
    /// Ops since last derived checkpoint on this handle (0 if unknown).
    pub derived_ops_since_checkpoint: u64,
    /// Always false for any derived artifact.
    pub primary_cache_authoritative: bool,
    /// Operator-safe narrative.
    pub detail: String,
}

fn encode_entries_v2(out: &mut Vec<u8>, index: &PrimaryIndex) {
    // Legacy layout without frame_offset (body embedded).
    out.extend_from_slice(&(index.len() as u64).to_le_bytes());
    for (subject, entry) in index.iter_all() {
        let subject_len = u16::try_from(subject.len()).unwrap_or(u16::MAX);
        out.extend_from_slice(&subject_len.to_le_bytes());
        out.extend_from_slice(&subject[..subject_len as usize]);
        match entry {
            IndexEntry::Live(lv) => {
                out.push(KIND_LIVE);
                out.extend_from_slice(&lv.item_id);
                out.extend_from_slice(&lv.event_id);
                out.extend_from_slice(&lv.segment_id);
                out.extend_from_slice(&lv.writer_sequence.to_le_bytes());
                let body_len = u32::try_from(lv.body.len()).unwrap_or(u32::MAX);
                out.extend_from_slice(&body_len.to_le_bytes());
                out.extend_from_slice(&lv.body[..body_len as usize]);
            }
            IndexEntry::Deleted {
                item_id,
                event_id,
                segment_id,
                writer_sequence,
                ..
            } => {
                out.push(KIND_DELETED);
                out.extend_from_slice(item_id);
                out.extend_from_slice(event_id);
                out.extend_from_slice(segment_id);
                out.extend_from_slice(&writer_sequence.to_le_bytes());
                out.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }
}

fn encode_entries_v3(out: &mut Vec<u8>, index: &PrimaryIndex) {
    out.extend_from_slice(&(index.len() as u64).to_le_bytes());
    for (subject, entry) in index.iter_all() {
        let subject_len = u16::try_from(subject.len()).unwrap_or(u16::MAX);
        out.extend_from_slice(&subject_len.to_le_bytes());
        out.extend_from_slice(&subject[..subject_len as usize]);
        match entry {
            IndexEntry::Live(lv) => {
                out.push(KIND_LIVE);
                out.extend_from_slice(&lv.item_id);
                out.extend_from_slice(&lv.event_id);
                out.extend_from_slice(&lv.segment_id);
                out.extend_from_slice(&lv.writer_sequence.to_le_bytes());
                out.extend_from_slice(&lv.frame_offset.to_le_bytes());
                let body_len = u32::try_from(lv.body.len()).unwrap_or(u32::MAX);
                out.extend_from_slice(&body_len.to_le_bytes());
                out.extend_from_slice(&lv.body[..body_len as usize]);
            }
            IndexEntry::Deleted {
                item_id,
                event_id,
                segment_id,
                writer_sequence,
                frame_offset,
            } => {
                out.push(KIND_DELETED);
                out.extend_from_slice(item_id);
                out.extend_from_slice(event_id);
                out.extend_from_slice(segment_id);
                out.extend_from_slice(&writer_sequence.to_le_bytes());
                out.extend_from_slice(&frame_offset.to_le_bytes());
                out.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }
}

fn decode_entries_v2(bytes: &[u8], mut off: usize) -> Option<(PrimaryIndex, usize)> {
    if off + 8 > bytes.len() {
        return None;
    }
    let entry_count = u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?) as usize;
    off += 8;

    let mut index = PrimaryIndex::new();
    for _ in 0..entry_count {
        if off + 2 > bytes.len() {
            return None;
        }
        let subject_len = u16::from_le_bytes(bytes[off..off + 2].try_into().ok()?) as usize;
        off += 2;
        if off + subject_len + 1 + 16 * 3 + 8 + 4 > bytes.len() {
            return None;
        }
        let subject = bytes[off..off + subject_len].to_vec();
        off += subject_len;
        let kind = bytes[off];
        off += 1;
        let item_id: [u8; 16] = bytes[off..off + 16].try_into().ok()?;
        off += 16;
        let event_id: [u8; 16] = bytes[off..off + 16].try_into().ok()?;
        off += 16;
        let segment_id: [u8; 16] = bytes[off..off + 16].try_into().ok()?;
        off += 16;
        let writer_sequence = u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?);
        off += 8;
        let body_len = u32::from_le_bytes(bytes[off..off + 4].try_into().ok()?) as usize;
        off += 4;
        match kind {
            KIND_LIVE => {
                if off + body_len > bytes.len() {
                    return None;
                }
                let body = bytes[off..off + body_len].to_vec();
                off += body_len;
                index.insert_entry(
                    subject,
                    IndexEntry::Live(LiveValue {
                        body,
                        item_id,
                        event_id,
                        segment_id,
                        writer_sequence,
                        frame_offset: 0,
                    }),
                );
            }
            KIND_DELETED => {
                if body_len != 0 {
                    return None;
                }
                index.insert_entry(
                    subject,
                    IndexEntry::Deleted {
                        item_id,
                        event_id,
                        segment_id,
                        writer_sequence,
                        frame_offset: 0,
                    },
                );
            }
            _ => return None,
        }
    }
    Some((index, off))
}

fn decode_entries_v3(bytes: &[u8], mut off: usize) -> Option<(PrimaryIndex, usize)> {
    if off + 8 > bytes.len() {
        return None;
    }
    let entry_count = u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?) as usize;
    off += 8;

    let mut index = PrimaryIndex::new();
    for _ in 0..entry_count {
        if off + 2 > bytes.len() {
            return None;
        }
        let subject_len = u16::from_le_bytes(bytes[off..off + 2].try_into().ok()?) as usize;
        off += 2;
        // subject + kind + 3 ids + writer_seq + frame_offset + body_len
        if off + subject_len + 1 + 16 * 3 + 8 + 8 + 4 > bytes.len() {
            return None;
        }
        let subject = bytes[off..off + subject_len].to_vec();
        off += subject_len;
        let kind = bytes[off];
        off += 1;
        let item_id: [u8; 16] = bytes[off..off + 16].try_into().ok()?;
        off += 16;
        let event_id: [u8; 16] = bytes[off..off + 16].try_into().ok()?;
        off += 16;
        let segment_id: [u8; 16] = bytes[off..off + 16].try_into().ok()?;
        off += 16;
        let writer_sequence = u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?);
        off += 8;
        let frame_offset = u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?);
        off += 8;
        let body_len = u32::from_le_bytes(bytes[off..off + 4].try_into().ok()?) as usize;
        off += 4;
        match kind {
            KIND_LIVE => {
                if off + body_len > bytes.len() {
                    return None;
                }
                let body = bytes[off..off + body_len].to_vec();
                off += body_len;
                index.insert_entry(
                    subject,
                    IndexEntry::Live(LiveValue {
                        body,
                        item_id,
                        event_id,
                        segment_id,
                        writer_sequence,
                        frame_offset,
                    }),
                );
            }
            KIND_DELETED => {
                if body_len != 0 {
                    return None;
                }
                index.insert_entry(
                    subject,
                    IndexEntry::Deleted {
                        item_id,
                        event_id,
                        segment_id,
                        writer_sequence,
                        frame_offset,
                    },
                );
            }
            _ => return None,
        }
    }
    Some((index, off))
}

fn encode_cache_v1(store_id: [u8; 16], fingerprint: [u8; 32], index: &PrimaryIndex) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 4 + 16 + 32 + 8 + index.len() * 64);
    out.extend_from_slice(MAGIC_V1);
    out.extend_from_slice(&VERSION_V1.to_le_bytes());
    out.extend_from_slice(&store_id);
    out.extend_from_slice(&fingerprint);
    encode_entries_v2(&mut out, index);
    out
}

fn decode_cache_v1(
    bytes: &[u8],
    store_id: [u8; 16],
    expected_fp: [u8; 32],
) -> Option<PrimaryIndex> {
    if bytes.len() < 8 + 4 + 16 + 32 + 8 {
        return None;
    }
    if &bytes[..8] != MAGIC_V1.as_slice() {
        return None;
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    if version != VERSION_V1 {
        return None;
    }
    let file_store: [u8; 16] = bytes[12..28].try_into().ok()?;
    if file_store != store_id {
        return None;
    }
    let fp: [u8; 32] = bytes[28..60].try_into().ok()?;
    if fp != expected_fp {
        return None;
    }
    let (index, off) = decode_entries_v2(bytes, 60)?;
    if off != bytes.len() {
        return None;
    }
    Some(index)
}

#[cfg(test)]
fn encode_cache_v2(store_id: [u8; 16], frontier: &IndexFrontier, index: &PrimaryIndex) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 4 + 16 + 32 + 16 + 8 + 8 + index.len() * 64);
    out.extend_from_slice(MAGIC_V2);
    out.extend_from_slice(&VERSION_V2.to_le_bytes());
    out.extend_from_slice(&store_id);
    out.extend_from_slice(&frontier.sealed_fingerprint);
    out.extend_from_slice(&frontier.active_segment_id);
    out.extend_from_slice(&frontier.active_covered_len.to_le_bytes());
    encode_entries_v2(&mut out, index);
    let mut hasher = Hasher::new();
    hasher.update(&out);
    out.extend_from_slice(hasher.finalize().as_bytes());
    out
}

fn decode_cache_v2(bytes: &[u8], store_id: [u8; 16]) -> Option<(PrimaryIndex, IndexFrontier)> {
    if bytes.len() < 8 + 4 + 16 + 32 + 16 + 8 + 8 + 32 {
        return None;
    }
    if &bytes[..8] != MAGIC_V2.as_slice() {
        return None;
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    if version != VERSION_V2 {
        return None;
    }
    let file_store: [u8; 16] = bytes[12..28].try_into().ok()?;
    if file_store != store_id {
        return None;
    }
    let sealed_fingerprint: [u8; 32] = bytes[28..60].try_into().ok()?;
    let active_segment_id: [u8; 16] = bytes[60..76].try_into().ok()?;
    let active_covered_len = u64::from_le_bytes(bytes[76..84].try_into().ok()?);
    let (index, off) = decode_entries_v2(bytes, 84)?;
    if off + 32 != bytes.len() {
        return None;
    }
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..off]);
    let expect = hasher.finalize();
    if expect.as_bytes() != &bytes[off..off + 32] {
        return None;
    }
    Some((
        index,
        IndexFrontier {
            sealed_fingerprint,
            active_segment_id,
            active_covered_len,
        },
    ))
}

#[cfg(test)]
fn encode_cache_v3(store_id: [u8; 16], frontier: &IndexFrontier, index: &PrimaryIndex) -> Vec<u8> {
    // Metadata-sized capacity: keys + locators, not payload bodies.
    let mut out = Vec::with_capacity(8 + 4 + 16 + 32 + 16 + 8 + 8 + index.len() * 96);
    out.extend_from_slice(MAGIC_V3);
    out.extend_from_slice(&VERSION_V3.to_le_bytes());
    out.extend_from_slice(&store_id);
    out.extend_from_slice(&frontier.sealed_fingerprint);
    out.extend_from_slice(&frontier.active_segment_id);
    out.extend_from_slice(&frontier.active_covered_len.to_le_bytes());
    encode_entries_v3(&mut out, index);
    let mut hasher = Hasher::new();
    hasher.update(&out);
    out.extend_from_slice(hasher.finalize().as_bytes());
    out
}

#[cfg(test)]
pub(crate) fn write_primary_index_frontier_v3_for_test(
    path: &Path,
    store_id: [u8; 16],
    frontier: &IndexFrontier,
    index: &PrimaryIndex,
) -> Result<(), StoreError> {
    crate::atomic_file::write_atomic(path, &encode_cache_v3(store_id, frontier, index))
}

fn decode_cache_v3(bytes: &[u8], store_id: [u8; 16]) -> Option<(PrimaryIndex, IndexFrontier)> {
    if bytes.len() < 8 + 4 + 16 + 32 + 16 + 8 + 8 + 32 {
        return None;
    }
    if &bytes[..8] != MAGIC_V3.as_slice() {
        return None;
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    if version != VERSION_V3 {
        return None;
    }
    let file_store: [u8; 16] = bytes[12..28].try_into().ok()?;
    if file_store != store_id {
        return None;
    }
    let sealed_fingerprint: [u8; 32] = bytes[28..60].try_into().ok()?;
    let active_segment_id: [u8; 16] = bytes[60..76].try_into().ok()?;
    let active_covered_len = u64::from_le_bytes(bytes[76..84].try_into().ok()?);
    let (index, off) = decode_entries_v3(bytes, 84)?;
    if off + 32 != bytes.len() {
        return None;
    }
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..off]);
    let expect = hasher.finalize();
    if expect.as_bytes() != &bytes[off..off + 32] {
        return None;
    }
    Some((
        index,
        IndexFrontier {
            sealed_fingerprint,
            active_segment_id,
            active_covered_len,
        },
    ))
}

fn encode_chunk_locators(out: &mut Vec<u8>, chunk_locators: &ChunkLocatorMap) {
    let mut entries: Vec<([u8; 16], &ChunkFrameLocator)> = chunk_locators
        .iter()
        .flat_map(|(event_id, locators)| locators.iter().map(move |loc| (*event_id, loc)))
        .collect();
    entries.sort_by(|(event_a, loc_a), (event_b, loc_b)| {
        event_a
            .cmp(event_b)
            .then(loc_a.segment_id.cmp(&loc_b.segment_id))
            .then(loc_a.frame_offset.cmp(&loc_b.frame_offset))
            .then(loc_a.chunk_index.cmp(&loc_b.chunk_index))
    });
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for (event_id, loc) in entries {
        out.extend_from_slice(&event_id);
        out.extend_from_slice(&loc.segment_id);
        out.extend_from_slice(&loc.frame_offset.to_le_bytes());
        out.extend_from_slice(&loc.item_id);
        out.extend_from_slice(&loc.chunk_index.to_le_bytes());
        out.extend_from_slice(&loc.chunk_total.to_le_bytes());
        out.extend_from_slice(&loc.logical_len.to_le_bytes());
        out.extend_from_slice(&loc.verified_body_hash);
    }
}

fn decode_chunk_locators(bytes: &[u8], mut off: usize) -> Option<(ChunkLocatorMap, usize)> {
    let count_bytes = bytes.get(off..off.checked_add(8)?)?;
    let count = u64::from_le_bytes(count_bytes.try_into().ok()?);
    off += 8;
    let remaining_records = bytes.len().saturating_sub(off + 32) / CHUNK_LOCATOR_ENCODED_LEN;
    let count = usize::try_from(count).ok()?;
    if count > remaining_records {
        return None;
    }
    let mut out = ChunkLocatorMap::new();
    for _ in 0..count {
        let end = off.checked_add(CHUNK_LOCATOR_ENCODED_LEN)?;
        let record = bytes.get(off..end)?;
        let event_id = record[0..16].try_into().ok()?;
        let segment_id = record[16..32].try_into().ok()?;
        let frame_offset = u64::from_le_bytes(record[32..40].try_into().ok()?);
        let item_id = record[40..56].try_into().ok()?;
        let chunk_index = u32::from_le_bytes(record[56..60].try_into().ok()?);
        let chunk_total = u32::from_le_bytes(record[60..64].try_into().ok()?);
        if chunk_total == 0 || chunk_index >= chunk_total {
            return None;
        }
        let logical_len = u64::from_le_bytes(record[64..72].try_into().ok()?);
        let verified_body_hash = record[72..104].try_into().ok()?;
        out.entry(event_id).or_default().push(ChunkFrameLocator {
            segment_id,
            frame_offset,
            item_id,
            chunk_index,
            chunk_total,
            logical_len,
            verified_body_hash,
        });
        off = end;
    }
    Some((out, off))
}

fn encode_cache_v4(
    store_id: [u8; 16],
    frontier: &IndexFrontier,
    index: &PrimaryIndex,
    chunk_locators: &ChunkLocatorMap,
) -> Vec<u8> {
    let locator_count: usize = chunk_locators.values().map(Vec::len).sum();
    let mut out = Vec::with_capacity(
        8 + 4
            + 16
            + 32
            + 16
            + 8
            + 8
            + index.len() * 96
            + locator_count * CHUNK_LOCATOR_ENCODED_LEN
            + 32,
    );
    out.extend_from_slice(MAGIC_V4);
    out.extend_from_slice(&VERSION_V4.to_le_bytes());
    out.extend_from_slice(&store_id);
    out.extend_from_slice(&frontier.sealed_fingerprint);
    out.extend_from_slice(&frontier.active_segment_id);
    out.extend_from_slice(&frontier.active_covered_len.to_le_bytes());
    encode_entries_v3(&mut out, index);
    encode_chunk_locators(&mut out, chunk_locators);
    let mut hasher = Hasher::new();
    hasher.update(&out);
    out.extend_from_slice(hasher.finalize().as_bytes());
    out
}

fn decode_cache_v4(
    bytes: &[u8],
    store_id: [u8; 16],
) -> Option<(PrimaryIndex, IndexFrontier, ChunkLocatorMap)> {
    if bytes.len() < 8 + 4 + 16 + 32 + 16 + 8 + 8 + 8 + 32 {
        return None;
    }
    if &bytes[..8] != MAGIC_V4.as_slice() {
        return None;
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    if version != VERSION_V4 {
        return None;
    }
    let file_store: [u8; 16] = bytes[12..28].try_into().ok()?;
    if file_store != store_id {
        return None;
    }
    let sealed_fingerprint: [u8; 32] = bytes[28..60].try_into().ok()?;
    let active_segment_id: [u8; 16] = bytes[60..76].try_into().ok()?;
    let active_covered_len = u64::from_le_bytes(bytes[76..84].try_into().ok()?);
    let (index, off) = decode_entries_v3(bytes, 84)?;
    let (chunk_locators, off) = decode_chunk_locators(bytes, off)?;
    if off + 32 != bytes.len() {
        return None;
    }
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..off]);
    if hasher.finalize().as_bytes() != &bytes[off..off + 32] {
        return None;
    }
    Some((
        index,
        IndexFrontier {
            sealed_fingerprint,
            active_segment_id,
            active_covered_len,
        },
        chunk_locators,
    ))
}

fn decode_cache_v4_without_locators(
    bytes: &[u8],
    store_id: [u8; 16],
) -> Option<(PrimaryIndex, IndexFrontier)> {
    decode_cache_v4(bytes, store_id).map(|(index, frontier, _)| (index, frontier))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::EventKind;

    fn sample_index() -> PrimaryIndex {
        let mut index = PrimaryIndex::new();
        index.apply_event(
            b"a".to_vec(),
            EventKind::Put,
            b"val".to_vec(),
            [1u8; 16],
            [2u8; 16],
            [3u8; 16],
            7,
            100,
        );
        index.apply_event(
            b"b".to_vec(),
            EventKind::Delete,
            vec![],
            [4u8; 16],
            [5u8; 16],
            [6u8; 16],
            8,
            200,
        );
        index
    }

    #[test]
    fn sealed_manifest_is_bound_to_exact_primary_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(PRIMARY_SEALED_MANIFEST_FILE);
        let store_id = [0x31; 16];
        let primary = b"primary-checkpoint-a";
        let entries = vec![
            SealedCheckpointEntry {
                segment_id: [2; 16],
                byte_len: 200,
            },
            SealedCheckpointEntry {
                segment_id: [1; 16],
                byte_len: 100,
            },
        ];
        write_primary_sealed_manifest(&path, store_id, primary, &entries).unwrap();
        let loaded = try_load_primary_sealed_manifest(&path, store_id, primary).unwrap();
        assert_eq!(loaded[0].segment_id, [1; 16]);
        assert_eq!(loaded[1].segment_id, [2; 16]);
        assert!(try_load_primary_sealed_manifest(
            &path,
            store_id,
            b"different-primary-checkpoint"
        )
        .is_none());
    }

    #[test]
    fn cache_roundtrip_v1() {
        let index = sample_index();
        let store_id = [9u8; 16];
        let fp = [0xabu8; 32];
        let bytes = encode_cache_v1(store_id, fp, &index);
        let loaded = decode_cache_v1(&bytes, store_id, fp).unwrap();
        assert_eq!(loaded.get_live(b"a"), Some(b"val".as_slice()));
        assert!(loaded.get_live(b"b").is_none());
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn fingerprint_mismatch_rejects_v1() {
        let index = sample_index();
        let bytes = encode_cache_v1([0u8; 16], [1u8; 32], &index);
        assert!(decode_cache_v1(&bytes, [0u8; 16], [2u8; 32]).is_none());
    }

    #[test]
    fn frontier_cache_roundtrip_v2() {
        let index = sample_index();
        let store_id = [9u8; 16];
        let frontier = IndexFrontier {
            sealed_fingerprint: [0x11u8; 32],
            active_segment_id: [0x22u8; 16],
            active_covered_len: 4096,
        };
        let bytes = encode_cache_v2(store_id, &frontier, &index);
        let (loaded, fr) = decode_cache_v2(&bytes, store_id).unwrap();
        assert_eq!(fr, frontier);
        assert_eq!(loaded.get_live(b"a"), Some(b"val".as_slice()));
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn frontier_cache_roundtrip_v3_preserves_offset() {
        let index = sample_index();
        let store_id = [9u8; 16];
        let frontier = IndexFrontier {
            sealed_fingerprint: [0x11u8; 32],
            active_segment_id: [0x22u8; 16],
            active_covered_len: 4096,
        };
        let bytes = encode_cache_v3(store_id, &frontier, &index);
        let (loaded, fr) = decode_cache_v3(&bytes, store_id).unwrap();
        assert_eq!(fr, frontier);
        assert_eq!(loaded.get_live(b"a"), Some(b"val".as_slice()));
        let lv = match loaded.get(b"a") {
            Some(IndexEntry::Live(v)) => v,
            _ => panic!("expected live"),
        };
        assert_eq!(lv.frame_offset, 100);
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn frontier_cache_roundtrip_v4_preserves_chunk_locators() {
        let index = sample_index();
        let store_id = [9u8; 16];
        let frontier = IndexFrontier {
            sealed_fingerprint: [0x11u8; 32],
            active_segment_id: [0x22u8; 16],
            active_covered_len: 4096,
        };
        let event_id = [0x33u8; 16];
        let locator = ChunkFrameLocator {
            segment_id: [0x44u8; 16],
            frame_offset: 512,
            item_id: [0x55u8; 16],
            chunk_index: 1,
            chunk_total: 3,
            logical_len: 16_384,
            verified_body_hash: [0x66u8; 32],
        };
        let mut locators = ChunkLocatorMap::new();
        locators.insert(event_id, vec![locator.clone()]);

        let bytes = encode_cache_v4(store_id, &frontier, &index, &locators);
        let (loaded, decoded_frontier, decoded_locators) =
            decode_cache_v4(&bytes, store_id).unwrap();
        assert_eq!(decoded_frontier, frontier);
        assert_eq!(loaded.get_live(b"a"), Some(b"val".as_slice()));
        assert_eq!(decoded_locators.get(&event_id), Some(&vec![locator]));
    }

    #[test]
    fn frontier_cache_v4_rejects_invalid_chunk_shape() {
        let index = sample_index();
        let store_id = [9u8; 16];
        let frontier = IndexFrontier {
            sealed_fingerprint: [0x11u8; 32],
            active_segment_id: [0x22u8; 16],
            active_covered_len: 4096,
        };
        let mut locators = ChunkLocatorMap::new();
        locators.insert(
            [0x33u8; 16],
            vec![ChunkFrameLocator {
                segment_id: [0x44u8; 16],
                frame_offset: 512,
                item_id: [0x55u8; 16],
                chunk_index: 3,
                chunk_total: 3,
                logical_len: 16_384,
                verified_body_hash: [0x66u8; 32],
            }],
        );
        let bytes = encode_cache_v4(store_id, &frontier, &index, &locators);
        assert!(decode_cache_v4(&bytes, store_id).is_none());
    }

    #[test]
    fn frontier_cache_corruption_rejects() {
        let index = sample_index();
        let mut bytes = encode_cache_v3(
            [0u8; 16],
            &IndexFrontier {
                sealed_fingerprint: [1u8; 32],
                active_segment_id: [0u8; 16],
                active_covered_len: 0,
            },
            &index,
        );
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(decode_cache_v3(&bytes, [0u8; 16]).is_none());
    }
}
