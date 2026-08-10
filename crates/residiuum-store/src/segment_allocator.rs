//! Durable segment-id high-water allocator (never-reuse).
//!
//! Process-local `segment_seq` counters are insufficient: active files omit the
//! seq from their filename, so open can under-count and remint a published id,
//! overwriting sealed `.residiuum` / Recovery Shadow media.
//!
//! Protocol:
//! 1. Reconstruct `reserved_thru` = max(durable file, every media-derived seq).
//! 2. **Lease** a bounded range above `reserved_thru` by atomically persisting
//!    its inclusive end **before** creating any active/pending bytes in it.
//! 3. Issue ids from that durable lease in memory. A crash may leave gaps, but
//!    never permits an id to be reused.
//! 4. If durable state is corrupt **and** media ids cannot be reconstructed
//!    unambiguously, **refuse** open without mutating existing media.

use crate::atomic_file::{sync_dir, write_atomic};
use crate::error::StoreError;
use crate::ids::{mint_sortable_segment_id, segment_seq_from_id};
use crate::layout::{list_residiuum_files, segment_id_from_filename, unhex16, StorePaths};
use crate::seal_pipeline::list_pending_paths;
use residiuum_format::{decode_descriptor_body, scan_forward, FrameKind, SafetyLimits};
use std::fs;
use std::path::{Path, PathBuf};

/// Filename under `store-info/` for the durable exclusive high-water mark.
pub const SEGMENT_SEQ_FILE: &str = "segment_seq.v1";

const MAGIC: &[u8; 8] = b"SEGSEQ01";

/// Number of segment ids durably leased at once.
///
/// At the default 64 MiB seal threshold this covers 4 GiB of new authoritative
/// media while replacing 64 tiny atomic metadata transactions with one. Gaps
/// after a crash are intentional; ids are identities, not a dense record count.
pub const SEGMENT_ID_LEASE: u64 = 64;

/// `store-info/segment_seq.v1`.
pub fn segment_seq_path(paths: &StorePaths) -> PathBuf {
    paths.store_info().join(SEGMENT_SEQ_FILE)
}

/// Decode durable reservation: `(store_id, reserved_thru_inclusive)`.
pub fn decode_segment_seq_file(bytes: &[u8]) -> Result<([u8; 16], u64), StoreError> {
    if bytes.len() < 8 + 16 + 8 {
        return Err(StoreError::CorruptMeta("segment_seq.v1 truncated"));
    }
    if &bytes[..8] != MAGIC {
        return Err(StoreError::CorruptMeta("segment_seq.v1 bad magic"));
    }
    let mut store_id = [0u8; 16];
    store_id.copy_from_slice(&bytes[8..24]);
    let reserved = u64::from_le_bytes(bytes[24..32].try_into().expect("8 bytes"));
    Ok((store_id, reserved))
}

fn encode_segment_seq_file(store_id: [u8; 16], reserved_thru: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&store_id);
    out.extend_from_slice(&reserved_thru.to_le_bytes());
    out
}

/// Persist `reserved_thru` (inclusive) for `store_id`.
pub fn persist_reserved_thru(
    paths: &StorePaths,
    store_id: [u8; 16],
    reserved_thru: u64,
) -> Result<(), StoreError> {
    crate::failpoint::hit("segalloc.before_reserve_persist")?;
    let bytes = encode_segment_seq_file(store_id, reserved_thru);
    write_atomic(&segment_seq_path(paths), &bytes)?;
    // Parent dir sync so reopen cannot lose the reservation after rename.
    sync_dir(&paths.store_info())?;
    crate::failpoint::hit("segalloc.after_reserve_persist")?;
    Ok(())
}

/// Load durable reservation when present and well-formed for `store_id`.
pub fn load_reserved_thru(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<Option<u64>, StoreError> {
    let path = segment_seq_path(paths);
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(&path)?;
    let (file_store, reserved) = match decode_segment_seq_file(&bytes) {
        Ok(v) => v,
        Err(e) => {
            // Ambiguous durable state: refuse rather than invent a counter.
            return Err(e);
        }
    };
    if file_store != store_id {
        return Err(StoreError::CorruptMeta(
            "segment_seq.v1 store_id mismatch; refuse allocator",
        ));
    }
    Ok(Some(reserved))
}

/// Collect every segment seq visible from authoritative / derived media names
/// and active segment descriptors.
pub fn scan_media_max_seq(
    paths: &StorePaths,
    store_id: [u8; 16],
    writer_shards: usize,
    limits: SafetyLimits,
) -> Result<u64, StoreError> {
    scan_media_max_seq_with_policy(paths, store_id, writer_shards, limits, false)
}

/// Like [`scan_media_max_seq`]; when `accept_foreign_store` is set, active
/// descriptors from another store_id still contribute their segment_id (salvage
/// / identity-reassign reopen).
pub fn scan_media_max_seq_with_policy(
    paths: &StorePaths,
    store_id: [u8; 16],
    writer_shards: usize,
    limits: SafetyLimits,
    accept_foreign_store: bool,
) -> Result<u64, StoreError> {
    let mut max = 0u64;

    let bump = |max: &mut u64, id: [u8; 16]| {
        *max = (*max).max(segment_seq_from_id(&id));
    };

    for path in list_residiuum_files(&paths.segments_dir())? {
        if let Some(id) = segment_id_from_filename(&path) {
            bump(&mut max, id);
        }
    }
    for path in list_pending_paths(paths)? {
        if let Some(id) = segment_id_from_filename(&path) {
            bump(&mut max, id);
        }
    }

    // Recovery Shadows (published + dual staging temps).
    let shadow = crate::recovery_shadow::shadow_dir(paths);
    if shadow.is_dir() {
        for ent in fs::read_dir(&shadow)? {
            let p = ent?.path();
            let name = p
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            // `{hex}.rsh` or `{hex}.rsh.dual.tmp`
            let stem = name
                .strip_suffix(".rsh.dual.tmp")
                .or_else(|| name.strip_suffix(".rsh"))
                .unwrap_or("");
            if let Some(id) = unhex16(stem) {
                bump(&mut max, id);
            }
        }
    }

    // Chimera / Hydra sidecars (derived, but prove prior publication).
    for sub in ["chimera", "seg"] {
        let dir = paths.indexes_dir().join(sub);
        if !dir.is_dir() {
            continue;
        }
        for ent in fs::read_dir(&dir)? {
            let p = ent?.path();
            let Some(stem) = p.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            // hydra may be `{hex}.hydra` etc — take 32-char hex prefix when present.
            let hex = if stem.len() >= 32 { &stem[..32] } else { stem };
            if let Some(id) = unhex16(hex) {
                bump(&mut max, id);
            }
        }
    }

    // Active headers (filename has no seq).
    for path in paths.list_active_segment_paths(writer_shards.max(1)) {
        match active_segment_id_from_file(&path, store_id, limits, accept_foreign_store)? {
            Some(id) => bump(&mut max, id),
            None => {
                // Non-empty active without a recoverable store-matching
                // descriptor is ambiguous — refuse rather than remint over it.
                let meta = fs::metadata(&path)?;
                if meta.len() > 0 {
                    return Err(StoreError::CorruptMeta(
                        "active segment present without recoverable segment_id; refuse allocator",
                    ));
                }
            }
        }
    }

    Ok(max)
}

fn active_segment_id_from_file(
    path: &Path,
    store_id: [u8; 16],
    limits: SafetyLimits,
    accept_foreign_store: bool,
) -> Result<Option<[u8; 16]>, StoreError> {
    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Ok(None);
    }
    let report = scan_forward(&bytes, limits);
    let mut foreign: Option<[u8; 16]> = None;
    for region in &report.regions {
        if let residiuum_format::ScanRegion::VerifiedFrame { frame, .. } = region {
            if frame.header.known_kind() == Some(FrameKind::SegmentDescriptor) {
                if let Some((ids, _, _)) = decode_descriptor_body(&frame.body) {
                    if ids.store_id == store_id {
                        return Ok(Some(ids.segment_id));
                    }
                    if accept_foreign_store && foreign.is_none() {
                        foreign = Some(ids.segment_id);
                    }
                }
            }
        }
    }
    Ok(foreign)
}

/// Open-time reconstruction: max(durable, media). Persists when durable is
/// missing or behind media (upgrade / crash-after-media paths).
///
/// Does **not** invent a counter below observed media. Corrupt durable with
/// matching store refusal is handled by [`load_reserved_thru`].
pub fn reconstruct_reserved_thru(
    paths: &StorePaths,
    store_id: [u8; 16],
    writer_shards: usize,
    limits: SafetyLimits,
) -> Result<u64, StoreError> {
    reconstruct_reserved_thru_with_policy(paths, store_id, writer_shards, limits, false)
}

/// Like [`reconstruct_reserved_thru`] with foreign-store active acceptance.
pub fn reconstruct_reserved_thru_with_policy(
    paths: &StorePaths,
    store_id: [u8; 16],
    writer_shards: usize,
    limits: SafetyLimits,
    accept_foreign_store: bool,
) -> Result<u64, StoreError> {
    crate::failpoint::hit("segalloc.before_reconstruct")?;
    let media_max = if accept_foreign_store {
        scan_media_max_seq_with_policy(paths, store_id, writer_shards, limits, true)?
    } else {
        scan_media_max_seq(paths, store_id, writer_shards, limits)?
    };
    let durable = match load_reserved_thru(paths, store_id) {
        Ok(v) => v,
        Err(StoreError::CorruptMeta(_)) => {
            // Durable unreadable. If media also empty → fresh store ok.
            // If media has ids → reconstruct from media only (persist below).
            // If media scan already refused, error propagated above.
            if media_max == 0
                && segment_seq_path(paths).is_file()
                && fs::metadata(segment_seq_path(paths))
                    .map(|m| m.len() > 0)
                    .unwrap_or(false)
            {
                // Non-empty corrupt durable + no media ids: ambiguous.
                return Err(StoreError::CorruptMeta(
                    "segment_seq.v1 corrupt and no media ids to reconstruct; refuse",
                ));
            }
            None
        }
        Err(e) => return Err(e),
    };

    let reserved = match durable {
        Some(d) => d.max(media_max),
        None => media_max,
    };

    // Persist so the next mint cannot under-count after reopen.
    if durable != Some(reserved) {
        persist_reserved_thru(paths, store_id, reserved)?;
    }
    crate::failpoint::hit("segalloc.after_reconstruct")?;
    Ok(reserved)
}

/// Raise in-memory high-water only (never invents below observed; never persists).
///
/// Sole in-process mutation helper for `segment_seq` outside reserve/reconstruct.
pub fn note_in_memory_high_water(reserved_thru: &mut u64, observed_seq: u64) {
    *reserved_thru = (*reserved_thru).max(observed_seq);
}

/// Reserve and return the next segment id. Persists the reservation **before**
/// the caller creates media for this id.
#[cfg(test)]
pub fn reserve_next_segment_id(
    paths: &StorePaths,
    store_id: [u8; 16],
    reserved_thru: &mut u64,
) -> Result<[u8; 16], StoreError> {
    let next = reserved_thru.saturating_add(1);
    persist_reserved_thru(paths, store_id, next)?;
    *reserved_thru = next;
    crate::failpoint::hit("segalloc.after_reserve_before_media")?;
    Ok(mint_sortable_segment_id(next, &store_id))
}

/// Issue the next id from an already-durable lease, extending the lease first
/// when exhausted.
///
/// `issued_thru` is the last id handed to media in this process;
/// `reserved_thru` is the inclusive end durably recorded in `segment_seq.v1`.
pub fn next_segment_id_from_lease(
    paths: &StorePaths,
    store_id: [u8; 16],
    issued_thru: &mut u64,
    reserved_thru: &mut u64,
) -> Result<[u8; 16], StoreError> {
    if *issued_thru >= *reserved_thru {
        let lease_end = issued_thru.saturating_add(SEGMENT_ID_LEASE);
        if lease_end == *issued_thru {
            return Err(StoreError::CorruptMeta("segment id sequence exhausted"));
        }
        persist_reserved_thru(paths, store_id, lease_end)?;
        *reserved_thru = lease_end;
    }
    *issued_thru = issued_thru.saturating_add(1);
    crate::failpoint::hit("segalloc.after_reserve_before_media")?;
    Ok(mint_sortable_segment_id(*issued_thru, &store_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::mint_sortable_segment_id;
    use tempfile::tempdir;

    #[test]
    fn encode_decode_roundtrip() {
        let sid = [7u8; 16];
        let bytes = encode_segment_seq_file(sid, 42);
        let (got, n) = decode_segment_seq_file(&bytes).unwrap();
        assert_eq!(got, sid);
        assert_eq!(n, 42);
    }

    #[test]
    fn reserve_monotonic_and_persistent() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let store_id = [9u8; 16];
        let mut thru = 0u64;
        let a = reserve_next_segment_id(&paths, store_id, &mut thru).unwrap();
        assert_eq!(thru, 1);
        assert_eq!(a, mint_sortable_segment_id(1, &store_id));
        let b = reserve_next_segment_id(&paths, store_id, &mut thru).unwrap();
        assert_eq!(b, mint_sortable_segment_id(2, &store_id));
        assert_eq!(thru, 2);
        let loaded = load_reserved_thru(&paths, store_id).unwrap().unwrap();
        assert_eq!(loaded, 2);
    }

    #[test]
    fn lease_persists_range_once_and_issues_unique_ids() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let store_id = [8u8; 16];
        let mut issued = 0u64;
        let mut reserved = 0u64;

        let first =
            next_segment_id_from_lease(&paths, store_id, &mut issued, &mut reserved).unwrap();
        assert_eq!(segment_seq_from_id(&first), 1);
        assert_eq!(reserved, SEGMENT_ID_LEASE);
        assert_eq!(
            load_reserved_thru(&paths, store_id).unwrap(),
            Some(SEGMENT_ID_LEASE)
        );

        for expect in 2..=SEGMENT_ID_LEASE {
            let id =
                next_segment_id_from_lease(&paths, store_id, &mut issued, &mut reserved).unwrap();
            assert_eq!(segment_seq_from_id(&id), expect);
        }
        assert_eq!(
            load_reserved_thru(&paths, store_id).unwrap(),
            Some(SEGMENT_ID_LEASE)
        );

        let next =
            next_segment_id_from_lease(&paths, store_id, &mut issued, &mut reserved).unwrap();
        assert_eq!(segment_seq_from_id(&next), SEGMENT_ID_LEASE + 1);
        assert_eq!(reserved, SEGMENT_ID_LEASE * 2);
    }

    #[test]
    fn lease_skips_abandoned_tail_after_reopen() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let store_id = [9u8; 16];
        persist_reserved_thru(&paths, store_id, SEGMENT_ID_LEASE).unwrap();

        // Reopen initializes both counters from durable reserved_thru. The
        // unused tail of the prior process's lease is never issued again.
        let mut issued = load_reserved_thru(&paths, store_id).unwrap().unwrap();
        let mut reserved = issued;
        let id = next_segment_id_from_lease(&paths, store_id, &mut issued, &mut reserved).unwrap();
        assert_eq!(segment_seq_from_id(&id), SEGMENT_ID_LEASE + 1);
        assert_eq!(reserved, SEGMENT_ID_LEASE * 2);
    }

    #[test]
    fn corrupt_durable_empty_media_refuses() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        fs::write(segment_seq_path(&paths), b"not-a-valid-file").unwrap();
        let err =
            reconstruct_reserved_thru(&paths, [1u8; 16], 1, SafetyLimits::default()).unwrap_err();
        assert!(matches!(err, StoreError::CorruptMeta(_)));
    }
}
