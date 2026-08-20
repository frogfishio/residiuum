//! Authenticated Recovery Shadow carriage for store-owned Atomic authority.
//!
//! Segment Shadows preserve exact segment images, but a compact-output segment
//! contains the live value projection rather than the prepare/member/decision
//! evidence retired with its sources. This bundle independently carries the
//! consolidated Atomic generation and the control files needed to reopen it.

use crate::atomic_file;
use crate::atomic_stage_recover::{
    atomic_coord_path, atomic_stage_checkpoint_path, authority_generation_paths, AtomicStageLimits,
};
use crate::atomic_tombstone_index::tombstone_index_path;
use crate::error::StoreError;
use crate::layout::StorePaths;
use std::fs;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"RSHATM01";
const HASH_LEN: usize = 32;
const MAX_ENTRIES: usize = 4;
const BUNDLE_FILE: &str = "atomic-authority.rsh";

#[derive(Debug)]
struct Entry {
    rel_path: String,
    bytes: Vec<u8>,
}

pub(crate) fn bundle_path(paths: &StorePaths) -> PathBuf {
    super::wire::shadow_dir(paths).join(BUNDLE_FILE)
}

/// Publish the exact current Atomic authority support set as one authenticated
/// Recovery Shadow. Returns false when the store has never issued an Atomic.
pub(crate) fn publish_current(paths: &StorePaths, store_id: [u8; 16]) -> Result<bool, StoreError> {
    let generations = authority_generation_paths(paths)?;
    if generations.is_empty() {
        return Ok(false);
    }
    if generations.len() != 1 {
        return Err(StoreError::AtomicStage(
            "Recovery Shadow authority publish requires exactly one current generation".into(),
        ));
    }
    let checkpoint = atomic_stage_checkpoint_path(paths);
    let coordinator = atomic_coord_path(paths);
    if !checkpoint.is_file() || !coordinator.is_file() {
        return Err(StoreError::AtomicStage(
            "Recovery Shadow authority publish requires checkpoint and coordinator".into(),
        ));
    }
    let mut source_paths = vec![generations[0].clone()];
    let tombstones = tombstone_index_path(paths);
    if tombstones.is_file() {
        source_paths.push(tombstones);
    }
    source_paths.push(coordinator);
    source_paths.push(checkpoint);
    let limits = AtomicStageLimits::operable();
    let mut total = 0u64;
    let mut entries = Vec::with_capacity(source_paths.len());
    for path in source_paths {
        let len = fs::metadata(&path)?.len();
        total = total
            .checked_add(len)
            .ok_or_else(|| StoreError::AtomicStage("Atomic Shadow size overflow".into()))?;
        if total > limits.max_work_bytes {
            return Err(StoreError::AtomicStage(
                "Atomic Shadow exceeds bounded recovery work budget".into(),
            ));
        }
        let rel_path = relative_store_path(paths, &path)?;
        entries.push(Entry {
            rel_path,
            bytes: fs::read(path)?,
        });
    }
    let encoded = encode(store_id, &entries)?;
    if encoded.len() as u64 > limits.max_work_bytes {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow exceeds bounded recovery work budget".into(),
        ));
    }
    // Verify the complete candidate before it can replace the previous
    // recovery source. Publication must never make an invalid bundle durable.
    let decoded = decode(&encoded, store_id, limits.max_work_bytes)?;
    if decoded.len() != entries.len() {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow verification lost an entry".into(),
        ));
    }
    crate::failpoint::hit("store.recovery_shadow.atomic.before_publish")?;
    atomic_file::write_atomic(&bundle_path(paths), &encoded)?;
    crate::failpoint::hit("store.recovery_shadow.atomic.after_publish")?;
    Ok(true)
}

/// Restore missing Atomic support files from a verified bundle. Immutable
/// authority-generation bytes are also repaired when their content differs;
/// mutable checkpoint/coordinator/index files are never rolled back over a
/// present newer file. The checkpoint is published last.
pub(crate) fn restore_missing(paths: &StorePaths, store_id: [u8; 16]) -> Result<bool, StoreError> {
    let has_checkpoint = atomic_stage_checkpoint_path(paths).is_file();
    let authority_dir = paths
        .store_info()
        .join(crate::atomic_stage_recover::ATOMIC_AUTHORITY_DIR);
    let generation_count = authority_dir
        .read_dir()
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| entry.path().is_file())
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .and_then(|name| name.strip_suffix(".residiuum"))
                        .is_some_and(|stem| {
                            stem.len() == 32
                                && stem.as_bytes().iter().all(|byte| {
                                    byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
                                })
                                && crate::layout::unhex16(stem).is_some()
                        })
                })
                .count()
        })
        .unwrap_or(0);
    let has_generation = generation_count == 1;
    let has_coordinator = atomic_coord_path(paths).is_file();
    if has_checkpoint && has_generation && has_coordinator {
        // Recovery media are not consulted while complete local authority is
        // present; a damaged unused Shadow cannot kneel the primary store.
        return Ok(false);
    }
    let path = bundle_path(paths);
    if !path.is_file() {
        return Ok(false);
    }
    let limits = AtomicStageLimits::operable();
    let len = fs::metadata(&path)?.len();
    if len > limits.max_work_bytes {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow exceeds bounded recovery work budget".into(),
        ));
    }
    let entries = decode(&fs::read(&path)?, store_id, limits.max_work_bytes)?;
    let checkpoint_rel = relative_store_path(paths, &atomic_stage_checkpoint_path(paths))?;
    let mut restored = false;
    for entry in entries
        .iter()
        .filter(|entry| entry.rel_path != checkpoint_rel)
        .chain(
            entries
                .iter()
                .filter(|entry| entry.rel_path == checkpoint_rel),
        )
    {
        let target = paths.root.join(&entry.rel_path);
        let immutable_generation = entry.rel_path.starts_with("store-info/atomic-authority/");
        let needs_restore = match fs::read(&target) {
            Ok(existing) => immutable_generation && existing != entry.bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => return Err(error.into()),
        };
        if !needs_restore {
            continue;
        }
        crate::failpoint::hit("store.recovery_shadow.atomic.before_restore_file")?;
        atomic_file::write_atomic(&target, &entry.bytes)?;
        crate::failpoint::hit("store.recovery_shadow.atomic.after_restore_file")?;
        restored = true;
    }
    Ok(restored)
}

fn encode(store_id: [u8; 16], entries: &[Entry]) -> Result<Vec<u8>, StoreError> {
    if entries.is_empty() || entries.len() > MAX_ENTRIES {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow entry count is outside the frozen profile".into(),
        ));
    }
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&store_id);
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        let rel = entry.rel_path.as_bytes();
        let rel_len: u16 = rel
            .len()
            .try_into()
            .map_err(|_| StoreError::AtomicStage("Atomic Shadow path too long".into()))?;
        out.extend_from_slice(&rel_len.to_le_bytes());
        out.extend_from_slice(rel);
        out.extend_from_slice(&(entry.bytes.len() as u64).to_le_bytes());
        out.extend_from_slice(blake3::hash(&entry.bytes).as_bytes());
        out.extend_from_slice(&entry.bytes);
    }
    let hash = blake3::hash(&out);
    out.extend_from_slice(hash.as_bytes());
    Ok(out)
}

fn decode(bytes: &[u8], expect_store: [u8; 16], max_bytes: u64) -> Result<Vec<Entry>, StoreError> {
    if bytes.len() as u64 > max_bytes || bytes.len() < 8 + 16 + 4 + HASH_LEN {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow is incomplete or over budget".into(),
        ));
    }
    let payload_end = bytes.len() - HASH_LEN;
    if blake3::hash(&bytes[..payload_end]).as_bytes() != &bytes[payload_end..] {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow commitment mismatch".into(),
        ));
    }
    let mut cur = &bytes[..payload_end];
    if take(&mut cur, 8)? != MAGIC {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow magic mismatch".into(),
        ));
    }
    if take(&mut cur, 16)? != expect_store {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow store identity mismatch".into(),
        ));
    }
    let count = u32::from_le_bytes(take(&mut cur, 4)?.try_into().unwrap()) as usize;
    if count == 0 || count > MAX_ENTRIES {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow entry count is outside the frozen profile".into(),
        ));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let rel_len = u16::from_le_bytes(take(&mut cur, 2)?.try_into().unwrap()) as usize;
        let rel_path = String::from_utf8(take(&mut cur, rel_len)?.to_vec())
            .map_err(|_| StoreError::AtomicStage("Atomic Shadow path is not UTF-8".into()))?;
        validate_relative_path(&rel_path)?;
        if entries
            .iter()
            .any(|entry: &Entry| entry.rel_path == rel_path)
        {
            return Err(StoreError::AtomicStage(
                "Atomic Shadow contains a duplicate path".into(),
            ));
        }
        let len = u64::from_le_bytes(take(&mut cur, 8)?.try_into().unwrap());
        let hash: [u8; 32] = take(&mut cur, 32)?.try_into().unwrap();
        let len: usize = len
            .try_into()
            .map_err(|_| StoreError::AtomicStage("Atomic Shadow entry too large".into()))?;
        let body = take(&mut cur, len)?.to_vec();
        if *blake3::hash(&body).as_bytes() != hash {
            return Err(StoreError::AtomicStage(
                "Atomic Shadow entry commitment mismatch".into(),
            ));
        }
        entries.push(Entry {
            rel_path,
            bytes: body,
        });
    }
    if !cur.is_empty() {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow has trailing bytes".into(),
        ));
    }
    let generations = entries
        .iter()
        .filter(|entry| entry.rel_path.starts_with("store-info/atomic-authority/"))
        .count();
    let checkpoints = entries
        .iter()
        .filter(|entry| entry.rel_path == "store-info/atomic-stage.ckpt")
        .count();
    let coordinators = entries
        .iter()
        .filter(|entry| entry.rel_path == "store-info/atomic-coord.ckpt")
        .count();
    if generations != 1 || checkpoints != 1 || coordinators != 1 {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow does not contain one complete authority generation".into(),
        ));
    }
    Ok(entries)
}

fn take<'a>(cur: &mut &'a [u8], len: usize) -> Result<&'a [u8], StoreError> {
    if cur.len() < len {
        return Err(StoreError::AtomicStage("Atomic Shadow truncated".into()));
    }
    let (head, tail) = cur.split_at(len);
    *cur = tail;
    Ok(head)
}

fn relative_store_path(paths: &StorePaths, path: &Path) -> Result<String, StoreError> {
    let rel = path
        .strip_prefix(&paths.root)
        .map_err(|_| StoreError::AtomicStage("Atomic Shadow path escaped store".into()))?;
    let rel = rel
        .to_str()
        .ok_or_else(|| StoreError::AtomicStage("Atomic Shadow path is not UTF-8".into()))?;
    validate_relative_path(rel)?;
    Ok(rel.to_string())
}

fn validate_relative_path(rel: &str) -> Result<(), StoreError> {
    let authority_name = rel
        .strip_prefix("store-info/atomic-authority/")
        .and_then(|name| name.strip_suffix(".residiuum"));
    let valid_authority_name = authority_name.is_some_and(|name| {
        name.len() == 32
            && name
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    });
    let allowed = rel == "store-info/atomic-stage.ckpt"
        || rel == "store-info/atomic-coord.ckpt"
        || rel == "store-info/atomic-tombstones.idx"
        || valid_authority_name;
    if !allowed
        || Path::new(rel).is_absolute()
        || Path::new(rel)
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(StoreError::AtomicStage(
            "Atomic Shadow contains an unsafe or unsupported path".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_entries() -> Vec<Entry> {
        vec![
            Entry {
                rel_path: "store-info/atomic-authority/00112233445566778899aabbccddeeff.residiuum"
                    .into(),
                bytes: b"authority".to_vec(),
            },
            Entry {
                rel_path: "store-info/atomic-coord.ckpt".into(),
                bytes: b"coordinator".to_vec(),
            },
            Entry {
                rel_path: "store-info/atomic-stage.ckpt".into(),
                bytes: b"checkpoint".to_vec(),
            },
        ]
    }

    #[test]
    fn authenticated_bundle_round_trips_complete_authority() {
        let store = [7u8; 16];
        let bytes = encode(store, &complete_entries()).unwrap();
        let decoded = decode(&bytes, store, bytes.len() as u64).unwrap();
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn authenticated_bundle_rejects_wrong_store_and_truncation() {
        let store = [7u8; 16];
        let bytes = encode(store, &complete_entries()).unwrap();
        assert!(decode(&bytes, [8u8; 16], bytes.len() as u64).is_err());
        assert!(decode(&bytes[..bytes.len() - 1], store, bytes.len() as u64).is_err());
    }

    #[test]
    fn authenticated_bundle_rejects_nested_or_noncanonical_generation_path() {
        for bad in [
            "store-info/atomic-authority/nested/00112233445566778899aabbccddeeff.residiuum",
            "store-info/atomic-authority/00112233445566778899AABBCCDDEEFF.residiuum",
        ] {
            let store = [7u8; 16];
            let mut entries = complete_entries();
            entries[0].rel_path = bad.into();
            let bytes = encode(store, &entries).unwrap();
            assert!(decode(&bytes, store, bytes.len() as u64).is_err(), "{bad}");
        }
    }

    #[test]
    fn authenticated_bundle_rejects_missing_or_duplicate_mandatory_entries() {
        let store = [7u8; 16];
        let mut missing = complete_entries();
        missing.pop();
        let bytes = encode(store, &missing).unwrap();
        assert!(decode(&bytes, store, bytes.len() as u64).is_err());

        let mut duplicate = complete_entries();
        duplicate.push(Entry {
            rel_path: "store-info/atomic-stage.ckpt".into(),
            bytes: b"other".to_vec(),
        });
        let bytes = encode(store, &duplicate).unwrap();
        assert!(decode(&bytes, store, bytes.len() as u64).is_err());
    }

    #[test]
    fn optional_tombstone_index_does_not_make_unused_corrupt_bundle_authoritative() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let generation = paths
            .store_info()
            .join(crate::atomic_stage_recover::ATOMIC_AUTHORITY_DIR)
            .join("00112233445566778899aabbccddeeff.residiuum");
        fs::create_dir_all(generation.parent().unwrap()).unwrap();
        fs::write(generation, b"authority").unwrap();
        fs::write(atomic_stage_checkpoint_path(&paths), b"checkpoint").unwrap();
        fs::write(atomic_coord_path(&paths), b"coordinator").unwrap();
        fs::create_dir_all(super::super::wire::shadow_dir(&paths)).unwrap();
        fs::write(bundle_path(&paths), b"corrupt").unwrap();

        assert!(!restore_missing(&paths, [7u8; 16]).unwrap());
    }
}
