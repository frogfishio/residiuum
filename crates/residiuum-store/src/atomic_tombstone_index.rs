//! Authenticated fixed-page lifetime Atomic decision index (ATM-4A).
//!
//! `ATTOMB1` records in store media remain authority. This file is a derived,
//! rebuildable point-lookup accelerator. Its Merkle root is bound into the
//! Atomic checkpoint, so a lookup reads and authenticates only one fixed page
//! plus `O(log pages)` sibling hashes. Heap-lifetime cardinality is never
//! materialized merely to answer one status or retry request.

use crate::atomic_file::write_atomic_if_changed;
use crate::atomic_stage_media::{RetainedDecisionTombstone, StageAtomicKey};
use crate::error::StoreError;
use crate::layout::StorePaths;
use residiuum_atomics::{
    AtomicAbortReason, AtomicId, ContentRoot, DecisionCode, DecisionTombstone, HeapId,
};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

const MAGIC: &[u8; 6] = b"ATTSI1";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 96;
const HEADER_DOMAIN: &[u8] = b"RESIDIUUM-ATOMIC-TOMBSTONE-INDEX-HEADER-V1";
const PAGE_MAGIC: &[u8; 5] = b"ATPG1";
const PAGE_DOMAIN: &[u8] = b"RESIDIUUM-ATOMIC-TOMBSTONE-INDEX-PAGE-V1";
const NODE_DOMAIN: &[u8] = b"RESIDIUUM-ATOMIC-TOMBSTONE-INDEX-NODE-V1";
const EMPTY_DOMAIN: &[u8] = b"RESIDIUUM-ATOMIC-TOMBSTONE-INDEX-EMPTY-V1";
const PAGE_SIZE: usize = 64 * 1024;
const PAGE_HEADER_LEN: usize = 16;
const PAGE_DIGEST_LEN: usize = 32;
const RECORD_LEN: usize = 132;
const RECORDS_PER_PAGE: usize = (PAGE_SIZE - PAGE_HEADER_LEN - PAGE_DIGEST_LEN) / RECORD_LEN;

pub(crate) const TOMBSTONE_INDEX_FILE: &str = "atomic-tombstones.idx";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TombstoneIndexMeta {
    pub record_count: u64,
    pub file_len: u64,
    pub root: [u8; 32],
}

pub(crate) fn tombstone_index_path(paths: &StorePaths) -> PathBuf {
    paths.store_info().join(TOMBSTONE_INDEX_FILE)
}

/// Merge newly catalogued tombstones into the sorted fixed-page index.
/// Exact duplicates retain the earliest authenticated decision time.
pub(crate) fn refresh_index(
    paths: &StorePaths,
    prior: Option<TombstoneIndexMeta>,
    additions: &BTreeMap<StageAtomicKey, RetainedDecisionTombstone>,
) -> Result<Option<TombstoneIndexMeta>, StoreError> {
    if prior.is_none() && additions.is_empty() {
        return Ok(None);
    }
    if let Some(meta) = prior {
        let mut complete = true;
        for (key, retained) in additions {
            match lookup(paths, meta, *key)? {
                Some(indexed) if indexed.tombstone == retained.tombstone => {
                    if retained.decided_at_unix_s < indexed.decided_at_unix_s {
                        complete = false;
                    }
                }
                Some(_) => {
                    return Err(StoreError::AtomicStage(
                        "conflicting lifetime tombstones cannot share one index key".into(),
                    ));
                }
                None => complete = false,
            }
        }
        if complete {
            return Ok(Some(meta));
        }
    }
    let mut records = match prior {
        Some(meta) => read_all(paths, meta)?,
        None => Vec::new(),
    };
    let mut by_key = records.drain(..).collect::<BTreeMap<_, _>>();
    for (key, retained) in additions {
        match by_key.get(key) {
            None => {
                by_key.insert(*key, *retained);
            }
            Some(existing) if existing.tombstone == retained.tombstone => {
                if retained.decided_at_unix_s < existing.decided_at_unix_s {
                    by_key.insert(*key, *retained);
                }
            }
            Some(_) => {
                return Err(StoreError::AtomicStage(
                    "conflicting lifetime tombstones cannot share one index key".into(),
                ));
            }
        }
    }
    let records = by_key.into_iter().collect::<Vec<_>>();
    let (bytes, meta) = encode_index(&records)?;
    write_atomic_if_changed(&tombstone_index_path(paths), &bytes)?;
    Ok(Some(meta))
}

/// Authenticate and find one `(HeapId, AtomicId)` without loading the index.
pub(crate) fn lookup(
    paths: &StorePaths,
    meta: TombstoneIndexMeta,
    key: StageAtomicKey,
) -> Result<Option<RetainedDecisionTombstone>, StoreError> {
    let path = tombstone_index_path(paths);
    let mut file = File::open(&path).map_err(|error| {
        StoreError::AtomicStage(format!("lifetime tombstone index unavailable: {error}"))
    })?;
    let header = read_header(&mut file, meta)?;
    if header.record_count == 0 {
        return Ok(None);
    }
    let mut lo = 0u64;
    let mut hi = header.record_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let page_index = (mid / RECORDS_PER_PAGE as u64) as u32;
        let slot = (mid % RECORDS_PER_PAGE as u64) as usize;
        let page = read_verified_page(&mut file, &header, page_index)?;
        let (candidate, retained) = decode_record(record_slice(&page, slot)?)?;
        match candidate.cmp(&key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(Some(retained)),
        }
    }
    Ok(None)
}

pub(crate) fn validate_index(
    paths: &StorePaths,
    meta: TombstoneIndexMeta,
) -> Result<(), StoreError> {
    let mut file = File::open(tombstone_index_path(paths))?;
    let _ = read_header(&mut file, meta)?;
    Ok(())
}

#[derive(Clone, Copy)]
struct Header {
    record_count: u64,
    page_count: u32,
    leaf_power: u32,
    root: [u8; 32],
    tree_offset: u64,
}

fn encode_index(
    records: &[(StageAtomicKey, RetainedDecisionTombstone)],
) -> Result<(Vec<u8>, TombstoneIndexMeta), StoreError> {
    let record_count = records.len() as u64;
    let page_count = records.len().div_ceil(RECORDS_PER_PAGE) as u32;
    let leaf_power = page_count.max(1).next_power_of_two();
    let mut pages = Vec::with_capacity(page_count as usize);
    let mut leaves = Vec::with_capacity(leaf_power as usize);
    for page_index in 0..page_count {
        let start = page_index as usize * RECORDS_PER_PAGE;
        let end = (start + RECORDS_PER_PAGE).min(records.len());
        let mut page = vec![0u8; PAGE_SIZE];
        page[..5].copy_from_slice(PAGE_MAGIC);
        page[8..12].copy_from_slice(&page_index.to_be_bytes());
        page[12..16].copy_from_slice(&((end - start) as u32).to_be_bytes());
        for (slot, (key, retained)) in records[start..end].iter().enumerate() {
            let encoded = encode_record(*key, *retained)?;
            let at = PAGE_HEADER_LEN + slot * RECORD_LEN;
            page[at..at + RECORD_LEN].copy_from_slice(&encoded);
        }
        let digest = page_hash(page_index, &page[..PAGE_SIZE - PAGE_DIGEST_LEN]);
        page[PAGE_SIZE - PAGE_DIGEST_LEN..].copy_from_slice(&digest);
        leaves.push(digest);
        pages.push(page);
    }
    while leaves.len() < leaf_power as usize {
        leaves.push(empty_leaf_hash(leaves.len() as u32));
    }
    let node_count = leaf_power as usize * 2 - 1;
    let leaf_base = leaf_power as usize - 1;
    let mut tree = vec![[0u8; 32]; node_count];
    tree[leaf_base..leaf_base + leaves.len()].copy_from_slice(&leaves);
    for index in (0..leaf_base).rev() {
        tree[index] = node_hash(tree[index * 2 + 1], tree[index * 2 + 2]);
    }
    let root = tree[0];
    let file_len =
        HEADER_LEN as u64 + page_count as u64 * PAGE_SIZE as u64 + node_count as u64 * 32;
    let mut header = vec![0u8; HEADER_LEN];
    header[..6].copy_from_slice(MAGIC);
    header[6] = VERSION;
    header[8..16].copy_from_slice(&record_count.to_be_bytes());
    header[16..20].copy_from_slice(&(RECORDS_PER_PAGE as u32).to_be_bytes());
    header[20..24].copy_from_slice(&page_count.to_be_bytes());
    header[24..28].copy_from_slice(&leaf_power.to_be_bytes());
    header[32..64].copy_from_slice(&root);
    let digest = header_hash(&header[..64]);
    header[64..96].copy_from_slice(&digest);
    let mut out = Vec::with_capacity(file_len as usize);
    out.extend_from_slice(&header);
    for page in pages {
        out.extend_from_slice(&page);
    }
    for node in tree {
        out.extend_from_slice(&node);
    }
    Ok((
        out,
        TombstoneIndexMeta {
            record_count,
            file_len,
            root,
        },
    ))
}

fn read_header(file: &mut File, expected: TombstoneIndexMeta) -> Result<Header, StoreError> {
    if file.metadata()?.len() != expected.file_len {
        return Err(corrupt("length mismatch"));
    }
    let mut bytes = [0u8; HEADER_LEN];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut bytes)?;
    if &bytes[..6] != MAGIC || bytes[6] != VERSION || bytes[7] != 0 {
        return Err(corrupt("header magic/version"));
    }
    if bytes[28..32] != [0; 4] || header_hash(&bytes[..64]) != bytes[64..96] {
        return Err(corrupt("header checksum/reserved"));
    }
    let record_count = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let records_per_page = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
    let page_count = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
    let leaf_power = u32::from_be_bytes(bytes[24..28].try_into().unwrap());
    let root: [u8; 32] = bytes[32..64].try_into().unwrap();
    if records_per_page != RECORDS_PER_PAGE as u32
        || page_count != record_count.div_ceil(RECORDS_PER_PAGE as u64) as u32
        || leaf_power != page_count.max(1).next_power_of_two()
        || record_count != expected.record_count
        || root != expected.root
    {
        return Err(corrupt("header descriptor mismatch"));
    }
    let tree_offset = HEADER_LEN as u64 + page_count as u64 * PAGE_SIZE as u64;
    let node_count = leaf_power as u64 * 2 - 1;
    if tree_offset.saturating_add(node_count.saturating_mul(32)) != expected.file_len {
        return Err(corrupt("tree extent"));
    }
    if read_node(file, tree_offset, 0)? != root {
        return Err(corrupt("stored Merkle root"));
    }
    Ok(Header {
        record_count,
        page_count,
        leaf_power,
        root,
        tree_offset,
    })
}

fn read_verified_page(
    file: &mut File,
    header: &Header,
    page_index: u32,
) -> Result<Vec<u8>, StoreError> {
    if page_index >= header.page_count {
        return Err(corrupt("page range"));
    }
    let mut page = vec![0u8; PAGE_SIZE];
    let offset = HEADER_LEN as u64 + page_index as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut page)?;
    let count = u32::from_be_bytes(page[12..16].try_into().unwrap()) as usize;
    let expected_count = if page_index + 1 == header.page_count {
        (header.record_count as usize - page_index as usize * RECORDS_PER_PAGE)
            .min(RECORDS_PER_PAGE)
    } else {
        RECORDS_PER_PAGE
    };
    if &page[..5] != PAGE_MAGIC
        || page[5..8] != [0; 3]
        || u32::from_be_bytes(page[8..12].try_into().unwrap()) != page_index
        || count != expected_count
    {
        return Err(corrupt("page header"));
    }
    let used = PAGE_HEADER_LEN + count * RECORD_LEN;
    if page[used..PAGE_SIZE - PAGE_DIGEST_LEN]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(corrupt("page padding"));
    }
    let digest = page_hash(page_index, &page[..PAGE_SIZE - PAGE_DIGEST_LEN]);
    if page[PAGE_SIZE - PAGE_DIGEST_LEN..] != digest {
        return Err(corrupt("page digest"));
    }
    verify_merkle_path(file, header, page_index, digest)?;
    Ok(page)
}

fn verify_merkle_path(
    file: &mut File,
    header: &Header,
    page_index: u32,
    leaf: [u8; 32],
) -> Result<(), StoreError> {
    let mut node = header.leaf_power as u64 - 1 + page_index as u64;
    let stored = read_node(file, header.tree_offset, node)?;
    if stored != leaf {
        return Err(corrupt("leaf hash"));
    }
    let mut hash = leaf;
    while node != 0 {
        let (sibling, parent, left) = if node % 2 == 1 {
            (node + 1, (node - 1) / 2, true)
        } else {
            (node - 1, (node - 2) / 2, false)
        };
        let sibling_hash = read_node(file, header.tree_offset, sibling)?;
        hash = if left {
            node_hash(hash, sibling_hash)
        } else {
            node_hash(sibling_hash, hash)
        };
        node = parent;
    }
    if hash != header.root {
        return Err(corrupt("Merkle root"));
    }
    Ok(())
}

fn read_node(file: &mut File, tree_offset: u64, index: u64) -> Result<[u8; 32], StoreError> {
    let mut hash = [0u8; 32];
    file.seek(SeekFrom::Start(tree_offset + index * 32))?;
    file.read_exact(&mut hash)?;
    Ok(hash)
}

fn read_all(
    paths: &StorePaths,
    meta: TombstoneIndexMeta,
) -> Result<Vec<(StageAtomicKey, RetainedDecisionTombstone)>, StoreError> {
    let mut file = File::open(tombstone_index_path(paths))?;
    let header = read_header(&mut file, meta)?;
    let mut out = Vec::with_capacity(header.record_count as usize);
    for page_index in 0..header.page_count {
        let page = read_verified_page(&mut file, &header, page_index)?;
        let count = u32::from_be_bytes(page[12..16].try_into().unwrap()) as usize;
        for slot in 0..count {
            out.push(decode_record(record_slice(&page, slot)?)?);
        }
    }
    if out.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(corrupt("record order"));
    }
    Ok(out)
}

fn record_slice(page: &[u8], slot: usize) -> Result<&[u8], StoreError> {
    let at = PAGE_HEADER_LEN + slot.saturating_mul(RECORD_LEN);
    page.get(at..at + RECORD_LEN)
        .ok_or_else(|| corrupt("record range"))
}

fn encode_record(
    key: StageAtomicKey,
    retained: RetainedDecisionTombstone,
) -> Result<[u8; RECORD_LEN], StoreError> {
    retained
        .tombstone
        .validate()
        .map_err(|error| StoreError::AtomicStage(error.to_string()))?;
    if key.1 != retained.tombstone.atomic_id {
        return Err(corrupt("record Atomic ID mismatch"));
    }
    let mut out = [0u8; RECORD_LEN];
    out[..16].copy_from_slice(key.0.as_bytes());
    out[16..48].copy_from_slice(key.1.as_bytes());
    out[48..56].copy_from_slice(&retained.decided_at_unix_s.to_be_bytes());
    out[56..88].copy_from_slice(retained.tombstone.content_root.as_bytes());
    out[88] = retained.tombstone.decision.wire_code();
    out[89] = retained
        .tombstone
        .abort_reason
        .map_or(0, |reason| reason.wire_code());
    out[92..100].copy_from_slice(
        &retained
            .tombstone
            .commit_position
            .unwrap_or(0)
            .to_be_bytes(),
    );
    out[100..132].copy_from_slice(&retained.tombstone.decision_hash);
    Ok(out)
}

fn decode_record(bytes: &[u8]) -> Result<(StageAtomicKey, RetainedDecisionTombstone), StoreError> {
    if bytes.len() != RECORD_LEN || bytes[90..92] != [0; 2] {
        return Err(corrupt("record shape"));
    }
    let heap_id = HeapId::from_bytes(bytes[..16].try_into().unwrap())
        .map_err(|_| corrupt("record Heap ID"))?;
    let atomic_id = AtomicId::from_bytes(bytes[16..48].try_into().unwrap())
        .map_err(|_| corrupt("record Atomic ID"))?;
    let decided_at_unix_s = u64::from_be_bytes(bytes[48..56].try_into().unwrap());
    let content_root = ContentRoot::from_bytes(bytes[56..88].try_into().unwrap())
        .map_err(|_| corrupt("record content root"))?;
    let decision = DecisionCode::from_wire_code(bytes[88]).ok_or_else(|| corrupt("decision"))?;
    let abort_reason = if bytes[89] == 0 {
        None
    } else {
        Some(AtomicAbortReason::from_wire_code(bytes[89]).ok_or_else(|| corrupt("abort reason"))?)
    };
    let raw_position = u64::from_be_bytes(bytes[92..100].try_into().unwrap());
    let commit_position = (raw_position != 0).then_some(raw_position);
    let decision_hash = bytes[100..132].try_into().unwrap();
    let tombstone = DecisionTombstone {
        atomic_id,
        content_root,
        decision,
        commit_position,
        decision_hash,
        abort_reason,
    };
    tombstone.validate().map_err(|_| corrupt("decision axes"))?;
    Ok((
        (heap_id, atomic_id),
        RetainedDecisionTombstone {
            decided_at_unix_s,
            tombstone,
        },
    ))
}

fn header_hash(bytes: &[u8]) -> [u8; 32] {
    domain_hash(HEADER_DOMAIN, &[bytes])
}

fn page_hash(index: u32, bytes: &[u8]) -> [u8; 32] {
    domain_hash(PAGE_DOMAIN, &[&index.to_be_bytes(), bytes])
}

fn empty_leaf_hash(index: u32) -> [u8; 32] {
    domain_hash(EMPTY_DOMAIN, &[&index.to_be_bytes()])
}

fn node_hash(left: [u8; 32], right: [u8; 32]) -> [u8; 32] {
    domain_hash(NODE_DOMAIN, &[&left, &right])
}

fn domain_hash(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    for part in parts {
        hasher.update(part);
    }
    *hasher.finalize().as_bytes()
}

fn corrupt(detail: &str) -> StoreError {
    StoreError::AtomicStage(format!("lifetime tombstone index corrupt: {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn heap(first: u8) -> HeapId {
        let mut bytes = [0u8; 16];
        bytes[0] = first;
        HeapId::from_bytes(bytes).unwrap()
    }

    fn atomic(n: u32) -> AtomicId {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&n.to_be_bytes());
        AtomicId::from_bytes(bytes).unwrap()
    }

    fn retained(n: u32) -> RetainedDecisionTombstone {
        let mut root = [0u8; 32];
        root[..4].copy_from_slice(&n.to_be_bytes());
        RetainedDecisionTombstone {
            decided_at_unix_s: u64::from(n),
            tombstone: DecisionTombstone {
                atomic_id: atomic(n),
                content_root: ContentRoot::from_bytes(root).unwrap(),
                decision: DecisionCode::NotCommitted,
                commit_position: None,
                decision_hash: [n as u8; 32],
                abort_reason: Some(AtomicAbortReason::RecoveryAbort),
            },
        }
    }

    #[test]
    fn fixed_page_index_point_lookup_crosses_pages() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let heap = heap(7);
        let mut additions = BTreeMap::new();
        for n in 1..=(RECORDS_PER_PAGE as u32 * 3 + 17) {
            additions.insert((heap, atomic(n)), retained(n));
        }
        let meta = refresh_index(&paths, None, &additions).unwrap().unwrap();
        assert_eq!(meta.record_count, additions.len() as u64);
        for n in [
            1,
            RECORDS_PER_PAGE as u32,
            RECORDS_PER_PAGE as u32 + 1,
            meta.record_count as u32,
        ] {
            assert_eq!(
                lookup(&paths, meta, (heap, atomic(n))).unwrap(),
                Some(retained(n))
            );
        }
        assert_eq!(
            lookup(&paths, meta, (heap, atomic(u32::MAX))).unwrap(),
            None
        );
    }

    #[test]
    fn page_corruption_fails_the_merkle_proof() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let heap = heap(7);
        let mut additions = BTreeMap::new();
        additions.insert((heap, atomic(1)), retained(1));
        let meta = refresh_index(&paths, None, &additions).unwrap().unwrap();
        let path = tombstone_index_path(&paths);
        let mut bytes = fs::read(&path).unwrap();
        bytes[HEADER_LEN + PAGE_HEADER_LEN + 3] ^= 0x80;
        fs::write(&path, bytes).unwrap();
        assert!(lookup(&paths, meta, (heap, atomic(1))).is_err());
    }

    #[test]
    fn duplicate_tombstone_retains_earliest_decision_time() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let key = (heap(7), atomic(1));
        let mut first = retained(1);
        first.decided_at_unix_s = 100;
        let meta = refresh_index(&paths, None, &BTreeMap::from([(key, first)]))
            .unwrap()
            .unwrap();
        let mut earlier = first;
        earlier.decided_at_unix_s = 50;
        let meta = refresh_index(&paths, Some(meta), &BTreeMap::from([(key, earlier)]))
            .unwrap()
            .unwrap();
        assert_eq!(lookup(&paths, meta, key).unwrap(), Some(earlier));
    }
}
