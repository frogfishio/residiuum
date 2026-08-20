//! Copy-on-write authenticated lifetime Atomic decision index (ATM-4A).
//!
//! `ATTOMB2` media remains authority. Updates append only the changed B+tree
//! leaf and ancestor path, sync those pages, then let the Atomic checkpoint
//! publish the new root. A crash leaves the old root valid; the next writer
//! discards any uncommitted suffix.

use crate::atomic_stage_media::{RetainedDecisionTombstone, StageAtomicKey};
use crate::error::StoreError;
use crate::layout::StorePaths;
use residiuum_atomics::{
    AtomicAbortReason, AtomicId, ContentRoot, DecisionCode, DecisionTombstone, HeapId,
};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"ATTSI2\0\0";
const VERSION: u8 = 2;
const HEADER_LEN: usize = 64;
const HEADER_DOMAIN: &[u8] = b"RESIDIUUM-ATOMIC-TOMBSTONE-INDEX-HEADER-V2";
const PAGE_MAGIC: &[u8; 4] = b"ATP2";
const PAGE_DOMAIN: &[u8] = b"RESIDIUUM-ATOMIC-TOMBSTONE-INDEX-PAGE-V2";
const PAGE_SIZE: usize = 4 * 1024;
const PAGE_HEADER_LEN: usize = 16;
const PAGE_DIGEST_LEN: usize = 32;
const PAGE_CONTENT_LEN: usize = PAGE_SIZE - PAGE_DIGEST_LEN;
const RECORD_LEN: usize = 132;
const CHILD_LEN: usize = 88;
const LEAF_CAPACITY: usize = (PAGE_CONTENT_LEN - PAGE_HEADER_LEN) / RECORD_LEN;
const INTERNAL_CAPACITY: usize = (PAGE_CONTENT_LEN - PAGE_HEADER_LEN) / CHILD_LEN;
const KIND_LEAF: u8 = 1;
const KIND_INTERNAL: u8 = 2;

pub(crate) const TOMBSTONE_INDEX_FILE: &str = "atomic-tombstones.idx";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TombstoneIndexMeta {
    pub record_count: u64,
    /// Exact checkpoint-committed prefix. A crash suffix may physically exist.
    pub file_len: u64,
    pub root: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NodeRef {
    max_key: StageAtomicKey,
    offset: u64,
    hash: [u8; 32],
    level: u8,
}

enum Node {
    Leaf(Vec<(StageAtomicKey, RetainedDecisionTombstone)>),
    Internal { level: u8, children: Vec<NodeRef> },
}

struct InsertResult {
    refs: Vec<NodeRef>,
    added: bool,
    changed: bool,
}

pub(crate) fn tombstone_index_path(paths: &StorePaths) -> PathBuf {
    paths.store_info().join(TOMBSTONE_INDEX_FILE)
}

/// Merge new tombstones using COW page updates. The first build uses a linear
/// bottom-up bulk loader so recovery does not incur per-record path rewrites.
pub(crate) fn refresh_index(
    paths: &StorePaths,
    prior: Option<TombstoneIndexMeta>,
    additions: &BTreeMap<StageAtomicKey, RetainedDecisionTombstone>,
) -> Result<Option<TombstoneIndexMeta>, StoreError> {
    if additions.is_empty() {
        return Ok(prior);
    }
    let path = tombstone_index_path(paths);
    if prior.is_none() {
        return bulk_build(&path, additions).map(Some);
    }
    let mut meta = prior.unwrap();
    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
    validate_header(&mut file)?;
    validate_meta_extent(&file, meta)?;
    if file.metadata()?.len() != meta.file_len {
        file.set_len(meta.file_len)?;
        file.sync_all()?;
    }
    let mut append_at = meta.file_len;
    let mut root = root_ref(&mut file, meta)?;
    let mut dirty = false;
    for (key, retained) in additions {
        let result = insert_at(&mut file, append_at, root, *key, *retained, &mut append_at)?;
        if !result.changed {
            continue;
        }
        dirty = true;
        if result.added {
            meta.record_count = meta.record_count.checked_add(1).ok_or_else(|| {
                StoreError::AtomicStage("lifetime tombstone count overflow".into())
            })?;
        }
        root = if result.refs.len() == 1 {
            result.refs[0]
        } else {
            append_internal(&mut file, &mut append_at, root.level + 1, &result.refs)?
        };
        meta.file_len = append_at;
        meta.root = root.hash;
    }
    if dirty {
        // New pages must be durable before the checkpoint may publish root.
        file.sync_all()?;
        crate::failpoint::hit("store.atomic.tombstone_index.after_sync")?;
    }
    Ok(Some(meta))
}

/// Authenticate and find one key in O(tree height) fixed-page reads.
pub(crate) fn lookup(
    paths: &StorePaths,
    meta: TombstoneIndexMeta,
    key: StageAtomicKey,
) -> Result<Option<RetainedDecisionTombstone>, StoreError> {
    let mut file = File::open(tombstone_index_path(paths)).map_err(|error| {
        StoreError::AtomicStage(format!("lifetime tombstone index unavailable: {error}"))
    })?;
    validate_header(&mut file)?;
    validate_meta_extent(&file, meta)?;
    let mut refer = root_ref(&mut file, meta)?;
    loop {
        match read_node(&mut file, meta.file_len, refer)? {
            Node::Leaf(rows) => {
                return match rows.binary_search_by_key(&key, |(candidate, _)| *candidate) {
                    Ok(index) => Ok(Some(rows[index].1)),
                    Err(_) => Ok(None),
                };
            }
            Node::Internal { children, .. } => {
                let index = children
                    .partition_point(|child| child.max_key < key)
                    .min(children.len() - 1);
                refer = children[index];
            }
        }
    }
}

/// Cheap open validation. Descendants authenticate lazily on lookup paths.
pub(crate) fn validate_index(
    paths: &StorePaths,
    meta: TombstoneIndexMeta,
) -> Result<(), StoreError> {
    let mut file = File::open(tombstone_index_path(paths))?;
    validate_header(&mut file)?;
    validate_meta_extent(&file, meta)?;
    let root = root_ref(&mut file, meta)?;
    let _ = read_node(&mut file, meta.file_len, root)?;
    Ok(())
}

fn bulk_build(
    path: &Path,
    additions: &BTreeMap<StageAtomicKey, RetainedDecisionTombstone>,
) -> Result<TombstoneIndexMeta, StoreError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(path)?;
    write_header(&mut file)?;
    let mut append_at = HEADER_LEN as u64;
    let rows = additions
        .iter()
        .map(|(key, value)| (*key, *value))
        .collect::<Vec<_>>();
    let mut level = rows
        .chunks(LEAF_CAPACITY)
        .map(|chunk| append_leaf(&mut file, &mut append_at, chunk))
        .collect::<Result<Vec<_>, _>>()?;
    while level.len() > 1 {
        let next_level = level[0].level + 1;
        level = level
            .chunks(INTERNAL_CAPACITY)
            .map(|chunk| append_internal(&mut file, &mut append_at, next_level, chunk))
            .collect::<Result<Vec<_>, _>>()?;
    }
    let root = level[0];
    file.sync_all()?;
    crate::failpoint::hit("store.atomic.tombstone_index.after_sync")?;
    Ok(TombstoneIndexMeta {
        record_count: additions.len() as u64,
        file_len: append_at,
        root: root.hash,
    })
}

fn insert_at(
    file: &mut File,
    readable_len: u64,
    refer: NodeRef,
    key: StageAtomicKey,
    retained: RetainedDecisionTombstone,
    append_at: &mut u64,
) -> Result<InsertResult, StoreError> {
    match read_node(file, readable_len, refer)? {
        Node::Leaf(mut rows) => {
            let (added, changed) =
                match rows.binary_search_by_key(&key, |(candidate, _)| *candidate) {
                    Ok(index) => {
                        let existing = rows[index].1;
                        if existing.tombstone != retained.tombstone {
                            return Err(StoreError::AtomicStage(
                                "conflicting lifetime tombstones cannot share one index key".into(),
                            ));
                        }
                        if retained.decided_at_unix_s < existing.decided_at_unix_s {
                            rows[index] = (key, retained);
                            (false, true)
                        } else {
                            (false, false)
                        }
                    }
                    Err(index) => {
                        rows.insert(index, (key, retained));
                        (true, true)
                    }
                };
            if !changed {
                return Ok(InsertResult {
                    refs: vec![refer],
                    added,
                    changed,
                });
            }
            let refs = if rows.len() <= LEAF_CAPACITY {
                vec![append_leaf(file, append_at, &rows)?]
            } else {
                let right = rows.split_off(rows.len() / 2);
                vec![
                    append_leaf(file, append_at, &rows)?,
                    append_leaf(file, append_at, &right)?,
                ]
            };
            Ok(InsertResult {
                refs,
                added,
                changed,
            })
        }
        Node::Internal {
            level,
            mut children,
        } => {
            let index = children
                .partition_point(|child| child.max_key < key)
                .min(children.len() - 1);
            let child = insert_at(file, *append_at, children[index], key, retained, append_at)?;
            if !child.changed {
                return Ok(InsertResult {
                    refs: vec![refer],
                    added: child.added,
                    changed: false,
                });
            }
            children.splice(index..=index, child.refs);
            let refs = if children.len() <= INTERNAL_CAPACITY {
                vec![append_internal(file, append_at, level, &children)?]
            } else {
                let right = children.split_off(children.len() / 2);
                vec![
                    append_internal(file, append_at, level, &children)?,
                    append_internal(file, append_at, level, &right)?,
                ]
            };
            Ok(InsertResult {
                refs,
                added: child.added,
                changed: true,
            })
        }
    }
}

fn append_leaf(
    file: &mut File,
    append_at: &mut u64,
    rows: &[(StageAtomicKey, RetainedDecisionTombstone)],
) -> Result<NodeRef, StoreError> {
    if rows.is_empty() || rows.len() > LEAF_CAPACITY {
        return Err(corrupt("leaf cardinality"));
    }
    let mut page = [0u8; PAGE_SIZE];
    page[..4].copy_from_slice(PAGE_MAGIC);
    page[4] = KIND_LEAF;
    page[6..8].copy_from_slice(&(rows.len() as u16).to_be_bytes());
    for (slot, (key, retained)) in rows.iter().enumerate() {
        let at = PAGE_HEADER_LEN + slot * RECORD_LEN;
        page[at..at + RECORD_LEN].copy_from_slice(&encode_record(*key, *retained)?);
    }
    append_page(file, append_at, page, rows.last().unwrap().0, 0)
}

fn append_internal(
    file: &mut File,
    append_at: &mut u64,
    level: u8,
    children: &[NodeRef],
) -> Result<NodeRef, StoreError> {
    if level == 0 || children.is_empty() || children.len() > INTERNAL_CAPACITY {
        return Err(corrupt("internal cardinality/level"));
    }
    if children
        .windows(2)
        .any(|pair| pair[0].max_key >= pair[1].max_key)
        || children.iter().any(|child| child.level + 1 != level)
    {
        return Err(corrupt("internal child order/level"));
    }
    let mut page = [0u8; PAGE_SIZE];
    page[..4].copy_from_slice(PAGE_MAGIC);
    page[4] = KIND_INTERNAL;
    page[5] = level;
    page[6..8].copy_from_slice(&(children.len() as u16).to_be_bytes());
    for (slot, child) in children.iter().enumerate() {
        let at = PAGE_HEADER_LEN + slot * CHILD_LEN;
        page[at..at + 16].copy_from_slice(child.max_key.0.as_bytes());
        page[at + 16..at + 48].copy_from_slice(child.max_key.1.as_bytes());
        page[at + 48..at + 56].copy_from_slice(&child.offset.to_be_bytes());
        page[at + 56..at + 88].copy_from_slice(&child.hash);
    }
    append_page(
        file,
        append_at,
        page,
        children.last().unwrap().max_key,
        level,
    )
}

fn append_page(
    file: &mut File,
    append_at: &mut u64,
    mut page: [u8; PAGE_SIZE],
    max_key: StageAtomicKey,
    level: u8,
) -> Result<NodeRef, StoreError> {
    if (*append_at - HEADER_LEN as u64) % PAGE_SIZE as u64 != 0 {
        return Err(corrupt("append alignment"));
    }
    let hash = page_hash(&page[..PAGE_CONTENT_LEN]);
    page[PAGE_CONTENT_LEN..].copy_from_slice(&hash);
    let offset = *append_at;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&page)?;
    *append_at = append_at
        .checked_add(PAGE_SIZE as u64)
        .ok_or_else(|| StoreError::AtomicStage("tombstone index length overflow".into()))?;
    Ok(NodeRef {
        max_key,
        offset,
        hash,
        level,
    })
}

fn root_ref(file: &mut File, meta: TombstoneIndexMeta) -> Result<NodeRef, StoreError> {
    let offset = meta.file_len - PAGE_SIZE as u64;
    let node = read_raw_node(file, meta.file_len, offset, meta.root)?;
    Ok(NodeRef {
        max_key: node_max_key(&node)?,
        offset,
        hash: meta.root,
        level: node_level(&node),
    })
}

fn read_node(file: &mut File, readable_len: u64, refer: NodeRef) -> Result<Node, StoreError> {
    let node = read_raw_node(file, readable_len, refer.offset, refer.hash)?;
    if node_level(&node) != refer.level || node_max_key(&node)? != refer.max_key {
        return Err(corrupt("child descriptor mismatch"));
    }
    Ok(node)
}

fn read_raw_node(
    file: &mut File,
    readable_len: u64,
    offset: u64,
    expected_hash: [u8; 32],
) -> Result<Node, StoreError> {
    if offset < HEADER_LEN as u64
        || (offset - HEADER_LEN as u64) % PAGE_SIZE as u64 != 0
        || offset.saturating_add(PAGE_SIZE as u64) > readable_len
    {
        return Err(corrupt("page extent/alignment"));
    }
    let mut page = [0u8; PAGE_SIZE];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut page)?;
    let actual = page_hash(&page[..PAGE_CONTENT_LEN]);
    if actual != expected_hash || page[PAGE_CONTENT_LEN..] != actual {
        return Err(corrupt("page authentication"));
    }
    if &page[..4] != PAGE_MAGIC || page[8..16] != [0; 8] {
        return Err(corrupt("page header"));
    }
    let kind = page[4];
    let level = page[5];
    let count = u16::from_be_bytes(page[6..8].try_into().unwrap()) as usize;
    match kind {
        KIND_LEAF if level == 0 && (1..=LEAF_CAPACITY).contains(&count) => {
            let mut rows = Vec::with_capacity(count);
            for slot in 0..count {
                let at = PAGE_HEADER_LEN + slot * RECORD_LEN;
                rows.push(decode_record(&page[at..at + RECORD_LEN])?);
            }
            let used = PAGE_HEADER_LEN + count * RECORD_LEN;
            if rows.windows(2).any(|pair| pair[0].0 >= pair[1].0)
                || page[used..PAGE_CONTENT_LEN].iter().any(|byte| *byte != 0)
            {
                return Err(corrupt("leaf order/padding"));
            }
            Ok(Node::Leaf(rows))
        }
        KIND_INTERNAL if level > 0 && (1..=INTERNAL_CAPACITY).contains(&count) => {
            let mut children = Vec::with_capacity(count);
            for slot in 0..count {
                let at = PAGE_HEADER_LEN + slot * CHILD_LEN;
                let heap = HeapId::from_bytes(page[at..at + 16].try_into().unwrap())
                    .map_err(|_| corrupt("child Heap ID"))?;
                let atomic = AtomicId::from_bytes(page[at + 16..at + 48].try_into().unwrap())
                    .map_err(|_| corrupt("child Atomic ID"))?;
                let child_offset = u64::from_be_bytes(page[at + 48..at + 56].try_into().unwrap());
                let hash = page[at + 56..at + 88].try_into().unwrap();
                if child_offset >= offset
                    || child_offset.saturating_add(PAGE_SIZE as u64) > readable_len
                {
                    return Err(corrupt("child extent/order"));
                }
                children.push(NodeRef {
                    max_key: (heap, atomic),
                    offset: child_offset,
                    hash,
                    level: level - 1,
                });
            }
            let used = PAGE_HEADER_LEN + count * CHILD_LEN;
            if children
                .windows(2)
                .any(|pair| pair[0].max_key >= pair[1].max_key)
                || page[used..PAGE_CONTENT_LEN].iter().any(|byte| *byte != 0)
            {
                return Err(corrupt("internal order/padding"));
            }
            Ok(Node::Internal { level, children })
        }
        _ => Err(corrupt("page kind/cardinality/level")),
    }
}

fn node_level(node: &Node) -> u8 {
    match node {
        Node::Leaf(_) => 0,
        Node::Internal { level, .. } => *level,
    }
}

fn node_max_key(node: &Node) -> Result<StageAtomicKey, StoreError> {
    match node {
        Node::Leaf(rows) => rows.last().map(|row| row.0),
        Node::Internal { children, .. } => children.last().map(|child| child.max_key),
    }
    .ok_or_else(|| corrupt("empty node"))
}

fn write_header(file: &mut File) -> Result<(), StoreError> {
    let mut header = [0u8; HEADER_LEN];
    header[..8].copy_from_slice(MAGIC);
    header[8] = VERSION;
    header[12..16].copy_from_slice(&(PAGE_SIZE as u32).to_be_bytes());
    header[16..20].copy_from_slice(&(LEAF_CAPACITY as u32).to_be_bytes());
    header[20..24].copy_from_slice(&(INTERNAL_CAPACITY as u32).to_be_bytes());
    let digest = header_hash(&header[..32]);
    header[32..64].copy_from_slice(&digest);
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    Ok(())
}

fn validate_header(file: &mut File) -> Result<(), StoreError> {
    let mut header = [0u8; HEADER_LEN];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;
    if &header[..8] != MAGIC
        || header[8] != VERSION
        || header[9..12] != [0; 3]
        || u32::from_be_bytes(header[12..16].try_into().unwrap()) != PAGE_SIZE as u32
        || u32::from_be_bytes(header[16..20].try_into().unwrap()) != LEAF_CAPACITY as u32
        || u32::from_be_bytes(header[20..24].try_into().unwrap()) != INTERNAL_CAPACITY as u32
        || header[24..32] != [0; 8]
        || header_hash(&header[..32]) != header[32..64]
    {
        return Err(corrupt("header"));
    }
    Ok(())
}

fn validate_meta_extent(file: &File, meta: TombstoneIndexMeta) -> Result<(), StoreError> {
    if meta.record_count == 0
        || meta.file_len < HEADER_LEN as u64 + PAGE_SIZE as u64
        || (meta.file_len - HEADER_LEN as u64) % PAGE_SIZE as u64 != 0
        || file.metadata()?.len() < meta.file_len
    {
        return Err(corrupt("committed extent"));
    }
    Ok(())
}

fn encode_record(
    key: StageAtomicKey,
    retained: RetainedDecisionTombstone,
) -> Result<[u8; RECORD_LEN], StoreError> {
    retained
        .tombstone
        .validate()
        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
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
    let tombstone = DecisionTombstone {
        atomic_id,
        content_root,
        decision,
        commit_position: (raw_position != 0).then_some(raw_position),
        decision_hash: bytes[100..132].try_into().unwrap(),
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
fn page_hash(bytes: &[u8]) -> [u8; 32] {
    domain_hash(PAGE_DOMAIN, &[bytes])
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
        let mut b = [0u8; 16];
        b[0] = first;
        HeapId::from_bytes(b).unwrap()
    }
    fn atomic(n: u32) -> AtomicId {
        let mut b = [0u8; 32];
        b[..4].copy_from_slice(&n.to_be_bytes());
        AtomicId::from_bytes(b).unwrap()
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
                decision_hash: *blake3::hash(&n.to_be_bytes()).as_bytes(),
                abort_reason: Some(AtomicAbortReason::RecoveryAbort),
            },
        }
    }
    fn records(
        heap: HeapId,
        from: u32,
        through: u32,
    ) -> BTreeMap<StageAtomicKey, RetainedDecisionTombstone> {
        (from..=through)
            .map(|n| ((heap, atomic(n)), retained(n)))
            .collect()
    }

    #[test]
    fn bulk_tree_and_incremental_splits_lookup_every_key() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let heap = heap(7);
        let meta = refresh_index(&paths, None, &records(heap, 1, 4_000))
            .unwrap()
            .unwrap();
        let before = meta.file_len;
        let meta = refresh_index(&paths, Some(meta), &records(heap, 4_001, 4_200))
            .unwrap()
            .unwrap();
        assert_eq!(meta.record_count, 4_200);
        for n in 1..=4_200 {
            assert_eq!(
                lookup(&paths, meta, (heap, atomic(n))).unwrap(),
                Some(retained(n))
            );
        }
        assert!(meta.file_len - before < 16 * 1024 * 1024);
    }

    #[test]
    fn one_insert_rewrites_only_one_tree_path_and_old_root_survives() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let heap = heap(7);
        let prior = refresh_index(&paths, None, &records(heap, 1, 10_000))
            .unwrap()
            .unwrap();
        let meta = refresh_index(&paths, Some(prior), &records(heap, 10_001, 10_001))
            .unwrap()
            .unwrap();
        assert!(meta.file_len - prior.file_len <= 64 * 1024);
        assert_eq!(
            lookup(&paths, prior, (heap, atomic(10_000))).unwrap(),
            Some(retained(10_000))
        );
        assert_eq!(
            lookup(&paths, meta, (heap, atomic(10_001))).unwrap(),
            Some(retained(10_001))
        );
    }

    #[test]
    fn crash_suffix_is_ignored_then_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let heap = heap(7);
        let meta = refresh_index(&paths, None, &records(heap, 1, 100))
            .unwrap()
            .unwrap();
        let path = tombstone_index_path(&paths);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0xAA; PAGE_SIZE]).unwrap();
        f.sync_all().unwrap();
        assert_eq!(
            lookup(&paths, meta, (heap, atomic(100))).unwrap(),
            Some(retained(100))
        );
        let next = refresh_index(&paths, Some(meta), &records(heap, 101, 101))
            .unwrap()
            .unwrap();
        assert_eq!(fs::metadata(path).unwrap().len(), next.file_len);
    }

    #[test]
    fn corruption_and_conflict_fail_closed_while_earlier_time_wins() {
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
        let mut conflict = earlier;
        conflict.tombstone.decision_hash[0] ^= 1;
        assert!(refresh_index(&paths, Some(meta), &BTreeMap::from([(key, conflict)])).is_err());
        let path = tombstone_index_path(&paths);
        let mut bytes = fs::read(&path).unwrap();
        bytes[meta.file_len as usize - PAGE_SIZE + 20] ^= 0x80;
        fs::write(path, bytes).unwrap();
        assert!(lookup(&paths, meta, key).is_err());
    }

    #[test]
    #[ignore = "ATM-4A million-identity qualification"]
    fn million_identity_bulk_build_and_point_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let heap = heap(7);
        let meta = refresh_index(&paths, None, &records(heap, 1, 1_000_000))
            .unwrap()
            .unwrap();
        assert!(meta.file_len < 160 * 1024 * 1024);
        for n in [1, 500_000, 1_000_000] {
            assert_eq!(
                lookup(&paths, meta, (heap, atomic(n))).unwrap(),
                Some(retained(n))
            );
        }
    }
}
