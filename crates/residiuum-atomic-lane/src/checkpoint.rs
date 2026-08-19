//! Durable recovery checkpoint: log tails plus identity summaries (CR-ATMR3-004).

use crate::error::LaneError;
use crate::io_fail::IoSite;
use crate::limits::RecoveryLimits;
use crate::persist;
use residiuum_atomics::{AtomicId, ContentRoot, MemberPhase, PlacementManifest, StagingHeap};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const MAGIC: &[u8] = b"R2CKP1";
const VERSION_V1: u8 = 1;
const VERSION_V2: u8 = 2;
const VERSION: u8 = 3;
const DOMAIN: &[u8] = b"RESIDIUUM-ATOMIC-LANE-CKP-V3";
const DOMAIN_CHAIN: &[u8] = b"RESIDIUUM-ATOMIC-LANE-FRN-V1";
const DOMAIN_BLOCK: &[u8] = b"RESIDIUUM-ATOMIC-LANE-BLK-V1";
/// Fixed leaf size for the incremental block frontier (CR-ATMR5-008).
pub const FRONTIER_BLOCK: u64 = 64 * 1024;

/// One reconstructed Atomic named by a checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointAtomic {
    /// Issued identity.
    pub atomic_id: AtomicId,
    /// Plan content root.
    pub content_root: ContentRoot,
    /// Frozen member-manifest root.
    pub manifest_root: [u8; 32],
    /// Intended member count.
    pub member_count: u32,
    /// Whether the first stable boundary was recorded.
    pub sealed: bool,
}

/// Incremental authenticated marks for one log prefix (CR-ATMR5-008).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PrefixMarks {
    /// First 32 bytes of the covered prefix (zero-padded if shorter).
    pub head: [u8; 32],
    /// Last 32 bytes of the covered prefix (zero-padded if shorter).
    pub tail: [u8; 32],
    /// BLAKE3 of each complete `FRONTIER_BLOCK` leaf, in order.
    pub block_hashes: Vec<[u8; 32]>,
    /// Bytes after the last complete block.
    pub leftover_len: u32,
    /// Hash of the leftover (zero if empty).
    pub leftover_hash: [u8; 32],
}

/// Offsets and identity summaries from the last successful durable prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryCheckpoint {
    /// Coordinator bytes covered by this checkpoint.
    pub coordinator_offset: u64,
    /// Per-shard covered lengths.
    pub shard_offsets: Vec<u64>,
    /// Incremental chain digest of the coordinator prefix.
    pub coordinator_hash: [u8; 32],
    /// Incremental chain digest of each shard prefix.
    pub shard_hashes: Vec<[u8; 32]>,
    /// Coordinator head/tail/block frontier.
    pub coordinator_marks: PrefixMarks,
    /// Per-shard head/tail/block frontiers.
    pub shard_marks: Vec<PrefixMarks>,
    /// Reconstructed Atomics in coordinator order.
    pub atomics: Vec<CheckpointAtomic>,
}

/// How `checkpoint` was classified (CR-ATMR4-002).
#[derive(Debug)]
pub enum CheckpointLoad {
    /// No checkpoint file.
    Missing,
    /// v1 or unrecognized image: rebuild from logs; do not apply facts.
    Legacy,
    /// Authenticated v3 body.
    Ready(RecoveryCheckpoint),
}

impl RecoveryCheckpoint {
    /// Snapshot the live kernel and current acknowledged log lengths.
    pub fn from_heap(
        heap: &StagingHeap,
        coordinator_offset: u64,
        shard_offsets: Vec<u64>,
        coordinator_hash: [u8; 32],
        shard_hashes: Vec<[u8; 32]>,
        coordinator_marks: PrefixMarks,
        shard_marks: Vec<PrefixMarks>,
    ) -> Self {
        let mut atomics = Vec::new();
        for rec in heap.coordinator().records() {
            let Some(placement) = heap.placement(rec.atomic_id) else {
                continue;
            };
            let sealed = heap
                .lifecycle(rec.atomic_id)
                .is_some_and(|l| l.members == MemberPhase::DurableInvisible);
            atomics.push(CheckpointAtomic {
                atomic_id: rec.atomic_id,
                content_root: rec.content_root,
                manifest_root: placement.member_manifest_root(),
                member_count: placement.entries().len() as u32,
                sealed,
            });
        }
        Self {
            coordinator_offset,
            shard_offsets,
            coordinator_hash,
            shard_hashes,
            coordinator_marks,
            shard_marks,
            atomics,
        }
    }
}

/// Encode a checkpoint to exclusive sidecar bytes.
pub fn encode_checkpoint(ck: &RecoveryCheckpoint) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&ck.coordinator_offset.to_be_bytes());
    buf.extend_from_slice(&(ck.shard_offsets.len() as u32).to_be_bytes());
    for off in &ck.shard_offsets {
        buf.extend_from_slice(&off.to_be_bytes());
    }
    buf.extend_from_slice(&ck.coordinator_hash);
    for hash in &ck.shard_hashes {
        buf.extend_from_slice(hash);
    }
    encode_marks(&mut buf, &ck.coordinator_marks);
    for marks in &ck.shard_marks {
        encode_marks(&mut buf, marks);
    }
    buf.extend_from_slice(&(ck.atomics.len() as u32).to_be_bytes());
    for a in &ck.atomics {
        buf.extend_from_slice(a.atomic_id.as_bytes());
        buf.extend_from_slice(a.content_root.as_bytes());
        buf.extend_from_slice(&a.manifest_root);
        buf.extend_from_slice(&a.member_count.to_be_bytes());
        buf.push(u8::from(a.sealed));
    }
    let digest = checkpoint_digest(&buf);
    buf.extend_from_slice(&digest);
    buf
}

fn encode_marks(buf: &mut Vec<u8>, marks: &PrefixMarks) {
    buf.extend_from_slice(&marks.head);
    buf.extend_from_slice(&marks.tail);
    buf.extend_from_slice(&(marks.block_hashes.len() as u32).to_be_bytes());
    for hash in &marks.block_hashes {
        buf.extend_from_slice(hash);
    }
    buf.extend_from_slice(&marks.leftover_len.to_be_bytes());
    buf.extend_from_slice(&marks.leftover_hash);
}

fn decode_marks(bytes: &[u8], off: &mut usize, body_len: usize) -> Result<PrefixMarks, LaneError> {
    if *off + 64 + 4 > body_len {
        return Err(LaneError::Corrupt("checkpoint frontier marks"));
    }
    let mut head = [0u8; 32];
    let mut tail = [0u8; 32];
    head.copy_from_slice(&bytes[*off..*off + 32]);
    *off += 32;
    tail.copy_from_slice(&bytes[*off..*off + 32]);
    *off += 32;
    let n = u32::from_be_bytes(bytes[*off..*off + 4].try_into().unwrap());
    *off += 4;
    if *off + n as usize * 32 + 4 + 32 > body_len {
        return Err(LaneError::Corrupt("checkpoint frontier blocks"));
    }
    let mut block_hashes = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes[*off..*off + 32]);
        block_hashes.push(hash);
        *off += 32;
    }
    let leftover_len = u32::from_be_bytes(bytes[*off..*off + 4].try_into().unwrap());
    *off += 4;
    let mut leftover_hash = [0u8; 32];
    leftover_hash.copy_from_slice(&bytes[*off..*off + 32]);
    *off += 32;
    Ok(PrefixMarks {
        head,
        tail,
        block_hashes,
        leftover_len,
        leftover_hash,
    })
}

fn checkpoint_digest(body: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    hasher.update(body);
    *hasher.finalize().as_bytes()
}

/// BLAKE3 of `DOMAIN || file[0..len]`. Missing file is empty prefix only when `len == 0`.
#[allow(dead_code)]
pub fn prefix_digest(path: &Path, len: u64) -> Result<[u8; 32], LaneError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    if len == 0 {
        return Ok(*hasher.finalize().as_bytes());
    }
    if !path.exists() {
        return Err(LaneError::Corrupt("checkpoint prefix missing log"));
    }
    let file_len = fs::metadata(path)?.len();
    if file_len < len {
        return Err(LaneError::Corrupt("checkpoint prefix beyond log"));
    }
    let mut file = fs::File::open(path)?;
    let mut left = len;
    let mut buf = [0u8; 8192];
    while left > 0 {
        let n = std::cmp::min(left, buf.len() as u64) as usize;
        use std::io::Read;
        file.read_exact(&mut buf[..n])?;
        hasher.update(&buf[..n]);
        left -= n as u64;
    }
    Ok(*hasher.finalize().as_bytes())
}

fn chain_empty() -> [u8; 32] {
    *blake3::Hasher::new()
        .update(DOMAIN_CHAIN)
        .finalize()
        .as_bytes()
}

fn chain_extend(prev: [u8; 32], prev_off: u64, new_off: u64, tail: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_CHAIN);
    hasher.update(&prev);
    hasher.update(&prev_off.to_be_bytes());
    hasher.update(&new_off.to_be_bytes());
    hasher.update(tail);
    *hasher.finalize().as_bytes()
}

fn block_hash(index: u64, bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_BLOCK);
    hasher.update(&index.to_be_bytes());
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn leftover_hash(bytes: &[u8]) -> [u8; 32] {
    if bytes.is_empty() {
        return [0u8; 32];
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_BLOCK);
    hasher.update(b"tail");
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn read_at(file: &mut File, off: u64, buf: &mut [u8]) -> Result<(), LaneError> {
    file.seek(SeekFrom::Start(off))?;
    file.read_exact(buf)?;
    Ok(())
}

/// Extend a log frontier by hashing only `[prev_offset, new_len)`.
///
/// Returns the new marks, chain digest, and bytes actually read.
pub fn extend_log_frontier(
    path: &Path,
    prev: Option<(&[u8; 32], u64, &PrefixMarks)>,
    new_len: u64,
) -> Result<(PrefixMarks, [u8; 32], u64), LaneError> {
    if new_len == 0 {
        return Ok((PrefixMarks::default(), chain_empty(), 0));
    }
    if let Some((digest, prev_off, marks)) = prev {
        if prev_off == new_len {
            return Ok((marks.clone(), *digest, 0));
        }
        if prev_off > new_len {
            return Err(LaneError::Corrupt("checkpoint frontier regression"));
        }
    }
    if !path.exists() {
        return Err(LaneError::Corrupt("checkpoint prefix missing log"));
    }
    let file_len = fs::metadata(path)?.len();
    if file_len < new_len {
        return Err(LaneError::Corrupt("checkpoint prefix beyond log"));
    }
    let start = prev.map(|(_, off, _)| off).unwrap_or(0);
    let mut file = File::open(path)?;
    let mut tail = vec![0u8; (new_len - start) as usize];
    if !tail.is_empty() {
        read_at(&mut file, start, &mut tail)?;
    }
    let bytes_read = tail.len() as u64;
    let digest = match prev {
        Some((prev_digest, prev_off, _)) => chain_extend(*prev_digest, prev_off, new_len, &tail),
        None => chain_extend(chain_empty(), 0, new_len, &tail),
    };
    let mut marks = match prev {
        Some((_, prev_off, prev_marks)) if prev_off > 0 => prev_marks.clone(),
        _ => PrefixMarks::default(),
    };
    if start == 0 {
        let n = std::cmp::min(32, tail.len());
        marks.head[..n].copy_from_slice(&tail[..n]);
    }
    let mut leftover = Vec::new();
    let mut leftover_read = 0u64;
    if marks.leftover_len > 0 {
        let leftover_start = start.saturating_sub(u64::from(marks.leftover_len));
        leftover.resize(marks.leftover_len as usize, 0);
        if leftover_start < start {
            read_at(&mut file, leftover_start, &mut leftover)?;
            leftover_read = u64::from(marks.leftover_len);
        }
    }
    leftover.extend_from_slice(&tail);
    marks.leftover_len = 0;
    marks.leftover_hash = [0u8; 32];
    let mut idx = marks.block_hashes.len() as u64;
    let mut rest = leftover.as_slice();
    while rest.len() as u64 >= FRONTIER_BLOCK {
        let (block, next) = rest.split_at(FRONTIER_BLOCK as usize);
        marks.block_hashes.push(block_hash(idx, block));
        idx += 1;
        rest = next;
    }
    if !rest.is_empty() {
        marks.leftover_len = rest.len() as u32;
        marks.leftover_hash = leftover_hash(rest);
    }
    let tail_n = std::cmp::min(32, new_len as usize);
    let mut tail_bytes = vec![0u8; tail_n];
    read_at(&mut file, new_len - tail_n as u64, &mut tail_bytes)?;
    marks.tail = [0u8; 32];
    marks.tail[..tail_n].copy_from_slice(&tail_bytes);
    // Head/tail reads are already in `tail` when start==0; extra tail read is
    // at most 32 bytes and is charged.
    let extra = if start == 0 { 0 } else { tail_n as u64 };
    Ok((
        marks,
        digest,
        bytes_read
            .saturating_add(extra)
            .saturating_add(leftover_read),
    ))
}

/// Cheap length/head/tail check. Not the production covered-prefix verifier.
#[allow(dead_code)]
pub fn verify_prefix_marks(
    path: &Path,
    offset: u64,
    marks: &PrefixMarks,
) -> Result<u64, LaneError> {
    if offset == 0 {
        return Ok(0);
    }
    if !path.exists() {
        return Err(LaneError::Corrupt("checkpoint prefix missing log"));
    }
    let file_len = fs::metadata(path)?.len();
    if file_len < offset {
        return Err(LaneError::Corrupt("checkpoint prefix beyond log"));
    }
    let mut file = File::open(path)?;
    let head_n = std::cmp::min(32, offset as usize);
    let mut head = [0u8; 32];
    read_at(&mut file, 0, &mut head[..head_n])?;
    if head != marks.head {
        return Err(LaneError::Corrupt("checkpoint prefix head"));
    }
    let tail_n = std::cmp::min(32, offset as usize);
    let mut tail = [0u8; 32];
    read_at(&mut file, offset - tail_n as u64, &mut tail[..tail_n])?;
    if tail != marks.tail {
        return Err(LaneError::Corrupt("checkpoint prefix tail"));
    }
    Ok((head_n + tail_n) as u64)
}

/// Re-hash stored leaves and leftover. Detects any changed covered byte.
///
/// Production recovery must use this, not [`verify_prefix_marks`] (CR-ATMR6-001).
pub fn verify_prefix_blocks(
    path: &Path,
    offset: u64,
    marks: &PrefixMarks,
) -> Result<u64, LaneError> {
    if offset == 0 {
        return Ok(0);
    }
    if !path.exists() {
        return Err(LaneError::Corrupt("checkpoint prefix missing log"));
    }
    let file_len = fs::metadata(path)?.len();
    if file_len < offset {
        return Err(LaneError::Corrupt("checkpoint prefix beyond log"));
    }
    let mut file = File::open(path)?;
    let mut read = 0u64;
    let mut buf = vec![0u8; FRONTIER_BLOCK as usize];
    for (i, expect) in marks.block_hashes.iter().enumerate() {
        read_at(&mut file, i as u64 * FRONTIER_BLOCK, &mut buf)?;
        read = read.saturating_add(FRONTIER_BLOCK);
        if block_hash(i as u64, &buf) != *expect {
            return Err(LaneError::Corrupt("checkpoint frontier block"));
        }
    }
    if marks.leftover_len > 0 {
        let mut left = vec![0u8; marks.leftover_len as usize];
        let off = marks.block_hashes.len() as u64 * FRONTIER_BLOCK;
        read_at(&mut file, off, &mut left)?;
        read = read.saturating_add(u64::from(marks.leftover_len));
        if leftover_hash(&left) != marks.leftover_hash {
            return Err(LaneError::Corrupt("checkpoint frontier leftover"));
        }
    }
    let covered = marks.block_hashes.len() as u64 * FRONTIER_BLOCK + u64::from(marks.leftover_len);
    if covered != offset {
        return Err(LaneError::Corrupt("checkpoint frontier coverage"));
    }
    Ok(read)
}

/// Decode a checkpoint. Any structural problem is corruption.
pub fn decode_checkpoint(
    bytes: &[u8],
    limits: RecoveryLimits,
) -> Result<RecoveryCheckpoint, LaneError> {
    let header = MAGIC.len() + 1 + 8 + 4;
    if bytes.len() < header + 32 || !bytes.starts_with(MAGIC) || bytes[MAGIC.len()] != VERSION {
        return Err(LaneError::Corrupt("checkpoint header"));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if checkpoint_digest(body) != digest {
        return Err(LaneError::Corrupt("checkpoint checksum"));
    }
    let mut off = MAGIC.len() + 1;
    let coordinator_offset = u64::from_be_bytes(bytes[off..off + 8].try_into().unwrap());
    off += 8;
    let shard_n = u32::from_be_bytes(bytes[off..off + 4].try_into().unwrap());
    off += 4;
    if shard_n > limits.max_atomics {
        return Err(LaneError::Incomplete {
            what: "checkpoint shards",
            observed: u64::from(shard_n),
            limit: u64::from(limits.max_atomics),
        });
    }
    let mut shard_offsets = Vec::new();
    for _ in 0..shard_n {
        if off + 8 > bytes.len() {
            return Err(LaneError::Corrupt("checkpoint shard offset"));
        }
        shard_offsets.push(u64::from_be_bytes(bytes[off..off + 8].try_into().unwrap()));
        off += 8;
    }
    if off + 32 + shard_offsets.len() * 32 + 4 > body.len() {
        return Err(LaneError::Corrupt("checkpoint prefix hashes"));
    }
    let mut coordinator_hash = [0u8; 32];
    coordinator_hash.copy_from_slice(&bytes[off..off + 32]);
    off += 32;
    let mut shard_hashes = Vec::with_capacity(shard_offsets.len());
    for _ in 0..shard_offsets.len() {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes[off..off + 32]);
        shard_hashes.push(hash);
        off += 32;
    }
    let coordinator_marks = decode_marks(bytes, &mut off, body.len())?;
    let mut shard_marks = Vec::with_capacity(shard_offsets.len());
    for _ in 0..shard_offsets.len() {
        shard_marks.push(decode_marks(bytes, &mut off, body.len())?);
    }
    if off + 4 > body.len() {
        return Err(LaneError::Corrupt("checkpoint atomic count"));
    }
    let n = u32::from_be_bytes(bytes[off..off + 4].try_into().unwrap());
    off += 4;
    if n > limits.max_atomics {
        return Err(LaneError::Incomplete {
            what: "checkpoint atomics",
            observed: u64::from(n),
            limit: u64::from(limits.max_atomics),
        });
    }
    let mut atomics = Vec::new();
    for _ in 0..n {
        if off + 32 + 32 + 32 + 4 + 1 > body.len() {
            return Err(LaneError::Corrupt("checkpoint atomic truncated"));
        }
        let atomic_id = AtomicId::from_bytes(bytes[off..off + 32].try_into().unwrap())?;
        off += 32;
        let content_root = ContentRoot::from_bytes(bytes[off..off + 32].try_into().unwrap())?;
        off += 32;
        let mut manifest_root = [0u8; 32];
        manifest_root.copy_from_slice(&bytes[off..off + 32]);
        off += 32;
        let member_count = u32::from_be_bytes(bytes[off..off + 4].try_into().unwrap());
        off += 4;
        let sealed = bytes[off] != 0;
        off += 1;
        atomics.push(CheckpointAtomic {
            atomic_id,
            content_root,
            manifest_root,
            member_count,
            sealed,
        });
    }
    if off != body.len() {
        return Err(LaneError::Corrupt("checkpoint trailing bytes"));
    }
    if shard_hashes.len() != shard_offsets.len() {
        return Err(LaneError::Corrupt("checkpoint hash count"));
    }
    if shard_marks.len() != shard_offsets.len() {
        return Err(LaneError::Corrupt("checkpoint mark count"));
    }
    Ok(RecoveryCheckpoint {
        coordinator_offset,
        shard_offsets,
        coordinator_hash,
        shard_hashes,
        coordinator_marks,
        shard_marks,
        atomics,
    })
}

/// Load `checkpoint` if present. Missing is `Ok(None)`. Corrupt is `Err`.
pub fn load_checkpoint(root: &Path, limits: RecoveryLimits) -> Result<CheckpointLoad, LaneError> {
    let path = checkpoint_path(root);
    if !path.exists() {
        return Ok(CheckpointLoad::Missing);
    }
    let len = fs::metadata(&path)?.len();
    if len > crate::limits::SidecarRole::Checkpoint.max_bytes() {
        return Err(residiuum_atomics::AtomicsError::Refused(
            residiuum_atomics::AtomicRefuseReason::LimitExceeded,
        )
        .into());
    }
    let bytes = fs::read(path)?;
    if bytes.len() > MAGIC.len()
        && bytes.starts_with(MAGIC)
        && (bytes[MAGIC.len()] == VERSION_V1 || bytes[MAGIC.len()] == VERSION_V2)
    {
        return Ok(CheckpointLoad::Legacy);
    }
    if bytes.len() < MAGIC.len() + 1 || !bytes.starts_with(MAGIC) || bytes[MAGIC.len()] != VERSION {
        return Ok(CheckpointLoad::Legacy);
    }
    Ok(CheckpointLoad::Ready(decode_checkpoint(&bytes, limits)?))
}

/// Persist `checkpoint` atomically.
pub fn store_checkpoint(root: &Path, ck: &RecoveryCheckpoint) -> Result<(), LaneError> {
    let bytes = encode_checkpoint(ck);
    persist::write_atomic(&checkpoint_path(root), &bytes, IoSite::Checkpoint)
}

pub fn checkpoint_path(root: &Path) -> std::path::PathBuf {
    root.join("checkpoint")
}

/// Confirm a reconstructed placement still matches the checkpoint summary.
pub fn placement_matches(ck: &CheckpointAtomic, placement: &PlacementManifest) -> bool {
    placement.atomic_id() == ck.atomic_id
        && placement.content_root() == ck.content_root
        && placement.member_manifest_root() == ck.manifest_root
        && placement.entries().len() as u32 == ck.member_count
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_log(path: &std::path::Path, n: usize, fill: u8) {
        fs::write(path, vec![fill; n]).unwrap();
    }

    #[test]
    fn second_extend_reads_only_the_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        write_log(&path, FRONTIER_BLOCK as usize * 3, 0x11);
        let (marks1, digest1, first) =
            extend_log_frontier(&path, None, FRONTIER_BLOCK * 3).unwrap();
        assert!(first >= FRONTIER_BLOCK * 3);
        write_log(&path, FRONTIER_BLOCK as usize * 3 + 100, 0x11);
        let (_, _, second) = extend_log_frontier(
            &path,
            Some((&digest1, FRONTIER_BLOCK * 3, &marks1)),
            FRONTIER_BLOCK * 3 + 100,
        )
        .unwrap();
        assert!(
            second < FRONTIER_BLOCK,
            "incremental extend must not reread history, read {second}"
        );
        assert!(second >= 100);
    }

    #[test]
    fn head_and_tail_mutation_fail_cheap_verify() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        write_log(&path, 1000, 0x22);
        let (marks, _, _) = extend_log_frontier(&path, None, 1000).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 1;
        fs::write(&path, &bytes).unwrap();
        assert!(verify_prefix_marks(&path, 1000, &marks).is_err());
        bytes[0] ^= 1;
        bytes[999] ^= 1;
        fs::write(&path, &bytes).unwrap();
        assert!(verify_prefix_marks(&path, 1000, &marks).is_err());
    }

    #[test]
    fn middle_byte_fails_block_frontier() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let n = FRONTIER_BLOCK as usize + 50;
        write_log(&path, n, 0x33);
        let (marks, _, _) = extend_log_frontier(&path, None, n as u64).unwrap();
        verify_prefix_blocks(&path, n as u64, &marks).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[FRONTIER_BLOCK as usize / 2] ^= 1;
        fs::write(&path, &bytes).unwrap();
        match verify_prefix_blocks(&path, n as u64, &marks) {
            Err(LaneError::Corrupt("checkpoint frontier block")) => {}
            other => panic!("expected block mismatch, got {other:?}"),
        }
    }
}
