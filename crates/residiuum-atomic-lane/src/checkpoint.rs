//! Durable recovery checkpoint: log tails plus identity summaries (CR-ATMR3-004).

use crate::error::LaneError;
use crate::limits::RecoveryLimits;
use residiuum_atomics::{AtomicId, ContentRoot, MemberPhase, PlacementManifest, StagingHeap};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

const MAGIC: &[u8] = b"R2CKP1";
const VERSION: u8 = 1;

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

/// Offsets and identity summaries from the last successful durable prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryCheckpoint {
    /// Coordinator bytes covered by this checkpoint.
    pub coordinator_offset: u64,
    /// Per-shard covered lengths.
    pub shard_offsets: Vec<u64>,
    /// Reconstructed Atomics in coordinator order.
    pub atomics: Vec<CheckpointAtomic>,
}

impl RecoveryCheckpoint {
    /// Snapshot the live kernel and current acknowledged log lengths.
    pub fn from_heap(heap: &StagingHeap, coordinator_offset: u64, shard_offsets: Vec<u64>) -> Self {
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
    buf.extend_from_slice(&(ck.atomics.len() as u32).to_be_bytes());
    for a in &ck.atomics {
        buf.extend_from_slice(a.atomic_id.as_bytes());
        buf.extend_from_slice(a.content_root.as_bytes());
        buf.extend_from_slice(&a.manifest_root);
        buf.extend_from_slice(&a.member_count.to_be_bytes());
        buf.push(u8::from(a.sealed));
    }
    buf
}

/// Decode a checkpoint. Any structural problem is corruption.
pub fn decode_checkpoint(
    bytes: &[u8],
    limits: RecoveryLimits,
) -> Result<RecoveryCheckpoint, LaneError> {
    let header = MAGIC.len() + 1 + 8 + 4;
    if bytes.len() < header || !bytes.starts_with(MAGIC) || bytes[MAGIC.len()] != VERSION {
        return Err(LaneError::Corrupt("checkpoint header"));
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
    if off + 4 > bytes.len() {
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
        if off + 32 + 32 + 32 + 4 + 1 > bytes.len() {
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
    if off != bytes.len() {
        return Err(LaneError::Corrupt("checkpoint trailing bytes"));
    }
    Ok(RecoveryCheckpoint {
        coordinator_offset,
        shard_offsets,
        atomics,
    })
}

/// Load `checkpoint` if present. Missing is `Ok(None)`. Corrupt is `Err`.
pub fn load_checkpoint(
    root: &Path,
    limits: RecoveryLimits,
) -> Result<Option<RecoveryCheckpoint>, LaneError> {
    let path = checkpoint_path(root);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    Ok(Some(decode_checkpoint(&bytes, limits)?))
}

/// Persist `checkpoint` atomically.
pub fn store_checkpoint(root: &Path, ck: &RecoveryCheckpoint) -> Result<(), LaneError> {
    let bytes = encode_checkpoint(ck);
    let path = checkpoint_path(root);
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
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
