//! File-backed coordinator / member lane.

use crate::error::LaneError;
use crate::recover::{build_lane_prepare, replay_members, replay_prepares, replay_seals, seal_for};
use crate::seal::encode_seal;
use residiuum_atomics::{
    encode_member, encode_prepare, AtomicId, AtomicMember, AtomicRefuseReason, AtomicsError,
    CanonicalKey, CollectionId, ContentRoot, CoordinatorSeq, HeapId, OrdinaryCell,
    PlacementManifest, StagingFailpoint, StagingHeap,
};
use residiuum_format::{
    encode_atomic_frame, encode_atomic_member_envelope, encode_atomic_prepare_envelope, FrameKind,
};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Durable per-Heap staging lane.
///
/// Layout under `root`:
/// - `heap.id`, `meta`
/// - `coordinator.log` — `BatchPrepare` frames (`sync_all` after each append)
/// - `shard-XXXXXXXX.log` — `ItemEvent` member frames
/// - `intent/<atomic>` — frozen members, written and synced *before* prepare
/// - `payload/<atomic>-<ord>` — value bytes, synced *before* the member frame
/// - `sealed/<atomic>` — versioned checksummed boundary (Heap, Atomic ID,
///   content root, manifest root, member count)
pub struct DurableLane {
    root: PathBuf,
    heap: StagingHeap,
    armed: Option<StagingFailpoint>,
}

impl DurableLane {
    /// Create an empty lane directory. Fails if `heap.id` already exists.
    pub fn create(
        root: impl AsRef<Path>,
        heap_id: HeapId,
        shard_count: u32,
    ) -> Result<Self, LaneError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let id_path = root.join("heap.id");
        if id_path.exists() {
            return Err(LaneError::Corrupt("heap.id already exists"));
        }
        let heap = StagingHeap::new(heap_id, shard_count)?;
        write_atomic(&id_path, heap_id.as_bytes())?;
        write_atomic(
            &root.join("meta"),
            format!("shard_count={shard_count}\n").as_bytes(),
        )?;
        fs::create_dir_all(root.join("intent"))?;
        fs::create_dir_all(root.join("payload"))?;
        fs::create_dir_all(root.join("sealed"))?;
        File::create(coordinator_path(&root))?;
        for shard in 0..shard_count {
            File::create(shard_path(&root, shard))?;
        }
        sync_dir(&root)?;
        sync_dir(&root.join("intent"))?;
        sync_dir(&root.join("payload"))?;
        sync_dir(&root.join("sealed"))?;
        Ok(Self {
            root,
            heap,
            armed: None,
        })
    }

    /// Reconstruct a lane from a directory image.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, LaneError> {
        let root = root.as_ref().to_path_buf();
        let mut id_bytes = [0u8; 16];
        File::open(root.join("heap.id"))?.read_exact(&mut id_bytes)?;
        let heap_id = HeapId::from_bytes(id_bytes)?;
        let shard_count = parse_shard_count(&fs::read_to_string(root.join("meta"))?)?;
        let mut heap = StagingHeap::new(heap_id, shard_count)?;
        replay_prepares(&root, heap_id, &mut heap)?;
        replay_members(&root, shard_count, &mut heap)?;
        replay_seals(&root, &mut heap)?;
        Ok(Self {
            root,
            heap,
            armed: None,
        })
    }

    /// Drop this process image and reopen the same directory.
    pub fn reopen(self) -> Result<Self, LaneError> {
        let root = self.root.clone();
        drop(self);
        Self::open(root)
    }

    /// Arm one crash prefix. Replaces any previous arm.
    pub fn arm(&mut self, point: StagingFailpoint) {
        self.armed = Some(point);
    }

    /// Clear the armed failpoint.
    pub fn disarm(&mut self) {
        self.armed = None;
    }

    /// Lane root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Borrow the reconstructed / live kernel.
    pub const fn heap(&self) -> &StagingHeap {
        &self.heap
    }

    /// Validate, then persist intent + prepare, then apply the kernel.
    ///
    /// CR-R2-001: kernel reservation happens before any durable path is
    /// created or replaced. A refused prepare leaves the directory image
    /// unchanged. Same ID / same root / same members is a no-write replay.
    pub fn begin_prepare(
        &mut self,
        atomic_id: AtomicId,
        content_root: ContentRoot,
        members: &[AtomicMember],
    ) -> Result<(CoordinatorSeq, PlacementManifest), LaneError> {
        if self.armed == Some(StagingFailpoint::BeforePrepare) {
            return Err(LaneError::Injected(StagingFailpoint::BeforePrepare));
        }
        if let Some(existing) = self.heap.placement(atomic_id) {
            if existing.content_root() == content_root && members_match(existing, members) {
                let seq = self
                    .heap
                    .prepare_seq(atomic_id)
                    .ok_or(LaneError::Corrupt("prepared without sequence"))?;
                return Ok((seq, existing.clone()));
            }
            return Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into());
        }
        let manifest = self
            .heap
            .check_begin_prepare(atomic_id, content_root, members)?;
        persist_intent(&self.root, atomic_id, members)?;
        persist_prepare(&self.root, &manifest)?;
        let out = self.heap.begin_prepare(atomic_id, content_root, members)?;
        if self.armed == Some(StagingFailpoint::AfterPrepare) {
            return Err(LaneError::Injected(StagingFailpoint::AfterPrepare));
        }
        Ok(out)
    }

    /// Validate, then persist payload + member frame, then apply the kernel.
    ///
    /// CR-R2-001: payload and member frames are written only after the kernel
    /// accepts the exact member. Existing payload files are never overwritten
    /// with different bytes.
    pub fn append_staged(
        &mut self,
        member: AtomicMember,
        payload: Vec<u8>,
    ) -> Result<(), LaneError> {
        let ordinal = member.ordinal;
        if let Some(staged) = self
            .heap
            .inspect_staged(member.atomic_id)
            .and_then(|ms| ms.iter().find(|s| s.member.ordinal == ordinal))
        {
            if staged.member == member && staged.payload == payload {
                return Ok(());
            }
            return Err(AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget).into());
        }
        let shard = self.heap.check_append_staged(&member, &payload)?;
        let content_root = self
            .heap
            .placement(member.atomic_id)
            .ok_or(LaneError::Corrupt("append without prepare"))?
            .content_root()
            .to_bytes();
        persist_payload(&self.root, member.atomic_id, ordinal, &payload)?;
        persist_member_frame(
            &self.root,
            self.heap.heap_id(),
            shard.as_u32(),
            &member,
            content_root,
        )?;
        self.heap.append_staged(member, payload)?;
        if self.armed == Some(StagingFailpoint::AfterMember(ordinal)) {
            return Err(LaneError::Injected(StagingFailpoint::AfterMember(ordinal)));
        }
        Ok(())
    }

    /// First stable boundary: kernel seal, then `sync_all` logs + `sealed/<id>`.
    pub fn seal_member_boundary(&mut self, atomic_id: AtomicId) -> Result<(), LaneError> {
        self.heap.seal_member_boundary(atomic_id)?;
        sync_path(&coordinator_path(&self.root))?;
        let shard_count = shard_count_from_layout(&self.root)?;
        for shard in 0..shard_count {
            sync_path(&shard_path(&self.root, shard))?;
        }
        let record = seal_for(&self.heap, atomic_id)?;
        write_atomic(&sealed_path(&self.root, atomic_id), &encode_seal(&record))?;
        Ok(())
    }

    /// Negative-control hook. Never called by prepare / append / seal.
    pub fn publish_ordinary(
        &mut self,
        collection: CollectionId,
        key: CanonicalKey,
        cell: OrdinaryCell,
    ) {
        self.heap.publish_ordinary(collection, key, cell);
    }
}

fn persist_intent(
    root: &Path,
    atomic_id: AtomicId,
    members: &[AtomicMember],
) -> Result<(), LaneError> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(members.len() as u32).to_be_bytes());
    for member in members {
        let encoded = encode_member(member)?;
        buf.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        buf.extend_from_slice(&encoded);
    }
    write_exclusive(&intent_path(root, atomic_id), &buf)
}

fn persist_prepare(root: &Path, manifest: &PlacementManifest) -> Result<(), LaneError> {
    let prepare = build_lane_prepare(
        manifest.heap_id(),
        manifest.member_manifest_root(),
        manifest.atomic_id(),
        manifest.content_root(),
    );
    let envelope = encode_atomic_prepare_envelope(
        prepare.heap_id.as_bytes(),
        manifest.atomic_id().as_bytes(),
        manifest.content_root().as_bytes(),
    )
    .map_err(|e| LaneError::Format(e.to_string()))?;
    let body = encode_prepare(&prepare)?;
    let frame = encode_atomic_frame(
        FrameKind::BatchPrepare,
        &envelope,
        &body,
        prepare_event_id(manifest.atomic_id()),
    )
    .map_err(|e| LaneError::Format(e.to_string()))?;
    append_synced(&coordinator_path(root), &frame)
}

fn persist_payload(
    root: &Path,
    atomic_id: AtomicId,
    ordinal: u32,
    payload: &[u8],
) -> Result<(), LaneError> {
    write_exclusive(&payload_path(root, atomic_id, ordinal), payload)
}

fn persist_member_frame(
    root: &Path,
    heap_id: HeapId,
    shard: u32,
    member: &AtomicMember,
    content_root: [u8; 32],
) -> Result<(), LaneError> {
    let envelope = encode_atomic_member_envelope(
        heap_id.as_bytes(),
        member.atomic_id.as_bytes(),
        u64::from(member.ordinal),
        &content_root,
        None,
    )
    .map_err(|e| LaneError::Format(e.to_string()))?;
    let body = encode_member(member)?;
    let frame = encode_atomic_frame(
        FrameKind::ItemEvent,
        &envelope,
        &body,
        member.event_id.to_bytes(),
    )
    .map_err(|e| LaneError::Format(e.to_string()))?;
    append_synced(&shard_path(root, shard), &frame)
}

fn parse_shard_count(meta: &str) -> Result<u32, LaneError> {
    for line in meta.lines() {
        if let Some(rest) = line.strip_prefix("shard_count=") {
            return rest
                .trim()
                .parse()
                .map_err(|_| LaneError::Corrupt("meta shard_count"));
        }
    }
    Err(LaneError::Corrupt("meta missing shard_count"))
}

fn shard_count_from_layout(root: &Path) -> Result<u32, LaneError> {
    parse_shard_count(&fs::read_to_string(root.join("meta"))?)
}

fn coordinator_path(root: &Path) -> PathBuf {
    root.join("coordinator.log")
}

fn shard_path(root: &Path, shard: u32) -> PathBuf {
    root.join(format!("shard-{shard:08x}.log"))
}

fn intent_path(root: &Path, atomic_id: AtomicId) -> PathBuf {
    root.join("intent").join(format!("{atomic_id}"))
}

fn payload_path(root: &Path, atomic_id: AtomicId, ordinal: u32) -> PathBuf {
    root.join("payload").join(format!("{atomic_id}-{ordinal}"))
}

fn sealed_path(root: &Path, atomic_id: AtomicId) -> PathBuf {
    root.join("sealed").join(format!("{atomic_id}"))
}

fn prepare_event_id(atomic_id: AtomicId) -> [u8; 16] {
    let mut id = [0u8; 16];
    id.copy_from_slice(&atomic_id.as_bytes()[..16]);
    id
}

fn members_match(manifest: &PlacementManifest, members: &[AtomicMember]) -> bool {
    let entries = manifest.entries();
    if entries.len() != members.len() {
        return false;
    }
    entries.iter().all(|e| {
        members
            .iter()
            .any(|m| m.ordinal == e.member.ordinal && *m == e.member)
    })
}

/// Create `path` or accept an identical existing file. Never replace bytes.
fn write_exclusive(path: &Path, bytes: &[u8]) -> Result<(), LaneError> {
    if path.exists() {
        let existing = fs::read(path)?;
        if existing == bytes {
            return Ok(());
        }
        return Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into());
    }
    write_atomic(path, bytes)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), LaneError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn append_synced(path: &Path, bytes: &[u8]) -> Result<(), LaneError> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn sync_path(path: &Path) -> Result<(), LaneError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn sync_dir(path: &Path) -> Result<(), LaneError> {
    File::open(path)?.sync_all()?;
    Ok(())
}
