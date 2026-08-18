//! File-backed coordinator / member lane.

use crate::error::LaneError;
use crate::limits::RecoveryLimits;
use crate::limits::{max_encoded_member_bytes, max_intent_members, max_payload_bytes, MAX_SHARDS};
use crate::recover::{
    encode_plan_sidecar, log_ack_path, persist_recovery_checkpoint, recover_heap, seal_for,
    RecoveryBudget, RecoveryStats,
};
use crate::seal::encode_seal;
use crate::writer::LaneWriterGuard;
use residiuum_atomics::{
    encode_member, encode_prepare, prepare_from_closed_plan, AtomicId, AtomicMember, AtomicPlan,
    AtomicRefuseReason, AtomicsError, CanonicalKey, ChunkPlan, CollectionId, CoordinatorSeq,
    HeapId, OrdinaryCell, PlacementManifest, StagingFailpoint, StagingHeap,
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
/// - `coordinator.ack` — acknowledged coordinator length
/// - `shard-XXXXXXXX.log` — `ItemEvent` member frames
/// - `shard-XXXXXXXX.ack` — acknowledged shard-log length
/// - `plan/<atomic>` — closed plan + bound frontier (`ATMPLAN1`)
/// - `intent/<atomic>` — frozen members, written and synced *before* prepare
/// - `payload/<atomic>-<ord>` — value bytes, synced *before* the member frame
/// - `sealed/<atomic>` — versioned checksummed boundary (Heap, Atomic ID,
///   content root, manifest root, member count)
/// - `checkpoint` — identity summaries + acknowledged log tails
/// - `writer.lock` — exclusive physical writer (CR-ATMR3-007)
/// - `chunk-manifest/<atomic>-<ord>` — frozen chunk map (total + hashes)
/// - `chunk/<atomic>-<ord>-<index>` — one chunk body
pub struct DurableLane {
    root: PathBuf,
    heap: StagingHeap,
    armed: Option<StagingFailpoint>,
    stats: RecoveryStats,
    _writer: LaneWriterGuard,
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
        let writer = LaneWriterGuard::acquire(&root)?;
        let id_path = root.join("heap.id");
        if id_path.exists() {
            return Err(LaneError::Corrupt("heap.id already exists"));
        }
        check_shard_count(shard_count)?;
        let heap = StagingHeap::new(heap_id, shard_count)?;
        write_atomic(&id_path, heap_id.as_bytes())?;
        write_atomic(
            &root.join("meta"),
            format!("shard_count={shard_count}\n").as_bytes(),
        )?;
        fs::create_dir_all(root.join("plan"))?;
        fs::create_dir_all(root.join("intent"))?;
        fs::create_dir_all(root.join("payload"))?;
        fs::create_dir_all(root.join("chunk-manifest"))?;
        fs::create_dir_all(root.join("chunk"))?;
        fs::create_dir_all(root.join("sealed"))?;
        File::create(coordinator_path(&root))?;
        persist_log_ack(&coordinator_path(&root))?;
        for shard in 0..shard_count {
            File::create(shard_path(&root, shard))?;
            persist_log_ack(&shard_path(&root, shard))?;
        }
        sync_dir(&root)?;
        sync_dir(&root.join("plan"))?;
        sync_dir(&root.join("intent"))?;
        sync_dir(&root.join("payload"))?;
        sync_dir(&root.join("chunk-manifest"))?;
        sync_dir(&root.join("chunk"))?;
        sync_dir(&root.join("sealed"))?;
        persist_recovery_checkpoint(&root, &heap, shard_count)?;
        Ok(Self {
            root,
            heap,
            armed: None,
            stats: RecoveryStats::default(),
            _writer: writer,
        })
    }

    /// Reconstruct a lane from a directory image.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, LaneError> {
        let root = root.as_ref().to_path_buf();
        let writer = LaneWriterGuard::acquire(&root)?;
        let mut id_bytes = [0u8; 16];
        File::open(root.join("heap.id"))?.read_exact(&mut id_bytes)?;
        let heap_id = HeapId::from_bytes(id_bytes)?;
        let shard_count = parse_shard_count(&fs::read_to_string(root.join("meta"))?)?;
        let mut heap = StagingHeap::new(heap_id, shard_count)?;
        let mut budget = RecoveryBudget::new(RecoveryLimits::prototype());
        recover_heap(&root, heap_id, shard_count, &mut heap, &mut budget)?;
        persist_recovery_checkpoint(&root, &heap, shard_count)?;
        Ok(Self {
            root,
            heap,
            armed: None,
            stats: budget.stats,
            _writer: writer,
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

    /// Bytes/frames/identities observed by the last `open`.
    pub const fn recovery_stats(&self) -> &RecoveryStats {
        &self.stats
    }

    /// True while this instance holds the exclusive writer lock.
    pub const fn holds_writer_lock(&self) -> bool {
        true
    }

    /// Validate a closed plan, then persist plan + intent + prepare, then apply.
    ///
    /// CR-ATMR3-001: the durable prepare is `prepare_from_closed_plan`. Independent
    /// Atomic IDs, content roots, and member slices are not accepted. CR-R2-001:
    /// kernel reservation happens before any durable path is created or replaced.
    pub fn begin_prepare(
        &mut self,
        plan: &AtomicPlan,
        frontier: [u8; 32],
        members: &[AtomicMember],
    ) -> Result<(CoordinatorSeq, PlacementManifest), LaneError> {
        if self.armed == Some(StagingFailpoint::BeforePrepare) {
            return Err(LaneError::Injected(StagingFailpoint::BeforePrepare));
        }
        if plan.heap_id() != self.heap.heap_id() {
            return Err(AtomicsError::Refused(AtomicRefuseReason::CrossHeapCollection).into());
        }
        let prepare = prepare_from_closed_plan(plan, frontier, members)?;
        let atomic_id = prepare.atomic_id;
        let content_root = prepare.content_root;
        let sidecar = encode_plan_sidecar(plan, frontier)?;
        if let Some(existing) = self.heap.placement(atomic_id) {
            if existing.content_root() == content_root
                && members_match(existing, members)
                && same_sidecar(&self.root, atomic_id, &sidecar)
            {
                let seq = self
                    .heap
                    .prepare_seq(atomic_id)
                    .ok_or(LaneError::Corrupt("prepared without sequence"))?;
                return Ok((seq, existing.clone()));
            }
            return Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into());
        }
        self.heap
            .check_begin_prepare(atomic_id, content_root, members)?;
        persist_plan(&self.root, atomic_id, &sidecar)?;
        persist_intent(&self.root, atomic_id, members)?;
        persist_prepare(&self.root, &prepare)?;
        let out = self.heap.begin_prepare(atomic_id, content_root, members)?;
        self.write_checkpoint()?;
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
        self.write_checkpoint()?;
        if self.armed == Some(StagingFailpoint::AfterMember(ordinal)) {
            return Err(LaneError::Injected(StagingFailpoint::AfterMember(ordinal)));
        }
        Ok(())
    }

    /// Persist the frozen chunk map, then install it in the kernel.
    pub fn commit_chunk_manifest(
        &mut self,
        atomic_id: AtomicId,
        ordinal: u32,
        plan: ChunkPlan,
    ) -> Result<(), LaneError> {
        if let Some(existing) = self.heap.chunk_plan(atomic_id, ordinal) {
            if existing == &plan {
                return Ok(());
            }
            return Err(AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget).into());
        }
        self.heap
            .check_commit_chunk_manifest(atomic_id, ordinal, &plan)?;
        persist_chunk_manifest(&self.root, atomic_id, ordinal, &plan)?;
        self.heap
            .commit_chunk_manifest(atomic_id, ordinal, plan)?;
        self.write_checkpoint()?;
        Ok(())
    }

    /// Persist one chunk body, then apply the kernel. Assembled payload is
    /// written only when every chunk is present.
    pub fn append_chunk(
        &mut self,
        member: AtomicMember,
        index: u32,
        body: Vec<u8>,
    ) -> Result<(), LaneError> {
        if let Some(staged) = self
            .heap
            .inspect_staged(member.atomic_id)
            .and_then(|ms| ms.iter().find(|s| s.member.ordinal == member.ordinal))
        {
            if let Some(chunks) = &staged.chunks {
                if let Some(Some(existing)) = chunks.get(index as usize) {
                    if existing == &body && staged.member == member {
                        return Ok(());
                    }
                    return Err(
                        AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget).into(),
                    );
                }
            }
        }
        self.heap.check_append_chunk(&member, index, &body)?;
        persist_chunk_body(&self.root, member.atomic_id, member.ordinal, index, &body)?;
        self.heap.append_chunk(member.clone(), index, body)?;
        if let Some(staged) = self
            .heap
            .inspect_staged(member.atomic_id)
            .and_then(|ms| ms.iter().find(|s| s.member.ordinal == member.ordinal))
        {
            if staged.payload_complete {
                persist_payload(
                    &self.root,
                    member.atomic_id,
                    member.ordinal,
                    &staged.payload,
                )?;
            }
        }
        self.write_checkpoint()?;
        if self.armed
            == Some(StagingFailpoint::AfterChunk {
                ordinal: member.ordinal,
                index,
            })
        {
            return Err(LaneError::Injected(StagingFailpoint::AfterChunk {
                ordinal: member.ordinal,
                index,
            }));
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
        self.write_checkpoint()?;
        Ok(())
    }

    fn write_checkpoint(&self) -> Result<(), LaneError> {
        let shard_count = shard_count_from_layout(&self.root)?;
        persist_recovery_checkpoint(&self.root, &self.heap, shard_count)
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

fn persist_plan(root: &Path, atomic_id: AtomicId, sidecar: &[u8]) -> Result<(), LaneError> {
    write_exclusive(&plan_path(root, atomic_id), sidecar)
}

fn same_sidecar(root: &Path, atomic_id: AtomicId, sidecar: &[u8]) -> bool {
    fs::read(plan_path(root, atomic_id))
        .ok()
        .is_some_and(|existing| existing == sidecar)
}

fn persist_intent(
    root: &Path,
    atomic_id: AtomicId,
    members: &[AtomicMember],
) -> Result<(), LaneError> {
    if members.len() as u32 > max_intent_members() {
        return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded).into());
    }
    let mut buf = Vec::new();
    buf.extend_from_slice(&(members.len() as u32).to_be_bytes());
    for member in members {
        let encoded = encode_member(member)?;
        if encoded.len() as u32 > max_encoded_member_bytes() {
            return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded).into());
        }
        buf.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        buf.extend_from_slice(&encoded);
    }
    write_exclusive(&intent_path(root, atomic_id), &buf)
}

fn persist_prepare(
    root: &Path,
    prepare: &residiuum_atomics::AtomicPrepare,
) -> Result<(), LaneError> {
    let envelope = encode_atomic_prepare_envelope(
        prepare.heap_id.as_bytes(),
        prepare.atomic_id.as_bytes(),
        prepare.content_root.as_bytes(),
    )
    .map_err(|e| LaneError::Format(e.to_string()))?;
    let body = encode_prepare(prepare)?;
    let frame = encode_atomic_frame(
        FrameKind::BatchPrepare,
        &envelope,
        &body,
        prepare_event_id(prepare.atomic_id),
    )
    .map_err(|e| LaneError::Format(e.to_string()))?;
    append_synced(&coordinator_path(root), &frame)
}

fn persist_chunk_manifest(
    root: &Path,
    atomic_id: AtomicId,
    ordinal: u32,
    plan: &ChunkPlan,
) -> Result<(), LaneError> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&plan.total.to_be_bytes());
    for hash in &plan.chunk_hashes {
        buf.extend_from_slice(hash);
    }
    write_exclusive(&chunk_manifest_path(root, atomic_id, ordinal), &buf)
}

fn persist_chunk_body(
    root: &Path,
    atomic_id: AtomicId,
    ordinal: u32,
    index: u32,
    body: &[u8],
) -> Result<(), LaneError> {
    write_exclusive(&chunk_body_path(root, atomic_id, ordinal, index), body)
}

fn persist_payload(
    root: &Path,
    atomic_id: AtomicId,
    ordinal: u32,
    payload: &[u8],
) -> Result<(), LaneError> {
    if payload.len() as u32 > max_payload_bytes() {
        return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded).into());
    }
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
            let n: u32 = rest
                .trim()
                .parse()
                .map_err(|_| LaneError::Corrupt("meta shard_count"))?;
            return check_shard_count(n);
        }
    }
    Err(LaneError::Corrupt("meta missing shard_count"))
}

fn check_shard_count(n: u32) -> Result<u32, LaneError> {
    if n == 0 || n > MAX_SHARDS {
        return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded).into());
    }
    Ok(n)
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

fn plan_path(root: &Path, atomic_id: AtomicId) -> PathBuf {
    root.join("plan").join(format!("{atomic_id}"))
}

fn intent_path(root: &Path, atomic_id: AtomicId) -> PathBuf {
    root.join("intent").join(format!("{atomic_id}"))
}

fn payload_path(root: &Path, atomic_id: AtomicId, ordinal: u32) -> PathBuf {
    root.join("payload").join(format!("{atomic_id}-{ordinal}"))
}

fn chunk_manifest_path(root: &Path, atomic_id: AtomicId, ordinal: u32) -> PathBuf {
    root.join("chunk-manifest")
        .join(format!("{atomic_id}-{ordinal}"))
}

fn chunk_body_path(root: &Path, atomic_id: AtomicId, ordinal: u32, index: u32) -> PathBuf {
    root.join("chunk")
        .join(format!("{atomic_id}-{ordinal}-{index}"))
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

/// Create `path` with `O_EXCL` or accept identical existing bytes. Never replace.
fn write_exclusive(path: &Path, bytes: &[u8]) -> Result<(), LaneError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.sync_all()?;
            if let Some(parent) = path.parent() {
                sync_dir(parent)?;
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = fs::read(path)?;
            if existing == bytes {
                Ok(())
            } else {
                Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into())
            }
        }
        Err(e) => Err(e.into()),
    }
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
    persist_log_ack(path)
}

fn persist_log_ack(log_path: &Path) -> Result<(), LaneError> {
    let len = fs::metadata(log_path)?.len();
    write_atomic(&log_ack_path(log_path), &len.to_be_bytes())
}

fn sync_path(path: &Path) -> Result<(), LaneError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn sync_dir(path: &Path) -> Result<(), LaneError> {
    File::open(path)?.sync_all()?;
    Ok(())
}