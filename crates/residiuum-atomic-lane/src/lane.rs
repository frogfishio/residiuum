//! File-backed coordinator / member lane.

use crate::error::LaneError;
use residiuum_atomics::{
    decode_member, decode_prepare, encode_member, encode_prepare, AtomicId, AtomicMember,
    AtomicPrepare, CanonicalKey, CollectionId, ContentRoot, CoordinationScope, CoordinatorSeq,
    HeapId, OrdinaryCell, PlacementManifest, ResourceLimits, StagingFailpoint, StagingHeap,
};
use residiuum_format::{
    encode_atomic_frame, encode_atomic_member_envelope, encode_atomic_prepare_envelope,
    examine_atomic_frame, scan_forward, AtomicEvidenceClass, AtomicFrameRole, FrameKind,
    SafetyLimits,
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
/// - `sealed/<atomic>` — first stable boundary after log `sync_all`
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

    /// Persist intent + prepare, then apply the kernel. Honour prepare failpoints.
    pub fn begin_prepare(
        &mut self,
        atomic_id: AtomicId,
        content_root: ContentRoot,
        members: &[AtomicMember],
    ) -> Result<(CoordinatorSeq, PlacementManifest), LaneError> {
        if self.armed == Some(StagingFailpoint::BeforePrepare) {
            return Err(LaneError::Injected(StagingFailpoint::BeforePrepare));
        }
        let manifest = PlacementManifest::assign(
            self.heap.heap_id(),
            atomic_id,
            content_root,
            shard_count_from_layout(&self.root)?,
            members,
        )?;
        persist_intent(&self.root, atomic_id, members)?;
        persist_prepare(&self.root, self.heap.heap_id(), &manifest)?;
        let out = self.heap.begin_prepare(atomic_id, content_root, members)?;
        if self.armed == Some(StagingFailpoint::AfterPrepare) {
            return Err(LaneError::Injected(StagingFailpoint::AfterPrepare));
        }
        Ok(out)
    }

    /// Persist payload + member frame, then apply the kernel.
    pub fn append_staged(
        &mut self,
        member: AtomicMember,
        payload: Vec<u8>,
    ) -> Result<(), LaneError> {
        let ordinal = member.ordinal;
        let shard = self
            .heap
            .placement(member.atomic_id)
            .and_then(|m| {
                m.entries()
                    .iter()
                    .find(|e| e.member.ordinal == ordinal)
                    .map(|e| e.shard.as_u32())
            })
            .ok_or(LaneError::Corrupt("append without prepare"))?;
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
            shard,
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
        write_atomic(&sealed_path(&self.root, atomic_id), b"boundary\n")?;
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
    write_atomic(&intent_path(root, atomic_id), &buf)
}

fn persist_prepare(
    root: &Path,
    heap_id: HeapId,
    manifest: &PlacementManifest,
) -> Result<(), LaneError> {
    let prepare = AtomicPrepare {
        atomic_id: manifest.atomic_id(),
        heap_id,
        scope: CoordinationScope::LocalHeap,
        content_root: manifest.content_root(),
        frontier: [1u8; 32],
        ordered_member_manifest_root: manifest.member_manifest_root(),
        read_set_root: [2u8; 32],
        predicate_set_root: [3u8; 32],
        active_rule_revision_root: [4u8; 32],
        limits: ResourceLimits::hard_local_heap(),
    };
    let envelope = encode_atomic_prepare_envelope(
        heap_id.as_bytes(),
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
    write_atomic(&payload_path(root, atomic_id, ordinal), payload)
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

fn replay_prepares(root: &Path, heap_id: HeapId, heap: &mut StagingHeap) -> Result<(), LaneError> {
    let path = coordinator_path(root);
    if !path.exists() {
        return Err(LaneError::Corrupt("coordinator.log missing"));
    }
    let bytes = fs::read(path)?;
    let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
    for (_, frame) in report.verified_frames() {
        let Some(AtomicEvidenceClass::Valid(link)) = examine_atomic_frame(frame) else {
            continue;
        };
        if link.role != AtomicFrameRole::Prepare {
            continue;
        }
        let prepare = decode_prepare(&frame.body)?;
        if prepare.heap_id != heap_id {
            return Err(LaneError::Corrupt("prepare heap mismatch"));
        }
        let members = read_intent(root, prepare.atomic_id)?;
        heap.begin_prepare(prepare.atomic_id, prepare.content_root, &members)?;
    }
    Ok(())
}

fn replay_members(root: &Path, shard_count: u32, heap: &mut StagingHeap) -> Result<(), LaneError> {
    let mut staged = Vec::new();
    for shard in 0..shard_count {
        let path = shard_path(root, shard);
        if !path.exists() {
            continue;
        }
        let bytes = fs::read(path)?;
        let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
        for (_, frame) in report.verified_frames() {
            let Some(AtomicEvidenceClass::Valid(link)) = examine_atomic_frame(frame) else {
                continue;
            };
            if link.role != AtomicFrameRole::Member {
                continue;
            }
            let member = decode_member(&frame.body)?;
            let payload = fs::read(payload_path(root, member.atomic_id, member.ordinal))?;
            staged.push((member, payload));
        }
    }
    staged.sort_by_key(|(m, _)| (m.atomic_id.to_bytes(), m.ordinal));
    for (member, payload) in staged {
        heap.append_staged(member, payload)?;
    }
    Ok(())
}

fn replay_seals(root: &Path, heap: &mut StagingHeap) -> Result<(), LaneError> {
    let dir = root.join("sealed");
    if !dir.exists() {
        return Ok(());
    }
    let mut ids = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.ends_with(".tmp") {
            continue;
        }
        let Some(bytes) = parse_hex32(name) else {
            continue;
        };
        ids.push(AtomicId::from_bytes(bytes)?);
    }
    ids.sort_by_key(|id| id.to_bytes());
    for id in ids {
        heap.seal_member_boundary(id)?;
    }
    Ok(())
}

fn read_intent(root: &Path, atomic_id: AtomicId) -> Result<Vec<AtomicMember>, LaneError> {
    let bytes = fs::read(intent_path(root, atomic_id))?;
    if bytes.len() < 4 {
        return Err(LaneError::Corrupt("intent truncated"));
    }
    let count = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut off = 4usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if off + 4 > bytes.len() {
            return Err(LaneError::Corrupt("intent truncated"));
        }
        let n = u32::from_be_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        if off + n > bytes.len() {
            return Err(LaneError::Corrupt("intent truncated"));
        }
        out.push(decode_member(&bytes[off..off + n])?);
        off += n;
    }
    Ok(out)
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

fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
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
