//! Authenticated reopen of coordinator, intent, members, and seals (CR-R2-002).

use crate::checkpoint::{
    checkpoint_path, extend_log_frontier, load_checkpoint, placement_matches, store_checkpoint,
    verify_prefix_marks, CheckpointLoad, RecoveryCheckpoint,
};
use crate::error::LaneError;
use crate::limits::{max_encoded_member_bytes, max_intent_members, RecoveryLimits, SidecarRole};
use crate::seal::{decode_seal, SealRecord};
use residiuum_atomics::{
    decode_canonical_plan, decode_member, decode_prepare, member_hash,
    ordered_member_manifest_root, prepare_from_closed_plan, AtomicId, AtomicMember, AtomicPlan,
    AtomicRefuseReason, AtomicsError, ChunkPlan, HeapId, MemberPhase, StagingHeap,
};
use residiuum_format::{
    examine_atomic_frame, scan_forward, AtomicEvidenceClass, AtomicFrameRole, DecodedFrame,
    HoleReason, SafetyLimits, ScanRegion, ScanReport,
};
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

/// Observed reopen accounting (CR-ATMR3-004). Independent of total media size.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Bytes actually read from logs.
    pub bytes_scanned: u64,
    /// Sidecar bytes charged after a metadata size check (CR-ATMR4-007).
    pub sidecar_bytes: u64,
    /// Verified frames consumed.
    pub frames_verified: u32,
    /// Atomics installed in the live kernel.
    pub atomics: u32,
    /// Members installed in the live kernel.
    pub members: u32,
    /// Directory entries visited.
    pub dirents: u32,
    /// True when a valid checkpoint supplied the prefix.
    pub used_checkpoint: bool,
    /// True when a v1/unrecognized checkpoint was ignored and logs were rebuilt.
    pub rebuilt_checkpoint: bool,
}

/// Running recovery ceilings.
pub struct RecoveryBudget {
    /// Configured ceilings.
    pub limits: RecoveryLimits,
    /// Observed counters.
    pub stats: RecoveryStats,
}

impl RecoveryBudget {
    /// Start a reopen with prototype ceilings.
    pub fn new(limits: RecoveryLimits) -> Self {
        Self {
            limits,
            stats: RecoveryStats::default(),
        }
    }

    fn charge_bytes(&mut self, n: u64) -> Result<(), LaneError> {
        let next = self.stats.bytes_scanned.saturating_add(n);
        if next > self.limits.max_log_bytes {
            return Err(LaneError::Incomplete {
                what: "log bytes",
                observed: next,
                limit: self.limits.max_log_bytes,
            });
        }
        self.stats.bytes_scanned = next;
        Ok(())
    }

    fn charge_sidecar(&mut self, n: u64) -> Result<(), LaneError> {
        let next = self.stats.sidecar_bytes.saturating_add(n);
        if next > self.limits.max_sidecar_bytes {
            return Err(LaneError::Incomplete {
                what: "sidecar bytes",
                observed: next,
                limit: self.limits.max_sidecar_bytes,
            });
        }
        self.stats.sidecar_bytes = next;
        Ok(())
    }

    fn charge_frame(&mut self) -> Result<(), LaneError> {
        let next = self.stats.frames_verified.saturating_add(1);
        if next
            > self
                .limits
                .max_members
                .saturating_add(self.limits.max_atomics)
        {
            return Err(LaneError::Incomplete {
                what: "frames",
                observed: u64::from(next),
                limit: u64::from(
                    self.limits
                        .max_members
                        .saturating_add(self.limits.max_atomics),
                ),
            });
        }
        self.stats.frames_verified = next;
        Ok(())
    }

    fn charge_atomic(&mut self) -> Result<(), LaneError> {
        let next = self.stats.atomics.saturating_add(1);
        if next > self.limits.max_atomics {
            return Err(LaneError::Incomplete {
                what: "atomics",
                observed: u64::from(next),
                limit: u64::from(self.limits.max_atomics),
            });
        }
        self.stats.atomics = next;
        Ok(())
    }

    fn charge_member(&mut self) -> Result<(), LaneError> {
        let next = self.stats.members.saturating_add(1);
        if next > self.limits.max_members {
            return Err(LaneError::Incomplete {
                what: "members",
                observed: u64::from(next),
                limit: u64::from(self.limits.max_members),
            });
        }
        self.stats.members = next;
        Ok(())
    }

    fn charge_dirent(&mut self) -> Result<(), LaneError> {
        let next = self.stats.dirents.saturating_add(1);
        if next > self.limits.max_dirents {
            return Err(LaneError::Incomplete {
                what: "directory entries",
                observed: u64::from(next),
                limit: u64::from(self.limits.max_dirents),
            });
        }
        self.stats.dirents = next;
        Ok(())
    }
}

/// Sidecar magic for a closed plan plus the bound serialization frontier.
pub const PLAN_SIDECAR_MAGIC: &[u8] = b"ATMPLAN1";

/// Encode the exclusive plan sidecar: magic, frontier, canonical plan bytes.
pub fn encode_plan_sidecar(plan: &AtomicPlan, frontier: [u8; 32]) -> Result<Vec<u8>, LaneError> {
    let plan_bytes = residiuum_atomics::encode_canonical_plan(plan)?;
    let mut buf = Vec::with_capacity(PLAN_SIDECAR_MAGIC.len() + 32 + plan_bytes.len());
    buf.extend_from_slice(PLAN_SIDECAR_MAGIC);
    buf.extend_from_slice(&frontier);
    buf.extend_from_slice(&plan_bytes);
    Ok(buf)
}

/// Decode a plan sidecar written by [`encode_plan_sidecar`].
pub fn decode_plan_sidecar(bytes: &[u8]) -> Result<(AtomicPlan, [u8; 32]), LaneError> {
    let header = PLAN_SIDECAR_MAGIC.len() + 32;
    if bytes.len() < header || !bytes.starts_with(PLAN_SIDECAR_MAGIC) {
        return Err(LaneError::Corrupt("plan sidecar"));
    }
    let mut frontier = [0u8; 32];
    frontier.copy_from_slice(&bytes[PLAN_SIDECAR_MAGIC.len()..header]);
    let plan = decode_canonical_plan(&bytes[header..])?;
    Ok((plan, frontier))
}

pub fn replay_prepares(
    root: &Path,
    heap_id: HeapId,
    heap: &mut StagingHeap,
    from_offset: u64,
    budget: &mut RecoveryBudget,
) -> Result<(), LaneError> {
    let path = coordinator_path(root);
    if !path.exists() {
        return Err(LaneError::Corrupt("coordinator.log missing"));
    }
    let (bytes, base) = read_log_tail(&path, from_offset, budget)?;
    let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
    refuse_coverage_damage(
        &report,
        read_log_ack(&path)?,
        base,
        base + bytes.len() as u64,
    )?;
    for (_, frame) in report.verified_frames() {
        let link = require_valid(frame, AtomicFrameRole::Prepare, "coordinator")?;
        let prepare = decode_prepare(&frame.body)?;
        if prepare.heap_id != heap_id {
            return Err(LaneError::Corrupt("prepare heap mismatch"));
        }
        if link.atomic_id != *prepare.atomic_id.as_bytes()
            || link.content_root != *prepare.content_root.as_bytes()
            || link.heap_id != Some(*heap_id.as_bytes())
        {
            return Err(LaneError::Corrupt("prepare envelope mismatch"));
        }
        let members = read_intent(root, prepare.atomic_id, budget)?;
        let recomputed = ordered_member_manifest_root(heap_id, &members)?;
        if recomputed != prepare.ordered_member_manifest_root {
            return Err(LaneError::Corrupt("intent manifest root"));
        }
        let (plan, frontier) = read_plan(root, prepare.atomic_id, budget)?;
        if frontier != prepare.frontier {
            return Err(LaneError::Corrupt("plan frontier mismatch"));
        }
        let expected = prepare_from_closed_plan(&plan, frontier, &members)?;
        if prepare != expected {
            return Err(LaneError::Corrupt("prepare not derived from plan"));
        }
        if heap.can_resolve(prepare.atomic_id) {
            let existing = heap
                .placement(prepare.atomic_id)
                .ok_or(LaneError::Corrupt("prepared without placement"))?;
            if existing.content_root() != prepare.content_root
                || existing.member_manifest_root() != recomputed
            {
                return Err(LaneError::Corrupt("conflicting prepare"));
            }
            continue;
        }
        budget.charge_frame()?;
        budget.charge_atomic()?;
        heap.begin_prepare(prepare.atomic_id, prepare.content_root, &members)?;
    }
    Ok(())
}

pub fn replay_members(
    root: &Path,
    shard_count: u32,
    heap: &mut StagingHeap,
    shard_offsets: &[u64],
    budget: &mut RecoveryBudget,
) -> Result<(), LaneError> {
    for shard in 0..shard_count {
        let path = shard_path(root, shard);
        if !path.exists() {
            continue;
        }
        let from = shard_offsets.get(shard as usize).copied().unwrap_or(0);
        let (bytes, base) = read_log_tail(&path, from, budget)?;
        let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
        refuse_coverage_damage(
            &report,
            read_log_ack(&path)?,
            base,
            base + bytes.len() as u64,
        )?;
        for (_, frame) in report.verified_frames() {
            let link = require_valid(frame, AtomicFrameRole::Member, "member")?;
            let member = decode_member(&frame.body)?;
            if link.atomic_id != *member.atomic_id.as_bytes()
                || link.ordinal != Some(u64::from(member.ordinal))
            {
                return Err(LaneError::Corrupt("member envelope mismatch"));
            }
            let placement = heap
                .placement(member.atomic_id)
                .ok_or(LaneError::Corrupt("member without prepare"))?;
            if link.content_root != *placement.content_root().as_bytes()
                || link.heap_id != Some(*placement.heap_id().as_bytes())
            {
                return Err(LaneError::Corrupt("member prepare mismatch"));
            }
            let entry = placement
                .entries()
                .iter()
                .find(|e| e.member.ordinal == member.ordinal)
                .ok_or(LaneError::Corrupt("member ordinal not in prepare"))?;
            if entry.member != member || entry.member_hash != member_hash(&member)? {
                return Err(LaneError::Corrupt("member does not match prepare"));
            }
            if entry.shard.as_u32() != shard {
                return Err(LaneError::Corrupt("member shard mismatch"));
            }
            budget.charge_frame()?;
            if chunk_manifest_path(root, member.atomic_id, member.ordinal).exists()
                && !payload_path(root, member.atomic_id, member.ordinal).exists()
            {
                continue;
            }
            let payload = read_payload(root, member.atomic_id, member.ordinal, budget)?;
            if let Some(staged) = heap
                .inspect_staged(member.atomic_id)
                .and_then(|ms| ms.iter().find(|s| s.member.ordinal == member.ordinal))
            {
                if staged.member == member && staged.payload == payload {
                    continue;
                }
                return Err(LaneError::Corrupt("conflicting member replay"));
            }
            budget.charge_member()?;
            heap.append_staged(member, payload)?;
        }
    }
    Ok(())
}

/// Reconstruct committed chunk maps and any present chunk bodies.
pub fn replay_chunks(
    root: &Path,
    heap: &mut StagingHeap,
    budget: &mut RecoveryBudget,
) -> Result<(), LaneError> {
    let dir = root.join("chunk-manifest");
    if !dir.exists() {
        return Ok(());
    }
    let mut manifests = Vec::new();
    for entry in fs::read_dir(&dir)? {
        budget.charge_dirent()?;
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(LaneError::Corrupt("chunk-manifest filename"));
        };
        if name.ends_with(".tmp") {
            continue;
        }
        let Some((atomic_id, ordinal)) = parse_chunk_manifest_name(name) else {
            return Err(LaneError::Corrupt("chunk-manifest filename"));
        };
        let plan = decode_chunk_manifest(&read_sidecar(
            &entry.path(),
            SidecarRole::ChunkManifest,
            budget,
        )?)?;
        manifests.push((atomic_id, ordinal, plan));
    }
    manifests.sort_by_key(|(id, ord, _)| (id.to_bytes(), *ord));
    for (atomic_id, ordinal, plan) in manifests {
        if heap
            .lifecycle(atomic_id)
            .is_some_and(|lc| lc.members == MemberPhase::DurableInvisible)
        {
            continue;
        }
        if heap
            .inspect_staged(atomic_id)
            .and_then(|ms| ms.iter().find(|s| s.member.ordinal == ordinal))
            .is_some_and(|s| s.payload_complete)
        {
            continue;
        }
        if let Some(existing) = heap.chunk_plan(atomic_id, ordinal) {
            if existing != &plan {
                return Err(LaneError::Corrupt("chunk-manifest conflict"));
            }
        } else {
            heap.commit_chunk_manifest(atomic_id, ordinal, plan.clone())?;
        }
        let member = heap
            .placement(atomic_id)
            .and_then(|p| {
                p.entries()
                    .iter()
                    .find(|e| e.member.ordinal == ordinal)
                    .map(|e| e.member.clone())
            })
            .ok_or(LaneError::Corrupt("chunk-manifest without member"))?;
        for index in 0..plan.total {
            let path = chunk_body_path(root, atomic_id, ordinal, index);
            if !path.exists() {
                continue;
            }
            let body = read_sidecar(&path, SidecarRole::ChunkBody, budget)?;
            if let Some(staged) = heap
                .inspect_staged(atomic_id)
                .and_then(|ms| ms.iter().find(|s| s.member.ordinal == ordinal))
            {
                if let Some(chunks) = &staged.chunks {
                    if let Some(Some(existing)) = chunks.get(index as usize) {
                        if existing == &body {
                            continue;
                        }
                        return Err(LaneError::Corrupt("chunk body conflict"));
                    }
                }
            }
            budget.charge_member()?;
            heap.append_chunk(member.clone(), index, body)?;
        }
    }
    Ok(())
}

pub fn replay_seals(
    root: &Path,
    heap: &mut StagingHeap,
    budget: &mut RecoveryBudget,
) -> Result<(), LaneError> {
    let dir = root.join("sealed");
    if !dir.exists() {
        return Ok(());
    }
    let mut records = Vec::new();
    for entry in fs::read_dir(dir)? {
        budget.charge_dirent()?;
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(LaneError::Corrupt("seal filename"));
        };
        if name.ends_with(".tmp") {
            continue;
        }
        let Some(id_bytes) = parse_hex32(name) else {
            return Err(LaneError::Corrupt("seal filename"));
        };
        let named = AtomicId::from_bytes(id_bytes)?;
        let record = decode_seal(&read_sidecar(&entry.path(), SidecarRole::Seal, budget)?)?;
        if record.atomic_id != named {
            return Err(LaneError::Corrupt("seal name mismatch"));
        }
        records.push(record);
    }
    records.sort_by_key(|r| r.atomic_id.to_bytes());
    for record in records {
        apply_seal(heap, record)?;
    }
    Ok(())
}

pub fn seal_for(heap: &StagingHeap, atomic_id: AtomicId) -> Result<SealRecord, LaneError> {
    let placement = heap
        .placement(atomic_id)
        .ok_or(LaneError::Corrupt("seal without prepare"))?;
    let staged = heap
        .inspect_staged(atomic_id)
        .ok_or(LaneError::Corrupt("seal without members"))?;
    Ok(SealRecord {
        heap_id: placement.heap_id(),
        atomic_id,
        content_root: placement.content_root(),
        manifest_root: placement.member_manifest_root(),
        member_count: staged.len() as u32,
    })
}

fn apply_seal(heap: &mut StagingHeap, record: SealRecord) -> Result<(), LaneError> {
    let placement = heap
        .placement(record.atomic_id)
        .ok_or(LaneError::Corrupt("seal without prepare"))?;
    if placement.heap_id() != record.heap_id
        || placement.content_root() != record.content_root
        || placement.member_manifest_root() != record.manifest_root
        || placement.entries().len() as u32 != record.member_count
    {
        return Err(LaneError::Corrupt("seal does not match prepare"));
    }
    let staged = heap
        .inspect_staged(record.atomic_id)
        .ok_or(LaneError::Corrupt("seal without members"))?;
    if staged.len() as u32 != record.member_count {
        return Err(LaneError::Corrupt("seal member count"));
    }
    heap.seal_member_boundary(record.atomic_id)?;
    Ok(())
}

fn require_valid(
    frame: &DecodedFrame,
    role: AtomicFrameRole,
    what: &'static str,
) -> Result<residiuum_format::AtomicLinkage, LaneError> {
    match examine_atomic_frame(frame) {
        Some(AtomicEvidenceClass::Valid(link)) if link.role == role => Ok(link),
        Some(AtomicEvidenceClass::Valid(_)) => Err(LaneError::Corrupt("unexpected atomic role")),
        Some(AtomicEvidenceClass::Partial { .. }) => Err(LaneError::Corrupt("partial evidence")),
        Some(AtomicEvidenceClass::Corrupt { .. }) => Err(LaneError::Corrupt("corrupt evidence")),
        Some(AtomicEvidenceClass::Unsupported { .. }) => {
            Err(LaneError::Corrupt("unsupported evidence"))
        }
        None => Err(LaneError::Corrupt(what)),
    }
}

/// Path of the exclusive acknowledged-length sidecar for a log.
pub fn log_ack_path(log_path: &Path) -> std::path::PathBuf {
    log_path.with_extension("ack")
}

/// Read the last synced log length. Missing ack means nothing is acknowledged.
pub fn read_log_ack(log_path: &Path) -> Result<u64, LaneError> {
    let path = log_ack_path(log_path);
    if !path.exists() {
        return Ok(0);
    }
    let len = fs::metadata(&path)?.len();
    if len > SidecarRole::Ack.max_bytes() {
        return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded).into());
    }
    let bytes = fs::read(path)?;
    let arr: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| LaneError::Corrupt("ack length"))?;
    Ok(u64::from_be_bytes(arr))
}

/// Holes inside the acknowledged prefix are damage, even when no later frame
/// verifies. Bytes at or after `ack_len` are an unacked torn tail.
fn refuse_coverage_damage(
    report: &ScanReport,
    ack_len: u64,
    base: u64,
    file_end: u64,
) -> Result<(), LaneError> {
    if file_end < ack_len {
        return Err(LaneError::Coverage {
            start: file_end,
            end: ack_len,
            reason: "acknowledged prefix truncated",
        });
    }
    let last_verified_end = report
        .regions
        .iter()
        .rev()
        .find_map(|r| match r {
            ScanRegion::VerifiedFrame { range, .. } => Some(base + range.end),
            _ => None,
        })
        .unwrap_or(base);
    let covered_end = ack_len.max(last_verified_end);
    for region in &report.regions {
        if let ScanRegion::Hole { range, reason } = region {
            if base + range.start < covered_end {
                return Err(LaneError::Coverage {
                    start: base + range.start,
                    end: base + range.end,
                    reason: hole_reason_label(reason),
                });
            }
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn verify_log_coverage(path: &Path, budget: &mut RecoveryBudget) -> Result<(), LaneError> {
    if !path.exists() {
        return Ok(());
    }
    let (bytes, base) = read_log_tail(path, 0, budget)?;
    let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
    refuse_coverage_damage(
        &report,
        read_log_ack(path)?,
        base,
        base + bytes.len() as u64,
    )
}

/// Read `[from, min(len, from+remaining_budget)]` without allocating `file.len()`.
fn read_log_tail(
    path: &Path,
    from: u64,
    budget: &mut RecoveryBudget,
) -> Result<(Vec<u8>, u64), LaneError> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if from > file_len {
        return Err(LaneError::Corrupt("log tail offset"));
    }
    let remaining = file_len - from;
    if remaining
        > budget
            .limits
            .max_log_bytes
            .saturating_sub(budget.stats.bytes_scanned)
    {
        return Err(LaneError::Incomplete {
            what: "log bytes",
            observed: remaining.saturating_add(budget.stats.bytes_scanned),
            limit: budget.limits.max_log_bytes,
        });
    }
    if from > 0 {
        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(from))?;
    }
    let mut buf = Vec::new();
    buf.try_reserve(remaining as usize)
        .map_err(|_| LaneError::Incomplete {
            what: "log bytes",
            observed: remaining,
            limit: budget.limits.max_log_bytes,
        })?;
    file.read_to_end(&mut buf)?;
    budget.charge_bytes(buf.len() as u64)?;
    Ok((buf, from))
}

/// Reconstruct prefix identities from a checkpoint without scanning prefix logs.
pub fn apply_checkpoint(
    root: &Path,
    heap: &mut StagingHeap,
    ck: &RecoveryCheckpoint,
    budget: &mut RecoveryBudget,
) -> Result<(), LaneError> {
    for item in &ck.atomics {
        budget.charge_atomic()?;
        let members = read_intent(root, item.atomic_id, budget)?;
        if members.len() as u32 != item.member_count {
            return Err(LaneError::Corrupt("checkpoint member count"));
        }
        let (plan, frontier) = read_plan(root, item.atomic_id, budget)?;
        let derived = prepare_from_closed_plan(&plan, frontier, &members)?;
        if derived.content_root != item.content_root
            || derived.ordered_member_manifest_root != item.manifest_root
            || derived.atomic_id != item.atomic_id
        {
            return Err(LaneError::Corrupt("checkpoint plan mismatch"));
        }
        heap.begin_prepare(item.atomic_id, item.content_root, &members)?;
        {
            let placement = heap
                .placement(item.atomic_id)
                .ok_or(LaneError::Corrupt("checkpoint placement"))?;
            if !placement_matches(item, placement) {
                return Err(LaneError::Corrupt("checkpoint placement mismatch"));
            }
        }
        for member in members {
            let path = payload_path(root, member.atomic_id, member.ordinal);
            if !path.exists() {
                continue;
            }
            budget.charge_member()?;
            let payload = read_payload(root, member.atomic_id, member.ordinal, budget)?;
            heap.append_staged(member, payload)?;
        }
    }
    replay_chunks(root, heap, budget)?;
    Ok(())
}

fn verify_checkpoint_prefixes(
    root: &Path,
    ck: &RecoveryCheckpoint,
    budget: &mut RecoveryBudget,
) -> Result<(), LaneError> {
    if ck.shard_hashes.len() != ck.shard_offsets.len()
        || ck.shard_marks.len() != ck.shard_offsets.len()
    {
        return Err(LaneError::Corrupt("checkpoint hash count"));
    }
    let n = verify_prefix_marks(
        &coordinator_path(root),
        ck.coordinator_offset,
        &ck.coordinator_marks,
    )?;
    budget.charge_bytes(n)?;
    for (i, (off, marks)) in ck
        .shard_offsets
        .iter()
        .zip(ck.shard_marks.iter())
        .enumerate()
    {
        let n = verify_prefix_marks(&shard_path(root, i as u32), *off, marks)?;
        budget.charge_bytes(n)?;
    }
    Ok(())
}

/// Open helper: authenticated checkpoint prefix plus bounded tails, or a
/// full scan when no v2 checkpoint exists (CR-ATMR4-003).
pub fn recover_heap(
    root: &Path,
    heap_id: HeapId,
    shard_count: u32,
    heap: &mut StagingHeap,
    budget: &mut RecoveryBudget,
) -> Result<(), LaneError> {
    match load_checkpoint(root, budget.limits)? {
        CheckpointLoad::Ready(ck) if ck.shard_offsets.len() as u32 == shard_count => {
            if let Ok(meta) = fs::metadata(checkpoint_path(root)) {
                budget.charge_sidecar(meta.len())?;
            }
            verify_checkpoint_prefixes(root, &ck, budget)?;
            apply_checkpoint(root, heap, &ck, budget)?;
            budget.stats.used_checkpoint = true;
            replay_prepares(root, heap_id, heap, ck.coordinator_offset, budget)?;
            replay_members(root, shard_count, heap, &ck.shard_offsets, budget)?;
            replay_chunks(root, heap, budget)?;
            replay_seals(root, heap, budget)?;
            return Ok(());
        }
        CheckpointLoad::Ready(_) => {
            return Err(LaneError::Corrupt("checkpoint shard count"));
        }
        CheckpointLoad::Legacy => {
            budget.stats.rebuilt_checkpoint = true;
        }
        CheckpointLoad::Missing => {}
    }
    replay_prepares(root, heap_id, heap, 0, budget)?;
    replay_members(root, shard_count, heap, &[], budget)?;
    replay_chunks(root, heap, budget)?;
    replay_seals(root, heap, budget)?;
    Ok(())
}

/// Persist a checkpoint of the reconstructed kernel and ack frontiers.
pub fn persist_recovery_checkpoint(
    root: &Path,
    heap: &StagingHeap,
    shard_count: u32,
) -> Result<(), LaneError> {
    let coordinator_offset = read_log_ack(&coordinator_path(root))?;
    let mut shard_offsets = Vec::with_capacity(shard_count as usize);
    for shard in 0..shard_count {
        shard_offsets.push(read_log_ack(&shard_path(root, shard))?);
    }
    let prev = match load_checkpoint(root, RecoveryLimits::prototype())? {
        CheckpointLoad::Ready(ck) => Some(ck),
        _ => None,
    };
    let (coordinator_marks, coordinator_hash, _) = extend_log_frontier(
        &coordinator_path(root),
        prev.as_ref().map(|c| {
            (
                &c.coordinator_hash,
                c.coordinator_offset,
                &c.coordinator_marks,
            )
        }),
        coordinator_offset,
    )?;
    let mut shard_hashes = Vec::with_capacity(shard_offsets.len());
    let mut shard_marks = Vec::with_capacity(shard_offsets.len());
    for (i, off) in shard_offsets.iter().enumerate() {
        let prev_shard = prev.as_ref().and_then(|c| {
            Some((
                c.shard_hashes.get(i)?,
                *c.shard_offsets.get(i)?,
                c.shard_marks.get(i)?,
            ))
        });
        let (marks, digest, _) =
            extend_log_frontier(&shard_path(root, i as u32), prev_shard, *off)?;
        shard_hashes.push(digest);
        shard_marks.push(marks);
    }
    let ck = RecoveryCheckpoint::from_heap(
        heap,
        coordinator_offset,
        shard_offsets,
        coordinator_hash,
        shard_hashes,
        coordinator_marks,
        shard_marks,
    );
    store_checkpoint(root, &ck)
}

fn hole_reason_label(reason: &HoleReason) -> &'static str {
    match reason {
        HoleReason::UnclassifiedGarbage => "unclassified garbage in coverage",
        HoleReason::CorruptCandidate { .. } => "corrupt candidate in coverage",
        HoleReason::DamagedCandidate { .. } => "damaged candidate in coverage",
    }
}

fn read_sidecar(
    path: &Path,
    role: SidecarRole,
    budget: &mut RecoveryBudget,
) -> Result<Vec<u8>, LaneError> {
    let len = fs::metadata(path)?.len();
    if len > role.max_bytes() {
        return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded).into());
    }
    budget.charge_sidecar(len)?;
    Ok(fs::read(path)?)
}

fn read_plan(
    root: &Path,
    atomic_id: AtomicId,
    budget: &mut RecoveryBudget,
) -> Result<(AtomicPlan, [u8; 32]), LaneError> {
    let bytes = read_sidecar(&plan_path(root, atomic_id), SidecarRole::Plan, budget)?;
    decode_plan_sidecar(&bytes)
}

fn read_intent(
    root: &Path,
    atomic_id: AtomicId,
    budget: &mut RecoveryBudget,
) -> Result<Vec<AtomicMember>, LaneError> {
    let bytes = read_sidecar(&intent_path(root, atomic_id), SidecarRole::Intent, budget)?;
    if bytes.len() < 4 {
        return Err(LaneError::Corrupt("intent truncated"));
    }
    let count = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
    if count > max_intent_members() {
        return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded).into());
    }
    let mut off = 4usize;
    let mut out = Vec::new();
    for _ in 0..count {
        let header_end = off
            .checked_add(4)
            .ok_or(LaneError::Corrupt("intent overflow"))?;
        if header_end > bytes.len() {
            return Err(LaneError::Corrupt("intent truncated"));
        }
        let n = u32::from_be_bytes(bytes[off..header_end].try_into().unwrap());
        if n == 0 || n > max_encoded_member_bytes() {
            return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded).into());
        }
        let body_end = header_end
            .checked_add(n as usize)
            .ok_or(LaneError::Corrupt("intent overflow"))?;
        if body_end > bytes.len() {
            return Err(LaneError::Corrupt("intent truncated"));
        }
        out.push(decode_member(&bytes[header_end..body_end])?);
        off = body_end;
    }
    if off != bytes.len() {
        return Err(LaneError::Corrupt("intent trailing bytes"));
    }
    Ok(out)
}

fn read_payload(
    root: &Path,
    atomic_id: AtomicId,
    ordinal: u32,
    budget: &mut RecoveryBudget,
) -> Result<Vec<u8>, LaneError> {
    read_sidecar(
        &payload_path(root, atomic_id, ordinal),
        SidecarRole::Payload,
        budget,
    )
}

fn coordinator_path(root: &Path) -> std::path::PathBuf {
    root.join("coordinator.log")
}

fn shard_path(root: &Path, shard: u32) -> std::path::PathBuf {
    root.join(format!("shard-{shard:08x}.log"))
}

fn plan_path(root: &Path, atomic_id: AtomicId) -> std::path::PathBuf {
    root.join("plan").join(format!("{atomic_id}"))
}

fn intent_path(root: &Path, atomic_id: AtomicId) -> std::path::PathBuf {
    root.join("intent").join(format!("{atomic_id}"))
}

fn payload_path(root: &Path, atomic_id: AtomicId, ordinal: u32) -> std::path::PathBuf {
    root.join("payload").join(format!("{atomic_id}-{ordinal}"))
}

fn chunk_manifest_path(root: &Path, atomic_id: AtomicId, ordinal: u32) -> std::path::PathBuf {
    root.join("chunk-manifest")
        .join(format!("{atomic_id}-{ordinal}"))
}

fn chunk_body_path(
    root: &Path,
    atomic_id: AtomicId,
    ordinal: u32,
    index: u32,
) -> std::path::PathBuf {
    root.join("chunk")
        .join(format!("{atomic_id}-{ordinal}-{index}"))
}

fn parse_chunk_manifest_name(name: &str) -> Option<(AtomicId, u32)> {
    let (hex, ord) = name.rsplit_once('-')?;
    let id_bytes = parse_hex32(hex)?;
    let atomic_id = AtomicId::from_bytes(id_bytes).ok()?;
    let ordinal = ord.parse().ok()?;
    Some((atomic_id, ordinal))
}

fn decode_chunk_manifest(bytes: &[u8]) -> Result<ChunkPlan, LaneError> {
    if bytes.len() < 4 {
        return Err(LaneError::Corrupt("chunk-manifest truncated"));
    }
    let total = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
    let expected = 4usize
        .checked_add(
            32usize
                .checked_mul(total as usize)
                .ok_or(LaneError::Corrupt("chunk-manifest overflow"))?,
        )
        .ok_or(LaneError::Corrupt("chunk-manifest overflow"))?;
    if bytes.len() != expected {
        return Err(LaneError::Corrupt("chunk-manifest length"));
    }
    let mut chunk_hashes = Vec::with_capacity(total as usize);
    let mut off = 4usize;
    for _ in 0..total {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes[off..off + 32]);
        chunk_hashes.push(hash);
        off += 32;
    }
    let plan = ChunkPlan {
        total,
        chunk_hashes,
    };
    plan.validate()?;
    Ok(plan)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{extend_log_frontier, store_checkpoint, RecoveryCheckpoint};

    #[test]
    fn covered_prefix_larger_than_budget_opens_from_tails() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let coord = coordinator_path(root);
        let shard = shard_path(root, 0);
        std::fs::write(&coord, []).unwrap();
        std::fs::write(&shard, []).unwrap();
        let covered = RecoveryLimits::prototype().max_log_bytes.saturating_mul(2);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&coord)
            .unwrap()
            .set_len(covered)
            .unwrap();
        std::fs::write(log_ack_path(&coord), covered.to_be_bytes()).unwrap();
        std::fs::write(log_ack_path(&shard), 0u64.to_be_bytes()).unwrap();
        let (coord_marks, coord_hash, _) = extend_log_frontier(&coord, None, covered).unwrap();
        let (shard_marks, shard_hash, _) = extend_log_frontier(&shard, None, 0).unwrap();
        let mut hid = [0u8; 16];
        hid[0] = 1;
        let heap_id = HeapId::from_bytes(hid).unwrap();
        let heap = StagingHeap::new(heap_id, 1).unwrap();
        let ck = RecoveryCheckpoint::from_heap(
            &heap,
            covered,
            vec![0],
            coord_hash,
            vec![shard_hash],
            coord_marks,
            vec![shard_marks],
        );
        store_checkpoint(root, &ck).unwrap();
        let mut heap = StagingHeap::new(heap_id, 1).unwrap();
        let mut budget = RecoveryBudget::new(RecoveryLimits::prototype());
        recover_heap(root, heap_id, 1, &mut heap, &mut budget).unwrap();
        assert!(budget.stats.used_checkpoint);
        assert!(
            budget.stats.bytes_scanned < 64 * 1024,
            "historical prefix must not be charged: {}",
            budget.stats.bytes_scanned
        );
    }
}
