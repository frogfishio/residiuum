//! Authenticated reopen of coordinator, intent, members, and seals (CR-R2-002).

use crate::error::LaneError;
use crate::seal::{decode_seal, SealRecord};
use residiuum_atomics::{
    decode_member, decode_prepare, member_hash, ordered_member_manifest_root, AtomicId,
    AtomicMember, AtomicPrepare, ContentRoot, CoordinationScope, HeapId, ResourceLimits,
    StagingHeap, DOMAIN_ATOMIC_PREDICATES, DOMAIN_ATOMIC_READSET,
};
use residiuum_format::{
    examine_atomic_frame, scan_forward, AtomicEvidenceClass, AtomicFrameRole, DecodedFrame,
    SafetyLimits, ScanRegion, ScanReport,
};
use std::fs;
use std::path::Path;

const DOMAIN_RULES: &[u8] = b"RESIDIUUM-ATOMIC-RULES-V1";
const DOMAIN_FRONTIER: &[u8] = b"RESIDIUUM-ATOMIC-LANE-FRONTIER-V1";

pub fn build_lane_prepare(
    heap_id: HeapId,
    manifest_root: [u8; 32],
    atomic_id: AtomicId,
    content_root: ContentRoot,
) -> AtomicPrepare {
    AtomicPrepare {
        atomic_id,
        heap_id,
        scope: CoordinationScope::LocalHeap,
        content_root,
        frontier: lane_frontier(heap_id, content_root, manifest_root),
        ordered_member_manifest_root: manifest_root,
        read_set_root: empty_root(DOMAIN_ATOMIC_READSET),
        predicate_set_root: empty_root(DOMAIN_ATOMIC_PREDICATES),
        active_rule_revision_root: empty_root(DOMAIN_RULES),
        limits: ResourceLimits::hard_local_heap(),
    }
}

pub fn replay_prepares(
    root: &Path,
    heap_id: HeapId,
    heap: &mut StagingHeap,
) -> Result<(), LaneError> {
    let path = coordinator_path(root);
    if !path.exists() {
        return Err(LaneError::Corrupt("coordinator.log missing"));
    }
    let bytes = fs::read(path)?;
    let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
    refuse_midstream_damage(&report)?;
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
        let members = read_intent(root, prepare.atomic_id)?;
        let recomputed = ordered_member_manifest_root(heap_id, &members)?;
        if recomputed != prepare.ordered_member_manifest_root {
            return Err(LaneError::Corrupt("intent manifest root"));
        }
        let expected =
            build_lane_prepare(heap_id, recomputed, prepare.atomic_id, prepare.content_root);
        if prepare != expected {
            return Err(LaneError::Corrupt("prepare not derived from members"));
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
        heap.begin_prepare(prepare.atomic_id, prepare.content_root, &members)?;
    }
    Ok(())
}

pub fn replay_members(
    root: &Path,
    shard_count: u32,
    heap: &mut StagingHeap,
) -> Result<(), LaneError> {
    let mut staged = Vec::new();
    for shard in 0..shard_count {
        let path = shard_path(root, shard);
        if !path.exists() {
            continue;
        }
        let bytes = fs::read(path)?;
        let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
        refuse_midstream_damage(&report)?;
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

pub fn replay_seals(root: &Path, heap: &mut StagingHeap) -> Result<(), LaneError> {
    let dir = root.join("sealed");
    if !dir.exists() {
        return Ok(());
    }
    let mut records = Vec::new();
    for entry in fs::read_dir(dir)? {
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
        let record = decode_seal(&fs::read(entry.path())?)?;
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

fn refuse_midstream_damage(report: &ScanReport) -> Result<(), LaneError> {
    let last_frame_end = report
        .regions
        .iter()
        .rev()
        .find_map(|r| match r {
            ScanRegion::VerifiedFrame { range, .. } => Some(range.end),
            _ => None,
        })
        .unwrap_or(0);
    for region in &report.regions {
        if let ScanRegion::Hole { range, .. } = region {
            if range.start < last_frame_end {
                return Err(LaneError::Corrupt("mid-stream hole"));
            }
        }
    }
    Ok(())
}

fn empty_root(domain: &[u8]) -> [u8; 32] {
    *blake3::hash(domain).as_bytes()
}

fn lane_frontier(heap_id: HeapId, content_root: ContentRoot, manifest_root: [u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_FRONTIER);
    hasher.update(heap_id.as_bytes());
    hasher.update(content_root.as_bytes());
    hasher.update(&manifest_root);
    *hasher.finalize().as_bytes()
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
    if off != bytes.len() {
        return Err(LaneError::Corrupt("intent trailing bytes"));
    }
    Ok(out)
}

fn coordinator_path(root: &Path) -> std::path::PathBuf {
    root.join("coordinator.log")
}

fn shard_path(root: &Path, shard: u32) -> std::path::PathBuf {
    root.join(format!("shard-{shard:08x}.log"))
}

fn intent_path(root: &Path, atomic_id: AtomicId) -> std::path::PathBuf {
    root.join("intent").join(format!("{atomic_id}"))
}

fn payload_path(root: &Path, atomic_id: AtomicId, ordinal: u32) -> std::path::PathBuf {
    root.join("payload").join(format!("{atomic_id}-{ordinal}"))
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
