//! Authenticated reopen of coordinator, intent, members, and seals (CR-R2-002).

use crate::error::LaneError;
use crate::limits::{max_encoded_member_bytes, max_intent_members, max_payload_bytes};
use crate::seal::{decode_seal, SealRecord};
use residiuum_atomics::{
    decode_canonical_plan, decode_member, decode_prepare, member_hash,
    ordered_member_manifest_root, prepare_from_closed_plan, AtomicId, AtomicMember, AtomicPlan,
    AtomicRefuseReason, AtomicsError, HeapId, StagingHeap,
};
use residiuum_format::{
    examine_atomic_frame, scan_forward, AtomicEvidenceClass, AtomicFrameRole, DecodedFrame,
    HoleReason, SafetyLimits, ScanRegion, ScanReport,
};
use std::fs;
use std::path::Path;

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
) -> Result<(), LaneError> {
    let path = coordinator_path(root);
    if !path.exists() {
        return Err(LaneError::Corrupt("coordinator.log missing"));
    }
    let bytes = fs::read(&path)?;
    let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
    refuse_coverage_damage(&report, read_log_ack(&path)?)?;
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
        let (plan, frontier) = read_plan(root, prepare.atomic_id)?;
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
        let bytes = fs::read(&path)?;
        let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
        refuse_coverage_damage(&report, read_log_ack(&path)?)?;
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
            let payload = read_payload(root, member.atomic_id, member.ordinal)?;
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
    let bytes = fs::read(path)?;
    let arr: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| LaneError::Corrupt("ack length"))?;
    Ok(u64::from_be_bytes(arr))
}

/// Holes inside the acknowledged prefix are damage, even when no later frame
/// verifies. Bytes at or after `ack_len` are an unacked torn tail.
fn refuse_coverage_damage(report: &ScanReport, ack_len: u64) -> Result<(), LaneError> {
    if report.source_len < ack_len {
        return Err(LaneError::Coverage {
            start: report.source_len,
            end: ack_len,
            reason: "acknowledged prefix truncated",
        });
    }
    let last_verified_end = report
        .regions
        .iter()
        .rev()
        .find_map(|r| match r {
            ScanRegion::VerifiedFrame { range, .. } => Some(range.end),
            _ => None,
        })
        .unwrap_or(0);
    let covered_end = ack_len.max(last_verified_end);
    for region in &report.regions {
        if let ScanRegion::Hole { range, reason } = region {
            if range.start < covered_end {
                return Err(LaneError::Coverage {
                    start: range.start,
                    end: range.end,
                    reason: hole_reason_label(reason),
                });
            }
        }
    }
    Ok(())
}

fn hole_reason_label(reason: &HoleReason) -> &'static str {
    match reason {
        HoleReason::UnclassifiedGarbage => "unclassified garbage in coverage",
        HoleReason::CorruptCandidate { .. } => "corrupt candidate in coverage",
        HoleReason::DamagedCandidate { .. } => "damaged candidate in coverage",
    }
}

fn read_plan(root: &Path, atomic_id: AtomicId) -> Result<(AtomicPlan, [u8; 32]), LaneError> {
    let bytes = fs::read(plan_path(root, atomic_id))?;
    decode_plan_sidecar(&bytes)
}

fn read_intent(root: &Path, atomic_id: AtomicId) -> Result<Vec<AtomicMember>, LaneError> {
    let bytes = fs::read(intent_path(root, atomic_id))?;
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

fn read_payload(root: &Path, atomic_id: AtomicId, ordinal: u32) -> Result<Vec<u8>, LaneError> {
    let path = payload_path(root, atomic_id, ordinal);
    let len = fs::metadata(&path)?.len();
    if len > u64::from(max_payload_bytes()) {
        return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded).into());
    }
    Ok(fs::read(path)?)
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
