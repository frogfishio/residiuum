//! Honest store Atomic classifier (CR-ATMR5-002 / CR-ATMR6-002).
//!
//! Recovery and examination share this path. Damaged, conflicting, foreign,
//! and incomplete records are reported. They are never last-wins, first-wins,
//! or translated into a reusable unused identity. Seal conflicts and
//! pre-prepare orphans block the named identity. Findings are persisted with
//! the catalogue so an ordinary reopen cannot forget them.

use crate::atomic_stage_media::{decode_stage_sidecar, SidecarDecode, SidecarKind, StageCatalog};
use residiuum_atomics::{
    members_match_prepare, AtomicId, AtomicMember, AtomicPrepare, ChunkPlan, HeapId,
};
use residiuum_format::{
    examine_atomic_frame, AtomicEvidenceClass, AtomicFrameRole, DecodedFrame, HoleReason,
};

/// Physical or logical Atomic evidence kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageEvidenceKind {
    /// Authenticated Atomic coverage is incomplete.
    Coverage,
    /// `scan_forward` hole (unclassified or corrupt candidate).
    Hole,
    /// Prepare (BatchPrepare or ATPREP1).
    Prepare,
    /// Member ItemEvent.
    Member,
    /// ATPAY1 payload sidecar.
    Payload,
    /// ATSEAL1 first-stable-boundary sidecar.
    Seal,
    /// ATMAP1 frozen chunk map.
    ChunkPlan,
    /// ATCHK1 verified chunk body.
    ChunkBody,
    /// Verified frame that is not Atomic staging evidence.
    Other,
}

/// Classifier outcome. Never a guessed recovery truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageEvidenceClass {
    /// Complete, heap-bound, non-conflicting record.
    Valid,
    /// Identity present but incomplete.
    Partial,
    /// Damaged envelope, body, or sidecar.
    Corrupt,
    /// Two valid records disagree for one identity.
    Conflict,
    /// Not Atomic staging evidence.
    Unsupported,
    /// Bound to a different Heap.
    ForeignHeap,
}

/// One classified observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageFinding {
    /// Evidence kind.
    pub kind: StageEvidenceKind,
    /// Classifier class.
    pub class: StageEvidenceClass,
    /// Atomic identity when it could be parsed.
    pub atomic_id: Option<AtomicId>,
}

/// Accumulated honest findings for one catalogue open.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StageFindings {
    /// Observations in encounter order.
    pub records: Vec<StageFinding>,
}

impl StageFindings {
    fn push(
        &mut self,
        kind: StageEvidenceKind,
        class: StageEvidenceClass,
        atomic_id: Option<AtomicId>,
    ) {
        let finding = StageFinding {
            kind,
            class,
            atomic_id,
        };
        if !self.records.contains(&finding) {
            self.records.push(finding);
        }
    }

    /// Count of one class.
    pub fn count(&self, class: StageEvidenceClass) -> u32 {
        self.records.iter().filter(|r| r.class == class).count() as u32
    }
}

/// Record a physical coverage hole. Shared with independent examination.
pub fn classify_hole(findings: &mut StageFindings, _reason: &HoleReason) {
    findings.push(StageEvidenceKind::Hole, StageEvidenceClass::Corrupt, None);
}

/// Covered checkpoint media is missing or replaced (CR-ATMR6-002).
pub fn classify_coverage_loss(findings: &mut StageFindings) {
    findings.push(
        StageEvidenceKind::Coverage,
        StageEvidenceClass::Corrupt,
        None,
    );
}

/// Remove the global coverage-loss marker after an authenticated scrub.
pub(crate) fn clear_coverage_loss(findings: &mut StageFindings) {
    findings
        .records
        .retain(|finding| finding.kind != StageEvidenceKind::Coverage);
}

/// Classify and fold one verified frame. Shared with independent examination.
pub fn ingest_classified_frame(
    catalog: &mut StageCatalog,
    bound_heap: HeapId,
    frame: &DecodedFrame,
    findings: &mut StageFindings,
) {
    match examine_atomic_frame(frame) {
        Some(AtomicEvidenceClass::Valid(link)) => match link.role {
            AtomicFrameRole::Prepare => {
                if let Ok(prepare) = residiuum_atomics::decode_prepare(&frame.body) {
                    admit_prepare(catalog, bound_heap, prepare, true, findings);
                } else {
                    let id = AtomicId::from_bytes(link.atomic_id).ok();
                    block(catalog, id);
                    findings.push(StageEvidenceKind::Prepare, StageEvidenceClass::Corrupt, id);
                }
            }
            AtomicFrameRole::Member => {
                if let Ok(member) = residiuum_atomics::decode_member(&frame.body) {
                    admit_member(catalog, bound_heap, link.heap_id, member, findings);
                } else {
                    let id = AtomicId::from_bytes(link.atomic_id).ok();
                    block(catalog, id);
                    findings.push(StageEvidenceKind::Member, StageEvidenceClass::Corrupt, id);
                }
            }
            AtomicFrameRole::Commit => {
                findings.push(
                    StageEvidenceKind::Other,
                    StageEvidenceClass::Unsupported,
                    AtomicId::from_bytes(link.atomic_id).ok(),
                );
            }
        },
        Some(AtomicEvidenceClass::Partial { linkage, .. }) => {
            let id = linkage
                .as_ref()
                .and_then(|l| AtomicId::from_bytes(l.atomic_id).ok());
            block(catalog, id);
            findings.push(
                kind_from_role(linkage.as_ref().map(|l| l.role)),
                StageEvidenceClass::Partial,
                id,
            );
        }
        Some(AtomicEvidenceClass::Corrupt { .. }) => {
            findings.push(StageEvidenceKind::Other, StageEvidenceClass::Corrupt, None);
        }
        Some(AtomicEvidenceClass::Unsupported { .. }) => {
            findings.push(
                StageEvidenceKind::Other,
                StageEvidenceClass::Unsupported,
                None,
            );
        }
        None => ingest_sidecar(catalog, bound_heap, &frame.body, findings),
    }
}

/// After all frames: members/seal must agree with prepare; mismatches are not
/// installed as guessed truth.
pub fn finalize_catalog(
    catalog: &mut StageCatalog,
    bound_heap: HeapId,
    findings: &mut StageFindings,
) {
    let ids: Vec<AtomicId> = catalog.prepares.keys().copied().collect();
    for id in ids {
        if catalog.blocked.contains(&id) {
            continue;
        }
        let Some(prepare) = catalog.prepares.get(&id).cloned() else {
            continue;
        };
        if prepare.heap_id != bound_heap {
            catalog.blocked.insert(id);
            findings.push(
                StageEvidenceKind::Prepare,
                StageEvidenceClass::ForeignHeap,
                Some(id),
            );
            continue;
        }
        let members = catalog.members.get(&id).cloned().unwrap_or_default();
        if members.is_empty() {
            findings.push(
                StageEvidenceKind::Member,
                StageEvidenceClass::Partial,
                Some(id),
            );
        } else if !members_match_prepare(&prepare, &members) {
            catalog.blocked.insert(id);
            findings.push(
                StageEvidenceKind::Member,
                StageEvidenceClass::Conflict,
                Some(id),
            );
        }
        if let Some(root) = catalog.seals.get(&id).copied() {
            if root != prepare.content_root {
                catalog.seals.remove(&id);
                catalog.blocked.insert(id);
                findings.push(
                    StageEvidenceKind::Seal,
                    StageEvidenceClass::Conflict,
                    Some(id),
                );
            }
        }
    }
    sweep_orphans(catalog, findings);
}

fn sweep_orphans(catalog: &mut StageCatalog, findings: &mut StageFindings) {
    let mut orphans = Vec::new();
    orphans.extend(catalog.members.keys().copied());
    orphans.extend(catalog.payloads.keys().map(|(id, _)| *id));
    orphans.extend(catalog.seals.keys().copied());
    orphans.extend(catalog.chunk_plans.keys().map(|(id, _)| *id));
    orphans.extend(catalog.chunks.keys().map(|(id, _, _)| *id));
    orphans.sort();
    orphans.dedup();
    for id in orphans {
        if catalog.prepares.contains_key(&id) || catalog.blocked.contains(&id) {
            continue;
        }
        catalog.blocked.insert(id);
        if catalog.members.remove(&id).is_some() {
            findings.push(
                StageEvidenceKind::Member,
                StageEvidenceClass::Corrupt,
                Some(id),
            );
        }
        if catalog.payloads.keys().any(|(oid, _)| *oid == id) {
            catalog.payloads.retain(|(oid, _), _| *oid != id);
            findings.push(
                StageEvidenceKind::Payload,
                StageEvidenceClass::Corrupt,
                Some(id),
            );
        }
        if catalog.seals.remove(&id).is_some() {
            findings.push(
                StageEvidenceKind::Seal,
                StageEvidenceClass::Corrupt,
                Some(id),
            );
        }
        if catalog.chunk_plans.keys().any(|(oid, _)| *oid == id) {
            catalog.chunk_plans.retain(|(oid, _), _| *oid != id);
            findings.push(
                StageEvidenceKind::ChunkPlan,
                StageEvidenceClass::Corrupt,
                Some(id),
            );
        }
        if catalog.chunks.keys().any(|(oid, _, _)| *oid == id) {
            catalog.chunks.retain(|(oid, _, _), _| *oid != id);
            findings.push(
                StageEvidenceKind::ChunkBody,
                StageEvidenceClass::Corrupt,
                Some(id),
            );
        }
    }
}

fn ingest_sidecar(
    catalog: &mut StageCatalog,
    bound_heap: HeapId,
    body: &[u8],
    findings: &mut StageFindings,
) {
    match decode_stage_sidecar(body) {
        SidecarDecode::NotSidecar => {}
        SidecarDecode::Partial { kind } => {
            findings.push(sidecar_kind(kind), StageEvidenceClass::Partial, None);
        }
        SidecarDecode::Corrupt { kind, atomic_id } => {
            block(catalog, atomic_id);
            findings.push(sidecar_kind(kind), StageEvidenceClass::Corrupt, atomic_id);
        }
        SidecarDecode::Prepare(prepare) => {
            admit_prepare(catalog, bound_heap, *prepare, false, findings);
        }
        SidecarDecode::Payload {
            atomic_id,
            ordinal,
            payload,
        } => admit_payload(catalog, atomic_id, ordinal, payload, findings),
        SidecarDecode::Seal {
            atomic_id,
            content_root,
        } => admit_seal(catalog, atomic_id, content_root, findings),
        SidecarDecode::ChunkPlan {
            atomic_id,
            ordinal,
            plan,
        } => admit_chunk_plan(catalog, atomic_id, ordinal, plan, findings),
        SidecarDecode::ChunkBody {
            atomic_id,
            ordinal,
            index,
            body,
        } => admit_chunk_body(catalog, atomic_id, ordinal, index, body, findings),
    }
}

fn admit_prepare(
    catalog: &mut StageCatalog,
    bound_heap: HeapId,
    prepare: AtomicPrepare,
    from_batch: bool,
    findings: &mut StageFindings,
) {
    let id = prepare.atomic_id;
    if prepare.heap_id != bound_heap {
        findings.push(
            StageEvidenceKind::Prepare,
            StageEvidenceClass::ForeignHeap,
            Some(id),
        );
        return;
    }
    if catalog.blocked.contains(&id) {
        findings.push(
            StageEvidenceKind::Prepare,
            StageEvidenceClass::Conflict,
            Some(id),
        );
        return;
    }
    match catalog.prepares.get(&id) {
        None => {
            catalog.prepares.insert(id, prepare);
            if from_batch {
                catalog.prepare_batch.insert(id);
            }
            if !catalog.prepare_seen.contains(&id) {
                catalog.prepare_seen.push(id);
            }
            findings.push(
                StageEvidenceKind::Prepare,
                StageEvidenceClass::Valid,
                Some(id),
            );
        }
        Some(existing) if existing == &prepare => {
            if from_batch {
                catalog.prepare_batch.insert(id);
            }
            findings.push(
                StageEvidenceKind::Prepare,
                StageEvidenceClass::Valid,
                Some(id),
            );
        }
        Some(_) => {
            catalog.blocked.insert(id);
            findings.push(
                StageEvidenceKind::Prepare,
                StageEvidenceClass::Conflict,
                Some(id),
            );
        }
    }
}

fn admit_member(
    catalog: &mut StageCatalog,
    bound_heap: HeapId,
    frame_heap: Option<[u8; 16]>,
    member: AtomicMember,
    findings: &mut StageFindings,
) {
    let id = member.atomic_id;
    if let Some(h) = frame_heap {
        if h != *bound_heap.as_bytes() {
            findings.push(
                StageEvidenceKind::Member,
                StageEvidenceClass::ForeignHeap,
                Some(id),
            );
            return;
        }
    }
    if catalog.blocked.contains(&id) {
        findings.push(
            StageEvidenceKind::Member,
            StageEvidenceClass::Conflict,
            Some(id),
        );
        return;
    }
    let slot = catalog.members.entry(id).or_default();
    match slot.iter().find(|m| m.ordinal == member.ordinal) {
        None => {
            slot.push(member);
            findings.push(
                StageEvidenceKind::Member,
                StageEvidenceClass::Valid,
                Some(id),
            );
        }
        Some(existing) if existing == &member => {
            findings.push(
                StageEvidenceKind::Member,
                StageEvidenceClass::Valid,
                Some(id),
            );
        }
        Some(_) => {
            catalog.blocked.insert(id);
            findings.push(
                StageEvidenceKind::Member,
                StageEvidenceClass::Conflict,
                Some(id),
            );
        }
    }
}

fn admit_payload(
    catalog: &mut StageCatalog,
    atomic_id: AtomicId,
    ordinal: u32,
    payload: Vec<u8>,
    findings: &mut StageFindings,
) {
    if catalog.blocked.contains(&atomic_id) {
        findings.push(
            StageEvidenceKind::Payload,
            StageEvidenceClass::Conflict,
            Some(atomic_id),
        );
        return;
    }
    match catalog.payloads.get(&(atomic_id, ordinal)) {
        None => {
            catalog.payloads.insert((atomic_id, ordinal), payload);
            findings.push(
                StageEvidenceKind::Payload,
                StageEvidenceClass::Valid,
                Some(atomic_id),
            );
        }
        Some(existing) if existing == &payload => {
            findings.push(
                StageEvidenceKind::Payload,
                StageEvidenceClass::Valid,
                Some(atomic_id),
            );
        }
        Some(_) => {
            catalog.blocked.insert(atomic_id);
            findings.push(
                StageEvidenceKind::Payload,
                StageEvidenceClass::Conflict,
                Some(atomic_id),
            );
        }
    }
}

fn admit_seal(
    catalog: &mut StageCatalog,
    atomic_id: AtomicId,
    content_root: residiuum_atomics::ContentRoot,
    findings: &mut StageFindings,
) {
    if catalog.blocked.contains(&atomic_id) {
        findings.push(
            StageEvidenceKind::Seal,
            StageEvidenceClass::Conflict,
            Some(atomic_id),
        );
        return;
    }
    match catalog.seals.get(&atomic_id) {
        None => {
            catalog.seals.insert(atomic_id, content_root);
            findings.push(
                StageEvidenceKind::Seal,
                StageEvidenceClass::Valid,
                Some(atomic_id),
            );
        }
        Some(existing) if existing == &content_root => {
            findings.push(
                StageEvidenceKind::Seal,
                StageEvidenceClass::Valid,
                Some(atomic_id),
            );
        }
        Some(_) => {
            catalog.seals.remove(&atomic_id);
            catalog.blocked.insert(atomic_id);
            findings.push(
                StageEvidenceKind::Seal,
                StageEvidenceClass::Conflict,
                Some(atomic_id),
            );
        }
    }
}

fn block(catalog: &mut StageCatalog, id: Option<AtomicId>) {
    if let Some(id) = id {
        catalog.blocked.insert(id);
    }
}

fn admit_chunk_plan(
    catalog: &mut StageCatalog,
    atomic_id: AtomicId,
    ordinal: u32,
    plan: ChunkPlan,
    findings: &mut StageFindings,
) {
    if catalog.blocked.contains(&atomic_id) {
        findings.push(
            StageEvidenceKind::ChunkPlan,
            StageEvidenceClass::Conflict,
            Some(atomic_id),
        );
        return;
    }
    match catalog.chunk_plans.get(&(atomic_id, ordinal)) {
        None => {
            catalog.chunk_plans.insert((atomic_id, ordinal), plan);
            findings.push(
                StageEvidenceKind::ChunkPlan,
                StageEvidenceClass::Valid,
                Some(atomic_id),
            );
        }
        Some(existing) if existing == &plan => {
            findings.push(
                StageEvidenceKind::ChunkPlan,
                StageEvidenceClass::Valid,
                Some(atomic_id),
            );
        }
        Some(_) => {
            catalog.blocked.insert(atomic_id);
            findings.push(
                StageEvidenceKind::ChunkPlan,
                StageEvidenceClass::Conflict,
                Some(atomic_id),
            );
        }
    }
}

fn admit_chunk_body(
    catalog: &mut StageCatalog,
    atomic_id: AtomicId,
    ordinal: u32,
    index: u32,
    body: Vec<u8>,
    findings: &mut StageFindings,
) {
    if catalog.blocked.contains(&atomic_id) {
        findings.push(
            StageEvidenceKind::ChunkBody,
            StageEvidenceClass::Conflict,
            Some(atomic_id),
        );
        return;
    }
    match catalog.chunks.get(&(atomic_id, ordinal, index)) {
        None => {
            catalog.chunks.insert((atomic_id, ordinal, index), body);
            findings.push(
                StageEvidenceKind::ChunkBody,
                StageEvidenceClass::Valid,
                Some(atomic_id),
            );
        }
        Some(existing) if existing == &body => {
            findings.push(
                StageEvidenceKind::ChunkBody,
                StageEvidenceClass::Valid,
                Some(atomic_id),
            );
        }
        Some(_) => {
            catalog.blocked.insert(atomic_id);
            findings.push(
                StageEvidenceKind::ChunkBody,
                StageEvidenceClass::Conflict,
                Some(atomic_id),
            );
        }
    }
}

fn sidecar_kind(kind: SidecarKind) -> StageEvidenceKind {
    match kind {
        SidecarKind::Prepare => StageEvidenceKind::Prepare,
        SidecarKind::Payload => StageEvidenceKind::Payload,
        SidecarKind::Seal => StageEvidenceKind::Seal,
        SidecarKind::ChunkPlan => StageEvidenceKind::ChunkPlan,
        SidecarKind::ChunkBody => StageEvidenceKind::ChunkBody,
    }
}

pub(crate) fn encode_finding_kind(kind: StageEvidenceKind) -> u8 {
    match kind {
        StageEvidenceKind::Coverage => 8,
        StageEvidenceKind::Hole => 0,
        StageEvidenceKind::Prepare => 1,
        StageEvidenceKind::Member => 2,
        StageEvidenceKind::Payload => 3,
        StageEvidenceKind::Seal => 4,
        StageEvidenceKind::ChunkPlan => 5,
        StageEvidenceKind::ChunkBody => 6,
        StageEvidenceKind::Other => 7,
    }
}

pub(crate) fn decode_finding_kind(byte: u8) -> Option<StageEvidenceKind> {
    Some(match byte {
        8 => StageEvidenceKind::Coverage,
        0 => StageEvidenceKind::Hole,
        1 => StageEvidenceKind::Prepare,
        2 => StageEvidenceKind::Member,
        3 => StageEvidenceKind::Payload,
        4 => StageEvidenceKind::Seal,
        5 => StageEvidenceKind::ChunkPlan,
        6 => StageEvidenceKind::ChunkBody,
        7 => StageEvidenceKind::Other,
        _ => return None,
    })
}

pub(crate) fn encode_finding_class(class: StageEvidenceClass) -> u8 {
    match class {
        StageEvidenceClass::Valid => 0,
        StageEvidenceClass::Partial => 1,
        StageEvidenceClass::Corrupt => 2,
        StageEvidenceClass::Conflict => 3,
        StageEvidenceClass::Unsupported => 4,
        StageEvidenceClass::ForeignHeap => 5,
    }
}

pub(crate) fn decode_finding_class(byte: u8) -> Option<StageEvidenceClass> {
    Some(match byte {
        0 => StageEvidenceClass::Valid,
        1 => StageEvidenceClass::Partial,
        2 => StageEvidenceClass::Corrupt,
        3 => StageEvidenceClass::Conflict,
        4 => StageEvidenceClass::Unsupported,
        5 => StageEvidenceClass::ForeignHeap,
        _ => return None,
    })
}

fn kind_from_role(role: Option<AtomicFrameRole>) -> StageEvidenceKind {
    match role {
        Some(AtomicFrameRole::Prepare) => StageEvidenceKind::Prepare,
        Some(AtomicFrameRole::Member) => StageEvidenceKind::Member,
        _ => StageEvidenceKind::Other,
    }
}
