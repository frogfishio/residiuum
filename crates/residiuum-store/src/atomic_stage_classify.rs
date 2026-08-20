//! Honest store Atomic classifier (CR-ATMR5-002 / CR-ATMR6-002).
//!
//! Recovery and examination share this path. Damaged, conflicting, foreign,
//! and incomplete records are reported. They are never last-wins, first-wins,
//! or translated into a reusable unused identity. Seal conflicts and
//! pre-prepare orphans block the named identity. Findings are persisted with
//! the catalogue so an ordinary reopen cannot forget them.

use crate::atomic_stage_media::{
    decode_stage_sidecar, stage_key, SidecarDecode, SidecarKind, StageAtomicKey, StageCatalog,
};
use residiuum_atomics::{
    members_match_prepare, ordered_member_manifest_root, prepare_hash, AtomicDecision, AtomicId,
    AtomicMember, AtomicPrepare, ChunkPlan, DecisionCode, HeapId,
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
    /// Durable BatchCommit decision.
    Decision,
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
    frame: &DecodedFrame,
    findings: &mut StageFindings,
) {
    match examine_atomic_frame(frame) {
        Some(AtomicEvidenceClass::Valid(link)) => match link.role {
            AtomicFrameRole::Prepare => {
                if let Ok(prepare) = residiuum_atomics::decode_prepare(&frame.body) {
                    admit_prepare(catalog, prepare, true, findings);
                } else {
                    let id = AtomicId::from_bytes(link.atomic_id).ok();
                    block(
                        catalog,
                        link.heap_id
                            .and_then(|bytes| HeapId::from_bytes(bytes).ok())
                            .zip(id),
                    );
                    findings.push(StageEvidenceKind::Prepare, StageEvidenceClass::Corrupt, id);
                }
            }
            AtomicFrameRole::Member => {
                if let Ok(member) = residiuum_atomics::decode_member(&frame.body) {
                    admit_member(catalog, link.heap_id, member, findings);
                } else {
                    let id = AtomicId::from_bytes(link.atomic_id).ok();
                    block(
                        catalog,
                        link.heap_id
                            .and_then(|bytes| HeapId::from_bytes(bytes).ok())
                            .zip(id),
                    );
                    findings.push(StageEvidenceKind::Member, StageEvidenceClass::Corrupt, id);
                }
            }
            AtomicFrameRole::Commit => {
                if let Ok(decision) = residiuum_atomics::decode_decision(&frame.body) {
                    admit_decision(catalog, link.heap_id, link.content_root, decision, findings);
                } else {
                    let id = AtomicId::from_bytes(link.atomic_id).ok();
                    block(
                        catalog,
                        link.heap_id
                            .and_then(|bytes| HeapId::from_bytes(bytes).ok())
                            .zip(id),
                    );
                    findings.push(StageEvidenceKind::Decision, StageEvidenceClass::Corrupt, id);
                }
            }
        },
        Some(AtomicEvidenceClass::Partial { linkage, .. }) => {
            let id = linkage
                .as_ref()
                .and_then(|l| AtomicId::from_bytes(l.atomic_id).ok());
            let key = linkage
                .as_ref()
                .and_then(|l| l.heap_id)
                .and_then(|bytes| HeapId::from_bytes(bytes).ok())
                .zip(id);
            block(catalog, key);
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
        None => ingest_sidecar(catalog, &frame.body, findings),
    }
}

/// After all frames: members/seal must agree with prepare; mismatches are not
/// installed as guessed truth.
pub fn finalize_catalog(catalog: &mut StageCatalog, findings: &mut StageFindings) {
    let keys: Vec<StageAtomicKey> = catalog.prepares.keys().copied().collect();
    for key in keys {
        let id = key.1;
        if catalog.blocked.contains(&key) {
            continue;
        }
        let Some(prepare) = catalog.prepares.get(&key).cloned() else {
            continue;
        };
        catalog.intended_members.insert(key, prepare.member_count);
        let members = catalog.members.get(&key).cloned().unwrap_or_default();
        let terminal_not_committed = catalog
            .decisions
            .get(&key)
            .is_some_and(|decision| decision.decision == DecisionCode::NotCommitted);
        if members.is_empty() && !terminal_not_committed {
            findings.push(
                StageEvidenceKind::Member,
                StageEvidenceClass::Partial,
                Some(id),
            );
        } else if !terminal_not_committed && !members_match_prepare(&prepare, &members) {
            catalog.blocked.insert(key);
            findings.push(
                StageEvidenceKind::Member,
                StageEvidenceClass::Conflict,
                Some(id),
            );
        } else if !members.is_empty() {
            // The prepare's ordered manifest root authenticates the complete
            // recovered member vector. This recovers the count when the
            // derived checkpoint was intentionally left behind by the
            // two-boundary whole-plan path.
            catalog.intended_members.insert(key, prepare.member_count);
        }
        if let Some(root) = catalog.seals.get(&key).copied() {
            if root != prepare.content_root {
                catalog.seals.remove(&key);
                catalog.blocked.insert(key);
                findings.push(
                    StageEvidenceKind::Seal,
                    StageEvidenceClass::Conflict,
                    Some(id),
                );
            }
        }
        if let Some(decision) = catalog.decisions.get(&key).cloned() {
            let prepare_ok = prepare_hash(&prepare)
                .ok()
                .is_some_and(|hash| hash == decision.prepare_hash);
            let root_ok = decision.member_root == prepare.ordered_member_manifest_root;
            let count_ok = prepare.member_count == decision.member_count;
            let committed_material_ok = decision.decision != DecisionCode::Committed
                || (ordered_member_manifest_root(prepare.heap_id, &members)
                    .ok()
                    .is_some_and(|root| root == decision.member_root)
                    && usize::try_from(decision.member_count)
                        .ok()
                        .is_some_and(|count| count == members.len())
                    && catalog.seals.get(&key) == Some(&prepare.content_root)
                    && catalog
                        .order_frontiers
                        .get(&key)
                        .is_some_and(|frontiers| !frontiers.is_empty()));
            if !prepare_ok || !root_ok || !count_ok || !committed_material_ok {
                catalog.blocked.insert(key);
                findings.push(
                    StageEvidenceKind::Decision,
                    StageEvidenceClass::Conflict,
                    Some(id),
                );
            }
        }
    }
    let mut positions = std::collections::BTreeMap::new();
    for (key, decision) in &catalog.decisions {
        let id = key.1;
        if decision.decision != DecisionCode::Committed || catalog.blocked.contains(key) {
            continue;
        }
        if let Some(position) = decision.commit_position {
            let heap_id = key.0;
            if let Some(other) = positions.insert((heap_id, position), *key) {
                catalog.blocked.insert(other);
                catalog.blocked.insert(*key);
                findings.push(
                    StageEvidenceKind::Decision,
                    StageEvidenceClass::Conflict,
                    Some(other.1),
                );
                findings.push(
                    StageEvidenceKind::Decision,
                    StageEvidenceClass::Conflict,
                    Some(id),
                );
            }
        }
    }
    catalog.reconstruct_commit_next();
    sweep_orphans(catalog, findings);
}

fn admit_decision(
    catalog: &mut StageCatalog,
    envelope_heap: Option<[u8; 16]>,
    envelope_content_root: [u8; 32],
    decision: AtomicDecision,
    findings: &mut StageFindings,
) {
    let id = decision.atomic_id;
    let Some(envelope_heap) = envelope_heap.and_then(|bytes| HeapId::from_bytes(bytes).ok()) else {
        findings.push(
            StageEvidenceKind::Decision,
            StageEvidenceClass::Corrupt,
            Some(id),
        );
        return;
    };
    let key = stage_key(envelope_heap, id);
    if catalog
        .prepares
        .get(&key)
        .is_some_and(|prepare| prepare.content_root.to_bytes() != envelope_content_root)
    {
        catalog.blocked.insert(key);
        findings.push(
            StageEvidenceKind::Decision,
            StageEvidenceClass::Conflict,
            Some(id),
        );
        return;
    }
    if catalog.blocked.contains(&key) {
        findings.push(
            StageEvidenceKind::Decision,
            StageEvidenceClass::Conflict,
            Some(id),
        );
        return;
    }
    match catalog.decisions.get(&key) {
        None => {
            catalog
                .intended_members
                .entry(key)
                .or_insert(decision.member_count);
            catalog.decisions.insert(key, decision);
            findings.push(
                StageEvidenceKind::Decision,
                StageEvidenceClass::Valid,
                Some(id),
            );
        }
        Some(existing) if existing == &decision => findings.push(
            StageEvidenceKind::Decision,
            StageEvidenceClass::Valid,
            Some(id),
        ),
        Some(_) => {
            catalog.blocked.insert(key);
            findings.push(
                StageEvidenceKind::Decision,
                StageEvidenceClass::Conflict,
                Some(id),
            );
        }
    }
}

fn sweep_orphans(catalog: &mut StageCatalog, findings: &mut StageFindings) {
    let mut orphans = Vec::new();
    orphans.extend(catalog.members.keys().copied());
    orphans.extend(catalog.payloads.keys().map(|(heap, id, _)| (*heap, *id)));
    orphans.extend(catalog.seals.keys().copied());
    orphans.extend(catalog.chunk_plans.keys().map(|(heap, id, _)| (*heap, *id)));
    orphans.extend(catalog.chunks.keys().map(|(heap, id, _, _)| (*heap, *id)));
    orphans.extend(catalog.decisions.keys().copied());
    orphans.sort();
    orphans.dedup();
    for key in orphans {
        let id = key.1;
        if catalog.prepares.contains_key(&key) || catalog.blocked.contains(&key) {
            continue;
        }
        catalog.blocked.insert(key);
        if catalog.members.remove(&key).is_some() {
            findings.push(
                StageEvidenceKind::Member,
                StageEvidenceClass::Corrupt,
                Some(id),
            );
        }
        if catalog
            .payloads
            .keys()
            .any(|(heap, oid, _)| (*heap, *oid) == key)
        {
            catalog
                .payloads
                .retain(|(heap, oid, _), _| (*heap, *oid) != key);
            findings.push(
                StageEvidenceKind::Payload,
                StageEvidenceClass::Corrupt,
                Some(id),
            );
        }
        if catalog.seals.remove(&key).is_some() {
            findings.push(
                StageEvidenceKind::Seal,
                StageEvidenceClass::Corrupt,
                Some(id),
            );
        }
        if catalog
            .chunk_plans
            .keys()
            .any(|(heap, oid, _)| (*heap, *oid) == key)
        {
            catalog
                .chunk_plans
                .retain(|(heap, oid, _), _| (*heap, *oid) != key);
            findings.push(
                StageEvidenceKind::ChunkPlan,
                StageEvidenceClass::Corrupt,
                Some(id),
            );
        }
        if catalog
            .chunks
            .keys()
            .any(|(heap, oid, _, _)| (*heap, *oid) == key)
        {
            catalog
                .chunks
                .retain(|(heap, oid, _, _), _| (*heap, *oid) != key);
            findings.push(
                StageEvidenceKind::ChunkBody,
                StageEvidenceClass::Corrupt,
                Some(id),
            );
        }
        // Retain an orphan decision as damaged lifetime evidence. In
        // particular, never forget a named commit position and later reuse it.
        if catalog.decisions.contains_key(&key) {
            findings.push(
                StageEvidenceKind::Decision,
                StageEvidenceClass::Corrupt,
                Some(id),
            );
        }
    }
}

fn ingest_sidecar(catalog: &mut StageCatalog, body: &[u8], findings: &mut StageFindings) {
    match decode_stage_sidecar(body) {
        SidecarDecode::NotSidecar => {}
        SidecarDecode::Partial { kind } => {
            findings.push(sidecar_kind(kind), StageEvidenceClass::Partial, None);
        }
        SidecarDecode::Corrupt { kind, atomic_id } => {
            block(catalog, sidecar_heap_id(body).zip(atomic_id));
            findings.push(sidecar_kind(kind), StageEvidenceClass::Corrupt, atomic_id);
        }
        SidecarDecode::Prepare(prepare) => {
            admit_prepare(catalog, *prepare, false, findings);
        }
        SidecarDecode::Payload {
            heap_id,
            atomic_id,
            ordinal,
            payload,
        } => admit_payload(catalog, heap_id, atomic_id, ordinal, payload, findings),
        SidecarDecode::Seal {
            heap_id,
            atomic_id,
            content_root,
        } => admit_seal(catalog, heap_id, atomic_id, content_root, findings),
        SidecarDecode::ChunkPlan {
            heap_id,
            atomic_id,
            ordinal,
            plan,
        } => admit_chunk_plan(catalog, heap_id, atomic_id, ordinal, plan, findings),
        SidecarDecode::ChunkBody {
            heap_id,
            atomic_id,
            ordinal,
            index,
            body,
        } => admit_chunk_body(catalog, heap_id, atomic_id, ordinal, index, body, findings),
        SidecarDecode::OrderFrontier {
            heap_id,
            atomic_id,
            frontiers,
        } => {
            let key = stage_key(heap_id, atomic_id);
            if let Some(existing) = catalog.order_frontiers.get(&key) {
                if existing != &frontiers {
                    catalog.blocked.insert(key);
                    findings.push(
                        StageEvidenceKind::Decision,
                        StageEvidenceClass::Conflict,
                        Some(atomic_id),
                    );
                }
            } else {
                catalog.order_frontiers.insert(key, frontiers);
            }
        }
    }
}

fn sidecar_heap_id(body: &[u8]) -> Option<HeapId> {
    for magic in [
        b"ATPAY1".as_slice(),
        b"ATSEAL1",
        b"ATMAP1",
        b"ATCHK1",
        b"ATORD1",
    ] {
        if body.starts_with(magic) && body.len() >= magic.len() + 16 {
            return HeapId::from_bytes(body[magic.len()..magic.len() + 16].try_into().ok()?).ok();
        }
    }
    None
}

fn admit_prepare(
    catalog: &mut StageCatalog,
    prepare: AtomicPrepare,
    from_batch: bool,
    findings: &mut StageFindings,
) {
    let id = prepare.atomic_id;
    let key = stage_key(prepare.heap_id, id);
    if catalog.blocked.contains(&key) {
        findings.push(
            StageEvidenceKind::Prepare,
            StageEvidenceClass::Conflict,
            Some(id),
        );
        return;
    }
    match catalog.prepares.get(&key) {
        None => {
            catalog.prepares.insert(key, prepare);
            if from_batch {
                catalog.prepare_batch.insert(key);
            }
            if !catalog.prepare_seen.contains(&key) {
                catalog.prepare_seen.push(key);
            }
            findings.push(
                StageEvidenceKind::Prepare,
                StageEvidenceClass::Valid,
                Some(id),
            );
        }
        Some(existing) if existing == &prepare => {
            if from_batch {
                catalog.prepare_batch.insert(key);
            }
            findings.push(
                StageEvidenceKind::Prepare,
                StageEvidenceClass::Valid,
                Some(id),
            );
        }
        Some(_) => {
            catalog.blocked.insert(key);
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
    frame_heap: Option<[u8; 16]>,
    member: AtomicMember,
    findings: &mut StageFindings,
) {
    let id = member.atomic_id;
    let Some(frame_heap) = frame_heap.and_then(|bytes| HeapId::from_bytes(bytes).ok()) else {
        findings.push(
            StageEvidenceKind::Member,
            StageEvidenceClass::Corrupt,
            Some(id),
        );
        return;
    };
    let key = stage_key(frame_heap, id);
    if catalog.blocked.contains(&key) {
        findings.push(
            StageEvidenceKind::Member,
            StageEvidenceClass::Conflict,
            Some(id),
        );
        return;
    }
    let slot = catalog.members.entry(key).or_default();
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
            catalog.blocked.insert(key);
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
    heap_id: HeapId,
    atomic_id: AtomicId,
    ordinal: u32,
    payload: Vec<u8>,
    findings: &mut StageFindings,
) {
    let key = stage_key(heap_id, atomic_id);
    if catalog.blocked.contains(&key) {
        findings.push(
            StageEvidenceKind::Payload,
            StageEvidenceClass::Conflict,
            Some(atomic_id),
        );
        return;
    }
    match catalog.payloads.get(&(heap_id, atomic_id, ordinal)) {
        None => {
            catalog
                .payloads
                .insert((heap_id, atomic_id, ordinal), payload);
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
            catalog.blocked.insert(key);
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
    heap_id: HeapId,
    atomic_id: AtomicId,
    content_root: residiuum_atomics::ContentRoot,
    findings: &mut StageFindings,
) {
    let key = stage_key(heap_id, atomic_id);
    if catalog.blocked.contains(&key) {
        findings.push(
            StageEvidenceKind::Seal,
            StageEvidenceClass::Conflict,
            Some(atomic_id),
        );
        return;
    }
    match catalog.seals.get(&key) {
        None => {
            catalog.seals.insert(key, content_root);
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
            catalog.seals.remove(&key);
            catalog.blocked.insert(key);
            findings.push(
                StageEvidenceKind::Seal,
                StageEvidenceClass::Conflict,
                Some(atomic_id),
            );
        }
    }
}

fn block(catalog: &mut StageCatalog, key: Option<StageAtomicKey>) {
    if let Some(key) = key {
        catalog.blocked.insert(key);
    }
}

fn admit_chunk_plan(
    catalog: &mut StageCatalog,
    heap_id: HeapId,
    atomic_id: AtomicId,
    ordinal: u32,
    plan: ChunkPlan,
    findings: &mut StageFindings,
) {
    let key = stage_key(heap_id, atomic_id);
    if catalog.blocked.contains(&key) {
        findings.push(
            StageEvidenceKind::ChunkPlan,
            StageEvidenceClass::Conflict,
            Some(atomic_id),
        );
        return;
    }
    match catalog.chunk_plans.get(&(heap_id, atomic_id, ordinal)) {
        None => {
            catalog
                .chunk_plans
                .insert((heap_id, atomic_id, ordinal), plan);
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
            catalog.blocked.insert(key);
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
    heap_id: HeapId,
    atomic_id: AtomicId,
    ordinal: u32,
    index: u32,
    body: Vec<u8>,
    findings: &mut StageFindings,
) {
    let key = stage_key(heap_id, atomic_id);
    if catalog.blocked.contains(&key) {
        findings.push(
            StageEvidenceKind::ChunkBody,
            StageEvidenceClass::Conflict,
            Some(atomic_id),
        );
        return;
    }
    match catalog.chunks.get(&(heap_id, atomic_id, ordinal, index)) {
        None => {
            catalog
                .chunks
                .insert((heap_id, atomic_id, ordinal, index), body);
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
            catalog.blocked.insert(key);
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
        SidecarKind::OrderFrontier => StageEvidenceKind::Decision,
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
        StageEvidenceKind::Decision => 9,
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
        9 => StageEvidenceKind::Decision,
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
        Some(AtomicFrameRole::Commit) => StageEvidenceKind::Decision,
        _ => StageEvidenceKind::Other,
    }
}
