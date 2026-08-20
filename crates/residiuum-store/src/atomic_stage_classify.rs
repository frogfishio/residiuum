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
    decision_hash, members_match_prepare, ordered_member_manifest_root, prepare_hash,
    AtomicDecision, AtomicId, AtomicMember, AtomicPrepare, ChunkPlan, DecisionCode, HeapId,
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
    /// ATORD1 order/publication frontier witness.
    OrderFrontier,
    /// ATTOMB2 lifetime decision authority.
    Tombstone,
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
    /// Heap identity when the damaged/conflicting evidence was attributable.
    /// `None` means the damage could not be safely bound and therefore
    /// degrades global coverage rather than a guessed Heap.
    pub heap_id: Option<HeapId>,
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
        self.push_bound(kind, class, None, atomic_id);
    }

    fn push_key(
        &mut self,
        kind: StageEvidenceKind,
        class: StageEvidenceClass,
        key: StageAtomicKey,
    ) {
        self.push_bound(kind, class, Some(key.0), Some(key.1));
    }

    fn push_bound(
        &mut self,
        kind: StageEvidenceKind,
        class: StageEvidenceClass,
        heap_id: Option<HeapId>,
        atomic_id: Option<AtomicId>,
    ) {
        let finding = StageFinding {
            kind,
            class,
            heap_id,
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
                    let heap_id = link
                        .heap_id
                        .and_then(|bytes| HeapId::from_bytes(bytes).ok());
                    block(catalog, heap_id.zip(id));
                    findings.push_bound(
                        StageEvidenceKind::Prepare,
                        StageEvidenceClass::Corrupt,
                        heap_id,
                        id,
                    );
                }
            }
            AtomicFrameRole::Member => {
                if let Ok(member) = residiuum_atomics::decode_member(&frame.body) {
                    admit_member(catalog, link.heap_id, member, findings);
                } else {
                    let id = AtomicId::from_bytes(link.atomic_id).ok();
                    let heap_id = link
                        .heap_id
                        .and_then(|bytes| HeapId::from_bytes(bytes).ok());
                    block(catalog, heap_id.zip(id));
                    findings.push_bound(
                        StageEvidenceKind::Member,
                        StageEvidenceClass::Corrupt,
                        heap_id,
                        id,
                    );
                }
            }
            AtomicFrameRole::Commit => {
                if let Ok(decision) = residiuum_atomics::decode_decision(&frame.body) {
                    admit_decision(catalog, link.heap_id, link.content_root, decision, findings);
                } else {
                    let id = AtomicId::from_bytes(link.atomic_id).ok();
                    let heap_id = link
                        .heap_id
                        .and_then(|bytes| HeapId::from_bytes(bytes).ok());
                    block(catalog, heap_id.zip(id));
                    findings.push_bound(
                        StageEvidenceKind::Decision,
                        StageEvidenceClass::Corrupt,
                        heap_id,
                        id,
                    );
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
            findings.push_bound(
                kind_from_role(linkage.as_ref().map(|l| l.role)),
                StageEvidenceClass::Partial,
                key.map(|candidate| candidate.0),
                id,
            );
        }
        Some(AtomicEvidenceClass::AttributedCorrupt { linkage, .. }) => {
            let id = AtomicId::from_bytes(linkage.atomic_id).ok();
            let heap_id = linkage
                .heap_id
                .and_then(|bytes| HeapId::from_bytes(bytes).ok());
            block(catalog, heap_id.zip(id));
            findings.push_bound(
                kind_from_role(Some(linkage.role)),
                StageEvidenceClass::Corrupt,
                heap_id,
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
            findings.push_key(StageEvidenceKind::Member, StageEvidenceClass::Partial, key);
        } else if !terminal_not_committed
            && members.len() == prepare.member_count as usize
            && !members_match_prepare(&prepare, &members)
        {
            catalog.blocked.insert(key);
            findings.push_key(StageEvidenceKind::Member, StageEvidenceClass::Conflict, key);
        } else if !terminal_not_committed && members.len() != prepare.member_count as usize {
            findings.push_key(StageEvidenceKind::Member, StageEvidenceClass::Partial, key);
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
                findings.push_key(StageEvidenceKind::Seal, StageEvidenceClass::Conflict, key);
            }
        }
        // Payload and chunk hashes are material truth for every issued
        // Atomic, not only committed decisions. A structurally valid sidecar
        // with the wrong bytes is conflicting material.
        for member in &members {
            if let Some(payload) = catalog.payloads.get(&(key.0, key.1, member.ordinal)) {
                let payload_matches = match member.after_content_hash {
                    Some(expected) => expected == *blake3::hash(payload).as_bytes(),
                    None => {
                        member.member_kind == residiuum_atomics::MutationKind::Delete
                            && payload.is_empty()
                    }
                };
                if !payload_matches {
                    catalog.blocked.insert(key);
                    findings.push_key(
                        StageEvidenceKind::Payload,
                        StageEvidenceClass::Conflict,
                        key,
                    );
                }
            }
            if let Some(plan) = catalog.chunk_plans.get(&(key.0, key.1, member.ordinal)) {
                let mut assembled = Vec::new();
                let mut all_present = true;
                for (index, expected) in plan.chunk_hashes.iter().enumerate() {
                    if let Some(body) =
                        catalog
                            .chunks
                            .get(&(key.0, key.1, member.ordinal, index as u32))
                    {
                        assembled.extend_from_slice(body);
                        if blake3::hash(body).as_bytes() != expected {
                            catalog.blocked.insert(key);
                            findings.push_key(
                                StageEvidenceKind::ChunkBody,
                                StageEvidenceClass::Conflict,
                                key,
                            );
                        }
                    } else {
                        all_present = false;
                    }
                }
                if all_present
                    && member
                        .after_content_hash
                        .is_some_and(|expected| expected != *blake3::hash(&assembled).as_bytes())
                {
                    catalog.blocked.insert(key);
                    findings.push_key(
                        StageEvidenceKind::ChunkPlan,
                        StageEvidenceClass::Conflict,
                        key,
                    );
                }
            }
        }
        if let Some(decision) = catalog.decisions.get(&key).cloned() {
            let prepare_ok = prepare_hash(&prepare)
                .ok()
                .is_some_and(|hash| hash == decision.prepare_hash);
            let root_ok = decision.member_root == prepare.ordered_member_manifest_root;
            let count_ok = prepare.member_count == decision.member_count;
            if !prepare_ok || !root_ok || !count_ok {
                catalog.blocked.insert(key);
                findings.push_key(
                    StageEvidenceKind::Prepare,
                    StageEvidenceClass::Conflict,
                    key,
                );
            }
            if decision.decision == DecisionCode::Committed {
                let full_member_set = usize::try_from(decision.member_count)
                    .ok()
                    .is_some_and(|count| count == members.len());
                if full_member_set
                    && ordered_member_manifest_root(prepare.heap_id, &members)
                        .ok()
                        .is_none_or(|root| root != decision.member_root)
                {
                    catalog.blocked.insert(key);
                    findings.push_key(StageEvidenceKind::Member, StageEvidenceClass::Conflict, key);
                }
                for member in &members {
                    if !catalog.has_payload(key, member.ordinal)
                        && !catalog
                            .payload_refs
                            .contains_key(&(key.0, key.1, member.ordinal))
                        && !crate::atomic_stage_status::material_complete_for_member(
                            catalog,
                            key,
                            member.ordinal,
                        )
                    {
                        findings.push_key(
                            StageEvidenceKind::Payload,
                            StageEvidenceClass::Partial,
                            key,
                        );
                    }
                }
                if catalog.seals.get(&key) != Some(&prepare.content_root) {
                    findings.push_key(StageEvidenceKind::Seal, StageEvidenceClass::Partial, key);
                }
                if catalog
                    .order_frontiers
                    .get(&key)
                    .is_none_or(|frontiers| frontiers.is_empty())
                {
                    findings.push_key(
                        StageEvidenceKind::OrderFrontier,
                        StageEvidenceClass::Partial,
                        key,
                    );
                }
            }
            if let Some(retained) = catalog.tombstones.get(&key) {
                let hash_ok = decision_hash(&decision)
                    .ok()
                    .is_some_and(|hash| hash == retained.tombstone.decision_hash);
                let summary_ok = retained.tombstone.atomic_id == decision.atomic_id
                    && retained.tombstone.content_root == prepare.content_root
                    && retained.tombstone.decision == decision.decision
                    && retained.tombstone.commit_position == decision.commit_position
                    && retained.tombstone.abort_reason == decision.abort_reason;
                if !hash_ok || !summary_ok {
                    catalog.blocked.insert(key);
                    findings.push_key(
                        StageEvidenceKind::Decision,
                        StageEvidenceClass::Conflict,
                        key,
                    );
                }
            }
        } else if let Some(retained) = catalog.tombstones.get(&key) {
            // A damaged/missing detailed decision can be recovered from the
            // lifetime authority only when surviving detail reproduces its
            // exact decision hash. A mismatch is decision-authority conflict,
            // not material absence.
            let reconstructed = prepare_hash(&prepare)
                .ok()
                .and_then(|prepare_hash| match retained.tombstone.decision {
                    DecisionCode::Committed => retained.tombstone.commit_position.and_then(|pos| {
                        AtomicDecision::committed(
                            key.1,
                            prepare_hash,
                            prepare.ordered_member_manifest_root,
                            prepare.member_count,
                            pos,
                        )
                        .ok()
                    }),
                    DecisionCode::NotCommitted => retained.tombstone.abort_reason.map(|reason| {
                        AtomicDecision::not_committed(
                            key.1,
                            prepare_hash,
                            prepare.ordered_member_manifest_root,
                            prepare.member_count,
                            reason,
                        )
                    }),
                });
            let exact = retained.tombstone.content_root == prepare.content_root
                && reconstructed.as_ref().is_some_and(|decision| {
                    decision_hash(decision)
                        .ok()
                        .is_some_and(|hash| hash == retained.tombstone.decision_hash)
                });
            if !exact {
                catalog.blocked.insert(key);
                findings.push_key(
                    StageEvidenceKind::Decision,
                    StageEvidenceClass::Conflict,
                    key,
                );
            }
        }
    }
    let mut positions = std::collections::BTreeMap::new();
    for (key, decision) in &catalog.decisions {
        if decision.decision != DecisionCode::Committed {
            continue;
        }
        if let Some(position) = decision.commit_position {
            let heap_id = key.0;
            if let Some(other) = positions.insert((heap_id, position), *key) {
                catalog.blocked.insert(other);
                catalog.blocked.insert(*key);
                findings.push_key(
                    StageEvidenceKind::Decision,
                    StageEvidenceClass::Conflict,
                    other,
                );
                findings.push_key(
                    StageEvidenceKind::Decision,
                    StageEvidenceClass::Conflict,
                    *key,
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
        findings.push_key(
            StageEvidenceKind::Decision,
            StageEvidenceClass::Conflict,
            key,
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
            findings.push_key(StageEvidenceKind::Decision, StageEvidenceClass::Valid, key);
        }
        Some(existing) if existing == &decision => {
            findings.push_key(StageEvidenceKind::Decision, StageEvidenceClass::Valid, key)
        }
        Some(_) => {
            catalog.blocked.insert(key);
            findings.push_key(
                StageEvidenceKind::Decision,
                StageEvidenceClass::Conflict,
                key,
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
    orphans.extend(catalog.tombstones.keys().copied());
    orphans.sort();
    orphans.dedup();
    for key in orphans {
        if catalog.prepares.contains_key(&key) || catalog.blocked.contains(&key) {
            continue;
        }
        // A lone tombstone is valid after lawful detail retirement.
        if catalog.tombstones.contains_key(&key) {
            continue;
        }
        catalog.blocked.insert(key);
        if catalog.members.remove(&key).is_some() {
            findings.push_key(StageEvidenceKind::Member, StageEvidenceClass::Corrupt, key);
        }
        if catalog
            .payloads
            .keys()
            .any(|(heap, oid, _)| (*heap, *oid) == key)
        {
            catalog
                .payloads
                .retain(|(heap, oid, _), _| (*heap, *oid) != key);
            findings.push_key(StageEvidenceKind::Payload, StageEvidenceClass::Corrupt, key);
        }
        if catalog.seals.remove(&key).is_some() {
            findings.push_key(StageEvidenceKind::Seal, StageEvidenceClass::Corrupt, key);
        }
        if catalog
            .chunk_plans
            .keys()
            .any(|(heap, oid, _)| (*heap, *oid) == key)
        {
            catalog
                .chunk_plans
                .retain(|(heap, oid, _), _| (*heap, *oid) != key);
            findings.push_key(
                StageEvidenceKind::ChunkPlan,
                StageEvidenceClass::Corrupt,
                key,
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
            findings.push_key(
                StageEvidenceKind::ChunkBody,
                StageEvidenceClass::Corrupt,
                key,
            );
        }
        // Retain an orphan decision as damaged lifetime evidence. In
        // particular, never forget a named commit position and later reuse it.
        if catalog.decisions.contains_key(&key) {
            findings.push_key(
                StageEvidenceKind::Decision,
                StageEvidenceClass::Corrupt,
                key,
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
            let heap_id = sidecar_heap_id(body);
            block(catalog, heap_id.zip(atomic_id));
            findings.push_bound(
                sidecar_kind(kind),
                StageEvidenceClass::Corrupt,
                heap_id,
                atomic_id,
            );
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
                    findings.push_key(
                        StageEvidenceKind::OrderFrontier,
                        StageEvidenceClass::Conflict,
                        key,
                    );
                }
            } else {
                catalog.order_frontiers.insert(key, frontiers);
            }
        }
        SidecarDecode::Tombstone { heap_id, retained } => {
            let id = retained.tombstone.atomic_id;
            let key = stage_key(heap_id, id);
            match catalog.tombstones.get(&key) {
                None => {
                    catalog.tombstones.insert(key, retained);
                    catalog.tombstone_index_dirty = true;
                    findings.push_key(StageEvidenceKind::Tombstone, StageEvidenceClass::Valid, key);
                }
                Some(existing) if existing.tombstone == retained.tombstone => {
                    // The first authenticated decision time wins. Re-emission
                    // may refresh a physical copy but cannot extend retention.
                    if retained.decided_at_unix_s < existing.decided_at_unix_s {
                        catalog.tombstones.insert(key, retained);
                        catalog.tombstone_index_dirty = true;
                    }
                    findings.push_key(StageEvidenceKind::Tombstone, StageEvidenceClass::Valid, key);
                }
                Some(_) => {
                    catalog.blocked.insert(key);
                    findings.push_key(
                        StageEvidenceKind::Decision,
                        StageEvidenceClass::Conflict,
                        key,
                    );
                }
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
        b"ATTOMB2",
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
    match catalog.prepares.get(&key) {
        None => {
            catalog.prepares.insert(key, prepare);
            if from_batch {
                catalog.prepare_batch.insert(key);
            }
            if !catalog.prepare_seen.contains(&key) {
                catalog.prepare_seen.push(key);
            }
            findings.push_key(StageEvidenceKind::Prepare, StageEvidenceClass::Valid, key);
        }
        Some(existing) if existing == &prepare => {
            if from_batch {
                catalog.prepare_batch.insert(key);
            }
            findings.push_key(StageEvidenceKind::Prepare, StageEvidenceClass::Valid, key);
        }
        Some(_) => {
            catalog.blocked.insert(key);
            findings.push_key(
                StageEvidenceKind::Prepare,
                StageEvidenceClass::Conflict,
                key,
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
    let slot = catalog.members.entry(key).or_default();
    match slot.iter().find(|m| m.ordinal == member.ordinal) {
        None => {
            slot.push(member);
            findings.push_key(StageEvidenceKind::Member, StageEvidenceClass::Valid, key);
        }
        Some(existing) if existing == &member => {
            findings.push_key(StageEvidenceKind::Member, StageEvidenceClass::Valid, key);
        }
        Some(_) => {
            catalog.blocked.insert(key);
            findings.push_key(StageEvidenceKind::Member, StageEvidenceClass::Conflict, key);
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
    match catalog.payloads.get(&(heap_id, atomic_id, ordinal)) {
        None => {
            catalog
                .payloads
                .insert((heap_id, atomic_id, ordinal), payload);
            findings.push_key(StageEvidenceKind::Payload, StageEvidenceClass::Valid, key);
        }
        Some(existing) if existing == &payload => {
            findings.push_key(StageEvidenceKind::Payload, StageEvidenceClass::Valid, key);
        }
        Some(_) => {
            catalog.blocked.insert(key);
            findings.push_key(
                StageEvidenceKind::Payload,
                StageEvidenceClass::Conflict,
                key,
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
    match catalog.seals.get(&key) {
        None => {
            catalog.seals.insert(key, content_root);
            findings.push_key(StageEvidenceKind::Seal, StageEvidenceClass::Valid, key);
        }
        Some(existing) if existing == &content_root => {
            findings.push_key(StageEvidenceKind::Seal, StageEvidenceClass::Valid, key);
        }
        Some(_) => {
            catalog.seals.remove(&key);
            catalog.blocked.insert(key);
            findings.push_key(StageEvidenceKind::Seal, StageEvidenceClass::Conflict, key);
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
    match catalog.chunk_plans.get(&(heap_id, atomic_id, ordinal)) {
        None => {
            catalog
                .chunk_plans
                .insert((heap_id, atomic_id, ordinal), plan);
            findings.push_key(StageEvidenceKind::ChunkPlan, StageEvidenceClass::Valid, key);
        }
        Some(existing) if existing == &plan => {
            findings.push_key(StageEvidenceKind::ChunkPlan, StageEvidenceClass::Valid, key);
        }
        Some(_) => {
            catalog.blocked.insert(key);
            findings.push_key(
                StageEvidenceKind::ChunkPlan,
                StageEvidenceClass::Conflict,
                key,
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
    match catalog.chunks.get(&(heap_id, atomic_id, ordinal, index)) {
        None => {
            catalog
                .chunks
                .insert((heap_id, atomic_id, ordinal, index), body);
            findings.push_key(StageEvidenceKind::ChunkBody, StageEvidenceClass::Valid, key);
        }
        Some(existing) if existing == &body => {
            findings.push_key(StageEvidenceKind::ChunkBody, StageEvidenceClass::Valid, key);
        }
        Some(_) => {
            catalog.blocked.insert(key);
            findings.push_key(
                StageEvidenceKind::ChunkBody,
                StageEvidenceClass::Conflict,
                key,
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
        SidecarKind::OrderFrontier => StageEvidenceKind::OrderFrontier,
        SidecarKind::Tombstone => StageEvidenceKind::Tombstone,
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
        StageEvidenceKind::OrderFrontier => 10,
        StageEvidenceKind::Tombstone => 11,
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
        10 => StageEvidenceKind::OrderFrontier,
        11 => StageEvidenceKind::Tombstone,
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
