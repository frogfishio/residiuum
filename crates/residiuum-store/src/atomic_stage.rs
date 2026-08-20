//! Store-owned Atomic staging (CR-ATMR4-005 / CR-ATMR5-001).
//! CR-ATMR5-003: exact same-ID member/payload/chunk retries are idempotent.
//!
//! One durable authority: the store segment under the exclusive writer.
//! `StagingHeap` is the in-memory model only. The peer `DurableLane` is not
//! opened, created, or consulted. Ordinary `get` / scan / history stay empty.
//! The live catalogue is opened once from a store-owned checkpoint plus tails.

use crate::atomic_stage_classify::{
    clear_coverage_loss, StageEvidenceClass, StageEvidenceKind, StageFindings,
};
use crate::atomic_stage_media::{
    chunk_body_event_id, chunk_plan_event_id, encode_order_frontier, encode_stage_chunk_body,
    encode_stage_chunk_plan, encode_stage_payload, encode_stage_seal, order_frontier_event_id,
    payload_event_id, seal_event_id, AtomicPublishMember, AtomicValueRef, BodyRef, StageCatalog,
};
use crate::atomic_stage_recover::{
    checkpoint_encoded_len, open_catalog, open_catalog_readonly, persist_live_checkpoint, rel_path,
    resolve_chunk_body, resolve_payload_body, resolve_published_payload, verify_missing_coverage,
    AtomicStageLimits, AtomicStageOpenReport, CoveredFile,
};
use crate::error::StoreError;
use crate::store::Store;
use residiuum_atomics::{
    decision_hash, encode_decision, encode_member, encode_prepare, members_match_prepare,
    ordered_member_manifest_root, plan_content_root, prepare_from_closed_plan, prepare_hash,
    validate_closed_plan, AtomicAbortReason, AtomicDecision, AtomicId, AtomicMember,
    AtomicMemberReceipt, AtomicOutcome, AtomicPlan, AtomicPrepare, AtomicReceipt,
    AtomicRefuseReason, AtomicsError, ChunkPlan, CoordinatorSeq, DecisionCode, HeapId, MemberPhase,
    MutationKind, ObjectIdentity, PlacementManifest, PredicateKind, StagingHeap, VersionId,
};
use residiuum_format::{
    encode_atomic_commit_envelope, encode_atomic_member_envelope, encode_atomic_prepare_envelope,
    encode_subject_v2, FrameKind, SubjectObjectKind, EMPTY_ENVELOPE,
};
use std::fs;
use std::path::PathBuf;

/// Store-owned handle to staged Atomic evidence.
pub struct StoreAtomicStage<'a> {
    store: &'a mut Store,
    heap: StagingHeap,
    catalog: StageCatalog,
    covered: Vec<CoveredFile>,
    report: AtomicStageOpenReport,
    findings: StageFindings,
    limits: AtomicStageLimits,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StagePersistMode {
    /// Legacy/manual staging surface: each accepted record and checkpoint is
    /// independently durable for fine-grained recovery tooling.
    StableCheckpointed,
    /// Whole-plan commit: submit member bytes without sync; the seal and
    /// decision establish the only two authoritative durability boundaries.
    BufferedCohort,
}

impl Store {
    /// Open the store-owned Atomic stage. Requires the exclusive writer lock.
    pub fn atomic_stage(&mut self) -> Result<StoreAtomicStage<'_>, StoreError> {
        self.atomic_stage_with_limits(AtomicStageLimits::operable())
    }

    /// Open Atomic stage with explicit operable/test limits (CR-ATMR6-004).
    pub fn atomic_stage_with_limits(
        &mut self,
        limits: AtomicStageLimits,
    ) -> Result<StoreAtomicStage<'_>, StoreError> {
        let heap_id = HeapId::from_bytes(self.store_id())
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        self.atomic_stage_for_heap_with_limits(heap_id, limits)
    }

    /// Open the deployment-wide Atomic catalogue with execution bound to one
    /// named Heap. Physical ownership remains shared; plans for another Heap
    /// cannot be installed through the returned handle.
    pub fn atomic_stage_for_heap(
        &mut self,
        heap_id: HeapId,
    ) -> Result<StoreAtomicStage<'_>, StoreError> {
        self.atomic_stage_for_heap_with_limits(heap_id, AtomicStageLimits::operable())
    }

    /// Named-Heap Atomic stage with explicit operable/test limits.
    pub fn atomic_stage_for_heap_with_limits(
        &mut self,
        heap_id: HeapId,
        limits: AtomicStageLimits,
    ) -> Result<StoreAtomicStage<'_>, StoreError> {
        if !self.holds_writer_lock() {
            return Err(StoreError::AtomicStage(
                "atomic stage requires the store writer lock".into(),
            ));
        }
        let opened = open_catalog(self.paths(), limits)?;
        let heap = rebuild_heap(self.paths(), heap_id, &opened.catalog)?;
        self.record_atomic_stage_open(opened.report);
        Ok(StoreAtomicStage {
            store: self,
            heap,
            catalog: opened.catalog,
            covered: opened.covered,
            report: opened.report,
            findings: opened.findings,
            limits,
        })
    }

    /// Reconstruct every committed Atomic projection after the ordinary index
    /// has opened. The decision catalogue is authority; publication is derived.
    pub(crate) fn recover_committed_atomic_publications(&mut self) -> Result<(), StoreError> {
        self.recover_committed_atomic_publications_inner(false)
    }

    /// Read-only counterpart used by inspection opens. It rebuilds the same
    /// committed projection but never refreshes Atomic checkpoint media.
    pub(crate) fn recover_committed_atomic_publications_readonly(
        &mut self,
    ) -> Result<(), StoreError> {
        self.recover_committed_atomic_publications_inner(true)
    }

    fn recover_committed_atomic_publications_inner(
        &mut self,
        readonly: bool,
    ) -> Result<(), StoreError> {
        // Preserve the ordinary-store fast path. Atomic admission creates both
        // control files before a decision can exist, so their joint absence is
        // a sufficient negative check without scanning segment contents.
        if !crate::atomic_stage_recover::atomic_stage_checkpoint_path(self.paths()).is_file()
            && !crate::atomic_stage_recover::atomic_coord_path(self.paths()).is_file()
        {
            return Ok(());
        }
        let opened = if readonly {
            open_catalog_readonly(self.paths(), AtomicStageLimits::operable())?
        } else {
            open_catalog(self.paths(), AtomicStageLimits::operable())?
        };
        self.record_atomic_stage_open(opened.report);
        if opened.catalog.coverage_degraded {
            if opened
                .catalog
                .decisions
                .values()
                .any(|decision| decision.decision == DecisionCode::Committed)
            {
                return Err(StoreError::AtomicStage(
                    "committed Atomic publication refused: evidence coverage is incomplete".into(),
                ));
            }
            return Ok(());
        }

        let mut committed: Vec<_> = opened
            .catalog
            .decisions
            .iter()
            .filter_map(|(atomic_id, decision)| {
                decision.commit_position.map(|position| {
                    let heap = opened.catalog.evidence_heaps.get(atomic_id).copied();
                    (*atomic_id, heap, position)
                })
            })
            .collect();
        committed.sort_by_key(|(atomic_id, heap, position)| {
            (
                heap.map(|id| *id.as_bytes()).unwrap_or([0; 16]),
                *position,
                *atomic_id,
            )
        });
        for (atomic_id, _, _) in committed {
            let delta = publication_delta(self.paths(), &opened.catalog, atomic_id)?;
            self.publish_atomic_generation(&delta, true)?;
        }
        Ok(())
    }
}

impl StoreAtomicStage<'_> {
    /// Historical peer-lane path. This stage does not create or open it.
    pub fn lane_root(&self) -> PathBuf {
        self.store.paths().store_info().join("atomic-lane")
    }

    /// In-memory kernel reconstructed from store media.
    pub fn kernel(&self) -> &StagingHeap {
        &self.heap
    }

    /// Bounded recovery costs for this handle (CR-ATMR5-001).
    pub fn open_report(&self) -> AtomicStageOpenReport {
        self.report
    }

    /// Honest damage/conflict observations (CR-ATMR5-002).
    pub fn findings(&self) -> &StageFindings {
        &self.findings
    }

    /// Store-authoritative examination of surviving prepare/material.
    ///
    /// A durable prepare with incomplete members is [`AtomicStageClass::Prepared`],
    /// never absence (CR-ATMR6-005).
    pub fn examine(&self, atomic_id: AtomicId) -> crate::AtomicStageStatus {
        if self.catalog.evidence_heaps.get(&atomic_id).copied() != Some(self.heap.heap_id()) {
            return crate::atomic_stage_status::project_atomic(&StageCatalog::default(), atomic_id);
        }
        crate::atomic_stage_status::project_atomic(&self.catalog, atomic_id)
    }

    /// Operator-only authenticated repair. Ordinary reopen and retry must not
    /// call this (CR-ATMR6-002 / CR-ATMR7-002).
    pub fn scrub_coverage(&mut self) -> Result<(), StoreError> {
        if self.catalog.findings.records.iter().any(|finding| {
            finding.kind != StageEvidenceKind::Coverage
                && finding.atomic_id.is_none()
                && matches!(
                    finding.class,
                    StageEvidenceClass::Corrupt | StageEvidenceClass::Partial
                )
        }) {
            return Err(StoreError::AtomicStage(
                "atomic stage scrub refused: unbound damaged Atomic evidence remains".into(),
            ));
        }
        verify_missing_coverage(
            self.store.paths(),
            &self.catalog,
            &self.covered,
            self.limits,
        )?;
        self.catalog.coverage_degraded = false;
        self.catalog.missing_covered.clear();
        clear_coverage_loss(&mut self.catalog.findings);
        clear_coverage_loss(&mut self.findings);
        self.report.coverage_degraded = false;
        persist_live_checkpoint(
            self.store.paths(),
            &self.catalog,
            &mut self.covered,
            self.limits,
        )
    }

    /// Validate a closed plan, persist it on the store segment, then apply.
    pub fn begin_prepare(
        &mut self,
        plan: &AtomicPlan,
        frontier: [u8; 32],
        members: &[AtomicMember],
    ) -> Result<(CoordinatorSeq, PlacementManifest), StoreError> {
        let mode = StagePersistMode::StableCheckpointed;
        let prepare = self.ensure_prepare_record(plan, frontier, members, mode)?;
        self.install_prepared_members(&prepare, members, mode)
    }

    /// Persist only the accepted prepare. Validation and member installation
    /// deliberately remain separate for the ATM-3 serialization algorithm.
    fn ensure_prepare_record(
        &mut self,
        plan: &AtomicPlan,
        frontier: [u8; 32],
        members: &[AtomicMember],
        mode: StagePersistMode,
    ) -> Result<AtomicPrepare, StoreError> {
        if plan.heap_id() != self.heap.heap_id() {
            return Err(StoreError::AtomicStage(
                "atomic plan Heap does not match the capability-bound stage".into(),
            ));
        }
        let prepare = prepare_from_closed_plan(plan, frontier, members)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        if self.catalog.blocked.contains(&prepare.atomic_id) {
            return Err(StoreError::AtomicStage(
                "atomic identity is blocked by conflicting or damaged evidence".into(),
            ));
        }
        if self.catalog.coverage_degraded && !self.catalog.prepares.contains_key(&prepare.atomic_id)
        {
            return Err(StoreError::AtomicStage(
                "atomic prepare refused: authenticated Atomic coverage is incomplete".into(),
            ));
        }
        if let Some(stored) = self.catalog.prepares.get(&prepare.atomic_id) {
            if stored != &prepare {
                return Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into());
            }
            if !self.catalog.prepare_batch.contains(&prepare.atomic_id) {
                // Legacy ATPREP1-only prefix: repair the BatchPrepare authority.
                self.persist_prepare(&prepare, members.len() as u32, mode)?;
            }
        } else {
            self.admit_new_atomic()?;
            self.persist_prepare(&prepare, members.len() as u32, mode)?;
        }
        Ok(prepare)
    }

    fn install_prepared_members(
        &mut self,
        prepare: &AtomicPrepare,
        members: &[AtomicMember],
        mode: StagePersistMode,
    ) -> Result<(CoordinatorSeq, PlacementManifest), StoreError> {
        if let Some(existing) = self.heap.placement(prepare.atomic_id) {
            if existing.content_root() == prepare.content_root
                && members_match_prepare(&prepare, members)
            {
                let seq = self
                    .heap
                    .prepare_seq(prepare.atomic_id)
                    .ok_or_else(|| StoreError::AtomicStage("prepared without sequence".into()))?;
                return Ok((seq, existing.clone()));
            }
            return Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into());
        }
        for member in members {
            if !self.catalog.has_member(member.atomic_id, member.ordinal) {
                self.persist_member(&prepare, member, mode)?;
            }
        }
        let seq = self
            .catalog
            .coord_seq
            .get(&prepare.atomic_id)
            .copied()
            .and_then(CoordinatorSeq::from_raw)
            .ok_or_else(|| {
                StoreError::AtomicStage("prepare without coordinator sequence".into())
            })?;
        self.heap
            .install_prepared(seq, prepare.atomic_id, prepare.content_root, members)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))
            .map(|manifest| (seq, manifest))
    }

    /// Persist a staged payload on the store segment. Not a `put`.
    ///
    /// Exact same-ID retries succeed without writing more media. A changed
    /// member or payload is `DuplicateTarget` (CR-ATMR5-003).
    pub fn append_staged(
        &mut self,
        member: AtomicMember,
        payload: Vec<u8>,
    ) -> Result<(), StoreError> {
        self.append_staged_inner(member, payload, StagePersistMode::StableCheckpointed)
    }

    fn append_staged_inner(
        &mut self,
        member: AtomicMember,
        payload: Vec<u8>,
        mode: StagePersistMode,
    ) -> Result<(), StoreError> {
        if self.existing_payload_conflicts(&member, &payload) {
            return Err(Self::duplicate_target());
        }
        if self.find_staged(member.atomic_id, member.ordinal).is_some() {
            if !self.catalog.has_payload(member.atomic_id, member.ordinal) {
                self.persist_payload(&member, &payload, mode)?;
            }
            return Ok(());
        }
        self.heap
            .check_append_staged(&member, &payload)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        if !self.catalog.has_payload(member.atomic_id, member.ordinal) {
            self.persist_payload(&member, &payload, mode)?;
        }
        self.heap
            .append_staged(member, payload)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))
    }

    /// Persist the frozen chunk map, then install it in the kernel (CR-ATMR5-005).
    /// Exact plan retry is a no-op; a different plan is `DuplicateTarget`.
    pub fn commit_chunk_manifest(
        &mut self,
        atomic_id: AtomicId,
        ordinal: u32,
        plan: ChunkPlan,
    ) -> Result<(), StoreError> {
        if let Some(existing) = self.heap.chunk_plan(atomic_id, ordinal) {
            if existing != &plan {
                return Err(Self::duplicate_target());
            }
        } else if let Some(stored) = self.catalog.chunk_plans.get(&(atomic_id, ordinal)) {
            if stored != &plan {
                return Err(Self::duplicate_target());
            }
        }
        if self.heap.chunk_plan(atomic_id, ordinal).is_some() {
            if !self.catalog.has_chunk_plan(atomic_id, ordinal) {
                self.persist_chunk_plan(atomic_id, ordinal, &plan)?;
            }
            return Ok(());
        }
        self.heap
            .check_commit_chunk_manifest(atomic_id, ordinal, &plan)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        if !self.catalog.has_chunk_plan(atomic_id, ordinal) {
            self.persist_chunk_plan(atomic_id, ordinal, &plan)?;
        }
        self.heap
            .commit_chunk_manifest(atomic_id, ordinal, plan)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))
    }

    /// Install one chunk in the model. When the payload first completes, persist
    /// the member payload on the store.
    ///
    /// Exact chunk retries succeed without extra media. A different member,
    /// index body, or completed payload is `DuplicateTarget` (CR-ATMR5-003).
    pub fn append_chunk(
        &mut self,
        member: AtomicMember,
        index: u32,
        body: Vec<u8>,
    ) -> Result<(), StoreError> {
        let atomic_id = member.atomic_id;
        let ordinal = member.ordinal;
        if let Some(decision) = self.existing_chunk_decision(&member, index, &body) {
            if decision.is_ok() {
                self.persist_completed_payload_if_missing(&member)?;
            }
            return decision;
        }
        if let Some(stored) = self.catalog.chunks.get(&(atomic_id, ordinal, index)) {
            if stored != &body {
                return Err(Self::duplicate_target());
            }
        }
        self.heap
            .check_append_chunk(&member, index, &body)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        if !self.catalog.has_chunk(atomic_id, ordinal, index) {
            self.persist_chunk_body(&member, index, &body)?;
        }
        self.heap
            .append_chunk(member.clone(), index, body)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let complete = self
            .heap
            .inspect_staged(atomic_id)
            .and_then(|ms| ms.iter().find(|s| s.member.ordinal == ordinal))
            .is_some_and(|s| s.payload_complete);
        if !complete {
            return Ok(());
        }
        let payload = self
            .heap
            .inspect_staged(atomic_id)
            .and_then(|ms| ms.iter().find(|s| s.member.ordinal == ordinal))
            .map(|s| s.payload.clone())
            .ok_or_else(|| StoreError::AtomicStage("complete chunk without payload".into()))?;
        if !self.catalog.has_payload(atomic_id, ordinal) {
            self.persist_payload(&member, &payload, StagePersistMode::StableCheckpointed)?;
        }
        Ok(())
    }

    /// First stable member boundary: persist a store seal, then apply the model.
    pub fn seal_member_boundary(&mut self, atomic_id: AtomicId) -> Result<(), StoreError> {
        self.seal_member_boundary_inner(atomic_id, StagePersistMode::StableCheckpointed)
    }

    fn seal_member_boundary_inner(
        &mut self,
        atomic_id: AtomicId,
        mode: StagePersistMode,
    ) -> Result<(), StoreError> {
        let already_applied = self
            .heap
            .lifecycle(atomic_id)
            .is_some_and(|life| life.members == MemberPhase::DurableInvisible);
        if !self.catalog.is_sealed(atomic_id) {
            self.heap
                .check_seal_member_boundary(atomic_id)
                .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
            let content_root = self
                .heap
                .placement(atomic_id)
                .ok_or_else(|| StoreError::AtomicStage("seal without prepare".into()))?
                .content_root();
            self.persist_seal(atomic_id, content_root, mode)?;
        }
        if !already_applied {
            self.heap
                .seal_member_boundary(atomic_id)
                .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        }
        Ok(())
    }

    /// ATM-3A decision primitive. This hidden qualification surface is not a
    /// product commit API: no caller may receive a commit acknowledgement while
    /// the durable decision is not yet ordinarily visible.
    #[doc(hidden)]
    pub fn persist_committed_decision(
        &mut self,
        atomic_id: AtomicId,
    ) -> Result<AtomicDecision, StoreError> {
        self.persist_committed_decision_inner(atomic_id, StagePersistMode::StableCheckpointed)
    }

    fn persist_committed_decision_inner(
        &mut self,
        atomic_id: AtomicId,
        mode: StagePersistMode,
    ) -> Result<AtomicDecision, StoreError> {
        if self.catalog.blocked.contains(&atomic_id) || self.catalog.coverage_degraded {
            return Err(StoreError::AtomicStage(
                "atomic decision refused: evidence is blocked or coverage is incomplete".into(),
            ));
        }
        if let Some(existing) = self.catalog.decisions.get(&atomic_id) {
            return Ok(existing.clone());
        }
        let prepare = self
            .catalog
            .prepares
            .get(&atomic_id)
            .cloned()
            .ok_or_else(|| StoreError::AtomicStage("decision without prepare".into()))?;
        if self.catalog.seals.get(&atomic_id) != Some(&prepare.content_root) {
            return Err(StoreError::AtomicStage(
                "decision before stable member boundary".into(),
            ));
        }
        let members = self
            .catalog
            .members
            .get(&atomic_id)
            .cloned()
            .unwrap_or_default();
        let intended = self
            .catalog
            .intended_members
            .get(&atomic_id)
            .copied()
            .unwrap_or(0);
        if intended == 0
            || members.len() != intended as usize
            || !members_match_prepare(&prepare, &members)
            || members.iter().any(|member| {
                !self.catalog.has_payload(atomic_id, member.ordinal)
                    && !self
                        .catalog
                        .payload_refs
                        .contains_key(&(atomic_id, member.ordinal))
            })
        {
            let missing_payloads = members
                .iter()
                .filter(|member| {
                    !self.catalog.has_payload(atomic_id, member.ordinal)
                        && !self
                            .catalog
                            .payload_refs
                            .contains_key(&(atomic_id, member.ordinal))
                })
                .count();
            return Err(StoreError::AtomicStage(format!(
                "decision requires the exact complete member manifest and payloads \
                 (members={}/{intended}, missing_payloads={missing_payloads})",
                members.len()
            )));
        }
        let position = self.catalog.next_commit_position(prepare.heap_id)?;
        let decision = AtomicDecision::committed(
            atomic_id,
            prepare_hash(&prepare).map_err(|e| StoreError::AtomicStage(e.to_string()))?,
            ordered_member_manifest_root(prepare.heap_id, &members)
                .map_err(|e| StoreError::AtomicStage(e.to_string()))?,
            intended,
            position,
        )
        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let body =
            encode_decision(&decision).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        if !self.catalog.order_frontiers.contains_key(&atomic_id) {
            let frontiers = self.store.atomic_order_frontier()?;
            let witness = encode_order_frontier(atomic_id, &frontiers)?;
            let mut candidate = self.catalog.clone();
            candidate
                .order_frontiers
                .insert(atomic_id, frontiers.clone());
            self.admit_catalog_change(&candidate, witness.len() as u64)?;
            crate::failpoint::hit("store.atomic.before_order_frontier")?;
            self.store.append_buffered_atomic_frame(
                FrameKind::PayloadChunk,
                EMPTY_ENVELOPE,
                &witness,
                order_frontier_event_id(atomic_id),
            )?;
            crate::failpoint::hit("store.atomic.after_order_frontier")?;
            self.catalog.order_frontiers.insert(atomic_id, frontiers);
        }
        let envelope = encode_atomic_commit_envelope(
            prepare.heap_id.as_bytes(),
            atomic_id.as_bytes(),
            prepare.content_root.as_bytes(),
            Some(position),
        )
        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        self.admit_catalog_change(&self.catalog.clone(), body.len() as u64)?;
        crate::failpoint::hit("store.atomic.before_decision")?;
        self.store.append_unindexed_atomic_frame(
            FrameKind::BatchCommit,
            &envelope,
            &body,
            decision_event_id(atomic_id),
        )?;
        crate::failpoint::hit("store.atomic.after_decision")?;
        self.catalog
            .evidence_heaps
            .insert(atomic_id, prepare.heap_id);
        self.catalog.decisions.insert(atomic_id, decision.clone());
        self.catalog
            .commit_next
            .insert(prepare.heap_id, position.saturating_add(1));
        if mode == StagePersistMode::StableCheckpointed {
            persist_live_checkpoint(
                self.store.paths(),
                &self.catalog,
                &mut self.covered,
                self.limits,
            )?;
        }
        Ok(decision)
    }

    /// Persist a terminal failed validation. This consumes the accepted Atomic
    /// identity but allocates no Heap commit position and publishes no member.
    #[doc(hidden)]
    pub fn persist_not_committed_decision(
        &mut self,
        atomic_id: AtomicId,
        reason: AtomicAbortReason,
    ) -> Result<AtomicDecision, StoreError> {
        self.persist_not_committed_decision_inner(
            atomic_id,
            reason,
            StagePersistMode::StableCheckpointed,
        )
    }

    fn persist_not_committed_decision_inner(
        &mut self,
        atomic_id: AtomicId,
        reason: AtomicAbortReason,
        mode: StagePersistMode,
    ) -> Result<AtomicDecision, StoreError> {
        if self.catalog.blocked.contains(&atomic_id) || self.catalog.coverage_degraded {
            return Err(StoreError::AtomicStage(
                "atomic decision refused: evidence is blocked or coverage is incomplete".into(),
            ));
        }
        if let Some(existing) = self.catalog.decisions.get(&atomic_id) {
            return Ok(existing.clone());
        }
        let prepare = self
            .catalog
            .prepares
            .get(&atomic_id)
            .cloned()
            .ok_or_else(|| StoreError::AtomicStage("decision without prepare".into()))?;
        let intended = self
            .catalog
            .intended_members
            .get(&atomic_id)
            .copied()
            .ok_or_else(|| StoreError::AtomicStage("prepare member count missing".into()))?;
        let decision = AtomicDecision::not_committed(
            atomic_id,
            prepare_hash(&prepare).map_err(|e| StoreError::AtomicStage(e.to_string()))?,
            prepare.ordered_member_manifest_root,
            intended,
            reason,
        );
        let body =
            encode_decision(&decision).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let envelope = encode_atomic_commit_envelope(
            prepare.heap_id.as_bytes(),
            atomic_id.as_bytes(),
            prepare.content_root.as_bytes(),
            None,
        )
        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        crate::failpoint::hit("store.atomic.before_decision")?;
        self.store.append_unindexed_atomic_frame(
            FrameKind::BatchCommit,
            &envelope,
            &body,
            decision_event_id(atomic_id),
        )?;
        crate::failpoint::hit("store.atomic.after_decision")?;
        self.catalog
            .evidence_heaps
            .insert(atomic_id, prepare.heap_id);
        self.catalog.decisions.insert(atomic_id, decision.clone());
        if mode == StagePersistMode::StableCheckpointed {
            persist_live_checkpoint(
                self.store.paths(),
                &self.catalog,
                &mut self.covered,
                self.limits,
            )?;
        }
        Ok(decision)
    }

    /// ATM-3B qualification path: accept one closed plan, validate it against
    /// the locked Heap frontier, and persist an exact terminal decision.
    /// Successful members remain invisible until ATM-3C publication.
    #[doc(hidden)]
    pub fn decide_plan_evidence(
        &mut self,
        plan: &AtomicPlan,
    ) -> Result<AtomicDecision, StoreError> {
        if plan.heap_id() != self.heap.heap_id() {
            return Err(StoreError::AtomicStage(
                "atomic plan Heap does not match the capability-bound stage".into(),
            ));
        }
        // Closing canonicalizes shape; admission is the separate operation
        // that enforces the executable profile, scope and applied hard limits.
        // It must precede content lookup or any durable evidence append.
        validate_closed_plan(plan, self.heap.heap_id())?;
        let root = plan_content_root(plan).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        if let Some(stored) = self.catalog.prepares.get(&plan.atomic_id()) {
            if stored.content_root != root {
                return Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into());
            }
            if let Some(decision) = self.catalog.decisions.get(&plan.atomic_id()) {
                let decision = decision.clone();
                if decision.decision == DecisionCode::Committed {
                    self.publish_decision(plan.atomic_id())?;
                }
                return Ok(decision);
            }
        }

        let members = self.members_for_frontier(plan)?;
        let frontier = self.frontier_for_plan(plan)?;
        let mode = StagePersistMode::BufferedCohort;
        let prepare = self.ensure_prepare_record(plan, frontier, &members, mode)?;
        if let Some(reason) = self.validate_at_frontier(plan)? {
            return self.persist_not_committed_decision_inner(prepare.atomic_id, reason, mode);
        }

        self.install_prepared_members(&prepare, &members, mode)?;
        for (member, mutation) in members.iter().zip(plan.mutations()) {
            let payload = mutation.encoded_value.clone().unwrap_or_default();
            self.append_staged_inner(member.clone(), payload, mode)?;
        }
        self.seal_member_boundary_inner(prepare.atomic_id, mode)?;
        let decision = self.persist_committed_decision_inner(prepare.atomic_id, mode)?;
        crate::failpoint::hit("store.atomic.before_publish")?;
        self.publish_decision(prepare.atomic_id)?;
        crate::failpoint::hit("store.atomic.after_publish")?;
        // The receipt has been fully determined and the complete generation is
        // visible, but it has not crossed the caller acknowledgement boundary.
        crate::failpoint::hit("store.atomic.before_ack")?;
        Ok(decision)
    }

    /// ATM-3 qualification outcome with the frozen product receipt shape.
    ///
    /// This remains hidden until the full public admission/authority surface is
    /// qualified, but it proves that a caller can retain exact CAS versions
    /// without rereading every committed member.
    #[doc(hidden)]
    pub fn decide_plan_outcome(&mut self, plan: &AtomicPlan) -> Result<AtomicOutcome, StoreError> {
        let replayed = self.catalog.decisions.contains_key(&plan.atomic_id());
        let decision = self.decide_plan_evidence(plan)?;
        if decision.decision == DecisionCode::NotCommitted {
            return decision.not_committed_outcome().map_err(StoreError::from);
        }

        let prepare = self
            .catalog
            .prepares
            .get(&plan.atomic_id())
            .ok_or_else(|| StoreError::AtomicStage("committed receipt missing prepare".into()))?;
        let members = self
            .catalog
            .members
            .get(&plan.atomic_id())
            .ok_or_else(|| StoreError::AtomicStage("committed receipt missing members".into()))?
            .iter()
            .map(|member| AtomicMemberReceipt {
                collection_id: member.object_identity.collection_id,
                key: member.object_identity.key.identity_bytes(),
                before_version: member.before_version,
                after_version: (member.member_kind != MutationKind::Delete)
                    .then_some(member.event_id),
                event_id: member.event_id,
            })
            .collect();
        let commit_position = decision.commit_position.ok_or_else(|| {
            StoreError::AtomicStage("committed receipt missing commit position".into())
        })?;
        Ok(AtomicOutcome::Committed(AtomicReceipt {
            atomic_id: decision.atomic_id,
            heap_id: prepare.heap_id,
            content_root: prepare.content_root,
            commit_position,
            durability: decision.durability,
            members,
            decision_hash: decision_hash(&decision)
                .map_err(|error| StoreError::AtomicStage(error.to_string()))?,
            replayed,
        }))
    }

    fn publish_decision(&mut self, atomic_id: AtomicId) -> Result<(), StoreError> {
        let delta = publication_delta(self.store.paths(), &self.catalog, atomic_id)?;
        self.store.publish_atomic_generation(&delta, false)
    }

    fn members_for_frontier(&self, plan: &AtomicPlan) -> Result<Vec<AtomicMember>, StoreError> {
        plan.mutations()
            .iter()
            .enumerate()
            .map(|(ordinal, mutation)| {
                let ordinal = u32::try_from(ordinal).map_err(|_| {
                    StoreError::AtomicStage("atomic member ordinal overflow".into())
                })?;
                let subject =
                    atomic_subject(plan.heap_id(), mutation.collection_id, &mutation.key)?;
                let observed = self
                    .store
                    .live_event_id(&subject)
                    .and_then(|bytes| VersionId::from_bytes(bytes).ok());
                let before_version = match mutation.kind {
                    MutationKind::Put => observed,
                    MutationKind::Create => None,
                    MutationKind::Replace | MutationKind::Delete => mutation.if_version,
                };
                let after_content_hash = mutation
                    .encoded_value
                    .as_deref()
                    .map(|value| *blake3::hash(value).as_bytes());
                Ok(AtomicMember {
                    atomic_id: plan.atomic_id(),
                    ordinal,
                    object_identity: ObjectIdentity::new(
                        mutation.collection_id,
                        mutation.key.clone(),
                    ),
                    member_kind: mutation.kind,
                    before_version,
                    after_content_hash,
                    event_id: atomic_member_event_id(plan.atomic_id(), ordinal)?,
                })
            })
            .collect()
    }

    fn frontier_for_plan(&self, plan: &AtomicPlan) -> Result<[u8; 32], StoreError> {
        let mut targets = Vec::new();
        for read in plan.reads() {
            targets.push(atomic_subject(
                plan.heap_id(),
                read.collection_id,
                &read.key,
            )?);
        }
        for predicate in plan.predicates() {
            if let (Some(collection), Some(key)) = (predicate.collection_id, predicate.key.as_ref())
            {
                targets.push(atomic_subject(plan.heap_id(), collection, key)?);
            }
        }
        for mutation in plan.mutations() {
            targets.push(atomic_subject(
                plan.heap_id(),
                mutation.collection_id,
                &mutation.key,
            )?);
        }
        targets.sort();
        targets.dedup();
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"RESIDIUUM-ATOMIC-HEAP-FRONTIER-V1");
        for subject in targets {
            hasher.update(&(subject.len() as u32).to_be_bytes());
            hasher.update(&subject);
            match self.store.live_event_id(&subject) {
                Some(version) => {
                    hasher.update(&[1]);
                    hasher.update(&version);
                }
                None => {
                    hasher.update(&[0]);
                }
            }
        }
        Ok(*hasher.finalize().as_bytes())
    }

    fn validate_at_frontier(
        &self,
        plan: &AtomicPlan,
    ) -> Result<Option<AtomicAbortReason>, StoreError> {
        for read in plan.reads() {
            let subject = atomic_subject(plan.heap_id(), read.collection_id, &read.key)?;
            let observed = self
                .store
                .live_event_id(&subject)
                .and_then(|bytes| VersionId::from_bytes(bytes).ok());
            if observed != read.observed_version {
                return Ok(Some(AtomicAbortReason::PreconditionConflict));
            }
        }
        for predicate in plan.predicates() {
            if predicate.kind == PredicateKind::HeapAuthorityRevision {
                // Heap authority state is not yet bound into the Store's
                // universal publication frontier. Silently accepting this
                // predicate would claim validation that never happened.
                return Ok(Some(AtomicAbortReason::RuleRejected));
            }
            if !predicate.kind.is_public_builder_assert() {
                return Ok(Some(AtomicAbortReason::RuleRejected));
            }
            let (Some(collection), Some(key)) = (predicate.collection_id, predicate.key.as_ref())
            else {
                return Ok(Some(AtomicAbortReason::PreconditionConflict));
            };
            let subject = atomic_subject(plan.heap_id(), collection, key)?;
            let observed = self
                .store
                .live_event_id(&subject)
                .and_then(|bytes| VersionId::from_bytes(bytes).ok());
            let valid = match predicate.kind {
                PredicateKind::AssertAbsent => observed.is_none(),
                PredicateKind::AssertPresent => observed.is_some(),
                PredicateKind::AssertVersion => observed == predicate.version,
                _ => false,
            };
            if !valid {
                return Ok(Some(AtomicAbortReason::PreconditionConflict));
            }
        }
        for mutation in plan.mutations() {
            let subject = atomic_subject(plan.heap_id(), mutation.collection_id, &mutation.key)?;
            let observed = self
                .store
                .live_event_id(&subject)
                .and_then(|bytes| VersionId::from_bytes(bytes).ok());
            let valid = match mutation.kind {
                MutationKind::Create => observed.is_none(),
                MutationKind::Put => true,
                MutationKind::Replace | MutationKind::Delete => observed == mutation.if_version,
            };
            if !valid {
                return Ok(Some(AtomicAbortReason::PreconditionConflict));
            }
        }
        if !plan.active_rule_revisions().is_empty() {
            return Ok(Some(AtomicAbortReason::RuleRejected));
        }
        Ok(None)
    }

    fn duplicate_target() -> StoreError {
        AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget).into()
    }

    fn find_staged(
        &self,
        atomic_id: AtomicId,
        ordinal: u32,
    ) -> Option<&residiuum_atomics::StagedMember> {
        self.heap
            .inspect_staged(atomic_id)
            .and_then(|ms| ms.iter().find(|s| s.member.ordinal == ordinal))
    }

    fn catalog_member(&self, atomic_id: AtomicId, ordinal: u32) -> Option<&AtomicMember> {
        self.catalog
            .members
            .get(&atomic_id)
            .and_then(|ms| ms.iter().find(|m| m.ordinal == ordinal))
    }

    fn existing_payload_conflicts(&self, member: &AtomicMember, payload: &[u8]) -> bool {
        if let Some(staged) = self.find_staged(member.atomic_id, member.ordinal) {
            return staged.member != *member || staged.payload.as_slice() != payload;
        }
        if let Some(stored) = self
            .catalog
            .payloads
            .get(&(member.atomic_id, member.ordinal))
        {
            if stored.as_slice() != payload {
                return true;
            }
            if let Some(stored_member) = self.catalog_member(member.atomic_id, member.ordinal) {
                return stored_member != member;
            }
        }
        false
    }

    /// `Some(Ok(()))` exact retry, `Some(Err(_))` identity conflict, `None` first write.
    fn existing_chunk_decision(
        &self,
        member: &AtomicMember,
        index: u32,
        body: &[u8],
    ) -> Option<Result<(), StoreError>> {
        let staged = self.find_staged(member.atomic_id, member.ordinal)?;
        if staged.member != *member {
            return Some(Err(Self::duplicate_target()));
        }
        if let Some(chunks) = staged.chunks.as_ref() {
            match chunks.get(index as usize) {
                Some(Some(existing)) if existing.as_slice() == body => {
                    return Some(Ok(()));
                }
                Some(Some(_)) => return Some(Err(Self::duplicate_target())),
                Some(None) | None => return None,
            }
        }
        if !staged.payload_complete {
            return None;
        }
        let plan = self.heap.chunk_plan(member.atomic_id, member.ordinal)?;
        if index >= plan.total {
            return Some(Err(AtomicsError::Refused(
                AtomicRefuseReason::MalformedInput,
            )
            .into()));
        }
        if *blake3::hash(body).as_bytes() != plan.chunk_hashes[index as usize] {
            return Some(Err(Self::duplicate_target()));
        }
        Some(Ok(()))
    }

    fn persist_chunk_plan(
        &mut self,
        atomic_id: AtomicId,
        ordinal: u32,
        plan: &ChunkPlan,
    ) -> Result<(), StoreError> {
        let mut candidate = self.catalog.clone();
        candidate
            .chunk_plans
            .insert((atomic_id, ordinal), plan.clone());
        let body_len = encode_stage_chunk_plan(atomic_id, ordinal, plan).len() as u64;
        self.admit_catalog_change(&candidate, body_len)?;
        crate::failpoint::hit("store.atomic.chunk_plan.before_append")?;
        let body = encode_stage_chunk_plan(atomic_id, ordinal, plan);
        self.store.append_unindexed_atomic_frame(
            FrameKind::PayloadChunk,
            EMPTY_ENVELOPE,
            &body,
            chunk_plan_event_id(atomic_id, ordinal),
        )?;
        crate::failpoint::hit("store.atomic.chunk_plan.after_append")?;
        self.catalog
            .chunk_plans
            .insert((atomic_id, ordinal), plan.clone());
        persist_live_checkpoint(
            self.store.paths(),
            &self.catalog,
            &mut self.covered,
            self.limits,
        )?;
        crate::failpoint::hit("store.atomic.chunk_plan.after_checkpoint")?;
        Ok(())
    }

    fn persist_chunk_body(
        &mut self,
        member: &AtomicMember,
        index: u32,
        body: &[u8],
    ) -> Result<(), StoreError> {
        if !self
            .catalog
            .has_chunk(member.atomic_id, member.ordinal, index)
        {
            self.admit_payload_bytes(body.len() as u64)?;
        }
        let start = self.active_len();
        let encoded = encode_stage_chunk_body(member.atomic_id, member.ordinal, index, body);
        let mut candidate = self.catalog.clone();
        candidate
            .chunks
            .insert((member.atomic_id, member.ordinal, index), body.to_vec());
        candidate.chunk_refs.insert(
            (member.atomic_id, member.ordinal, index),
            self.candidate_body_ref(start, encoded.len()),
        );
        self.admit_catalog_change(&candidate, encoded.len() as u64)?;
        crate::failpoint::hit("store.atomic.chunk_body.before_append")?;
        self.store.append_unindexed_atomic_frame(
            FrameKind::PayloadChunk,
            EMPTY_ENVELOPE,
            &encoded,
            chunk_body_event_id(member.atomic_id, member.ordinal, index),
        )?;
        crate::failpoint::hit("store.atomic.chunk_body.after_append")?;
        self.catalog
            .chunks
            .insert((member.atomic_id, member.ordinal, index), body.to_vec());
        self.note_chunk_ref(member.atomic_id, member.ordinal, index, start, &encoded);
        persist_live_checkpoint(
            self.store.paths(),
            &self.catalog,
            &mut self.covered,
            self.limits,
        )?;
        crate::failpoint::hit("store.atomic.chunk_body.after_checkpoint")?;
        Ok(())
    }

    fn persist_completed_payload_if_missing(
        &mut self,
        member: &AtomicMember,
    ) -> Result<(), StoreError> {
        if self.catalog.has_payload(member.atomic_id, member.ordinal) {
            return Ok(());
        }
        let Some(staged) = self.find_staged(member.atomic_id, member.ordinal) else {
            return Ok(());
        };
        if !staged.payload_complete {
            return Ok(());
        }
        let payload = staged.payload.clone();
        self.persist_payload(member, &payload, StagePersistMode::StableCheckpointed)
    }

    fn admit_new_atomic(&self) -> Result<(), StoreError> {
        let next = self.catalog.prepares.len().saturating_add(1) as u32;
        if next > self.limits.max_atomics {
            return Err(StoreError::AtomicStage(format!(
                "atomic stage admission atomics {next} exceeds limit {}",
                self.limits.max_atomics
            )));
        }
        Ok(())
    }

    fn admit_payload_bytes(&self, extra: u64) -> Result<(), StoreError> {
        let next = self.catalog.retained_payload_bytes().saturating_add(extra);
        if next > self.limits.max_payload_bytes {
            return Err(StoreError::AtomicStage(format!(
                "atomic stage admission payload bytes {next} exceeds limit {}",
                self.limits.max_payload_bytes
            )));
        }
        Ok(())
    }

    fn active_len(&self) -> u64 {
        fs::metadata(self.coordinator_active_path())
            .map(|m| m.len())
            .unwrap_or(0)
    }

    fn coordinator_active_path(&self) -> PathBuf {
        self.store
            .paths()
            .active_segment_for_shard(0, self.store.writer_shards())
    }

    fn note_written_ref(&self, start: u64) -> Option<BodyRef> {
        let end = self.active_len();
        if end < start {
            return None;
        }
        let path = self.coordinator_active_path();
        let mut file = fs::File::open(&path).ok()?;
        use std::io::{Read, Seek, SeekFrom};
        file.seek(SeekFrom::Start(start)).ok()?;
        let mut bytes = vec![0u8; (end - start) as usize];
        file.read_exact(&mut bytes).ok()?;
        Some(BodyRef {
            rel_path: rel_path(self.store.paths(), &path),
            offset: start,
            len: bytes.len() as u32,
            hash: *blake3::hash(&bytes).as_bytes(),
        })
    }

    fn note_payload_ref(&mut self, id: AtomicId, ordinal: u32, start: u64, _body: &[u8]) {
        if let Some(refer) = self.note_written_ref(start) {
            self.catalog.payload_refs.insert((id, ordinal), refer);
        }
    }

    fn note_chunk_ref(&mut self, id: AtomicId, ordinal: u32, index: u32, start: u64, _body: &[u8]) {
        if let Some(refer) = self.note_written_ref(start) {
            self.catalog.chunk_refs.insert((id, ordinal, index), refer);
        }
    }

    fn persist_prepare(
        &mut self,
        prepare: &AtomicPrepare,
        intended_members: u32,
        mode: StagePersistMode,
    ) -> Result<(), StoreError> {
        let _seq = self.catalog.assign_coord(prepare.atomic_id)?;
        let mut candidate = self.catalog.clone();
        candidate
            .prepares
            .insert(prepare.atomic_id, prepare.clone());
        candidate
            .evidence_heaps
            .insert(prepare.atomic_id, prepare.heap_id);
        candidate.prepare_batch.insert(prepare.atomic_id);
        candidate
            .intended_members
            .insert(prepare.atomic_id, intended_members);
        let encoded_prepare =
            encode_prepare(prepare).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        self.admit_catalog_change(&candidate, encoded_prepare.len() as u64)?;
        if mode == StagePersistMode::StableCheckpointed {
            persist_live_checkpoint(
                self.store.paths(),
                &self.catalog,
                &mut self.covered,
                self.limits,
            )?;
        }
        crate::failpoint::hit("store.atomic.prepare.before_append")?;
        let envelope = encode_atomic_prepare_envelope(
            prepare.heap_id.as_bytes(),
            prepare.atomic_id.as_bytes(),
            prepare.content_root.as_bytes(),
        )
        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let body = encoded_prepare;
        let mut event_id = [0u8; 16];
        event_id.copy_from_slice(&prepare.atomic_id.as_bytes()[..16]);
        match mode {
            StagePersistMode::StableCheckpointed => self.store.append_unindexed_atomic_frame(
                FrameKind::BatchPrepare,
                &envelope,
                &body,
                event_id,
            )?,
            StagePersistMode::BufferedCohort => self.store.append_buffered_atomic_frame(
                FrameKind::BatchPrepare,
                &envelope,
                &body,
                event_id,
            )?,
        }
        crate::failpoint::hit("store.atomic.prepare.after_append")?;
        self.catalog
            .prepares
            .insert(prepare.atomic_id, prepare.clone());
        self.catalog
            .evidence_heaps
            .insert(prepare.atomic_id, prepare.heap_id);
        self.catalog.prepare_batch.insert(prepare.atomic_id);
        self.catalog
            .intended_members
            .insert(prepare.atomic_id, intended_members);
        if mode == StagePersistMode::StableCheckpointed {
            persist_live_checkpoint(
                self.store.paths(),
                &self.catalog,
                &mut self.covered,
                self.limits,
            )?;
        }
        crate::failpoint::hit("store.atomic.prepare.after_checkpoint")?;
        Ok(())
    }

    fn persist_member(
        &mut self,
        prepare: &AtomicPrepare,
        member: &AtomicMember,
        mode: StagePersistMode,
    ) -> Result<(), StoreError> {
        let envelope = encode_atomic_member_envelope(
            prepare.heap_id.as_bytes(),
            member.atomic_id.as_bytes(),
            u64::from(member.ordinal),
            prepare.content_root.as_bytes(),
            None,
        )
        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let body = encode_member(member).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let mut candidate = self.catalog.clone();
        let slot = candidate.members.entry(member.atomic_id).or_default();
        if !slot
            .iter()
            .any(|existing| existing.ordinal == member.ordinal)
        {
            slot.push(member.clone());
        }
        self.admit_catalog_change(&candidate, body.len() as u64)?;
        crate::failpoint::hit("store.atomic.member.before_append")?;
        match mode {
            StagePersistMode::StableCheckpointed => self.store.append_unindexed_atomic_frame(
                FrameKind::ItemEvent,
                &envelope,
                &body,
                member.event_id.to_bytes(),
            )?,
            StagePersistMode::BufferedCohort => self.store.append_buffered_atomic_frame(
                FrameKind::ItemEvent,
                &envelope,
                &body,
                member.event_id.to_bytes(),
            )?,
        }
        crate::failpoint::hit("store.atomic.member.after_append")?;
        let slot = self.catalog.members.entry(member.atomic_id).or_default();
        if !slot.iter().any(|m| m.ordinal == member.ordinal) {
            slot.push(member.clone());
        }
        if mode == StagePersistMode::StableCheckpointed {
            persist_live_checkpoint(
                self.store.paths(),
                &self.catalog,
                &mut self.covered,
                self.limits,
            )?;
        }
        crate::failpoint::hit("store.atomic.member.after_checkpoint")?;
        Ok(())
    }

    fn persist_payload(
        &mut self,
        member: &AtomicMember,
        payload: &[u8],
        mode: StagePersistMode,
    ) -> Result<(), StoreError> {
        if !self.catalog.has_payload(member.atomic_id, member.ordinal) {
            self.admit_payload_bytes(payload.len() as u64)?;
        }
        let start = self.active_len();
        let body = encode_stage_payload(member.atomic_id, member.ordinal, payload);
        let mut candidate = self.catalog.clone();
        candidate
            .payloads
            .insert((member.atomic_id, member.ordinal), payload.to_vec());
        candidate.payload_refs.insert(
            (member.atomic_id, member.ordinal),
            self.candidate_body_ref(start, body.len()),
        );
        self.admit_catalog_change(&candidate, body.len() as u64)?;
        crate::failpoint::hit("store.atomic.payload.before_append")?;
        match mode {
            StagePersistMode::StableCheckpointed => self.store.append_unindexed_atomic_frame(
                FrameKind::PayloadChunk,
                EMPTY_ENVELOPE,
                &body,
                payload_event_id(member.atomic_id, member.ordinal),
            )?,
            StagePersistMode::BufferedCohort => self.store.append_buffered_atomic_frame(
                FrameKind::PayloadChunk,
                EMPTY_ENVELOPE,
                &body,
                payload_event_id(member.atomic_id, member.ordinal),
            )?,
        }
        crate::failpoint::hit("store.atomic.payload.after_append")?;
        self.catalog
            .payloads
            .insert((member.atomic_id, member.ordinal), payload.to_vec());
        self.note_payload_ref(member.atomic_id, member.ordinal, start, &body);
        if mode == StagePersistMode::StableCheckpointed {
            persist_live_checkpoint(
                self.store.paths(),
                &self.catalog,
                &mut self.covered,
                self.limits,
            )?;
        }
        crate::failpoint::hit("store.atomic.payload.after_checkpoint")?;
        Ok(())
    }

    fn persist_seal(
        &mut self,
        atomic_id: AtomicId,
        content_root: residiuum_atomics::ContentRoot,
        mode: StagePersistMode,
    ) -> Result<(), StoreError> {
        if self.catalog.is_sealed(atomic_id) {
            return Ok(());
        }
        let body = encode_stage_seal(atomic_id, content_root);
        let mut candidate = self.catalog.clone();
        candidate.seals.insert(atomic_id, content_root);
        self.admit_catalog_change(&candidate, body.len() as u64)?;
        crate::failpoint::hit("store.atomic.seal.before_append")?;
        self.store.append_unindexed_atomic_frame(
            FrameKind::PayloadChunk,
            EMPTY_ENVELOPE,
            &body,
            seal_event_id(atomic_id),
        )?;
        self.catalog.seals.insert(atomic_id, content_root);
        crate::failpoint::hit("store.atomic.seal.after_append")?;
        if mode == StagePersistMode::StableCheckpointed {
            persist_live_checkpoint(
                self.store.paths(),
                &self.catalog,
                &mut self.covered,
                self.limits,
            )?;
        }
        crate::failpoint::hit("store.atomic.seal.after_checkpoint")?;
        Ok(())
    }

    fn candidate_body_ref(&self, start: u64, body_len: usize) -> BodyRef {
        BodyRef {
            rel_path: rel_path(self.store.paths(), &self.coordinator_active_path()),
            offset: start,
            // The frame is larger than its body, but encoded checkpoint size
            // depends only on this fixed-width field, not its value.
            len: u32::try_from(body_len).unwrap_or(u32::MAX),
            hash: [0; 32],
        }
    }

    fn admit_catalog_change(
        &self,
        candidate: &StageCatalog,
        append_body_bytes: u64,
    ) -> Result<(), StoreError> {
        let checkpoint = checkpoint_encoded_len(candidate, &self.covered)?;
        // A store frame adds a bounded header/envelope around the body. The
        // frontier needs one hash per crossed 64-KiB block plus the remainder.
        let frontier_growth =
            (append_body_bytes.saturating_add(512) / (64 * 1024) + 2).saturating_mul(32);
        let durable = checkpoint.saturating_add(frontier_growth);
        if durable > self.limits.max_checkpoint_bytes {
            return Err(StoreError::AtomicStage(format!(
                "atomic stage admission checkpoint bytes {durable} exceeds limit {}",
                self.limits.max_checkpoint_bytes
            )));
        }
        let work = candidate.work_bytes();
        if work > self.limits.max_work_bytes {
            return Err(StoreError::AtomicStage(format!(
                "atomic stage admission work bytes {work} exceeds limit {}",
                self.limits.max_work_bytes
            )));
        }
        Ok(())
    }
}

fn decision_event_id(atomic_id: AtomicId) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.decision");
    hasher.update(atomic_id.as_bytes());
    let hash = hasher.finalize();
    let mut event_id = [0u8; 16];
    event_id.copy_from_slice(&hash.as_bytes()[..16]);
    event_id
}

fn atomic_member_event_id(atomic_id: AtomicId, ordinal: u32) -> Result<VersionId, StoreError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"RESIDIUUM-ATOMIC-MEMBER-EVENT-V1");
    hasher.update(atomic_id.as_bytes());
    hasher.update(&ordinal.to_be_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    VersionId::from_bytes(bytes).map_err(|e| StoreError::AtomicStage(e.to_string()))
}

fn atomic_subject(
    heap_id: HeapId,
    collection_id: residiuum_atomics::CollectionId,
    key: &residiuum_atomics::CanonicalKey,
) -> Result<Vec<u8>, StoreError> {
    encode_subject_v2(
        heap_id.as_bytes(),
        SubjectObjectKind::Collection,
        collection_id.as_bytes(),
        &key.subject_bytes(),
    )
    .map_err(|e| StoreError::AtomicStage(format!("atomic SubjectV2 encode: {e}")))
}

/// Authenticate and fully resolve one committed decision into a private
/// publication delta. No live projection is touched until this succeeds.
fn publication_delta(
    paths: &crate::layout::StorePaths,
    catalog: &StageCatalog,
    atomic_id: AtomicId,
) -> Result<Vec<AtomicPublishMember>, StoreError> {
    if catalog.blocked.contains(&atomic_id) {
        return Err(StoreError::AtomicStage(
            "committed Atomic publication blocked by damaged evidence".into(),
        ));
    }
    let decision = catalog
        .decisions
        .get(&atomic_id)
        .ok_or_else(|| StoreError::AtomicStage("publication without decision".into()))?;
    if decision.decision != DecisionCode::Committed {
        return Err(StoreError::AtomicStage(
            "not-committed Atomic cannot be published".into(),
        ));
    }
    let commit_position = decision
        .commit_position
        .filter(|position| *position != 0)
        .ok_or_else(|| StoreError::AtomicStage("committed decision without position".into()))?;
    let prepare = catalog
        .prepares
        .get(&atomic_id)
        .ok_or_else(|| StoreError::AtomicStage("committed decision without prepare".into()))?;
    if catalog.seals.get(&atomic_id) != Some(&prepare.content_root)
        || prepare_hash(prepare).map_err(|e| StoreError::AtomicStage(e.to_string()))?
            != decision.prepare_hash
    {
        return Err(StoreError::AtomicStage(
            "committed Atomic prepare or stable boundary does not verify".into(),
        ));
    }
    let mut members = catalog.members.get(&atomic_id).cloned().unwrap_or_default();
    members.sort_by_key(|member| member.ordinal);
    if members.len() != decision.member_count as usize
        || !members_match_prepare(prepare, &members)
        || ordered_member_manifest_root(prepare.heap_id, &members)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?
            != decision.member_root
    {
        return Err(StoreError::AtomicStage(
            "committed Atomic member manifest does not verify".into(),
        ));
    }

    let mut delta = Vec::with_capacity(members.len());
    let frontiers = catalog.order_frontiers.get(&atomic_id).ok_or_else(|| {
        StoreError::AtomicStage("committed Atomic lacks its durable order frontier".into())
    })?;
    for member in members {
        let subject = atomic_subject(
            prepare.heap_id,
            member.object_identity.collection_id,
            &member.object_identity.key,
        )?;
        if frontiers.is_empty() {
            return Err(StoreError::AtomicStage(
                "Atomic order frontier has no writer shards".into(),
            ));
        }
        let shard = crate::store::subject_writer_shard(&subject, frontiers.len());
        let order_frontier = frontiers.get(shard).copied().ok_or_else(|| {
            StoreError::AtomicStage("Atomic order frontier does not cover target shard".into())
        })?;
        let payload = if member.member_kind == MutationKind::Delete {
            None
        } else {
            let body = catalog
                .payload_refs
                .get(&(atomic_id, member.ordinal))
                .cloned()
                .ok_or_else(|| {
                    StoreError::AtomicStage(
                        "committed Atomic member lacks a durable payload locator".into(),
                    )
                })?;
            let bytes = resolve_published_payload(paths, &body, atomic_id, member.ordinal)?;
            if member.after_content_hash != Some(*blake3::hash(&bytes).as_bytes()) {
                return Err(StoreError::AtomicStage(
                    "committed Atomic payload hash does not match member".into(),
                ));
            }
            Some(AtomicValueRef {
                atomic_id,
                ordinal: member.ordinal,
                body,
            })
        };
        delta.push(AtomicPublishMember {
            subject,
            member,
            payload,
            commit_position,
            order_frontier,
        });
    }
    Ok(delta)
}

impl From<AtomicsError> for StoreError {
    fn from(err: AtomicsError) -> Self {
        StoreError::AtomicStage(err.to_string())
    }
}

fn rebuild_heap(
    paths: &crate::layout::StorePaths,
    heap_id: HeapId,
    catalog: &StageCatalog,
) -> Result<StagingHeap, StoreError> {
    let mut heap =
        StagingHeap::new(heap_id, 1).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
    let mut ids: Vec<AtomicId> = catalog.prepares.keys().copied().collect();
    ids.sort_by_key(|id| catalog.coord_seq.get(id).copied().unwrap_or(u64::MAX));
    for atomic_id in ids {
        if catalog.blocked.contains(&atomic_id) {
            continue;
        }
        let prepare = &catalog.prepares[&atomic_id];
        if prepare.heap_id != heap_id {
            continue;
        }
        let members = catalog.members.get(&atomic_id).cloned().unwrap_or_default();
        if !members_match_prepare(prepare, &members) {
            continue;
        }
        let seq = catalog
            .coord_seq
            .get(&atomic_id)
            .copied()
            .and_then(CoordinatorSeq::from_raw)
            .ok_or_else(|| {
                StoreError::AtomicStage("prepare missing durable coordinator sequence".into())
            })?;
        heap.install_prepared(seq, atomic_id, prepare.content_root, &members)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        for member in &members {
            if let Some(plan) = catalog.chunk_plans.get(&(atomic_id, member.ordinal)) {
                heap.commit_chunk_manifest(atomic_id, member.ordinal, plan.clone())
                    .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
                let mut indexes: Vec<u32> = catalog
                    .chunks
                    .keys()
                    .chain(catalog.chunk_refs.keys())
                    .filter(|(id, ord, _)| *id == atomic_id && *ord == member.ordinal)
                    .map(|(_, _, idx)| *idx)
                    .collect();
                indexes.sort_unstable();
                indexes.dedup();
                for index in indexes {
                    let body =
                        resolve_chunk_body(paths, catalog, atomic_id, member.ordinal, index)?
                            .ok_or_else(|| {
                                StoreError::AtomicStage("chunk index missing after key scan".into())
                            })?;
                    heap.append_chunk(member.clone(), index, body)
                        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
                }
            } else if let Some(payload) =
                resolve_payload_body(paths, catalog, atomic_id, member.ordinal)?
            {
                heap.append_staged(member.clone(), payload)
                    .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
            }
        }
        if let Some(root) = catalog.seals.get(&atomic_id) {
            if *root != prepare.content_root {
                continue;
            }
            heap.seal_member_boundary(atomic_id)
                .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        }
    }
    Ok(heap)
}
