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
    chunk_body_event_id, chunk_plan_event_id, encode_stage_chunk_body, encode_stage_chunk_plan,
    encode_stage_payload, encode_stage_seal, payload_event_id, seal_event_id, BodyRef,
    StageCatalog,
};
use crate::atomic_stage_recover::{
    checkpoint_encoded_len, open_catalog, persist_live_checkpoint, rel_path, resolve_chunk_body,
    resolve_payload_body, verify_missing_coverage, AtomicStageLimits, AtomicStageOpenReport,
    CoveredFile,
};
use crate::error::StoreError;
use crate::store::Store;
use residiuum_atomics::{
    encode_decision, encode_member, encode_prepare, members_match_prepare,
    ordered_member_manifest_root, prepare_from_closed_plan, prepare_hash, AtomicDecision, AtomicId,
    AtomicMember, AtomicPlan, AtomicPrepare, AtomicRefuseReason, AtomicsError, ChunkPlan,
    CoordinatorSeq, HeapId, MemberPhase, PlacementManifest, StagingHeap,
};
use residiuum_format::{
    encode_atomic_commit_envelope, encode_atomic_member_envelope, encode_atomic_prepare_envelope,
    FrameKind, EMPTY_ENVELOPE,
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
                self.persist_prepare(&prepare, members.len() as u32)?;
            }
        } else {
            self.admit_new_atomic()?;
            self.persist_prepare(&prepare, members.len() as u32)?;
        }
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
                self.persist_member(&prepare, member)?;
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
        if self.existing_payload_conflicts(&member, &payload) {
            return Err(Self::duplicate_target());
        }
        if self.find_staged(member.atomic_id, member.ordinal).is_some() {
            if !self.catalog.has_payload(member.atomic_id, member.ordinal) {
                self.persist_payload(&member, &payload)?;
            }
            return Ok(());
        }
        self.heap
            .check_append_staged(&member, &payload)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        if !self.catalog.has_payload(member.atomic_id, member.ordinal) {
            self.persist_payload(&member, &payload)?;
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
            self.persist_payload(&member, &payload)?;
        }
        Ok(())
    }

    /// First stable member boundary: persist a store seal, then apply the model.
    pub fn seal_member_boundary(&mut self, atomic_id: AtomicId) -> Result<(), StoreError> {
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
            self.persist_seal(atomic_id, content_root)?;
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
            return Err(StoreError::AtomicStage(
                "decision requires the exact complete member manifest and payloads".into(),
            ));
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
        persist_live_checkpoint(
            self.store.paths(),
            &self.catalog,
            &mut self.covered,
            self.limits,
        )?;
        Ok(decision)
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
        self.persist_payload(member, &payload)
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
        fs::metadata(self.store.paths().active_segment())
            .map(|m| m.len())
            .unwrap_or(0)
    }

    fn note_written_ref(&self, start: u64) -> Option<BodyRef> {
        let end = self.active_len();
        if end < start {
            return None;
        }
        let path = self.store.paths().active_segment();
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
        persist_live_checkpoint(
            self.store.paths(),
            &self.catalog,
            &mut self.covered,
            self.limits,
        )?;
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
        self.store.append_unindexed_atomic_frame(
            FrameKind::BatchPrepare,
            &envelope,
            &body,
            event_id,
        )?;
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
        persist_live_checkpoint(
            self.store.paths(),
            &self.catalog,
            &mut self.covered,
            self.limits,
        )?;
        crate::failpoint::hit("store.atomic.prepare.after_checkpoint")?;
        Ok(())
    }

    fn persist_member(
        &mut self,
        prepare: &AtomicPrepare,
        member: &AtomicMember,
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
        self.store.append_unindexed_atomic_frame(
            FrameKind::ItemEvent,
            &envelope,
            &body,
            member.event_id.to_bytes(),
        )?;
        crate::failpoint::hit("store.atomic.member.after_append")?;
        let slot = self.catalog.members.entry(member.atomic_id).or_default();
        if !slot.iter().any(|m| m.ordinal == member.ordinal) {
            slot.push(member.clone());
        }
        persist_live_checkpoint(
            self.store.paths(),
            &self.catalog,
            &mut self.covered,
            self.limits,
        )?;
        crate::failpoint::hit("store.atomic.member.after_checkpoint")?;
        Ok(())
    }

    fn persist_payload(&mut self, member: &AtomicMember, payload: &[u8]) -> Result<(), StoreError> {
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
        self.store.append_unindexed_atomic_frame(
            FrameKind::PayloadChunk,
            EMPTY_ENVELOPE,
            &body,
            payload_event_id(member.atomic_id, member.ordinal),
        )?;
        crate::failpoint::hit("store.atomic.payload.after_append")?;
        self.catalog
            .payloads
            .insert((member.atomic_id, member.ordinal), payload.to_vec());
        self.note_payload_ref(member.atomic_id, member.ordinal, start, &body);
        persist_live_checkpoint(
            self.store.paths(),
            &self.catalog,
            &mut self.covered,
            self.limits,
        )?;
        crate::failpoint::hit("store.atomic.payload.after_checkpoint")?;
        Ok(())
    }

    fn persist_seal(
        &mut self,
        atomic_id: AtomicId,
        content_root: residiuum_atomics::ContentRoot,
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
        persist_live_checkpoint(
            self.store.paths(),
            &self.catalog,
            &mut self.covered,
            self.limits,
        )?;
        crate::failpoint::hit("store.atomic.seal.after_checkpoint")?;
        Ok(())
    }

    fn candidate_body_ref(&self, start: u64, body_len: usize) -> BodyRef {
        BodyRef {
            rel_path: rel_path(self.store.paths(), &self.store.paths().active_segment()),
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
