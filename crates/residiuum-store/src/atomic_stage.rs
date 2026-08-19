//! Store-owned Atomic staging (CR-ATMR4-005 / CR-ATMR5-001).
//!
//! One durable authority: the store segment under the exclusive writer.
//! `StagingHeap` is the in-memory model only. The peer `DurableLane` is not
//! opened, created, or consulted. Ordinary `get` / scan / history stay empty.
//! The live catalogue is opened once from a store-owned checkpoint plus tails.

use crate::atomic_stage_classify::StageFindings;
use crate::atomic_stage_media::{
    encode_stage_payload, encode_stage_prepare, encode_stage_seal, payload_event_id,
    prepare_event_id, seal_event_id, StageCatalog,
};
use crate::atomic_stage_recover::{
    open_catalog, persist_live_checkpoint, AtomicStageLimits, AtomicStageOpenReport, CoveredFile,
};
use crate::error::StoreError;
use crate::store::Store;
use residiuum_atomics::{
    encode_member, encode_prepare, members_match_prepare, prepare_from_closed_plan, AtomicId,
    AtomicMember, AtomicPlan, AtomicPrepare, AtomicRefuseReason, AtomicsError, ChunkPlan,
    CoordinatorSeq, HeapId, MemberPhase, PlacementManifest, StagingHeap,
};
use residiuum_format::{
    encode_atomic_member_envelope, encode_atomic_prepare_envelope, FrameKind, EMPTY_ENVELOPE,
};
use std::path::PathBuf;

/// Store-owned handle to staged Atomic evidence.
pub struct StoreAtomicStage<'a> {
    store: &'a mut Store,
    heap: StagingHeap,
    catalog: StageCatalog,
    covered: Vec<CoveredFile>,
    report: AtomicStageOpenReport,
    findings: StageFindings,
}

impl Store {
    /// Open the store-owned Atomic stage. Requires the exclusive writer lock.
    pub fn atomic_stage(&mut self) -> Result<StoreAtomicStage<'_>, StoreError> {
        if !self.holds_writer_lock() {
            return Err(StoreError::AtomicStage(
                "atomic stage requires the store writer lock".into(),
            ));
        }
        let heap_id = HeapId::from_bytes(self.store_id())
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let opened = open_catalog(self.paths(), AtomicStageLimits::prototype(), heap_id)?;
        let heap = rebuild_heap(heap_id, &opened.catalog)?;
        self.record_atomic_stage_open(opened.report);
        Ok(StoreAtomicStage {
            store: self,
            heap,
            catalog: opened.catalog,
            covered: opened.covered,
            report: opened.report,
            findings: opened.findings,
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

    /// Validate a closed plan, persist it on the store segment, then apply.
    pub fn begin_prepare(
        &mut self,
        plan: &AtomicPlan,
        frontier: [u8; 32],
        members: &[AtomicMember],
    ) -> Result<(CoordinatorSeq, PlacementManifest), StoreError> {
        let prepare = prepare_from_closed_plan(plan, frontier, members)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        if self.catalog.blocked.contains(&prepare.atomic_id) {
            return Err(StoreError::AtomicStage(
                "atomic identity is blocked by conflicting or damaged evidence".into(),
            ));
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
        if let Some(stored) = self.catalog.prepares.get(&prepare.atomic_id) {
            if stored != &prepare {
                return Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into());
            }
        } else {
            self.persist_prepare(&prepare)?;
        }
        for member in members {
            if !self.catalog.has_member(member.atomic_id, member.ordinal) {
                self.persist_member(&prepare, member)?;
            }
        }
        self.heap
            .begin_prepare(prepare.atomic_id, prepare.content_root, members)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))
    }

    /// Persist a staged payload on the store segment. Not a `put`.
    pub fn append_staged(
        &mut self,
        member: AtomicMember,
        payload: Vec<u8>,
    ) -> Result<(), StoreError> {
        self.heap
            .check_append_staged(&member, &payload)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        if !self.catalog.has_payload(member.atomic_id, member.ordinal) {
            self.persist_payload(&member, &payload)?;
        }
        if self
            .heap
            .inspect_staged(member.atomic_id)
            .and_then(|ms| ms.iter().find(|s| s.member.ordinal == member.ordinal))
            .is_some()
        {
            return Ok(());
        }
        self.heap
            .append_staged(member, payload)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))
    }

    /// Record the chunk map in the in-memory model only (CR-ATMR4-004 persists it).
    pub fn commit_chunk_manifest(
        &mut self,
        atomic_id: AtomicId,
        ordinal: u32,
        plan: ChunkPlan,
    ) -> Result<(), StoreError> {
        self.heap
            .commit_chunk_manifest(atomic_id, ordinal, plan)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))
    }

    /// Install one chunk in the model. When the payload first completes, persist
    /// the member payload on the store. Retry writes a missing store payload
    /// even if the in-memory member is already complete.
    pub fn append_chunk(
        &mut self,
        member: AtomicMember,
        index: u32,
        body: Vec<u8>,
    ) -> Result<(), StoreError> {
        let atomic_id = member.atomic_id;
        let ordinal = member.ordinal;
        let already = self
            .heap
            .inspect_staged(atomic_id)
            .and_then(|ms| ms.iter().find(|s| s.member.ordinal == ordinal))
            .is_some_and(|s| s.payload_complete);
        if !already {
            self.heap
                .check_append_chunk(&member, index, &body)
                .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
            self.heap
                .append_chunk(member.clone(), index, body)
                .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        }
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
        if !self
            .heap
            .lifecycle(atomic_id)
            .is_some_and(|life| life.members == MemberPhase::DurableInvisible)
        {
            self.heap
                .seal_member_boundary(atomic_id)
                .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        }
        if !self.catalog.is_sealed(atomic_id) {
            let content_root = self
                .heap
                .placement(atomic_id)
                .ok_or_else(|| StoreError::AtomicStage("seal without prepare".into()))?
                .content_root();
            self.persist_seal(atomic_id, content_root)?;
        }
        Ok(())
    }

    fn persist_prepare(&mut self, prepare: &AtomicPrepare) -> Result<(), StoreError> {
        let envelope = encode_atomic_prepare_envelope(
            prepare.heap_id.as_bytes(),
            prepare.atomic_id.as_bytes(),
            prepare.content_root.as_bytes(),
        )
        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let body = encode_prepare(prepare).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let mut event_id = [0u8; 16];
        event_id.copy_from_slice(&prepare.atomic_id.as_bytes()[..16]);
        self.store.append_unindexed_atomic_frame(
            FrameKind::BatchPrepare,
            &envelope,
            &body,
            event_id,
        )?;
        // Open rebuild keeps ItemEvent/PayloadChunk only. Persist the prepare
        // again as a store-stage chunk so reopen does not lose authority.
        let stage_body = encode_stage_prepare(prepare)?;
        self.store.append_unindexed_atomic_frame(
            FrameKind::PayloadChunk,
            EMPTY_ENVELOPE,
            &stage_body,
            prepare_event_id(prepare.atomic_id),
        )?;
        self.catalog
            .prepares
            .insert(prepare.atomic_id, prepare.clone());
        persist_live_checkpoint(self.store.paths(), &self.catalog, &mut self.covered)
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
        self.store.append_unindexed_atomic_frame(
            FrameKind::ItemEvent,
            &envelope,
            &body,
            member.event_id.to_bytes(),
        )?;
        let slot = self.catalog.members.entry(member.atomic_id).or_default();
        if !slot.iter().any(|m| m.ordinal == member.ordinal) {
            slot.push(member.clone());
        }
        persist_live_checkpoint(self.store.paths(), &self.catalog, &mut self.covered)
    }

    fn persist_payload(&mut self, member: &AtomicMember, payload: &[u8]) -> Result<(), StoreError> {
        let body = encode_stage_payload(member.atomic_id, member.ordinal, payload);
        self.store.append_unindexed_atomic_frame(
            FrameKind::PayloadChunk,
            EMPTY_ENVELOPE,
            &body,
            payload_event_id(member.atomic_id, member.ordinal),
        )?;
        self.catalog
            .payloads
            .insert((member.atomic_id, member.ordinal), payload.to_vec());
        persist_live_checkpoint(self.store.paths(), &self.catalog, &mut self.covered)
    }

    fn persist_seal(
        &mut self,
        atomic_id: AtomicId,
        content_root: residiuum_atomics::ContentRoot,
    ) -> Result<(), StoreError> {
        let body = encode_stage_seal(atomic_id, content_root);
        self.store.append_unindexed_atomic_frame(
            FrameKind::PayloadChunk,
            EMPTY_ENVELOPE,
            &body,
            seal_event_id(atomic_id),
        )?;
        self.catalog.seals.insert(atomic_id, content_root);
        persist_live_checkpoint(self.store.paths(), &self.catalog, &mut self.covered)
    }
}

impl From<AtomicsError> for StoreError {
    fn from(err: AtomicsError) -> Self {
        StoreError::AtomicStage(err.to_string())
    }
}

fn rebuild_heap(heap_id: HeapId, catalog: &StageCatalog) -> Result<StagingHeap, StoreError> {
    let mut heap =
        StagingHeap::new(heap_id, 1).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
    for (atomic_id, prepare) in &catalog.prepares {
        if catalog.blocked.contains(atomic_id) {
            continue;
        }
        if prepare.heap_id != heap_id {
            continue;
        }
        let members = catalog.members.get(atomic_id).cloned().unwrap_or_default();
        if !members_match_prepare(prepare, &members) {
            return Err(StoreError::AtomicStage(format!(
                "atomic {atomic_id:?} members do not match prepare; identity is not reusable"
            )));
        }
        heap.begin_prepare(*atomic_id, prepare.content_root, &members)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        for member in &members {
            if let Some(payload) = catalog.payloads.get(&(*atomic_id, member.ordinal)) {
                heap.append_staged(member.clone(), payload.clone())
                    .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
            }
        }
        if let Some(root) = catalog.seals.get(atomic_id) {
            if *root != prepare.content_root {
                continue;
            }
            heap.seal_member_boundary(*atomic_id)
                .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        }
    }
    Ok(heap)
}
