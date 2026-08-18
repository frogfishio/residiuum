//! Store-owned Atomic staging (CR-ATMR3-006).
//!
//! The peer `DurableLane` remains the mechanic/test-oracle. This type is the
//! store boundary: construction requires the exclusive writer lock, prepares
//! and members are appended to the live store segment without primary/history
//! /secondary publication, and ordinary `get` / scan / history stay empty.

use crate::error::StoreError;
use crate::store::Store;
use residiuum_atomic_lane::DurableLane;
use residiuum_atomics::{
    encode_member, encode_prepare, prepare_from_closed_plan, AtomicId, AtomicMember, AtomicPlan,
    ChunkPlan, CoordinatorSeq, HeapId, PlacementManifest,
};
use residiuum_format::{encode_atomic_member_envelope, encode_atomic_prepare_envelope, FrameKind};
use std::path::PathBuf;

/// Store-owned handle to staged Atomic evidence.
pub struct StoreAtomicStage<'a> {
    store: &'a mut Store,
    lane: DurableLane,
}

impl Store {
    /// Open the store-owned Atomic stage. Requires the exclusive writer lock.
    pub fn atomic_stage(&mut self) -> Result<StoreAtomicStage<'_>, StoreError> {
        if !self.holds_writer_lock() {
            return Err(StoreError::AtomicStage(
                "atomic stage requires the store writer lock".into(),
            ));
        }
        let root = self.paths().store_info().join("atomic-lane");
        let heap_id = HeapId::from_bytes(self.store_id())
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let lane = if root.join("heap.id").exists() {
            DurableLane::open(&root).map_err(|e| StoreError::AtomicStage(e.to_string()))?
        } else {
            DurableLane::create(&root, heap_id, 1)
                .map_err(|e| StoreError::AtomicStage(e.to_string()))?
        };
        Ok(StoreAtomicStage { store: self, lane })
    }
}

impl StoreAtomicStage<'_> {
    /// Directory of the peer lane under `store-info/`.
    pub fn lane_root(&self) -> PathBuf {
        self.store.paths().store_info().join("atomic-lane")
    }

    /// Validate and persist a closed plan, then append the prepare to the store segment.
    pub fn begin_prepare(
        &mut self,
        plan: &AtomicPlan,
        frontier: [u8; 32],
        members: &[AtomicMember],
    ) -> Result<(CoordinatorSeq, PlacementManifest), StoreError> {
        let prepare = prepare_from_closed_plan(plan, frontier, members)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let out = self
            .lane
            .begin_prepare(plan, frontier, members)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let envelope = encode_atomic_prepare_envelope(
            prepare.heap_id.as_bytes(),
            prepare.atomic_id.as_bytes(),
            prepare.content_root.as_bytes(),
        )
        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let body = encode_prepare(&prepare).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let mut event_id = [0u8; 16];
        event_id.copy_from_slice(&prepare.atomic_id.as_bytes()[..16]);
        self.store.append_unindexed_atomic_frame(
            FrameKind::BatchPrepare,
            &envelope,
            &body,
            event_id,
        )?;
        Ok(out)
    }

    /// Persist a staged member (lane + unindexed store frame). Not a `put`.
    pub fn append_staged(
        &mut self,
        member: AtomicMember,
        payload: Vec<u8>,
    ) -> Result<(), StoreError> {
        let content_root = self
            .lane
            .heap()
            .placement(member.atomic_id)
            .ok_or_else(|| StoreError::AtomicStage("append without prepare".into()))?
            .content_root()
            .to_bytes();
        let heap = self.lane.heap().heap_id();
        let envelope = encode_atomic_member_envelope(
            heap.as_bytes(),
            member.atomic_id.as_bytes(),
            u64::from(member.ordinal),
            &content_root,
            None,
        )
        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let body = encode_member(&member).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let event_id = member.event_id.to_bytes();
        self.lane
            .append_staged(member, payload)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        self.store.append_unindexed_atomic_frame(
            FrameKind::ItemEvent,
            &envelope,
            &body,
            event_id,
        )?;
        Ok(())
    }

    /// Persist the frozen chunk map on the peer lane.
    pub fn commit_chunk_manifest(
        &mut self,
        atomic_id: AtomicId,
        ordinal: u32,
        plan: ChunkPlan,
    ) -> Result<(), StoreError> {
        self.lane
            .commit_chunk_manifest(atomic_id, ordinal, plan)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))
    }

    /// Persist one chunk body. Appends an unindexed member frame only when the
    /// assembled payload first becomes complete.
    pub fn append_chunk(
        &mut self,
        member: AtomicMember,
        index: u32,
        body: Vec<u8>,
    ) -> Result<(), StoreError> {
        let content_root = self
            .lane
            .heap()
            .placement(member.atomic_id)
            .ok_or_else(|| StoreError::AtomicStage("append without prepare".into()))?
            .content_root()
            .to_bytes();
        let heap = self.lane.heap().heap_id();
        let envelope = encode_atomic_member_envelope(
            heap.as_bytes(),
            member.atomic_id.as_bytes(),
            u64::from(member.ordinal),
            &content_root,
            None,
        )
        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let encoded = encode_member(&member).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let event_id = member.event_id.to_bytes();
        let atomic_id = member.atomic_id;
        let ordinal = member.ordinal;
        let was_complete = self
            .lane
            .heap()
            .inspect_staged(atomic_id)
            .and_then(|ms| ms.iter().find(|s| s.member.ordinal == ordinal))
            .is_some_and(|s| s.payload_complete);
        self.lane
            .append_chunk(member, index, body)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let now_complete = self
            .lane
            .heap()
            .inspect_staged(atomic_id)
            .and_then(|ms| ms.iter().find(|s| s.member.ordinal == ordinal))
            .is_some_and(|s| s.payload_complete);
        if !was_complete && now_complete {
            self.store.append_unindexed_atomic_frame(
                FrameKind::ItemEvent,
                &envelope,
                &encoded,
                event_id,
            )?;
        }
        Ok(())
    }

    /// First stable member boundary on the peer lane.
    pub fn seal_member_boundary(&mut self, atomic_id: AtomicId) -> Result<(), StoreError> {
        self.lane
            .seal_member_boundary(atomic_id)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))
    }
}
