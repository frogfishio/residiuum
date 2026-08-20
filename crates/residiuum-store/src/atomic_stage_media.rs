//! Store-segment records for Atomic staging (CR-ATMR4-005).
//!
//! Prepare and member identity live in Atomic frames. Payloads and the first
//! stable boundary live in `PayloadChunk` frames that ordinary recovery skips
//! (`decode_piece_body` / `decode_item_envelope` fail closed). The peer lane
//! is not a second authority.

use crate::atomic_tombstone_index::TombstoneIndexMeta;
use crate::error::StoreError;
use residiuum_atomics::{
    decode_prepare, decode_tombstone, encode_prepare, encode_tombstone, AtomicDecision, AtomicId,
    AtomicMember, AtomicPrepare, ChunkPlan, ContentRoot, DecisionTombstone, HeapId,
};
use std::collections::{BTreeMap, BTreeSet};

const PAYLOAD_MAGIC: &[u8] = b"ATPAY1";
const SEAL_MAGIC: &[u8] = b"ATSEAL1";
const PREPARE_MAGIC: &[u8] = b"ATPREP1";
const CHUNK_PLAN_MAGIC: &[u8] = b"ATMAP1";
const CHUNK_BODY_MAGIC: &[u8] = b"ATCHK1";
const ORDER_FRONTIER_MAGIC: &[u8] = b"ATORD1";
const TOMBSTONE_MAGIC: &[u8] = b"ATTOMB2";

/// Physical Atomic identity. Caller-chosen Atomic IDs are scoped to a Heap;
/// neither component alone is a store-wide identity.
pub(crate) type StageAtomicKey = (HeapId, AtomicId);

pub(crate) fn stage_key(heap_id: HeapId, atomic_id: AtomicId) -> StageAtomicKey {
    (heap_id, atomic_id)
}

/// One writer-shard position captured immediately before an Atomic decision.
/// `next_writer_sequence` is the first sequence that is strictly after the
/// witnessed frontier in `segment_id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AtomicShardFrontier {
    pub shard: u16,
    pub segment_id: [u8; 16],
    pub next_writer_sequence: u64,
}

/// Locator for staged payload/chunk bytes in store media (CR-ATMR6-004).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BodyRef {
    pub rel_path: String,
    pub offset: u64,
    pub len: u32,
    pub hash: [u8; 32],
}

/// Derived locator installed in the live primary projection for a committed
/// Atomic put. The referenced ATPAY1 frame remains authoritative staging
/// material; this locator is rebuilt from the durable decision catalogue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AtomicValueRef {
    pub heap_id: HeapId,
    pub atomic_id: AtomicId,
    pub ordinal: u32,
    pub body: BodyRef,
}

/// One member of a privately assembled Atomic publication generation.
#[derive(Clone, Debug)]
pub(crate) struct AtomicPublishMember {
    pub subject: Vec<u8>,
    pub member: AtomicMember,
    pub payload: Option<AtomicValueRef>,
    pub commit_position: u64,
    pub order_frontier: AtomicShardFrontier,
}

/// Store-owned lifetime authority. `decided_at_unix_s` is retention metadata;
/// the canonical tombstone remains the exact logical decision summary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RetainedDecisionTombstone {
    pub decided_at_unix_s: u64,
    pub tombstone: DecisionTombstone,
}

/// Staging facts recovered from store media only.
#[derive(Clone, Debug, Default)]
pub(crate) struct StageCatalog {
    pub prepares: BTreeMap<StageAtomicKey, AtomicPrepare>,
    pub members: BTreeMap<StageAtomicKey, Vec<AtomicMember>>,
    pub payloads: BTreeMap<(HeapId, AtomicId, u32), Vec<u8>>,
    /// Media locators for payloads (checkpoint authority, not a second copy).
    pub payload_refs: BTreeMap<(HeapId, AtomicId, u32), BodyRef>,
    /// Frozen chunk maps (CR-ATMR5-005).
    pub chunk_plans: BTreeMap<(HeapId, AtomicId, u32), ChunkPlan>,
    /// Verified chunk bodies keyed by (atomic, ordinal, index).
    pub chunks: BTreeMap<(HeapId, AtomicId, u32, u32), Vec<u8>>,
    /// Media locators for chunk bodies.
    pub chunk_refs: BTreeMap<(HeapId, AtomicId, u32, u32), BodyRef>,
    pub seals: BTreeMap<StageAtomicKey, ContentRoot>,
    /// Durable terminal decisions. A committed decision is the ATM-3
    /// linearization point; the checkpoint is only a rebuild accelerator.
    pub decisions: BTreeMap<StageAtomicKey, AtomicDecision>,
    /// Compact terminal authority retained until complete Heap purge.
    pub tombstones: BTreeMap<StageAtomicKey, RetainedDecisionTombstone>,
    /// Authenticated, paged lifetime tombstone accelerator. The checkpoint
    /// binds this descriptor; ATTOMB2 media remains the rebuild authority.
    pub tombstone_index: Option<TombstoneIndexMeta>,
    /// Runtime-only: new authoritative tombstones must be merged before the
    /// next checkpoint can bind the index. Never encoded in the checkpoint.
    pub tombstone_index_dirty: bool,
    /// Next non-zero Heap commit position. Reconstructed above every admitted
    /// committed decision during recovery.
    pub commit_next: BTreeMap<HeapId, u64>,
    /// Per-shard ordinary-write frontier durably covered by each committed
    /// decision. The witness frame precedes the decision stable boundary.
    pub order_frontiers: BTreeMap<StageAtomicKey, Vec<AtomicShardFrontier>>,
    /// Identities that must not be installed or reused (CR-ATMR5-002).
    pub blocked: BTreeSet<StageAtomicKey>,
    /// Prepares observed as format-admitted `BatchPrepare` (CR-ATMR5-006).
    pub prepare_batch: BTreeSet<StageAtomicKey>,
    /// Durable coordinator sequence per Atomic (CR-ATMR5-004).
    pub coord_seq: BTreeMap<StageAtomicKey, u64>,
    /// Next sequence to allocate (strictly above every issued sequence).
    pub coord_next: u64,
    /// Encounter order of prepares (not Atomic-ID sort).
    pub prepare_seen: Vec<StageAtomicKey>,
    /// Persisted honest findings (CR-ATMR6-002). Ordinary reopen must not drop them.
    pub findings: crate::atomic_stage_classify::StageFindings,
    /// Store/coverage degradation. Ordinary reopen must not clear it.
    pub coverage_degraded: bool,
    /// Covered paths that vanished after a checkpoint. Cleared only by scrub.
    pub missing_covered: Vec<String>,
    /// Media made obsolete by an authenticated authority-generation swap.
    ///
    /// These paths may coexist with the replacement after the checkpoint
    /// linearization point (or survive a crash during deletion), but recovery
    /// must never ingest them as a second authority generation.
    pub superseded_media: Vec<String>,
    /// Intended member counts from the issuing prepare (CR-ATMR6-005).
    pub intended_members: BTreeMap<StageAtomicKey, u32>,
}

impl StageCatalog {
    pub(crate) fn is_outstanding(&self, key: StageAtomicKey) -> bool {
        self.prepares.contains_key(&key) && !self.decisions.contains_key(&key)
    }

    pub(crate) fn outstanding_atomics(&self) -> u32 {
        self.prepares
            .keys()
            .filter(|key| self.is_outstanding(**key))
            .count()
            .min(u32::MAX as usize) as u32
    }

    pub(crate) fn outstanding_members(&self) -> u32 {
        self.members
            .iter()
            .filter(|(key, _)| self.is_outstanding(**key))
            .map(|(_, members)| members.len() as u64)
            .sum::<u64>()
            .min(u64::from(u32::MAX)) as u32
    }

    pub(crate) fn outstanding_payload_bytes(&self) -> u64 {
        let inline = self
            .payloads
            .iter()
            .filter(|((heap, atomic, _), _)| self.is_outstanding((*heap, *atomic)))
            .map(|(_, payload)| payload.len() as u64)
            .sum::<u64>()
            .saturating_add(
                self.chunks
                    .iter()
                    .filter(|((heap, atomic, _, _), _)| self.is_outstanding((*heap, *atomic)))
                    .map(|(_, payload)| payload.len() as u64)
                    .sum(),
            );
        let referenced = self
            .payload_refs
            .iter()
            .filter(|((heap, atomic, _), _)| self.is_outstanding((*heap, *atomic)))
            .map(|(_, refer)| u64::from(refer.len))
            .sum::<u64>()
            .saturating_add(
                self.chunk_refs
                    .iter()
                    .filter(|((heap, atomic, _, _), _)| self.is_outstanding((*heap, *atomic)))
                    .map(|(_, refer)| u64::from(refer.len))
                    .sum(),
            );
        inline.max(referenced)
    }

    pub(crate) fn outstanding_work_bytes(&self) -> u64 {
        let atomics = u64::from(self.outstanding_atomics());
        let members = u64::from(self.outstanding_members());
        let references = self
            .payload_refs
            .keys()
            .filter(|(heap, atomic, _)| self.is_outstanding((*heap, *atomic)))
            .count()
            .saturating_add(
                self.chunk_refs
                    .keys()
                    .filter(|(heap, atomic, _, _)| self.is_outstanding((*heap, *atomic)))
                    .count(),
            ) as u64;
        self.outstanding_payload_bytes()
            .saturating_add(atomics.saturating_mul(1024))
            .saturating_add(members.saturating_mul(256))
            .saturating_add(references.saturating_mul(80))
    }

    /// Rebind physical locators after an authenticated byte-identical media
    /// relocation. Logical Atomic identity is never derived from this path.
    pub(crate) fn relocate_body_refs(&mut self, from: &str, to: &str) {
        for refer in self
            .payload_refs
            .values_mut()
            .chain(self.chunk_refs.values_mut())
        {
            if refer.rel_path == from {
                refer.rel_path = to.to_string();
            }
        }
        for missing in &mut self.missing_covered {
            if missing == from {
                *missing = to.to_string();
            }
        }
    }

    pub(crate) fn has_member(&self, key: StageAtomicKey, ordinal: u32) -> bool {
        self.members
            .get(&key)
            .is_some_and(|ms| ms.iter().any(|m| m.ordinal == ordinal))
    }

    pub(crate) fn has_payload(&self, key: StageAtomicKey, ordinal: u32) -> bool {
        self.payloads.contains_key(&(key.0, key.1, ordinal))
    }

    pub(crate) fn has_chunk_plan(&self, key: StageAtomicKey, ordinal: u32) -> bool {
        self.chunk_plans.contains_key(&(key.0, key.1, ordinal))
    }

    pub(crate) fn has_chunk(&self, key: StageAtomicKey, ordinal: u32, index: u32) -> bool {
        self.chunks.contains_key(&(key.0, key.1, ordinal, index))
    }

    pub(crate) fn is_sealed(&self, key: StageAtomicKey) -> bool {
        self.seals.contains_key(&key)
    }

    pub(crate) fn retained_payload_bytes(&self) -> u64 {
        let live: u64 = self
            .payloads
            .values()
            .map(|p| p.len() as u64)
            .sum::<u64>()
            .saturating_add(self.chunks.values().map(|p| p.len() as u64).sum());
        let refs: u64 = self
            .payload_refs
            .values()
            .map(|r| u64::from(r.len))
            .sum::<u64>()
            .saturating_add(self.chunk_refs.values().map(|r| u64::from(r.len)).sum());
        live.max(refs)
    }

    /// True when this catalogue still holds staging facts that rotation or
    /// compaction must not retire (CR-ATMR6-006).
    pub(crate) fn has_outstanding_evidence(&self) -> bool {
        !self.prepares.is_empty()
            || !self.members.is_empty()
            || !self.payloads.is_empty()
            || !self.payload_refs.is_empty()
            || !self.chunk_plans.is_empty()
            || !self.chunks.is_empty()
            || !self.chunk_refs.is_empty()
            || !self.seals.is_empty()
            || !self.decisions.is_empty()
            || !self.tombstones.is_empty()
            || self
                .tombstone_index
                .is_some_and(|index| index.record_count != 0)
            || !self.order_frontiers.is_empty()
            || !self.blocked.is_empty()
            || !self.prepare_batch.is_empty()
            || !self.coord_seq.is_empty()
            || !self.findings.records.is_empty()
            || self.coverage_degraded
            || !self.missing_covered.is_empty()
            || !self.intended_members.is_empty()
    }

    /// Approximate retained catalogue bytes (not a wire encoding).
    pub(crate) fn work_bytes(&self) -> u64 {
        let refs: u64 =
            (self.payload_refs.len() as u64 + self.chunk_refs.len() as u64).saturating_mul(80);
        let plans: u64 = self
            .chunk_plans
            .values()
            .map(|p| 8 + p.chunk_hashes.len() as u64 * 32)
            .sum();
        let members: u64 = self.members.values().map(|ms| ms.len() as u64).sum();
        refs.saturating_add(plans)
            .saturating_add(self.prepares.len() as u64 * 512)
            .saturating_add(members.saturating_mul(256))
            .saturating_add(self.seals.len() as u64 * 64)
            .saturating_add(self.decisions.len() as u64 * 192)
            .saturating_add(self.tombstones.len() as u64 * 160)
            .saturating_add(self.commit_next.len() as u64 * 24)
            .saturating_add(
                self.order_frontiers
                    .values()
                    .map(|frontiers| 40u64.saturating_add(frontiers.len() as u64 * 26))
                    .sum(),
            )
            .saturating_add(self.blocked.len() as u64 * 32)
            .saturating_add(self.prepare_batch.len() as u64 * 32)
            .saturating_add(self.coord_seq.len() as u64 * 40)
            .saturating_add(self.findings.records.len() as u64 * 56)
            .saturating_add(u64::from(self.coverage_degraded))
            .saturating_add(self.missing_covered.iter().map(|p| p.len() as u64).sum())
            .saturating_add(self.superseded_media.iter().map(|p| p.len() as u64).sum())
    }

    /// Allocate the next Heap commit position while the exclusive store writer
    /// is held. Allocation becomes authoritative only when its decision frame
    /// is durably appended.
    pub(crate) fn next_commit_position(&self, heap_id: HeapId) -> Result<u64, StoreError> {
        let next = self.commit_next.get(&heap_id).copied().unwrap_or(1).max(1);
        if self
            .decisions
            .iter()
            .filter(|((decision_heap, _), _)| *decision_heap == heap_id)
            .filter_map(|(_, decision)| decision.commit_position)
            .any(|position| position == next)
        {
            return Err(StoreError::AtomicStage(format!(
                "duplicate Heap commit position {next}"
            )));
        }
        Ok(next)
    }

    /// Reconstruct the high-water mark from authoritative admitted decisions.
    pub(crate) fn reconstruct_commit_next(&mut self) {
        let mut next = BTreeMap::new();
        for ((decision_heap, _), decision) in &self.decisions {
            let Some(position) = decision.commit_position else {
                continue;
            };
            let heap_id = *decision_heap;
            let entry = next.entry(heap_id).or_insert(1u64);
            *entry = (*entry).max(position.saturating_add(1).max(1));
        }
        self.commit_next = next;
    }

    /// Allocate or return the durable coordinator sequence for `atomic_id`.
    pub(crate) fn assign_coord(&mut self, key: StageAtomicKey) -> Result<u64, StoreError> {
        if let Some(seq) = self.coord_seq.get(&key).copied() {
            return Ok(seq);
        }
        let seq = if self.coord_next == 0 {
            1
        } else {
            self.coord_next
        };
        if self.coord_seq.values().any(|&s| s == seq) {
            return Err(StoreError::AtomicStage(format!(
                "duplicate coordinator sequence {seq}"
            )));
        }
        self.coord_seq.insert(key, seq);
        self.coord_next = seq.saturating_add(1);
        if !self.prepare_seen.contains(&key) {
            self.prepare_seen.push(key);
        }
        Ok(seq)
    }

    /// Fill missing sequences in encounter order, never Atomic-ID sort.
    pub(crate) fn assign_missing_coord_seqs(&mut self) {
        let mut ids = self.prepare_seen.clone();
        for id in self.prepares.keys() {
            if !ids.contains(id) {
                ids.push(*id);
            }
        }
        for id in ids {
            if self.blocked.contains(&id) {
                continue;
            }
            let _ = self.assign_coord(id);
        }
    }
}

#[cfg(test)]
mod commit_position_tests {
    use super::*;

    fn heap(first: u8) -> HeapId {
        let mut bytes = [0u8; 16];
        bytes[0] = first;
        HeapId::from_bytes(bytes).unwrap()
    }

    fn atomic(first: u8) -> AtomicId {
        let mut bytes = [0u8; 32];
        bytes[0] = first;
        AtomicId::from_bytes(bytes).unwrap()
    }

    #[test]
    fn heap_commit_positions_are_independent_and_reconstructed() {
        let heap_a = heap(1);
        let heap_b = heap(2);
        let atomic_a = atomic(1);
        let atomic_b = atomic(2);
        let mut catalog = StageCatalog::default();
        catalog.decisions.insert(
            stage_key(heap_a, atomic_a),
            AtomicDecision::committed(atomic_a, [1; 32], [2; 32], 1, 7).unwrap(),
        );
        catalog.decisions.insert(
            stage_key(heap_b, atomic_b),
            AtomicDecision::committed(atomic_b, [3; 32], [4; 32], 1, 2).unwrap(),
        );
        catalog.reconstruct_commit_next();

        assert_eq!(catalog.next_commit_position(heap_a).unwrap(), 8);
        assert_eq!(catalog.next_commit_position(heap_b).unwrap(), 3);
    }

    #[test]
    fn order_frontier_roundtrips_and_refuses_non_contiguous_shards() {
        let heap_id = heap(7);
        let id = atomic(7);
        let frontiers = vec![
            AtomicShardFrontier {
                shard: 0,
                segment_id: [1; 16],
                next_writer_sequence: 9,
            },
            AtomicShardFrontier {
                shard: 1,
                segment_id: [2; 16],
                next_writer_sequence: 4,
            },
        ];
        let encoded = encode_order_frontier(heap_id, id, &frontiers).unwrap();
        match decode_stage_sidecar(&encoded) {
            SidecarDecode::OrderFrontier {
                heap_id: decoded_heap,
                atomic_id,
                frontiers: decoded,
            } => {
                assert_eq!(decoded_heap, heap_id);
                assert_eq!(atomic_id, id);
                assert_eq!(decoded, frontiers);
            }
            other => panic!("unexpected order-frontier decode: {other:?}"),
        }
        let invalid = vec![AtomicShardFrontier {
            shard: 1,
            segment_id: [1; 16],
            next_writer_sequence: 9,
        }];
        assert!(encode_order_frontier(heap_id, id, &invalid).is_err());
    }
}

pub(crate) fn encode_stage_payload(
    heap_id: HeapId,
    atomic_id: AtomicId,
    ordinal: u32,
    payload: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(PAYLOAD_MAGIC.len() + 52 + payload.len());
    out.extend_from_slice(PAYLOAD_MAGIC);
    out.extend_from_slice(heap_id.as_bytes());
    out.extend_from_slice(atomic_id.as_bytes());
    out.extend_from_slice(&ordinal.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

pub(crate) fn encode_order_frontier(
    heap_id: HeapId,
    atomic_id: AtomicId,
    frontiers: &[AtomicShardFrontier],
) -> Result<Vec<u8>, StoreError> {
    let count = u16::try_from(frontiers.len())
        .map_err(|_| StoreError::AtomicStage("too many Atomic writer frontiers".into()))?;
    let mut ordered = frontiers.to_vec();
    ordered.sort_by_key(|frontier| frontier.shard);
    if ordered
        .iter()
        .enumerate()
        .any(|(expected, frontier)| usize::from(frontier.shard) != expected)
    {
        return Err(StoreError::AtomicStage(
            "Atomic writer frontiers must cover contiguous shards".into(),
        ));
    }
    let mut out = Vec::with_capacity(ORDER_FRONTIER_MAGIC.len() + 50 + ordered.len() * 26);
    out.extend_from_slice(ORDER_FRONTIER_MAGIC);
    out.extend_from_slice(heap_id.as_bytes());
    out.extend_from_slice(atomic_id.as_bytes());
    out.extend_from_slice(&count.to_be_bytes());
    for frontier in ordered {
        out.extend_from_slice(&frontier.shard.to_be_bytes());
        out.extend_from_slice(&frontier.segment_id);
        out.extend_from_slice(&frontier.next_writer_sequence.to_be_bytes());
    }
    Ok(out)
}

/// Legacy ATPREP1 writer. New prepares use `BatchPrepare` only (CR-ATMR5-006).
#[allow(dead_code)]
pub(crate) fn encode_stage_prepare(prepare: &AtomicPrepare) -> Result<Vec<u8>, StoreError> {
    let encoded = encode_prepare(prepare).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
    let mut out = Vec::with_capacity(PREPARE_MAGIC.len() + encoded.len());
    out.extend_from_slice(PREPARE_MAGIC);
    out.extend_from_slice(&encoded);
    Ok(out)
}

pub(crate) fn encode_stage_seal(
    heap_id: HeapId,
    atomic_id: AtomicId,
    content_root: ContentRoot,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(SEAL_MAGIC.len() + 80);
    out.extend_from_slice(SEAL_MAGIC);
    out.extend_from_slice(heap_id.as_bytes());
    out.extend_from_slice(atomic_id.as_bytes());
    out.extend_from_slice(content_root.as_bytes());
    out
}

pub(crate) fn encode_stage_chunk_plan(
    heap_id: HeapId,
    atomic_id: AtomicId,
    ordinal: u32,
    plan: &ChunkPlan,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(CHUNK_PLAN_MAGIC.len() + 56 + plan.chunk_hashes.len() * 32);
    out.extend_from_slice(CHUNK_PLAN_MAGIC);
    out.extend_from_slice(heap_id.as_bytes());
    out.extend_from_slice(atomic_id.as_bytes());
    out.extend_from_slice(&ordinal.to_be_bytes());
    out.extend_from_slice(&plan.total.to_be_bytes());
    for hash in &plan.chunk_hashes {
        out.extend_from_slice(hash);
    }
    out
}

pub(crate) fn encode_stage_chunk_body(
    heap_id: HeapId,
    atomic_id: AtomicId,
    ordinal: u32,
    index: u32,
    body: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(CHUNK_BODY_MAGIC.len() + 56 + body.len());
    out.extend_from_slice(CHUNK_BODY_MAGIC);
    out.extend_from_slice(heap_id.as_bytes());
    out.extend_from_slice(atomic_id.as_bytes());
    out.extend_from_slice(&ordinal.to_be_bytes());
    out.extend_from_slice(&index.to_be_bytes());
    out.extend_from_slice(body);
    out
}

pub(crate) fn encode_stage_tombstone(
    heap_id: HeapId,
    retained: RetainedDecisionTombstone,
) -> Result<Vec<u8>, StoreError> {
    let encoded = encode_tombstone(&retained.tombstone)
        .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
    let mut out = Vec::with_capacity(TOMBSTONE_MAGIC.len() + 16 + 32 + 8 + 4 + encoded.len());
    out.extend_from_slice(TOMBSTONE_MAGIC);
    out.extend_from_slice(heap_id.as_bytes());
    out.extend_from_slice(retained.tombstone.atomic_id.as_bytes());
    out.extend_from_slice(&retained.decided_at_unix_s.to_be_bytes());
    out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    out.extend_from_slice(&encoded);
    Ok(out)
}

pub(crate) fn tombstone_event_id(heap_id: HeapId, atomic_id: AtomicId) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.tombstone");
    hasher.update(heap_id.as_bytes());
    hasher.update(atomic_id.as_bytes());
    let mut id = [0u8; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    id
}

pub(crate) fn payload_event_id(heap_id: HeapId, atomic_id: AtomicId, ordinal: u32) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.payload");
    hasher.update(heap_id.as_bytes());
    hasher.update(atomic_id.as_bytes());
    hasher.update(&ordinal.to_be_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

#[allow(dead_code)]
pub(crate) fn prepare_event_id(heap_id: HeapId, atomic_id: AtomicId) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.prepare");
    hasher.update(heap_id.as_bytes());
    hasher.update(atomic_id.as_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

pub(crate) fn seal_event_id(heap_id: HeapId, atomic_id: AtomicId) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.seal");
    hasher.update(heap_id.as_bytes());
    hasher.update(atomic_id.as_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

pub(crate) fn order_frontier_event_id(heap_id: HeapId, atomic_id: AtomicId) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.order-frontier");
    hasher.update(heap_id.as_bytes());
    hasher.update(atomic_id.as_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

pub(crate) fn chunk_plan_event_id(heap_id: HeapId, atomic_id: AtomicId, ordinal: u32) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.chunk-plan");
    hasher.update(heap_id.as_bytes());
    hasher.update(atomic_id.as_bytes());
    hasher.update(&ordinal.to_be_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

pub(crate) fn chunk_body_event_id(
    heap_id: HeapId,
    atomic_id: AtomicId,
    ordinal: u32,
    index: u32,
) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.chunk-body");
    hasher.update(heap_id.as_bytes());
    hasher.update(atomic_id.as_bytes());
    hasher.update(&ordinal.to_be_bytes());
    hasher.update(&index.to_be_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

/// Which staging sidecar magic was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SidecarKind {
    Prepare,
    Payload,
    Seal,
    ChunkPlan,
    ChunkBody,
    OrderFrontier,
    Tombstone,
}

/// Decode of an Atomic staging sidecar (`ATPREP1` / `ATPAY1` / `ATSEAL1`).
#[derive(Debug)]
pub(crate) enum SidecarDecode {
    /// Body is not a staging sidecar.
    NotSidecar,
    /// Magic present but truncated.
    Partial { kind: SidecarKind },
    /// Magic present but the body is damaged.
    Corrupt {
        kind: SidecarKind,
        atomic_id: Option<AtomicId>,
    },
    /// Valid prepare sidecar.
    Prepare(Box<AtomicPrepare>),
    /// Valid payload sidecar.
    Payload {
        heap_id: HeapId,
        atomic_id: AtomicId,
        ordinal: u32,
        payload: Vec<u8>,
    },
    /// Valid seal sidecar (identity + content root).
    Seal {
        heap_id: HeapId,
        atomic_id: AtomicId,
        content_root: ContentRoot,
    },
    /// Valid frozen chunk map.
    ChunkPlan {
        heap_id: HeapId,
        atomic_id: AtomicId,
        ordinal: u32,
        plan: ChunkPlan,
    },
    /// Valid verified chunk body.
    ChunkBody {
        heap_id: HeapId,
        atomic_id: AtomicId,
        ordinal: u32,
        index: u32,
        body: Vec<u8>,
    },
    /// Durable per-shard order witness covered by the decision boundary.
    OrderFrontier {
        heap_id: HeapId,
        atomic_id: AtomicId,
        frontiers: Vec<AtomicShardFrontier>,
    },
    /// Heap-qualified lifetime decision summary.
    Tombstone {
        heap_id: HeapId,
        retained: RetainedDecisionTombstone,
    },
}

/// Classify a PayloadChunk body as a staging sidecar or not-ours.
pub(crate) fn decode_stage_sidecar(body: &[u8]) -> SidecarDecode {
    if body.starts_with(TOMBSTONE_MAGIC) {
        let header = TOMBSTONE_MAGIC.len() + 16 + 32 + 8 + 4;
        if body.len() < header {
            return SidecarDecode::Partial {
                kind: SidecarKind::Tombstone,
            };
        }
        let heap_at = TOMBSTONE_MAGIC.len();
        let Some(heap_id) = decode_heap_id(&body[heap_at..heap_at + 16]) else {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::Tombstone,
                atomic_id: None,
            };
        };
        let id_at = heap_at + 16;
        let atomic_id =
            match AtomicId::from_bytes(body[id_at..id_at + 32].try_into().unwrap_or([0; 32])) {
                Ok(id) => id,
                Err(_) => {
                    return SidecarDecode::Corrupt {
                        kind: SidecarKind::Tombstone,
                        atomic_id: None,
                    }
                }
            };
        let time_at = id_at + 32;
        let decided_at_unix_s =
            u64::from_be_bytes(body[time_at..time_at + 8].try_into().unwrap_or([0; 8]));
        let len_at = time_at + 8;
        let encoded_len =
            u32::from_be_bytes(body[len_at..len_at + 4].try_into().unwrap_or([0; 4])) as usize;
        if body.len() != header.saturating_add(encoded_len) {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::Tombstone,
                atomic_id: Some(atomic_id),
            };
        }
        let tombstone = match decode_tombstone(&body[header..]) {
            Ok(value) => value,
            Err(_) => {
                return SidecarDecode::Corrupt {
                    kind: SidecarKind::Tombstone,
                    atomic_id: Some(atomic_id),
                }
            }
        };
        if tombstone.atomic_id != atomic_id {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::Tombstone,
                atomic_id: Some(atomic_id),
            };
        }
        return SidecarDecode::Tombstone {
            heap_id,
            retained: RetainedDecisionTombstone {
                decided_at_unix_s,
                tombstone,
            },
        };
    }
    if body.starts_with(ORDER_FRONTIER_MAGIC) {
        let header = ORDER_FRONTIER_MAGIC.len() + 16 + 32 + 2;
        if body.len() < header {
            return SidecarDecode::Partial {
                kind: SidecarKind::OrderFrontier,
            };
        }
        let heap_at = ORDER_FRONTIER_MAGIC.len();
        let Some(heap_id) = decode_heap_id(&body[heap_at..heap_at + 16]) else {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::OrderFrontier,
                atomic_id: None,
            };
        };
        let id_at = heap_at + 16;
        let atomic_id =
            match AtomicId::from_bytes(body[id_at..id_at + 32].try_into().unwrap_or([0; 32])) {
                Ok(id) => id,
                Err(_) => {
                    return SidecarDecode::Corrupt {
                        kind: SidecarKind::OrderFrontier,
                        atomic_id: None,
                    };
                }
            };
        let count_at = id_at + 32;
        let count =
            u16::from_be_bytes(body[count_at..count_at + 2].try_into().unwrap_or([0; 2])) as usize;
        let Some(expected) = header.checked_add(count.saturating_mul(26)) else {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::OrderFrontier,
                atomic_id: Some(atomic_id),
            };
        };
        if count == 0 || body.len() != expected {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::OrderFrontier,
                atomic_id: Some(atomic_id),
            };
        }
        let mut frontiers = Vec::with_capacity(count);
        let mut offset = header;
        for expected_shard in 0..count {
            let shard = u16::from_be_bytes(body[offset..offset + 2].try_into().unwrap_or([0; 2]));
            let segment_id = body[offset + 2..offset + 18].try_into().unwrap_or([0; 16]);
            let next_writer_sequence =
                u64::from_be_bytes(body[offset + 18..offset + 26].try_into().unwrap_or([0; 8]));
            if usize::from(shard) != expected_shard || segment_id == [0; 16] {
                return SidecarDecode::Corrupt {
                    kind: SidecarKind::OrderFrontier,
                    atomic_id: Some(atomic_id),
                };
            }
            frontiers.push(AtomicShardFrontier {
                shard,
                segment_id,
                next_writer_sequence,
            });
            offset += 26;
        }
        return SidecarDecode::OrderFrontier {
            heap_id,
            atomic_id,
            frontiers,
        };
    }
    if body.starts_with(PREPARE_MAGIC) {
        return match decode_prepare(&body[PREPARE_MAGIC.len()..]) {
            Ok(prepare) => SidecarDecode::Prepare(Box::new(prepare)),
            Err(_) if body.len() < PREPARE_MAGIC.len() + 8 => SidecarDecode::Partial {
                kind: SidecarKind::Prepare,
            },
            Err(_) => SidecarDecode::Corrupt {
                kind: SidecarKind::Prepare,
                atomic_id: None,
            },
        };
    }
    if body.starts_with(PAYLOAD_MAGIC) {
        let header = PAYLOAD_MAGIC.len() + 16 + 32 + 4;
        if body.len() < header {
            return SidecarDecode::Partial {
                kind: SidecarKind::Payload,
            };
        }
        let heap_at = PAYLOAD_MAGIC.len();
        let Some(heap_id) = decode_heap_id(&body[heap_at..heap_at + 16]) else {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::Payload,
                atomic_id: None,
            };
        };
        let id_at = heap_at + 16;
        let id = match body[id_at..id_at + 32].try_into() {
            Ok(bytes) => match AtomicId::from_bytes(bytes) {
                Ok(id) => id,
                Err(_) => {
                    return SidecarDecode::Corrupt {
                        kind: SidecarKind::Payload,
                        atomic_id: None,
                    };
                }
            },
            Err(_) => {
                return SidecarDecode::Corrupt {
                    kind: SidecarKind::Payload,
                    atomic_id: None,
                };
            }
        };
        let ord_at = id_at + 32;
        let ordinal = u32::from_be_bytes(body[ord_at..ord_at + 4].try_into().unwrap_or([0; 4]));
        return SidecarDecode::Payload {
            heap_id,
            atomic_id: id,
            ordinal,
            payload: body[header..].to_vec(),
        };
    }
    if body.starts_with(SEAL_MAGIC) {
        let header = SEAL_MAGIC.len() + 16 + 64;
        if body.len() < header {
            return SidecarDecode::Partial {
                kind: SidecarKind::Seal,
            };
        }
        if body.len() != header {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::Seal,
                atomic_id: AtomicId::from_bytes(
                    body[SEAL_MAGIC.len() + 16..SEAL_MAGIC.len() + 48]
                        .try_into()
                        .unwrap_or([0; 32]),
                )
                .ok(),
            };
        }
        let heap_at = SEAL_MAGIC.len();
        let Some(heap_id) = decode_heap_id(&body[heap_at..heap_at + 16]) else {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::Seal,
                atomic_id: None,
            };
        };
        let id_at = heap_at + 16;
        let id_bytes: [u8; 32] = match body[id_at..id_at + 32].try_into() {
            Ok(b) => b,
            Err(_) => {
                return SidecarDecode::Corrupt {
                    kind: SidecarKind::Seal,
                    atomic_id: None,
                };
            }
        };
        let root_bytes: [u8; 32] = match body[id_at + 32..id_at + 64].try_into() {
            Ok(b) => b,
            Err(_) => {
                return SidecarDecode::Corrupt {
                    kind: SidecarKind::Seal,
                    atomic_id: AtomicId::from_bytes(id_bytes).ok(),
                };
            }
        };
        let (Ok(atomic_id), Ok(content_root)) = (
            AtomicId::from_bytes(id_bytes),
            ContentRoot::from_bytes(root_bytes),
        ) else {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::Seal,
                atomic_id: AtomicId::from_bytes(id_bytes).ok(),
            };
        };
        return SidecarDecode::Seal {
            heap_id,
            atomic_id,
            content_root,
        };
    }
    if body.starts_with(CHUNK_PLAN_MAGIC) {
        let header = CHUNK_PLAN_MAGIC.len() + 16 + 32 + 4 + 4;
        if body.len() < header {
            return SidecarDecode::Partial {
                kind: SidecarKind::ChunkPlan,
            };
        }
        let heap_at = CHUNK_PLAN_MAGIC.len();
        let Some(heap_id) = decode_heap_id(&body[heap_at..heap_at + 16]) else {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::ChunkPlan,
                atomic_id: None,
            };
        };
        let id_at = heap_at + 16;
        let id = match decode_atomic_id(&body[id_at..id_at + 32]) {
            Some(id) => id,
            None => {
                return SidecarDecode::Corrupt {
                    kind: SidecarKind::ChunkPlan,
                    atomic_id: None,
                };
            }
        };
        let ord_at = id_at + 32;
        let ordinal = u32::from_be_bytes(body[ord_at..ord_at + 4].try_into().unwrap_or([0; 4]));
        let total = u32::from_be_bytes(body[ord_at + 4..ord_at + 8].try_into().unwrap_or([0; 4]));
        let hashes_len = body.len() - header;
        if total < 2 || hashes_len % 32 != 0 || hashes_len / 32 != total as usize {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::ChunkPlan,
                atomic_id: Some(id),
            };
        }
        let mut chunk_hashes = Vec::with_capacity(total as usize);
        let mut rest = &body[header..];
        for _ in 0..total {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&rest[..32]);
            chunk_hashes.push(hash);
            rest = &rest[32..];
        }
        return SidecarDecode::ChunkPlan {
            heap_id,
            atomic_id: id,
            ordinal,
            plan: ChunkPlan {
                total,
                chunk_hashes,
            },
        };
    }
    if body.starts_with(CHUNK_BODY_MAGIC) {
        let header = CHUNK_BODY_MAGIC.len() + 16 + 32 + 4 + 4;
        if body.len() < header {
            return SidecarDecode::Partial {
                kind: SidecarKind::ChunkBody,
            };
        }
        let heap_at = CHUNK_BODY_MAGIC.len();
        let Some(heap_id) = decode_heap_id(&body[heap_at..heap_at + 16]) else {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::ChunkBody,
                atomic_id: None,
            };
        };
        let id_at = heap_at + 16;
        let id = match decode_atomic_id(&body[id_at..id_at + 32]) {
            Some(id) => id,
            None => {
                return SidecarDecode::Corrupt {
                    kind: SidecarKind::ChunkBody,
                    atomic_id: None,
                };
            }
        };
        let ord_at = id_at + 32;
        let ordinal = u32::from_be_bytes(body[ord_at..ord_at + 4].try_into().unwrap_or([0; 4]));
        let index = u32::from_be_bytes(body[ord_at + 4..ord_at + 8].try_into().unwrap_or([0; 4]));
        return SidecarDecode::ChunkBody {
            heap_id,
            atomic_id: id,
            ordinal,
            index,
            body: body[header..].to_vec(),
        };
    }
    SidecarDecode::NotSidecar
}

fn decode_atomic_id(bytes: &[u8]) -> Option<AtomicId> {
    let arr: [u8; 32] = bytes.try_into().ok()?;
    AtomicId::from_bytes(arr).ok()
}

fn decode_heap_id(bytes: &[u8]) -> Option<HeapId> {
    let arr: [u8; 16] = bytes.try_into().ok()?;
    HeapId::from_bytes(arr).ok()
}
