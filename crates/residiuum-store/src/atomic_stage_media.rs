//! Store-segment records for Atomic staging (CR-ATMR4-005).
//!
//! Prepare and member identity live in Atomic frames. Payloads and the first
//! stable boundary live in `PayloadChunk` frames that ordinary recovery skips
//! (`decode_piece_body` / `decode_item_envelope` fail closed). The peer lane
//! is not a second authority.

use crate::error::StoreError;
use residiuum_atomics::{
    decode_prepare, encode_prepare, AtomicDecision, AtomicId, AtomicMember, AtomicPrepare,
    ChunkPlan, ContentRoot, HeapId,
};
use std::collections::{BTreeMap, BTreeSet};

const PAYLOAD_MAGIC: &[u8] = b"ATPAY1";
const SEAL_MAGIC: &[u8] = b"ATSEAL1";
const PREPARE_MAGIC: &[u8] = b"ATPREP1";
const CHUNK_PLAN_MAGIC: &[u8] = b"ATMAP1";
const CHUNK_BODY_MAGIC: &[u8] = b"ATCHK1";

/// Locator for staged payload/chunk bytes in store media (CR-ATMR6-004).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BodyRef {
    pub rel_path: String,
    pub offset: u64,
    pub len: u32,
    pub hash: [u8; 32],
}

/// Staging facts recovered from store media only.
#[derive(Clone, Debug, Default)]
pub(crate) struct StageCatalog {
    pub prepares: BTreeMap<AtomicId, AtomicPrepare>,
    pub members: BTreeMap<AtomicId, Vec<AtomicMember>>,
    pub payloads: BTreeMap<(AtomicId, u32), Vec<u8>>,
    /// Media locators for payloads (checkpoint authority, not a second copy).
    pub payload_refs: BTreeMap<(AtomicId, u32), BodyRef>,
    /// Frozen chunk maps (CR-ATMR5-005).
    pub chunk_plans: BTreeMap<(AtomicId, u32), ChunkPlan>,
    /// Verified chunk bodies keyed by (atomic, ordinal, index).
    pub chunks: BTreeMap<(AtomicId, u32, u32), Vec<u8>>,
    /// Media locators for chunk bodies.
    pub chunk_refs: BTreeMap<(AtomicId, u32, u32), BodyRef>,
    pub seals: BTreeMap<AtomicId, ContentRoot>,
    /// Durable terminal decisions. A committed decision is the ATM-3
    /// linearization point; the checkpoint is only a rebuild accelerator.
    pub decisions: BTreeMap<AtomicId, AtomicDecision>,
    /// Heap ownership authenticated by each decision envelope. Retained even
    /// when its prepare is damaged so a named commit position is never reused.
    pub evidence_heaps: BTreeMap<AtomicId, HeapId>,
    /// Next non-zero Heap commit position. Reconstructed above every admitted
    /// committed decision during recovery.
    pub commit_next: BTreeMap<HeapId, u64>,
    /// Identities that must not be installed or reused (CR-ATMR5-002).
    pub blocked: BTreeSet<AtomicId>,
    /// Prepares observed as format-admitted `BatchPrepare` (CR-ATMR5-006).
    pub prepare_batch: BTreeSet<AtomicId>,
    /// Durable coordinator sequence per Atomic (CR-ATMR5-004).
    pub coord_seq: BTreeMap<AtomicId, u64>,
    /// Next sequence to allocate (strictly above every issued sequence).
    pub coord_next: u64,
    /// Encounter order of prepares (not Atomic-ID sort).
    pub prepare_seen: Vec<AtomicId>,
    /// Persisted honest findings (CR-ATMR6-002). Ordinary reopen must not drop them.
    pub findings: crate::atomic_stage_classify::StageFindings,
    /// Store/coverage degradation. Ordinary reopen must not clear it.
    pub coverage_degraded: bool,
    /// Covered paths that vanished after a checkpoint. Cleared only by scrub.
    pub missing_covered: Vec<String>,
    /// Intended member counts from the issuing prepare (CR-ATMR6-005).
    pub intended_members: BTreeMap<AtomicId, u32>,
}

impl StageCatalog {
    pub(crate) fn has_member(&self, atomic_id: AtomicId, ordinal: u32) -> bool {
        self.members
            .get(&atomic_id)
            .is_some_and(|ms| ms.iter().any(|m| m.ordinal == ordinal))
    }

    pub(crate) fn has_payload(&self, atomic_id: AtomicId, ordinal: u32) -> bool {
        self.payloads.contains_key(&(atomic_id, ordinal))
    }

    pub(crate) fn has_chunk_plan(&self, atomic_id: AtomicId, ordinal: u32) -> bool {
        self.chunk_plans.contains_key(&(atomic_id, ordinal))
    }

    pub(crate) fn has_chunk(&self, atomic_id: AtomicId, ordinal: u32, index: u32) -> bool {
        self.chunks.contains_key(&(atomic_id, ordinal, index))
    }

    pub(crate) fn is_sealed(&self, atomic_id: AtomicId) -> bool {
        self.seals.contains_key(&atomic_id)
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
            || !self.evidence_heaps.is_empty()
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
            .saturating_add(self.evidence_heaps.len() as u64 * 48)
            .saturating_add(self.commit_next.len() as u64 * 24)
            .saturating_add(self.blocked.len() as u64 * 32)
            .saturating_add(self.prepare_batch.len() as u64 * 32)
            .saturating_add(self.coord_seq.len() as u64 * 40)
            .saturating_add(self.findings.records.len() as u64 * 40)
            .saturating_add(u64::from(self.coverage_degraded))
            .saturating_add(self.missing_covered.iter().map(|p| p.len() as u64).sum())
    }

    /// Allocate the next Heap commit position while the exclusive store writer
    /// is held. Allocation becomes authoritative only when its decision frame
    /// is durably appended.
    pub(crate) fn next_commit_position(&self, heap_id: HeapId) -> Result<u64, StoreError> {
        let next = self.commit_next.get(&heap_id).copied().unwrap_or(1).max(1);
        if self
            .decisions
            .iter()
            .filter(|(id, _)| {
                self.evidence_heaps
                    .get(id)
                    .copied()
                    .or_else(|| self.prepares.get(id).map(|prepare| prepare.heap_id))
                    == Some(heap_id)
            })
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
        for (id, decision) in &self.decisions {
            let Some(position) = decision.commit_position else {
                continue;
            };
            let Some(heap_id) = self
                .evidence_heaps
                .get(id)
                .copied()
                .or_else(|| self.prepares.get(id).map(|prepare| prepare.heap_id))
            else {
                continue;
            };
            let entry = next.entry(heap_id).or_insert(1u64);
            *entry = (*entry).max(position.saturating_add(1).max(1));
        }
        self.commit_next = next;
    }

    /// Allocate or return the durable coordinator sequence for `atomic_id`.
    pub(crate) fn assign_coord(&mut self, atomic_id: AtomicId) -> Result<u64, StoreError> {
        if let Some(seq) = self.coord_seq.get(&atomic_id).copied() {
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
        self.coord_seq.insert(atomic_id, seq);
        self.coord_next = seq.saturating_add(1);
        if !self.prepare_seen.contains(&atomic_id) {
            self.prepare_seen.push(atomic_id);
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
            atomic_a,
            AtomicDecision::committed(atomic_a, [1; 32], [2; 32], 1, 7).unwrap(),
        );
        catalog.decisions.insert(
            atomic_b,
            AtomicDecision::committed(atomic_b, [3; 32], [4; 32], 1, 2).unwrap(),
        );
        catalog.evidence_heaps.insert(atomic_a, heap_a);
        catalog.evidence_heaps.insert(atomic_b, heap_b);

        catalog.reconstruct_commit_next();

        assert_eq!(catalog.next_commit_position(heap_a).unwrap(), 8);
        assert_eq!(catalog.next_commit_position(heap_b).unwrap(), 3);
    }
}

pub(crate) fn encode_stage_payload(atomic_id: AtomicId, ordinal: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PAYLOAD_MAGIC.len() + 36 + payload.len());
    out.extend_from_slice(PAYLOAD_MAGIC);
    out.extend_from_slice(atomic_id.as_bytes());
    out.extend_from_slice(&ordinal.to_be_bytes());
    out.extend_from_slice(payload);
    out
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

pub(crate) fn encode_stage_seal(atomic_id: AtomicId, content_root: ContentRoot) -> Vec<u8> {
    let mut out = Vec::with_capacity(SEAL_MAGIC.len() + 64);
    out.extend_from_slice(SEAL_MAGIC);
    out.extend_from_slice(atomic_id.as_bytes());
    out.extend_from_slice(content_root.as_bytes());
    out
}

pub(crate) fn encode_stage_chunk_plan(
    atomic_id: AtomicId,
    ordinal: u32,
    plan: &ChunkPlan,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(CHUNK_PLAN_MAGIC.len() + 40 + plan.chunk_hashes.len() * 32);
    out.extend_from_slice(CHUNK_PLAN_MAGIC);
    out.extend_from_slice(atomic_id.as_bytes());
    out.extend_from_slice(&ordinal.to_be_bytes());
    out.extend_from_slice(&plan.total.to_be_bytes());
    for hash in &plan.chunk_hashes {
        out.extend_from_slice(hash);
    }
    out
}

pub(crate) fn encode_stage_chunk_body(
    atomic_id: AtomicId,
    ordinal: u32,
    index: u32,
    body: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(CHUNK_BODY_MAGIC.len() + 40 + body.len());
    out.extend_from_slice(CHUNK_BODY_MAGIC);
    out.extend_from_slice(atomic_id.as_bytes());
    out.extend_from_slice(&ordinal.to_be_bytes());
    out.extend_from_slice(&index.to_be_bytes());
    out.extend_from_slice(body);
    out
}

pub(crate) fn payload_event_id(atomic_id: AtomicId, ordinal: u32) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.payload");
    hasher.update(atomic_id.as_bytes());
    hasher.update(&ordinal.to_be_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

#[allow(dead_code)]
pub(crate) fn prepare_event_id(atomic_id: AtomicId) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.prepare");
    hasher.update(atomic_id.as_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

pub(crate) fn seal_event_id(atomic_id: AtomicId) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.seal");
    hasher.update(atomic_id.as_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

pub(crate) fn chunk_plan_event_id(atomic_id: AtomicId, ordinal: u32) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.chunk-plan");
    hasher.update(atomic_id.as_bytes());
    hasher.update(&ordinal.to_be_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

pub(crate) fn chunk_body_event_id(atomic_id: AtomicId, ordinal: u32, index: u32) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.chunk-body");
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
        atomic_id: AtomicId,
        ordinal: u32,
        payload: Vec<u8>,
    },
    /// Valid seal sidecar (identity + content root).
    Seal {
        atomic_id: AtomicId,
        content_root: ContentRoot,
    },
    /// Valid frozen chunk map.
    ChunkPlan {
        atomic_id: AtomicId,
        ordinal: u32,
        plan: ChunkPlan,
    },
    /// Valid verified chunk body.
    ChunkBody {
        atomic_id: AtomicId,
        ordinal: u32,
        index: u32,
        body: Vec<u8>,
    },
}

/// Classify a PayloadChunk body as a staging sidecar or not-ours.
pub(crate) fn decode_stage_sidecar(body: &[u8]) -> SidecarDecode {
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
        let header = PAYLOAD_MAGIC.len() + 32 + 4;
        if body.len() < header {
            return SidecarDecode::Partial {
                kind: SidecarKind::Payload,
            };
        }
        let id = match body[PAYLOAD_MAGIC.len()..PAYLOAD_MAGIC.len() + 32].try_into() {
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
        let ord_at = PAYLOAD_MAGIC.len() + 32;
        let ordinal = u32::from_be_bytes(body[ord_at..ord_at + 4].try_into().unwrap_or([0; 4]));
        return SidecarDecode::Payload {
            atomic_id: id,
            ordinal,
            payload: body[header..].to_vec(),
        };
    }
    if body.starts_with(SEAL_MAGIC) {
        let header = SEAL_MAGIC.len() + 64;
        if body.len() < header {
            return SidecarDecode::Partial {
                kind: SidecarKind::Seal,
            };
        }
        if body.len() != header {
            return SidecarDecode::Corrupt {
                kind: SidecarKind::Seal,
                atomic_id: AtomicId::from_bytes(
                    body[SEAL_MAGIC.len()..SEAL_MAGIC.len() + 32]
                        .try_into()
                        .unwrap_or([0; 32]),
                )
                .ok(),
            };
        }
        let id_bytes: [u8; 32] = match body[SEAL_MAGIC.len()..SEAL_MAGIC.len() + 32].try_into() {
            Ok(b) => b,
            Err(_) => {
                return SidecarDecode::Corrupt {
                    kind: SidecarKind::Seal,
                    atomic_id: None,
                };
            }
        };
        let root_bytes: [u8; 32] =
            match body[SEAL_MAGIC.len() + 32..SEAL_MAGIC.len() + 64].try_into() {
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
            atomic_id,
            content_root,
        };
    }
    if body.starts_with(CHUNK_PLAN_MAGIC) {
        let header = CHUNK_PLAN_MAGIC.len() + 32 + 4 + 4;
        if body.len() < header {
            return SidecarDecode::Partial {
                kind: SidecarKind::ChunkPlan,
            };
        }
        let id = match decode_atomic_id(&body[CHUNK_PLAN_MAGIC.len()..CHUNK_PLAN_MAGIC.len() + 32])
        {
            Some(id) => id,
            None => {
                return SidecarDecode::Corrupt {
                    kind: SidecarKind::ChunkPlan,
                    atomic_id: None,
                };
            }
        };
        let ord_at = CHUNK_PLAN_MAGIC.len() + 32;
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
            atomic_id: id,
            ordinal,
            plan: ChunkPlan {
                total,
                chunk_hashes,
            },
        };
    }
    if body.starts_with(CHUNK_BODY_MAGIC) {
        let header = CHUNK_BODY_MAGIC.len() + 32 + 4 + 4;
        if body.len() < header {
            return SidecarDecode::Partial {
                kind: SidecarKind::ChunkBody,
            };
        }
        let id = match decode_atomic_id(&body[CHUNK_BODY_MAGIC.len()..CHUNK_BODY_MAGIC.len() + 32])
        {
            Some(id) => id,
            None => {
                return SidecarDecode::Corrupt {
                    kind: SidecarKind::ChunkBody,
                    atomic_id: None,
                };
            }
        };
        let ord_at = CHUNK_BODY_MAGIC.len() + 32;
        let ordinal = u32::from_be_bytes(body[ord_at..ord_at + 4].try_into().unwrap_or([0; 4]));
        let index = u32::from_be_bytes(body[ord_at + 4..ord_at + 8].try_into().unwrap_or([0; 4]));
        return SidecarDecode::ChunkBody {
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
