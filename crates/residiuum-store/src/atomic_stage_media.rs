//! Store-segment records for Atomic staging (CR-ATMR4-005).
//!
//! Prepare and member identity live in Atomic frames. Payloads and the first
//! stable boundary live in `PayloadChunk` frames that ordinary recovery skips
//! (`decode_piece_body` / `decode_item_envelope` fail closed). The peer lane
//! is not a second authority.

use crate::error::StoreError;
use residiuum_atomics::{
    decode_member, decode_prepare, encode_prepare, AtomicId, AtomicMember, AtomicPrepare,
    ContentRoot,
};
use residiuum_format::{
    examine_atomic_frame, AtomicEvidenceClass, AtomicFrameRole, DecodedFrame, FrameKind,
};
use std::collections::{BTreeMap, BTreeSet};

const PAYLOAD_MAGIC: &[u8] = b"ATPAY1";
const SEAL_MAGIC: &[u8] = b"ATSEAL1";
const PREPARE_MAGIC: &[u8] = b"ATPREP1";

/// Staging facts recovered from store media only.
#[derive(Debug, Default)]
pub(crate) struct StageCatalog {
    pub prepares: BTreeMap<AtomicId, AtomicPrepare>,
    pub members: BTreeMap<AtomicId, Vec<AtomicMember>>,
    pub payloads: BTreeMap<(AtomicId, u32), Vec<u8>>,
    pub seals: BTreeSet<AtomicId>,
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

    pub(crate) fn is_sealed(&self, atomic_id: AtomicId) -> bool {
        self.seals.contains(&atomic_id)
    }

    /// Approximate retained catalogue bytes (not a wire encoding).
    pub(crate) fn work_bytes(&self) -> u64 {
        let payload: u64 = self.payloads.values().map(|p| p.len() as u64).sum();
        let members: u64 = self.members.values().map(|ms| ms.len() as u64).sum();
        payload
            .saturating_add(self.prepares.len() as u64 * 512)
            .saturating_add(members.saturating_mul(256))
            .saturating_add(self.seals.len() as u64 * 32)
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

/// Fold one verified store frame into the live catalogue (CR-ATMR5-001).
pub(crate) fn ingest_frame(catalog: &mut StageCatalog, frame: &DecodedFrame) {
    if frame.header.known_kind() == Some(FrameKind::PayloadChunk) {
        if let Some((id, ordinal, payload)) = decode_stage_payload(&frame.body) {
            catalog.payloads.insert((id, ordinal), payload);
        }
        if let Some(id) = decode_stage_seal(&frame.body) {
            catalog.seals.insert(id);
        }
        if let Some(prepare) = decode_stage_prepare(&frame.body) {
            catalog.prepares.insert(prepare.atomic_id, prepare);
        }
        return;
    }
    let Some(AtomicEvidenceClass::Valid(link)) = examine_atomic_frame(frame) else {
        return;
    };
    match link.role {
        AtomicFrameRole::Prepare => {
            if let Ok(prepare) = decode_prepare(&frame.body) {
                catalog.prepares.insert(prepare.atomic_id, prepare);
            }
        }
        AtomicFrameRole::Member => {
            if let Ok(member) = decode_member(&frame.body) {
                let slot = catalog.members.entry(member.atomic_id).or_default();
                if !slot.iter().any(|m| m.ordinal == member.ordinal) {
                    slot.push(member);
                }
            }
        }
        AtomicFrameRole::Commit => {}
    }
}

fn decode_stage_payload(body: &[u8]) -> Option<(AtomicId, u32, Vec<u8>)> {
    let header = PAYLOAD_MAGIC.len() + 32 + 4;
    if body.len() < header || !body.starts_with(PAYLOAD_MAGIC) {
        return None;
    }
    let id = AtomicId::from_bytes(
        body[PAYLOAD_MAGIC.len()..PAYLOAD_MAGIC.len() + 32]
            .try_into()
            .ok()?,
    )
    .ok()?;
    let ord_at = PAYLOAD_MAGIC.len() + 32;
    let ordinal = u32::from_be_bytes(body[ord_at..ord_at + 4].try_into().ok()?);
    Some((id, ordinal, body[header..].to_vec()))
}

fn decode_stage_prepare(body: &[u8]) -> Option<AtomicPrepare> {
    if !body.starts_with(PREPARE_MAGIC) {
        return None;
    }
    decode_prepare(&body[PREPARE_MAGIC.len()..]).ok()
}

fn decode_stage_seal(body: &[u8]) -> Option<AtomicId> {
    let header = SEAL_MAGIC.len() + 64;
    if body.len() != header || !body.starts_with(SEAL_MAGIC) {
        return None;
    }
    AtomicId::from_bytes(
        body[SEAL_MAGIC.len()..SEAL_MAGIC.len() + 32]
            .try_into()
            .ok()?,
    )
    .ok()
}
