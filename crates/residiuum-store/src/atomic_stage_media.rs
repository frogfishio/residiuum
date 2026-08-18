//! Store-segment records for Atomic staging (CR-ATMR4-005).
//!
//! Prepare and member identity live in Atomic frames. Payloads and the first
//! stable boundary live in `PayloadChunk` frames that ordinary recovery skips
//! (`decode_piece_body` / `decode_item_envelope` fail closed). The peer lane
//! is not a second authority.

use crate::error::StoreError;
use crate::layout::StorePaths;
use residiuum_atomics::{
    decode_member, decode_prepare, AtomicId, AtomicMember, AtomicPrepare, ContentRoot,
};
use residiuum_format::{
    examine_atomic_frame, scan_forward, AtomicEvidenceClass, AtomicFrameRole, FrameKind,
    SafetyLimits,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

const PAYLOAD_MAGIC: &[u8] = b"ATPAY1";
const SEAL_MAGIC: &[u8] = b"ATSEAL1";

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
}

pub(crate) fn encode_stage_payload(atomic_id: AtomicId, ordinal: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PAYLOAD_MAGIC.len() + 36 + payload.len());
    out.extend_from_slice(PAYLOAD_MAGIC);
    out.extend_from_slice(atomic_id.as_bytes());
    out.extend_from_slice(&ordinal.to_be_bytes());
    out.extend_from_slice(payload);
    out
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

pub(crate) fn seal_event_id(atomic_id: AtomicId) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum.atomic-stage.seal");
    hasher.update(atomic_id.as_bytes());
    let hash = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

pub(crate) fn scan_stage_catalog(paths: &StorePaths) -> Result<StageCatalog, StoreError> {
    let mut catalog = StageCatalog::default();
    for path in segment_files(paths) {
        let bytes = fs::read(&path)?;
        let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
        for (_, frame) in report.verified_frames() {
            if frame.header.known_kind() == Some(FrameKind::PayloadChunk) {
                if let Some((id, ordinal, payload)) = decode_stage_payload(&frame.body) {
                    catalog.payloads.insert((id, ordinal), payload);
                }
                if let Some(id) = decode_stage_seal(&frame.body) {
                    catalog.seals.insert(id);
                }
                continue;
            }
            let Some(AtomicEvidenceClass::Valid(link)) = examine_atomic_frame(frame) else {
                continue;
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
    }
    Ok(catalog)
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

fn segment_files(paths: &StorePaths) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_files(&paths.active_dir(), &mut out);
    collect_files(&paths.segments_dir(), &mut out);
    collect_files(&paths.pending_seal_dir(), &mut out);
    out
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if entry.file_name().to_string_lossy().ends_with(".tmp") {
            continue;
        }
        if path.is_dir() {
            collect_files(&path, out);
        } else if path.is_file() {
            out.push(path);
        }
    }
}
