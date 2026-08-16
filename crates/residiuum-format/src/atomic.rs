//! Atomic `BatchPrepare` / `BatchCommit` envelopes and member linkage
//! (`ATOMICS_SPEC` §9).
//!
//! Uses existing frame kinds. Envelope keys 37–40 are the Atomic extension;
//! keys 1–36 keep format/ownership meanings and are never Atomic identity.
//! A legacy `batch_id` is not Atomic evidence. This module does not write
//! store media; the recovery reader scans a byte buffer.

use crate::cbor_envelope::{
    decode_deterministic_uint_map, encode_deterministic_uint_map,
    validate_deterministic_cbor_envelope, CborEnvelopeError, CborValue, EMPTY_ENVELOPE,
};
use crate::frame::{encode_frame, DecodedFrame, FrameHeader, FrameParts, FrameVerifyError};
use crate::kinds::FrameKind;
use crate::limits::SafetyLimits;
use crate::scan::{scan_forward, ScanReport};
use thiserror::Error;

/// Envelope key: `atomic_id` (`bstr` 32).
pub const ENV_ATOMIC_ID: u64 = 37;
/// Envelope key: member `ordinal` (uint; `ItemEvent` only).
pub const ENV_ATOMIC_ORDINAL: u64 = 38;
/// Envelope key: plan `content_root` (`bstr` 32).
pub const ENV_ATOMIC_CONTENT_ROOT: u64 = 39;
/// Envelope key: Heap `commit_position` (nonzero uint).
pub const ENV_ATOMIC_COMMIT_POSITION: u64 = 40;

/// Role of a verified Atomic frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomicFrameRole {
    /// `BatchPrepare` coordinator record.
    Prepare,
    /// `BatchCommit` decision record.
    Commit,
    /// `ItemEvent` member with Atomic linkage.
    Member,
}

/// Linkage extracted from the frozen envelope extension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicLinkage {
    /// Frame role.
    pub role: AtomicFrameRole,
    /// 32-byte Atomic identity.
    pub atomic_id: [u8; 32],
    /// Plan content root.
    pub content_root: [u8; 32],
    /// Member ordinal when the frame is an `ItemEvent`.
    pub ordinal: Option<u64>,
    /// Heap commit position when a committed decision names this evidence.
    pub commit_position: Option<u64>,
}

/// Why examination is not `Valid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomicExamReason {
    /// Envelope CBOR failed deterministic rules.
    EnvelopeCorrupt,
    /// A required Atomic field has the wrong type or width.
    FieldCorrupt,
    /// Atomic identity is present but the role is missing a required field.
    IncompleteLinkage,
    /// Frame kind cannot carry this Atomic field (e.g. ordinal on prepare).
    RoleMismatch,
    /// Legacy batch / missing Atomic identity — not Atomic evidence.
    NotAtomicEvidence,
    /// Prepare/commit body is not deterministic CBOR.
    BodyCorrupt,
    /// Prepare/commit body is missing.
    BodyMissing,
}

/// Examination class for one verified frame (`ATOMICS_SPEC` §9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtomicEvidenceClass {
    /// Complete, well-typed Atomic linkage for this role.
    Valid(AtomicLinkage),
    /// Atomic identity present but linkage or body is incomplete.
    Partial {
        /// Partial linkage when some fields parsed.
        linkage: Option<AtomicLinkage>,
        /// Why it is incomplete.
        reason: AtomicExamReason,
    },
    /// Verified frame whose Atomic envelope or body is damaged.
    Corrupt {
        /// Why it is corrupt.
        reason: AtomicExamReason,
    },
    /// Not Atomic evidence (legacy batch, empty envelope).
    Unsupported {
        /// Why it is unsupported.
        reason: AtomicExamReason,
    },
}

/// One examined frame plus its physical start offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExaminedAtomicFrame {
    /// Start offset of the verified frame.
    pub offset: u64,
    /// Examination class.
    pub class: AtomicEvidenceClass,
}

/// Recovery-reader report. Independent of any store write path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicRecoveryReport {
    /// Examined Atomic-related frames in scan order.
    pub examined: Vec<ExaminedAtomicFrame>,
    /// Underlying salvage scan (holes stay explicit).
    pub scan: ScanReport,
}

impl AtomicRecoveryReport {
    /// Valid Atomic records only.
    pub fn valid(&self) -> impl Iterator<Item = &AtomicLinkage> {
        self.examined.iter().filter_map(|e| match &e.class {
            AtomicEvidenceClass::Valid(link) => Some(link),
            _ => None,
        })
    }
}

/// Envelope encode/decode failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AtomicEnvelopeError {
    /// Deterministic CBOR failed.
    #[error("atomic envelope invalid: {0}")]
    Envelope(#[from] CborEnvelopeError),
    /// `commit_position` must be nonzero when present.
    #[error("atomic commit_position must be nonzero")]
    ZeroCommitPosition,
}

/// Encode a `BatchPrepare` Atomic envelope.
pub fn encode_atomic_prepare_envelope(
    atomic_id: &[u8; 32],
    content_root: &[u8; 32],
) -> Result<Vec<u8>, AtomicEnvelopeError> {
    encode_deterministic_uint_map(&[
        (ENV_ATOMIC_ID, CborValue::Bytes(atomic_id.to_vec())),
        (
            ENV_ATOMIC_CONTENT_ROOT,
            CborValue::Bytes(content_root.to_vec()),
        ),
    ])
    .map_err(Into::into)
}

/// Encode a `BatchCommit` Atomic envelope.
pub fn encode_atomic_commit_envelope(
    atomic_id: &[u8; 32],
    content_root: &[u8; 32],
    commit_position: Option<u64>,
) -> Result<Vec<u8>, AtomicEnvelopeError> {
    let mut entries = vec![
        (ENV_ATOMIC_ID, CborValue::Bytes(atomic_id.to_vec())),
        (
            ENV_ATOMIC_CONTENT_ROOT,
            CborValue::Bytes(content_root.to_vec()),
        ),
    ];
    if let Some(pos) = commit_position {
        if pos == 0 {
            return Err(AtomicEnvelopeError::ZeroCommitPosition);
        }
        entries.push((ENV_ATOMIC_COMMIT_POSITION, CborValue::Uint(pos)));
    }
    encode_deterministic_uint_map(&entries).map_err(Into::into)
}

/// Encode an `ItemEvent` Atomic member envelope.
pub fn encode_atomic_member_envelope(
    atomic_id: &[u8; 32],
    ordinal: u64,
    content_root: &[u8; 32],
    commit_position: Option<u64>,
) -> Result<Vec<u8>, AtomicEnvelopeError> {
    let mut entries = vec![
        (ENV_ATOMIC_ID, CborValue::Bytes(atomic_id.to_vec())),
        (ENV_ATOMIC_ORDINAL, CborValue::Uint(ordinal)),
        (
            ENV_ATOMIC_CONTENT_ROOT,
            CborValue::Bytes(content_root.to_vec()),
        ),
    ];
    if let Some(pos) = commit_position {
        if pos == 0 {
            return Err(AtomicEnvelopeError::ZeroCommitPosition);
        }
        entries.push((ENV_ATOMIC_COMMIT_POSITION, CborValue::Uint(pos)));
    }
    encode_deterministic_uint_map(&entries).map_err(Into::into)
}

/// Wrap Atomic envelope + body in a structurally valid frame.
pub fn encode_atomic_frame(
    kind: FrameKind,
    envelope: &[u8],
    body: &[u8],
    event_id: [u8; 16],
) -> Result<Vec<u8>, FrameVerifyError> {
    let parts = FrameParts {
        header: FrameHeader::new_draft(kind, envelope.len() as u32, body.len() as u64, event_id),
        envelope: envelope.to_vec(),
        body: body.to_vec(),
    };
    encode_frame(&parts)
}

/// Classify one verified frame. Non-Atomic item events return `None`.
pub fn examine_atomic_frame(frame: &DecodedFrame) -> Option<AtomicEvidenceClass> {
    let kind = FrameKind::from_u8(frame.header.frame_kind).ok()?;
    match kind {
        FrameKind::BatchPrepare => Some(examine_coordinator(frame, AtomicFrameRole::Prepare)),
        FrameKind::BatchCommit => Some(examine_coordinator(frame, AtomicFrameRole::Commit)),
        FrameKind::ItemEvent => examine_member(frame),
        _ => None,
    }
}

/// Scan a byte buffer and examine Atomic evidence. Holes stay explicit.
pub fn read_atomic_evidence(bytes: &[u8], limits: SafetyLimits) -> AtomicRecoveryReport {
    let scan = scan_forward(bytes, limits);
    let mut examined = Vec::new();
    for (offset, frame) in scan.verified_frames() {
        if let Some(class) = examine_atomic_frame(frame) {
            examined.push(ExaminedAtomicFrame { offset, class });
        }
    }
    AtomicRecoveryReport { examined, scan }
}

fn examine_coordinator(frame: &DecodedFrame, role: AtomicFrameRole) -> AtomicEvidenceClass {
    if frame.envelope == EMPTY_ENVELOPE {
        return AtomicEvidenceClass::Unsupported {
            reason: AtomicExamReason::NotAtomicEvidence,
        };
    }
    match parse_fields(&frame.envelope) {
        Err(AtomicExamReason::EnvelopeCorrupt) => AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::EnvelopeCorrupt,
        },
        Err(reason) => AtomicEvidenceClass::Corrupt { reason },
        Ok(fields) if fields.atomic_id.is_none() => AtomicEvidenceClass::Unsupported {
            reason: AtomicExamReason::NotAtomicEvidence,
        },
        Ok(fields) => classify_linkage(role, fields, coordinator_body(&frame.body)),
    }
}

fn examine_member(frame: &DecodedFrame) -> Option<AtomicEvidenceClass> {
    if frame.envelope == EMPTY_ENVELOPE {
        return None;
    }
    match parse_fields(&frame.envelope) {
        Err(AtomicExamReason::EnvelopeCorrupt) => None,
        Err(reason) => Some(AtomicEvidenceClass::Corrupt { reason }),
        Ok(fields) if fields.atomic_id.is_none() => None,
        Ok(fields) => Some(classify_linkage(AtomicFrameRole::Member, fields, Ok(()))),
    }
}

fn coordinator_body(body: &[u8]) -> Result<(), AtomicExamReason> {
    if body.is_empty() {
        return Err(AtomicExamReason::BodyMissing);
    }
    validate_deterministic_cbor_envelope(body).map_err(|_| AtomicExamReason::BodyCorrupt)
}

struct Fields {
    atomic_id: Option<[u8; 32]>,
    content_root: Option<[u8; 32]>,
    ordinal: Option<u64>,
    commit_position: Option<u64>,
    field_corrupt: bool,
}

fn parse_fields(envelope: &[u8]) -> Result<Fields, AtomicExamReason> {
    let map =
        decode_deterministic_uint_map(envelope).map_err(|_| AtomicExamReason::EnvelopeCorrupt)?;
    let mut fields = Fields {
        atomic_id: None,
        content_root: None,
        ordinal: None,
        commit_position: None,
        field_corrupt: false,
    };
    for (k, v) in map {
        match k {
            ENV_ATOMIC_ID => match b32(&v) {
                Some(id) => fields.atomic_id = Some(id),
                None => fields.field_corrupt = true,
            },
            ENV_ATOMIC_ORDINAL => match v {
                CborValue::Uint(n) => fields.ordinal = Some(n),
                _ => fields.field_corrupt = true,
            },
            ENV_ATOMIC_CONTENT_ROOT => match b32(&v) {
                Some(root) => fields.content_root = Some(root),
                None => fields.field_corrupt = true,
            },
            ENV_ATOMIC_COMMIT_POSITION => match v {
                CborValue::Uint(0) => fields.field_corrupt = true,
                CborValue::Uint(n) => fields.commit_position = Some(n),
                _ => fields.field_corrupt = true,
            },
            _ => {}
        }
    }
    if fields.field_corrupt {
        return Err(AtomicExamReason::FieldCorrupt);
    }
    Ok(fields)
}

fn classify_linkage(
    role: AtomicFrameRole,
    fields: Fields,
    body: Result<(), AtomicExamReason>,
) -> AtomicEvidenceClass {
    let Some(atomic_id) = fields.atomic_id else {
        return AtomicEvidenceClass::Unsupported {
            reason: AtomicExamReason::NotAtomicEvidence,
        };
    };
    if matches!(role, AtomicFrameRole::Prepare | AtomicFrameRole::Commit)
        && fields.ordinal.is_some()
    {
        return AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::RoleMismatch,
        };
    }
    let Some(content_root) = fields.content_root else {
        return AtomicEvidenceClass::Partial {
            linkage: None,
            reason: AtomicExamReason::IncompleteLinkage,
        };
    };
    if role == AtomicFrameRole::Member && fields.ordinal.is_none() {
        return AtomicEvidenceClass::Partial {
            linkage: Some(AtomicLinkage {
                role,
                atomic_id,
                content_root,
                ordinal: None,
                commit_position: fields.commit_position,
            }),
            reason: AtomicExamReason::IncompleteLinkage,
        };
    }
    if let Err(reason) = body {
        let linkage = AtomicLinkage {
            role,
            atomic_id,
            content_root,
            ordinal: fields.ordinal,
            commit_position: fields.commit_position,
        };
        return match reason {
            AtomicExamReason::BodyMissing => AtomicEvidenceClass::Partial {
                linkage: Some(linkage),
                reason,
            },
            _ => AtomicEvidenceClass::Corrupt { reason },
        };
    }
    AtomicEvidenceClass::Valid(AtomicLinkage {
        role,
        atomic_id,
        content_root,
        ordinal: fields.ordinal,
        commit_position: fields.commit_position,
    })
}

fn b32(v: &CborValue) -> Option<[u8; 32]> {
    match v {
        CborValue::Bytes(b) if b.len() == 32 => {
            let mut a = [0u8; 32];
            a.copy_from_slice(b);
            Some(a)
        }
        _ => None,
    }
}
