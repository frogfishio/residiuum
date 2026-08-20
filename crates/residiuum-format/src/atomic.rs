//! Atomic `BatchPrepare` / `BatchCommit` envelopes and member linkage
//! (`ATOMICS_SPEC` §9).
//!
//! Uses existing frame kinds. Envelope keys 37–40 are the Atomic extension;
//! keys 1–36 keep format/ownership meanings and are never Atomic identity.
//! A legacy `batch_id` is not Atomic evidence. This module does not write
//! store media; the recovery reader scans a byte buffer and decodes frozen
//! AtomicPrepare / AtomicMember / AtomicDecision bodies (CR-ATM2-003).

use crate::cbor_envelope::{
    decode_deterministic_uint_map, encode_deterministic_uint_map, CborEnvelopeError, CborValue,
    EMPTY_ENVELOPE,
};
use crate::envelope_keys::{ENV_HEAP_ID, ENV_OWNERSHIP_PROFILE};
use crate::frame::{encode_frame, DecodedFrame, FrameHeader, FrameParts, FrameVerifyError};
use crate::kinds::FrameKind;
use crate::limits::SafetyLimits;
use crate::ownership::OWNERSHIP_PROFILE_V1;
use crate::ownership::{parse_ownership_envelope, OwnershipEvidence};
use crate::scan::{scan_forward, ScanReport};
use residiuum_atomics::{
    decode_decision, decode_member, decode_prepare, ordered_member_manifest_root, prepare_hash,
    AtomicDecision, AtomicMember, AtomicPrepare, DecisionCode, Durability, HeapId,
};
use thiserror::Error;

pub use crate::envelope_keys::{
    ENV_ATOMIC_COMMIT_POSITION, ENV_ATOMIC_CONTENT_ROOT, ENV_ATOMIC_ID, ENV_ATOMIC_ORDINAL,
};

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
    /// Heap identity from ownership keys 31/34, when present.
    pub heap_id: Option<[u8; 16]>,
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
    /// Body is not a frozen AtomicPrepare / AtomicMember / AtomicDecision.
    BodyCorrupt,
    /// Prepare/commit/member body is missing.
    BodyMissing,
    /// Decoded body disagrees with envelope linkage.
    BodyMismatch,
    /// Two valid decisions disagree for the same identity.
    ConflictingDecision,
    /// Same Heap/Atomic ID names more than one content root.
    ConflictingContentRoot,
    /// Atomic identity is present but Heap ownership keys 31/34 are missing.
    MissingOwnership,
    /// Group is missing a required prepare, member set, or decision.
    GroupIncomplete,
}

/// Examination class for one verified frame (`ATOMICS_SPEC` §9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtomicEvidenceClass {
    /// Complete linkage and a decoded, matching frozen body for this role.
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
    /// Damaged role body whose Heap-qualified envelope remains authenticated.
    /// This is distinct from unbound corruption so recovery can degrade the
    /// named identity without poisoning unrelated Heaps.
    AttributedCorrupt {
        /// Surviving authenticated envelope linkage.
        linkage: AtomicLinkage,
        /// Why the role body is corrupt.
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

/// Aggregated examination of one `(heap_id, atomic_id)` identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExaminedAtomicGroup {
    /// Heap from ownership (required on Valid frames).
    pub heap_id: Option<[u8; 16]>,
    /// Atomic identity.
    pub atomic_id: [u8; 32],
    /// First observed plan content root (not unique when roots conflict).
    pub content_root: [u8; 32],
    /// Group class. Never a guessed decision.
    pub class: AtomicGroupClass,
}

/// Outcome of aggregating Valid frames for one identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtomicGroupClass {
    /// One prepare, matching committed members, one committed decision.
    Consistent,
    /// Durable not-committed decision. Staged leftovers are reclaimable, not committed.
    NotCommitted {
        /// True when member frames exist beside the abort and must not be treated
        /// as committed data.
        reclaimable_members: bool,
    },
    /// Identity is present but incomplete.
    Partial {
        /// Why the group is incomplete.
        reason: AtomicExamReason,
    },
    /// Conflicting valid decisions.
    Conflicting {
        /// Why the group conflicts.
        reason: AtomicExamReason,
    },
    /// Prepare/member/decision hashes or counts disagree.
    Corrupt {
        /// Why the group is corrupt.
        reason: AtomicExamReason,
    },
}

/// Recovery-reader report. Independent of any store write path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicRecoveryReport {
    /// Examined Atomic-related frames in scan order.
    pub examined: Vec<ExaminedAtomicFrame>,
    /// Groups aggregated from Valid frames.
    pub groups: Vec<ExaminedAtomicGroup>,
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

/// Heap ownership pair present on every Atomic envelope.
fn ownership_entries(heap_id: &[u8; 16]) -> [(u64, CborValue); 2] {
    [
        (ENV_HEAP_ID, CborValue::Bytes(heap_id.to_vec())),
        (ENV_OWNERSHIP_PROFILE, CborValue::Uint(OWNERSHIP_PROFILE_V1)),
    ]
}

/// Encode a `BatchPrepare` Atomic envelope (ownership + Atomic linkage).
pub fn encode_atomic_prepare_envelope(
    heap_id: &[u8; 16],
    atomic_id: &[u8; 32],
    content_root: &[u8; 32],
) -> Result<Vec<u8>, AtomicEnvelopeError> {
    let [h, p] = ownership_entries(heap_id);
    encode_deterministic_uint_map(&[
        h,
        p,
        (ENV_ATOMIC_ID, CborValue::Bytes(atomic_id.to_vec())),
        (
            ENV_ATOMIC_CONTENT_ROOT,
            CborValue::Bytes(content_root.to_vec()),
        ),
    ])
    .map_err(Into::into)
}

/// Encode a `BatchCommit` Atomic envelope (ownership + Atomic linkage).
pub fn encode_atomic_commit_envelope(
    heap_id: &[u8; 16],
    atomic_id: &[u8; 32],
    content_root: &[u8; 32],
    commit_position: Option<u64>,
) -> Result<Vec<u8>, AtomicEnvelopeError> {
    let [h, p] = ownership_entries(heap_id);
    let mut entries = vec![
        h,
        p,
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

/// Encode an `ItemEvent` Atomic member envelope (ownership + Atomic linkage).
pub fn encode_atomic_member_envelope(
    heap_id: &[u8; 16],
    atomic_id: &[u8; 32],
    ordinal: u64,
    content_root: &[u8; 32],
    commit_position: Option<u64>,
) -> Result<Vec<u8>, AtomicEnvelopeError> {
    let [h, p] = ownership_entries(heap_id);
    let mut entries = vec![
        h,
        p,
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
    let mut valid_bodies = Vec::new();
    for (offset, frame) in scan.verified_frames() {
        if let Some(class) = examine_atomic_frame(frame) {
            if let AtomicEvidenceClass::Valid(link) = &class {
                valid_bodies.push((link.clone(), frame.body.clone()));
            }
            examined.push(ExaminedAtomicFrame { offset, class });
        }
    }
    let groups = aggregate_groups(&valid_bodies);
    AtomicRecoveryReport {
        examined,
        groups,
        scan,
    }
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
        Ok(fields) => {
            let body = check_body(role, &fields, &frame.body);
            classify_linkage(role, fields, body)
        }
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
        Ok(fields) => {
            let body = check_body(AtomicFrameRole::Member, &fields, &frame.body);
            Some(classify_linkage(AtomicFrameRole::Member, fields, body))
        }
    }
}

fn check_body(role: AtomicFrameRole, fields: &Fields, body: &[u8]) -> Result<(), AtomicExamReason> {
    if body.is_empty() {
        return Err(AtomicExamReason::BodyMissing);
    }
    match role {
        AtomicFrameRole::Prepare => {
            let prep = decode_prepare(body).map_err(|_| AtomicExamReason::BodyCorrupt)?;
            if prep.atomic_id.to_bytes() != fields.atomic_id.unwrap_or([0; 32])
                || prep.content_root.to_bytes() != fields.content_root.unwrap_or([0; 32])
            {
                return Err(AtomicExamReason::BodyMismatch);
            }
            if let Some(heap) = fields.heap_id {
                if prep.heap_id.to_bytes() != heap {
                    return Err(AtomicExamReason::BodyMismatch);
                }
            }
            Ok(())
        }
        AtomicFrameRole::Member => {
            let member = decode_member(body).map_err(|_| AtomicExamReason::BodyCorrupt)?;
            if member.atomic_id.to_bytes() != fields.atomic_id.unwrap_or([0; 32])
                || Some(u64::from(member.ordinal)) != fields.ordinal
            {
                return Err(AtomicExamReason::BodyMismatch);
            }
            Ok(())
        }
        AtomicFrameRole::Commit => {
            let decision = decode_decision(body).map_err(|_| AtomicExamReason::BodyCorrupt)?;
            if decision.atomic_id.to_bytes() != fields.atomic_id.unwrap_or([0; 32]) {
                return Err(AtomicExamReason::BodyMismatch);
            }
            match decision.decision {
                DecisionCode::Committed if decision.commit_position != fields.commit_position => {
                    return Err(AtomicExamReason::BodyMismatch);
                }
                DecisionCode::NotCommitted if fields.commit_position.is_some() => {
                    return Err(AtomicExamReason::BodyMismatch);
                }
                _ => {}
            }
            Ok(())
        }
    }
}

struct Fields {
    atomic_id: Option<[u8; 32]>,
    content_root: Option<[u8; 32]>,
    ordinal: Option<u64>,
    commit_position: Option<u64>,
    heap_id: Option<[u8; 16]>,
    field_corrupt: bool,
}

fn parse_fields(envelope: &[u8]) -> Result<Fields, AtomicExamReason> {
    let map =
        decode_deterministic_uint_map(envelope).map_err(|_| AtomicExamReason::EnvelopeCorrupt)?;
    let heap_id = match parse_ownership_envelope(envelope) {
        Ok(OwnershipEvidence::Known { heap_id, .. }) => Some(heap_id),
        Ok(OwnershipEvidence::Unknown) => None,
        Err(crate::ownership::OwnershipError::Envelope(_)) => {
            return Err(AtomicExamReason::EnvelopeCorrupt);
        }
        Err(_) => {
            return Err(AtomicExamReason::FieldCorrupt);
        }
    };
    let mut fields = Fields {
        atomic_id: None,
        content_root: None,
        ordinal: None,
        commit_position: None,
        heap_id,
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
    let Some(heap_id) = fields.heap_id else {
        return AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::MissingOwnership,
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
                heap_id: Some(heap_id),
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
            heap_id: Some(heap_id),
        };
        return match reason {
            AtomicExamReason::BodyMissing => AtomicEvidenceClass::Partial {
                linkage: Some(linkage),
                reason,
            },
            _ => AtomicEvidenceClass::AttributedCorrupt { linkage, reason },
        };
    }
    AtomicEvidenceClass::Valid(AtomicLinkage {
        role,
        atomic_id,
        content_root,
        ordinal: fields.ordinal,
        commit_position: fields.commit_position,
        heap_id: Some(heap_id),
    })
}

fn aggregate_groups(valid: &[(AtomicLinkage, Vec<u8>)]) -> Vec<ExaminedAtomicGroup> {
    let mut keys: Vec<([u8; 16], [u8; 32])> = Vec::new();
    for (link, _) in valid {
        let Some(heap_id) = link.heap_id else {
            continue;
        };
        let key = (heap_id, link.atomic_id);
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    keys.into_iter()
        .map(|(heap_id, atomic_id)| {
            let frames: Vec<&(AtomicLinkage, Vec<u8>)> = valid
                .iter()
                .filter(|(l, _)| l.atomic_id == atomic_id && l.heap_id == Some(heap_id))
                .collect();
            let mut roots: Vec<[u8; 32]> = Vec::new();
            for (link, _) in &frames {
                if !roots.contains(&link.content_root) {
                    roots.push(link.content_root);
                }
            }
            let content_root = roots.first().copied().unwrap_or([0; 32]);
            let class = if roots.len() > 1 {
                AtomicGroupClass::Conflicting {
                    reason: AtomicExamReason::ConflictingContentRoot,
                }
            } else {
                classify_group(&frames)
            };
            ExaminedAtomicGroup {
                heap_id: Some(heap_id),
                atomic_id,
                content_root,
                class,
            }
        })
        .collect()
}

fn classify_group(frames: &[&(AtomicLinkage, Vec<u8>)]) -> AtomicGroupClass {
    let mut prepares = Vec::new();
    let mut members = Vec::new();
    let mut decisions = Vec::new();
    for (link, body) in frames {
        match link.role {
            AtomicFrameRole::Prepare => match decode_prepare(body) {
                Ok(p) => prepares.push(p),
                Err(_) => {
                    return AtomicGroupClass::Corrupt {
                        reason: AtomicExamReason::BodyCorrupt,
                    };
                }
            },
            AtomicFrameRole::Member => match decode_member(body) {
                Ok(m) => members.push(m),
                Err(_) => {
                    return AtomicGroupClass::Corrupt {
                        reason: AtomicExamReason::BodyCorrupt,
                    };
                }
            },
            AtomicFrameRole::Commit => match decode_decision(body) {
                Ok(d) => decisions.push(d),
                Err(_) => {
                    return AtomicGroupClass::Corrupt {
                        reason: AtomicExamReason::BodyCorrupt,
                    };
                }
            },
        }
    }
    if !same_records(&prepares) {
        return AtomicGroupClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch,
        };
    }
    if !same_records(&decisions) {
        return AtomicGroupClass::Conflicting {
            reason: AtomicExamReason::ConflictingDecision,
        };
    }
    let members = match dedup_members(members) {
        Ok(m) => m,
        Err(class) => return class,
    };
    let prepare = prepares.into_iter().next();
    let decision = decisions.into_iter().next();
    let Some(prepare) = prepare else {
        return AtomicGroupClass::Partial {
            reason: AtomicExamReason::GroupIncomplete,
        };
    };
    let Some(decision) = decision else {
        return AtomicGroupClass::Partial {
            reason: AtomicExamReason::GroupIncomplete,
        };
    };
    let Ok(ph) = prepare_hash(&prepare) else {
        return AtomicGroupClass::Corrupt {
            reason: AtomicExamReason::BodyCorrupt,
        };
    };
    if ph != decision.prepare_hash {
        return AtomicGroupClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch,
        };
    }
    match decision.decision {
        DecisionCode::Committed => classify_committed_group(prepare, decision, &members),
        DecisionCode::NotCommitted => classify_not_committed_group(prepare, decision, &members),
    }
}

fn classify_committed_group(
    prepare: AtomicPrepare,
    decision: AtomicDecision,
    members: &[AtomicMember],
) -> AtomicGroupClass {
    if decision.member_count as usize != members.len() {
        return AtomicGroupClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch,
        };
    }
    let heap = match HeapId::from_bytes(prepare.heap_id.to_bytes()) {
        Ok(h) => h,
        Err(_) => {
            return AtomicGroupClass::Corrupt {
                reason: AtomicExamReason::BodyCorrupt,
            };
        }
    };
    match ordered_member_manifest_root(heap, members) {
        Ok(root)
            if root == decision.member_root && root == prepare.ordered_member_manifest_root =>
        {
            AtomicGroupClass::Consistent
        }
        Ok(_) => AtomicGroupClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch,
        },
        Err(_) => AtomicGroupClass::Corrupt {
            reason: AtomicExamReason::BodyCorrupt,
        },
    }
}

/// CR-ATMR3-002: not-committed is a normal terminal outcome.
///
/// The decision names the intended manifest and zero committed members. Observed
/// staged frames are reclaimable leftovers, not a body mismatch against that
/// intended root.
fn classify_not_committed_group(
    prepare: AtomicPrepare,
    decision: AtomicDecision,
    members: &[AtomicMember],
) -> AtomicGroupClass {
    if decision.validate().is_err()
        || decision.durability != Durability::Durable
        || decision.member_count != 0
        || decision.member_root != prepare.ordered_member_manifest_root
    {
        return AtomicGroupClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch,
        };
    }
    AtomicGroupClass::NotCommitted {
        reclaimable_members: !members.is_empty(),
    }
}

fn same_records<T: PartialEq>(items: &[T]) -> bool {
    items.windows(2).all(|w| w[0] == w[1])
}

fn dedup_members(members: Vec<AtomicMember>) -> Result<Vec<AtomicMember>, AtomicGroupClass> {
    let mut out: Vec<AtomicMember> = Vec::new();
    for m in members {
        if let Some(existing) = out.iter().find(|e| e.ordinal == m.ordinal) {
            if existing != &m {
                return Err(AtomicGroupClass::Corrupt {
                    reason: AtomicExamReason::BodyMismatch,
                });
            }
        } else {
            out.push(m);
        }
    }
    Ok(out)
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
