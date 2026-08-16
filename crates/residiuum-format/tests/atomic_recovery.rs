//! ATM-2.1: Atomic envelopes and byte-level recovery reader (no store write path).

use residiuum_format::{
    encode_atomic_commit_envelope, encode_atomic_frame, encode_atomic_member_envelope,
    encode_atomic_prepare_envelope, encode_deterministic_uint_map, encode_frame,
    read_atomic_evidence, AtomicEvidenceClass, AtomicExamReason, AtomicFrameRole, CborValue,
    FrameHeader, FrameKind, FrameParts, SafetyLimits, EMPTY_ENVELOPE, WIRE_MAJOR, WIRE_MINOR,
};

fn eid(n: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = n;
    id
}

fn hid(n: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = n;
    id
}

fn aid(n: u8) -> [u8; 32] {
    let mut id = [0u8; 32];
    id[0] = n;
    id
}

fn root(n: u8) -> [u8; 32] {
    let mut id = [0u8; 32];
    id[31] = n;
    id
}

fn body_map() -> Vec<u8> {
    encode_deterministic_uint_map(&[(1, CborValue::Uint(1))]).unwrap()
}

fn legacy_batch() -> Vec<u8> {
    let parts = FrameParts {
        header: FrameHeader {
            wire_major: WIRE_MAJOR,
            wire_minor: WIRE_MINOR,
            frame_kind: FrameKind::BatchPrepare.as_u8(),
            flags: Default::default(),
            envelope_len: EMPTY_ENVELOPE.len() as u32,
            body_len: 0,
            logical_len: 0,
            writer_sequence: 0,
            event_id: eid(9),
        },
        envelope: EMPTY_ENVELOPE.to_vec(),
        body: Vec::new(),
    };
    encode_frame(&parts).unwrap()
}

#[test]
fn prepare_member_commit_roundtrip_is_valid() {
    let id = aid(1);
    let cr = root(7);
    let prep = encode_atomic_frame(
        FrameKind::BatchPrepare,
        &encode_atomic_prepare_envelope(&hid(1), &id, &cr).unwrap(),
        &body_map(),
        eid(1),
    )
    .unwrap();
    let member = encode_atomic_frame(
        FrameKind::ItemEvent,
        &encode_atomic_member_envelope(&hid(1), &id, 0, &cr, Some(3)).unwrap(),
        b"payload",
        eid(2),
    )
    .unwrap();
    let commit = encode_atomic_frame(
        FrameKind::BatchCommit,
        &encode_atomic_commit_envelope(&hid(1), &id, &cr, Some(3)).unwrap(),
        &body_map(),
        eid(3),
    )
    .unwrap();

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"GARBAGE");
    bytes.extend_from_slice(&prep);
    bytes.extend_from_slice(b"xx");
    bytes.extend_from_slice(&member);
    bytes.extend_from_slice(&commit);

    let report = read_atomic_evidence(&bytes, SafetyLimits::default());
    let valid: Vec<_> = report.valid().collect();
    assert_eq!(valid.len(), 3);
    assert_eq!(valid[0].role, AtomicFrameRole::Prepare);
    assert_eq!(valid[1].role, AtomicFrameRole::Member);
    assert_eq!(valid[1].ordinal, Some(0));
    assert_eq!(valid[1].commit_position, Some(3));
    assert_eq!(valid[2].role, AtomicFrameRole::Commit);
    assert!(report.scan.holes().next().is_some());
}

#[test]
fn legacy_batch_prepare_is_not_atomic_evidence() {
    let report = read_atomic_evidence(&legacy_batch(), SafetyLimits::default());
    assert_eq!(report.examined.len(), 1);
    assert_eq!(
        report.examined[0].class,
        AtomicEvidenceClass::Unsupported {
            reason: AtomicExamReason::NotAtomicEvidence
        }
    );
    assert_eq!(report.valid().count(), 0);
}

#[test]
fn batch_id_only_envelope_is_not_atomic_evidence() {
    let env = encode_deterministic_uint_map(&[(1, CborValue::Bytes(vec![1, 2, 3, 4]))]).unwrap();
    let frame = encode_atomic_frame(FrameKind::BatchCommit, &env, &body_map(), eid(4)).unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    assert_eq!(
        report.examined[0].class,
        AtomicEvidenceClass::Unsupported {
            reason: AtomicExamReason::NotAtomicEvidence
        }
    );
}

#[test]
fn member_missing_ordinal_is_partial() {
    let env = encode_atomic_prepare_envelope(&hid(1), &aid(2), &root(1)).unwrap();
    let frame = encode_atomic_frame(FrameKind::ItemEvent, &env, b"x", eid(5)).unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    match &report.examined[0].class {
        AtomicEvidenceClass::Partial {
            reason: AtomicExamReason::IncompleteLinkage,
            ..
        } => {}
        other => panic!("{other:?}"),
    }
}

#[test]
fn wrong_length_atomic_id_is_corrupt() {
    let env = encode_deterministic_uint_map(&[
        (37, CborValue::Bytes(vec![1, 2, 3])),
        (39, CborValue::Bytes(root(1).to_vec())),
    ])
    .unwrap();
    let frame = encode_atomic_frame(FrameKind::BatchPrepare, &env, &body_map(), eid(6)).unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    assert_eq!(
        report.examined[0].class,
        AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::FieldCorrupt
        }
    );
}

#[test]
fn prepare_with_ordinal_is_role_mismatch() {
    let env = encode_atomic_member_envelope(&hid(1), &aid(3), 1, &root(2), None).unwrap();
    let frame = encode_atomic_frame(FrameKind::BatchPrepare, &env, &body_map(), eid(7)).unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    assert_eq!(
        report.examined[0].class,
        AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::RoleMismatch
        }
    );
}

#[test]
fn missing_prepare_body_is_partial() {
    let env = encode_atomic_prepare_envelope(&hid(1), &aid(4), &root(3)).unwrap();
    let frame = encode_atomic_frame(FrameKind::BatchPrepare, &env, &[], eid(8)).unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    match &report.examined[0].class {
        AtomicEvidenceClass::Partial {
            reason: AtomicExamReason::BodyMissing,
            linkage: Some(link),
        } => {
            assert_eq!(link.role, AtomicFrameRole::Prepare);
            assert_eq!(link.atomic_id, aid(4));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn ordinary_item_event_is_ignored() {
    let frame =
        encode_atomic_frame(FrameKind::ItemEvent, EMPTY_ENVELOPE, b"hello", eid(10)).unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    assert!(report.examined.is_empty());
    assert_eq!(report.scan.verified_count(), 1);
}

#[test]
fn zero_commit_position_refuses_encode() {
    assert!(encode_atomic_commit_envelope(&hid(1), &aid(1), &root(1), Some(0)).is_err());
}
