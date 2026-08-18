//! ATM-2.1 / CR-ATM2-003: Atomic envelopes and recovery reader with frozen bodies.

use residiuum_format::{
    decode_deterministic_uint_map, encode_atomic_commit_envelope, encode_atomic_frame,
    encode_atomic_member_envelope, encode_atomic_prepare_envelope, encode_deterministic_uint_map,
    encode_frame, read_atomic_evidence, AtomicEvidenceClass, AtomicExamReason, AtomicFrameRole,
    AtomicGroupClass, CborValue, FrameHeader, FrameKind, FrameParts, SafetyLimits, EMPTY_ENVELOPE,
    ENV_ATOMIC_CONTENT_ROOT, ENV_ATOMIC_ID, WIRE_MAJOR, WIRE_MINOR,
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

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn vector_body(name: &str) -> Vec<u8> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../spec/atomics/evidence-vectors.json"
    );
    let doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("vectors")).unwrap();
    for item in doc["accepted"].as_array().unwrap() {
        if item["name"].as_str() == Some(name) {
            return hex_decode(item["bytes_hex"].as_str().unwrap());
        }
    }
    panic!("missing vector {name}");
}

/// Identities frozen in `prepare_local_heap` / `member_create_*` / `decision_committed`.
fn vec_heap() -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0] = 1;
    b
}
fn vec_aid() -> [u8; 32] {
    let mut b = [0u8; 32];
    b[0] = 9;
    b
}
fn vec_root() -> [u8; 32] {
    [7u8; 32]
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
    let id = vec_aid();
    let cr = vec_root();
    let heap = vec_heap();
    let prep = encode_atomic_frame(
        FrameKind::BatchPrepare,
        &encode_atomic_prepare_envelope(&heap, &id, &cr).unwrap(),
        &vector_body("prepare_local_heap"),
        eid(1),
    )
    .unwrap();
    let member = encode_atomic_frame(
        FrameKind::ItemEvent,
        &encode_atomic_member_envelope(&heap, &id, 0, &cr, Some(1)).unwrap(),
        &vector_body("member_create_with_object_identity"),
        eid(2),
    )
    .unwrap();
    let commit = encode_atomic_frame(
        FrameKind::BatchCommit,
        &encode_atomic_commit_envelope(&heap, &id, &cr, Some(1)).unwrap(),
        &vector_body("decision_committed"),
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
    assert_eq!(valid[1].commit_position, Some(1));
    assert_eq!(valid[2].role, AtomicFrameRole::Commit);
    assert!(report.scan.holes().next().is_some());
    assert_eq!(report.groups.len(), 1);
    assert_eq!(report.groups[0].class, AtomicGroupClass::Consistent);
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

#[test]
fn mere_cbor_body_is_never_valid() {
    let env = encode_atomic_prepare_envelope(&vec_heap(), &vec_aid(), &vec_root()).unwrap();
    let frame = encode_atomic_frame(FrameKind::BatchPrepare, &env, &body_map(), eid(11)).unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    assert_eq!(
        report.examined[0].class,
        AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::BodyCorrupt
        }
    );
    assert_eq!(report.valid().count(), 0);
}

#[test]
fn mutating_prepare_atomic_id_is_body_mismatch() {
    let mut map = decode_deterministic_uint_map(&vector_body("prepare_local_heap")).unwrap();
    for (k, v) in &mut map {
        if *k == 1 {
            *v = CborValue::Bytes(vec![0x0a; 32]);
        }
    }
    let body = encode_deterministic_uint_map(&map).unwrap();
    let env = encode_atomic_prepare_envelope(&vec_heap(), &vec_aid(), &vec_root()).unwrap();
    let frame = encode_atomic_frame(FrameKind::BatchPrepare, &env, &body, eid(12)).unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    assert_eq!(
        report.examined[0].class,
        AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch
        }
    );
}

#[test]
fn mutating_member_ordinal_is_body_mismatch() {
    let mut map =
        decode_deterministic_uint_map(&vector_body("member_create_with_object_identity")).unwrap();
    for (k, v) in &mut map {
        if *k == 2 {
            *v = CborValue::Uint(9);
        }
    }
    let body = encode_deterministic_uint_map(&map).unwrap();
    let env =
        encode_atomic_member_envelope(&vec_heap(), &vec_aid(), 0, &vec_root(), Some(1)).unwrap();
    let frame = encode_atomic_frame(FrameKind::ItemEvent, &env, &body, eid(13)).unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    assert_eq!(
        report.examined[0].class,
        AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch
        }
    );
}

#[test]
fn mutating_decision_id_is_body_mismatch() {
    let mut map = decode_deterministic_uint_map(&vector_body("decision_committed")).unwrap();
    for (k, v) in &mut map {
        if *k == 1 {
            *v = CborValue::Bytes(vec![0x0a; 32]);
        }
    }
    let body = encode_deterministic_uint_map(&map).unwrap();
    let env = encode_atomic_commit_envelope(&vec_heap(), &vec_aid(), &vec_root(), Some(1)).unwrap();
    let frame = encode_atomic_frame(FrameKind::BatchCommit, &env, &body, eid(14)).unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    assert_eq!(
        report.examined[0].class,
        AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch
        }
    );
}

#[test]
fn conflicting_decisions_are_not_guessed() {
    let heap = vec_heap();
    let id = vec_aid();
    let cr = vec_root();
    let prep = encode_atomic_frame(
        FrameKind::BatchPrepare,
        &encode_atomic_prepare_envelope(&heap, &id, &cr).unwrap(),
        &vector_body("prepare_local_heap"),
        eid(15),
    )
    .unwrap();
    let member = encode_atomic_frame(
        FrameKind::ItemEvent,
        &encode_atomic_member_envelope(&heap, &id, 0, &cr, Some(1)).unwrap(),
        &vector_body("member_create_with_object_identity"),
        eid(16),
    )
    .unwrap();
    let committed = encode_atomic_frame(
        FrameKind::BatchCommit,
        &encode_atomic_commit_envelope(&heap, &id, &cr, Some(1)).unwrap(),
        &vector_body("decision_committed"),
        eid(17),
    )
    .unwrap();
    let aborted = encode_atomic_frame(
        FrameKind::BatchCommit,
        &encode_atomic_commit_envelope(&heap, &id, &cr, None).unwrap(),
        &vector_body("decision_not_committed_precondition"),
        eid(18),
    )
    .unwrap();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&prep);
    bytes.extend_from_slice(&member);
    bytes.extend_from_slice(&committed);
    bytes.extend_from_slice(&aborted);
    let report = read_atomic_evidence(&bytes, SafetyLimits::default());
    assert_eq!(report.valid().count(), 4);
    assert_eq!(report.groups.len(), 1);
    assert_eq!(
        report.groups[0].class,
        AtomicGroupClass::Conflicting {
            reason: AtomicExamReason::ConflictingDecision
        }
    );
}

#[test]
fn cross_linked_member_is_body_mismatch() {
    let env = encode_atomic_member_envelope(&vec_heap(), &aid(1), 0, &vec_root(), Some(1)).unwrap();
    let frame = encode_atomic_frame(
        FrameKind::ItemEvent,
        &env,
        &vector_body("member_create_with_object_identity"),
        eid(19),
    )
    .unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    assert_eq!(
        report.examined[0].class,
        AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch
        }
    );
}

#[test]
fn atomic_frame_without_ownership_is_not_valid() {
    let env = encode_deterministic_uint_map(&[
        (ENV_ATOMIC_ID, CborValue::Bytes(vec_aid().to_vec())),
        (
            ENV_ATOMIC_CONTENT_ROOT,
            CborValue::Bytes(vec_root().to_vec()),
        ),
    ])
    .unwrap();
    let frame = encode_atomic_frame(
        FrameKind::BatchPrepare,
        &env,
        &vector_body("prepare_local_heap"),
        eid(20),
    )
    .unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    assert_eq!(
        report.examined[0].class,
        AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::MissingOwnership
        }
    );
    assert_eq!(report.valid().count(), 0);
    assert!(report.groups.is_empty());
}

#[test]
fn malformed_ownership_is_field_corrupt() {
    let env = encode_deterministic_uint_map(&[
        (31, CborValue::Uint(1)),
        (34, CborValue::Uint(1)),
        (ENV_ATOMIC_ID, CborValue::Bytes(vec_aid().to_vec())),
        (
            ENV_ATOMIC_CONTENT_ROOT,
            CborValue::Bytes(vec_root().to_vec()),
        ),
    ])
    .unwrap();
    let frame = encode_atomic_frame(
        FrameKind::BatchPrepare,
        &env,
        &vector_body("prepare_local_heap"),
        eid(21),
    )
    .unwrap();
    let report = read_atomic_evidence(&frame, SafetyLimits::default());
    assert_eq!(
        report.examined[0].class,
        AtomicEvidenceClass::Corrupt {
            reason: AtomicExamReason::FieldCorrupt
        }
    );
}

#[test]
fn same_id_different_roots_are_one_conflict() {
    let heap = vec_heap();
    let id = vec_aid();
    let body = vector_body("member_create_with_object_identity");
    let a = encode_atomic_frame(
        FrameKind::ItemEvent,
        &encode_atomic_member_envelope(&heap, &id, 0, &root(1), None).unwrap(),
        &body,
        eid(22),
    )
    .unwrap();
    let b = encode_atomic_frame(
        FrameKind::ItemEvent,
        &encode_atomic_member_envelope(&heap, &id, 0, &root(2), None).unwrap(),
        &body,
        eid(23),
    )
    .unwrap();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&a);
    bytes.extend_from_slice(&b);
    let report = read_atomic_evidence(&bytes, SafetyLimits::default());
    assert_eq!(report.valid().count(), 2);
    assert_eq!(report.groups.len(), 1);
    assert_eq!(report.groups[0].heap_id, Some(heap));
    assert_eq!(report.groups[0].atomic_id, id);
    assert_eq!(
        report.groups[0].class,
        AtomicGroupClass::Conflicting {
            reason: AtomicExamReason::ConflictingContentRoot
        }
    );
}
