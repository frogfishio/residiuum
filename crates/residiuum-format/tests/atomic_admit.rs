//! CR-ATM2-002: Atomic frames admit through ownership + segment/scan/recovery.

use residiuum_format::{
    admit_frame_to_heap, encode_atomic_commit_envelope, encode_atomic_frame,
    encode_atomic_member_envelope, encode_atomic_prepare_envelope, encode_deterministic_uint_map,
    encode_heap_binding_envelope, parse_ownership_envelope, read_atomic_evidence, scan_forward,
    ActiveSegment, AdmitDecision, AtomicFrameRole, CborValue, FrameKind, OwnershipEvidence,
    SafetyLimits, SegmentId, ENV_ATOMIC_CONTENT_ROOT, ENV_ATOMIC_ID, ENV_ATOMIC_ORDINAL,
};

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

fn eid(n: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = n;
    id
}

fn body_map() -> Vec<u8> {
    encode_deterministic_uint_map(&[(1, CborValue::Uint(1))]).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn golden_prepare_member_commit_envelopes_are_stable() {
    let heap = [0xaau8; 16];
    let id = aid(1);
    let cr = root(7);
    let prepare = encode_atomic_prepare_envelope(&heap, &id, &cr).unwrap();
    let member = encode_atomic_member_envelope(&heap, &id, 0, &cr, Some(3)).unwrap();
    let commit = encode_atomic_commit_envelope(&heap, &id, &cr, Some(3)).unwrap();

    assert_eq!(
        hex(&prepare),
        concat!(
            "a4181f50aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa182201182558200100000000",
            "0000000000000000000000000000000000000000000000000000001827582000",
            "00000000000000000000000000000000000000000000000000000000000007"
        )
    );
    assert_eq!(
        hex(&member),
        concat!(
            "a6181f50aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa182201182558200100000000",
            "0000000000000000000000000000000000000000000000000000001826001827",
            "5820000000000000000000000000000000000000000000000000000000000000",
            "0007182803"
        )
    );
    assert_eq!(
        hex(&commit),
        concat!(
            "a5181f50aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa182201182558200100000000",
            "0000000000000000000000000000000000000000000000000000001827582000",
            "0000000000000000000000000000000000000000000000000000000000000718",
            "2803"
        )
    );
}

#[test]
fn admit_prepare_member_commit_to_bound_heap_only() {
    let heap_a = hid(0xaa);
    let heap_b = hid(0xbb);
    let seg_a = encode_heap_binding_envelope(&heap_a).unwrap();
    let id = aid(1);
    let cr = root(7);

    let prepare = encode_atomic_prepare_envelope(&heap_a, &id, &cr).unwrap();
    let member = encode_atomic_member_envelope(&heap_a, &id, 0, &cr, Some(3)).unwrap();
    let commit = encode_atomic_commit_envelope(&heap_a, &id, &cr, Some(3)).unwrap();

    for env in [&prepare, &member, &commit] {
        assert!(
            matches!(
                admit_frame_to_heap(&heap_a, &seg_a, env, None),
                AdmitDecision::Admit {
                    ownership: OwnershipEvidence::Known { heap_id, .. }
                } if heap_id == heap_a
            ),
            "same-heap admit failed"
        );
        assert!(
            matches!(
                admit_frame_to_heap(&heap_b, &seg_a, env, None),
                AdmitDecision::RejectWrongHeap { claimed } if claimed == heap_a
            ),
            "cross-heap must reject"
        );
    }
}

#[test]
fn atomic_only_envelope_without_ownership_is_unknown() {
    let heap = hid(0xaa);
    let seg = encode_heap_binding_envelope(&heap).unwrap();
    let env = encode_deterministic_uint_map(&[
        (ENV_ATOMIC_ID, CborValue::Bytes(aid(1).to_vec())),
        (ENV_ATOMIC_CONTENT_ROOT, CborValue::Bytes(root(7).to_vec())),
        (ENV_ATOMIC_ORDINAL, CborValue::Uint(0)),
    ])
    .unwrap();
    // Segment Known + frame Unknown agrees as Known, so admission still
    // succeeds from the segment binding. Frame-only parse is Unknown.
    assert_eq!(
        parse_ownership_envelope(&env).unwrap(),
        OwnershipEvidence::Unknown
    );
    assert!(matches!(
        admit_frame_to_heap(&heap, &seg, &env, None),
        AdmitDecision::Admit { .. }
    ));
    assert!(matches!(
        admit_frame_to_heap(&heap, residiuum_format::EMPTY_ENVELOPE, &env, None),
        AdmitDecision::RejectUnknown
    ));
}

#[test]
fn golden_frames_append_scan_and_recover() {
    let heap = hid(0xaa);
    let id = aid(1);
    let cr = root(7);
    let seg_env = encode_heap_binding_envelope(&heap).unwrap();
    let mut seg = ActiveSegment::create_with_descriptor_envelope(
        SegmentId::new([0x11; 16], [0x22; 16]),
        SafetyLimits::default(),
        0,
        &seg_env,
    )
    .unwrap();

    let prepare = encode_atomic_frame(
        FrameKind::BatchPrepare,
        &encode_atomic_prepare_envelope(&heap, &id, &cr).unwrap(),
        &body_map(),
        eid(1),
    )
    .unwrap();
    let member = encode_atomic_frame(
        FrameKind::ItemEvent,
        &encode_atomic_member_envelope(&heap, &id, 0, &cr, Some(3)).unwrap(),
        b"payload",
        eid(2),
    )
    .unwrap();
    let commit = encode_atomic_frame(
        FrameKind::BatchCommit,
        &encode_atomic_commit_envelope(&heap, &id, &cr, Some(3)).unwrap(),
        &body_map(),
        eid(3),
    )
    .unwrap();

    for frame in [&prepare, &member, &commit] {
        let decoded = residiuum_format::decode_frame(frame, SafetyLimits::default()).unwrap();
        assert!(matches!(
            admit_frame_to_heap(&heap, &seg_env, &decoded.envelope, None),
            AdmitDecision::Admit { .. }
        ));
        seg.append_preencoded_frame(frame).unwrap();
    }

    let bytes = seg.as_bytes();
    let scan = scan_forward(bytes, SafetyLimits::default());
    // descriptor + 3 atomic frames
    assert_eq!(scan.verified_count(), 4);
    assert_eq!(scan.holes().count(), 0);

    let report = read_atomic_evidence(bytes, SafetyLimits::default());
    let valid: Vec<_> = report.valid().collect();
    assert_eq!(valid.len(), 3);
    assert_eq!(valid[0].role, AtomicFrameRole::Prepare);
    assert_eq!(valid[1].role, AtomicFrameRole::Member);
    assert_eq!(valid[2].role, AtomicFrameRole::Commit);
    assert_eq!(valid[0].atomic_id, id);
    assert_eq!(valid[1].ordinal, Some(0));
    assert_eq!(valid[2].commit_position, Some(3));
}

#[test]
fn old_reader_disposition_unknown_key_vs_new_ignore() {
    // Compatibility: a reader that still rejects keys >36 cannot admit Atomic
    // frames. The new parser ignores 37–40. Documented in FORMAT_SPEC §4.4.
    let heap = hid(0xaa);
    let env = encode_atomic_prepare_envelope(&heap, &aid(1), &root(7)).unwrap();
    match parse_ownership_envelope(&env).unwrap() {
        OwnershipEvidence::Known { heap_id, .. } => assert_eq!(heap_id, heap),
        other => panic!("{other:?}"),
    }
}
