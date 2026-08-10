//! HP-002 store-level one-heap admission.

use residiuum_format::{
    encode_subject_v2, ActiveSegment, SafetyLimits, SegmentId, SubjectObjectKind,
};
use residiuum_store::{heap_binding_envelope, require_admit};

#[test]
fn bound_segment_never_admits_foreign_frame() {
    let heap_a = [0xA1u8; 16];
    let heap_b = [0xB2u8; 16];
    let env_a = heap_binding_envelope(&heap_a).unwrap();
    let env_b = heap_binding_envelope(&heap_b).unwrap();

    let ids = SegmentId {
        store_id: [1u8; 16],
        segment_id: [2u8; 16],
    };
    let seg =
        ActiveSegment::create_with_descriptor_envelope(ids, SafetyLimits::default(), 0, &env_a)
            .unwrap();
    assert_eq!(seg.frame_count(), 1);

    let subj = encode_subject_v2(
        &heap_a,
        SubjectObjectKind::HeapMetadata,
        &[0u8; 16],
        b"\x01",
    )
    .unwrap();
    require_admit(&heap_a, &env_a, &env_a, Some(&subj)).unwrap();
    assert!(require_admit(&heap_a, &env_a, &env_b, Some(&subj)).is_err());
    assert!(require_admit(&heap_b, &env_a, &env_a, None).is_err());

    // Concatenated / hole-like garbage cannot flip ownership.
    let mut concat = env_a.clone();
    concat.extend_from_slice(&env_b);
    assert!(require_admit(&heap_a, &concat, &env_a, None).is_err());
}
