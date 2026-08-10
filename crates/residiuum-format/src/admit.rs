//! One-heap frame admission (`HEAP_SPEC` §34.1 / §34.6).

use crate::ownership::{
    agree_ownership, parse_ownership_envelope, OwnershipError, OwnershipEvidence,
};
use crate::subject_v2::{decode_subject_v2, SubjectObjectKind, SubjectV2Error};
use thiserror::Error;

/// Outcome of trying to admit a frame into a bound heap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmitDecision {
    /// Frame belongs to the bound heap.
    Admit {
        /// Agreed ownership.
        ownership: OwnershipEvidence,
    },
    /// Missing / insufficient ownership for ordinary mount (salvage may still use Known).
    RejectUnknown,
    /// Integrity-valid conflicting ownership.
    RejectConflict,
    /// Ownership names a different heap.
    RejectWrongHeap {
        /// Claimed owner.
        claimed: [u8; 16],
    },
    /// Subject / envelope disagree.
    RejectSubjectMismatch,
    /// Malformed ownership or subject.
    RejectMalformed,
}

/// Admission errors (programming / parse).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AdmitError {
    /// Ownership parse failed hard (unknown key etc.).
    #[error("ownership: {0}")]
    Ownership(#[from] OwnershipError),
    /// Subject decode failed.
    #[error("subject: {0}")]
    Subject(#[from] SubjectV2Error),
}

/// Ordinary live-store admission: segment descriptor ownership, frame envelope,
/// and subject must agree on `bound_heap`.
pub fn admit_frame_to_heap(
    bound_heap: &[u8; 16],
    segment_descriptor_envelope: &[u8],
    frame_envelope: &[u8],
    subject: Option<&[u8]>,
) -> AdmitDecision {
    let seg = match parse_ownership_envelope(segment_descriptor_envelope) {
        Ok(o) => o,
        Err(OwnershipError::Conflict) => return AdmitDecision::RejectConflict,
        Err(_) => return AdmitDecision::RejectMalformed,
    };
    let frame = match parse_ownership_envelope(frame_envelope) {
        Ok(o) => o,
        Err(OwnershipError::Conflict) => return AdmitDecision::RejectConflict,
        Err(_) => return AdmitDecision::RejectMalformed,
    };
    let agreed = match agree_ownership(&seg, &frame) {
        Ok(o) => o,
        Err(OwnershipError::Conflict) => return AdmitDecision::RejectConflict,
        Err(_) => return AdmitDecision::RejectMalformed,
    };
    match &agreed {
        OwnershipEvidence::Unknown => return AdmitDecision::RejectUnknown,
        OwnershipEvidence::Known { heap_id, .. } if heap_id != bound_heap => {
            return AdmitDecision::RejectWrongHeap { claimed: *heap_id };
        }
        OwnershipEvidence::Known { .. } => {}
    }

    if let Some(subj) = subject {
        match decode_subject_v2(subj) {
            Ok(sv2) => {
                if sv2.heap_id != bound_heap {
                    return AdmitDecision::RejectWrongHeap {
                        claimed: *sv2.heap_id,
                    };
                }
                if let OwnershipEvidence::Known {
                    heap_id,
                    collection_id,
                    stream_id,
                    ..
                } = &agreed
                {
                    if heap_id != sv2.heap_id {
                        return AdmitDecision::RejectSubjectMismatch;
                    }
                    match sv2.object_kind {
                        SubjectObjectKind::HeapMetadata => {
                            if collection_id.is_some() || stream_id.is_some() {
                                return AdmitDecision::RejectSubjectMismatch;
                            }
                        }
                        SubjectObjectKind::Collection => {
                            if collection_id.as_ref() != Some(sv2.object_id) {
                                return AdmitDecision::RejectSubjectMismatch;
                            }
                        }
                        SubjectObjectKind::Stream => {
                            if stream_id.as_ref() != Some(sv2.object_id) {
                                return AdmitDecision::RejectSubjectMismatch;
                            }
                        }
                    }
                }
            }
            Err(_) => return AdmitDecision::RejectMalformed,
        }
    }

    AdmitDecision::Admit { ownership: agreed }
}

/// Salvage admission: frame envelope + subject alone may admit when they agree,
/// even if the segment descriptor is missing/damaged.
pub fn salvage_admit_frame(
    frame_envelope: &[u8],
    subject: &[u8],
) -> Result<OwnershipEvidence, AdmitError> {
    let frame = parse_ownership_envelope(frame_envelope)?;
    let sv2 = decode_subject_v2(subject)?;
    match frame {
        OwnershipEvidence::Known { heap_id, .. } if heap_id == *sv2.heap_id => Ok(frame),
        OwnershipEvidence::Known { .. } => Err(AdmitError::Ownership(OwnershipError::Conflict)),
        OwnershipEvidence::Unknown => Ok(OwnershipEvidence::Unknown),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ownership::encode_heap_binding_envelope;
    use crate::subject_v2::{encode_subject_v2, SubjectObjectKind};

    #[test]
    fn admits_matching_and_rejects_cross_heap() {
        let heap_a = [0xAAu8; 16];
        let heap_b = [0xBBu8; 16];
        let env_a = encode_heap_binding_envelope(&heap_a).unwrap();
        let env_b = encode_heap_binding_envelope(&heap_b).unwrap();
        let subj =
            encode_subject_v2(&heap_a, SubjectObjectKind::Collection, &[0x11u8; 16], b"k").unwrap();
        // Collection subject needs collection_id on envelope — use binding-only for segment
        // and build a richer frame envelope.
        let frame_env = crate::cbor_envelope::encode_deterministic_uint_map(&[
            (31, crate::cbor_envelope::CborValue::Bytes(heap_a.to_vec())),
            (32, crate::cbor_envelope::CborValue::Bytes(vec![0x11; 16])),
            (34, crate::cbor_envelope::CborValue::Uint(1)),
        ])
        .unwrap();

        assert!(matches!(
            admit_frame_to_heap(&heap_a, &env_a, &frame_env, Some(&subj)),
            AdmitDecision::Admit { .. }
        ));
        assert!(matches!(
            admit_frame_to_heap(&heap_a, &env_a, &env_b, Some(&subj)),
            AdmitDecision::RejectConflict | AdmitDecision::RejectWrongHeap { .. }
        ));
        assert!(matches!(
            admit_frame_to_heap(&heap_b, &env_a, &frame_env, Some(&subj)),
            AdmitDecision::RejectWrongHeap { .. }
        ));
    }

    #[test]
    fn adversarial_swapped_segment_label_rejected() {
        let heap_a = [0x01u8; 16];
        let heap_b = [0x02u8; 16];
        let env_a = encode_heap_binding_envelope(&heap_a).unwrap();
        let env_b = encode_heap_binding_envelope(&heap_b).unwrap();
        let subj = encode_subject_v2(
            &heap_a,
            SubjectObjectKind::HeapMetadata,
            &[0u8; 16],
            &[0x01],
        )
        .unwrap();
        // Segment claims B, frame+subject claim A → conflict or wrong heap for binder A.
        let d = admit_frame_to_heap(&heap_a, &env_b, &env_a, Some(&subj));
        assert!(
            matches!(
                d,
                AdmitDecision::RejectConflict | AdmitDecision::RejectWrongHeap { .. }
            ),
            "{d:?}"
        );
    }

    #[test]
    fn truncated_rejected_and_empty_segment_admits_known_frame() {
        let heap = [0x03u8; 16];
        let env = encode_heap_binding_envelope(&heap).unwrap();
        assert!(matches!(
            admit_frame_to_heap(&heap, &env[..3], &env, None),
            AdmitDecision::RejectMalformed
        ));
        // Empty segment + known frame: agree picks Known from frame.
        assert!(matches!(
            admit_frame_to_heap(&heap, crate::cbor_envelope::EMPTY_ENVELOPE, &env, None),
            AdmitDecision::Admit { .. }
        ));
        // Both unknown → RejectUnknown.
        assert!(matches!(
            admit_frame_to_heap(
                &heap,
                crate::cbor_envelope::EMPTY_ENVELOPE,
                crate::cbor_envelope::EMPTY_ENVELOPE,
                None
            ),
            AdmitDecision::RejectUnknown
        ));
    }

    #[test]
    fn adversarial_concat_and_hole_never_cross_heap() {
        let heap_a = [0xAAu8; 16];
        let heap_b = [0xBBu8; 16];
        let env_a = encode_heap_binding_envelope(&heap_a).unwrap();
        let env_b = encode_heap_binding_envelope(&heap_b).unwrap();
        // Concatenated envelopes are malformed CBOR, not a second owner.
        let mut concat = env_a.clone();
        concat.extend_from_slice(&env_b);
        assert!(matches!(
            admit_frame_to_heap(&heap_a, &concat, &env_a, None),
            AdmitDecision::RejectMalformed
        ));
        // Hole / empty subject path still rejects wrong-heap frame.
        assert!(matches!(
            admit_frame_to_heap(&heap_a, &env_a, &env_b, None),
            AdmitDecision::RejectConflict | AdmitDecision::RejectWrongHeap { .. }
        ));
    }
}
