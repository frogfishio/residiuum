//! One-heap frame admission for store façades (`HEAP_SPEC` HP-002).

use crate::error::StoreError;
use residiuum_format::{
    admit_frame_to_heap, encode_heap_binding_envelope, AdmitDecision, OwnershipEvidence,
};

/// Salvage classification of one frame against a bound heap (mixed multi-heap media).
///
/// Used when scanning a damaged volume that may contain labelled units from more
/// than one heap: operators must separate **this** heap's frames from foreign /
/// unknown / conflicting evidence without reassigning ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MixedHeapSalvageClass {
    /// Frame admits to the bound heap (recoverable for this heap).
    BelongingToBound,
    /// Integrity-valid ownership for a different heap.
    Foreign {
        /// Claimed owner heap id.
        claimed: [u8; 16],
    },
    /// Missing / insufficient ownership (may still be examined offline).
    Unknown,
    /// Conflicting ownership claims.
    Conflict,
    /// Malformed envelope / subject mismatch.
    Malformed,
}

/// Classify a frame for mixed-heap salvage (no heap reassignment).
///
/// Wraps [`admit_frame_to_heap`] into an operator-facing salvage taxonomy.
pub fn classify_mixed_heap_frame(
    bound_heap: &[u8; 16],
    segment_descriptor_envelope: &[u8],
    frame_envelope: &[u8],
    subject: Option<&[u8]>,
) -> MixedHeapSalvageClass {
    match admit_frame_to_heap(
        bound_heap,
        segment_descriptor_envelope,
        frame_envelope,
        subject,
    ) {
        AdmitDecision::Admit { .. } => MixedHeapSalvageClass::BelongingToBound,
        AdmitDecision::RejectWrongHeap { claimed } => MixedHeapSalvageClass::Foreign { claimed },
        AdmitDecision::RejectUnknown => MixedHeapSalvageClass::Unknown,
        AdmitDecision::RejectConflict => MixedHeapSalvageClass::Conflict,
        AdmitDecision::RejectSubjectMismatch | AdmitDecision::RejectMalformed => {
            MixedHeapSalvageClass::Malformed
        }
    }
}

/// Convert an [`AdmitDecision`] into a store result.
pub fn require_admit(
    bound_heap: &[u8; 16],
    segment_descriptor_envelope: &[u8],
    frame_envelope: &[u8],
    subject: Option<&[u8]>,
) -> Result<OwnershipEvidence, StoreError> {
    match admit_frame_to_heap(
        bound_heap,
        segment_descriptor_envelope,
        frame_envelope,
        subject,
    ) {
        AdmitDecision::Admit { ownership } => Ok(ownership),
        AdmitDecision::RejectUnknown => Err(StoreError::HeapAdmit("unknown ownership".into())),
        AdmitDecision::RejectConflict => Err(StoreError::HeapAdmit("ownership conflict".into())),
        AdmitDecision::RejectWrongHeap { claimed } => Err(StoreError::HeapAdmit(format!(
            "wrong heap {}",
            hex16(&claimed)
        ))),
        AdmitDecision::RejectSubjectMismatch => {
            Err(StoreError::HeapAdmit("subject mismatch".into()))
        }
        AdmitDecision::RejectMalformed => Err(StoreError::HeapAdmit("malformed ownership".into())),
    }
}

/// Build the canonical heap-binding envelope for a segment descriptor.
pub fn heap_binding_envelope(heap_id: &[u8; 16]) -> Result<Vec<u8>, StoreError> {
    encode_heap_binding_envelope(heap_id).map_err(|e| StoreError::HeapAdmit(e.to_string()))
}

fn hex16(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_cross_heap_and_accepts_match() {
        let a = [0x11u8; 16];
        let b = [0x22u8; 16];
        let ea = heap_binding_envelope(&a).unwrap();
        let eb = heap_binding_envelope(&b).unwrap();
        assert!(require_admit(&a, &ea, &ea, None).is_ok());
        assert!(require_admit(&a, &ea, &eb, None).is_err());
    }

    #[test]
    fn mixed_heap_salvage_classifies_bound_and_foreign() {
        let a = [0x11u8; 16];
        let b = [0x22u8; 16];
        let ea = heap_binding_envelope(&a).unwrap();
        let eb = heap_binding_envelope(&b).unwrap();
        assert_eq!(
            classify_mixed_heap_frame(&a, &ea, &ea, None),
            MixedHeapSalvageClass::BelongingToBound
        );
        // Pure foreign unit (segment + frame both heap B).
        assert_eq!(
            classify_mixed_heap_frame(&a, &eb, &eb, None),
            MixedHeapSalvageClass::Foreign { claimed: b }
        );
        // Cross-labelled envelopes disagree → conflict (not reassigned).
        assert_eq!(
            classify_mixed_heap_frame(&a, &ea, &eb, None),
            MixedHeapSalvageClass::Conflict
        );
        assert_eq!(
            classify_mixed_heap_frame(&a, &[], &[], None),
            MixedHeapSalvageClass::Malformed
        );
    }
}
