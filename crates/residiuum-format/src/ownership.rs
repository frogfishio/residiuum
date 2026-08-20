//! Heap ownership envelope keys and agreement checks (`HEAP_SPEC` §34.1–§34.3).

use crate::cbor_envelope::{decode_deterministic_uint_map, CborEnvelopeError, CborValue};
use crate::envelope_keys::is_atomic_extension_key;
use crate::envelope_keys::{ENV_OPERATION_CONTENT_HASH, ENV_OPERATION_ID};
use thiserror::Error;

pub use crate::envelope_keys::{
    ENV_COLLECTION_ID, ENV_HEAP_ID, ENV_OWNERSHIP_PROFILE, ENV_SOURCE_HEAP_ID,
    ENV_SOURCE_OBJECT_ID, ENV_STREAM_ID,
};

/// `residiuum-heap-v1` ownership profile value.
pub const OWNERSHIP_PROFILE_V1: u64 = 1;

/// Ownership evidence extracted from an envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipEvidence {
    /// Known heap owner.
    Known {
        /// Heap id bytes.
        heap_id: [u8; 16],
        /// Profile value (must be 1 for v1).
        profile: u64,
        /// Optional collection id.
        collection_id: Option<[u8; 16]>,
        /// Optional stream id.
        stream_id: Option<[u8; 16]>,
    },
    /// Missing ownership fields.
    Unknown,
}

/// Ownership agreement / conflict outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OwnershipError {
    /// Envelope CBOR invalid.
    #[error("ownership envelope invalid: {0}")]
    Envelope(#[from] CborEnvelopeError),
    /// Integrity-valid claims disagree.
    #[error("ownership conflict")]
    Conflict,
    /// Unknown envelope key for profile v1.
    #[error("unknown envelope key {0}")]
    UnknownKey(u64),
    /// Profile is not v1.
    #[error("unsupported ownership profile")]
    BadProfile,
    /// Field length wrong.
    #[error("ownership field length invalid")]
    BadLength,
}

/// Parse ownership from a deterministic envelope map.
pub fn parse_ownership_envelope(bytes: &[u8]) -> Result<OwnershipEvidence, OwnershipError> {
    if bytes == crate::cbor_envelope::EMPTY_ENVELOPE {
        return Ok(OwnershipEvidence::Unknown);
    }
    let map = decode_deterministic_uint_map(bytes)?;
    let mut heap_id = None;
    let mut profile = None;
    let mut collection_id = None;
    let mut stream_id = None;
    for (k, v) in map {
        match k {
            1..=30 => {
                // Pre-heap keys retain existing meanings; ignored for ownership.
            }
            ENV_HEAP_ID => heap_id = Some(expect_b16(&v)?),
            ENV_COLLECTION_ID => collection_id = Some(expect_b16(&v)?),
            ENV_STREAM_ID => stream_id = Some(expect_b16(&v)?),
            ENV_OWNERSHIP_PROFILE => match v {
                CborValue::Uint(u) => profile = Some(u),
                _ => return Err(OwnershipError::BadLength),
            },
            ENV_SOURCE_HEAP_ID | ENV_SOURCE_OBJECT_ID => {
                // Provenance only; not ownership.
            }
            other if is_atomic_extension_key(other) => {
                // Reserved Atomic namespace (FORMAT_SPEC: understood
                // extensions are ignored by readers that do not consume them).
            }
            ENV_OPERATION_ID | ENV_OPERATION_CONTENT_HASH => {
                // Known client-operation namespace. It neither establishes nor
                // conflicts with Heap ownership and is not Atomic evidence.
            }
            other => return Err(OwnershipError::UnknownKey(other)),
        }
    }
    match (heap_id, profile) {
        (Some(heap_id), Some(profile)) => {
            if profile != OWNERSHIP_PROFILE_V1 {
                return Err(OwnershipError::BadProfile);
            }
            Ok(OwnershipEvidence::Known {
                heap_id,
                profile,
                collection_id,
                stream_id,
            })
        }
        (None, None) => Ok(OwnershipEvidence::Unknown),
        _ => Ok(OwnershipEvidence::Unknown),
    }
}

/// Encode the minimal heap-binding envelope `{31: heap_id, 34: 1}`.
pub fn encode_heap_binding_envelope(heap_id: &[u8; 16]) -> Result<Vec<u8>, CborEnvelopeError> {
    crate::cbor_envelope::encode_deterministic_uint_map(&[
        (ENV_HEAP_ID, CborValue::Bytes(heap_id.to_vec())),
        (ENV_OWNERSHIP_PROFILE, CborValue::Uint(OWNERSHIP_PROFILE_V1)),
    ])
}

/// Encode `{31: heap_id, 32: collection_id, 34: 1}`.
pub fn encode_collection_binding_envelope(
    heap_id: &[u8; 16],
    collection_id: &[u8; 16],
) -> Result<Vec<u8>, CborEnvelopeError> {
    crate::cbor_envelope::encode_deterministic_uint_map(&[
        (ENV_HEAP_ID, CborValue::Bytes(heap_id.to_vec())),
        (ENV_COLLECTION_ID, CborValue::Bytes(collection_id.to_vec())),
        (ENV_OWNERSHIP_PROFILE, CborValue::Uint(OWNERSHIP_PROFILE_V1)),
    ])
}

/// Encode `{31: heap_id, 33: stream_id, 34: 1}`.
pub fn encode_stream_binding_envelope(
    heap_id: &[u8; 16],
    stream_id: &[u8; 16],
) -> Result<Vec<u8>, CborEnvelopeError> {
    crate::cbor_envelope::encode_deterministic_uint_map(&[
        (ENV_HEAP_ID, CborValue::Bytes(heap_id.to_vec())),
        (ENV_STREAM_ID, CborValue::Bytes(stream_id.to_vec())),
        (ENV_OWNERSHIP_PROFILE, CborValue::Uint(OWNERSHIP_PROFILE_V1)),
    ])
}

/// Agree two ownership claims; conflict if both Known and disagree on heap,
/// collection, or stream. Matching Known claims merge optional ids.
pub fn agree_ownership(
    a: &OwnershipEvidence,
    b: &OwnershipEvidence,
) -> Result<OwnershipEvidence, OwnershipError> {
    match (a, b) {
        (
            OwnershipEvidence::Known {
                heap_id: h1,
                profile: p1,
                collection_id: c1,
                stream_id: s1,
            },
            OwnershipEvidence::Known {
                heap_id: h2,
                profile: p2,
                collection_id: c2,
                stream_id: s2,
            },
        ) => {
            if h1 != h2 {
                return Err(OwnershipError::Conflict);
            }
            if c1.is_some() && c2.is_some() && c1 != c2 {
                return Err(OwnershipError::Conflict);
            }
            if s1.is_some() && s2.is_some() && s1 != s2 {
                return Err(OwnershipError::Conflict);
            }
            Ok(OwnershipEvidence::Known {
                heap_id: *h1,
                profile: (*p1).max(*p2),
                collection_id: c1.or(*c2),
                stream_id: s1.or(*s2),
            })
        }
        (OwnershipEvidence::Known { .. }, _) => Ok(a.clone()),
        (_, OwnershipEvidence::Known { .. }) => Ok(b.clone()),
        _ => Ok(OwnershipEvidence::Unknown),
    }
}

fn expect_b16(v: &CborValue) -> Result<[u8; 16], OwnershipError> {
    match v {
        CborValue::Bytes(b) if b.len() == 16 => {
            let mut a = [0u8; 16];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(OwnershipError::BadLength),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cbor_envelope::{CborValue, EMPTY_ENVELOPE};

    #[test]
    fn empty_envelope_is_unknown() {
        assert_eq!(
            parse_ownership_envelope(EMPTY_ENVELOPE).unwrap(),
            OwnershipEvidence::Unknown
        );
    }

    #[test]
    fn heap_binding_roundtrip_and_conflict() {
        let heap_a = [0xAAu8; 16];
        let heap_b = [0xBBu8; 16];
        let env_a = encode_heap_binding_envelope(&heap_a).unwrap();
        let env_b = encode_heap_binding_envelope(&heap_b).unwrap();
        let a = parse_ownership_envelope(&env_a).unwrap();
        let b = parse_ownership_envelope(&env_b).unwrap();
        match &a {
            OwnershipEvidence::Known {
                heap_id, profile, ..
            } => {
                assert_eq!(heap_id, &heap_a);
                assert_eq!(*profile, OWNERSHIP_PROFILE_V1);
            }
            _ => panic!("expected known"),
        }
        assert!(matches!(
            agree_ownership(&a, &b),
            Err(OwnershipError::Conflict)
        ));
        assert_eq!(agree_ownership(&a, &a).unwrap(), a);
    }

    #[test]
    fn atomic_extension_keys_are_ignored() {
        let heap = [0xAAu8; 16];
        let env = crate::cbor_envelope::encode_deterministic_uint_map(&[
            (ENV_HEAP_ID, CborValue::Bytes(heap.to_vec())),
            (ENV_OWNERSHIP_PROFILE, CborValue::Uint(OWNERSHIP_PROFILE_V1)),
            (37, CborValue::Bytes(vec![1; 32])),
            (38, CborValue::Uint(0)),
            (39, CborValue::Bytes(vec![2; 32])),
            (40, CborValue::Uint(3)),
        ])
        .unwrap();
        match parse_ownership_envelope(&env).unwrap() {
            OwnershipEvidence::Known {
                heap_id, profile, ..
            } => {
                assert_eq!(heap_id, heap);
                assert_eq!(profile, OWNERSHIP_PROFILE_V1);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn truly_unknown_key_still_rejected() {
        let heap = [0xAAu8; 16];
        let env = crate::cbor_envelope::encode_deterministic_uint_map(&[
            (ENV_HEAP_ID, CborValue::Bytes(heap.to_vec())),
            (ENV_OWNERSHIP_PROFILE, CborValue::Uint(OWNERSHIP_PROFILE_V1)),
            (99, CborValue::Uint(1)),
        ])
        .unwrap();
        assert!(matches!(
            parse_ownership_envelope(&env),
            Err(OwnershipError::UnknownKey(99))
        ));
    }
}
