//! Item-event envelope layout (FORMAT_SPEC §4.4 deterministic CBOR).
//!
//! Wire major 1 envelopes are a single definite-length CBOR map with unsigned
//! integer keys. Core keys used here:
//!
//! | Key | Name | Type |
//! |---:|---|---|
//! | 1 | `item_id` | bstr(16) |
//! | 2 | `event_kind` | uint |
//! | 3 | `store_id` | bstr(16) |
//! | 4 | `segment_id` | bstr(16) |
//! | 5 | `created_ns` | uint |
//! | 6 | `subject_id` | bstr |
//! | 31 | `operation_id` | bstr(16), optional |
//! | 32 | `operation_content_hash` | bstr(32), optional |

use residiuum_format::{decode_deterministic_uint_map, encode_deterministic_uint_map, CborValue};

/// Maximum subject length in this draft (also bounds envelopes).
pub const MAX_SUBJECT_LEN: usize = 4096;

/// Core event kinds for Stage 3 (OVERVIEW §5.4 subset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EventKind {
    /// Associate a payload with a logical subject.
    Put = 1,
    /// Record a logical deletion.
    Delete = 2,
}

impl EventKind {
    /// Parse a raw kind byte.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Put),
            2 => Some(Self::Delete),
            _ => None,
        }
    }

    /// Wire byte.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Stable name for diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Delete => "delete",
        }
    }
}

/// Decoded item envelope fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemEnvelope {
    /// Store identifier.
    pub store_id: [u8; 16],
    /// Segment that holds this frame (at write time).
    pub segment_id: [u8; 16],
    /// Stable item identifier for this subject lineage.
    pub item_id: [u8; 16],
    /// Event kind.
    pub event_kind: EventKind,
    /// Writer-supplied creation timestamp (nanoseconds); 0 if unknown.
    pub created_ns: u64,
    /// Logical subject key bytes (UTF-8 for Stage 4 string APIs; SubjectV2 binary on heap path).
    pub subject: Vec<u8>,
    /// Stable client mutation identity when this event is an idempotent command.
    pub operation_id: Option<[u8; 16]>,
    /// Canonical logical request hash bound to `operation_id`.
    pub operation_content_hash: Option<[u8; 32]>,
}

fn bstr16(bytes: &[u8]) -> Option<[u8; 16]> {
    if bytes.len() != 16 {
        return None;
    }
    bytes.try_into().ok()
}

/// Encode an item envelope as deterministic CBOR.
pub fn encode_item_envelope(env: &ItemEnvelope) -> Result<Vec<u8>, &'static str> {
    if env.subject.len() > MAX_SUBJECT_LEN {
        return Err("subject too long");
    }
    if env.operation_id.is_some() != env.operation_content_hash.is_some() {
        return Err("operation identity must include id and content hash");
    }
    let mut entries = vec![
        (1u64, CborValue::Bytes(env.item_id.to_vec())),
        (2u64, CborValue::Uint(u64::from(env.event_kind.as_u8()))),
        (3u64, CborValue::Bytes(env.store_id.to_vec())),
        (4u64, CborValue::Bytes(env.segment_id.to_vec())),
        (5u64, CborValue::Uint(env.created_ns)),
        (6u64, CborValue::Bytes(env.subject.clone())),
    ];
    if let (Some(operation_id), Some(content_hash)) = (env.operation_id, env.operation_content_hash)
    {
        entries.push((31u64, CborValue::Bytes(operation_id.to_vec())));
        entries.push((32u64, CborValue::Bytes(content_hash.to_vec())));
    }
    encode_deterministic_uint_map(&entries).map_err(|_| "cbor encode failed")
}

/// Decode an item envelope from deterministic CBOR. Returns `None` if the map
/// is missing required keys or has wrong value types.
pub fn decode_item_envelope(bytes: &[u8]) -> Option<ItemEnvelope> {
    let map = decode_deterministic_uint_map(bytes).ok()?;
    let mut item_id = None;
    let mut event_kind = None;
    let mut store_id = None;
    let mut segment_id = None;
    let mut created_ns = None;
    let mut subject = None;
    let mut operation_id = None;
    let mut operation_content_hash = None;
    for (k, v) in map {
        match k {
            1 => {
                let CborValue::Bytes(b) = v else {
                    return None;
                };
                item_id = Some(bstr16(&b)?);
            }
            2 => {
                let CborValue::Uint(n) = v else {
                    return None;
                };
                if n > u64::from(u8::MAX) {
                    return None;
                }
                event_kind = EventKind::from_u8(n as u8);
            }
            3 => {
                let CborValue::Bytes(b) = v else {
                    return None;
                };
                store_id = Some(bstr16(&b)?);
            }
            4 => {
                let CborValue::Bytes(b) = v else {
                    return None;
                };
                segment_id = Some(bstr16(&b)?);
            }
            5 => {
                let CborValue::Uint(n) = v else {
                    return None;
                };
                created_ns = Some(n);
            }
            6 => {
                let CborValue::Bytes(b) = v else {
                    return None;
                };
                if b.len() > MAX_SUBJECT_LEN {
                    return None;
                }
                subject = Some(b);
            }
            31 => {
                let CborValue::Bytes(b) = v else {
                    return None;
                };
                operation_id = Some(bstr16(&b)?);
            }
            32 => {
                let CborValue::Bytes(b) = v else {
                    return None;
                };
                operation_content_hash = Some(b.as_slice().try_into().ok()?);
            }
            // Unknown keys retained by lossless tools; readers may ignore.
            _ => {}
        }
    }
    if operation_id.is_some() != operation_content_hash.is_some() {
        return None;
    }
    Some(ItemEnvelope {
        store_id: store_id?,
        segment_id: segment_id?,
        item_id: item_id?,
        event_kind: event_kind?,
        created_ns: created_ns.unwrap_or(0),
        subject: subject.unwrap_or_default(),
        operation_id,
        operation_content_hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use residiuum_format::validate_deterministic_cbor_envelope;

    #[test]
    fn roundtrip() {
        let env = ItemEnvelope {
            store_id: [1u8; 16],
            segment_id: [2u8; 16],
            item_id: [3u8; 16],
            event_kind: EventKind::Put,
            created_ns: 99,
            subject: b"user-42".to_vec(),
            operation_id: Some([4; 16]),
            operation_content_hash: Some([5; 32]),
        };
        let bytes = encode_item_envelope(&env).unwrap();
        validate_deterministic_cbor_envelope(&bytes).unwrap();
        let decoded = decode_item_envelope(&bytes).unwrap();
        assert_eq!(decoded, env);
    }

    #[test]
    fn delete_kind_roundtrip() {
        let env = ItemEnvelope {
            store_id: [0u8; 16],
            segment_id: [0u8; 16],
            item_id: [9u8; 16],
            event_kind: EventKind::Delete,
            created_ns: 0,
            subject: b"k".to_vec(),
            operation_id: None,
            operation_content_hash: None,
        };
        let decoded = decode_item_envelope(&encode_item_envelope(&env).unwrap()).unwrap();
        assert_eq!(decoded.event_kind, EventKind::Delete);
    }

    #[test]
    fn rejects_non_cbor() {
        assert!(decode_item_envelope(b"RENV0001notcbor").is_none());
    }

    #[test]
    fn rejects_incomplete_map() {
        // map{1: bstr(16 zeros)} only — missing required keys.
        let mut bytes = vec![0xa1, 0x01, 0x50];
        bytes.extend_from_slice(&[0u8; 16]);
        assert!(decode_item_envelope(&bytes).is_none());
    }
}
