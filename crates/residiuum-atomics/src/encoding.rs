//! Frozen collection key/value encoding contracts (CR-ATM1-002).
//!
//! Integer and decimal payloads use two's-complement big-endian bytes at
//! minimal width. Zero is the single byte `0x00`. Leading `0x00`/`0xff` that
//! do not change the sign bit are aliases and are refused.

use crate::cbor::{self, Value};
use crate::error::AtomicsError;
use crate::outcome::AtomicRefuseReason;
use crate::plan::{CanonicalKey, CanonicalKeyKind};

/// How a collection encodes admitted values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ValueEncoding {
    /// Opaque byte string. Empty is valid.
    Bytes,
    /// UTF-8 string bytes.
    Utf8,
    /// Canonical signed integer bytes.
    Integer,
    /// Canonical exact decimal (coefficient + scale), encoded as the same
    /// three-field map used for decimal keys.
    Decimal,
}

impl ValueEncoding {
    /// Wire-adjacent name for tests and later SDK mapping.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bytes => "bytes",
            Self::Utf8 => "utf8",
            Self::Integer => "integer",
            Self::Decimal => "decimal",
        }
    }
}

/// Frozen key/value encoding profile bound to a trusted collection handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EncodingProfile {
    key: CanonicalKeyKind,
    value: ValueEncoding,
}

impl EncodingProfile {
    /// String keys and opaque byte values. Default for existing test grants.
    pub const STRING_BYTES: Self = Self {
        key: CanonicalKeyKind::String,
        value: ValueEncoding::Bytes,
    };

    /// Opaque keys and opaque byte values.
    pub const OPAQUE_BYTES: Self = Self {
        key: CanonicalKeyKind::Opaque,
        value: ValueEncoding::Bytes,
    };

    /// Integer keys and integer values.
    pub const INTEGER: Self = Self {
        key: CanonicalKeyKind::Integer,
        value: ValueEncoding::Integer,
    };

    /// Decimal keys and decimal values.
    pub const DECIMAL: Self = Self {
        key: CanonicalKeyKind::Decimal,
        value: ValueEncoding::Decimal,
    };

    /// Construct a profile from independent key and value encodings.
    pub const fn new(key: CanonicalKeyKind, value: ValueEncoding) -> Self {
        Self { key, value }
    }

    /// Declared key kind.
    pub const fn key_kind(self) -> CanonicalKeyKind {
        self.key
    }

    /// Declared value encoding.
    pub const fn value_encoding(self) -> ValueEncoding {
        self.value
    }

    /// Refuse a key that is the wrong kind or a noncanonical integer/decimal.
    pub fn admit_key(self, key: &CanonicalKey) -> Result<(), AtomicsError> {
        if key.kind() != self.key {
            return invalid();
        }
        validate_canonical_key(key)
    }

    /// Refuse a value payload that fails this profile.
    pub fn admit_value_bytes(self, bytes: &[u8]) -> Result<(), AtomicsError> {
        match self.value {
            ValueEncoding::Bytes => Ok(()),
            ValueEncoding::Utf8 => std::str::from_utf8(bytes)
                .map(|_| ())
                .map_err(|_| invalid_err()),
            ValueEncoding::Integer => verify_signed_integer(bytes),
            ValueEncoding::Decimal => verify_decimal_value(bytes).map(|_| ()),
        }
    }
}

/// Validate integer/decimal key payloads. String and opaque carry no extra rule.
pub fn validate_canonical_key(key: &CanonicalKey) -> Result<(), AtomicsError> {
    match key {
        CanonicalKey::String(_) | CanonicalKey::Opaque(_) => Ok(()),
        CanonicalKey::Integer(bytes) => verify_signed_integer(bytes),
        CanonicalKey::Decimal { coefficient, .. } => verify_signed_integer(coefficient),
    }
}

/// Two's-complement big-endian minimal-width encoding of `n`.
pub fn encode_signed_integer(n: i128) -> Vec<u8> {
    if n == 0 {
        return vec![0];
    }
    let bytes = n.to_be_bytes();
    let mut start = 0;
    while start + 1 < bytes.len() {
        let b0 = bytes[start];
        let b1 = bytes[start + 1];
        if (b0 == 0 && b1 < 0x80) || (b0 == 0xff && b1 >= 0x80) {
            start += 1;
        } else {
            break;
        }
    }
    bytes[start..].to_vec()
}

/// Refuse empty or non-minimal signed integer encodings.
pub fn verify_signed_integer(bytes: &[u8]) -> Result<(), AtomicsError> {
    if bytes.is_empty() {
        return invalid();
    }
    if bytes.len() > 1 {
        let b0 = bytes[0];
        let b1 = bytes[1];
        if (b0 == 0 && b1 < 0x80) || (b0 == 0xff && b1 >= 0x80) {
            return invalid();
        }
    }
    Ok(())
}

/// Canonical decimal value bytes: kind 4, coefficient, scale.
pub fn encode_decimal_value(coefficient: i128, scale: i64) -> Result<Vec<u8>, AtomicsError> {
    let coeff = encode_signed_integer(coefficient);
    cbor::encode_map(&[
        (
            1,
            Value::Uint(u64::from(CanonicalKeyKind::Decimal.wire_code())),
        ),
        (2, Value::Bytes(coeff)),
        (3, encode_int(scale)),
    ])
}

/// Decode and verify a decimal value payload.
pub fn verify_decimal_value(bytes: &[u8]) -> Result<(Vec<u8>, i64), AtomicsError> {
    let map = cbor::decode_map(bytes).map_err(|_| invalid_err())?;
    if map.len() != 3 {
        return invalid();
    }
    let kind = match field(&map, 1) {
        Some(Value::Uint(n)) => *n,
        _ => return invalid(),
    };
    if kind != u64::from(CanonicalKeyKind::Decimal.wire_code()) {
        return invalid();
    }
    let coefficient = match field(&map, 2) {
        Some(Value::Bytes(b)) => b.clone(),
        _ => return invalid(),
    };
    let scale = match field(&map, 3) {
        Some(v) => decode_int(v)?,
        None => return invalid(),
    };
    verify_signed_integer(&coefficient)?;
    Ok((coefficient, scale))
}

fn field(map: &[(u64, Value)], key: u64) -> Option<&Value> {
    map.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
}

fn encode_int(n: i64) -> Value {
    if n >= 0 {
        Value::Uint(n as u64)
    } else {
        Value::Nint((!n) as u64)
    }
}

fn decode_int(v: &Value) -> Result<i64, AtomicsError> {
    match v {
        Value::Uint(n) => i64::try_from(*n).map_err(|_| invalid_err()),
        Value::Nint(n) => {
            let mag = i64::try_from(*n).map_err(|_| invalid_err())?;
            mag.checked_neg()
                .and_then(|v| v.checked_sub(1))
                .ok_or_else(invalid_err)
        }
        _ => invalid(),
    }
}

fn invalid<T>() -> Result<T, AtomicsError> {
    Err(invalid_err())
}

fn invalid_err() -> AtomicsError {
    AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_integer_minimal_width() {
        assert_eq!(encode_signed_integer(0), vec![0x00]);
        assert_eq!(encode_signed_integer(1), vec![0x01]);
        assert_eq!(encode_signed_integer(127), vec![0x7f]);
        assert_eq!(encode_signed_integer(128), vec![0x00, 0x80]);
        assert_eq!(encode_signed_integer(-1), vec![0xff]);
        assert_eq!(encode_signed_integer(-128), vec![0x80]);
        assert_eq!(encode_signed_integer(-129), vec![0xff, 0x7f]);
    }

    #[test]
    fn signed_integer_rejects_aliases() {
        assert!(verify_signed_integer(&[]).is_err());
        assert!(verify_signed_integer(&[0x00, 0x01]).is_err());
        assert!(verify_signed_integer(&[0xff, 0xff]).is_err());
        assert!(verify_signed_integer(&[0x00, 0x00]).is_err());
        assert!(verify_signed_integer(&[0x00]).is_ok());
        assert!(verify_signed_integer(&[0x01]).is_ok());
        assert!(verify_signed_integer(&[0x00, 0x80]).is_ok());
        assert!(verify_signed_integer(&[0xff]).is_ok());
    }

    #[test]
    fn encode_then_verify_round_trip() {
        for n in [0i128, 1, -1, 127, 128, -128, -129, i128::MAX, i128::MIN] {
            let bytes = encode_signed_integer(n);
            verify_signed_integer(&bytes).unwrap();
        }
    }

    #[test]
    fn decimal_value_is_unique() {
        let a = encode_decimal_value(10, 1).unwrap();
        let b = encode_decimal_value(10, 1).unwrap();
        assert_eq!(a, b);
        let (coeff, scale) = verify_decimal_value(&a).unwrap();
        assert_eq!(coeff, encode_signed_integer(10));
        assert_eq!(scale, 1);
    }

    #[test]
    fn profile_rejects_wrong_key_kind() {
        let err = EncodingProfile::STRING_BYTES
            .admit_key(&CanonicalKey::integer(1))
            .unwrap_err();
        assert_eq!(err, AtomicsError::Refused(AtomicRefuseReason::InvalidValue));
    }
}
