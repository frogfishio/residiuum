//! Canonical compiled predicate payloads shared by builders and executors.
//!
//! These types are the execution-level result of RQL/RRE compilation. They
//! contain no host closure and have one deterministic representation.

use crate::cbor::{self, Value};
use crate::encoding::ValueEncoding;
use crate::error::AtomicsError;
use crate::outcome::AtomicRefuseReason;

const EXACT_SCALAR_PROFILE_V1: u64 = 1;

/// A compiled exact-scalar equality predicate.
///
/// Equality is over admitted canonical scalar bytes under the collection's
/// frozen value encoding. Absence never equals a scalar value; JSON document
/// predicates are compiled by the RQL/RRE layer, not interpreted here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExactScalarEquality {
    encoding: ValueEncoding,
    expected: Vec<u8>,
}

impl ExactScalarEquality {
    /// Construct after validating the expected scalar's canonical encoding.
    pub fn new(encoding: ValueEncoding, expected: &[u8]) -> Result<Self, AtomicsError> {
        encoding.admit_bytes(expected)?;
        Ok(Self {
            encoding,
            expected: expected.to_vec(),
        })
    }

    /// Frozen scalar encoding.
    pub const fn encoding(&self) -> ValueEncoding {
        self.encoding
    }

    /// Canonical expected scalar bytes.
    pub fn expected(&self) -> &[u8] {
        &self.expected
    }

    /// Total evaluation. An absent object does not equal a present scalar.
    pub fn evaluate(&self, observed: Option<&[u8]>) -> bool {
        observed == Some(self.expected.as_slice())
    }
}

/// Deterministically encode an exact-scalar compiled payload.
pub fn encode_exact_scalar_payload(
    predicate: &ExactScalarEquality,
) -> Result<Vec<u8>, AtomicsError> {
    cbor::encode_map(&[
        (1, Value::Uint(EXACT_SCALAR_PROFILE_V1)),
        (2, Value::Uint(u64::from(predicate.encoding.wire_code()))),
        (3, Value::Bytes(predicate.expected.clone())),
    ])
}

/// Decode and fully validate an exact-scalar compiled payload.
pub fn decode_exact_scalar_payload(bytes: &[u8]) -> Result<ExactScalarEquality, AtomicsError> {
    let map = cbor::decode_map(bytes).map_err(|_| malformed())?;
    if map.len() != 3 {
        return Err(malformed());
    }
    let profile = require_uint(&map, 1)?;
    if profile != EXACT_SCALAR_PROFILE_V1 {
        return Err(malformed());
    }
    let encoding = u8::try_from(require_uint(&map, 2)?)
        .ok()
        .and_then(ValueEncoding::from_wire_code)
        .ok_or_else(malformed)?;
    let expected = require_bytes(&map, 3)?;
    ExactScalarEquality::new(encoding, &expected).map_err(|_| malformed())
}

fn require_uint(map: &[(u64, Value)], field: u64) -> Result<u64, AtomicsError> {
    match map
        .iter()
        .find(|(key, _)| *key == field)
        .map(|(_, value)| value)
    {
        Some(Value::Uint(value)) => Ok(*value),
        _ => Err(malformed()),
    }
}

fn require_bytes(map: &[(u64, Value)], field: u64) -> Result<Vec<u8>, AtomicsError> {
    match map
        .iter()
        .find(|(key, _)| *key == field)
        .map(|(_, value)| value)
    {
        Some(Value::Bytes(value)) => Ok(value.clone()),
        _ => Err(malformed()),
    }
}

fn malformed() -> AtomicsError {
    AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_scalar_payload_round_trips_and_is_total() {
        let predicate = ExactScalarEquality::new(ValueEncoding::Integer, &[1]).unwrap();
        let bytes = encode_exact_scalar_payload(&predicate).unwrap();
        assert_eq!(decode_exact_scalar_payload(&bytes).unwrap(), predicate);
        assert!(predicate.evaluate(Some(&[1])));
        assert!(!predicate.evaluate(Some(&[2])));
        assert!(!predicate.evaluate(None));
    }

    #[test]
    fn malformed_or_noncanonical_payload_is_refused() {
        let bad_integer = cbor::encode_map(&[
            (1, Value::Uint(1)),
            (
                2,
                Value::Uint(u64::from(ValueEncoding::Integer.wire_code())),
            ),
            (3, Value::Bytes(vec![0, 1])),
        ])
        .unwrap();
        assert_eq!(
            decode_exact_scalar_payload(&bad_integer).unwrap_err(),
            malformed()
        );
        assert_eq!(decode_exact_scalar_payload(&[0]).unwrap_err(), malformed());
    }
}
