//! Typed payload encoding on top of opaque store bodies.

use crate::error::Error;

/// JSON payload type tag.
pub const TAG_JSON: u8 = 0x01;
/// Raw-bytes payload type tag.
pub const TAG_BYTES: u8 = 0x02;

/// Encode a JSON document as a typed store body.
pub fn encode_json(value: &serde_json::Value) -> Result<Vec<u8>, Error> {
    let mut body = Vec::with_capacity(1 + 64);
    body.push(TAG_JSON);
    serde_json::to_writer(&mut body, value)?;
    Ok(body)
}

/// Encode raw bytes as a typed store body.
pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(1 + bytes.len());
    body.push(TAG_BYTES);
    body.extend_from_slice(bytes);
    body
}

/// Decode a typed body as JSON.
pub fn decode_json(body: &[u8]) -> Result<serde_json::Value, Error> {
    match body.split_first() {
        Some((&TAG_JSON, rest)) => Ok(serde_json::from_slice(rest)?),
        Some((&TAG_BYTES, _)) => Err(Error::TypeMismatch {
            expected: "json",
            found: "bytes",
        }),
        Some(_) | None => Err(Error::BadPayload),
    }
}

/// Length of the compact JSON payload inside a typed store body.
///
/// Returning `None` for a non-JSON body lets query hosts fall back to measuring
/// the decoded value rather than trusting incompatible storage metadata.
pub(crate) fn encoded_json_payload_len(body: &[u8]) -> Option<u64> {
    body.first()
        .is_some_and(|tag| *tag == TAG_JSON)
        .then(|| body.len().saturating_sub(1) as u64)
}

/// Decode a typed body as raw bytes.
pub fn decode_bytes(body: &[u8]) -> Result<Vec<u8>, Error> {
    match body.split_first() {
        Some((&TAG_BYTES, rest)) => Ok(rest.to_vec()),
        Some((&TAG_JSON, _)) => Err(Error::TypeMismatch {
            expected: "bytes",
            found: "json",
        }),
        Some(_) | None => Err(Error::BadPayload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_roundtrip() {
        let v = json!({"name": "Alice"});
        let body = encode_json(&v).unwrap();
        assert_eq!(decode_json(&body).unwrap(), v);
        assert_eq!(
            encoded_json_payload_len(&body),
            Some(serde_json::to_vec(&v).unwrap().len() as u64)
        );
        assert_eq!(encoded_json_payload_len(&encode_bytes(b"json?")), None);
    }

    #[test]
    fn bytes_roundtrip() {
        let body = encode_bytes(b"\x00\xff");
        assert_eq!(decode_bytes(&body).unwrap(), b"\x00\xff");
    }

    #[test]
    fn type_mismatch() {
        let body = encode_bytes(b"x");
        assert!(matches!(
            decode_json(&body),
            Err(Error::TypeMismatch {
                expected: "json",
                found: "bytes"
            })
        ));
    }
}
