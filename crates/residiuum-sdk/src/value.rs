//! Typed payload encoding on top of opaque store bodies.

use crate::error::Error;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt;

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

#[derive(Default)]
struct ProjectionNode {
    output: Option<usize>,
    children: BTreeMap<String, ProjectionNode>,
}

/// Presence-aware variant used by predicate projection. `None` means the path
/// was absent; `Some(Value::Null)` means it was explicitly present as null.
pub(crate) fn project_json_fields_present(
    body: &[u8],
    fields: &[String],
) -> Result<Option<Vec<Option<serde_json::Value>>>, Error> {
    let payload = match body.split_first() {
        Some((&TAG_JSON, rest)) => rest,
        Some((&TAG_BYTES, _)) => {
            return Err(Error::TypeMismatch {
                expected: "json",
                found: "bytes",
            });
        }
        Some(_) | None => return Err(Error::BadPayload),
    };
    let mut root = ProjectionNode::default();
    for (output, field) in fields.iter().enumerate() {
        let mut node = &mut root;
        for segment in field.split('.') {
            node = node.children.entry(segment.to_string()).or_default();
            // A parent and one of its descendants cannot both be decoded
            // partially: retaining the parent necessarily retains its subtree.
            if node.output.is_some() {
                return Ok(None);
            }
        }
        if !node.children.is_empty() || node.output.replace(output).is_some() {
            return Ok(None);
        }
    }
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    let found = ProjectionSeed { node: &root }.deserialize(&mut deserializer)?;
    deserializer.end()?;
    let mut values = vec![None; fields.len()];
    for (slot, value) in found {
        values[slot] = Some(value);
    }
    Ok(Some(values))
}

struct ProjectionSeed<'a> {
    node: &'a ProjectionNode,
}

impl<'de> DeserializeSeed<'de> for ProjectionSeed<'_> {
    type Value = Vec<(usize, serde_json::Value)>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if let Some(output) = self.node.output {
            return serde_json::Value::deserialize(deserializer).map(|value| vec![(output, value)]);
        }
        deserializer.deserialize_any(ProjectionVisitor { node: self.node })
    }
}

struct ProjectionVisitor<'a> {
    node: &'a ProjectionNode,
}

impl<'de> Visitor<'de> for ProjectionVisitor<'_> {
    type Value = Vec<(usize, serde_json::Value)>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut found = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            if let Some(child) = self.node.children.get(&key) {
                found.extend(map.next_value_seed(ProjectionSeed { node: child })?);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(found)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(Vec::new())
    }

    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
        Ok(Vec::new())
    }

    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
        Ok(Vec::new())
    }

    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
        Ok(Vec::new())
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
        Ok(Vec::new())
    }

    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E> {
        Ok(Vec::new())
    }

    fn visit_string<E>(self, _: String) -> Result<Self::Value, E> {
        Ok(Vec::new())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Vec::new())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Vec::new())
    }
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

    #[test]
    fn projected_json_reads_nested_fields_and_validates_the_rest() {
        let body = encode_json(&json!({
            "region": "r0",
            "nested": {"score": 7, "ignored": [1, 2, 3]},
            "large": {"unused": "x".repeat(1_000)}
        }))
        .unwrap();
        let fields = vec![
            "region".to_string(),
            "nested.score".to_string(),
            "missing".to_string(),
        ];
        assert_eq!(
            project_json_fields_present(&body, &fields)
                .unwrap()
                .unwrap(),
            vec![Some(json!("r0")), Some(json!(7)), None]
        );

        let mut damaged = body;
        damaged.extend_from_slice(b" trailing");
        assert!(project_json_fields_present(&damaged, &fields).is_err());
    }

    #[test]
    fn projected_json_refuses_overlapping_paths() {
        let body = encode_json(&json!({"nested": {"score": 7}})).unwrap();
        let fields = vec!["nested".to_string(), "nested.score".to_string()];
        assert!(project_json_fields_present(&body, &fields)
            .unwrap()
            .is_none());
    }
}
