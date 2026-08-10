//! Result canonicalisation — digests for Q0 equivalence dimensions.
//!
//! Compare keys, values, multiplicity, order (when declared), coverage.
//! Row count alone is never authority.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Canonical multi-dimension result summary for cross-engine compare.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalResult {
    /// SHA-256 hex of sorted key multiset encoding.
    pub keys_digest: String,
    /// SHA-256 hex of sorted (key, canonical_value) multiset encoding.
    pub values_digest: String,
    /// `multiset` | `set` | `ordered`.
    pub multiplicity: String,
    pub order_sensitive: bool,
    pub coverage_complete: bool,
    pub row_count: u64,
    /// Optional small preview for debug (not used in equality).
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub raw_preview: Value,
}

/// One result row: immutable key + JSON body.
#[derive(Debug, Clone)]
pub struct ResultRow {
    pub key: String,
    pub value: Value,
}

/// Normalize JSON for digest stability (sort object keys; strip `$key`).
pub fn normalize_value(v: &Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut out = Map::new();
            for (k, val) in m {
                if k == "$key" {
                    continue;
                }
                out.insert(k.clone(), normalize_value(val));
            }
            Value::Object(out)
        }
        Value::Array(a) => Value::Array(a.iter().map(normalize_value).collect()),
        other => other.clone(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// Build digests from rows.
///
/// When `order_sensitive`, digest includes sequential order; otherwise multiset.
pub fn canonicalize_rows(
    rows: &[ResultRow],
    order_sensitive: bool,
    coverage_complete: bool,
) -> CanonicalResult {
    let multiplicity = if order_sensitive {
        "ordered"
    } else {
        "multiset"
    };

    let mut keys: Vec<&str> = rows.iter().map(|r| r.key.as_str()).collect();
    if !order_sensitive {
        keys.sort_unstable();
    }
    let keys_blob = keys.join("\n");
    let keys_digest = sha256_hex(keys_blob.as_bytes());

    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| {
            let v = serde_json::to_string(&normalize_value(&r.value)).unwrap_or_default();
            (r.key.clone(), v)
        })
        .collect();
    if !order_sensitive {
        pairs.sort();
    }
    let mut values_blob = String::new();
    for (k, v) in &pairs {
        values_blob.push_str(k);
        values_blob.push('\t');
        values_blob.push_str(v);
        values_blob.push('\n');
    }
    let values_digest = sha256_hex(values_blob.as_bytes());

    CanonicalResult {
        keys_digest,
        values_digest,
        multiplicity: multiplicity.into(),
        order_sensitive,
        coverage_complete,
        row_count: rows.len() as u64,
        raw_preview: Value::Null,
    }
}

/// Full shape equality for comparative cells (not row-count alone).
pub fn results_equivalent(a: &CanonicalResult, b: &CanonicalResult) -> Result<(), String> {
    if a.coverage_complete != b.coverage_complete {
        return Err(format!(
            "coverage diverge a={} b={}",
            a.coverage_complete, b.coverage_complete
        ));
    }
    if a.order_sensitive != b.order_sensitive {
        return Err("order_sensitive flag diverge".into());
    }
    if a.keys_digest != b.keys_digest {
        return Err(format!(
            "keys_digest diverge a={} b={}",
            a.keys_digest, b.keys_digest
        ));
    }
    if a.values_digest != b.values_digest {
        return Err(format!(
            "values_digest diverge a={} b={}",
            a.values_digest, b.values_digest
        ));
    }
    if a.row_count != b.row_count {
        return Err(format!(
            "row_count diverge a={} b={} (should not happen if digests match)",
            a.row_count, b.row_count
        ));
    }
    Ok(())
}

/// Multiset counts helper (tests / debugging).
pub fn multiset_counts(rows: &[ResultRow]) -> BTreeMap<(String, String), u32> {
    let mut m = BTreeMap::new();
    for r in rows {
        let v = serde_json::to_string(&normalize_value(&r.value)).unwrap_or_default();
        *m.entry((r.key.clone(), v)).or_default() += 1;
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn multiset_order_insensitive() {
        let a = vec![
            ResultRow {
                key: "b".into(),
                value: json!({"n": 2}),
            },
            ResultRow {
                key: "a".into(),
                value: json!({"n": 1}),
            },
        ];
        let b = vec![
            ResultRow {
                key: "a".into(),
                value: json!({"n": 1}),
            },
            ResultRow {
                key: "b".into(),
                value: json!({"n": 2}),
            },
        ];
        let ca = canonicalize_rows(&a, false, true);
        let cb = canonicalize_rows(&b, false, true);
        results_equivalent(&ca, &cb).unwrap();
    }

    #[test]
    fn ordered_detects_permutation() {
        let a = vec![
            ResultRow {
                key: "a".into(),
                value: json!(1),
            },
            ResultRow {
                key: "b".into(),
                value: json!(2),
            },
        ];
        let b = vec![
            ResultRow {
                key: "b".into(),
                value: json!(2),
            },
            ResultRow {
                key: "a".into(),
                value: json!(1),
            },
        ];
        let ca = canonicalize_rows(&a, true, true);
        let cb = canonicalize_rows(&b, true, true);
        assert!(results_equivalent(&ca, &cb).is_err());
    }

    #[test]
    fn coverage_mismatch_is_defect() {
        let rows = vec![ResultRow {
            key: "a".into(),
            value: json!({}),
        }];
        let ca = canonicalize_rows(&rows, false, true);
        let mut cb = ca.clone();
        cb.coverage_complete = false;
        assert!(results_equivalent(&ca, &cb).is_err());
    }
}
