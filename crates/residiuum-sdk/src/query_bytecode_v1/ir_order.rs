//! Core order / sort-tuple IR phase (RQL-IR2).
//!
//! Profile: **`residiuum-query-ir-order-v1`**
//! Normative: [QUERY_IR_ORDER_V1.md](../../../../../doc/todo/rql/QUERY_IR_ORDER_V1.md)
//!
//! Application Core `order by` compare + sort-tuple resume live here — not as
//! private helpers inside the page loop. Still a **Rust IR residual** (not an
//! opcode machine). Decision 0 remains OPEN; RQL-C1 must not be accepted.

use crate::plan_v1::{NullsOrder, OrderDir, OrderTerm};
use crate::predicate::{resolve_path, Resolve};
use serde_json::Value as JsonValue;
use std::cmp::Ordering;

/// IR profile id for Core order / sort-tuple.
pub const ORDER_IR_PROFILE: &str = "residiuum-query-ir-order-v1";

/// Compare two keyed documents under `order`.
pub fn compare_rows(
    ka: &str,
    va: &JsonValue,
    kb: &str,
    vb: &JsonValue,
    order: &[OrderTerm],
) -> Ordering {
    for term in order {
        if term.tie_break || term.path.0 == ["$key"] {
            let c = ka.cmp(kb);
            return apply_dir(c, term.dir);
        }
        let ra = resolve_path(va, &term.path);
        let rb = resolve_path(vb, &term.path);
        let c = compare_resolve(&ra, &rb, term.nulls);
        if c != Ordering::Equal {
            return apply_dir(c, term.dir);
        }
    }
    ka.cmp(kb)
}

/// Sort-tuple for a full document (pre-projection), aligned with [`compare_rows`].
pub(crate) fn build_sort_tuple(key: &str, doc: &JsonValue, order: &[OrderTerm]) -> JsonValue {
    let mut parts = Vec::with_capacity(order.len());
    for term in order {
        if term.tie_break || term.path.0.as_slice() == ["$key"] {
            parts.push(JsonValue::String(key.to_string()));
        } else {
            match resolve_path(doc, &term.path) {
                Resolve::Present(v) => parts.push(v),
                // Distinct from JSON null so nulls placement matches compare_rows.
                Resolve::Absent => parts.push(serde_json::json!({"__rv":"absent"})),
            }
        }
    }
    JsonValue::Array(parts)
}

/// Compare two sort-tuples under `order`.
pub(crate) fn cmp_sort_tuples(a: &JsonValue, b: &JsonValue, order: &[OrderTerm]) -> Ordering {
    let aa = a.as_array().map(|x| x.as_slice()).unwrap_or(&[]);
    let bb = b.as_array().map(|x| x.as_slice()).unwrap_or(&[]);
    for (i, term) in order.iter().enumerate() {
        let av = aa.get(i).unwrap_or(&JsonValue::Null);
        let bv = bb.get(i).unwrap_or(&JsonValue::Null);
        let c = if term.tie_break || term.path.0.as_slice() == ["$key"] {
            let as_ = av.as_str().unwrap_or("");
            let bs_ = bv.as_str().unwrap_or("");
            as_.cmp(bs_)
        } else {
            compare_resolve(
                &resolve_from_tuple_part(av),
                &resolve_from_tuple_part(bv),
                term.nulls,
            )
        };
        if c != Ordering::Equal {
            return apply_dir(c, term.dir);
        }
    }
    Ordering::Equal
}

/// Drop rows whose sort-tuple is `<= last` (multipage field-order resume).
pub fn retain_after_sort_tuple(
    full: &mut Vec<(String, JsonValue)>,
    order: &[OrderTerm],
    last: &JsonValue,
) {
    full.retain(|(k, doc)| {
        let t = build_sort_tuple(k, doc, order);
        let c = cmp_sort_tuples(&t, last, order);
        c == Ordering::Greater
    });
}

/// Key stream resume: last element of sort tuple is the document key when
/// order is key-only (or includes a trailing key tie-break).
pub(crate) fn key_from_sort_tuple(t: &JsonValue) -> Option<String> {
    let arr = t.as_array()?;
    arr.iter()
        .rev()
        .find_map(|v| v.as_str().map(|s| s.to_string()))
        .or_else(|| arr.first().and_then(|v| v.as_str().map(|s| s.to_string())))
}

fn apply_dir(c: Ordering, dir: OrderDir) -> Ordering {
    match dir {
        OrderDir::Asc => c,
        OrderDir::Desc => c.reverse(),
    }
}

fn compare_resolve(a: &Resolve, b: &Resolve, nulls: NullsOrder) -> Ordering {
    match (a, b) {
        (Resolve::Absent, Resolve::Absent) => Ordering::Equal,
        (Resolve::Absent, Resolve::Present(_)) => match nulls {
            NullsOrder::Last => Ordering::Greater,
            NullsOrder::First => Ordering::Less,
        },
        (Resolve::Present(_), Resolve::Absent) => match nulls {
            NullsOrder::Last => Ordering::Less,
            NullsOrder::First => Ordering::Greater,
        },
        (Resolve::Present(x), Resolve::Present(y)) => json_ord(x, y),
    }
}

fn resolve_from_tuple_part(v: &JsonValue) -> Resolve {
    if v.get("__rv").and_then(|x| x.as_str()) == Some("absent") {
        Resolve::Absent
    } else {
        Resolve::Present(v.clone())
    }
}

fn json_ord(a: &JsonValue, b: &JsonValue) -> Ordering {
    match (a, b) {
        (JsonValue::Null, JsonValue::Null) => Ordering::Equal,
        (JsonValue::Null, _) => Ordering::Less,
        (_, JsonValue::Null) => Ordering::Greater,
        (JsonValue::Bool(x), JsonValue::Bool(y)) => x.cmp(y),
        (JsonValue::Number(x), JsonValue::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(xf), Some(yf)) => xf.partial_cmp(&yf).unwrap_or(Ordering::Equal),
            _ => x.to_string().cmp(&y.to_string()),
        },
        (JsonValue::String(x), JsonValue::String(y)) => x.cmp(y),
        _ => a.to_string().cmp(&b.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_v1::{NullsOrder, OrderDir, OrderTerm};
    use crate::predicate::Path;
    use serde_json::json;
    use std::cmp::Ordering;

    fn key_term() -> OrderTerm {
        OrderTerm {
            path: Path::parse_dotted("$key").unwrap(),
            dir: OrderDir::Asc,
            nulls: NullsOrder::Last,
            tie_break: true,
        }
    }

    #[test]
    fn order_ir_profile_constant() {
        assert_eq!(ORDER_IR_PROFILE, "residiuum-query-ir-order-v1");
    }

    #[test]
    fn compare_rows_key_only() {
        let order = vec![key_term()];
        assert_eq!(
            compare_rows("a", &json!({}), "b", &json!({}), &order),
            Ordering::Less
        );
    }

    #[test]
    fn after_c20_keeps_b30_and_d40() {
        let order = vec![
            OrderTerm {
                path: Path::parse_dotted("n").unwrap(),
                dir: OrderDir::Asc,
                nulls: NullsOrder::Last,
                tie_break: false,
            },
            key_term(),
        ];
        let mut full: Vec<(String, serde_json::Value)> = vec![
            ("a".to_string(), json!({"n": 10})),
            ("b".to_string(), json!({"n": 30})),
            ("c".to_string(), json!({"n": 20})),
            ("d".to_string(), json!({"n": 40})),
        ];
        full.sort_by(|(ka, va), (kb, vb)| compare_rows(ka, va, kb, vb, &order));
        let last = build_sort_tuple("c", &json!({"n": 20}), &order);
        retain_after_sort_tuple(&mut full, &order, &last);
        let keys: Vec<_> = full.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["b", "d"]);
    }

    #[test]
    fn key_from_sort_tuple_trailing() {
        let order = vec![key_term()];
        let t = build_sort_tuple("k1", &json!({}), &order);
        assert_eq!(key_from_sort_tuple(&t).as_deref(), Some("k1"));
    }
}
