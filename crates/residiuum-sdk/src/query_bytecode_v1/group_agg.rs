//! Group-by + aggregate IR phase (RQL-Q2 `pkg_group_aggregate`).
//!
//! Profile stamp: **`residiuum-query-ir-group-agg-v1`**
//! Normative: [RQL_SPEC.md](../../../../../doc/wip/query/RQL_SPEC.md) §9a
//!
//! Lowers through Core `ProjectPaths` immediate (`ProjectImm` with group payload).
//! Phase body is a Rust IR residual (Decision 0 still OPEN) — same honesty class
//! as order/page/project. **Not** RQL-C1 / pure micro-VM.

use crate::error::Error;
use crate::plan_v1::{AggFn, GroupAggSpec};
use crate::predicate::{resolve_path, Path, Resolve};
use serde_json::{Map, Number, Value as JsonValue};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// IR profile id for group/aggregate.
pub const GROUP_AGG_IR_PROFILE: &str = "residiuum-query-ir-group-agg-v1";

/// Apply group-by + aggregates to the working bag.
///
/// - Empty [`GroupAggSpec`] is a no-op (returns `working` unchanged).
/// - Empty `group_by` with aggregates ⇒ one global group.
/// - `count()` counts input rows in the group (including null/absent fields).
/// - `sum` / `min` / `max` / `avg` ignore null, absent, and non-numeric present
///   sources (heterogeneous document bags — same skip class as null).
/// - Output row keys are deterministic: `g:` + canonical group-key encoding.
pub(crate) fn apply_group_agg(
    working: Vec<(String, JsonValue)>,
    spec: &GroupAggSpec,
) -> Result<Vec<(String, JsonValue)>, Error> {
    if !spec.is_active() {
        return Ok(working);
    }

    let mut accumulator = GroupAccumulator::new(spec);
    for (_doc_key, doc) in working {
        accumulator.ingest(&doc)?;
    }
    accumulator.finish()
}

#[derive(Debug, Clone)]
enum AggregateState {
    Count(u64),
    Sum { sum: f64, any: bool },
    Min(Option<f64>),
    Max(Option<f64>),
    Avg { sum: f64, count: u64 },
}

impl AggregateState {
    fn new(fun: AggFn) -> Self {
        match fun {
            AggFn::Count => Self::Count(0),
            AggFn::Sum => Self::Sum {
                sum: 0.0,
                any: false,
            },
            AggFn::Min => Self::Min(None),
            AggFn::Max => Self::Max(None),
            AggFn::Avg => Self::Avg { sum: 0.0, count: 0 },
        }
    }

    fn ingest(&mut self, value: Option<f64>) {
        match self {
            Self::Count(count) => *count = count.saturating_add(1),
            Self::Sum { sum, any } => {
                if let Some(value) = value {
                    *sum += value;
                    *any = true;
                }
            }
            Self::Min(best) => {
                if let Some(value) = value {
                    *best = Some(best.map_or(value, |current| current.min(value)));
                }
            }
            Self::Max(best) => {
                if let Some(value) = value {
                    *best = Some(best.map_or(value, |current| current.max(value)));
                }
            }
            Self::Avg { sum, count } => {
                if let Some(value) = value {
                    *sum += value;
                    *count = count.saturating_add(1);
                }
            }
        }
    }

    fn finish(self) -> Result<JsonValue, Error> {
        match self {
            Self::Count(count) => Ok(JsonValue::Number(Number::from(count))),
            Self::Sum { sum: _, any: false } => Ok(JsonValue::Null),
            Self::Sum { sum, any: true } => json_number(sum),
            Self::Min(None) | Self::Max(None) => Ok(JsonValue::Null),
            Self::Min(Some(value)) | Self::Max(Some(value)) => json_number(value),
            Self::Avg { count: 0, .. } => Ok(JsonValue::Null),
            Self::Avg { sum, count } => json_number(sum / count as f64),
        }
    }
}

#[derive(Debug)]
struct GroupState {
    aggregates: Vec<AggregateState>,
}

#[derive(Debug)]
struct GroupKey(Vec<JsonValue>);

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(&other.0)
                .all(|(left, right)| json_group_value_eq(left, right))
    }
}

fn json_group_value_eq(left: &JsonValue, right: &JsonValue) -> bool {
    match (left, right) {
        (JsonValue::Null, JsonValue::Null) => true,
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left == right,
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            left.to_string() == right.to_string()
        }
        (JsonValue::String(left), JsonValue::String(right)) => left == right,
        (JsonValue::Array(left), JsonValue::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| json_group_value_eq(left, right))
        }
        (JsonValue::Object(left), JsonValue::Object(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(
                    |((left_key, left_value), (right_key, right_value))| {
                        left_key == right_key && json_group_value_eq(left_value, right_value)
                    },
                )
        }
        _ => false,
    }
}

impl Eq for GroupKey {}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_group_values(&self.0, state);
    }
}

fn hash_group_values<H: Hasher>(values: &[JsonValue], state: &mut H) {
    values.len().hash(state);
    for value in values {
        hash_json_value(value, state);
    }
}

fn hash_json_value<H: Hasher>(value: &JsonValue, state: &mut H) {
    match value {
        JsonValue::Null => 0u8.hash(state),
        JsonValue::Bool(value) => {
            1u8.hash(state);
            value.hash(state);
        }
        JsonValue::Number(value) => {
            2u8.hash(state);
            value.to_string().hash(state);
        }
        JsonValue::String(value) => {
            3u8.hash(state);
            value.hash(state);
        }
        JsonValue::Array(values) => {
            4u8.hash(state);
            hash_group_values(values, state);
        }
        JsonValue::Object(fields) => {
            5u8.hash(state);
            fields.len().hash(state);
            for (key, value) in fields {
                key.hash(state);
                hash_json_value(value, state);
            }
        }
    }
}

struct CoveringSlots {
    fields: Vec<String>,
    group: Vec<usize>,
    numeric: Vec<usize>,
}

/// One-pass group accumulator. It retains group keys and scalar accumulator
/// state only; full input documents are consumed and released immediately.
pub(crate) struct GroupAccumulator<'a> {
    spec: &'a GroupAggSpec,
    groups: HashMap<GroupKey, GroupState>,
    numeric_paths: Vec<&'a Path>,
    aggregate_source_slots: Vec<Option<usize>>,
    covering_slots: Option<CoveringSlots>,
}

impl<'a> GroupAccumulator<'a> {
    pub(crate) fn new(spec: &'a GroupAggSpec) -> Self {
        let mut numeric_paths = Vec::new();
        let mut aggregate_source_slots = Vec::with_capacity(spec.aggregates.len());
        for aggregate in &spec.aggregates {
            if aggregate.fun == AggFn::Count {
                aggregate_source_slots.push(None);
                continue;
            }
            let Some(path) = aggregate.source.as_ref() else {
                aggregate_source_slots.push(None);
                continue;
            };
            let slot = numeric_paths
                .iter()
                .position(|existing| *existing == path)
                .unwrap_or_else(|| {
                    numeric_paths.push(path);
                    numeric_paths.len() - 1
                });
            aggregate_source_slots.push(Some(slot));
        }
        Self {
            spec,
            groups: HashMap::new(),
            numeric_paths,
            aggregate_source_slots,
            covering_slots: None,
        }
    }

    /// Whether grouping or aggregate inputs can observe the logical key that
    /// lives outside the stored JSON body.
    pub(crate) fn requires_logical_key(&self) -> bool {
        self.spec
            .group_by
            .iter()
            .chain(
                self.spec
                    .aggregates
                    .iter()
                    .filter_map(|aggregate| aggregate.source.as_ref()),
            )
            .any(is_logical_key_path)
    }

    /// Exact field order requested from a covering secondary index.
    pub(crate) fn covering_fields(&self) -> Option<Vec<String>> {
        let mut fields = Vec::new();
        for path in self
            .spec
            .group_by
            .iter()
            .chain(self.numeric_paths.iter().copied())
        {
            if is_logical_key_path(path) {
                return None;
            }
            let field = path.dotted();
            if !fields.contains(&field) {
                fields.push(field);
            }
        }
        (!fields.is_empty()).then_some(fields)
    }

    /// Ingest values supplied by an exact covering index without constructing
    /// or resolving a full JSON document.
    pub(crate) fn ingest_covering(
        &mut self,
        fields: &[String],
        values: Vec<JsonValue>,
    ) -> Result<(), Error> {
        if fields.len() != values.len() {
            return Err(Error::Internal(
                "covering aggregate row width mismatch".into(),
            ));
        }
        if self.covering_slots.is_none() {
            let resolve_slots = |paths: &[&Path]| -> Result<Vec<usize>, Error> {
                paths
                    .iter()
                    .map(|path| {
                        let dotted = path.dotted();
                        fields
                            .iter()
                            .position(|field| field == &dotted)
                            .ok_or_else(|| {
                                Error::Internal(format!(
                                    "covering aggregate omitted requested field `{dotted}`"
                                ))
                            })
                    })
                    .collect()
            };
            let group_paths = self.spec.group_by.iter().collect::<Vec<_>>();
            self.covering_slots = Some(CoveringSlots {
                fields: fields.to_vec(),
                group: resolve_slots(&group_paths)?,
                numeric: resolve_slots(&self.numeric_paths)?,
            });
        }
        let slots = self.covering_slots.as_ref().expect("covering slots set");
        if slots.fields != fields {
            return Err(Error::Internal(
                "covering aggregate fields changed within one scan".into(),
            ));
        }
        let numeric_values = slots
            .numeric
            .iter()
            .map(|slot| match &values[*slot] {
                JsonValue::Number(number) => number.as_f64().map(Some).ok_or_else(|| {
                    Error::QueryInvalid(format!(
                        "aggregate field `{}` is not numeric",
                        fields[*slot]
                    ))
                }),
                JsonValue::Null => Ok(None),
                _ => Err(Error::QueryInvalid(format!(
                    "aggregate field `{}` is not numeric",
                    fields[*slot]
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut values = values.into_iter().map(Some).collect::<Vec<_>>();
        let key_values = slots
            .group
            .iter()
            .map(|slot| {
                values[*slot]
                    .take()
                    .expect("covering group fields are unique")
            })
            .collect::<Vec<_>>();
        self.ingest_resolved(key_values, &numeric_values)
    }

    pub(crate) fn ingest(&mut self, doc: &JsonValue) -> Result<(), Error> {
        let mut key_values = Vec::with_capacity(self.spec.group_by.len());
        for path in &self.spec.group_by {
            key_values.push(match resolve_path(doc, path) {
                Resolve::Present(value) => value,
                Resolve::Absent => JsonValue::Null,
            });
        }
        let numeric_values = self
            .numeric_paths
            .iter()
            .map(|path| numeric_present(doc, path))
            .collect::<Result<Vec<_>, _>>()?;
        self.ingest_resolved(key_values, &numeric_values)
    }

    fn ingest_resolved(
        &mut self,
        key_values: Vec<JsonValue>,
        numeric_values: &[Option<f64>],
    ) -> Result<(), Error> {
        let group = self
            .groups
            .entry(GroupKey(key_values))
            .or_insert_with(|| GroupState {
                aggregates: self
                    .spec
                    .aggregates
                    .iter()
                    .map(|aggregate| AggregateState::new(aggregate.fun))
                    .collect(),
            });
        for ((aggregate, source_slot), state) in self
            .spec
            .aggregates
            .iter()
            .zip(&self.aggregate_source_slots)
            .zip(&mut group.aggregates)
        {
            let value = match aggregate.fun {
                AggFn::Count => None,
                _ => {
                    let slot = source_slot.ok_or_else(|| {
                        Error::QueryInvalid(format!(
                            "{}() requires a field path",
                            aggregate.fun.as_str()
                        ))
                    })?;
                    numeric_values[slot]
                }
            };
            state.ingest(value);
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<(String, JsonValue)>, Error> {
        let mut output = Vec::with_capacity(self.groups.len());
        let mut groups = self
            .groups
            .into_iter()
            .map(|(key, group)| {
                let canonical = canonical_group_key_bytes(&key.0);
                (canonical, key.0, group)
            })
            .collect::<Vec<_>>();
        groups.sort_by(|(left, _, _), (right, _, _)| left.cmp(right));
        for (canonical_bytes, key_values, group) in groups {
            let mut object = Map::new();
            for (index, path) in self.spec.group_by.iter().enumerate() {
                object.insert(output_field_name(path), key_values[index].clone());
            }
            for (aggregate, state) in self.spec.aggregates.iter().zip(group.aggregates) {
                object.insert(aggregate.output.clone(), state.finish()?);
            }
            output.push((
                format!("g:{}", canonical_group_key_from_bytes(&canonical_bytes)),
                JsonValue::Object(object),
            ));
        }
        Ok(output)
    }
}

fn is_logical_key_path(path: &Path) -> bool {
    matches!(path.0.first().map(String::as_str), Some("_key" | "$key"))
}

fn output_field_name(path: &Path) -> String {
    path.0.last().cloned().unwrap_or_else(|| path.dotted())
}

fn canonical_group_key_bytes(vals: &[JsonValue]) -> Vec<u8> {
    serde_json::to_vec(vals).unwrap_or_else(|_| b"[]".to_vec())
}

fn canonical_group_key_from_bytes(body: &[u8]) -> String {
    let hash = blake3::hash(body);
    bytes_to_hex(hash.as_bytes())
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

fn numeric_present(doc: &JsonValue, path: &Path) -> Result<Option<f64>, Error> {
    match resolve_path(doc, path) {
        Resolve::Absent => Ok(None),
        Resolve::Present(JsonValue::Null) => Ok(None),
        Resolve::Present(JsonValue::Number(n)) => {
            match n.as_f64() {
                Some(f) if f.is_finite() => Ok(Some(f)),
                // Non-finite JSON numbers (if any) skip like null.
                _ => Ok(None),
            }
        }
        // Heterogeneous bags: non-numeric present values skip (do not fail the query).
        Resolve::Present(JsonValue::String(s)) => {
            // Accept plain decimal strings when generators encode amounts as text.
            // Reject NaN/Inf tokens and non-numeric labels.
            let t = s.trim();
            if t.eq_ignore_ascii_case("nan")
                || t.eq_ignore_ascii_case("inf")
                || t.eq_ignore_ascii_case("+inf")
                || t.eq_ignore_ascii_case("-inf")
                || t.eq_ignore_ascii_case("infinity")
                || t.eq_ignore_ascii_case("+infinity")
                || t.eq_ignore_ascii_case("-infinity")
            {
                return Ok(None);
            }
            match t.parse::<f64>() {
                Ok(f) if f.is_finite() => Ok(Some(f)),
                _ => Ok(None),
            }
        }
        Resolve::Present(_) => Ok(None),
    }
}

fn json_number(v: f64) -> Result<JsonValue, Error> {
    if !v.is_finite() {
        return Err(Error::QueryInvalid(
            "aggregate produced non-finite number".into(),
        ));
    }
    // Prefer integer encoding when the value is integral and in i64 range.
    if v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
        return Ok(JsonValue::Number(Number::from(v as i64)));
    }
    Number::from_f64(v)
        .map(JsonValue::Number)
        .ok_or_else(|| Error::QueryInvalid("aggregate number encode failed".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_v1::AggregateSpec;
    use std::collections::hash_map::DefaultHasher;
    use std::collections::BTreeMap;

    fn path(s: &str) -> Path {
        Path::parse_dotted(s).unwrap()
    }

    #[test]
    fn semantic_group_key_matches_canonical_json_identity() {
        let values = [
            vec![],
            vec![JsonValue::Null],
            vec![serde_json::json!(true)],
            vec![serde_json::json!(1)],
            vec![serde_json::json!(1.0)],
            vec![serde_json::json!("x")],
            vec![serde_json::json!([1, "x", null])],
            vec![serde_json::json!({"a": 1, "b": [false]})],
            vec![serde_json::json!("x"), serde_json::json!(2)],
        ];
        for left in &values {
            for right in &values {
                let left_key = GroupKey(left.clone());
                let right_key = GroupKey(right.clone());
                let canonical_equal =
                    canonical_group_key_bytes(left) == canonical_group_key_bytes(right);
                assert_eq!(
                    left_key == right_key,
                    canonical_equal,
                    "{left:?} vs {right:?}"
                );
                if canonical_equal {
                    let mut left_hash = DefaultHasher::new();
                    let mut right_hash = DefaultHasher::new();
                    left_key.hash(&mut left_hash);
                    right_key.hash(&mut right_hash);
                    assert_eq!(left_hash.finish(), right_hash.finish());
                }
            }
        }
    }

    #[test]
    fn count_by_region() {
        let working = vec![
            ("a".into(), serde_json::json!({"region":"us","amount":1})),
            ("b".into(), serde_json::json!({"region":"eu","amount":2})),
            ("c".into(), serde_json::json!({"region":"us","amount":3})),
        ];
        let spec = GroupAggSpec {
            group_by: vec![path("region")],
            aggregates: vec![AggregateSpec {
                fun: AggFn::Count,
                source: None,
                output: "order_count".into(),
            }],
        };
        let out = apply_group_agg(working, &spec).unwrap();
        assert_eq!(out.len(), 2);
        let mut by_region = BTreeMap::new();
        for (_, v) in out {
            let r = v
                .get("region")
                .and_then(|x| x.as_str())
                .unwrap()
                .to_string();
            let c = v.get("order_count").and_then(|x| x.as_u64()).unwrap();
            by_region.insert(r, c);
        }
        assert_eq!(by_region.get("us"), Some(&2));
        assert_eq!(by_region.get("eu"), Some(&1));
    }

    #[test]
    fn global_min_max() {
        let working = vec![
            ("a".into(), serde_json::json!({"amount": 10})),
            ("b".into(), serde_json::json!({"amount": 3})),
            ("c".into(), serde_json::json!({"amount": 7})),
        ];
        let spec = GroupAggSpec {
            group_by: vec![],
            aggregates: vec![
                AggregateSpec {
                    fun: AggFn::Min,
                    source: Some(path("amount")),
                    output: "min_amount".into(),
                },
                AggregateSpec {
                    fun: AggFn::Max,
                    source: Some(path("amount")),
                    output: "max_amount".into(),
                },
            ],
        };
        let out = apply_group_agg(working, &spec).unwrap();
        assert_eq!(out.len(), 1);
        let v = &out[0].1;
        assert_eq!(v.get("min_amount").and_then(|x| x.as_i64()), Some(3));
        assert_eq!(v.get("max_amount").and_then(|x| x.as_i64()), Some(10));
    }
}
