//! RQL-Q3.1 — independent semantic oracle (test-only).
//!
//! Authority: `doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md` §6.1;
//! human report: `doc/todo/rql/RQL_Q3_1_SEMANTIC_ORACLE.md`.
//!
//! # Boundary (hard)
//!
//! This module is **deliberately unoptimised** and **not a product query path**:
//! - It reads complete **logical fixtures** (generator + seed), never store media.
//! - It evaluates `where` with [`Predicate::eval`] (total predicate profile / APP-4).
//! - It never calls product index lookup, plan selection, `CollectionClient::rql`,
//!   `execute_rql_full`, or QVM `run_vm`.
//! - Plan structure comes only from [`compile_app_core`] (language → logical plan).
//!   That is shared **meaning**, not optimiser/index-selection code.
//!
//! Exit of this suite = oracle runs corpus expected-result checks independently
//! of Residiuum plan selection. Green ≠ Gate-1; green ≠ full Q3 package accept.

#[path = "common/rql_evidence_write.rs"]
mod rql_evidence_write;

use residiuum_heap::CollectionId;
use residiuum_sdk::predicate::resolve_path;
use residiuum_sdk::{
    compile_app_core, compile_rql_full, source_uses_rql_full_constructs, AggFn, CollectionBindings,
    EnrichCardinality, EnrichStepV1, FullPipelineStepV1, NullsOrder, OrderDir, OrderTerm,
    Parameters, PredPath as Path, Predicate, ProjectExprV1, ProjectItemV1, Resolve, RqlPlanV1,
    WithinStepV1, APP_CORE_PROFILE,
};
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use std::process::Command;

// ---------------------------------------------------------------------------
// Workspace + fixture materialisation (logical only — no product store)
// ---------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

/// Bind `$param` names found in source (aligned with Q2 audit host binds).
fn parameters_for_source(source: &str) -> Parameters {
    let mut values = BTreeMap::new();
    let bytes = source.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                end += 1;
            }
            if end > start {
                let name = &source[start..end];
                // Cursor tokens need authenticated continuation, not JSON params.
                if name != "cursor" && !values.contains_key(name) {
                    let val = match name {
                        "sender" => json!("u-0004"),
                        "st" | "status" => json!("paid"),
                        _ if name.contains("id") => json!(format!("{name}-0004")),
                        _ => json!(name),
                    };
                    values.insert(name.to_string(), val);
                }
            }
            i = end;
            continue;
        }
        i += 1;
    }
    Parameters { values }
}

fn materialise_fixture(
    root: &FsPath,
    generator_id: &str,
    seed: u64,
    params: &Value,
) -> Result<Value, String> {
    let script = root.join("tools/rql_q1/materialise_fixture.py");
    let params_s = serde_json::to_string(params).map_err(|e| e.to_string())?;
    let out = Command::new("python3")
        .arg(&script)
        .arg("--generator")
        .arg(generator_id)
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--params")
        .arg(params_s)
        .output()
        .map_err(|e| format!("spawn materialise_fixture: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "materialise_fixture failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    serde_json::from_slice(&out.stdout).map_err(|e| format!("fixture json: {e}"))
}

fn companion_generator_for(collection: &str) -> Option<&'static str> {
    match collection {
        "orders" => Some("commerce.orders_v1"),
        "customers" => Some("commerce.customers_v1"),
        "line_items" => Some("commerce.line_items_v1"),
        "products" => Some("commerce.products_v1"),
        "inventory" => Some("commerce.inventory_v1"),
        "conversations" => Some("messaging.conversations_v1"),
        "messages" => Some("messaging.messages_v1"),
        "participants" => Some("messaging.participants_v1"),
        "entries" => Some("directory.entries_v1"),
        "categories" => Some("directory.categories_v1"),
        "locations" => Some("directory.locations_v1"),
        "devices" => Some("telemetry.devices_v1"),
        "events" => Some("telemetry.events_v1"),
        "metrics" => Some("telemetry.metrics_v1"),
        "projects" => Some("project_management.projects_v1"),
        "tasks" => Some("project_management.tasks_v1"),
        "revisions" => Some("project_management.revisions_v1"),
        "memberships" => Some("project_management.memberships_v1"),
        _ => None,
    }
}

fn extract_collection_names(source: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let tokens: Vec<&str> = source.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let t = tokens[i].to_ascii_lowercase();
        if (t == "from" || t == "using") && i + 1 < tokens.len() {
            let n = tokens[i + 1]
                .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .to_string();
            if !n.is_empty() && n != "where" && n != "project" {
                names.insert(n);
            }
        }
        if t == "enrich" && i + 3 < tokens.len() && tokens[i + 2].eq_ignore_ascii_case("from") {
            let n = tokens[i + 3]
                .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .to_string();
            if !n.is_empty() {
                names.insert(n);
            }
        }
        i += 1;
    }
    names
}

fn compile_bindings_for(names: &BTreeSet<String>) -> CollectionBindings {
    let mut b = CollectionBindings::default();
    for (i, name) in names.iter().enumerate() {
        let mut bytes = [0u8; 16];
        let n = name.as_bytes();
        for (j, &c) in n.iter().enumerate().take(12) {
            bytes[j] = c;
        }
        bytes[12] = (i as u8).wrapping_add(1);
        bytes[13] = 0xC3;
        bytes[14] = 0x01;
        bytes[15] = 0x03;
        let id = CollectionId::from_bytes_unchecked_nonzero(bytes)
            .or_else(|_| CollectionId::new_random())
            .expect("collection id");
        b.bind(name, id);
    }
    b
}

/// Logical fixture: collection name → keyed documents.
///
/// Each body retains `_key` so paths like `_key` and projections of `_key`
/// evaluate against the complete logical document (store media strip `_key`).
type LogicalDb = BTreeMap<String, BTreeMap<String, Value>>;

fn docs_from_materialised(collections: &Map<String, Value>) -> Result<LogicalDb, String> {
    let mut db = LogicalDb::new();
    for (name, docs) in collections {
        let arr = docs
            .as_array()
            .ok_or_else(|| format!("collection `{name}` is not an array"))?;
        let mut map = BTreeMap::new();
        for doc in arr {
            let key = doc
                .get("_key")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("doc in `{name}` missing string `_key`"))?
                .to_string();
            // Keep full logical document including `_key`.
            map.insert(key, doc.clone());
        }
        db.insert(name.clone(), map);
    }
    Ok(db)
}

fn load_case_fixture(
    root: &FsPath,
    generator_id: &str,
    seed: u64,
    params: &Value,
    needed: &BTreeSet<String>,
) -> Result<LogicalDb, String> {
    let mat = materialise_fixture(root, generator_id, seed, params)?;
    let mut cols = mat
        .get("collections")
        .and_then(|c| c.as_object())
        .cloned()
        .ok_or_else(|| "fixture missing collections object".to_string())?;
    for name in needed {
        if cols.contains_key(name) {
            continue;
        }
        let Some(gen) = companion_generator_for(name) else {
            continue;
        };
        let mat2 = materialise_fixture(root, gen, seed, params)?;
        if let Some(obj) = mat2.get("collections").and_then(|c| c.as_object()) {
            for (k, v) in obj {
                cols.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
    }
    docs_from_materialised(&cols)
}

// ---------------------------------------------------------------------------
// Independent oracle evaluation (full scan of logical fixture)
// ---------------------------------------------------------------------------

/// One oracle result row.
#[derive(Clone, Debug, PartialEq)]
struct OracleRow {
    key: String,
    value: Value,
}

/// Complete oracle answer for one query against a logical fixture.
#[derive(Clone, Debug, PartialEq)]
struct OracleResult {
    rows: Vec<OracleRow>,
    /// Always true for this unoptimised complete-fixture scan (no holes).
    coverage_complete: bool,
    /// Profile stamp — never a product runtime profile.
    oracle_profile: &'static str,
}

const ORACLE_PROFILE: &str = "residiuum-rql-q3-semantic-oracle-v1";

/// Errors from the independent oracle (not product ErrorCode surface).
#[derive(Debug)]
enum OracleError {
    Compile(String),
    Eval(String),
    Fixture(String),
    Unsupported(String),
}

impl std::fmt::Display for OracleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compile(s) | Self::Eval(s) | Self::Fixture(s) | Self::Unsupported(s) => {
                write!(f, "{s}")
            }
        }
    }
}

fn is_key_path(path: &Path) -> bool {
    path.0.len() == 1 && (path.0[0] == "_key" || path.0[0] == "$key")
}

fn path_resolve_with_key(key: &str, doc: &Value, path: &Path) -> Resolve {
    if is_key_path(path) {
        return Resolve::Present(Value::String(key.to_string()));
    }
    resolve_path(doc, path)
}

/// Predicate eval that treats `_key` / `$key` as the immutable document key.
fn eval_pred_with_key(
    pred: &Predicate,
    key: &str,
    doc: &Value,
    params: &BTreeMap<String, Value>,
) -> Result<bool, OracleError> {
    // Inject synthetic `_key` for ordinary Predicate::eval without forking the
    // total predicate profile implementation. Product store bodies omit `_key`;
    // the logical fixture already has it, but `$key` still needs a field.
    let mut eval_doc = doc.clone();
    if let Value::Object(ref mut m) = eval_doc {
        m.insert("_key".into(), Value::String(key.to_string()));
        m.insert("$key".into(), Value::String(key.to_string()));
    }
    pred.eval(&eval_doc, params)
        .map_err(|e| OracleError::Eval(e.to_string()))
}

fn apply_project_paths_oracle(key: &str, doc: &Value, paths: Option<&Vec<Path>>) -> Value {
    let Some(paths) = paths else {
        return doc.clone();
    };
    let mut out = Map::new();
    for p in paths {
        match path_resolve_with_key(key, doc, p) {
            Resolve::Present(v) => {
                if p.0.len() == 1 {
                    out.insert(p.0[0].clone(), v);
                } else {
                    out.insert(p.dotted(), v);
                }
            }
            Resolve::Absent => {}
        }
    }
    Value::Object(out)
}

fn eval_project_expr_oracle(
    key: &str,
    doc: &Value,
    expr: &ProjectExprV1,
    params: &BTreeMap<String, Value>,
) -> Result<Value, OracleError> {
    match expr {
        ProjectExprV1::Literal(v) => Ok(v.clone()),
        ProjectExprV1::Path(path) => Ok(match path_resolve_with_key(key, doc, path) {
            Resolve::Present(v) => v,
            Resolve::Absent => Value::Null,
        }),
        ProjectExprV1::Conditional {
            when,
            then_expr,
            else_expr,
        } => {
            let selected = if eval_pred_with_key(when, key, doc, params)? {
                then_expr
            } else {
                else_expr
            };
            eval_project_expr_oracle(key, doc, selected, params)
        }
    }
}

fn apply_computed_project_oracle(
    result: &mut OracleResult,
    fields: &[ProjectItemV1],
    params: &BTreeMap<String, Value>,
) -> Result<(), OracleError> {
    for row in &mut result.rows {
        let mut out = Map::new();
        for field in fields {
            match field {
                ProjectItemV1::Leaf { output, source } => {
                    if let Resolve::Present(v) = path_resolve_with_key(&row.key, &row.value, source)
                    {
                        out.insert(output.clone(), v);
                    }
                }
                ProjectItemV1::Computed { output, expression } => {
                    out.insert(
                        output.clone(),
                        eval_project_expr_oracle(&row.key, &row.value, expression, params)?,
                    );
                }
                ProjectItemV1::Nested { output, fields } => {
                    let source = Path(vec![output.clone()]);
                    let projected = match path_resolve_with_key(&row.key, &row.value, &source) {
                        Resolve::Absent => continue,
                        Resolve::Present(Value::Object(obj)) => {
                            project_value_oracle(&row.key, Value::Object(obj), fields, params)?
                        }
                        Resolve::Present(Value::Array(items)) => Value::Array(
                            items
                                .into_iter()
                                .enumerate()
                                .map(|(i, item)| {
                                    project_value_oracle(
                                        &format!("{}#{i}", row.key),
                                        item,
                                        fields,
                                        params,
                                    )
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                        ),
                        Resolve::Present(Value::Null) => Value::Null,
                        Resolve::Present(_) => {
                            return Err(OracleError::Eval(format!(
                                "nested project `{output}` requires object/array"
                            )));
                        }
                    };
                    out.insert(output.clone(), projected);
                }
            }
        }
        row.value = Value::Object(out);
    }
    Ok(())
}

fn project_value_oracle(
    key: &str,
    value: Value,
    fields: &[ProjectItemV1],
    params: &BTreeMap<String, Value>,
) -> Result<Value, OracleError> {
    let mut result = OracleResult {
        rows: vec![OracleRow {
            key: key.into(),
            value,
        }],
        coverage_complete: true,
        oracle_profile: ORACLE_PROFILE,
    };
    apply_computed_project_oracle(&mut result, fields, params)?;
    Ok(result.rows.pop().expect("one row").value)
}

fn strip_logical_key(mut value: Value) -> Value {
    if let Value::Object(map) = &mut value {
        map.remove("_key");
    }
    value
}

fn attach_enrich_oracle(
    rows: Vec<OracleRow>,
    db: &LogicalDb,
    step: &EnrichStepV1,
    params: &BTreeMap<String, Value>,
) -> Result<Vec<OracleRow>, OracleError> {
    let foreign = db.get(&step.using_name).ok_or_else(|| {
        OracleError::Fixture(format!("missing foreign collection {}", step.using_name))
    })?;
    let mut out = Vec::with_capacity(rows.len());
    for mut row in rows {
        let left = path_resolve_with_key(&row.key, &row.value, &step.left);
        let mut matches = Vec::new();
        if let Resolve::Present(left_value) = left {
            for (foreign_key, foreign_doc) in foreign {
                if let Some(pred) = &step.candidate_where {
                    if !eval_pred_with_key(pred, foreign_key, foreign_doc, params)? {
                        continue;
                    }
                }
                if let Resolve::Present(right_value) =
                    path_resolve_with_key(foreign_key, foreign_doc, &step.right)
                {
                    if right_value == left_value {
                        matches.push(strip_logical_key(foreign_doc.clone()));
                    }
                }
            }
        }
        let attached = match (step.expect, matches.len()) {
            (EnrichCardinality::ExactlyOne, 1) => matches.remove(0),
            (EnrichCardinality::ExactlyOne, n) => {
                return Err(OracleError::Eval(format!(
                    "exactly_one expected 1 match, got {n}"
                )));
            }
            (EnrichCardinality::Optional, 0) => Value::Null,
            (EnrichCardinality::Optional, 1) => matches.remove(0),
            (EnrichCardinality::Optional, n) => {
                return Err(OracleError::Eval(format!(
                    "optional expected at most 1 match, got {n}"
                )));
            }
            (EnrichCardinality::Many, _) => Value::Array(matches),
        };
        row.value
            .as_object_mut()
            .ok_or_else(|| OracleError::Eval("enrich root must be object".into()))?
            .insert(step.output.clone(), attached);
        out.push(row);
    }
    Ok(out)
}

fn set_path_oracle(doc: &mut Value, path: &Path, value: Value) -> Result<(), OracleError> {
    let (last, parents) = path
        .0
        .split_last()
        .ok_or_else(|| OracleError::Eval("empty within carrier".into()))?;
    let mut current = doc;
    for segment in parents {
        current = current
            .as_object_mut()
            .and_then(|map| map.get_mut(segment))
            .ok_or_else(|| {
                OracleError::Eval(format!("missing within carrier parent `{segment}`"))
            })?;
    }
    current
        .as_object_mut()
        .ok_or_else(|| OracleError::Eval("within carrier parent must be object".into()))?
        .insert(last.clone(), value);
    Ok(())
}

fn apply_within_oracle(
    rows: Vec<OracleRow>,
    db: &LogicalDb,
    step: &WithinStepV1,
    params: &BTreeMap<String, Value>,
) -> Result<Vec<OracleRow>, OracleError> {
    let mut out = Vec::with_capacity(rows.len());
    for mut parent in rows {
        let items = match path_resolve_with_key(&parent.key, &parent.value, &step.carrier) {
            Resolve::Present(Value::Array(items)) => items,
            _ => {
                return Err(OracleError::Eval(format!(
                    "within `{}` requires array",
                    step.carrier.dotted()
                )));
            }
        };
        let elements = items
            .into_iter()
            .enumerate()
            .map(|(i, value)| OracleRow {
                key: format!("{}#{i}", parent.key),
                value,
            })
            .collect();
        let elements = apply_pipeline_oracle(elements, db, &step.steps, params)?;
        set_path_oracle(
            &mut parent.value,
            &step.carrier,
            Value::Array(elements.into_iter().map(|row| row.value).collect()),
        )?;
        out.push(parent);
    }
    Ok(out)
}

fn apply_pipeline_oracle(
    mut rows: Vec<OracleRow>,
    db: &LogicalDb,
    steps: &[FullPipelineStepV1],
    params: &BTreeMap<String, Value>,
) -> Result<Vec<OracleRow>, OracleError> {
    for step in steps {
        rows = match step {
            FullPipelineStepV1::Enrich(step) => attach_enrich_oracle(rows, db, step, params)?,
            FullPipelineStepV1::Within(step) => apply_within_oracle(rows, db, step, params)?,
            FullPipelineStepV1::Filter(pred) => {
                let mut kept = Vec::new();
                for row in rows {
                    if eval_pred_with_key(pred, &row.key, &row.value, params)? {
                        kept.push(row);
                    }
                }
                kept
            }
        };
    }
    Ok(rows)
}

fn json_ord(a: &Value, b: &Value) -> Ordering {
    use Value::*;
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Bool(x), Bool(y)) => x.cmp(y),
        (Number(x), Number(y)) => {
            // Prefer i64/u64 exact; else f64 total-order via partial_cmp.
            if let (Some(xi), Some(yi)) = (x.as_i64(), y.as_i64()) {
                return xi.cmp(&yi);
            }
            if let (Some(xu), Some(yu)) = (x.as_u64(), y.as_u64()) {
                return xu.cmp(&yu);
            }
            let xf = x.as_f64().unwrap_or(f64::NAN);
            let yf = y.as_f64().unwrap_or(f64::NAN);
            xf.partial_cmp(&yf).unwrap_or(Ordering::Equal)
        }
        (String(x), String(y)) => x.cmp(y),
        (Array(x), Array(y)) => {
            for (xi, yi) in x.iter().zip(y.iter()) {
                let c = json_ord(xi, yi);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        (Object(x), Object(y)) => {
            // Deterministic map order via BTreeMap clone of keys.
            let xk: BTreeSet<_> = x.keys().collect();
            let yk: BTreeSet<_> = y.keys().collect();
            for (xk, yk) in xk.iter().zip(yk.iter()) {
                let c = (*xk).cmp(*yk);
                if c != Ordering::Equal {
                    return c;
                }
                let c = json_ord(&x[*xk], &y[*yk]);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        // Cross-type: type rank for stable oracle order (not a product claim).
        _ => type_rank(a).cmp(&type_rank(b)).then_with(|| {
            serde_json::to_string(a)
                .unwrap_or_default()
                .cmp(&serde_json::to_string(b).unwrap_or_default())
        }),
    }
}

fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

fn is_nullish(r: &Resolve) -> bool {
    matches!(r, Resolve::Absent | Resolve::Present(Value::Null))
}

fn compare_resolve_oracle(a: &Resolve, b: &Resolve, nulls: NullsOrder) -> Ordering {
    let a_null = is_nullish(a);
    let b_null = is_nullish(b);
    if a_null && b_null {
        return Ordering::Equal;
    }
    if a_null {
        return match nulls {
            NullsOrder::First => Ordering::Less,
            NullsOrder::Last => Ordering::Greater,
        };
    }
    if b_null {
        return match nulls {
            NullsOrder::First => Ordering::Greater,
            NullsOrder::Last => Ordering::Less,
        };
    }
    match (a, b) {
        (Resolve::Present(x), Resolve::Present(y)) => json_ord(x, y),
        _ => Ordering::Equal,
    }
}

fn apply_dir(c: Ordering, dir: OrderDir) -> Ordering {
    match dir {
        OrderDir::Asc => c,
        OrderDir::Desc => c.reverse(),
    }
}

fn compare_rows_oracle(
    ka: &str,
    va: &Value,
    kb: &str,
    vb: &Value,
    order: &[OrderTerm],
) -> Ordering {
    for term in order {
        if term.tie_break || is_key_path(&term.path) {
            return apply_dir(ka.cmp(kb), term.dir);
        }
        let ra = path_resolve_with_key(ka, va, &term.path);
        let rb = path_resolve_with_key(kb, vb, &term.path);
        let c = compare_resolve_oracle(&ra, &rb, term.nulls);
        if c != Ordering::Equal {
            return apply_dir(c, term.dir);
        }
    }
    ka.cmp(kb)
}

fn num_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn apply_group_agg(
    rows: &[(String, Value)],
    plan: &RqlPlanV1,
) -> Result<Vec<(String, Value)>, OracleError> {
    let g = &plan.group_agg;
    if !g.is_active() {
        return Ok(rows.to_vec());
    }

    // Group key → member docs (preserve first-seen order of groups by key digest).
    let mut groups: BTreeMap<String, Vec<(String, Value)>> = BTreeMap::new();
    for (k, doc) in rows {
        let mut key_parts = Vec::new();
        for p in &g.group_by {
            match path_resolve_with_key(k, doc, p) {
                Resolve::Present(v) => key_parts.push(v),
                Resolve::Absent => key_parts.push(json!({"__rv": "absent"})),
            }
        }
        let gk = serde_json::to_string(&key_parts).unwrap_or_default();
        groups.entry(gk).or_default().push((k.clone(), doc.clone()));
    }

    let mut out = Vec::new();
    for (gi, (_gk, members)) in groups.into_iter().enumerate() {
        let mut obj = Map::new();
        if let Some((mk, md)) = members.first() {
            for p in &g.group_by {
                match path_resolve_with_key(mk, md, p) {
                    Resolve::Present(v) => {
                        if p.0.len() == 1 {
                            obj.insert(p.0[0].clone(), v);
                        } else {
                            obj.insert(p.dotted(), v);
                        }
                    }
                    Resolve::Absent => {}
                }
            }
        }
        for agg in &g.aggregates {
            let val = match agg.fun {
                AggFn::Count => Value::Number((members.len() as u64).into()),
                AggFn::Sum | AggFn::Min | AggFn::Max | AggFn::Avg => {
                    let path = agg.source.as_ref().ok_or_else(|| {
                        OracleError::Eval(format!("{} requires a source path", agg.fun.as_str()))
                    })?;
                    let mut nums: Vec<f64> = Vec::new();
                    for (mk, md) in &members {
                        if let Resolve::Present(v) = path_resolve_with_key(mk, md, path) {
                            if let Some(n) = num_f64(&v) {
                                if n.is_finite() {
                                    nums.push(n);
                                }
                            }
                        }
                    }
                    match agg.fun {
                        AggFn::Sum => {
                            let s: f64 = nums.iter().sum();
                            json_number_prefer_int(s)
                        }
                        AggFn::Min => nums
                            .iter()
                            .cloned()
                            .reduce(f64::min)
                            .map(json_number_prefer_int)
                            .unwrap_or(Value::Null),
                        AggFn::Max => nums
                            .iter()
                            .cloned()
                            .reduce(f64::max)
                            .map(json_number_prefer_int)
                            .unwrap_or(Value::Null),
                        AggFn::Avg => {
                            if nums.is_empty() {
                                Value::Null
                            } else {
                                let s: f64 = nums.iter().sum();
                                json_number_prefer_int(s / (nums.len() as f64))
                            }
                        }
                        AggFn::Count => unreachable!(),
                    }
                }
            };
            obj.insert(agg.output.clone(), val);
        }
        // Synthetic group row key — stable, not a product identity claim.
        out.push((format!("__group_{gi}"), Value::Object(obj)));
    }
    Ok(out)
}

fn json_number_prefer_int(n: f64) -> Value {
    if n.is_finite() && n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
        json!(n as i64)
    } else {
        json!(n)
    }
}

/// Run the independent semantic oracle over a logical fixture + App Core plan.
fn oracle_eval_plan(
    db: &LogicalDb,
    plan: &RqlPlanV1,
    params: &BTreeMap<String, Value>,
) -> Result<OracleResult, OracleError> {
    let src_name = &plan.from.source_name;
    let coll = db.get(src_name).ok_or_else(|| {
        OracleError::Fixture(format!("logical fixture missing collection `{src_name}`"))
    })?;

    // Full scan — deliberately unoptimised; no index, no plan selection.
    let mut working: Vec<(String, Value)> = Vec::new();
    for (key, doc) in coll {
        if eval_pred_with_key(&plan.where_pred, key, doc, params)? {
            working.push((key.clone(), doc.clone()));
        }
    }

    if plan.group_agg.is_active() {
        working = apply_group_agg(&working, plan)?;
    } else if !plan.order.is_empty() {
        working.sort_by(|(ka, va), (kb, vb)| compare_rows_oracle(ka, va, kb, vb, &plan.order));
    } else {
        // Deterministic bag materialisation order by key (not a declared order).
        working.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));
    }

    if let Some(n) = plan.limit {
        working.truncate(n as usize);
    }
    // First page only — `after` is Application Core residual / APP-6.
    let page_size = plan.page_size as usize;
    if working.len() > page_size {
        working.truncate(page_size);
    }

    let rows = working
        .into_iter()
        .map(|(k, v)| {
            let value = if plan.group_agg.is_active() {
                v
            } else {
                apply_project_paths_oracle(&k, &v, plan.project.as_ref())
            };
            OracleRow { key: k, value }
        })
        .collect();

    Ok(OracleResult {
        rows,
        coverage_complete: true,
        oracle_profile: ORACLE_PROFILE,
    })
}

fn oracle_eval_source(
    db: &LogicalDb,
    source: &str,
    params: &Parameters,
) -> Result<(RqlPlanV1, OracleResult), OracleError> {
    let names = extract_collection_names(source);
    let bindings = compile_bindings_for(&names);
    if source_uses_rql_full_constructs(source) {
        let compiled =
            compile_rql_full(source, &bindings).map_err(|e| OracleError::Compile(e.to_string()))?;
        let mut result = oracle_eval_plan(db, &compiled.base.plan, &params.values)?;
        result.rows = apply_pipeline_oracle(result.rows, db, &compiled.pipeline, &params.values)?;
        if let Some(fields) = &compiled.project {
            apply_computed_project_oracle(&mut result, fields, &params.values)?;
        }
        return Ok((compiled.base.plan, result));
    }
    let compiled = compile_app_core(source, &bindings).map_err(|e| {
        let msg = e.to_string();
        let low = msg.to_ascii_lowercase();
        if low.contains("after")
            || low.contains("enrich")
            || low.contains("rql_feature_unavailable")
            || low.contains("full")
        {
            OracleError::Unsupported(msg)
        } else {
            OracleError::Compile(msg)
        }
    })?;
    assert_eq!(compiled.profile, APP_CORE_PROFILE);
    let result = oracle_eval_plan(db, &compiled.plan, &params.values)?;
    Ok((compiled.plan, result))
}

// ---------------------------------------------------------------------------
// Canonicalisation + digests (equivalence dimensions sketch for Q3.2)
// ---------------------------------------------------------------------------

fn canonical_row_json(row: &OracleRow) -> Value {
    json!({
        "key": row.key,
        "value": row.value,
    })
}

/// Digest over keys + values + coverage. Order-sensitive when `ordered`.
fn result_digest(result: &OracleResult, ordered: bool) -> String {
    let mut rows: Vec<Value> = result.rows.iter().map(canonical_row_json).collect();
    if !ordered {
        rows.sort_by(|a, b| {
            let ka = a["key"].as_str().unwrap_or("");
            let kb = b["key"].as_str().unwrap_or("");
            ka.cmp(kb).then_with(|| {
                let sa = serde_json::to_string(&a["value"]).unwrap_or_default();
                let sb = serde_json::to_string(&b["value"]).unwrap_or_default();
                sa.cmp(&sb)
            })
        });
    }
    let body = json!({
        "oracle_profile": result.oracle_profile,
        "coverage_complete": result.coverage_complete,
        "ordered": ordered,
        "row_count": rows.len(),
        "rows": rows,
    });
    let bytes = serde_json::to_vec(&body).expect("digest body");
    // Test-local FNV-1a digest (not product crypto). Determinism is the claim.
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in &bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("fnv1a64:{h:016x}:len:{}", bytes.len())
}

fn ordering_is_sensitive(meta: &Value) -> bool {
    match meta.get("order").and_then(|v| v.as_str()) {
        Some("unordered_multiset") | Some("unordered_set") | None => false,
        Some(_) => true,
    }
}

/// Cases admitted to independent semantics after Q2 capability closure. The
/// corpus labels remain untouched pending the Q1 principal amendment.
fn semantic_oracle_eligible(case: &Value) -> bool {
    if case["expected"]["kind"].as_str() == Some("oracle_rule") {
        return true;
    }
    case["expected"]["kind"].as_str() == Some("deferred_q2")
        && case["family_tags"].as_array().is_some_and(|tags| {
            tags.iter().any(|tag| {
                matches!(
                    tag.as_str(),
                    Some(
                        "group_aggregate"
                            | "projection_computed_conditional"
                            | "enrichment_cardinality"
                    )
                )
            })
        })
}

// ---------------------------------------------------------------------------
// Unit checks (hand fixtures — expected results independent of product path)
// ---------------------------------------------------------------------------

#[test]
fn q3_oracle_hand_eq_status_paid() {
    let mut orders = BTreeMap::new();
    orders.insert(
        "o-1".into(),
        json!({"_key":"o-1","status":"paid","region":"us"}),
    );
    orders.insert(
        "o-2".into(),
        json!({"_key":"o-2","status":"open","region":"us"}),
    );
    orders.insert(
        "o-3".into(),
        json!({"_key":"o-3","status":"paid","region":"eu"}),
    );
    let mut db = LogicalDb::new();
    db.insert("orders".into(), orders);

    let (_plan, res) = oracle_eval_source(
        &db,
        r#"from orders where status = "paid""#,
        &Parameters::default(),
    )
    .expect("oracle");
    assert!(res.coverage_complete);
    assert_eq!(res.oracle_profile, ORACLE_PROFILE);
    let keys: Vec<_> = res.rows.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(keys, vec!["o-1", "o-3"]);
}

#[test]
fn q3_oracle_hand_key_get_uses_logical_key() {
    let mut orders = BTreeMap::new();
    orders.insert("o-0007".into(), json!({"_key":"o-0007","status":"paid"}));
    orders.insert("o-0008".into(), json!({"_key":"o-0008","status":"open"}));
    let mut db = LogicalDb::new();
    db.insert("orders".into(), orders);

    let (_plan, res) = oracle_eval_source(
        &db,
        r#"from orders where _key = "o-0007""#,
        &Parameters::default(),
    )
    .expect("oracle");
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0].key, "o-0007");
}

#[test]
fn q3_oracle_hand_missing_vs_null() {
    let mut orders = BTreeMap::new();
    orders.insert("a".into(), json!({"_key":"a","status":"paid"})); // missing notes
    orders.insert("b".into(), json!({"_key":"b","status":"paid","notes":null}));
    orders.insert("c".into(), json!({"_key":"c","status":"paid","notes":"x"}));
    let mut db = LogicalDb::new();
    db.insert("orders".into(), orders);

    let (_, missing) = oracle_eval_source(
        &db,
        "from orders where missing(notes)",
        &Parameters::default(),
    )
    .expect("missing");
    assert_eq!(
        missing
            .rows
            .iter()
            .map(|r| r.key.as_str())
            .collect::<Vec<_>>(),
        vec!["a"]
    );

    let (_, is_null) = oracle_eval_source(
        &db,
        "from orders where notes is null",
        &Parameters::default(),
    )
    .expect("null");
    assert_eq!(
        is_null
            .rows
            .iter()
            .map(|r| r.key.as_str())
            .collect::<Vec<_>>(),
        vec!["b"]
    );
}

#[test]
fn q3_oracle_hand_order_and_limit() {
    let mut orders = BTreeMap::new();
    for (k, t) in [
        ("o-1", "2024-01-01"),
        ("o-2", "2024-01-03"),
        ("o-3", "2024-01-02"),
    ] {
        orders.insert(
            k.into(),
            json!({"_key": k, "created_at": t, "status": "paid"}),
        );
    }
    let mut db = LogicalDb::new();
    db.insert("orders".into(), orders);

    let (_, res) = oracle_eval_source(
        &db,
        "from orders order by created_at desc, _key asc limit 2",
        &Parameters::default(),
    )
    .expect("order");
    let keys: Vec<_> = res.rows.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(keys, vec!["o-2", "o-3"]);
}

#[test]
fn q3_oracle_hand_project_paths() {
    let mut orders = BTreeMap::new();
    orders.insert(
        "o-1".into(),
        json!({"_key":"o-1","status":"paid","region":"us","secret":99}),
    );
    let mut db = LogicalDb::new();
    db.insert("orders".into(), orders);

    let (_, res) = oracle_eval_source(
        &db,
        r#"from orders where status = "paid" project _key, status"#,
        &Parameters::default(),
    )
    .expect("project");
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0].value, json!({"_key":"o-1","status":"paid"}));
    assert!(res.rows[0].value.get("secret").is_none());
    assert!(res.rows[0].value.get("region").is_none());
}

#[test]
fn q3_oracle_is_not_product_runtime_profile() {
    // Firewall documentation stamp — product runtimes use different profiles.
    assert_ne!(ORACLE_PROFILE, "residiuum-query-bytecode-v1");
    assert_ne!(ORACLE_PROFILE, "residiuum-app-core-exec-v1");
    assert_ne!(ORACLE_PROFILE, "rql-full-v1");
    assert!(ORACLE_PROFILE.contains("oracle"));
}

// ---------------------------------------------------------------------------
// Corpus suite — expected-result checks without product plan selection
// ---------------------------------------------------------------------------

#[test]
fn rql_q3_1_corpus_oracle_suite() {
    let root = workspace_root();
    let corpus_path = root.join("spec/rql/qualification/corpus-v1/corpus-v1.json");
    let text = fs::read_to_string(&corpus_path).expect("read corpus");
    let doc: Value = serde_json::from_str(&text).expect("parse corpus");
    let corpus_version = doc["corpus_version"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let cases = doc["cases"].as_array().expect("cases");
    let tier_a_total = cases
        .iter()
        .filter(|case| case["tier"].as_str() == Some("A"))
        .count() as u64;

    let mut case_results: Vec<Value> = Vec::new();
    let mut oracle_ok = 0u64;
    let mut oracle_unsupported = 0u64;
    let mut oracle_compile_fail = 0u64;
    let mut oracle_eval_fail = 0u64;
    let mut oracle_fixture_fail = 0u64;
    let mut skipped_non_oracle_rule = 0u64;
    let mut skipped_no_source = 0u64;
    let mut tier_a_considered = 0u64;
    let mut digest_mismatch = 0u64;
    let mut stable_refusal_ok = 0u64;
    let mut explain_contract_ok = 0u64;
    let mut non_row_fail = 0u64;

    for case in cases {
        let tier = case["tier"].as_str().unwrap_or("");
        if tier != "A" {
            continue;
        }
        let rql = &case["implementations"]["rql"];
        let rql_status = rql["status"].as_str().unwrap_or("");
        let case_id = case["case_id"].as_str().unwrap_or("?").to_string();
        let source = rql["source"].as_str().unwrap_or("").to_string();
        let expected_kind = case["expected"]["kind"].as_str().unwrap_or("");

        if expected_kind == "stable_refusal" {
            let expected_code = rql["refusal_code"].as_str().unwrap_or("");
            let bindings = compile_bindings_for(&extract_collection_names(&source));
            let result = compile_rql_full(&source, &bindings);
            match result {
                Err(error)
                    if !expected_code.is_empty() && error.to_string().contains(expected_code) =>
                {
                    stable_refusal_ok += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "stable_refusal_ok",
                        "refusal_code": expected_code,
                        "source": source,
                    }));
                }
                other => {
                    non_row_fail += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "stable_refusal_fail",
                        "expected_code": expected_code,
                        "observed": format!("{other:?}"),
                        "source": source,
                    }));
                }
            }
            continue;
        }

        if source.to_ascii_lowercase().starts_with("explain ") {
            let bindings = compile_bindings_for(&extract_collection_names(&source));
            let first = compile_app_core(&source, &bindings);
            let second = compile_app_core(&source, &bindings);
            match (first, second) {
                (Ok(a), Ok(b))
                    if a.explain
                        && b.explain
                        && a.plan.plan_hash() == b.plan.plan_hash()
                        && a.plan.to_canonical_json() == b.plan.to_canonical_json() =>
                {
                    explain_contract_ok += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "explain_contract_ok",
                        "plan_hash": a.plan.plan_hash_hex(),
                        "row_materialization": false,
                        "source": source,
                    }));
                }
                other => {
                    non_row_fail += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "explain_contract_fail",
                        "observed": format!("{other:?}"),
                        "source": source,
                    }));
                }
            }
            continue;
        }

        if !semantic_oracle_eligible(case) {
            skipped_non_oracle_rule += 1;
            continue;
        }
        if rql_status != "source" && rql_status != "pending" {
            skipped_no_source += 1;
            continue;
        }
        tier_a_considered += 1;
        let domain = case["domain"].as_str().unwrap_or("").to_string();
        let ordered = ordering_is_sensitive(&case["ordering_and_multiplicity"]);

        let fixture = &case["fixture"];
        let generator_id = fixture["generator_id"].as_str().unwrap_or("");
        let seed = fixture["seed"].as_u64().unwrap_or(0);
        let params_json = fixture.get("params").cloned().unwrap_or_else(|| json!({}));
        let needed = extract_collection_names(&source);

        let run_once = || -> Result<(usize, String, bool), OracleError> {
            let db = load_case_fixture(&root, generator_id, seed, &params_json, &needed)
                .map_err(OracleError::Fixture)?;
            let run_params = parameters_for_source(&source);
            let (_plan, res) = oracle_eval_source(&db, &source, &run_params)?;
            let digest = result_digest(&res, ordered);
            Ok((res.rows.len(), digest, res.coverage_complete))
        };

        // Determinism: two independent materialise+eval runs must agree.
        let first = run_once();
        let second = run_once();

        let entry = match (first, second) {
            (Ok((n1, d1, cov1)), Ok((n2, d2, cov2))) => {
                if n1 != n2 || d1 != d2 || cov1 != cov2 {
                    digest_mismatch += 1;
                    json!({
                        "case_id": case_id,
                        "domain": domain,
                        "status": "digest_mismatch",
                        "row_count_a": n1,
                        "row_count_b": n2,
                        "digest_a": d1,
                        "digest_b": d2,
                    })
                } else {
                    oracle_ok += 1;
                    json!({
                        "case_id": case_id,
                        "domain": domain,
                        "status": "oracle_ok",
                        "row_count": n1,
                        "digest": d1,
                        "coverage_complete": cov1,
                        "ordered": ordered,
                        "source": source,
                    })
                }
            }
            (Err(OracleError::Unsupported(msg)), _) | (_, Err(OracleError::Unsupported(msg))) => {
                oracle_unsupported += 1;
                json!({
                    "case_id": case_id,
                    "domain": domain,
                    "status": "oracle_unsupported",
                    "reason": msg,
                    "source": source,
                })
            }
            (Err(OracleError::Compile(msg)), _) | (_, Err(OracleError::Compile(msg))) => {
                oracle_compile_fail += 1;
                json!({
                    "case_id": case_id,
                    "domain": domain,
                    "status": "oracle_compile_fail",
                    "reason": msg,
                    "source": source,
                })
            }
            (Err(OracleError::Eval(msg)), _) | (_, Err(OracleError::Eval(msg))) => {
                oracle_eval_fail += 1;
                json!({
                    "case_id": case_id,
                    "domain": domain,
                    "status": "oracle_eval_fail",
                    "reason": msg,
                    "source": source,
                })
            }
            (Err(OracleError::Fixture(msg)), _) | (_, Err(OracleError::Fixture(msg))) => {
                oracle_fixture_fail += 1;
                json!({
                    "case_id": case_id,
                    "domain": domain,
                    "status": "oracle_fixture_fail",
                    "reason": msg,
                    "source": source,
                })
            }
        };
        case_results.push(entry);
    }

    let qualification_green = oracle_ok + stable_refusal_ok + explain_contract_ok;
    let summary = json!({
        "format": "residiuum-rql-q3-1-oracle-report-v1",
        "oracle_profile": ORACLE_PROFILE,
        "corpus_version": corpus_version,
        "authority": "RQL_QUERY_QUALIFICATION_PROGRAM.md §6.1",
        "boundary": {
            "product_callable": false,
            "uses_index_selection": false,
            "uses_collection_client_rql": false,
            "uses_execute_rql_full": false,
            "uses_qvm_run_vm": false,
            "uses_compile_app_core_for_plan_structure": true,
            "uses_predicate_eval_total_profile": true,
            "fixture_kind": "logical_generator_seed",
        },
        "summary": {
            "tier_a_total": tier_a_total,
            "qualification_green": qualification_green,
            "qualification_residual": tier_a_total.saturating_sub(qualification_green),
            "package_exit_ready": qualification_green == tier_a_total,
            "tier_a_semantic_source": tier_a_considered,
            "admitted_deferred_families": [
                "group_aggregate",
                "projection_computed_conditional",
                "enrichment_cardinality"
            ],
            "oracle_ok": oracle_ok,
            "stable_refusal_ok": stable_refusal_ok,
            "explain_contract_ok": explain_contract_ok,
            "non_row_fail": non_row_fail,
            "oracle_unsupported": oracle_unsupported,
            "oracle_compile_fail": oracle_compile_fail,
            "oracle_eval_fail": oracle_eval_fail,
            "oracle_fixture_fail": oracle_fixture_fail,
            "digest_mismatch": digest_mismatch,
            "skipped_non_oracle_rule_tier_a": skipped_non_oracle_rule,
            "skipped_no_source": skipped_no_source,
        },
        "cases": case_results,
    });

    // F8: default → target/ only; set RESIDIUUM_WRITE_SPEC_EVIDENCE=1 to publish spec/.
    let report_path = rql_evidence_write::write_q3_report(
        &root,
        "q3_1_oracle_report.json",
        &serde_json::to_string_pretty(&summary).expect("report json"),
    );

    eprintln!(
        "rql_q3_1: qualification_green={qualification_green} oracle_ok={oracle_ok} stable_refusal_ok={stable_refusal_ok} explain_contract_ok={explain_contract_ok} unsupported={oracle_unsupported} compile_fail={oracle_compile_fail} eval_fail={oracle_eval_fail} fixture_fail={oracle_fixture_fail} digest_mismatch={digest_mismatch} considered={tier_a_considered}"
    );
    eprintln!("report: {}", report_path.display());

    assert_eq!(digest_mismatch, 0, "oracle digests must be deterministic");
    assert_eq!(
        oracle_eval_fail, 0,
        "oracle eval must not error on Core cases"
    );
    assert_eq!(oracle_fixture_fail, 0, "fixtures must materialise");
    assert_eq!(non_row_fail, 0, "non-row contracts must qualify");
    // Floor: majority of Tier-A oracle_rule sources must be oracle-green.
    // Residual unsupported (after/enrich/…) is explicit, not silent skip.
    assert!(
        oracle_ok >= 90,
        "expected ≥90 oracle_ok cases, got {oracle_ok} (unsupported={oracle_unsupported} compile_fail={oracle_compile_fail})"
    );
    assert_eq!(
        oracle_ok + oracle_unsupported + oracle_compile_fail,
        tier_a_considered,
        "every considered case must be classified"
    );
}
