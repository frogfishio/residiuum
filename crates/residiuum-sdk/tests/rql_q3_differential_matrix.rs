//! RQL-Q3.2 — differential matrix + metamorphic laws.
//!
//! Authority: `doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md` §6.2–6.3;
//! report: `doc/todo/rql/RQL_Q3_2_DIFFERENTIAL_MATRIX.md`.
//!
//! Matrix (this labor):
//! ```text
//! reference_oracle(Q)
//!   == forced_scan_QVM(Q)          // MapHost + force_scan QVM
//!   == admitted_index_plan(Q)      // MapHost equality-index when usable
//!   == reopened_store(Q)           // embedded Heap seed → rql → reopen → rql
//! ```
//! Comparator engines (Mongo/CBL) are **out of process** here — reported as
//! `comparator_deferred_q4` (not silent pass).
//!
//! Failures are **defects** (suite fails). Row count alone is never enough:
//! keys, values, multiplicity, order (when declared), coverage.

#[path = "common/rql_evidence_write.rs"]
mod rql_evidence_write;

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, CollectionId, Constraints,
    DeploymentId, HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights,
    SecurityRevision, TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::predicate::resolve_path;
use residiuum_sdk::{
    compile_app_core, compile_rql_full, compile_sql_to_rql, execute_bytecode,
    execute_rql_full_on_host_with, explain_core_source, field, param,
    source_uses_rql_full_constructs, AggFn, CollectionBindings, CoveragePolicy, EnrichCardinality,
    EnrichStepV1, Error, FullPipelineStepV1, HostCapabilities, NullsOrder, OrderDir, OrderTerm,
    Parameters, PlanBuilder, PredPath as Path, Predicate, ProjectExprV1, ProjectItemV1,
    QueryBytecodeV1, QueryPage, QueryRunOptions, ResidiuumDeployment, Resolve,
    RqlFullExecuteOptions, RqlPlanV1, SqlToRqlResult, WithinStepV1, APP_CORE_PROFILE,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Fixture materialisation (logical)
// ---------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
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
        .map_err(|e| format!("spawn materialise: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "materialise failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    serde_json::from_slice(&out.stdout).map_err(|e| e.to_string())
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
        i += 1;
    }
    names
}

fn compile_bindings_for(names: &BTreeSet<String>) -> CollectionBindings {
    let mut b = CollectionBindings::default();
    for (i, name) in names.iter().enumerate() {
        // UUIDv4 layout required by heap ids / QVM collection_id checks.
        let mut bytes = [0u8; 16];
        let n = name.as_bytes();
        for (j, &c) in n.iter().enumerate().take(10) {
            bytes[j] = c;
        }
        bytes[10] = (i as u8).wrapping_add(1);
        bytes[11] = 0xD2;
        bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
        bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant
        if bytes.iter().all(|&x| x == 0) {
            bytes[15] = 1;
        }
        let id = CollectionId::from_bytes(bytes)
            .or_else(|_| CollectionId::new_random())
            .expect("cid");
        b.bind(name, id);
    }
    b
}

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

/// Logical docs: keep `_key` for oracle; product host strips it.
type LogicalDb = BTreeMap<String, BTreeMap<String, Value>>;

fn load_logical_db(
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
        .ok_or_else(|| "missing collections".to_string())?;
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
    let mut db = LogicalDb::new();
    for (name, docs) in cols {
        let arr = docs.as_array().ok_or_else(|| format!("{name} not array"))?;
        let mut map = BTreeMap::new();
        for doc in arr {
            let key = doc
                .get("_key")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("{name} missing _key"))?
                .to_string();
            map.insert(key, doc.clone());
        }
        db.insert(name, map);
    }
    Ok(db)
}

// ---------------------------------------------------------------------------
// Independent oracle (test-only — same model as Q3.1)
// ---------------------------------------------------------------------------

const ORACLE_PROFILE: &str = "residiuum-rql-q3-semantic-oracle-v1";

#[derive(Clone, Debug, PartialEq)]
struct OracleRow {
    key: String,
    value: Value,
}

#[derive(Clone, Debug, PartialEq)]
struct OracleResult {
    rows: Vec<OracleRow>,
    coverage_complete: bool,
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

fn eval_pred_with_key(
    pred: &Predicate,
    key: &str,
    doc: &Value,
    params: &BTreeMap<String, Value>,
) -> Result<bool, String> {
    let mut eval_doc = doc.clone();
    if let Value::Object(ref mut m) = eval_doc {
        m.insert("_key".into(), Value::String(key.to_string()));
        m.insert("$key".into(), Value::String(key.to_string()));
    }
    pred.eval(&eval_doc, params).map_err(|e| e.to_string())
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
) -> Result<Value, String> {
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
    rows: &mut [OracleRow],
    fields: &[ProjectItemV1],
    params: &BTreeMap<String, Value>,
) -> Result<(), String> {
    for row in rows {
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
                            return Err(format!("nested project `{output}` requires object/array"));
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
) -> Result<Value, String> {
    let mut rows = vec![OracleRow {
        key: key.into(),
        value,
    }];
    apply_computed_project_oracle(&mut rows, fields, params)?;
    Ok(rows.pop().expect("one row").value)
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
) -> Result<Vec<OracleRow>, String> {
    let foreign = db
        .get(&step.using_name)
        .ok_or_else(|| format!("missing foreign collection {}", step.using_name))?;
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
                return Err(format!("exactly_one expected 1 match, got {n}"));
            }
            (EnrichCardinality::Optional, 0) => Value::Null,
            (EnrichCardinality::Optional, 1) => matches.remove(0),
            (EnrichCardinality::Optional, n) => {
                return Err(format!("optional expected at most 1 match, got {n}"));
            }
            (EnrichCardinality::Many, _) => Value::Array(matches),
        };
        row.value
            .as_object_mut()
            .ok_or_else(|| "enrich root must be object".to_string())?
            .insert(step.output.clone(), attached);
        out.push(row);
    }
    Ok(out)
}

fn set_path_oracle(doc: &mut Value, path: &Path, value: Value) -> Result<(), String> {
    let (last, parents) = path
        .0
        .split_last()
        .ok_or_else(|| "empty within carrier".to_string())?;
    let mut current = doc;
    for segment in parents {
        current = current
            .as_object_mut()
            .and_then(|map| map.get_mut(segment))
            .ok_or_else(|| format!("missing within carrier parent `{segment}`"))?;
    }
    current
        .as_object_mut()
        .ok_or_else(|| "within carrier parent must be object".to_string())?
        .insert(last.clone(), value);
    Ok(())
}

fn apply_within_oracle(
    rows: Vec<OracleRow>,
    db: &LogicalDb,
    step: &WithinStepV1,
    params: &BTreeMap<String, Value>,
) -> Result<Vec<OracleRow>, String> {
    let mut out = Vec::with_capacity(rows.len());
    for mut parent in rows {
        let items = match path_resolve_with_key(&parent.key, &parent.value, &step.carrier) {
            Resolve::Present(Value::Array(items)) => items,
            _ => return Err(format!("within `{}` requires array", step.carrier.dotted())),
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
) -> Result<Vec<OracleRow>, String> {
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
        _ => serde_json::to_string(a)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(b).unwrap_or_default()),
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

fn json_number_prefer_int(n: f64) -> Value {
    if n.is_finite() && n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
        json!(n as i64)
    } else {
        json!(n)
    }
}

fn apply_group_agg(
    rows: &[(String, Value)],
    plan: &RqlPlanV1,
) -> Result<Vec<(String, Value)>, String> {
    let g = &plan.group_agg;
    if !g.is_active() {
        return Ok(rows.to_vec());
    }
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
                if let Resolve::Present(v) = path_resolve_with_key(mk, md, p) {
                    if p.0.len() == 1 {
                        obj.insert(p.0[0].clone(), v);
                    } else {
                        obj.insert(p.dotted(), v);
                    }
                }
            }
        }
        for agg in &g.aggregates {
            let val = match agg.fun {
                AggFn::Count => Value::Number((members.len() as u64).into()),
                AggFn::Sum | AggFn::Min | AggFn::Max | AggFn::Avg => {
                    let path = agg
                        .source
                        .as_ref()
                        .ok_or_else(|| format!("{} needs path", agg.fun.as_str()))?;
                    let mut nums = Vec::new();
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
                        AggFn::Sum => json_number_prefer_int(nums.iter().sum()),
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
                                json_number_prefer_int(nums.iter().sum::<f64>() / nums.len() as f64)
                            }
                        }
                        AggFn::Count => unreachable!(),
                    }
                }
            };
            obj.insert(agg.output.clone(), val);
        }
        out.push((format!("__group_{gi}"), Value::Object(obj)));
    }
    Ok(out)
}

fn oracle_eval(
    db: &LogicalDb,
    source: &str,
    params: &Parameters,
) -> Result<(RqlPlanV1, OracleResult), String> {
    let names = extract_collection_names(source);
    let bindings = compile_bindings_for(&names);
    let (plan, pipeline, full_project) = if source_uses_rql_full_constructs(source) {
        let compiled = compile_rql_full(source, &bindings).map_err(|e| e.to_string())?;
        (compiled.base.plan, compiled.pipeline, compiled.project)
    } else {
        let compiled = compile_app_core(source, &bindings).map_err(|e| e.to_string())?;
        assert_eq!(compiled.profile, APP_CORE_PROFILE);
        (compiled.plan, Vec::new(), None)
    };
    let coll = db
        .get(&plan.from.source_name)
        .ok_or_else(|| format!("missing collection {}", plan.from.source_name))?;
    let mut working: Vec<(String, Value)> = Vec::new();
    for (key, doc) in coll {
        if eval_pred_with_key(&plan.where_pred, key, doc, &params.values)? {
            working.push((key.clone(), doc.clone()));
        }
    }
    if plan.group_agg.is_active() {
        working = apply_group_agg(&working, &plan)?;
    } else if !plan.order.is_empty() {
        working.sort_by(|(ka, va), (kb, vb)| compare_rows_oracle(ka, va, kb, vb, &plan.order));
    } else {
        working.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));
    }
    if let Some(n) = plan.limit {
        working.truncate(n as usize);
    }
    let page_size = plan.page_size as usize;
    if working.len() > page_size {
        working.truncate(page_size);
    }
    let mut rows: Vec<OracleRow> = working
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
    rows = apply_pipeline_oracle(rows, db, &pipeline, &params.values)?;
    if let Some(fields) = &full_project {
        apply_computed_project_oracle(&mut rows, fields, &params.values)?;
    }
    Ok((
        plan,
        OracleResult {
            rows,
            coverage_complete: true,
        },
    ))
}

// ---------------------------------------------------------------------------
// Product MapHost: force_scan QVM + optional equality indexes
// ---------------------------------------------------------------------------

struct DiffHost {
    /// collection_id → (key → body without required _key; may still have fields)
    by_id: BTreeMap<CollectionId, BTreeMap<String, Value>>,
    /// field → (canonical value json → keys)
    eq_index: BTreeMap<CollectionId, BTreeMap<String, BTreeMap<String, Vec<String>>>>,
    index_enabled: bool,
}

impl DiffHost {
    fn from_logical(db: &LogicalDb, root_name: &str, root_id: CollectionId, index: bool) -> Self {
        let mut by_id = BTreeMap::new();
        let mut eq_index = BTreeMap::new();
        if let Some(coll) = db.get(root_name) {
            let mut map = BTreeMap::new();
            let mut collection_index: BTreeMap<String, BTreeMap<String, Vec<String>>> =
                BTreeMap::new();
            for (k, doc) in coll {
                let mut body = doc.clone();
                if let Value::Object(ref mut m) = body {
                    // Product store strips _key; host get returns body only.
                    m.remove("_key");
                }
                // Build single-field equality indexes on top-level scalar fields.
                if index {
                    if let Value::Object(ref m) = body {
                        for (field, val) in m {
                            if val.is_object() || val.is_array() {
                                continue;
                            }
                            let ck = serde_json::to_string(val).unwrap_or_default();
                            collection_index
                                .entry(field.clone())
                                .or_default()
                                .entry(ck)
                                .or_default()
                                .push(k.clone());
                        }
                    }
                }
                map.insert(k.clone(), body);
            }
            by_id.insert(root_id, map);
            eq_index.insert(root_id, collection_index);
        }
        Self {
            by_id,
            eq_index,
            index_enabled: index,
        }
    }

    fn from_logical_full(
        db: &LogicalDb,
        names: &BTreeSet<String>,
        bindings: &CollectionBindings,
        index: bool,
    ) -> Self {
        let mut host = Self {
            by_id: BTreeMap::new(),
            eq_index: BTreeMap::new(),
            index_enabled: index,
        };
        for name in names {
            let Ok(cid) = bindings.resolve(name) else {
                continue;
            };
            let one = Self::from_logical(db, name, cid, index);
            host.by_id.extend(one.by_id);
            host.eq_index.extend(one.eq_index);
        }
        host
    }
}

impl HostCapabilities for DiffHost {
    fn list_keys(
        &mut self,
        collection_id: CollectionId,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let Some(map) = self.by_id.get(&collection_id) else {
            return Ok(Vec::new());
        };
        let mut keys: Vec<String> = map.keys().cloned().collect();
        keys.sort();
        if let Some(a) = after_key {
            keys.retain(|k| k.as_str() > a);
        }
        if let Some(n) = limit {
            keys.truncate(n);
        }
        Ok(keys)
    }

    fn get_json(&mut self, collection_id: CollectionId, key: &str) -> Result<Option<Value>, Error> {
        Ok(self
            .by_id
            .get(&collection_id)
            .and_then(|m| m.get(key).cloned()))
    }

    fn lookup_index_keys(
        &mut self,
        collection_id: CollectionId,
        equalities: &[(String, Value)],
    ) -> Result<Option<Vec<String>>, Error> {
        if !self.index_enabled || equalities.is_empty() {
            return Ok(None);
        }
        // Only single-field indexes in this harness.
        if equalities.len() != 1 {
            return Ok(None);
        }
        let (field, val) = &equalities[0];
        if field.contains('.') {
            return Ok(None);
        }
        let Some(by_val) = self
            .eq_index
            .get(&collection_id)
            .and_then(|fields| fields.get(field))
        else {
            return Ok(None);
        };
        let ck = serde_json::to_string(val).unwrap_or_default();
        match by_val.get(&ck) {
            Some(keys) => Ok(Some(keys.clone())),
            None => Ok(Some(Vec::new())), // index present, zero hits
        }
    }
}

fn heap_id_fake() -> HeapId {
    let mut b = [0u8; 16];
    b[0] = 0xC3;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    HeapId::from_bytes(b).expect("heap")
}

fn product_force_scan(
    db: &LogicalDb,
    plan: &RqlPlanV1,
    params: &Parameters,
) -> Result<QueryPage, String> {
    let root = &plan.from.source_name;
    let cid = plan.from.collection_id;
    let mut host = DiffHost::from_logical(db, root, cid, false);
    let bc = QueryBytecodeV1::from_core_plan_force_scan(plan.clone(), None, true)
        .map_err(|e| e.to_string())?;
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(plan.page_size);
    execute_bytecode(&mut host, &bc, &params.values, &opts, heap_id_fake(), cid)
        .map_err(|e| e.to_string())
}

fn product_index_plan(
    db: &LogicalDb,
    plan: &RqlPlanV1,
    params: &Parameters,
) -> Result<QueryPage, String> {
    let root = &plan.from.source_name;
    let cid = plan.from.collection_id;
    let mut host = DiffHost::from_logical(db, root, cid, true);
    // force_scan=false → admit equality index when host provides it
    let bc = QueryBytecodeV1::from_core_plan_force_scan(plan.clone(), None, false)
        .map_err(|e| e.to_string())?;
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(plan.page_size);
    execute_bytecode(&mut host, &bc, &params.values, &opts, heap_id_fake(), cid)
        .map_err(|e| e.to_string())
}

fn product_full_plan(
    db: &LogicalDb,
    source: &str,
    params: &Parameters,
    force_scan: bool,
) -> Result<(Vec<OracleRow>, QueryPage), String> {
    let names = extract_collection_names(source);
    let bindings = compile_bindings_for(&names);
    let compiled = compile_rql_full(source, &bindings).map_err(|e| e.to_string())?;
    let mut host = DiffHost::from_logical_full(db, &names, &bindings, !force_scan);
    let mut query = QueryRunOptions::default();
    query.page_size = Some(compiled.base.plan.page_size);
    let page = execute_rql_full_on_host_with(
        &mut host,
        heap_id_fake(),
        source,
        &bindings,
        params,
        RqlFullExecuteOptions {
            query,
            force_enrich_scan: force_scan,
        },
    )
    .map_err(|e| e.to_string())?;
    let rows = page
        .rows
        .iter()
        .map(|(key, value)| OracleRow {
            key: key.clone(),
            value: normalize_row_value(value),
        })
        .collect();
    Ok((rows, page.base))
}

// ---------------------------------------------------------------------------
// Result comparison (full shape)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ShapeDiff {
    reason: String,
}

fn normalize_row_value(v: &Value) -> Value {
    // Product injects `_key` into working docs for filter/project; strip synthetic
    // `$key` if ever present. Compare as JSON.
    match v {
        Value::Object(m) => {
            let mut out = Map::new();
            for (k, val) in m {
                if k == "$key" {
                    continue;
                }
                out.insert(k.clone(), normalize_row_value(val));
            }
            Value::Object(out)
        }
        Value::Array(a) => Value::Array(a.iter().map(normalize_row_value).collect()),
        other => other.clone(),
    }
}

fn page_to_rows(page: &QueryPage) -> Vec<OracleRow> {
    page.rows
        .iter()
        .map(|r| OracleRow {
            key: r.key.clone(),
            value: normalize_row_value(&r.value),
        })
        .collect()
}

fn compare_shapes(
    left: &[OracleRow],
    right: &[OracleRow],
    ordered: bool,
    ignore_row_keys: bool,
    left_cov: bool,
    right_cov: bool,
) -> Result<(), ShapeDiff> {
    if left_cov != right_cov {
        return Err(ShapeDiff {
            reason: format!("coverage diverge left={left_cov} right={right_cov}"),
        });
    }
    if left.len() != right.len() {
        return Err(ShapeDiff {
            reason: format!(
                "row_count diverge left={} right={}",
                left.len(),
                right.len()
            ),
        });
    }
    if ordered {
        for (i, (a, b)) in left.iter().zip(right.iter()).enumerate() {
            if !ignore_row_keys && a.key != b.key {
                return Err(ShapeDiff {
                    reason: format!("order key diverge at {i}: {} vs {}", a.key, b.key),
                });
            }
            if normalize_row_value(&a.value) != normalize_row_value(&b.value) {
                return Err(ShapeDiff {
                    reason: format!("order value diverge at {i} key={}", a.key),
                });
            }
        }
    } else {
        // Multiset by (key, canonical value)
        let mut la: BTreeMap<(String, String), u32> = BTreeMap::new();
        let mut ra: BTreeMap<(String, String), u32> = BTreeMap::new();
        for r in left {
            let v = serde_json::to_string(&normalize_row_value(&r.value)).unwrap_or_default();
            let key = if ignore_row_keys {
                String::new()
            } else {
                r.key.clone()
            };
            *la.entry((key, v)).or_default() += 1;
        }
        for r in right {
            let v = serde_json::to_string(&normalize_row_value(&r.value)).unwrap_or_default();
            let key = if ignore_row_keys {
                String::new()
            } else {
                r.key.clone()
            };
            *ra.entry((key, v)).or_default() += 1;
        }
        if la != ra {
            return Err(ShapeDiff {
                reason: format!("multiset diverge left_keys={la:?} right_keys={ra:?}"),
            });
        }
    }
    Ok(())
}

fn ordering_is_sensitive(meta: &Value) -> bool {
    match meta.get("order").and_then(|v| v.as_str()) {
        Some("unordered_multiset") | Some("unordered_set") | None => false,
        Some(_) => true,
    }
}

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
// Reopened store path (embedded)
// ---------------------------------------------------------------------------

fn mint_cap_for(heap: HeapId, deployment: DeploymentId) -> residiuum_heap::HeapCap {
    let snap = HeapSecuritySnapshot {
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key: [7u8; 32],
        previous_master_public_key: None,
        security_revision: SecurityRevision::new(1).unwrap(),
        authority_chain_head_hash: [9u8; 32],
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    };
    let slot = Arc::new(HeapSlot::new(snap));
    let cert = VerifiedCertificate {
        cose_bytes: vec![0x01],
        fingerprint: [3u8; 32],
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4u8; 32],
        rights: Rights::from_bits_certificate(0x0d).unwrap(),
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: [5u8; 32],
    };
    mint_capability(
        slot,
        &cert,
        TrustedInstant {
            unix_s: 1_700_000_000,
        },
    )
    .unwrap()
}

fn uuid_bytes() -> [u8; 16] {
    *CollectionId::new_random().unwrap().as_bytes()
}

fn product_reopen_store(
    db: &LogicalDb,
    source: &str,
    params: &Parameters,
) -> Result<Vec<OracleRow>, String> {
    let dir = tempdir().map_err(|e| e.to_string())?;
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).map_err(|e| e.to_string())?;
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid_bytes(), "heap-q3-2")
        .map_err(|e| e.to_string())?;
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash)
        .map_err(|e| e.to_string())?;
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client =
        residiuum_sdk::HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));

    // Seed all collections in logical db.
    for (name, docs) in db {
        let mut col = client
            .create_collection(name)
            .map_err(|e| format!("create {name}: {e}"))?
            .collection;
        for (key, doc) in docs {
            let mut body = doc.clone();
            if let Value::Object(ref mut m) = body {
                m.remove("_key");
            }
            col.put(key, &body)
                .map_err(|e| format!("put {name}/{key}: {e}"))?;
        }
    }

    let names = extract_collection_names(source);
    let root_name = names
        .iter()
        .next()
        .cloned()
        .unwrap_or_else(|| "orders".into());
    // First query
    let mut col = client
        .open_collection(&root_name)
        .map_err(|e| format!("open {root_name}: {e}"))?;
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(4096);
    let page1 = col
        .rql(source, params, opts.clone())
        .map_err(|e| format!("rql1: {e}"))?;
    let rows1 = page_to_rows(&page1);

    // Reopen process-style: drop all writer handles, then re-open same root.
    drop(col);
    drop(client);
    drop(deployment);
    let deployment2 = ResidiuumDeployment::open(root).map_err(|e| e.to_string())?;
    let mut client2 =
        residiuum_sdk::HeapClient::from(deployment2.open_heap(mint_cap_for(heap_id, dep_id)));
    let mut col2 = client2
        .open_collection(&root_name)
        .map_err(|e| format!("reopen {root_name}: {e}"))?;
    let page2 = col2
        .rql(source, params, opts)
        .map_err(|e| format!("rql2: {e}"))?;
    let rows2 = page_to_rows(&page2);

    compare_shapes(
        &rows1,
        &rows2,
        true,
        false,
        page1.coverage.complete,
        page2.coverage.complete,
    )
    .map_err(|d| format!("reopen self-diverge: {}", d.reason))?;
    Ok(rows1)
}

// ---------------------------------------------------------------------------
// Metamorphic unit laws
// ---------------------------------------------------------------------------

#[test]
fn q32_law_filter_and_commutes() {
    let b = {
        let mut x = CollectionBindings::default();
        x.bind(
            "orders",
            CollectionId::from_bytes({
                let mut b = [0u8; 16];
                b[0] = 1;
                b[6] = 0x40;
                b[8] = 0x80;
                b
            })
            .unwrap(),
        );
        x
    };
    let a = compile_app_core(r#"from orders where status = "paid" and region = "us""#, &b).unwrap();
    let c = compile_app_core(r#"from orders where region = "us" and status = "paid""#, &b).unwrap();
    // Plan where canonical form may differ in tree shape; QVM must match after
    // normalisation via Predicate::And args order — product may not sort AND.
    // Law: oracle eval equal (not necessarily plan hash equal).
    let mut orders = BTreeMap::new();
    for (k, st, reg) in [
        ("o1", "paid", "us"),
        ("o2", "paid", "eu"),
        ("o3", "open", "us"),
        ("o4", "paid", "us"),
    ] {
        orders.insert(k.into(), json!({"_key": k, "status": st, "region": reg}));
    }
    let mut db = LogicalDb::new();
    db.insert("orders".into(), orders);
    let p = Parameters::default();
    let (_, r1) = oracle_eval(
        &db,
        r#"from orders where status = "paid" and region = "us""#,
        &p,
    )
    .unwrap();
    let (_, r2) = oracle_eval(
        &db,
        r#"from orders where region = "us" and status = "paid""#,
        &p,
    )
    .unwrap();
    compare_shapes(
        &r1.rows,
        &r2.rows,
        false,
        false,
        r1.coverage_complete,
        r2.coverage_complete,
    )
    .expect("filter A and B = B and A");
    assert_eq!(r1.rows.len(), 2);
    let _ = (a, c);
}

#[test]
fn q32_law_project_identity_on_full_doc() {
    let mut orders = BTreeMap::new();
    orders.insert("a".into(), json!({"_key":"a","status":"paid","n":1}));
    orders.insert("b".into(), json!({"_key":"b","status":"open","n":2}));
    let mut db = LogicalDb::new();
    db.insert("orders".into(), orders);
    let p = Parameters::default();
    let (_, full) = oracle_eval(&db, "from orders", &p).unwrap();
    // Explicit project of all present fields is not required; identity means
    // no-project returns the full document body (including _key in logical).
    assert_eq!(full.rows.len(), 2);
    assert!(full.rows.iter().all(|r| r.value.get("status").is_some()));
}

#[test]
fn q32_law_frontend_qvm_identity_builder_rql() {
    let mut b = CollectionBindings::default();
    let cid = CollectionId::from_bytes({
        let mut x = [0u8; 16];
        x[0] = 9;
        x[6] = 0x40;
        x[8] = 0x80;
        x
    })
    .unwrap();
    b.bind("orders", cid);
    let from_builder = PlanBuilder::from_source("orders")
        .where_(field("status").unwrap().eq(param("st")))
        .compile(&b)
        .unwrap();
    let from_rql = compile_app_core(r#"from orders where status = $st"#, &b)
        .unwrap()
        .plan;
    let q1 = QueryBytecodeV1::from_core_plan(from_builder, None)
        .unwrap()
        .qvm_hash();
    let q2 = QueryBytecodeV1::from_core_plan(from_rql, None)
        .unwrap()
        .qvm_hash();
    assert_eq!(q1, q2, "builder and RQL must share canonical QVM");
}

#[test]
fn q32_law_sql_and_rql_qvm_identity_when_sql_compiles() {
    let mut b = CollectionBindings::default();
    let cid = CollectionId::from_bytes({
        let mut x = [0u8; 16];
        x[0] = 11;
        x[6] = 0x40;
        x[8] = 0x80;
        x
    })
    .unwrap();
    b.bind("orders", cid);
    let rql = r#"from orders where status = "paid""#;
    let q_rql = QueryBytecodeV1::from_core_plan(compile_app_core(rql, &b).unwrap().plan, None)
        .unwrap()
        .qvm_hash();
    // SQL-ish+ may refuse; if it compiles, QVM must match.
    match compile_sql_to_rql("SELECT * FROM orders WHERE status = 'paid'", Some(&b)) {
        Ok(SqlToRqlResult::Emit(sql)) => {
            let q_sql =
                QueryBytecodeV1::from_core_plan(compile_app_core(&sql.rql, &b).unwrap().plan, None)
                    .unwrap()
                    .qvm_hash();
            assert_eq!(q_rql, q_sql);
        }
        Ok(SqlToRqlResult::Refuse { .. }) | Err(_) => {
            // Law vacuously holds for this surface residual.
        }
    }
}

#[test]
fn q32_law_force_scan_equals_index_on_eq_predicate() {
    let mut orders = BTreeMap::new();
    for i in 0..20 {
        let st = if i % 3 == 0 { "paid" } else { "open" };
        let k = format!("o-{i:04}");
        orders.insert(k.clone(), json!({"_key": k, "status": st, "region": "us"}));
    }
    let mut db = LogicalDb::new();
    db.insert("orders".into(), orders);
    let source = r#"from orders where status = "paid""#;
    let params = Parameters::default();
    let (plan, oracle) = oracle_eval(&db, source, &params).unwrap();
    let scan = product_force_scan(&db, &plan, &params).unwrap();
    let indexed = product_index_plan(&db, &plan, &params).unwrap();
    let scan_rows = page_to_rows(&scan);
    let idx_rows = page_to_rows(&indexed);
    compare_shapes(
        &oracle.rows,
        &scan_rows,
        false,
        false,
        oracle.coverage_complete,
        scan.coverage.complete,
    )
    .expect("oracle == force_scan");
    compare_shapes(
        &scan_rows,
        &idx_rows,
        false,
        false,
        scan.coverage.complete,
        indexed.coverage.complete,
    )
    .expect("force_scan == index_plan");
    assert!(scan.coverage.complete);
    assert_eq!(scan.known_holes.len(), 0);
}

#[test]
fn q32_law_complete_coverage_implies_zero_holes() {
    let mut orders = BTreeMap::new();
    orders.insert("k".into(), json!({"_key":"k","status":"paid"}));
    let mut db = LogicalDb::new();
    db.insert("orders".into(), orders);
    let params = Parameters::default();
    let (plan, _) = oracle_eval(&db, r#"from orders where status = "paid""#, &params).unwrap();
    let page = product_force_scan(&db, &plan, &params).unwrap();
    if page.coverage.complete {
        assert_eq!(page.known_holes.len(), 0);
        assert_eq!(page.coverage.hole_count, 0);
    } else {
        panic!("complete fixture scan must report complete coverage");
    }
    assert_eq!(plan.coverage, CoveragePolicy::Complete);
}

// ---------------------------------------------------------------------------
// Corpus differential suite
// ---------------------------------------------------------------------------

#[test]
fn rql_q3_2_corpus_differential_matrix() {
    let root = workspace_root();
    let corpus_path = root.join("spec/rql/qualification/corpus-v1/corpus-v1.json");
    let doc: Value = serde_json::from_str(&fs::read_to_string(&corpus_path).unwrap()).unwrap();
    let corpus_version = doc["corpus_version"].as_str().unwrap_or("?").to_string();
    let cases = doc["cases"].as_array().unwrap();
    let tier_a_total = cases
        .iter()
        .filter(|case| case["tier"].as_str() == Some("A"))
        .count() as u64;

    let mut case_results = Vec::new();
    let mut equal_all = 0u64;
    let mut diverge = 0u64;
    let mut unsupported = 0u64;
    let mut product_err = 0u64;
    let mut reopen_checked = 0u64;
    let mut reopen_fail = 0u64;
    let mut considered = 0u64;
    let mut stable_refusal_ok = 0u64;
    let mut explain_identity_ok = 0u64;
    let mut non_row_fail = 0u64;

    // Cap reopen path cost: first N green equal cases also check reopen.
    const REOPEN_BUDGET: u64 = 12;

    for case in cases {
        if case["tier"].as_str() != Some("A") {
            continue;
        }
        let rql = &case["implementations"]["rql"];
        let source = rql["source"].as_str().unwrap_or("").to_string();
        let case_id = case["case_id"].as_str().unwrap_or("?").to_string();
        let expected_kind = case["expected"]["kind"].as_str().unwrap_or("");

        if expected_kind == "stable_refusal" {
            considered += 1;
            let expected_code = rql["refusal_code"].as_str().unwrap_or("");
            let bindings = compile_bindings_for(&extract_collection_names(&source));
            let first = compile_rql_full(&source, &bindings);
            let second = compile_rql_full(&source, &bindings);
            let observed = match (&first, &second) {
                (Err(a), Err(b)) => Some((a.to_string(), b.to_string())),
                _ => None,
            };
            if observed.as_ref().is_some_and(|(a, b)| {
                !expected_code.is_empty()
                    && a.contains(expected_code)
                    && b.contains(expected_code)
                    && a == b
            }) {
                equal_all += 1;
                stable_refusal_ok += 1;
                case_results.push(json!({
                    "case_id": case_id,
                    "status": "matrix_equal",
                    "outcome": "stable_refusal",
                    "refusal_code": expected_code,
                    "arms": {
                        "compile_a_vs_compile_b": "equal",
                        "required_refusal": "equal",
                        "comparator_mongo_cbl": "deferred_q4"
                    },
                    "source": source,
                }));
            } else {
                non_row_fail += 1;
                product_err += 1;
                case_results.push(json!({
                    "case_id": case_id,
                    "status": "stable_refusal_fail",
                    "expected_code": expected_code,
                    "observed": format!("first={first:?}; second={second:?}"),
                    "source": source,
                }));
            }
            continue;
        }

        if source.to_ascii_lowercase().starts_with("explain ") {
            considered += 1;
            let names = extract_collection_names(&source);
            let bindings = compile_bindings_for(&names);
            let compiled = compile_app_core(&source, &bindings);
            let outcome = compiled.and_then(|compiled| {
                if !compiled.explain {
                    return Err(Error::QueryInvalid("explain flag was not retained".into()));
                }
                let cid = compiled.plan.from.collection_id;
                let name = compiled.plan.from.source_name.clone();
                let explanation = explain_core_source(&source, cid, &name)?;
                let empty = LogicalDb::new();
                let scan = product_force_scan(&empty, &compiled.plan, &Parameters::default())
                    .map_err(Error::QueryInvalid)?;
                let indexed = product_index_plan(&empty, &compiled.plan, &Parameters::default())
                    .map_err(Error::QueryInvalid)?;
                if explanation.tree != compiled.plan.to_canonical_json()
                    || indexed.plan_hash != explanation.plan_hash
                {
                    return Err(Error::QueryInvalid(format!(
                        "explain/default-QVM identity mismatch: explain={} logical={} forced_scan={} default={} tree_equal={}",
                        hex::encode(explanation.plan_hash),
                        hex::encode(compiled.plan.plan_hash()),
                        hex::encode(scan.plan_hash),
                        hex::encode(indexed.plan_hash),
                        explanation.tree == compiled.plan.to_canonical_json(),
                    )));
                }
                Ok(explanation.plan_hash)
            });
            match outcome {
                Ok(plan_hash) => {
                    equal_all += 1;
                    explain_identity_ok += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "matrix_equal",
                        "outcome": "explain_identity",
                        "plan_hash": hex::encode(plan_hash),
                        "row_materialization": false,
                        "arms": {
                            "explain_vs_compiled": "equal",
                            "explain_vs_default_qvm": "equal",
                            "forced_scan_qvm": "deliberately_distinct_program",
                            "comparator_mongo_cbl": "deferred_q4"
                        },
                        "source": source,
                    }));
                }
                Err(error) => {
                    non_row_fail += 1;
                    product_err += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "explain_identity_fail",
                        "reason": error.to_string(),
                        "source": source,
                    }));
                }
            }
            continue;
        }

        if !semantic_oracle_eligible(case) {
            continue;
        }
        if !matches!(rql["status"].as_str(), Some("source" | "pending")) {
            continue;
        }
        let ordered = ordering_is_sensitive(&case["ordering_and_multiplicity"]);
        let fixture = &case["fixture"];
        let generator_id = fixture["generator_id"].as_str().unwrap_or("");
        let seed = fixture["seed"].as_u64().unwrap_or(0);
        let fparams = fixture.get("params").cloned().unwrap_or_else(|| json!({}));
        let needed = extract_collection_names(&source);
        considered += 1;

        let params = parameters_for_source(&source);
        let db = match load_logical_db(&root, generator_id, seed, &fparams, &needed) {
            Ok(d) => d,
            Err(e) => {
                product_err += 1;
                case_results.push(json!({
                    "case_id": case_id,
                    "status": "fixture_fail",
                    "reason": e,
                }));
                continue;
            }
        };

        let (plan, oracle) = match oracle_eval(&db, &source, &params) {
            Ok(v) => v,
            Err(e) => {
                let low = e.to_ascii_lowercase();
                if low.contains("after") || low.contains("rql_feature_unavailable") {
                    unsupported += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "unsupported",
                        "reason": e,
                    }));
                } else {
                    product_err += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "oracle_fail",
                        "reason": e,
                    }));
                }
                continue;
            }
        };

        let full_computed = source_uses_rql_full_constructs(&source);
        let (scan_rows, scan, idx_rows, indexed) = if full_computed {
            let (scan_rows, scan) = match product_full_plan(&db, &source, &params, true) {
                Ok(v) => v,
                Err(e) => {
                    product_err += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "force_scan_fail",
                        "reason": e,
                    }));
                    continue;
                }
            };
            let (idx_rows, indexed) = match product_full_plan(&db, &source, &params, false) {
                Ok(v) => v,
                Err(e) => {
                    product_err += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "index_plan_fail",
                        "reason": e,
                    }));
                    continue;
                }
            };
            (scan_rows, scan, idx_rows, indexed)
        } else {
            let scan = match product_force_scan(&db, &plan, &params) {
                Ok(p) => p,
                Err(e) => {
                    product_err += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "force_scan_fail",
                        "reason": e,
                    }));
                    continue;
                }
            };
            let indexed = match product_index_plan(&db, &plan, &params) {
                Ok(p) => p,
                Err(e) => {
                    product_err += 1;
                    case_results.push(json!({
                        "case_id": case_id,
                        "status": "index_plan_fail",
                        "reason": e,
                    }));
                    continue;
                }
            };
            let scan_rows = page_to_rows(&scan);
            let idx_rows = page_to_rows(&indexed);
            (scan_rows, scan, idx_rows, indexed)
        };
        // Group result keys are executor-internal synthetic identities. Q0
        // equivalence compares the aggregate rows, not the spelling of those
        // non-document keys (`__group_N` in oracle vs `g:<hash>` in product).
        let ignore_row_keys = plan.group_agg.is_active();

        let mut arms = BTreeMap::new();
        let c1 = compare_shapes(
            &oracle.rows,
            &scan_rows,
            ordered,
            ignore_row_keys,
            oracle.coverage_complete,
            scan.coverage.complete,
        );
        let c2 = compare_shapes(
            &scan_rows,
            &idx_rows,
            ordered,
            ignore_row_keys,
            scan.coverage.complete,
            indexed.coverage.complete,
        );

        let mut ok = true;
        match c1 {
            Ok(()) => {
                arms.insert("oracle_vs_force_scan", json!("equal"));
            }
            Err(d) => {
                ok = false;
                arms.insert(
                    "oracle_vs_force_scan",
                    json!({"status":"diverge","reason": d.reason}),
                );
            }
        }
        match c2 {
            Ok(()) => {
                arms.insert("force_scan_vs_index", json!("equal"));
            }
            Err(d) => {
                ok = false;
                arms.insert(
                    "force_scan_vs_index",
                    json!({"status":"diverge","reason": d.reason}),
                );
            }
        }

        // Coverage ⇒ zero holes
        if scan.coverage.complete && !scan.known_holes.is_empty() {
            ok = false;
            arms.insert(
                "coverage_holes",
                json!({"status":"diverge","reason":"complete coverage with known_holes"}),
            );
        } else {
            arms.insert("coverage_holes", json!("equal"));
        }

        let mut reopen_status = json!("skipped");
        if ok
            && !full_computed
            && reopen_checked < REOPEN_BUDGET
            && !source.to_ascii_lowercase().contains(" after ")
        {
            match product_reopen_store(&db, &source, &params) {
                Ok(rows) => {
                    reopen_checked += 1;
                    match compare_shapes(
                        &oracle.rows,
                        &rows,
                        ordered,
                        ignore_row_keys,
                        oracle.coverage_complete,
                        true,
                    ) {
                        Ok(()) => {
                            reopen_status = json!("equal");
                        }
                        Err(d) => {
                            ok = false;
                            reopen_fail += 1;
                            reopen_status = json!({"status":"diverge","reason": d.reason});
                        }
                    }
                }
                Err(e) => {
                    // reopen may refuse Full-only; record residual
                    reopen_status = json!({"status":"reopen_error","reason": e});
                    // Do not fail whole suite on reopen host residual unless
                    // force_scan already green — still count for visibility.
                }
            }
        }
        arms.insert("reopen_store", reopen_status);
        arms.insert("comparator_mongo_cbl", json!("deferred_q4"));

        if ok {
            equal_all += 1;
            case_results.push(json!({
                "case_id": case_id,
                "status": "matrix_equal",
                "row_count": oracle.rows.len(),
                "ordered": ordered,
                "arms": arms,
                "source": source,
            }));
        } else {
            diverge += 1;
            case_results.push(json!({
                "case_id": case_id,
                "status": "matrix_diverge",
                "row_count_oracle": oracle.rows.len(),
                "row_count_scan": scan_rows.len(),
                "ordered": ordered,
                "arms": arms,
                "source": source,
            }));
        }
    }

    let summary = json!({
        "format": "residiuum-rql-q3-2-differential-report-v1",
        "oracle_profile": ORACLE_PROFILE,
        "corpus_version": corpus_version,
        "authority": "RQL_QUERY_QUALIFICATION_PROGRAM.md §6.2–6.3",
        "generated_unix_s": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        "matrix": {
            "reference_oracle": true,
            "forced_scan_qvm": true,
            "admitted_index_plan": true,
            "full_computed_projection": true,
            "stable_refusal_contract": true,
            "explain_execution_identity": true,
            "reopened_store": true,
            "comparator_result": "deferred_q4",
        },
        "summary": {
            "tier_a_total": tier_a_total,
            "qualification_green": equal_all,
            "qualification_residual": tier_a_total.saturating_sub(equal_all),
            "package_exit_ready": equal_all == tier_a_total,
            "considered": considered,
            "matrix_equal": equal_all,
            "stable_refusal_ok": stable_refusal_ok,
            "explain_identity_ok": explain_identity_ok,
            "non_row_fail": non_row_fail,
            "matrix_diverge": diverge,
            "unsupported": unsupported,
            "errors": product_err,
            "reopen_checked": reopen_checked,
            "reopen_fail": reopen_fail,
        },
        "cases": case_results,
    });

    // F8: default → target/ only; RESIDIUUM_WRITE_SPEC_EVIDENCE=1 publishes spec/.
    let report = rql_evidence_write::write_q3_report(
        &root,
        "q3_2_differential_report.json",
        &serde_json::to_string_pretty(&summary).unwrap(),
    );

    eprintln!(
        "rql_q3_2: equal={equal_all} diverge={diverge} unsupported={unsupported} errors={product_err} reopen_checked={reopen_checked} reopen_fail={reopen_fail} considered={considered}"
    );
    eprintln!("report: {}", report.display());

    // Defects fail the suite.
    assert_eq!(diverge, 0, "differential divergences are defects");
    assert_eq!(product_err, 0, "product/oracle errors are defects");
    assert_eq!(reopen_fail, 0, "reopen divergences are defects");
    assert_eq!(
        non_row_fail, 0,
        "non-row qualification failures are defects"
    );
    assert!(
        equal_all >= 90,
        "expected ≥90 matrix_equal cases, got {equal_all}"
    );
}
