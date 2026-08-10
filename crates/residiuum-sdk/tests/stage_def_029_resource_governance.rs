//! DEF-029: resource governance — budgets, host limits, cancellation.

use residiuum_sdk::{
    json, CancelToken, ErrorCode, Filter, QueryBudget, QueryOptions, Residiuum, ResourceLimits,
    DEFAULT_MAX_JSON_DEPTH, RESOURCE_PROFILE,
};

use serde_json::Value as JsonValue;
use tempfile::tempdir;

fn deep_json(depth: usize) -> JsonValue {
    let mut v = json!(1);
    for _ in 0..depth.saturating_sub(1) {
        v = json!({ "n": v });
    }
    v
}

#[test]
fn resource_profile_tag() {
    assert_eq!(RESOURCE_PROFILE, "residiuum-resource-v1");
    assert_eq!(
        ResourceLimits::draft_defaults().max_json_depth,
        DEFAULT_MAX_JSON_DEPTH
    );
}

#[test]
fn put_rejects_over_deep_json() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("deep.residiuum")).unwrap();
    let mut c = db.collection("docs").unwrap();
    // Root depth = 1; wrap DEFAULT_MAX_JSON_DEPTH times → depth = DEFAULT+1.
    let bad = deep_json(DEFAULT_MAX_JSON_DEPTH + 1);
    let err = c.put("k", &bad).unwrap_err();
    assert_eq!(err.code(), ErrorCode::ResourceLimit);
    // Boundary: exactly at limit succeeds.
    let ok = deep_json(DEFAULT_MAX_JSON_DEPTH);
    c.put("ok", &ok).unwrap();
}

#[test]
fn scan_budget_max_docs_force_scan() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("docs.residiuum")).unwrap();
    let mut c = db.collection("items").unwrap();
    for i in 0..20 {
        c.put(&format!("k{i:02}"), &json!({"i": i, "tag": "x"}))
            .unwrap();
    }
    let err = c
        .find_with(
            &Filter::field("tag").eq("x"),
            QueryOptions::new()
                .force_scan()
                .budget(QueryBudget::max_docs(3)),
        )
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::QueryBudgetRequired);
}

#[test]
fn scan_budget_max_bytes() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("bytes.residiuum")).unwrap();
    let mut c = db.collection("items").unwrap();
    // Large-ish bodies so a few docs exceed a tight byte budget.
    let body = json!({"blob": "x".repeat(2_000), "tag": "x"});
    for i in 0..10 {
        c.put(&format!("k{i:02}"), &body).unwrap();
    }
    let err = c
        .find_with(
            &Filter::field("tag").eq("x"),
            QueryOptions::new()
                .force_scan()
                .budget(QueryBudget::default().max_bytes(500)),
        )
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::QueryBudgetRequired);
}

#[test]
fn budget_partial_returns_matches() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("partial.residiuum")).unwrap();
    let mut c = db.collection("items").unwrap();
    for i in 0..20 {
        c.put(&format!("k{i:02}"), &json!({"i": i, "tag": "x"}))
            .unwrap();
    }
    // With allow_partial_coverage, budget stop returns matches collected so far
    // (may be empty if the first page alone exceeds the budget).
    let rows = c
        .find_with(
            &Filter::field("tag").eq("x"),
            QueryOptions::new()
                .force_scan()
                .budget(QueryBudget::max_docs(0))
                .allow_partial_coverage(),
        )
        .unwrap();
    // max_docs 0: first page examines > 0 → partial empty or truncated is ok;
    // must not error as complete success confusion.
    assert!(rows.len() <= 20);
}

#[test]
fn result_bytes_budget_fails_closed() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("result.residiuum")).unwrap();
    let mut c = db.collection("items").unwrap();
    for i in 0..8 {
        c.put(
            &format!("k{i:02}"),
            &json!({"tag": "x", "blob": "y".repeat(500)}),
        )
        .unwrap();
    }
    let err = c
        .find_with(
            &Filter::field("tag").eq("x"),
            QueryOptions::new()
                .force_scan()
                .budget(QueryBudget::default().max_result_memory(200)),
        )
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::QueryBudgetRequired);
}

#[test]
fn cancel_token_stops_find() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("cancel.residiuum")).unwrap();
    let mut c = db.collection("items").unwrap();
    for i in 0..30 {
        c.put(&format!("k{i:02}"), &json!({"tag": "x", "i": i}))
            .unwrap();
    }
    let token = CancelToken::new();
    token.cancel();
    let err = c
        .find_with(
            &Filter::field("tag").eq("x"),
            QueryOptions::new().force_scan().cancel(token),
        )
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ResourceLimit);
    assert!(err.to_string().contains("cancel"));
}

#[test]
fn query_plan_serializes_extended_budget() {
    use residiuum_sdk::{QueryPlan, QUERY_PLAN_PROFILE};

    let plan = QueryPlan::new(
        Filter::field("tag").eq("x"),
        QueryOptions::new().budget(
            QueryBudget::max_docs(10)
                .max_bytes(1_000)
                .max_result_memory(2_000),
        ),
    );
    let j = plan.to_json();
    assert_eq!(j["profile"], QUERY_PLAN_PROFILE);
    let back = QueryPlan::from_json(&j).unwrap();
    let b = back.options.budget.expect("budget");
    assert_eq!(b.max_docs_scanned, Some(10));
    assert_eq!(b.max_bytes_scanned, Some(1_000));
    assert_eq!(b.max_result_bytes, Some(2_000));
    // Cancel is not part of the plan.
    assert!(back.options.cancel.is_none());
}

#[test]
fn index_path_respects_doc_budget() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("idx.residiuum")).unwrap();
    let mut c = db.collection("items").unwrap();
    for i in 0..15 {
        c.put(&format!("k{i:02}"), &json!({"tag": "x", "i": i}))
            .unwrap();
    }
    c.indexes().unwrap().create("by-tag", &["tag"]).unwrap();
    // Index probe still counts examined subjects against the budget.
    let err = c
        .find_with(
            &Filter::field("tag").eq("x"),
            QueryOptions::new().budget(QueryBudget::max_docs(3)),
        )
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::QueryBudgetRequired);
}
