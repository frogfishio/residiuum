//! APB-7 T7 dual-backend Application Core query parity scenarios.
//!
//! Shared behavioral checks for [`CollectionClient`] query surfaces that both
//! the **embedded** suite (`apb7_query_dual_pack`) and the **remote** suite
//! (`residiuum-server` `apb7_query_from_remote_*`) should run.
//!
//! Normative: MUST_ADD §11 (builder + RQL + pages vs complete-scan oracle +
//! embedded/remote parity). **Not** package accept — op **118** remains
//! reserved; remote scenarios today use collection-plane `list_keys`+`get`
//! (same scan plane as embedded), not product `rql_query` wire.
//!
//! Include from another integration test crate with:
//! ```ignore
//! #[path = "../../residiuum-sdk/tests/common/apb7_query_parity.rs"]
//! mod apb7_query_parity;
//! ```

use residiuum_sdk::{
    compile_app_core, field, param, AppQueryBudget, CollectionBindings, CollectionClient,
    ErrorCode, OrderDir, Parameters, QueryBytecodeV1, QueryRunOptions, ScanJsonOptions,
};
use residiuum_store::IndexState;

/// Scenario ids covered by this module (checklist for APB-7 T7 dual pack).
pub const SCENARIO_IDS: &[&str] = &[
    "builder_rql_plan_hash",
    "equality_filter_keys",
    "multipage_key_order_oracle",
    "multipage_field_order_oracle",
    "explain_rql_surface",
    "scan_json_page",
    "budget_fail_closed",
    "index_equality_pushdown",
];

/// Independent oracle: full list_keys + get, optional filter, key order.
fn oracle_key_order(
    col: &mut CollectionClient,
    pred: impl Fn(&serde_json::Value) -> bool,
) -> Vec<String> {
    let mut out = Vec::new();
    for k in col
        .list_keys(Some(1000), None)
        .unwrap_or_else(|e| panic!("parity list_keys: {e:?}"))
    {
        if let Some(v) = col
            .get(&k)
            .unwrap_or_else(|e| panic!("parity get({k}): {e:?}"))
        {
            if pred(&v) {
                out.push(k);
            }
        }
    }
    out
}

/// Independent oracle: full scan then sort by score desc, key asc.
fn oracle_score_desc(col: &mut CollectionClient) -> Vec<String> {
    let mut rows: Vec<(String, i64)> = Vec::new();
    for k in col
        .list_keys(Some(1000), None)
        .unwrap_or_else(|e| panic!("parity list_keys: {e:?}"))
    {
        if let Some(v) = col
            .get(&k)
            .unwrap_or_else(|e| panic!("parity get({k}): {e:?}"))
        {
            rows.push((k, v["score"].as_i64().unwrap_or(0)));
        }
    }
    rows.sort_by(|(ka, sa), (kb, sb)| sb.cmp(sa).then_with(|| ka.cmp(kb)));
    rows.into_iter().map(|(k, _)| k).collect()
}

fn drain_rql(
    col: &mut CollectionClient,
    source: &str,
    params: &Parameters,
    page_size: u32,
) -> Vec<String> {
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(page_size);
    let mut all = Vec::new();
    let mut pages = 0u32;
    loop {
        pages += 1;
        assert!(pages < 100, "parity: runaway pagination");
        let page = col
            .rql(source, params, opts.clone())
            .unwrap_or_else(|e| panic!("parity rql page: {e:?}"));
        assert!(
            page.coverage.complete,
            "parity pack expects complete coverage on quiet collection"
        );
        for r in &page.rows {
            all.push(r.key.clone());
        }
        if page.exhausted {
            assert!(page.next.is_none());
            break;
        }
        opts.after = page.next;
    }
    all
}

/// Builder compile plan_hash equals Application Core RQL compile plan_hash.
pub fn scenario_builder_rql_plan_hash(col: &mut CollectionClient) {
    let name = col.name().to_string();
    let built = col
        .query()
        .where_(field("status").unwrap().eq(param("status")))
        .project(["id", "status"])
        .expect("project")
        .order_by("created_at", OrderDir::Desc)
        .expect("order")
        .limit(1000)
        .page_size(100)
        .expect("page_size")
        .compile()
        .expect("compile builder");

    let mut bindings = CollectionBindings::default();
    bindings.bind(&name, col.id());
    let source = format!(
        r#"from {name}
           where status = $status
           project id, status
           order by created_at desc
           limit 1000
           page size 100"#
    );
    let from_rql = compile_app_core(&source, &bindings)
        .expect("compile rql")
        .plan;

    assert_eq!(
        built.plan_hash(),
        from_rql.plan_hash(),
        "parity: builder vs RQL plan_hash"
    );
    assert_eq!(built.plan_hash_hex(), from_rql.plan_hash_hex());
}

/// Single-page equality filter: builder keys == RQL keys == list_keys+get oracle.
pub fn scenario_equality_filter_keys(col: &mut CollectionClient) {
    // Isolate fixtures under unique key prefix so dual hosts can re-run.
    for (k, st, n) in [
        ("eq_a", "open", 1),
        ("eq_b", "closed", 2),
        ("eq_c", "open", 3),
    ] {
        col.put(k, &serde_json::json!({"status": st, "n": n}))
            .unwrap_or_else(|e| panic!("parity put({k}): {e:?}"));
    }

    let mut params = Parameters::default();
    params
        .values
        .insert("status".into(), serde_json::json!("open"));

    let name = col.name().to_string();
    let from_builder = col
        .query()
        .where_(field("status").unwrap().eq(param("status")))
        .run(&params, QueryRunOptions::default())
        .expect("query().run");
    let keys_b: Vec<_> = from_builder.rows.iter().map(|r| r.key.clone()).collect();

    let source = format!(r#"from {name} where status = $status"#);
    let from_rql = col
        .rql(&source, &params, QueryRunOptions::default())
        .expect("rql");
    let keys_r: Vec<_> = from_rql.rows.iter().map(|r| r.key.clone()).collect();

    // Oracle only over our eq_* keys so pre-seeded remote collections don't poison.
    let oracle: Vec<String> = oracle_key_order(col, |v| v["status"] == "open")
        .into_iter()
        .filter(|k| k.starts_with("eq_"))
        .collect();
    let keys_b: Vec<_> = keys_b
        .into_iter()
        .filter(|k| k.starts_with("eq_"))
        .collect();
    let keys_r: Vec<_> = keys_r
        .into_iter()
        .filter(|k| k.starts_with("eq_"))
        .collect();

    assert_eq!(keys_b, keys_r, "parity: builder vs RQL keys");
    assert_eq!(keys_b, oracle, "parity: keys vs list_keys+get oracle");
    assert_eq!(keys_b, vec!["eq_a".to_string(), "eq_c".to_string()]);
    assert_eq!(from_builder.plan_hash, from_rql.plan_hash);
}

/// Multipage key-order drain matches complete-scan oracle.
pub fn scenario_multipage_key_order_oracle(col: &mut CollectionClient) {
    for i in 0..7 {
        let k = format!("mpk{i}");
        col.put(&k, &serde_json::json!({"i": i}))
            .unwrap_or_else(|e| panic!("parity put({k}): {e:?}"));
    }
    let name = col.name().to_string();
    let oracle: Vec<String> = oracle_key_order(col, |_| true)
        .into_iter()
        .filter(|k| k.starts_with("mpk"))
        .collect();
    let got: Vec<String> = drain_rql(col, &format!("from {name}"), &Parameters::default(), 2)
        .into_iter()
        .filter(|k| k.starts_with("mpk"))
        .collect();
    assert_eq!(got, oracle);
    assert_eq!(got.len(), 7);
}

/// Multipage field-order drain matches independent score-desc oracle.
pub fn scenario_multipage_field_order_oracle(col: &mut CollectionClient) {
    // Distinct prefix so co-tenant keys from other scenarios do not poison order.
    for (k, score) in [
        ("fo_z", 1),
        ("fo_y", 2),
        ("fo_x", 3),
        ("fo_w", 4),
        ("fo_v", 5),
        ("fo_u", 6),
    ] {
        col.put(k, &serde_json::json!({"score": score}))
            .unwrap_or_else(|e| panic!("parity put({k}): {e:?}"));
    }
    let name = col.name().to_string();
    let is_fo = |k: &str| k.starts_with("fo_");
    let oracle: Vec<String> = oracle_score_desc(col)
        .into_iter()
        .filter(|k| is_fo(k))
        .collect();
    let got: Vec<String> = drain_rql(
        col,
        &format!("from {name} order by score desc"),
        &Parameters::default(),
        2,
    )
    .into_iter()
    .filter(|k| is_fo(k))
    .collect();
    assert_eq!(got, oracle);
    assert_eq!(
        got,
        vec![
            "fo_u".to_string(),
            "fo_v".to_string(),
            "fo_w".to_string(),
            "fo_x".to_string(),
            "fo_y".to_string(),
            "fo_z".to_string()
        ]
    );
}

/// explain_rql identifies the default QVM produced from the compiled plan.
pub fn scenario_explain_rql_surface(col: &mut CollectionClient) {
    let name = col.name().to_string();
    let source = format!(r#"from {name} where status = $status"#);
    let explained = col
        .explain_rql(&source, &Parameters::default(), QueryRunOptions::default())
        .expect("explain_rql");

    let mut bindings = CollectionBindings::default();
    bindings.bind(&name, col.id());
    let compiled = compile_app_core(&source, &bindings).expect("compile");
    let expected_qvm =
        QueryBytecodeV1::from_core_plan(compiled.plan.clone(), compiled.budget.clone())
            .expect("lower default QVM");
    assert_eq!(explained.plan_hash, expected_qvm.qvm_hash());
    assert_eq!(explained.plan_profile, compiled.plan.profile);
    assert!(!explained.tree.is_null());
}

/// scan_json pages and exhausts with complete coverage on quiet collection.
pub fn scenario_scan_json_page(col: &mut CollectionClient) {
    for i in 0..5 {
        let k = format!("sj{i}");
        col.put(&k, &serde_json::json!({"i": i}))
            .unwrap_or_else(|e| panic!("parity put({k}): {e:?}"));
    }
    let p1 = col
        .scan_json(ScanJsonOptions {
            limit: Some(2),
            after_key: None,
        })
        .expect("scan page1");
    assert!(p1.rows.len() >= 2 || p1.exhausted);
    assert!(p1.coverage.complete);
    // Drain remaining with after_key when present.
    if let Some(after) = p1.next_after_key {
        let p2 = col
            .scan_json(ScanJsonOptions {
                limit: Some(100),
                after_key: Some(after),
            })
            .expect("scan page2");
        assert!(p2.coverage.complete);
    }
}

/// Document budget fail-closed (ResourceLimit), not silent partial page.
pub fn scenario_budget_fail_closed(col: &mut CollectionClient) {
    for i in 0..10 {
        let k = format!("bdg{i}");
        col.put(&k, &serde_json::json!({"i": i}))
            .unwrap_or_else(|e| panic!("parity put({k}): {e:?}"));
    }
    let name = col.name().to_string();
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(3);
    opts.budget = Some(AppQueryBudget {
        max_documents: Some(2),
        max_bytes: None,
        max_result_bytes: None,
    });
    let err = col
        .rql(&format!("from {name}"), &Parameters::default(), opts)
        .expect_err("budget must fail closed");
    assert_eq!(err.code(), ErrorCode::ResourceLimit);
}

/// Equality index pushdown multipage matches list_keys+get oracle (needs IndexAdmin).
pub fn scenario_index_equality_pushdown(col: &mut CollectionClient) {
    for (k, st) in [
        ("ix_a", "open"),
        ("ix_b", "closed"),
        ("ix_c", "open"),
        ("ix_d", "open"),
        ("ix_e", "closed"),
        ("ix_f", "open"),
    ] {
        col.put(k, &serde_json::json!({"status": st}))
            .unwrap_or_else(|e| panic!("parity put({k}): {e:?}"));
    }
    let info = col
        .indexes()
        .create("by_status_t7", &["status"])
        .expect("create_index by_status_t7");
    assert_eq!(info.state, IndexState::Ready);
    assert!(info.complete_coverage);

    let name = col.name().to_string();
    let oracle: Vec<String> = oracle_key_order(col, |v| v["status"] == "open")
        .into_iter()
        .filter(|k| k.starts_with("ix_"))
        .collect();
    let got: Vec<String> = drain_rql(
        col,
        &format!(r#"from {name} where status = "open""#),
        &Parameters::default(),
        2,
    )
    .into_iter()
    .filter(|k| k.starts_with("ix_"))
    .collect();
    assert_eq!(got, oracle);
}

/// Full collection-plane query pack (all scenarios on one bound collection).
///
/// Prefer a freshly created empty collection (embedded create or remote
/// HeapAdmin create). Index scenario requires IndexAdmin rights.
pub fn run_query_collection_plane_parity(col: &mut CollectionClient) {
    scenario_builder_rql_plan_hash(col);
    scenario_equality_filter_keys(col);
    scenario_multipage_key_order_oracle(col);
    scenario_multipage_field_order_oracle(col);
    scenario_explain_rql_surface(col);
    scenario_scan_json_page(col);
    scenario_budget_fail_closed(col);
    scenario_index_equality_pushdown(col);
}

/// Create collection then run full query pack (embedded / HeapAdmin remote).
pub fn run_full_query_parity(client: &mut residiuum_sdk::HeapClient, collection_name: &str) {
    assert!(
        client.is_bound(),
        "parity: HeapClient must be bound (From<Heap|RemoteHeap>)"
    );
    let mut col = client
        .create_collection(collection_name)
        .unwrap_or_else(|e| panic!("parity create_collection({collection_name}): {e:?}"))
        .collection;
    run_query_collection_plane_parity(&mut col);
}
