//! APB-7 T11: independent multipage complete-scan oracle matrix.
//!
//! For each scenario, drain Application Core pages (builder and/or RQL) and
//! compare concatenated keys to an independent list_keys+get oracle.
//! Residuals (not claimed here): dual remote backend, full budget differential
//! under multipage, range index pushdown — see inventory.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    field, param, AppQueryBudget, CollectionClient, ErrorCode, HeapClient, OrderDir, Parameters,
    QueryRunOptions, ResidiuumDeployment,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout, IndexState};
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

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

fn uuid() -> [u8; 16] {
    *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes()
}

fn open_bound_client() -> (TempDir, HeapClient) {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb7-t11").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

/// Independent oracle: full list_keys + get, filter, sort by key (scan order).
fn oracle_key_order(
    col: &mut CollectionClient,
    pred: impl Fn(&serde_json::Value) -> bool,
) -> Vec<String> {
    let mut out = Vec::new();
    for k in col.list_keys(Some(1000), None).unwrap() {
        if let Some(v) = col.get(&k).unwrap() {
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
    for k in col.list_keys(Some(1000), None).unwrap() {
        if let Some(v) = col.get(&k).unwrap() {
            rows.push((k, v["score"].as_i64().unwrap_or(0)));
        }
    }
    rows.sort_by(|(ka, sa), (kb, sb)| sb.cmp(sa).then_with(|| ka.cmp(kb)));
    rows.into_iter().map(|(k, _)| k).collect()
}

/// Drain all pages from rql until exhausted; return concatenated keys.
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
        assert!(pages < 100, "runaway pagination");
        let page = col.rql(source, params, opts.clone()).expect("rql page");
        assert!(
            page.coverage.complete,
            "oracle matrix expects complete coverage"
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

#[test]
fn multipage_key_order_matches_complete_scan_oracle() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..7 {
        col.put(&format!("k{i}"), &serde_json::json!({"i": i}))
            .unwrap();
    }
    let oracle = oracle_key_order(&mut col, |_| true);
    let got = drain_rql(&mut col, "from docs", &Parameters::default(), 2);
    assert_eq!(got, oracle);
    assert_eq!(got.len(), 7);
}

#[test]
fn multipage_equality_filter_matches_oracle() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    for (k, st, n) in [
        ("a", "open", 1),
        ("b", "closed", 2),
        ("c", "open", 3),
        ("d", "open", 4),
        ("e", "closed", 5),
        ("f", "open", 6),
        ("g", "open", 7),
    ] {
        col.put(k, &serde_json::json!({"status": st, "n": n}))
            .unwrap();
    }
    let oracle = oracle_key_order(&mut col, |v| v["status"] == "open");
    let got = drain_rql(
        &mut col,
        r#"from orders where status = "open""#,
        &Parameters::default(),
        2,
    );
    assert_eq!(got, oracle);
    assert_eq!(got, vec!["a", "c", "d", "f", "g"]);
}

#[test]
fn multipage_field_order_matches_oracle() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    // Keys reverse of score order.
    for (k, score) in [("z", 1), ("y", 2), ("x", 3), ("w", 4), ("v", 5), ("u", 6)] {
        col.put(k, &serde_json::json!({"score": score})).unwrap();
    }
    let oracle = oracle_score_desc(&mut col);
    let got = drain_rql(
        &mut col,
        "from orders order by score desc",
        &Parameters::default(),
        2,
    );
    assert_eq!(got, oracle);
    assert_eq!(got, vec!["u", "v", "w", "x", "y", "z"]);
}

#[test]
fn multipage_index_pushdown_matches_scan_oracle() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    for (k, st) in [
        ("a", "open"),
        ("b", "closed"),
        ("c", "open"),
        ("d", "open"),
        ("e", "closed"),
        ("f", "open"),
    ] {
        col.put(k, &serde_json::json!({"status": st})).unwrap();
    }
    let oracle = oracle_key_order(&mut col, |v| v["status"] == "open");

    let info = col
        .indexes()
        .create("by_status", &["status"])
        .expect("create_index");
    assert_eq!(info.state, IndexState::Ready);
    assert!(info.complete_coverage);

    // With index present, multipage rql must still match the scan oracle.
    let got = drain_rql(
        &mut col,
        r#"from orders where status = "open""#,
        &Parameters::default(),
        2,
    );
    assert_eq!(got, oracle);

    // Builder multipage with parameters.
    let mut params = Parameters::default();
    params
        .values
        .insert("status".into(), serde_json::json!("open"));
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    let mut all = Vec::new();
    loop {
        let page = col
            .query()
            .where_(field("status").unwrap().eq(param("status")))
            .run(&params, opts.clone())
            .unwrap();
        for r in &page.rows {
            all.push(r.key.clone());
        }
        if page.exhausted {
            break;
        }
        opts.after = page.next;
    }
    assert_eq!(all, oracle);
}

#[test]
fn multipage_builder_and_rql_agree_on_keys() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("items").unwrap().collection;
    for (k, n) in [("a", 10), ("b", 40), ("c", 20), ("d", 30), ("e", 50)] {
        col.put(k, &serde_json::json!({"n": n})).unwrap();
    }
    let rql_keys = drain_rql(
        &mut col,
        "from items order by n asc",
        &Parameters::default(),
        2,
    );
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    let mut b_keys = Vec::new();
    loop {
        let page = col
            .query()
            .order_by("n", OrderDir::Asc)
            .unwrap()
            .run(&Parameters::default(), opts.clone())
            .unwrap();
        for r in &page.rows {
            b_keys.push(r.key.clone());
        }
        if page.exhausted {
            break;
        }
        opts.after = page.next;
    }
    assert_eq!(b_keys, rql_keys);
    assert_eq!(b_keys, vec!["a", "c", "d", "b", "e"]);
}

#[test]
fn multipage_document_budget_fails_closed_consistently() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..10 {
        col.put(&format!("k{i}"), &serde_json::json!({"i": i}))
            .unwrap();
    }
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(3);
    opts.budget = Some(AppQueryBudget {
        max_documents: Some(2),
        max_bytes: None,
        max_result_bytes: None,
    });
    // First page examines > budget → ResourceLimit (not partial silent page).
    let err = col
        .rql("from docs", &Parameters::default(), opts)
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ResourceLimit);
}
