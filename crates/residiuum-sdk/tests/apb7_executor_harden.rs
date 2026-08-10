//! APB-7 T2: executor budgets (bytes) + field-order scan oracle.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    field, param, AppQueryBudget, ErrorCode, HeapClient, OrderDir, Parameters, QueryRunOptions,
    ResidiuumDeployment,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
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
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb7-t2").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

#[test]
fn max_bytes_budget_fails_closed() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    // Several small docs; tiny max_bytes trips after first get(s).
    for i in 0..5 {
        col.put(
            &format!("k{i}"),
            &serde_json::json!({"payload": "xxxxxxxxxxxxxxxx", "i": i}),
        )
        .unwrap();
    }
    let mut opts = QueryRunOptions::default();
    opts.budget = Some(AppQueryBudget {
        max_documents: None,
        max_bytes: Some(20),
        max_result_bytes: None,
    });
    let err = col
        .rql("from docs", &Parameters::default(), opts)
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ResourceLimit);
    assert!(
        err.to_string().contains("max_bytes"),
        "expected max_bytes in error, got {err}"
    );
}

#[test]
fn max_result_bytes_budget_fails_closed() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..4 {
        col.put(
            &format!("k{i}"),
            &serde_json::json!({"blob": "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyy", "i": i}),
        )
        .unwrap();
    }
    let mut opts = QueryRunOptions::default();
    // Allow examining many docs but cap projected result payload tightly.
    opts.budget = Some(AppQueryBudget {
        max_documents: Some(100),
        max_bytes: None,
        max_result_bytes: Some(40),
    });
    let err = col
        .rql("from docs", &Parameters::default(), opts)
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ResourceLimit);
    assert!(
        err.to_string().contains("max_result_bytes"),
        "expected max_result_bytes in error, got {err}"
    );
}

#[test]
fn field_order_matches_scan_oracle_and_projects() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    // Keys are reverse of score order so key-order would differ.
    col.put("z", &serde_json::json!({"score": 1, "name": "low"}))
        .unwrap();
    col.put("m", &serde_json::json!({"score": 3, "name": "high"}))
        .unwrap();
    col.put("a", &serde_json::json!({"score": 2, "name": "mid"}))
        .unwrap();

    // Manual oracle: all docs, sort by score desc, then key asc.
    let mut oracle: Vec<(String, serde_json::Value)> = Vec::new();
    for k in col.list_keys(Some(100), None).unwrap() {
        if let Some(v) = col.get(&k).unwrap() {
            oracle.push((k, v));
        }
    }
    oracle.sort_by(|(ka, va), (kb, vb)| {
        let sa = va["score"].as_i64().unwrap_or(0);
        let sb = vb["score"].as_i64().unwrap_or(0);
        sb.cmp(&sa).then_with(|| ka.cmp(kb))
    });
    let oracle_keys: Vec<_> = oracle.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        oracle_keys,
        vec!["m".to_string(), "a".to_string(), "z".to_string()]
    );

    // Builder path: order by score desc; project only name (order field not projected).
    let page = col
        .query()
        .order_by("score", OrderDir::Desc)
        .unwrap()
        .project(["name"])
        .unwrap()
        .run(&Parameters::default(), QueryRunOptions::default())
        .expect("field-order query");
    let keys: Vec<_> = page.rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(keys, oracle_keys);
    // Projected value has name, not score.
    assert_eq!(page.rows[0].value["name"], "high");
    assert!(page.rows[0].value.get("score").is_none());
    assert!(page.exhausted);

    // RQL path same order.
    let rql_page = col
        .rql(
            r#"from orders order by score desc project name"#,
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .expect("rql field order");
    let rql_keys: Vec<_> = rql_page.rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(rql_keys, oracle_keys);
    assert_eq!(rql_page.plan_hash, page.plan_hash);
}

#[test]
fn equality_filter_field_order_oracle() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    col.put("k1", &serde_json::json!({"status": "open", "n": 2}))
        .unwrap();
    col.put("k2", &serde_json::json!({"status": "closed", "n": 9}))
        .unwrap();
    col.put("k3", &serde_json::json!({"status": "open", "n": 1}))
        .unwrap();
    col.put("k4", &serde_json::json!({"status": "open", "n": 5}))
        .unwrap();

    let mut params = Parameters::default();
    params
        .values
        .insert("status".into(), serde_json::json!("open"));

    let mut oracle: Vec<(String, i64)> = Vec::new();
    for k in col.list_keys(Some(100), None).unwrap() {
        if let Some(v) = col.get(&k).unwrap() {
            if v["status"] == "open" {
                oracle.push((k, v["n"].as_i64().unwrap_or(0)));
            }
        }
    }
    oracle.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    let oracle_keys: Vec<_> = oracle.iter().map(|(k, _)| k.clone()).collect();

    let page = col
        .query()
        .where_(field("status").unwrap().eq(param("status")))
        .order_by("n", OrderDir::Asc)
        .unwrap()
        .run(&params, QueryRunOptions::default())
        .expect("run");
    let keys: Vec<_> = page.rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(keys, oracle_keys);
    assert_eq!(
        keys,
        vec!["k3".to_string(), "k1".to_string(), "k4".to_string()]
    );
}
