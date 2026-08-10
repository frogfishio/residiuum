//! APP-6 T2: bounded Application Core page executor vs list_keys/get oracle.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    AppQueryBudget, ErrorCode, HeapClient, Parameters, QueryRunOptions, ResidiuumDeployment,
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
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-app6").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

#[test]
fn rql_equality_filter_matches_scan_oracle() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;

    col.put("a", &serde_json::json!({"status": "open", "n": 1}))
        .unwrap();
    col.put("b", &serde_json::json!({"status": "closed", "n": 2}))
        .unwrap();
    col.put("c", &serde_json::json!({"status": "open", "n": 3}))
        .unwrap();

    // Manual oracle: list_keys + get + status == open
    let mut oracle = Vec::new();
    for k in col.list_keys(Some(100), None).unwrap() {
        if let Some(v) = col.get(&k).unwrap() {
            if v["status"] == "open" {
                oracle.push(k);
            }
        }
    }
    assert_eq!(oracle, vec!["a".to_string(), "c".to_string()]);

    let page = col
        .rql(
            r#"from orders where status = "open""#,
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .expect("rql");
    let keys: Vec<_> = page.rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(keys, oracle);
    assert!(page.exhausted);
    assert!(page.next.is_none());
    assert_eq!(page.collection_id, col.id());
}

#[test]
fn rql_page_size_and_continuation() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..5 {
        let k = format!("k{i}");
        col.put(&k, &serde_json::json!({"i": i})).unwrap();
    }

    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    let p1 = col
        .rql("from docs", &Parameters::default(), opts.clone())
        .expect("page1");
    assert_eq!(p1.rows.len(), 2);
    assert!(!p1.exhausted);
    let cont = p1.next.expect("continuation");

    opts.after = Some(cont);
    let p2 = col
        .rql("from docs", &Parameters::default(), opts.clone())
        .expect("page2");
    assert_eq!(p2.rows.len(), 2);
    assert!(!p2.exhausted);

    opts.after = p2.next;
    let p3 = col
        .rql("from docs", &Parameters::default(), opts)
        .expect("page3");
    assert_eq!(p3.rows.len(), 1);
    assert!(p3.exhausted);

    let all: Vec<_> = p1
        .rows
        .iter()
        .chain(p2.rows.iter())
        .chain(p3.rows.iter())
        .map(|r| r.key.clone())
        .collect();
    assert_eq!(all, col.list_keys(Some(100), None).unwrap());
}

#[test]
fn rql_budget_documents_resource_limit() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..10 {
        col.put(&format!("k{i}"), &serde_json::json!({"i": i}))
            .unwrap();
    }
    let mut opts = QueryRunOptions::default();
    opts.budget = Some(AppQueryBudget {
        max_documents: Some(3),
        max_bytes: None,
        max_result_bytes: None,
    });
    let err = col
        .rql("from docs", &Parameters::default(), opts)
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ResourceLimit);
}

#[test]
fn explain_rql_returns_plan_hash() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    let exp = col
        .explain_rql(
            r#"from orders where status = "open""#,
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .expect("explain");
    assert_eq!(exp.plan_profile, "rql-plan-v1");
    assert_ne!(exp.plan_hash, [0u8; 32]);
    assert!(exp.tree.get("from").is_some());
}
