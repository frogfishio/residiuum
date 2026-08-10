//! APB-7 T1: CollectionClient::query() PlanBuilder path vs RQL plan_hash + run.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    compile_app_core, field, param, CollectionBindings, ErrorCode, HeapClient, OrderDir,
    Parameters, QueryRunOptions, ResidiuumDeployment,
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
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb7").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

#[test]
fn query_builder_plan_hash_matches_rql() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;

    let built = col
        .query()
        .where_(field("status").unwrap().eq(param("status")))
        .project(["id", "status"])
        .unwrap()
        .order_by("created_at", OrderDir::Desc)
        .unwrap()
        .limit(1000)
        .page_size(100)
        .unwrap()
        .compile()
        .expect("compile builder");

    let mut bindings = CollectionBindings::default();
    bindings.bind("orders", col.id());
    let from_rql = compile_app_core(
        r#"from orders
           where status = $status
           project id, status
           order by created_at desc
           limit 1000
           page size 100"#,
        &bindings,
    )
    .expect("compile rql")
    .plan;

    assert_eq!(
        built.plan_hash(),
        from_rql.plan_hash(),
        "builder vs RQL plan_hash must match"
    );
    assert_eq!(built.plan_hash_hex(), from_rql.plan_hash_hex());
}

#[test]
fn query_run_matches_rql_scan() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    col.put("a", &serde_json::json!({"status": "open", "n": 1}))
        .unwrap();
    col.put("b", &serde_json::json!({"status": "closed", "n": 2}))
        .unwrap();
    col.put("c", &serde_json::json!({"status": "open", "n": 3}))
        .unwrap();

    let mut params = Parameters::default();
    params
        .values
        .insert("status".into(), serde_json::json!("open"));

    let from_builder = col
        .query()
        .where_(field("status").unwrap().eq(param("status")))
        .run(&params, QueryRunOptions::default())
        .expect("query().run");
    let keys_b: Vec<_> = from_builder.rows.iter().map(|r| r.key.clone()).collect();

    let from_rql = col
        .rql(
            r#"from orders where status = $status"#,
            &params,
            QueryRunOptions::default(),
        )
        .expect("rql");
    let keys_r: Vec<_> = from_rql.rows.iter().map(|r| r.key.clone()).collect();

    assert_eq!(keys_b, keys_r);
    assert_eq!(keys_b, vec!["a".to_string(), "c".to_string()]);
    assert_eq!(from_builder.plan_hash, from_rql.plan_hash);
    assert!(from_builder.exhausted);
}

#[test]
fn query_explain_has_plan_hash() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    let expl = col
        .query()
        .where_(field("status").unwrap().eq(param("status")))
        .explain()
        .expect("explain");
    assert_ne!(expl.plan_hash, [0u8; 32]);
    assert!(!expl.plan_profile.is_empty());
    assert!(expl.tree.is_object() || expl.tree.is_array() || !expl.tree.is_null());
}

#[test]
fn unbound_query_run_fails_closed() {
    let heap = HeapId::from_bytes_unchecked_nonzero([3u8; 16]).unwrap();
    let cid = residiuum_heap::CollectionId::from_bytes_unchecked_nonzero([4u8; 16]).unwrap();
    let mut col = residiuum_sdk::CollectionClient::from_parts_for_contract(heap, cid, "orders");
    let err = col
        .query()
        .run(&Parameters::default(), QueryRunOptions::default())
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Internal);
}
