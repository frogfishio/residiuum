//! Phase 3.3: enrich candidate where-filter via execute_rql_full.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    execute_rql_full, HeapClient, Parameters, QueryRunOptions, ResidiuumDeployment,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use std::sync::Arc;
use tempfile::tempdir;

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

#[test]
fn execute_rql_full_candidate_where() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-where").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));

    let mut orders = client.create_collection("orders").unwrap().collection;
    let mut customers = client.create_collection("customers").unwrap().collection;
    customers
        .put(
            "c1a",
            &serde_json::json!({"id": "c1", "active": false, "name": "old"}),
        )
        .unwrap();
    customers
        .put(
            "c1b",
            &serde_json::json!({"id": "c1", "active": true, "name": "Ada"}),
        )
        .unwrap();
    orders
        .put("o1", &serde_json::json!({"customer_id": "c1"}))
        .unwrap();
    drop(orders);
    drop(customers);

    let page = execute_rql_full(
        &mut client,
        r#"from orders
           enrich customer using customers matching customer_id = id
           where active = true
           expect exactly_one"#,
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .expect("execute_rql_full with candidate where");

    assert!(page.enrich()[0].candidate_where.is_some());
    assert_eq!(page.rows[0].1["customer"]["name"], "Ada");
}

#[test]
fn execute_rql_full_many_with_where() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-many-w").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));

    let mut orders = client.create_collection("orders").unwrap().collection;
    let mut lines = client.create_collection("line_items").unwrap().collection;
    orders
        .put("o1", &serde_json::json!({"order_id": "o1"}))
        .unwrap();
    lines
        .put(
            "l1",
            &serde_json::json!({"order_id": "o1", "sku": "A", "qty": 1}),
        )
        .unwrap();
    lines
        .put(
            "l2",
            &serde_json::json!({"order_id": "o1", "sku": "B", "qty": 0}),
        )
        .unwrap();
    drop(orders);
    drop(lines);

    let page = execute_rql_full(
        &mut client,
        "from orders enrich items using line_items matching order_id = order_id where qty > 0 expect many",
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .unwrap();
    let bag = page.rows[0].1["items"].as_array().unwrap();
    assert_eq!(bag.len(), 1);
    assert_eq!(bag[0]["sku"], "A");
}
