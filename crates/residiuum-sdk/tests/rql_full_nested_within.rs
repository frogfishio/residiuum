//! Phase 3.6: nested within + enrich-after-within via execute_rql_full.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    execute_rql_full, FullPipelineStepV1, HeapClient, Parameters, QueryRunOptions,
    ResidiuumDeployment, RQL_FULL_PROFILE,
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
fn execute_rql_full_nested_within() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-nested").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));

    let mut orders = client.create_collection("orders").unwrap().collection;
    let mut lines = client.create_collection("line_items").unwrap().collection;
    let mut components = client.create_collection("components").unwrap().collection;
    let mut products = client.create_collection("products").unwrap().collection;

    orders
        .put("o1", &serde_json::json!({"order_id": "o1"}))
        .unwrap();
    lines
        .put("l1", &serde_json::json!({"order_id": "o1", "sku": "A"}))
        .unwrap();
    components
        .put(
            "c1",
            &serde_json::json!({"parent_sku": "A", "product_id": "p1"}),
        )
        .unwrap();
    components
        .put(
            "c2",
            &serde_json::json!({"parent_sku": "A", "product_id": "p2"}),
        )
        .unwrap();
    products
        .put("p1", &serde_json::json!({"id": "p1", "name": "Widget"}))
        .unwrap();
    products
        .put("p2", &serde_json::json!({"id": "p2", "name": "Gadget"}))
        .unwrap();
    drop(orders);
    drop(lines);
    drop(components);
    drop(products);

    let page = execute_rql_full(
        &mut client,
        r#"from orders
           enrich items using line_items matching order_id = order_id expect many
           within items as item {
             enrich parts using components matching item.sku = parent_sku expect many
             within item.parts as part {
               enrich product using products as candidate
                 matching part.product_id = candidate.id
                 expect exactly_one
             }
           }
           page size 64"#,
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .expect("nested within");

    assert_eq!(page.profile, RQL_FULL_PROFILE);
    let w = page.within().expect("within");
    assert_eq!(w.steps.len(), 2);
    assert!(matches!(&w.steps[1], FullPipelineStepV1::Within(_)));
    let parts = page.rows[0].1["items"].as_array().unwrap()[0]["parts"]
        .as_array()
        .unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0]["product"]["name"], "Widget");
    assert_eq!(parts[1]["product"]["name"], "Gadget");
}

#[test]
fn execute_rql_full_enrich_after_within() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-eaw").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));

    let mut orders = client.create_collection("orders").unwrap().collection;
    let mut lines = client.create_collection("line_items").unwrap().collection;
    let mut products = client.create_collection("products").unwrap().collection;
    let mut customers = client.create_collection("customers").unwrap().collection;

    orders
        .put(
            "o1",
            &serde_json::json!({"order_id": "o1", "customer_id": "c1"}),
        )
        .unwrap();
    lines
        .put(
            "l1",
            &serde_json::json!({"order_id": "o1", "product_id": "p1"}),
        )
        .unwrap();
    products
        .put("p1", &serde_json::json!({"id": "p1", "name": "Widget"}))
        .unwrap();
    customers
        .put("c1", &serde_json::json!({"id": "c1", "name": "Ada"}))
        .unwrap();
    drop(orders);
    drop(lines);
    drop(products);
    drop(customers);

    let page = execute_rql_full(
        &mut client,
        r#"from orders
           enrich items using line_items matching order_id = order_id expect many
           within items as item {
             enrich product using products as candidate
               matching item.product_id = candidate.id
               expect exactly_one
           }
           enrich customer using customers matching customer_id = id expect exactly_one
           page size 64"#,
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .expect("enrich after within");

    assert_eq!(page.pipeline.len(), 3);
    assert!(matches!(&page.pipeline[2], FullPipelineStepV1::Enrich(_)));
    assert_eq!(page.rows[0].1["customer"]["name"], "Ada");
    assert_eq!(
        page.rows[0].1["items"].as_array().unwrap()[0]["product"]["name"],
        "Widget"
    );
}
