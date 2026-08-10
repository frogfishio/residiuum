//! Phase 3.8: root pipeline where after enrich via execute_rql_full.

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
fn execute_rql_full_root_where_after_enrich() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-rwhere").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));

    let mut orders = client.create_collection("orders").unwrap().collection;
    let mut customers = client.create_collection("customers").unwrap().collection;

    orders
        .put(
            "o1",
            &serde_json::json!({"customer_id": "c1", "status": "paid"}),
        )
        .unwrap();
    orders
        .put(
            "o2",
            &serde_json::json!({"customer_id": "c2", "status": "paid"}),
        )
        .unwrap();
    orders
        .put(
            "o3",
            &serde_json::json!({"customer_id": "c1", "status": "open"}),
        )
        .unwrap();
    customers
        .put(
            "c1",
            &serde_json::json!({"id": "c1", "country": "TH", "name": "Ada"}),
        )
        .unwrap();
    customers
        .put(
            "c2",
            &serde_json::json!({"id": "c2", "country": "US", "name": "Bob"}),
        )
        .unwrap();
    drop(orders);
    drop(customers);

    let page = execute_rql_full(
        &mut client,
        r#"from orders
           where status = "paid"
           enrich customer using customers matching customer_id = id expect exactly_one
           where customer.country = "TH"
           page size 64"#,
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .expect("root where after enrich");

    assert_eq!(page.profile, RQL_FULL_PROFILE);
    assert_eq!(page.pipeline.len(), 2);
    assert!(matches!(&page.pipeline[1], FullPipelineStepV1::Filter(_)));
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].0, "o1");
    assert_eq!(page.rows[0].1["customer"]["name"], "Ada");
}
