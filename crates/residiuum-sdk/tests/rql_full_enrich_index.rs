//! RQL-I1: root enrich equality-index pushdown vs full-scan differential oracle.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    execute_rql_full, execute_rql_full_with, EnrichAttachMode, HeapClient, IndexState, Parameters,
    QueryRunOptions, ResidiuumDeployment, RqlFullExecuteOptions,
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
fn enrich_index_matches_scan_oracle() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-i1").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));

    let mut orders = client.create_collection("orders").unwrap().collection;
    let mut customers = client.create_collection("customers").unwrap().collection;
    orders
        .put("o1", &serde_json::json!({"customer_id": "c1"}))
        .unwrap();
    orders
        .put("o2", &serde_json::json!({"customer_id": "c2"}))
        .unwrap();
    orders
        .put("o3", &serde_json::json!({"customer_id": "c1"}))
        .unwrap();
    customers
        .put("c1", &serde_json::json!({"id": "c1", "name": "Ada"}))
        .unwrap();
    customers
        .put("c2", &serde_json::json!({"id": "c2", "name": "Bob"}))
        .unwrap();
    customers
        .put("c3", &serde_json::json!({"id": "c3", "name": "unused"}))
        .unwrap();
    for index in 0..50 {
        let id = format!("x{index:02}");
        orders
            .put(
                &format!("ox{index:02}"),
                &serde_json::json!({"customer_id": id}),
            )
            .unwrap();
        customers
            .put(
                &id,
                &serde_json::json!({"id": id, "name": format!("Customer {index}")}),
            )
            .unwrap();
    }

    let info = customers
        .indexes()
        .create("by_id", &["id"])
        .expect("create index");
    assert_eq!(info.state, IndexState::Ready);
    assert!(info.complete_coverage);
    drop(orders);
    drop(customers);

    let src = r#"from orders
                 enrich customer using customers matching customer_id = id expect exactly_one
                 page size 64"#;

    let indexed = execute_rql_full(
        &mut client,
        src,
        &Parameters::default(),
        QueryRunOptions {
            diagnostics: true,
            ..Default::default()
        },
    )
    .expect("index path");
    assert_eq!(indexed.enrich_loads.len(), 1);
    assert_eq!(
        indexed.enrich_loads[0].mode,
        EnrichAttachMode::EqualityIndex
    );
    assert_eq!(indexed.enrich_loads[0].using, "customers");
    assert!(
        indexed
            .base
            .diagnostics
            .as_ref()
            .expect("diagnostics")
            .host_read_calls
            < 20,
        "distinct join values must not restore an N+1 probe/read loop"
    );

    let scanned = execute_rql_full_with(
        &mut client,
        src,
        &Parameters::default(),
        RqlFullExecuteOptions {
            query: QueryRunOptions::default(),
            force_enrich_scan: true,
        },
    )
    .expect("scan oracle");
    assert_eq!(scanned.enrich_loads[0].mode, EnrichAttachMode::Scan);

    assert_eq!(indexed.rows.len(), scanned.rows.len());
    for ((ik, iv), (sk, sv)) in indexed.rows.iter().zip(scanned.rows.iter()) {
        assert_eq!(ik, sk);
        assert_eq!(iv, sv, "index attach must match scan oracle for key {ik}");
    }
    assert_eq!(indexed.rows[0].1["customer"]["name"], "Ada");
    assert_eq!(indexed.rows[1].1["customer"]["name"], "Bob");
    assert_eq!(indexed.rows[2].1["customer"]["name"], "Ada");
}

#[test]
fn enrich_without_index_falls_back_to_scan() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-i1-scan").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));

    let mut orders = client.create_collection("orders").unwrap().collection;
    let mut customers = client.create_collection("customers").unwrap().collection;
    orders
        .put("o1", &serde_json::json!({"customer_id": "c1"}))
        .unwrap();
    customers
        .put("c1", &serde_json::json!({"id": "c1", "name": "Ada"}))
        .unwrap();
    drop(orders);
    drop(customers);

    let page = execute_rql_full(
        &mut client,
        r#"from orders
           enrich customer using customers matching customer_id = id expect exactly_one
           page size 8"#,
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .expect("scan fallback");
    assert_eq!(page.enrich_loads[0].mode, EnrichAttachMode::Scan);
    assert_eq!(page.rows[0].1["customer"]["name"], "Ada");
}
