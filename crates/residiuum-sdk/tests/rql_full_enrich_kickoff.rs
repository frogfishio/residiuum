//! Phase 3 kickoff: compile_rql_full + full-language execute via ISA (RQL-P0b).
//!
//! Attach helpers are crate-private; product path is `execute_rql_full`.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    compile_app_core, compile_rql_full, execute_rql_full, AppQueryBudget, CollectionBindings,
    ErrorCode, HeapClient, Parameters, QueryRunOptions, ResidiuumDeployment,
    DIAG_RQL_FEATURE_UNAVAILABLE,
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
fn enrich_exactly_one_attach_oracle() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-enrich").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));

    let mut orders = client
        .create_collection("orders")
        .expect("orders")
        .collection;
    let mut customers = client
        .create_collection("customers")
        .expect("customers")
        .collection;

    customers
        .put("c1", &serde_json::json!({"id": "c1", "name": "Ada"}))
        .unwrap();
    customers
        .put("c2", &serde_json::json!({"id": "c2", "name": "Bob"}))
        .unwrap();
    orders
        .put("o1", &serde_json::json!({"customer_id": "c1", "n": 1}))
        .unwrap();
    orders
        .put("o2", &serde_json::json!({"customer_id": "c2", "n": 2}))
        .unwrap();

    let mut bindings = CollectionBindings::default();
    bindings.bind(orders.name(), orders.id());
    bindings.bind(customers.name(), customers.id());

    let src = format!(
        "from {} enrich customer using {} matching customer_id = id expect exactly_one page size 64",
        orders.name(),
        customers.name()
    );

    // Core path still refuses enrich.
    let err = compile_app_core(&src, &bindings).unwrap_err();
    assert!(err.to_string().contains(DIAG_RQL_FEATURE_UNAVAILABLE));

    let compiled = compile_rql_full(&src, &bindings).expect("compile_rql_full");
    assert_eq!(compiled.root_enrich().len(), 1);

    // Product execute: compile → ISA → attach (no public attach_enrich_rows).
    let page = execute_rql_full(
        &mut client,
        &src,
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .expect("execute_rql_full");
    assert_eq!(page.rows.len(), 2);
    let by_key: std::collections::BTreeMap<_, _> =
        page.rows.into_iter().map(|(k, v)| (k, v)).collect();
    assert_eq!(by_key.get("o1").unwrap()["customer"]["name"], "Ada");
    assert_eq!(by_key.get("o2").unwrap()["customer"]["name"], "Bob");

    // Core rows fit comfortably, but Full attachment must re-check the final
    // expanded result rather than bypassing max_result_bytes.
    customers
        .put(
            "c1",
            &serde_json::json!({"id": "c1", "name": "x".repeat(16 * 1024)}),
        )
        .unwrap();
    let mut bounded = QueryRunOptions::default();
    bounded.budget = Some(AppQueryBudget {
        max_documents: None,
        max_bytes: None,
        max_result_bytes: Some(2 * 1024),
    });
    let err = execute_rql_full(&mut client, &src, &Parameters::default(), bounded)
        .expect_err("Full attachment must enforce result budget");
    assert_eq!(err.code(), ErrorCode::QueryBudgetRequired);
}
