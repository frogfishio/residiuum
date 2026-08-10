//! APP-1 embedded collection create (CORE plan §6 / HAR-1 capability first cut).

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{ErrorCode, ResidiuumDeployment};
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
        rights: Rights::from_bits_certificate(0x5).unwrap(),
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
fn embedded_create_list_open_and_duplicate_name() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);

    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-app1").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();

    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    let heap = deployment.open_heap(cap);

    let created = heap.create_collection("orders").expect("create");
    assert_eq!(created.collection.name(), "orders");
    assert_eq!(created.collection.heap_id(), heap_id);
    assert_ne!(created.receipt_id, [0u8; 16]);
    assert_ne!(created.descriptor_hash, [0u8; 32]);

    let listed = heap.list_collections().expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "orders");
    assert_eq!(listed[0].collection_id, created.collection.id());

    let opened = heap.collection("orders").expect("open by name");
    assert_eq!(opened.id(), created.collection.id());

    // Data plane works on the new collection.
    created
        .collection
        .put("k1", &serde_json::json!({"n": 1}))
        .expect("put");
    let got = created.collection.get("k1").expect("get");
    assert_eq!(got, Some(serde_json::json!({"n": 1})));

    match heap.create_collection("orders") {
        Ok(_) => panic!("duplicate name must fail"),
        Err(err) => assert_eq!(err.code(), ErrorCode::AlreadyExists),
    }
}

#[test]
fn two_heaps_same_name_distinct_ids() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);

    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let ha = *HeapId::new_random().unwrap().as_bytes();
    let hb = *HeapId::new_random().unwrap().as_bytes();
    let sa = stage_heap_genesis(&layout, dep, ha, uuid(), "heap-a").unwrap();
    let sb = stage_heap_genesis(&layout, dep, hb, uuid(), "heap-b").unwrap();
    publish_staged_genesis(&layout, &sa.staging_id, &sa.descriptor_hash).unwrap();
    publish_staged_genesis(&layout, &sb.staging_id, &sb.descriptor_hash).unwrap();

    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let heap_a = deployment.open_heap(mint_cap_for(
        HeapId::from_bytes_unchecked_nonzero(ha).unwrap(),
        dep_id,
    ));
    let heap_b = deployment.open_heap(mint_cap_for(
        HeapId::from_bytes_unchecked_nonzero(hb).unwrap(),
        dep_id,
    ));

    let ca = heap_a.create_collection("users").unwrap();
    let cb = heap_b.create_collection("users").unwrap();
    assert_ne!(ca.collection.id(), cb.collection.id());
    assert_eq!(ca.collection.name(), "users");
    assert_eq!(cb.collection.name(), "users");
}

#[test]
fn operation_id_exact_retry_replays_and_conflict() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-app1-r1").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();

    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let heap = deployment.open_heap(mint_cap_for(heap_id, dep_id));

    let op = uuid();
    let first = heap
        .create_collection_with("orders", Some(op))
        .expect("first create");
    assert!(!first.replayed);
    assert_eq!(first.operation_id, op);

    let again = heap
        .create_collection_with("orders", Some(op))
        .expect("exact retry");
    assert!(again.replayed);
    assert_eq!(again.collection.id(), first.collection.id());
    assert_eq!(again.receipt_id, first.receipt_id);
    assert_eq!(again.descriptor_hash, first.descriptor_hash);
    assert_eq!(again.created_at_unix_s, first.created_at_unix_s);
    assert_eq!(again.operation_id, op);

    // Same operation id, different name → consistency violation (fingerprint conflict).
    match heap.create_collection_with("users", Some(op)) {
        Ok(_) => panic!("conflicting operation_id reuse must fail"),
        Err(err) => assert_eq!(err.code(), ErrorCode::ConsistencyViolation),
    }

    // Different operation id, same name → already exists.
    match heap.create_collection_with("orders", Some(uuid())) {
        Ok(_) => panic!("duplicate name under new op must fail"),
        Err(err) => assert_eq!(err.code(), ErrorCode::AlreadyExists),
    }

    // Dedup survives process-style reopen (release first writer lock).
    let first_id = first.collection.id();
    let first_receipt = first.receipt_id;
    drop(first);
    drop(again);
    drop(heap);
    drop(deployment);

    let deployment2 = ResidiuumDeployment::open(root).unwrap();
    let heap2 = deployment2.open_heap(mint_cap_for(heap_id, dep_id));
    let after_reopen = heap2
        .create_collection_with("orders", Some(op))
        .expect("retry after reopen");
    assert!(after_reopen.replayed);
    assert_eq!(after_reopen.collection.id(), first_id);
    assert_eq!(after_reopen.receipt_id, first_receipt);
}

#[test]
fn empty_name_rejected() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap = deployment.open_heap(mint_cap_for(
        HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap(),
        DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap(),
    ));
    match heap.create_collection("") {
        Ok(_) => panic!("empty name must fail"),
        Err(err) => assert_eq!(err.code(), ErrorCode::ValidationFailed),
    }
}
