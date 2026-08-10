//! APP-1: qualified op 106 collection_create dispatch + admin_op_dedup.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_server::{
    dispatch_heap_request_with, layout_for_root, HeapDataCtx, HeapDispatchResult,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, Store, StoreHost};
use std::sync::Arc;
use tempfile::TempDir;

fn mint_admin_cap(heap: HeapId, deployment: DeploymentId) -> residiuum_heap::HeapCap {
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
    // READ|WRITE|INDEX_ADMIN|HEAP_ADMIN
    let rights = Rights::from_bits_certificate(0x1 | 0x4 | 0x8 | 0x1000).unwrap();
    let cert = VerifiedCertificate {
        cose_bytes: vec![0x01],
        fingerprint: [3u8; 32],
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4u8; 32],
        rights,
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

#[test]
fn op_106_create_replay_and_conflict() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().join("store");
    Store::create(&root).unwrap();
    let layout = layout_for_root(&root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_b = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_b, [1u8; 16], "heap-app1").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();

    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_b).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_admin_cap(heap_id, dep_id);
    let host = StoreHost::open(&root).unwrap();
    let store = host.open_heap(cap.clone());

    let op = "00112233445566778899aabbccddeeff";
    let req = serde_json::json!({
        "v": 1,
        "id": 1,
        "operation_id": op,
        "op_id": 106,
        "args": { "canonical_name": "orders" }
    });
    let raw = serde_json::to_vec(&req).unwrap();
    let ctx = HeapDataCtx {
        store: &store,
        layout: &layout,
    };
    let first = match dispatch_heap_request_with(&cap, &raw, Some(ctx)) {
        HeapDispatchResult::Response(r) => r,
    };
    assert!(first.ok, "first create: {first:?}");
    let cid = first.result.as_ref().unwrap()["collection_id"]
        .as_str()
        .unwrap()
        .to_string();
    let receipt = first.result.as_ref().unwrap()["receipt"]["receipt_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        first.result.as_ref().unwrap()["replayed"],
        serde_json::json!(false)
    );

    // Exact retry.
    let ctx = HeapDataCtx {
        store: &store,
        layout: &layout,
    };
    let again = match dispatch_heap_request_with(&cap, &raw, Some(ctx)) {
        HeapDispatchResult::Response(r) => r,
    };
    assert!(again.ok, "replay: {again:?}");
    assert_eq!(again.result.as_ref().unwrap()["collection_id"], cid);
    assert_eq!(
        again.result.as_ref().unwrap()["receipt"]["receipt_id"],
        receipt
    );
    assert_eq!(
        again.result.as_ref().unwrap()["replayed"],
        serde_json::json!(true)
    );

    // Conflicting fingerprint.
    let bad = serde_json::json!({
        "v": 1,
        "id": 2,
        "operation_id": op,
        "op_id": 106,
        "args": { "canonical_name": "users" }
    });
    let ctx = HeapDataCtx {
        store: &store,
        layout: &layout,
    };
    let conflict =
        match dispatch_heap_request_with(&cap, &serde_json::to_vec(&bad).unwrap(), Some(ctx)) {
            HeapDispatchResult::Response(r) => r,
        };
    assert!(!conflict.ok);
    assert_eq!(
        conflict.error.as_ref().unwrap().code,
        "consistency_violation"
    );

    // Missing HeapAdmin → unavailable (use READ-only cap).
    let snap = HeapSecuritySnapshot {
        deployment_id: dep_id,
        heap_id,
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
        deployment_id: dep_id,
        heap_id,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4u8; 32],
        rights: Rights::from_bits_certificate(0x1).unwrap(),
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: [5u8; 32],
    };
    let read_cap = mint_capability(
        slot,
        &cert,
        TrustedInstant {
            unix_s: 1_700_000_000,
        },
    )
    .unwrap();
    let read_store = host.open_heap(read_cap.clone());
    let ctx = HeapDataCtx {
        store: &read_store,
        layout: &layout,
    };
    let denied = match dispatch_heap_request_with(
        &read_cap,
        &serde_json::to_vec(&serde_json::json!({
            "v": 1,
            "id": 3,
            "operation_id": "aabbccddeeff00112233445566778899",
            "op_id": 106,
            "args": { "canonical_name": "other" }
        }))
        .unwrap(),
        Some(ctx),
    ) {
        HeapDispatchResult::Response(r) => r,
    };
    assert!(!denied.ok);
    assert_eq!(denied.error.as_ref().unwrap().code, "heap_unavailable");
}
