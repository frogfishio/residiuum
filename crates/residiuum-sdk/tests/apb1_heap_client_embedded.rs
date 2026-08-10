//! APB-1 G1–G6: `HeapClient` / `CollectionClient` façade over embedded [`Heap`].
//!
//! Normative: MUST_ADD §5; inventory `APB1_CLIENT_GAP_INVENTORY.md`.
//! Shared scenarios: `tests/common/apb1_facade_parity.rs` (G6 dual suite).
//! Remote pack: server `apb1_heap_client_from_remote_*`.
//! No package accept.

#[path = "common/apb1_facade_parity.rs"]
mod apb1_facade_parity;

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{ErrorCode, HeapClient, ResidiuumDeployment};
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
        // READ | WRITE | INDEX_ADMIN (G3 index create/drop/rebuild)
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
fn facade_parity_pack_embedded() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);

    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb1").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();

    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    let heap = deployment.open_heap(cap);

    let mut client = HeapClient::from(heap);
    assert!(client.is_bound());
    assert_eq!(client.id(), heap_id);

    // G6: same scenario pack as remote façade suite.
    assert_eq!(
        apb1_facade_parity::SCENARIO_IDS,
        &[
            "create_open_list",
            "put_get_delete",
            "history_versions",
            "index_lifecycle",
            "apb2_mutations",
        ]
    );
    apb1_facade_parity::run_full_facade_parity(&mut client, "orders");

    match client.create_collection("orders") {
        Ok(_) => panic!("duplicate name must fail"),
        Err(err) => assert_eq!(err.code(), ErrorCode::AlreadyExists),
    }
}

#[test]
fn unbound_facade_still_fail_closed() {
    let mut b = [1u8; 16];
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hid = HeapId::from_bytes(b).expect("heap id");
    let mut client = HeapClient::from_id_for_contract(hid);
    assert!(!client.is_bound());
    let err = client.create_collection("x").unwrap_err();
    assert_eq!(err.code(), ErrorCode::Internal);
}
