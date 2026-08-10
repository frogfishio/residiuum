//! APB-7 T7: dual-pack host — embedded Application Core query parity.
//!
//! Shared scenarios: `tests/common/apb7_query_parity.rs`.
//! Remote collection-plane host: server `apb7_query_from_remote_*`.
//! Product op 118 residual — not claimed. No package accept.

#[path = "common/apb7_query_parity.rs"]
mod apb7_query_parity;

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{HeapClient, ResidiuumDeployment};
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
        // READ | WRITE | INDEX_ADMIN
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
fn query_dual_pack_embedded() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);

    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb7-t7").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();

    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    let mut client = HeapClient::from(deployment.open_heap(cap));
    assert!(client.is_bound());

    assert_eq!(
        apb7_query_parity::SCENARIO_IDS,
        &[
            "builder_rql_plan_hash",
            "equality_filter_keys",
            "multipage_key_order_oracle",
            "multipage_field_order_oracle",
            "explain_rql_surface",
            "scan_json_page",
            "budget_fail_closed",
            "index_equality_pushdown",
        ]
    );
    apb7_query_parity::run_full_query_parity(&mut client, "orders_t7");
}
