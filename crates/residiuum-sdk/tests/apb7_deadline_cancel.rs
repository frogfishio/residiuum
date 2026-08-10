//! APB-7 T8: query deadline + cooperative cancellation.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    CancelToken, ErrorCode, HeapClient, Parameters, QueryRunOptions, ResidiuumDeployment,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use std::sync::Arc;
use std::time::Duration;
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
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb7-t8").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

#[test]
fn pre_cancelled_token_fails_closed() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    col.put("a", &serde_json::json!({"n": 1})).unwrap();

    let token = CancelToken::new();
    token.cancel();
    let mut opts = QueryRunOptions::default();
    opts.cancel = Some(token);

    let err = col
        .rql("from docs", &Parameters::default(), opts)
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ResourceLimit);
    assert!(
        err.to_string().contains("cancel"),
        "expected cancel in error, got {err}"
    );
}

#[test]
fn zero_deadline_fails_closed() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    col.put("a", &serde_json::json!({"n": 1})).unwrap();

    let mut opts = QueryRunOptions::default();
    // Instantly elapsed relative deadline.
    opts.deadline = Some(Duration::from_secs(0));

    let err = col
        .rql("from docs", &Parameters::default(), opts)
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::DeadlineExceeded);
    assert!(
        err.to_string().contains("deadline"),
        "expected deadline in error, got {err}"
    );
}

#[test]
fn generous_deadline_allows_query() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    col.put("a", &serde_json::json!({"n": 1})).unwrap();
    col.put("b", &serde_json::json!({"n": 2})).unwrap();

    let mut opts = QueryRunOptions::default();
    opts.deadline = Some(Duration::from_secs(30));
    opts.cancel = Some(CancelToken::new()); // not cancelled

    let page = col
        .rql("from docs", &Parameters::default(), opts)
        .expect("query under generous deadline");
    assert_eq!(page.rows.len(), 2);
    assert!(page.exhausted);
}

#[test]
fn cancel_shared_across_clone() {
    let token = CancelToken::new();
    let clone = token.clone();
    assert!(!token.is_cancelled());
    clone.cancel();
    assert!(token.is_cancelled());
    assert!(token.check().is_err());
}
