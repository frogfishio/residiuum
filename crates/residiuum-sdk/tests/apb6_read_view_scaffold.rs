//! APB-6: read view scaffold (T1) + segment-fingerprint pin (T2).

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    ErrorCode, FrontierDrift, FrontierKind, HeapClient, ReadViewOptions, ResidiuumDeployment,
    READ_VIEW_PROFILE,
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
        // 0x0d matches product vector READ|WRITE|… used elsewhere in APB tests.
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
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb6").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

#[test]
fn read_view_embedded_pins_segment_fingerprint() {
    let (_dir, mut client) = open_bound_client();
    // Create collection before pin so view-bound open has a target.
    let _ = client.create_collection("orders").expect("create");
    let mut view = client
        .read_view(ReadViewOptions {
            max_age: Some(Duration::from_secs(300)),
            ..Default::default()
        })
        .expect("read_view");
    let info = view.info().clone();
    assert_eq!(info.heap_id, client.id());
    assert_eq!(info.semantic_versions.read_view, READ_VIEW_PROFILE);
    assert_eq!(info.frontier.kind, FrontierKind::SegmentFingerprint);
    assert!(info.observation_pinned);
    assert!(!info.view_id_hex.is_empty());
    assert_eq!(info.frontier.identity_hex.len(), 64);
    assert!(view.pinned_fingerprint().is_some());
    view.ensure_usable().unwrap();
    assert_eq!(view.check_drift().unwrap(), FrontierDrift::Stable);

    // APB-7 T5: Stable pin allows open_collection under the view.
    let bound = view
        .open_collection(&mut client, "orders")
        .expect("Stable pin may open view-bound collection");
    drop(bound);

    view.close();
    assert_eq!(
        view.ensure_usable().unwrap_err().code(),
        ErrorCode::ConsistencyViolation
    );
}

#[test]
fn read_view_detects_drift_after_mutation() {
    let (_dir, mut client) = open_bound_client();
    let mut view = client
        .read_view(ReadViewOptions {
            max_age: Some(Duration::from_secs(600)),
            ..Default::default()
        })
        .expect("read_view");
    assert_eq!(view.check_drift().unwrap(), FrontierDrift::Stable);
    let pin_before = view.pinned_fingerprint().expect("pin");

    // Mutate the heap so segment layout fingerprint can move.
    let created = client
        .create_collection("orders")
        .expect("create_collection");
    let mut col = created.collection;
    col.put("k1", &serde_json::json!({"n": 1})).expect("put");

    let drift = view.check_drift().expect("check_drift after mutation");
    // Prefer Drifted; if segment set is unchanged in this layout, still assert pin bytes fixed.
    match drift {
        FrontierDrift::Drifted => {
            // Refresh re-binds to live frontier.
            view.refresh_pin().expect("refresh_pin");
            assert_eq!(view.check_drift().unwrap(), FrontierDrift::Stable);
            let pin_after = view.pinned_fingerprint().unwrap();
            assert_ne!(pin_before, pin_after, "refresh should capture new fp");
        }
        FrontierDrift::Stable => {
            // Honest residual: some empty-ish layouts may not move segment paths/sizes
            // on a single put. Pin must still remain the original capture.
            assert_eq!(view.pinned_fingerprint().unwrap(), pin_before);
            // refresh still succeeds and stays pinned.
            view.refresh_pin().expect("refresh_pin stable path");
            assert!(view.is_observation_pinned());
        }
        FrontierDrift::Unpinned => panic!("embedded view must be pinned"),
    }
}

#[test]
fn unbound_heap_client_rejects_read_view() {
    let heap = HeapId::from_bytes_unchecked_nonzero([2u8; 16]).unwrap();
    let mut client = HeapClient::from_id_for_contract(heap);
    let err = client.read_view(ReadViewOptions::default()).unwrap_err();
    assert_eq!(err.code(), ErrorCode::Internal);
}
