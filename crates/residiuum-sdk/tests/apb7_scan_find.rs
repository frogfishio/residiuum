//! APB-7 T3: CollectionClient scan_json + find_json façade.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{ErrorCode, FindJsonOptions, HeapClient, ResidiuumDeployment, ScanJsonOptions};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use std::sync::Arc;
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
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb7-t3").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

#[test]
fn scan_json_pages_and_exhausts() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..5 {
        col.put(&format!("k{i}"), &serde_json::json!({"i": i}))
            .unwrap();
    }

    let p1 = col
        .scan_json(ScanJsonOptions {
            limit: Some(2),
            after_key: None,
        })
        .expect("page1");
    assert_eq!(p1.rows.len(), 2);
    assert!(!p1.exhausted);
    assert!(p1.coverage.complete);
    assert!(p1.known_holes.is_empty());
    let after = p1.next_after_key.expect("next");

    let p2 = col
        .scan_json(ScanJsonOptions {
            limit: Some(2),
            after_key: Some(after),
        })
        .expect("page2");
    assert_eq!(p2.rows.len(), 2);
    assert!(!p2.exhausted);

    let p3 = col
        .scan_json(ScanJsonOptions {
            limit: Some(2),
            after_key: p2.next_after_key,
        })
        .expect("page3");
    assert_eq!(p3.rows.len(), 1);
    assert!(p3.exhausted);
    assert!(p3.next_after_key.is_none());

    let all: Vec<_> = p1
        .rows
        .iter()
        .chain(p2.rows.iter())
        .chain(p3.rows.iter())
        .map(|r| r.key.clone())
        .collect();
    assert_eq!(all, col.list_keys(Some(100), None).unwrap());
    assert_eq!(p1.collection_id, col.id());
}

#[test]
fn find_json_equality_filter() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    col.put("a", &serde_json::json!({"status": "open", "n": 1}))
        .unwrap();
    col.put("b", &serde_json::json!({"status": "closed", "n": 2}))
        .unwrap();
    col.put("c", &serde_json::json!({"status": "open", "n": 3}))
        .unwrap();

    let rows = col
        .find_json(
            &serde_json::json!({"status": "open"}),
            FindJsonOptions::default(),
        )
        .expect("find");
    let keys: Vec<_> = rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(keys, vec!["a".to_string(), "c".to_string()]);
    assert_eq!(rows[0].value["n"], 1);
}

#[test]
fn find_json_respects_limit() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    for i in 0..5 {
        col.put(
            &format!("k{i}"),
            &serde_json::json!({"status": "open", "i": i}),
        )
        .unwrap();
    }
    let rows = col
        .find_json(
            &serde_json::json!({"status": "open"}),
            FindJsonOptions { limit: Some(2) },
        )
        .expect("find");
    assert_eq!(rows.len(), 2);
}

#[test]
fn unbound_scan_json_fails_closed() {
    let heap = HeapId::from_bytes_unchecked_nonzero([5u8; 16]).unwrap();
    let cid = residiuum_heap::CollectionId::from_bytes_unchecked_nonzero([6u8; 16]).unwrap();
    let mut col = residiuum_sdk::CollectionClient::from_parts_for_contract(heap, cid, "orders");
    let err = col.scan_json(ScanJsonOptions::default()).unwrap_err();
    assert_eq!(err.code(), ErrorCode::Internal);
}
