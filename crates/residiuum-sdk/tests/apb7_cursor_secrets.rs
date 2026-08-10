//! APB-7 T10: product cursor key ring + parameter-bound MAC.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    cursor_parameter_hash, field, param, CursorKeyRing, CursorKeyRingGuard, ErrorCode, HeapClient,
    Parameters, QueryRunOptions, ResidiuumDeployment,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use std::collections::BTreeMap;
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
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb7-t10").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

#[test]
fn parameter_hash_stable_and_sensitive() {
    let mut a = BTreeMap::new();
    a.insert("status".into(), serde_json::json!("open"));
    let mut b = BTreeMap::new();
    b.insert("status".into(), serde_json::json!("closed"));
    let empty = BTreeMap::new();
    let ha = cursor_parameter_hash(&a);
    let hb = cursor_parameter_hash(&b);
    let he = cursor_parameter_hash(&empty);
    assert_eq!(ha.len(), 64);
    assert_ne!(ha, hb);
    assert_ne!(ha, he);
    assert_ne!(ha, "00".repeat(32));
    // Same map → same hash.
    assert_eq!(ha, cursor_parameter_hash(&a));
}

#[test]
fn product_ring_multipage_roundtrip() {
    let _guard = CursorKeyRingGuard::install(CursorKeyRing::product("prod-ck-1", [0xABu8; 32]));

    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..5 {
        col.put(&format!("k{i}"), &serde_json::json!({"i": i}))
            .unwrap();
    }
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    let p1 = col
        .rql("from docs", &Parameters::default(), opts.clone())
        .expect("page1 under product ring");
    assert_eq!(p1.rows.len(), 2);
    assert!(!p1.exhausted);
    // Token must advertise product key id.
    let token_json: serde_json::Value =
        serde_json::from_slice(&p1.next.as_ref().unwrap().token).unwrap();
    assert_eq!(token_json["key_id"], "prod-ck-1");
    assert_ne!(token_json["parameter_hash"], "00".repeat(32));

    opts.after = p1.next;
    let p2 = col
        .rql("from docs", &Parameters::default(), opts)
        .expect("page2 under product ring");
    assert_eq!(p2.rows.len(), 2);
}

#[test]
fn wrong_product_secret_rejects_continuation() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..4 {
        col.put(&format!("k{i}"), &serde_json::json!({"i": i}))
            .unwrap();
    }

    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    let p1 = {
        let _g = CursorKeyRingGuard::install(CursorKeyRing::product("ck-a", [1u8; 32]));
        col.rql("from docs", &Parameters::default(), opts.clone())
            .unwrap()
    };
    opts.after = p1.next;
    // Different secret installed for page 2.
    let _g = CursorKeyRingGuard::install(CursorKeyRing::product("ck-a", [2u8; 32]));
    let err = col
        .rql("from docs", &Parameters::default(), opts)
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ConsistencyViolation);
}

#[test]
fn parameter_mismatch_rejects_continuation() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    col.put("a", &serde_json::json!({"status": "open", "n": 1}))
        .unwrap();
    col.put("b", &serde_json::json!({"status": "open", "n": 2}))
        .unwrap();
    col.put("c", &serde_json::json!({"status": "closed", "n": 3}))
        .unwrap();
    col.put("d", &serde_json::json!({"status": "open", "n": 4}))
        .unwrap();

    let mut params = Parameters::default();
    params
        .values
        .insert("status".into(), serde_json::json!("open"));
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(1);

    let p1 = col
        .query()
        .where_(field("status").unwrap().eq(param("status")))
        .run(&params, opts.clone())
        .expect("page1");
    assert_eq!(p1.rows.len(), 1);
    assert!(!p1.exhausted);

    // Same plan, different parameter value on resume.
    params
        .values
        .insert("status".into(), serde_json::json!("closed"));
    opts.after = p1.next;
    let err = col
        .query()
        .where_(field("status").unwrap().eq(param("status")))
        .run(&params, opts)
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ConsistencyViolation);
    assert!(
        err.to_string().contains("parameter") || err.to_string().contains("cursor"),
        "{err}"
    );
}
