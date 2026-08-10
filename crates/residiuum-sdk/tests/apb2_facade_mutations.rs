//! APB-2 / APP-3 slice: create / upsert / list_keys / replace / delete_with / add.
//!
//! Normative: MUST_ADD §6 (safe single-key mutations). First cut is
//! read-then-write; store Key Atomic CAS residual is documented. No package accept.
//!
//! OCC token for `if_version` is establishing event id (`WriteReceipt.version`
//! / `event_id`).

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    ErrorCode, HeapClient, KeyProfile, PutOptions, ResidiuumDeployment, KEY_PROFILE_RANDOM_V1,
};
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
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb2").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

#[test]
fn facade_create_upsert_list_keys() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;

    let up = col
        .upsert("k1", &serde_json::json!({"n": 1}))
        .expect("upsert insert");
    assert!(up.inserted);
    assert_eq!(up.receipt.key, "k1");

    let up2 = col
        .upsert("k1", &serde_json::json!({"n": 2}))
        .expect("upsert replace");
    assert!(!up2.inserted);
    assert_eq!(col.get("k1").unwrap().unwrap()["n"], 2);

    match col.create("k1", &serde_json::json!({"n": 3})) {
        Ok(_) => panic!("create on present key must fail"),
        Err(e) => assert_eq!(e.code(), ErrorCode::AlreadyExists),
    }

    col.create("k2", &serde_json::json!({"n": 9}))
        .expect("create absent");
    assert_eq!(col.get("k2").unwrap().unwrap()["n"], 9);

    let keys = col.list_keys(Some(10), None).expect("list_keys");
    assert!(keys.contains(&"k1".to_string()));
    assert!(keys.contains(&"k2".to_string()));
    assert_eq!(keys.len(), 2);

    let after = col.list_keys(Some(10), Some("k1")).expect("list after");
    assert_eq!(after, vec!["k2".to_string()]);
}

#[test]
fn facade_replace_if_version_and_delete_with() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;

    // replace on absent → VersionConflict (observed None)
    let bogus = [0xab; 16];
    match col.replace("missing", &serde_json::json!({"n": 0}), bogus) {
        Ok(_) => panic!("replace absent must fail"),
        Err(e) => {
            assert_eq!(e.code(), ErrorCode::VersionConflict);
            match e {
                residiuum_sdk::Error::VersionConflict { expected, observed } => {
                    assert_eq!(expected, bogus);
                    assert!(observed.is_none());
                }
                other => panic!("expected VersionConflict variant, got {other:?}"),
            }
        }
    }

    let r1 = col
        .put("k", &serde_json::json!({"n": 1}))
        .expect("seed put");
    // OCC token: WriteReceipt.version == establishing event_id.
    assert_eq!(r1.version, r1.event_id);
    let v1 = r1.version;

    let r2 = col
        .replace("k", &serde_json::json!({"n": 2}), v1)
        .expect("replace matching version");
    assert_eq!(col.get("k").unwrap().unwrap()["n"], 2);
    assert_ne!(r2.version, v1);
    assert_eq!(r2.version, r2.event_id);

    // Stale token must not clobber newer value.
    match col.replace("k", &serde_json::json!({"n": 99}), v1) {
        Ok(_) => panic!("stale if_version must fail"),
        Err(e) => {
            assert_eq!(e.code(), ErrorCode::VersionConflict);
            match e {
                residiuum_sdk::Error::VersionConflict { expected, observed } => {
                    assert_eq!(expected, v1);
                    assert_eq!(observed, Some(r2.version));
                }
                other => panic!("expected VersionConflict variant, got {other:?}"),
            }
        }
    }
    assert_eq!(col.get("k").unwrap().unwrap()["n"], 2);

    // delete_with wrong version
    match col.delete_with("k", Some(v1), true, PutOptions::default()) {
        Ok(_) => panic!("delete stale version must fail"),
        Err(e) => assert_eq!(e.code(), ErrorCode::VersionConflict),
    }
    assert!(col.get("k").unwrap().is_some());

    // delete_with matching version
    let del = col
        .delete_with("k", Some(r2.version), true, PutOptions::default())
        .expect("delete matching");
    assert!(del.removed);
    assert!(col.get("k").unwrap().is_none());

    // if_present + absent → NotFound
    match col.delete_with("k", None, true, PutOptions::default()) {
        Ok(_) => panic!("if_present on absent must NotFound"),
        Err(e) => assert_eq!(e.code(), ErrorCode::NotFound),
    }

    // if_present false + absent → removed false
    let soft = col
        .delete_with("gone", None, false, PutOptions::default())
        .expect("soft absent delete");
    assert!(!soft.removed);
}

#[test]
fn facade_add_generated_key() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("events").unwrap().collection;

    let a = col
        .add(&serde_json::json!({"kind": "ping"}))
        .expect("add default profile");
    assert_eq!(a.key_profile, KEY_PROFILE_RANDOM_V1);
    assert_eq!(a.key.len(), 32);
    assert!(a.key.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(a.receipt.key, a.key);
    assert_eq!(a.receipt.version, a.receipt.event_id);
    assert_eq!(col.get(&a.key).unwrap().unwrap()["kind"], "ping");

    let b = col
        .add_with(
            &serde_json::json!({"kind": "pong"}),
            KeyProfile::RandomV1,
            PutOptions::default(),
        )
        .expect("add_with");
    assert_ne!(a.key, b.key);
    assert_eq!(b.key_profile, KeyProfile::RandomV1.as_str());

    let keys = col.list_keys(Some(10), None).expect("list");
    assert!(keys.contains(&a.key));
    assert!(keys.contains(&b.key));
}
