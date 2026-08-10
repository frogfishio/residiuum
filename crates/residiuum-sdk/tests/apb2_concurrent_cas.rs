//! APB-2 T8: concurrent lost-update matrix (embedded).
//!
//! Multiple threads share cloned [`HeapCollection`] handles over one
//! [`HeapStore`] mutex. Exactly one conditional mutation wins.
//!
//! Residual: multi-process remote concurrent clients; crash matrix (R3).
//! No package accept.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{ErrorCode, HeapCollection, ResidiuumDeployment};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout, WriteCondition};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
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

fn open_collection() -> (tempfile::TempDir, HeapCollection) {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb2-t8").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    let heap = deployment.open_heap(cap);
    let col = heap.create_collection("docs").unwrap().collection;
    (dir, col)
}

#[test]
fn concurrent_replace_if_version_one_wins() {
    let (_dir, col) = open_collection();
    let r0 = col
        .put("k", &serde_json::json!({"n": 0}))
        .expect("seed put");
    let v0 = r0.version;
    let n = 8usize;
    let barrier = Arc::new(Barrier::new(n));
    let wins = Arc::new(Mutex::new(0u32));
    let conflicts = Arc::new(Mutex::new(0u32));
    let mut handles = Vec::new();
    for i in 0..n {
        let col = col.clone();
        let barrier = Arc::clone(&barrier);
        let wins = Arc::clone(&wins);
        let conflicts = Arc::clone(&conflicts);
        handles.push(thread::spawn(move || {
            barrier.wait();
            match col.replace_if_version("k", &serde_json::json!({"n": i as i64}), v0) {
                Ok(_) => *wins.lock().unwrap() += 1,
                Err(e) if e.code() == ErrorCode::VersionConflict => {
                    *conflicts.lock().unwrap() += 1;
                }
                Err(e) => panic!("unexpected: {e:?}"),
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(*wins.lock().unwrap(), 1, "exactly one replace winner");
    assert_eq!(*conflicts.lock().unwrap(), (n as u32) - 1);
    let v = col.get("k").unwrap().unwrap();
    assert!(v["n"].as_i64().is_some());
}

#[test]
fn concurrent_create_if_absent_one_wins() {
    let (_dir, col) = open_collection();
    let n = 8usize;
    let barrier = Arc::new(Barrier::new(n));
    let wins = Arc::new(Mutex::new(0u32));
    let exists = Arc::new(Mutex::new(0u32));
    let mut handles = Vec::new();
    for i in 0..n {
        let col = col.clone();
        let barrier = Arc::clone(&barrier);
        let wins = Arc::clone(&wins);
        let exists = Arc::clone(&exists);
        handles.push(thread::spawn(move || {
            barrier.wait();
            match col.create_if_absent("solo", &serde_json::json!({"i": i as i64})) {
                Ok(_) => *wins.lock().unwrap() += 1,
                Err(e) if e.code() == ErrorCode::AlreadyExists => {
                    *exists.lock().unwrap() += 1;
                }
                Err(e) => panic!("unexpected: {e:?}"),
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(*wins.lock().unwrap(), 1);
    assert_eq!(*exists.lock().unwrap(), (n as u32) - 1);
    assert!(col.get("solo").unwrap().is_some());
}

#[test]
fn concurrent_delete_if_version_one_wins() {
    let (_dir, col) = open_collection();
    let r0 = col.put("d", &serde_json::json!({"n": 1})).expect("seed");
    let v0 = r0.version;
    let n = 6usize;
    let barrier = Arc::new(Barrier::new(n));
    let wins = Arc::new(Mutex::new(0u32));
    let conflicts = Arc::new(Mutex::new(0u32));
    let mut handles = Vec::new();
    for _ in 0..n {
        let col = col.clone();
        let barrier = Arc::clone(&barrier);
        let wins = Arc::clone(&wins);
        let conflicts = Arc::clone(&conflicts);
        handles.push(thread::spawn(move || {
            barrier.wait();
            match col.delete_if("d", WriteCondition::LiveEventId(v0)) {
                Ok(_) => *wins.lock().unwrap() += 1,
                Err(e) if e.code() == ErrorCode::VersionConflict => {
                    *conflicts.lock().unwrap() += 1;
                }
                Err(e) => panic!("unexpected: {e:?}"),
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(*wins.lock().unwrap(), 1);
    assert_eq!(*conflicts.lock().unwrap(), (n as u32) - 1);
    assert!(col.get("d").unwrap().is_none());
}
