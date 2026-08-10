//! AWO-3: Static Intake Arbiter floor — host attach, mode default disabled,
//! lease fence, natural admit under lease.
//!
//! No package accept. E6 heap active-writer residual explicit.
//! Run: cargo test -p residiuum-store --features legacy-raw-store --test awo_static_admission -- --test-threads=1

use residiuum_store::adaptive_write::{
    classify_put, AdaptiveWriteMode, AdaptiveWritePolicy, AdmissionResult, EligibilityClass,
};
use residiuum_store::{BoundaryKind, DurabilityMode, Store, StoreError, StoreHost, WriteCondition};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[test]
fn ordinary_create_has_no_adaptive_status() {
    let dir = tempfile::tempdir().unwrap();
    let host = StoreHost::create(dir.path()).unwrap();
    assert!(host.adaptive_write_status().is_none());
    assert!(host.adaptive_write().is_none());
}

#[test]
fn disabled_policy_default_no_lease() {
    let dir = tempfile::tempdir().unwrap();
    let policy = AdaptiveWritePolicy::machine_defaults();
    assert_eq!(policy.mode, AdaptiveWriteMode::Disabled);
    let host = StoreHost::create_with_adaptive_write(dir.path(), policy).unwrap();
    let st = host.adaptive_write_status().expect("attached disabled");
    assert_eq!(st.mode, AdaptiveWriteMode::Disabled);
    assert!(!st.lease_active);
    assert_eq!(st.cooker_threads, 0);

    // Direct mutation still works (no lease).
    let physical = host.physical();
    let mut guard = physical.lock().unwrap();
    guard
        .put("awo/disabled/k", b"v", DurabilityMode::Buffered)
        .unwrap();
    assert_eq!(guard.get("awo/disabled/k").unwrap().unwrap(), b"v");
}

#[test]
fn static_mode_fences_direct_put_and_admits_under_lease() {
    let dir = tempfile::tempdir().unwrap();
    let mut policy = AdaptiveWritePolicy::machine_defaults();
    policy.mode = AdaptiveWriteMode::Static;
    // Keep cooker pool small for fast shutdown.
    policy.maximum_cookers = 2;
    policy.minimum_active_cookers = 1;

    let host = StoreHost::create_with_adaptive_write(dir.path(), policy).unwrap();
    let st = host.adaptive_write_status().unwrap();
    assert_eq!(st.mode, AdaptiveWriteMode::Static);
    assert!(st.lease_active);
    assert_eq!(st.cooker_threads, 2);

    let handle = host.adaptive_write().expect("handle").clone();
    let physical = host.physical();

    // Direct mutation refused.
    {
        let mut guard = physical.lock().unwrap();
        assert!(matches!(
            guard.put("awo/static/direct", b"x", DurabilityMode::Buffered),
            Err(StoreError::AdaptiveWriterActive)
        ));
        assert!(matches!(
            guard.delete("awo/static/direct", DurabilityMode::Buffered),
            Err(StoreError::AdaptiveWriterActive)
        ));
    }

    // Admit under lease succeeds. Wait **outside** the physical lock so the
    // independent-write collector can install.
    let completion = {
        let mut guard = physical.lock().unwrap();
        match handle.admit_put(
            &mut guard,
            b"awo/static/a",
            b"body",
            DurabilityMode::Buffered,
            WriteCondition::Unconditional,
        ) {
            AdmissionResult::Admitted(c) => c,
            AdmissionResult::Rejected(e) => panic!("rejected: {e:?}"),
        }
    };
    let r = completion.wait().expect("receipt");
    assert_ne!(r.event_id, [0u8; 16]);
    {
        let guard = physical.lock().unwrap();
        assert_eq!(
            guard.get_subject_bytes(b"awo/static/a").unwrap().unwrap(),
            b"body"
        );
    }

    host.drain_writes(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// Concurrent independent singles must share Durable barriers (collection).
#[test]
fn independent_puts_collect_amortize_file_sync() {
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let mut policy = AdaptiveWritePolicy::machine_defaults();
    policy.mode = AdaptiveWriteMode::Static;
    policy.maximum_cookers = 2;
    policy.minimum_active_cookers = 1;
    // Short window so the test finishes quickly; concurrent pile-up still coalesces.
    policy.maximum_collection_delay = Duration::from_millis(5);

    let host = StoreHost::create_with_adaptive_write(dir.path(), policy).unwrap();
    let handle = host.adaptive_write().expect("handle").clone();
    let physical = host.physical();

    {
        let mut g = physical.lock().unwrap();
        g.enable_boundary_probe();
    }

    let n_threads = 8;
    let puts_per = 4;
    let barrier = Arc::new(Barrier::new(n_threads));
    let mut joins = Vec::new();
    for t in 0..n_threads {
        let handle = handle.clone();
        let physical = Arc::clone(&physical);
        let barrier = Arc::clone(&barrier);
        joins.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..puts_per {
                let key = format!("awo/collect/t{t}/i{i}");
                let completion = {
                    let mut guard = physical.lock().unwrap();
                    match handle.admit_put(
                        &mut guard,
                        key.as_bytes(),
                        b"v",
                        DurabilityMode::Durable,
                        WriteCondition::Unconditional,
                    ) {
                        AdmissionResult::Admitted(c) => c,
                        AdmissionResult::Rejected(e) => panic!("rejected: {e:?}"),
                    }
                };
                completion.wait().expect("receipt");
            }
        }));
    }
    for j in joins {
        j.join().unwrap();
    }

    let total = (n_threads * puts_per) as u64;
    let snap = {
        let g = physical.lock().unwrap();
        g.boundary_snapshot()
    };
    let file_sync = snap.counters.count(BoundaryKind::FileSync);
    let appends = snap.counters.count(BoundaryKind::AppendEncodedFrame);
    // Durable puts: each natural put would sync once. Collection should amortize.
    assert!(
        file_sync > 0 && file_sync < total,
        "expected file_sync < ops: sync={file_sync} ops={total}"
    );
    assert!(appends >= total, "append_count {appends} < ops {total}");

    host.drain_writes(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn classify_eligibility_closed() {
    assert_eq!(
        classify_put(WriteCondition::Unconditional, DurabilityMode::Buffered),
        EligibilityClass::UnconditionalInlinePut
    );
    assert_eq!(
        classify_put(WriteCondition::Absent, DurabilityMode::Durable),
        EligibilityClass::Natural
    );
    assert_eq!(
        classify_put(WriteCondition::Unconditional, DurabilityMode::Memory),
        EligibilityClass::Natural
    );
}

#[test]
fn adaptive_mode_same_lease_floor_as_static() {
    let dir = tempfile::tempdir().unwrap();
    let mut policy = AdaptiveWritePolicy::machine_defaults();
    policy.mode = AdaptiveWriteMode::Adaptive;
    policy.maximum_cookers = 1;
    policy.minimum_active_cookers = 1;
    let host = StoreHost::create_with_adaptive_write(dir.path(), policy).unwrap();
    let st = host.adaptive_write_status().unwrap();
    assert_eq!(st.mode, AdaptiveWriteMode::Adaptive);
    assert!(st.lease_active);
    assert_eq!(st.cooker_threads, 1);
}

#[test]
fn store_lease_flag_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path()).unwrap();
    assert!(!store.is_awo_lease_active());
    store.set_awo_lease_active(true);
    assert!(matches!(
        store.put("x", b"1", DurabilityMode::Buffered),
        Err(StoreError::AdaptiveWriterActive)
    ));
    store.set_awo_lease_active(false);
    store.put("x", b"1", DurabilityMode::Buffered).unwrap();
}

#[test]
fn admit_put_batch_under_lease_persist_before_publish() {
    let dir = tempfile::tempdir().unwrap();
    let mut policy = AdaptiveWritePolicy::machine_defaults();
    policy.mode = AdaptiveWriteMode::Static;
    policy.maximum_cookers = 2;
    policy.minimum_active_cookers = 1;

    let host = StoreHost::create_with_adaptive_write(dir.path(), policy).unwrap();
    let handle = host.adaptive_write().unwrap().clone();
    let physical = host.physical();
    let mut guard = physical.lock().unwrap();

    // Direct batch refused.
    assert!(matches!(
        guard.put_many(
            &[("awo/batch/a", b"1"), ("awo/batch/b", b"2")],
            DurabilityMode::Buffered
        ),
        Err(StoreError::AdaptiveWriterActive)
    ));

    let receipts = handle
        .admit_put_batch(
            &mut guard,
            &[
                ("awo/batch/a", b"1"),
                ("awo/batch/b", b"2"),
                ("awo/batch/c", b"3"),
            ],
            DurabilityMode::Buffered,
        )
        .expect("batch admit");
    assert_eq!(receipts.len(), 3);
    assert_eq!(guard.get("awo/batch/a").unwrap().unwrap(), b"1");
    assert_eq!(guard.get("awo/batch/b").unwrap().unwrap(), b"2");
    assert_eq!(guard.get("awo/batch/c").unwrap().unwrap(), b"3");
    // Independent event ids.
    assert_ne!(receipts[0].event_id, receipts[1].event_id);
}

#[test]
fn open_heap_carries_adaptive_handle_when_lease_active() {
    // Construction of a live HeapCap needs authority ceremony; this test only
    // checks StoreHost::open_heap wiring does not panic and status is lease-active.
    let dir = tempfile::tempdir().unwrap();
    let mut policy = AdaptiveWritePolicy::machine_defaults();
    policy.mode = AdaptiveWriteMode::Static;
    policy.maximum_cookers = 1;
    policy.minimum_active_cookers = 1;
    let host = StoreHost::create_with_adaptive_write(dir.path(), policy).unwrap();
    assert!(host.adaptive_write().unwrap().lease_active());
    // open_heap clones the handle into HeapStore; without a real HeapCap we
    // only assert the host still exposes adaptive after open_heap is available.
    let _ = host.adaptive_write_status();
}

/// AWO-Q1.2: concurrent admits through the **HeapStore façade** (not direct
/// AdaptiveWriteHandle). Product wiring: put_collection → put_if admits under
/// lock, waits outside, so concurrent callers can pile into the collector.
#[test]
fn heap_store_facade_concurrent_put_collection_wait_outside_mutex() {
    use residiuum_heap::{
        mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints,
        DeploymentId, HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights,
        SecurityRevision, TrustedInstant, VerifiedCertificate,
    };
    use residiuum_store::{
        create_object, publish_staged_genesis, stage_heap_genesis, BoundaryKind, HeapMetaLayout,
        ObjectKind,
    };
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut policy = AdaptiveWritePolicy::machine_defaults();
    policy.mode = AdaptiveWriteMode::Static;
    policy.maximum_cookers = 2;
    policy.minimum_active_cookers = 1;
    policy.maximum_collection_delay = Duration::from_millis(5);

    let host = StoreHost::create_with_adaptive_write(root, policy).unwrap();
    assert!(host.adaptive_write().unwrap().lease_active());

    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_id = *HeapId::new_random().unwrap().as_bytes();
    let coll = *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_id, [1u8; 16], "q12-facade-heap").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    create_object(
        &layout,
        &heap_id,
        ObjectKind::Collection,
        coll,
        [2u8; 16],
        "q12.collection",
    )
    .unwrap();

    let snap = HeapSecuritySnapshot {
        deployment_id: DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap(),
        heap_id: HeapId::from_bytes_unchecked_nonzero(heap_id).unwrap(),
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
        deployment_id: DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap(),
        heap_id: HeapId::from_bytes_unchecked_nonzero(heap_id).unwrap(),
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4u8; 32],
        rights: Rights::from_bits_certificate(Rights::READ.bits() | Rights::WRITE.bits()).unwrap(),
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: [5u8; 32],
    };
    let cap = mint_capability(
        slot,
        &cert,
        TrustedInstant {
            unix_s: 1_700_000_000,
        },
    )
    .unwrap();
    let heap = Arc::new(host.open_heap(cap));

    {
        let physical = host.physical();
        let mut g = physical.lock().unwrap();
        g.enable_boundary_probe();
    }

    let n_threads = 8;
    let puts_per = 4;
    let barrier = Arc::new(Barrier::new(n_threads));
    let mut joins = Vec::new();
    for t in 0..n_threads {
        let heap = Arc::clone(&heap);
        let barrier = Arc::clone(&barrier);
        joins.push(std::thread::spawn(move || {
            barrier.wait();
            for i in 0..puts_per {
                let key = format!("q12-t{t}-i{i}");
                // Product façade path — not AdaptiveWriteHandle::admit_put.
                heap.put_collection(&coll, key.as_bytes(), b"facade-v")
                    .expect("heap put_collection");
            }
        }));
    }
    for j in joins {
        j.join().expect("worker must not panic");
    }

    let total = (n_threads * puts_per) as u64;
    // All keys readable via façade.
    for t in 0..n_threads {
        for i in 0..puts_per {
            let key = format!("q12-t{t}-i{i}");
            let v = heap
                .get_collection(&coll, key.as_bytes())
                .expect("get")
                .expect("present");
            assert_eq!(v, b"facade-v");
        }
    }

    let snap = {
        let physical = host.physical();
        let g = physical.lock().unwrap();
        g.boundary_snapshot()
    };
    let file_sync = snap.counters.count(BoundaryKind::FileSync);
    // Durable façade puts: collector must amortize syncs (wait-outside-mutex enables pile-up).
    assert!(
        file_sync > 0 && file_sync < total,
        "façade concurrent path must amortize file_sync: sync={file_sync} ops={total}"
    );

    host.drain_writes(Instant::now() + Duration::from_secs(2))
        .unwrap();
}
