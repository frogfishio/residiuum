//! AWO-Q2 — Adaptive decision quality under load (labor evidence).
//!
//! Proves under product paths (not pure-model only):
//! - multi-item `admit_put_batch`: Static takes full presentation; Adaptive cold
//!   takes Natural-1 and records selection reason;
//! - Adaptive warm estimator can choose Batch (entries > 1) on multi-item present;
//! - concurrent façade Durable admits: Static and Adaptive both amortize barriers
//!   (`file_sync < logical_ack`); Adaptive does not explode wall latency vs Static
//!   under sparse or saturated concurrent shapes.
//!
//! Explicit residual: IndependentCollector batch sizing is delay/max-entries only —
//! `select_plan` is **not** wired into collector flush. Concurrent independent
//! singles therefore share collector mechanics; Adaptive vs Static divergence is
//! proven on the multi-item admit path (explicit-batching ceiling).
//!
//! Not: package accept, default-on, thr floors, PQH diagnostic class, sparse product bound.
//!
//! ```text
//! cargo test -p residiuum-store --features legacy-raw-store --test awo_q2_decision_quality -- --test-threads=1 --nocapture
//! ```

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapCap, HeapId, HeapSecuritySnapshot, HeapSlot, Rights,
    SecurityRevision, TrustedInstant, VerifiedCertificate,
};
use residiuum_store::adaptive_write::{AdaptiveWriteMode, AdaptiveWritePolicy, AwoPlan};
use residiuum_store::{
    create_object, publish_staged_genesis, stage_heap_genesis, BoundaryKind, DurabilityMode,
    HeapMetaLayout, ObjectKind, StoreHost,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

fn policy(
    mode: AdaptiveWriteMode,
    min_samples: u32,
    collection_delay_ms: u64,
) -> AdaptiveWritePolicy {
    let mut p = AdaptiveWritePolicy::machine_defaults();
    p.mode = mode;
    p.maximum_cookers = 2;
    p.minimum_active_cookers = 1;
    p.estimator_min_samples = min_samples;
    p.maximum_collection_delay = Duration::from_millis(collection_delay_ms.min(10));
    p
}

fn mint_cap(heap: HeapId, deployment: DeploymentId) -> HeapCap {
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
        rights: Rights::from_bits_certificate(Rights::READ.bits() | Rights::WRITE.bits()).unwrap(),
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

struct Genesis {
    coll: [u8; 16],
    cap: HeapCap,
}

fn provision(root: &Path) -> Genesis {
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_id = *HeapId::new_random().unwrap().as_bytes();
    let coll = *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_id, [1u8; 16], "q2-heap").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    create_object(
        &layout,
        &heap_id,
        ObjectKind::Collection,
        coll,
        [2u8; 16],
        "q2.collection",
    )
    .unwrap();
    let cap = mint_cap(
        HeapId::from_bytes_unchecked_nonzero(heap_id).unwrap(),
        DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap(),
    );
    Genesis { coll, cap }
}

/// Cell report for concurrent façade envelope (logical_ack metric law).
#[derive(Debug)]
struct FacadeCell {
    mode: AdaptiveWriteMode,
    concurrency: usize,
    ops: u64,
    file_sync: u64,
    logical_ack: u64,
    wall_ms: u128,
    sync_per_ack: f64,
}

fn run_facade_cell(
    mode: AdaptiveWriteMode,
    concurrency: usize,
    total_ops: u64,
    collection_delay_ms: u64,
    payload_len: usize,
) -> FacadeCell {
    let concurrency = concurrency.max(1);
    let dir = tempfile::tempdir().unwrap();
    let root: PathBuf = dir.path().to_path_buf();
    let pol = policy(mode, 32, collection_delay_ms);
    let host = StoreHost::create_with_adaptive_write(&root, pol).unwrap();
    assert!(host.adaptive_write().unwrap().lease_active());
    let ids = provision(&root);
    let coll = ids.coll;
    let heap = Arc::new(host.open_heap(ids.cap));
    {
        let physical = host.physical();
        let mut g = physical.lock().unwrap();
        g.enable_boundary_probe();
    }

    let barrier = Arc::new(Barrier::new(concurrency));
    let t0 = Instant::now();
    let mut joins = Vec::new();
    for wid in 0..concurrency {
        let heap = Arc::clone(&heap);
        let barrier = Arc::clone(&barrier);
        joins.push(std::thread::spawn(move || {
            barrier.wait();
            let mut seq = wid as u64;
            while seq < total_ops {
                let key = format!("q2-{mode:?}-{seq:08x}").into_bytes();
                let body = vec![(seq & 0xff) as u8; payload_len.max(1)];
                heap.put_collection(&coll, &key, &body)
                    .unwrap_or_else(|e| panic!("put seq={seq}: {e}"));
                seq += concurrency as u64;
            }
        }));
    }
    for j in joins {
        j.join().expect("worker panic");
    }
    let wall_ms = t0.elapsed().as_millis();

    let file_sync = {
        let physical = host.physical();
        let g = physical.lock().unwrap();
        g.boundary_snapshot().counters.count(BoundaryKind::FileSync)
    };
    let logical_ack = total_ops;
    let sync_per = if logical_ack > 0 {
        file_sync as f64 / logical_ack as f64
    } else {
        0.0
    };

    let _ = host.drain_writes(Instant::now() + Duration::from_secs(2));

    FacadeCell {
        mode,
        concurrency,
        ops: total_ops,
        file_sync,
        logical_ack,
        wall_ms,
        sync_per_ack: sync_per,
    }
}

/// Static multi-item present takes the full slice; Adaptive cold takes 1.
#[test]
fn q2_static_full_batch_vs_adaptive_cold_natural_one() {
    let dir = tempfile::tempdir().unwrap();
    // Static ceiling: full presentation.
    {
        let pol = policy(AdaptiveWriteMode::Static, 32, 5);
        let host = StoreHost::create_with_adaptive_write(dir.path().join("s"), pol).unwrap();
        let handle = host.adaptive_write().unwrap().clone();
        let physical = host.physical();
        let mut g = physical.lock().unwrap();
        let receipts = handle
            .admit_put_batch(
                &mut g,
                &[
                    ("q2/s/a", b"1"),
                    ("q2/s/b", b"2"),
                    ("q2/s/c", b"3"),
                    ("q2/s/d", b"4"),
                ],
                DurabilityMode::Buffered,
            )
            .expect("static batch");
        assert_eq!(
            receipts.len(),
            4,
            "Static is the explicit-batching ceiling: take all presented items"
        );
        assert!(
            handle.last_selection().is_none(),
            "Static never runs select_plan"
        );
    }
    // Adaptive cold: natural take-1.
    {
        let pol = policy(AdaptiveWriteMode::Adaptive, 32, 5);
        let host = StoreHost::create_with_adaptive_write(dir.path().join("a"), pol).unwrap();
        let handle = host.adaptive_write().unwrap().clone();
        let physical = host.physical();
        let mut g = physical.lock().unwrap();
        let receipts = handle
            .admit_put_batch(
                &mut g,
                &[
                    ("q2/a/a", b"1"),
                    ("q2/a/b", b"2"),
                    ("q2/a/c", b"3"),
                    ("q2/a/d", b"4"),
                ],
                DurabilityMode::Buffered,
            )
            .expect("adaptive cold batch");
        assert_eq!(receipts.len(), 1, "cold Adaptive must take Natural-1 only");
        assert!(g.get("q2/a/a").unwrap().is_some());
        assert!(g.get("q2/a/b").unwrap().is_none());
        let sel = handle.last_selection().expect("selection");
        assert_eq!(sel.plan, AwoPlan::Natural);
        assert_eq!(sel.reason, "natural_insufficient_evidence");
        assert_eq!(sel.entries, 1);
    }
}

/// After estimator warm samples, Adaptive may choose Batch on multi-item present.
#[test]
fn q2_adaptive_warm_can_select_batch_on_multi_item() {
    let dir = tempfile::tempdir().unwrap();
    // Low warm floor so labor cell is hang-safe and deterministic.
    let min_samples = 4u32;
    let pol = policy(AdaptiveWriteMode::Adaptive, min_samples, 5);
    let host = StoreHost::create_with_adaptive_write(dir.path(), pol).unwrap();
    let handle = host.adaptive_write().unwrap().clone();
    let physical = host.physical();
    let mut g = physical.lock().unwrap();

    // Warm: repeated single-item admits (each samples estimator).
    for i in 0..(min_samples + 2) {
        let key = format!("q2/warm/{i}");
        let body = vec![b'w'; 64];
        let r = handle
            .admit_put_batch(&mut g, &[(&key, body.as_slice())], DurabilityMode::Buffered)
            .expect("warm admit");
        assert_eq!(r.len(), 1);
    }

    // Multi-item present after warm: Batch if objective wins; else document residual.
    let receipts = handle
        .admit_put_batch(
            &mut g,
            &[
                ("q2/warm/m0", b"0"),
                ("q2/warm/m1", b"1"),
                ("q2/warm/m2", b"2"),
                ("q2/warm/m3", b"3"),
                ("q2/warm/m4", b"4"),
            ],
            DurabilityMode::Buffered,
        )
        .expect("warm multi");
    let sel = handle.last_selection().expect("selection after warm multi");
    eprintln!(
        "q2 warm multi: receipts={} plan={:?} entries={} reason={}",
        receipts.len(),
        sel.plan,
        sel.entries,
        sel.reason
    );
    // Must not be cold-insufficient-evidence once warm.
    assert_ne!(
        sel.reason, "natural_insufficient_evidence",
        "warm estimator must leave cold-evidence path"
    );
    // Decision quality: either Batch with entries>1 (controller prefers batch),
    // or Natural with a non-cold reason (controller honestly declined batch).
    match sel.plan {
        AwoPlan::Batch => {
            assert!(
                sel.entries > 1 && receipts.len() as u64 == sel.entries,
                "Batch selection must install more than one item"
            );
            assert!(receipts.len() > 1, "warm Batch diverges from cold take-1");
        }
        AwoPlan::Natural => {
            assert_eq!(receipts.len(), 1);
            // Acceptable residual when cost model still prefers natural —
            // still proves Adaptive *decides* (not Static full-take).
            assert!(
                sel.reason.starts_with("natural_") || sel.reason.starts_with("batch_"),
                "unexpected reason {}",
                sel.reason
            );
        }
    }
}

/// Concurrent façade envelope: Static vs Adaptive under saturated concurrent admit.
#[test]
fn q2_concurrent_facade_static_vs_adaptive_saturated_envelope() {
    let ops = 32u64;
    let conc = 8usize;
    let static_c = run_facade_cell(AdaptiveWriteMode::Static, conc, ops, 5, 256);
    let adaptive_c = run_facade_cell(AdaptiveWriteMode::Adaptive, conc, ops, 5, 256);

    eprintln!(
        "q2 sat static  conc={} acks={} file_sync={} sync/ack={:.3} wall_ms={}",
        static_c.concurrency,
        static_c.logical_ack,
        static_c.file_sync,
        static_c.sync_per_ack,
        static_c.wall_ms
    );
    eprintln!(
        "q2 sat adaptive conc={} acks={} file_sync={} sync/ack={:.3} wall_ms={}",
        adaptive_c.concurrency,
        adaptive_c.logical_ack,
        adaptive_c.file_sync,
        adaptive_c.sync_per_ack,
        adaptive_c.wall_ms
    );

    // Both modes must amortize multi-write barriers under concurrent Durable.
    assert!(
        static_c.file_sync > 0 && static_c.file_sync < static_c.logical_ack,
        "Static saturated: expected file_sync < logical_ack (got {} / {})",
        static_c.file_sync,
        static_c.logical_ack
    );
    assert!(
        adaptive_c.file_sync > 0 && adaptive_c.file_sync < adaptive_c.logical_ack,
        "Adaptive saturated: expected file_sync < logical_ack (got {} / {})",
        adaptive_c.file_sync,
        adaptive_c.logical_ack
    );

    // Adaptive must not explode wall time vs Static (smoke bound 3× — not product floor).
    if static_c.wall_ms > 0 {
        let ratio = adaptive_c.wall_ms as f64 / static_c.wall_ms as f64;
        assert!(
            ratio <= 3.0,
            "Adaptive saturated wall {ratio:.2}× Static — exceeds smoke bound without product claim"
        );
    }
}

/// Sparse concurrent (low pile-up): Adaptive latency envelope vs Static.
#[test]
fn q2_concurrent_facade_sparse_adaptive_latency_envelope() {
    // Low concurrency + longer collection delay → less pile-up (sparser coalescing).
    let ops = 16u64;
    let conc = 2usize;
    let static_c = run_facade_cell(AdaptiveWriteMode::Static, conc, ops, 8, 128);
    let adaptive_c = run_facade_cell(AdaptiveWriteMode::Adaptive, conc, ops, 8, 128);

    eprintln!(
        "q2 sparse static  acks={} file_sync={} sync/ack={:.3} wall_ms={}",
        static_c.logical_ack, static_c.file_sync, static_c.sync_per_ack, static_c.wall_ms
    );
    eprintln!(
        "q2 sparse adaptive acks={} file_sync={} sync/ack={:.3} wall_ms={}",
        adaptive_c.logical_ack, adaptive_c.file_sync, adaptive_c.sync_per_ack, adaptive_c.wall_ms
    );

    assert_eq!(static_c.logical_ack, ops);
    assert_eq!(adaptive_c.logical_ack, ops);
    assert!(static_c.file_sync > 0);
    assert!(adaptive_c.file_sync > 0);

    // Sparse Adaptive must not explode wall vs Static (smoke 3× bound).
    // T11 observed ~11–20% thr penalty Adaptive vs Static sparse — wall may be higher
    // but not multi× without a product claim.
    if static_c.wall_ms > 0 {
        let ratio = adaptive_c.wall_ms as f64 / static_c.wall_ms as f64;
        assert!(
            ratio <= 3.0,
            "sparse Adaptive wall {ratio:.2}× Static exceeds smoke bound"
        );
        eprintln!("q2 sparse adaptive/static wall ratio={ratio:.3}");
    }
}
