//! AWO-Q1.3 — Concurrent façade admit + clean reopen correctness.
//!
//! Proves under genuine concurrent `HeapStore` callers (Static + Adaptive):
//! - every acknowledged seq binds to key + body hash + length;
//! - clean close + normal product reopen preserves every ack exactly once;
//! - pre-close value digest matches reopened-state digest;
//! - **segment rotation during concurrent collection** (low seal threshold from
//!   the start — not deferred to a sequential phase);
//! - after reopen, a **coverage-aware full collection scan** matches the
//!   acknowledged ledger key/hash set exactly (no unexpected records);
//! - concurrent-phase `file_sync` / `logical_ack` evidence (barrier snapshot
//!   taken immediately after concurrent joins — not mixed with other writes).
//!
//! **Hang note:** do not use a tight spin-wait outstanding pool under Durable AWO
//! (spinners burn CPU while collectors wait). Outstanding is modeled as the number
//! of concurrent façade threads (each holds one in-flight put_collection).
//! Concurrent automatic rotation must complete; hang = product defect.
//!
//! Crash-boundary semantics are **out of scope** (later crash matrix).
//!
//! ```text
//! cargo test -p residiuum-store --features legacy-raw-store --test awo_q13_reopen_correctness -- --test-threads=1
//! ```

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapCap, HeapId, HeapSecuritySnapshot, HeapSlot, Rights,
    SecurityRevision, TrustedInstant, VerifiedCertificate,
};
use residiuum_store::adaptive_write::{AdaptiveWriteMode, AdaptiveWritePolicy};
use residiuum_store::{
    create_object, publish_staged_genesis, stage_heap_genesis, BoundaryKind, CollectionScanPage,
    HeapMetaLayout, ObjectKind, StoreHost,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
struct ExpectedOp {
    seq: u64,
    key: Vec<u8>,
    body: Vec<u8>,
    body_hash: [u8; 32],
}

fn fill_body(seed: u64, seq: u64, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len.max(1)];
    let mut state = seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(seq.wrapping_mul(0xBF58_476D_1CE4_E5B9));
    for b in &mut out {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *b = (state >> 33) as u8;
    }
    out
}

fn body_hash(body: &[u8]) -> [u8; 32] {
    *blake3::hash(body).as_bytes()
}

fn chain_digest(ops: &[(u64, &[u8], [u8; 32], u64)]) -> String {
    let mut prev: Option<[u8; 32]> = None;
    for (seq, key, hash, len) in ops {
        let mut h = blake3::Hasher::new();
        if let Some(p) = prev {
            h.update(&p);
        }
        h.update(&seq.to_le_bytes());
        h.update(key);
        h.update(hash);
        h.update(&len.to_le_bytes());
        prev = Some(*h.finalize().as_bytes());
    }
    prev.map(|p| hex_encode(&p))
        .unwrap_or_else(|| "empty".into())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

struct GenesisIds {
    coll: [u8; 16],
    cap: HeapCap,
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

fn provision(root: &Path) -> GenesisIds {
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_id = *HeapId::new_random().unwrap().as_bytes();
    let coll = *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_id, [1u8; 16], "q13-heap").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    create_object(
        &layout,
        &heap_id,
        ObjectKind::Collection,
        coll,
        [2u8; 16],
        "q13.collection",
    )
    .unwrap();
    let cap = mint_cap(
        HeapId::from_bytes_unchecked_nonzero(heap_id).unwrap(),
        DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap(),
    );
    GenesisIds { coll, cap }
}

fn policy_for(mode: AdaptiveWriteMode) -> AdaptiveWritePolicy {
    let mut policy = AdaptiveWritePolicy::machine_defaults();
    policy.mode = mode;
    policy.maximum_cookers = 2;
    policy.minimum_active_cookers = 1;
    // Short collection window (same posture as Q1.2).
    policy.maximum_collection_delay = Duration::from_millis(5);
    policy
}

/// Full coverage-aware scan: pages until `!has_more`; requires `complete`.
fn scan_all_complete(heap: &residiuum_store::HeapStore, coll: &[u8; 16]) -> CollectionScanPage {
    let mut entries = Vec::new();
    let mut incomplete = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    let mut examined = 0usize;
    loop {
        let page = heap
            .scan_collection_page(coll, 64, after.as_deref())
            .expect("scan_collection_page");
        examined += page.examined;
        entries.extend(page.entries);
        incomplete.extend(page.incomplete);
        if !page.has_more {
            let complete = incomplete.is_empty();
            assert!(
                complete,
                "coverage-aware scan incomplete: holes={}",
                incomplete.len()
            );
            return CollectionScanPage {
                entries,
                incomplete,
                examined,
                complete: true,
                has_more: false,
                last_key: page.last_key,
            };
        }
        after = page.last_key;
    }
}

fn ledger_key_hash_set(ops: &[ExpectedOp]) -> BTreeSet<(Vec<u8>, [u8; 32])> {
    ops.iter()
        .map(|op| (op.key.clone(), op.body_hash))
        .collect()
}

fn scan_key_hash_set(page: &CollectionScanPage) -> BTreeSet<(Vec<u8>, [u8; 32])> {
    page.entries
        .iter()
        .map(|(k, body)| (k.clone(), body_hash(body)))
        .collect()
}

#[derive(Debug)]
struct CellReport {
    seed: u64,
    concurrency: usize,
    issued: u64,
    acked: u64,
    pre_digest: String,
    reopen_digest: String,
    file_sync: u64,
    segment_rotate: u64,
    sync_per_logical_ack: f64,
    scanned_keys: usize,
}

/// Concurrent façade puts with fixed per-thread work (no spin semaphore).
///
/// When `rotate_during` is true, a low seal threshold is active **before**
/// concurrent writers start so automatic `SegmentRotate` occurs while the
/// IndependentCollector is operating. Hang under that posture is a product
/// defect (do not defer seal to a sequential epilogue).
fn run_cell(
    mode: AdaptiveWriteMode,
    seed: u64,
    concurrency: usize,
    total_ops: u64,
    rotate_during: bool,
    payload_len: usize,
) -> CellReport {
    let concurrency = concurrency.max(1);
    let dir = tempfile::tempdir().unwrap();
    let root: PathBuf = dir.path().to_path_buf();
    let policy = policy_for(mode);
    let host = StoreHost::create_with_adaptive_write(&root, policy.clone()).unwrap();
    assert!(
        host.adaptive_write().unwrap().lease_active(),
        "AWO lease must be active for concurrent collection"
    );
    let ids = provision(&root);
    let coll = ids.coll;
    let heap = Arc::new(host.open_heap(ids.cap.clone()));

    {
        let physical = host.physical();
        let mut g = physical.lock().unwrap();
        g.enable_boundary_probe();
        if rotate_during {
            // Active from the beginning — ordinary automatic rotation under load.
            g.set_seal_threshold(8 * 1024);
        }
    }

    let expected: Arc<Vec<ExpectedOp>> = Arc::new(
        (0..total_ops)
            .map(|seq| {
                let key = format!("q13-s{seed:04x}-{seq:08x}").into_bytes();
                let body = fill_body(seed, seq, payload_len);
                let hash = body_hash(&body);
                ExpectedOp {
                    seq,
                    key,
                    body,
                    body_hash: hash,
                }
            })
            .collect(),
    );

    // Multi-terminal detector: seq → first ack only.
    let acked: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(vec![false; total_ops as usize]));
    let barrier = Arc::new(Barrier::new(concurrency));
    let mut joins = Vec::new();

    for wid in 0..concurrency {
        let heap = Arc::clone(&heap);
        let expected = Arc::clone(&expected);
        let acked = Arc::clone(&acked);
        let barrier = Arc::clone(&barrier);
        joins.push(std::thread::spawn(move || {
            barrier.wait();
            // Partition sequences across workers (no shared credit pool).
            let mut seq = wid as u64;
            while seq < total_ops {
                let op = &expected[seq as usize];
                heap.put_collection(&coll, &op.key, &op.body)
                    .unwrap_or_else(|e| panic!("put_collection seq={seq}: {e}"));
                {
                    let mut g = acked.lock().unwrap();
                    assert!(!g[seq as usize], "double-ack for seq={seq}");
                    g[seq as usize] = true;
                }
                seq += concurrency as u64;
            }
        }));
    }
    for j in joins {
        j.join().expect("worker panic invalidates evidence");
    }

    {
        let g = acked.lock().unwrap();
        for (s, ok) in g.iter().enumerate() {
            assert!(*ok, "missing ack for seq={s}");
        }
    }

    // Barrier counters for the concurrent phase only (no post-write epilogue).
    let (file_sync, appends, segment_rotate) = {
        let physical = host.physical();
        let g = physical.lock().unwrap();
        let snap = g.boundary_snapshot();
        (
            snap.counters.count(BoundaryKind::FileSync),
            snap.counters.count(BoundaryKind::AppendEncodedFrame),
            snap.counters.count(BoundaryKind::SegmentRotate),
        )
    };

    let all_ops: Vec<&ExpectedOp> = expected.iter().collect();

    let mut pre_chain: Vec<(u64, Vec<u8>, [u8; 32], u64)> = Vec::with_capacity(all_ops.len());
    for op in &all_ops {
        let got = heap
            .get_collection(&coll, &op.key)
            .expect("get")
            .unwrap_or_else(|| panic!("missing key before close seq={}", op.seq));
        assert_eq!(got, op.body, "pre-close body mismatch seq={}", op.seq);
        assert_eq!(body_hash(&got), op.body_hash);
        pre_chain.push((op.seq, op.key.clone(), op.body_hash, op.body.len() as u64));
    }
    let pre_refs: Vec<(u64, &[u8], [u8; 32], u64)> = pre_chain
        .iter()
        .map(|(s, k, h, l)| (*s, k.as_slice(), *h, *l))
        .collect();
    let pre_digest = chain_digest(&pre_refs);

    host.drain_writes(Instant::now() + Duration::from_secs(3))
        .expect("drain");
    drop(heap);
    drop(host);

    // Normal product reopen (not crash salvage).
    let host2 = StoreHost::open_with_adaptive_write(&root, policy).unwrap();
    let heap2 = host2.open_heap(ids.cap);

    let mut reopen_chain: Vec<(u64, Vec<u8>, [u8; 32], u64)> = Vec::with_capacity(all_ops.len());
    for op in &all_ops {
        let got = heap2
            .get_collection(&coll, &op.key)
            .expect("reopen get")
            .unwrap_or_else(|| panic!("missing after reopen seq={}", op.seq));
        assert_eq!(got, op.body, "reopen body mismatch seq={}", op.seq);
        let h = body_hash(&got);
        assert_eq!(h, op.body_hash, "reopen hash mismatch seq={}", op.seq);
        reopen_chain.push((op.seq, op.key.clone(), h, got.len() as u64));
    }
    let reopen_refs: Vec<(u64, &[u8], [u8; 32], u64)> = reopen_chain
        .iter()
        .map(|(s, k, h, l)| (*s, k.as_slice(), *h, *l))
        .collect();
    let reopen_digest = chain_digest(&reopen_refs);
    assert_eq!(
        pre_digest, reopen_digest,
        "pre-close digest must equal reopen digest (mode={mode:?} seed={seed})"
    );

    // Exact recovered set via coverage-aware scan (not point-get alone).
    let page = scan_all_complete(&heap2, &coll);
    let expected_set = ledger_key_hash_set(&expected);
    let scanned_set = scan_key_hash_set(&page);
    if expected_set != scanned_set {
        let only_ledger: BTreeSet<_> = expected_set.difference(&scanned_set).cloned().collect();
        let only_scan: BTreeSet<_> = scanned_set.difference(&expected_set).cloned().collect();
        let mut only_ledger_keys: BTreeMap<Vec<u8>, [u8; 32]> = BTreeMap::new();
        for (k, h) in only_ledger {
            only_ledger_keys.insert(k, h);
        }
        let mut only_scan_keys: BTreeMap<Vec<u8>, [u8; 32]> = BTreeMap::new();
        for (k, h) in only_scan {
            only_scan_keys.insert(k, h);
        }
        panic!(
            "reopened scan set ≠ acknowledged ledger (mode={mode:?} seed={seed}): \
             missing_from_scan={} unexpected_in_scan={} examined={}",
            only_ledger_keys.len(),
            only_scan_keys.len(),
            page.examined
        );
    }
    assert_eq!(
        page.entries.len(),
        expected.len(),
        "scan entry count must equal ledger size"
    );

    let logical_ack = all_ops.len() as u64;
    let sync_per = if logical_ack > 0 {
        file_sync as f64 / logical_ack as f64
    } else {
        0.0
    };
    // Amortization evidence: without forced rotation, Durable collection should
    // share barriers (file_sync < acks). With rotate_during, each SegmentRotate
    // also FileSyncs — sync count may meet or exceed acks; still require >0 and
    // SegmentRotate>0 as the concurrent-seal claim.
    if concurrency > 1 && total_ops >= 16 && !rotate_during {
        assert!(
            file_sync > 0 && file_sync < logical_ack,
            "expected concurrent multi-write barrier amortization: file_sync={file_sync} acks={logical_ack}"
        );
    } else if concurrency > 1 && total_ops >= 16 {
        assert!(
            file_sync > 0,
            "expected FileSync activity under concurrent Durable admits"
        );
    }
    assert!(
        appends >= total_ops,
        "append count {appends} < concurrent acks {total_ops}"
    );
    if rotate_during {
        assert!(
            segment_rotate > 0,
            "rotate_during cell expected SegmentRotate>0 during concurrent phase (got 0); \
             seal threshold was active from the start"
        );
    }

    let _ = host2.drain_writes(Instant::now() + Duration::from_secs(2));

    CellReport {
        seed,
        concurrency,
        issued: logical_ack,
        acked: logical_ack,
        pre_digest,
        reopen_digest,
        file_sync,
        segment_rotate,
        sync_per_logical_ack: sync_per,
        scanned_keys: page.entries.len(),
    }
}

#[test]
fn q13_static_concurrent_reopen_matrix() {
    // rotate_during=true cells: low seal from t0; payload sized to force rotation
    // while IndependentCollector is live.
    let cells = [
        // seed, concurrency, ops, rotate_during, payload
        (1u64, 4usize, 16u64, false, 128usize),
        (42, 4, 24, true, 2048),
        (99, 8, 32, true, 2048),
    ];
    for (seed, conc, ops, rot, plen) in cells {
        let r = run_cell(AdaptiveWriteMode::Static, seed, conc, ops, rot, plen);
        assert_eq!(r.pre_digest, r.reopen_digest);
        assert_eq!(r.acked, r.issued);
        assert_eq!(r.scanned_keys as u64, r.acked);
        eprintln!(
            "q13 static seed={} conc={} acked={} file_sync={} rotate={} sync/ack={:.3} scan={} digest={}",
            r.seed,
            r.concurrency,
            r.acked,
            r.file_sync,
            r.segment_rotate,
            r.sync_per_logical_ack,
            r.scanned_keys,
            &r.pre_digest[..16.min(r.pre_digest.len())]
        );
    }
}

#[test]
fn q13_adaptive_concurrent_reopen_matrix() {
    let cells = [
        (7u64, 4usize, 16u64, false, 128usize),
        (42, 4, 24, true, 2048),
        (1001, 8, 32, true, 2048),
    ];
    for (seed, conc, ops, rot, plen) in cells {
        let r = run_cell(AdaptiveWriteMode::Adaptive, seed, conc, ops, rot, plen);
        assert_eq!(r.pre_digest, r.reopen_digest);
        assert_eq!(r.acked, r.issued);
        assert_eq!(r.scanned_keys as u64, r.acked);
        eprintln!(
            "q13 adaptive seed={} conc={} acked={} file_sync={} rotate={} sync/ack={:.3} scan={} digest={}",
            r.seed,
            r.concurrency,
            r.acked,
            r.file_sync,
            r.segment_rotate,
            r.sync_per_logical_ack,
            r.scanned_keys,
            &r.pre_digest[..16.min(r.pre_digest.len())]
        );
    }
}

/// Same seed/content must reopen-correct under Static and Adaptive (digest equal).
/// Uses concurrent rotation so both modes exercise seal × collector.
#[test]
fn q13_static_and_adaptive_same_seed_both_reopen_ok() {
    let seed = 2026u64;
    let s = run_cell(AdaptiveWriteMode::Static, seed, 4, 20, true, 2048);
    let a = run_cell(AdaptiveWriteMode::Adaptive, seed, 4, 20, true, 2048);
    assert_eq!(s.acked, 20);
    assert_eq!(a.acked, 20);
    assert!(s.segment_rotate > 0, "static same-seed cell must rotate");
    assert!(a.segment_rotate > 0, "adaptive same-seed cell must rotate");
    assert_eq!(s.pre_digest, s.reopen_digest);
    assert_eq!(a.pre_digest, a.reopen_digest);
    assert_eq!(
        s.pre_digest, a.pre_digest,
        "Static and Adaptive must install identical logical content for same seed"
    );
    assert_eq!(s.scanned_keys, a.scanned_keys);
}
