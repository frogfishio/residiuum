//! CR-ATMR6-004: operable limits, tail-only scan, incremental catalogue.

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId,
    CoordinationScope, HeapId, MutationKind, ObjectIdentity, PlanMutation, ResourceLimits,
    VersionId,
};
use residiuum_store::{
    arm_failpoint_once, atomic_stage_checkpoint_path, clear_failpoints, AtomicStageLimits,
    FailpointAction, Store, StoreError, ADMISSION_OUTSTANDING_ATOMICS,
};
use std::fs;
use std::io::Write;
use std::sync::Mutex;

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    guard
}

const FRONTIER: [u8; 32] = [0xA1; 32];

fn aid(n: u8) -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = n;
    AtomicId::from_bytes(b).unwrap()
}

fn cid() -> CollectionId {
    let mut b = [0u8; 16];
    b[0] = 2;
    CollectionId::from_bytes(b).unwrap()
}

fn vid(n: u8) -> VersionId {
    let mut b = [0u8; 16];
    b[0] = n;
    VersionId::from_bytes(b).unwrap()
}

fn member(id: AtomicId, ev: u8, value: &[u8]) -> AtomicMember {
    AtomicMember {
        atomic_id: id,
        ordinal: 0,
        object_identity: ObjectIdentity::new(cid(), CanonicalKey::String("k".into())),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(value).as_bytes()),
        event_id: vid(ev),
    }
}

fn plan(heap: HeapId, m: &AtomicMember, value: &[u8]) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: m.atomic_id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: vec![PlanMutation {
            kind: m.member_kind,
            collection_id: m.object_identity.collection_id,
            key: m.object_identity.key.clone(),
            encoded_value: Some(value.to_vec()),
            if_version: m.before_version,
        }],
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

fn stage_value(store: &mut Store, id: AtomicId, ev: u8, value: &[u8]) {
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member(id, ev, value);
    let p = plan(heap, &m, value);
    let mut stage = store.atomic_stage().unwrap();
    stage
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    stage.append_staged(m, value.to_vec()).unwrap();
}

#[test]
fn operable_limits_are_derived_and_exposed() {
    let limits = AtomicStageLimits::operable();
    assert_eq!(limits.max_atomics, ADMISSION_OUTSTANDING_ATOMICS);
    assert_eq!(limits.max_segment_bytes, 64 * 1024 * 1024);
    assert_eq!(
        limits.max_payload_bytes,
        u64::from(ADMISSION_OUTSTANDING_ATOMICS) * 8 * 1024 * 1024
    );
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let report = store.atomic_stage().unwrap().open_report();
    assert_eq!(report.limits, limits);
}

#[test]
fn covered_64mib_segment_plus_small_tail_opens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    stage_value(&mut store, aid(9), 3, b"secret");
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    let big = segs.join("covered-64m.residiuum");
    let f = fs::File::create(&big).unwrap();
    f.set_len(64 * 1024 * 1024).unwrap();
    drop(f);
    drop(store.atomic_stage().unwrap());
    let mut tail = fs::OpenOptions::new().append(true).open(&big).unwrap();
    tail.write_all(b"tail").unwrap();
    drop(tail);
    let stage = store.atomic_stage().unwrap();
    let report = stage.open_report();
    assert!(
        report.files_tailed >= 1 || report.files_skipped >= 1,
        "covered 64MiB media must open, report={report:?}"
    );
    assert!(
        report.bytes_scanned < 8 * 1024 * 1024,
        "dirty tail must be the scan charge, scanned {}",
        report.bytes_scanned
    );
    assert!(stage.kernel().placement(aid(9)).is_some());
}

#[test]
fn one_unit_over_payload_refuses_before_append() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let mut limits = AtomicStageLimits::operable();
    limits.max_payload_bytes = 8;
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let value = vec![0u8; 9];
    let m = member(aid(9), 3, &value);
    let p = plan(heap, &m, &value);
    let mut stage = store.atomic_stage_with_limits(limits).unwrap();
    stage
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    match stage.append_staged(m, value) {
        Err(StoreError::AtomicStage(msg)) => {
            assert!(
                msg.contains("admission") && msg.contains("payload"),
                "expected pre-append admission refusal, got {msg}"
            );
        }
        Ok(()) => panic!("one-unit-over payload must refuse before append"),
        Err(other) => panic!("expected AtomicStage admission, got {other}"),
    }
}

#[test]
fn checkpoint_does_not_grow_with_payload_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    stage_value(&mut store, aid(1), 1, &vec![0x11; 32 * 1024]);
    let first = fs::metadata(atomic_stage_checkpoint_path(store.paths()))
        .unwrap()
        .len();
    stage_value(&mut store, aid(2), 2, &vec![0x22; 32 * 1024]);
    let second = fs::metadata(atomic_stage_checkpoint_path(store.paths()))
        .unwrap()
        .len();
    assert!(
        second < first + 8 * 1024,
        "checkpoint must not copy payload history, first={first} second={second}"
    );
    drop(store);
    let mut store = Store::open(dir.path().join("s")).unwrap();
    let stage = store.atomic_stage().unwrap();
    assert!(stage.kernel().placement(aid(1)).is_some());
    assert!(stage.kernel().placement(aid(2)).is_some());
}

#[test]
fn checkpoint_capacity_failpoint_does_not_strand() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    stage_value(&mut store, aid(9), 3, b"secret");
    arm_failpoint_once("store.atomic.checkpoint.capacity", FailpointAction::Error);
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let value = b"again";
    let m = member(aid(10), 4, value);
    let p = plan(heap, &m, value);
    {
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage.append_staged(m, value.to_vec()).unwrap();
    }
    clear_failpoints();
    drop(store);
    let mut store = Store::open(&path).unwrap();
    let stage = store.atomic_stage().unwrap();
    assert!(
        stage.kernel().placement(aid(9)).is_some() || stage.kernel().placement(aid(10)).is_some(),
        "acknowledged evidence must survive a skipped checkpoint rewrite"
    );
}
