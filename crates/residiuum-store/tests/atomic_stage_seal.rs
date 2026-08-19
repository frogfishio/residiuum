//! CR-ATMR6-003: persist the store seal before applying DurableInvisible.

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId,
    CoordinationScope, HeapId, MemberPhase, MutationKind, ObjectIdentity, PlanMutation,
    ResourceLimits, VersionId,
};
use residiuum_store::{arm_failpoint_once, clear_failpoints, FailpointAction, Store, StoreError};
use std::fs;
use std::sync::Mutex;

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    guard
}

const FRONTIER: [u8; 32] = [0xA1; 32];

fn aid() -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = 9;
    AtomicId::from_bytes(b).unwrap()
}

fn cid() -> CollectionId {
    let mut b = [0u8; 16];
    b[0] = 2;
    CollectionId::from_bytes(b).unwrap()
}

fn vid() -> VersionId {
    let mut b = [0u8; 16];
    b[0] = 3;
    VersionId::from_bytes(b).unwrap()
}

fn member() -> AtomicMember {
    AtomicMember {
        atomic_id: aid(),
        ordinal: 0,
        object_identity: ObjectIdentity::new(cid(), CanonicalKey::String("k".into())),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(b"secret").as_bytes()),
        event_id: vid(),
    }
}

fn plan(heap: HeapId, members: &[AtomicMember], value: &[u8]) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: members[0].atomic_id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: members
            .iter()
            .map(|m| PlanMutation {
                kind: m.member_kind,
                collection_id: m.object_identity.collection_id,
                key: m.object_identity.key.clone(),
                encoded_value: Some(value.to_vec()),
                if_version: m.before_version,
            })
            .collect(),
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

fn media_bytes_at(dirs: &[std::path::PathBuf]) -> Vec<u8> {
    let mut out = Vec::new();
    for dir in dirs {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Ok(bytes) = fs::read(&path) {
                    out.extend_from_slice(&bytes);
                }
            }
        }
    }
    out
}

fn media_dirs(store: &Store) -> Vec<std::path::PathBuf> {
    vec![
        store.paths().active_dir(),
        store.paths().segments_dir(),
        store.paths().pending_seal_dir(),
    ]
}

fn count_atseal1(dirs: &[std::path::PathBuf]) -> usize {
    media_bytes_at(dirs)
        .windows(7)
        .filter(|w| *w == b"ATSEAL1")
        .count()
}

fn members_phase(stage: &residiuum_store::StoreAtomicStage<'_>) -> Option<MemberPhase> {
    stage.kernel().lifecycle(aid()).map(|life| life.members)
}

fn stage_ready(store: &mut Store) {
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), b"secret");
    let mut stage = store.atomic_stage().unwrap();
    stage
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    stage.append_staged(m, b"secret".to_vec()).unwrap();
}

fn assert_staged_not_durable(stage: &residiuum_store::StoreAtomicStage<'_>) {
    assert_eq!(
        members_phase(stage),
        Some(MemberPhase::Staged),
        "live handle must stay staged, not DurableInvisible"
    );
}

fn assert_durable(stage: &residiuum_store::StoreAtomicStage<'_>) {
    assert_eq!(
        members_phase(stage),
        Some(MemberPhase::DurableInvisible),
        "live handle must be DurableInvisible"
    );
}

#[test]
fn before_append_failure_leaves_live_handle_staged() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    stage_ready(&mut store);
    let dirs = media_dirs(&store);
    arm_failpoint_once("store.atomic.seal.before_append", FailpointAction::Error);
    {
        let mut stage = store.atomic_stage().unwrap();
        let err = stage.seal_member_boundary(aid()).unwrap_err();
        assert!(matches!(err, StoreError::Failpoint(_)));
        assert_staged_not_durable(&stage);
        assert_eq!(count_atseal1(&dirs), 0);
        clear_failpoints();
        stage.seal_member_boundary(aid()).unwrap();
        assert_durable(&stage);
        assert_eq!(count_atseal1(&dirs), 1);
    }
    drop(store);
    let mut store = Store::open(&path).unwrap();
    let stage = store.atomic_stage().unwrap();
    assert_durable(&stage);
    assert_eq!(count_atseal1(&media_dirs(&store)), 1);
}

#[test]
fn after_append_retry_is_durable_without_second_seal() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    stage_ready(&mut store);
    let dirs = media_dirs(&store);
    arm_failpoint_once("store.atomic.seal.after_append", FailpointAction::Error);
    {
        let mut stage = store.atomic_stage().unwrap();
        let err = stage.seal_member_boundary(aid()).unwrap_err();
        assert!(matches!(err, StoreError::Failpoint(_)));
        assert_staged_not_durable(&stage);
        assert_eq!(count_atseal1(&dirs), 1);
        clear_failpoints();
        stage.seal_member_boundary(aid()).unwrap();
        assert_durable(&stage);
        assert_eq!(
            count_atseal1(&dirs),
            1,
            "retry must not write a second seal"
        );
    }
    drop(store);
    let mut store = Store::open(&path).unwrap();
    let stage = store.atomic_stage().unwrap();
    assert_durable(&stage);
    assert_eq!(count_atseal1(&media_dirs(&store)), 1);
}

#[test]
fn after_checkpoint_retry_is_durable_without_second_seal() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    stage_ready(&mut store);
    let dirs = media_dirs(&store);
    arm_failpoint_once("store.atomic.seal.after_checkpoint", FailpointAction::Error);
    {
        let mut stage = store.atomic_stage().unwrap();
        let err = stage.seal_member_boundary(aid()).unwrap_err();
        assert!(matches!(err, StoreError::Failpoint(_)));
        assert_staged_not_durable(&stage);
        assert_eq!(count_atseal1(&dirs), 1);
        clear_failpoints();
        stage.seal_member_boundary(aid()).unwrap();
        assert_durable(&stage);
        assert_eq!(count_atseal1(&dirs), 1);
    }
    drop(store);
    let mut store = Store::open(&path).unwrap();
    let stage = store.atomic_stage().unwrap();
    assert_durable(&stage);
    assert_eq!(count_atseal1(&media_dirs(&store)), 1);
}

#[test]
fn already_sealed_retry_is_idempotent() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    stage_ready(&mut store);
    let dirs = media_dirs(&store);
    {
        let mut stage = store.atomic_stage().unwrap();
        stage.seal_member_boundary(aid()).unwrap();
        stage.seal_member_boundary(aid()).unwrap();
        assert_durable(&stage);
        assert_eq!(count_atseal1(&dirs), 1);
    }
    drop(store);
    let mut store = Store::open(&path).unwrap();
    let mut stage = store.atomic_stage().unwrap();
    stage.seal_member_boundary(aid()).unwrap();
    assert_durable(&stage);
    assert_eq!(count_atseal1(&media_dirs(&store)), 1);
}
