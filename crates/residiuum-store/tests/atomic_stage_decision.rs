//! ATM-3A: durable decision ordering, replay, and commit-position recovery.

#![cfg(feature = "legacy-raw-store")]

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId,
    CoordinationScope, HeapId, MutationKind, ObjectIdentity, PlanMutation, ResourceLimits,
    VersionId,
};
use residiuum_format::{encode_subject_v2, SubjectObjectKind};
use residiuum_store::{
    arm_failpoint_once, atomic_stage_checkpoint_path, clear_failpoints, AtomicStageClass,
    FailpointAction, Store, StoreError,
};
use std::sync::Mutex;

const FRONTIER: [u8; 32] = [0xA3; 32];
static FAILPOINT_SERIAL: Mutex<()> = Mutex::new(());

fn id(first: u8) -> AtomicId {
    let mut bytes = [0u8; 32];
    bytes[0] = first;
    AtomicId::from_bytes(bytes).unwrap()
}

fn collection() -> CollectionId {
    let mut bytes = [0u8; 16];
    bytes[0] = 2;
    CollectionId::from_bytes(bytes).unwrap()
}

fn version(first: u8) -> VersionId {
    let mut bytes = [0u8; 16];
    bytes[0] = first;
    VersionId::from_bytes(bytes).unwrap()
}

fn member(atomic_id: AtomicId, key: &str, value: &[u8], event: u8) -> AtomicMember {
    AtomicMember {
        atomic_id,
        ordinal: 0,
        object_identity: ObjectIdentity::new(collection(), CanonicalKey::String(key.to_owned())),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(value).as_bytes()),
        event_id: version(event),
    }
}

fn plan(heap: HeapId, member: &AtomicMember, value: &[u8]) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: member.atomic_id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: vec![PlanMutation {
            kind: member.member_kind,
            collection_id: member.object_identity.collection_id,
            key: member.object_identity.key.clone(),
            encoded_value: Some(value.to_vec()),
            if_version: None,
        }],
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

fn stage_one(
    store: &mut Store,
    atomic_id: AtomicId,
    key: &str,
    value: &[u8],
    event: u8,
) -> residiuum_atomics::AtomicDecision {
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    stage_one_for_heap(store, heap, atomic_id, key, value, event)
}

fn stage_one_for_heap(
    store: &mut Store,
    heap: HeapId,
    atomic_id: AtomicId,
    key: &str,
    value: &[u8],
    event: u8,
) -> residiuum_atomics::AtomicDecision {
    let member = member(atomic_id, key, value, event);
    let plan = plan(heap, &member, value);
    let mut stage = store.atomic_stage_for_heap(heap).unwrap();
    stage
        .begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .unwrap();
    stage.append_staged(member, value.to_vec()).unwrap();
    stage.seal_member_boundary(atomic_id).unwrap();
    stage.persist_committed_decision(atomic_id).unwrap()
}

fn heap(first: u8) -> HeapId {
    let mut bytes = [0u8; 16];
    bytes[0] = first;
    HeapId::from_bytes(bytes).unwrap()
}

#[test]
fn decision_requires_seal_and_exact_retry_is_stable() {
    let _serial = FAILPOINT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let atomic_id = id(7);
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let value = b"one";
    let member = member(atomic_id, "a", value, 3);
    let plan = plan(heap, &member, value);
    let mut stage = store.atomic_stage().unwrap();
    stage
        .begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .unwrap();
    stage.append_staged(member, value.to_vec()).unwrap();
    assert!(stage.persist_committed_decision(atomic_id).is_err());
    stage.seal_member_boundary(atomic_id).unwrap();
    let first = stage.persist_committed_decision(atomic_id).unwrap();
    let replay = stage.persist_committed_decision(atomic_id).unwrap();
    assert_eq!(first, replay);
    assert_eq!(first.commit_position, Some(1));
    drop(stage);
    let subject = encode_subject_v2(
        heap.as_bytes(),
        SubjectObjectKind::Collection,
        collection().as_bytes(),
        b"a",
    )
    .unwrap();
    assert_eq!(
        store.get_subject_bytes(&subject).unwrap(),
        None,
        "ATM-3A decision evidence must not leak a partially implemented publication"
    );
}

#[test]
fn reopen_recovers_decision_and_advances_heap_position() {
    let _serial = FAILPOINT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let first_id = id(7);
    {
        let mut store = Store::create(&path).unwrap();
        let first = stage_one(&mut store, first_id, "a", b"one", 3);
        assert_eq!(first.commit_position, Some(1));
    }
    let mut store = Store::open(&path).unwrap();
    {
        let stage = store.atomic_stage().unwrap();
        let status = stage.examine(first_id);
        assert_eq!(status.class, AtomicStageClass::Committed);
        assert_eq!(status.commit_position, Some(1));
        assert!(
            !status.coverage_degraded,
            "clean reopen unexpectedly degraded: {:?}; findings={:?}",
            stage.open_report(),
            stage.findings()
        );
    }
    let second = stage_one(&mut store, id(8), "b", b"two", 4);
    assert_eq!(second.commit_position, Some(2));
}

#[test]
fn segment_rebuild_recovers_decision_without_checkpoint() {
    let _serial = FAILPOINT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let atomic_id = id(7);
    {
        let mut store = Store::create(&path).unwrap();
        let decision = stage_one(&mut store, atomic_id, "a", b"one", 3);
        assert_eq!(decision.commit_position, Some(1));
    }
    let store = Store::open(&path).unwrap();
    std::fs::remove_file(atomic_stage_checkpoint_path(store.paths())).unwrap();
    drop(store);

    let mut store = Store::open(&path).unwrap();
    let stage = store.atomic_stage().unwrap();
    let status = stage.examine(atomic_id);
    assert_eq!(status.class, AtomicStageClass::Committed);
    assert_eq!(status.commit_position, Some(1));
    assert!(!status.coverage_degraded);
}

#[test]
fn failpoint_before_decision_leaves_no_commit() {
    let _serial = FAILPOINT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let atomic_id = id(7);
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let value = b"one";
    let member = member(atomic_id, "a", value, 3);
    let plan = plan(heap, &member, value);
    let mut stage = store.atomic_stage().unwrap();
    stage
        .begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .unwrap();
    stage.append_staged(member, value.to_vec()).unwrap();
    stage.seal_member_boundary(atomic_id).unwrap();
    arm_failpoint_once("store.atomic.before_decision", FailpointAction::Error);
    assert!(matches!(
        stage.persist_committed_decision(atomic_id),
        Err(StoreError::Failpoint("store.atomic.before_decision"))
    ));
    clear_failpoints();
    assert_eq!(stage.examine(atomic_id).class, AtomicStageClass::Sealed);
}

#[test]
fn failpoint_after_durable_decision_recovers_commit() {
    let _serial = FAILPOINT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let atomic_id = id(7);
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let value = b"one";
    let member = member(atomic_id, "a", value, 3);
    let plan = plan(heap, &member, value);
    {
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
            .unwrap();
        stage.append_staged(member, value.to_vec()).unwrap();
        stage.seal_member_boundary(atomic_id).unwrap();
        arm_failpoint_once("store.atomic.after_decision", FailpointAction::Error);
        assert!(matches!(
            stage.persist_committed_decision(atomic_id),
            Err(StoreError::Failpoint("store.atomic.after_decision"))
        ));
    }
    clear_failpoints();
    store.abandon_for_crash_test();
    drop(store);

    let mut store = Store::open(&path).unwrap();
    let stage = store.atomic_stage().unwrap();
    let status = stage.examine(atomic_id);
    assert_eq!(status.class, AtomicStageClass::Committed);
    assert_eq!(status.commit_position, Some(1));
}

#[test]
fn identical_atomic_ids_are_independent_across_named_heaps_and_restart() {
    let _serial = FAILPOINT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let heap_a = heap(0xA1);
    let heap_b = heap(0xB1);
    let atomic_a = id(7);
    let atomic_b = atomic_a;
    {
        let mut store = Store::create(&path).unwrap();
        let first_a = stage_one_for_heap(&mut store, heap_a, atomic_a, "a", b"one", 3);
        let first_b = stage_one_for_heap(&mut store, heap_b, atomic_b, "b", b"two", 4);
        assert_eq!(first_a.commit_position, Some(1));
        assert_eq!(first_b.commit_position, Some(1));
        assert_ne!(first_a.prepare_hash, first_b.prepare_hash);
    }

    let mut store = Store::open(&path).unwrap();
    {
        let stage_a = store.atomic_stage_for_heap(heap_a).unwrap();
        assert_eq!(stage_a.examine(atomic_a).class, AtomicStageClass::Committed);
        assert_eq!(
            stage_a
                .atomic_status(atomic_a)
                .unwrap()
                .receipt
                .unwrap()
                .heap_id,
            heap_a
        );
    }
    {
        let stage_b = store.atomic_stage_for_heap(heap_b).unwrap();
        assert_eq!(stage_b.examine(atomic_b).class, AtomicStageClass::Committed);
        assert_eq!(
            stage_b
                .atomic_status(atomic_b)
                .unwrap()
                .receipt
                .unwrap()
                .heap_id,
            heap_b
        );
    }
    let second_a = stage_one_for_heap(&mut store, heap_a, id(9), "c", b"three", 5);
    let second_b = stage_one_for_heap(&mut store, heap_b, id(10), "d", b"four", 6);
    assert_eq!(second_a.commit_position, Some(2));
    assert_eq!(second_b.commit_position, Some(2));
}
