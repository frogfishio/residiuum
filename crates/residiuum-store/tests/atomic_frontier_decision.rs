//! ATM-3B serialization-frontier validation and terminal decision proofs.

#![cfg(feature = "legacy-raw-store")]

use residiuum_atomics::{
    AtomicAbortReason, AtomicId, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey,
    CollectionId, CoordinationScope, DecisionCode, HeapId, MutationKind, PlanMutation,
    ResourceLimits, VersionId,
};
use residiuum_format::{encode_subject_v2, SubjectObjectKind};
use residiuum_store::{
    arm_failpoint_once, clear_failpoints, AtomicStageClass, DurabilityMode, FailpointAction,
    ReadBudget, StageEvidenceClass, Store, StoreError,
};
use std::sync::Mutex;

static FAILPOINT_LOCK: Mutex<()> = Mutex::new(());

fn test_guard() -> std::sync::MutexGuard<'static, ()> {
    FAILPOINT_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn atomic(first: u8) -> AtomicId {
    let mut bytes = [0u8; 32];
    bytes[0] = first;
    AtomicId::from_bytes(bytes).unwrap()
}

fn collection() -> CollectionId {
    let mut bytes = [0u8; 16];
    bytes[0] = 9;
    CollectionId::from_bytes(bytes).unwrap()
}

fn subject(heap: HeapId, key: &CanonicalKey) -> Vec<u8> {
    encode_subject_v2(
        heap.as_bytes(),
        SubjectObjectKind::Collection,
        collection().as_bytes(),
        &key.identity_bytes(),
    )
    .unwrap()
}

fn plan(
    heap: HeapId,
    atomic_id: AtomicId,
    kind: MutationKind,
    key: CanonicalKey,
    value: Option<&[u8]>,
    if_version: Option<VersionId>,
) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: vec![PlanMutation {
            kind,
            collection_id: collection(),
            key,
            encoded_value: value.map(ToOwned::to_owned),
            if_version,
        }],
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

fn create_two_plan(heap: HeapId, atomic_id: AtomicId) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: vec![
            PlanMutation {
                kind: MutationKind::Create,
                collection_id: collection(),
                key: CanonicalKey::string("left"),
                encoded_value: Some(b"L".to_vec()),
                if_version: None,
            },
            PlanMutation {
                kind: MutationKind::Create,
                collection_id: collection(),
                key: CanonicalKey::string("right"),
                encoded_value: Some(b"R".to_vec()),
                if_version: None,
            },
        ],
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

#[test]
fn stale_create_is_durably_not_committed_and_consumes_no_position() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let key = CanonicalKey::string("a");
    let subject = subject(heap, &key);
    store
        .put_subject_bytes(&subject, b"old", DurabilityMode::Durable)
        .unwrap();

    let stale_id = atomic(1);
    let stale = plan(
        heap,
        stale_id,
        MutationKind::Create,
        key.clone(),
        Some(b"new"),
        None,
    );
    let decision = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&stale)
        .unwrap();
    assert_eq!(decision.decision, DecisionCode::NotCommitted);
    assert_eq!(decision.commit_position, None);
    assert_eq!(
        decision.abort_reason,
        Some(AtomicAbortReason::PreconditionConflict)
    );
    assert_eq!(
        store.get_subject_bytes(&subject).unwrap(),
        Some(b"old".to_vec())
    );

    let success = plan(heap, atomic(2), MutationKind::Put, key, Some(b"new"), None);
    let committed = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&success)
        .unwrap();
    assert_eq!(committed.decision, DecisionCode::Committed);
    assert_eq!(committed.commit_position, Some(1));
    assert_eq!(
        store.get_subject_bytes(&subject).unwrap(),
        Some(b"new".to_vec()),
        "ATM-3C publishes the committed delta before returning"
    );
    drop(store);

    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.get_subject_bytes(&subject).unwrap(),
        Some(b"new".to_vec()),
        "restart must reconstruct committed publication from decision evidence"
    );
}

#[test]
fn stale_replace_replays_exact_terminal_decision_after_restart() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let atomic_id = atomic(3);
    let (heap, stale_plan, original) = {
        let mut store = Store::create(&path).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let key = CanonicalKey::string("r");
        let subject = subject(heap, &key);
        let first = store
            .put_subject_bytes(&subject, b"v1", DurabilityMode::Durable)
            .unwrap();
        let mut wrong = first.event_id;
        wrong[0] ^= 0xFF;
        let stale = plan(
            heap,
            atomic_id,
            MutationKind::Replace,
            key,
            Some(b"v2"),
            Some(VersionId::from_bytes(wrong).unwrap()),
        );
        let original = store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_evidence(&stale)
            .unwrap();
        (heap, stale, original)
    };

    let mut store = Store::open(&path).unwrap();
    let replay = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&stale_plan)
        .unwrap();
    assert_eq!(replay, original);
    let stage = store.atomic_stage_for_heap(heap).unwrap();
    assert_eq!(
        stage.examine(atomic_id).class,
        AtomicStageClass::NotCommitted
    );
    assert!(!stage
        .findings()
        .records
        .iter()
        .any(|finding| finding.atomic_id == Some(atomic_id)
            && finding.class == StageEvidenceClass::Partial));
}

#[test]
fn two_member_commit_is_visible_as_one_generation_and_survives_restart() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let left = subject(heap, &CanonicalKey::string("left"));
    let right = subject(heap, &CanonicalKey::string("right"));

    let decision = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&create_two_plan(heap, atomic(4)))
        .unwrap();
    assert_eq!(decision.decision, DecisionCode::Committed);
    assert_eq!(store.get_subject_bytes(&left).unwrap(), Some(b"L".to_vec()));
    assert_eq!(
        store.get_subject_bytes(&right).unwrap(),
        Some(b"R".to_vec())
    );
    drop(store);

    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.get_subject_bytes(&left).unwrap(),
        Some(b"L".to_vec())
    );
    assert_eq!(
        reopened.get_subject_bytes(&right).unwrap(),
        Some(b"R".to_vec())
    );
}

#[test]
fn durable_decision_before_publish_recovers_the_whole_batch() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let left = subject(heap, &CanonicalKey::string("left"));
    let right = subject(heap, &CanonicalKey::string("right"));
    arm_failpoint_once("store.atomic.before_publish", FailpointAction::Error);

    let outcome = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&create_two_plan(heap, atomic(5)));
    assert!(matches!(
        outcome,
        Err(StoreError::Failpoint("store.atomic.before_publish"))
    ));
    assert_eq!(store.get_subject_bytes(&left).unwrap(), None);
    assert_eq!(store.get_subject_bytes(&right).unwrap(), None);
    clear_failpoints();
    drop(store);

    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.get_subject_bytes(&left).unwrap(),
        Some(b"L".to_vec())
    );
    assert_eq!(
        reopened.get_subject_bytes(&right).unwrap(),
        Some(b"R".to_vec())
    );
}

#[test]
fn failure_after_publish_never_rolls_back_the_committed_generation() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let left = subject(heap, &CanonicalKey::string("left"));
    let right = subject(heap, &CanonicalKey::string("right"));
    arm_failpoint_once("store.atomic.after_publish", FailpointAction::Error);

    let outcome = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&create_two_plan(heap, atomic(6)));
    assert!(matches!(
        outcome,
        Err(StoreError::Failpoint("store.atomic.after_publish"))
    ));
    assert_eq!(store.get_subject_bytes(&left).unwrap(), Some(b"L".to_vec()));
    assert_eq!(
        store.get_subject_bytes(&right).unwrap(),
        Some(b"R".to_vec())
    );
    clear_failpoints();
    drop(store);

    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.get_subject_bytes(&left).unwrap(),
        Some(b"L".to_vec())
    );
    assert_eq!(
        reopened.get_subject_bytes(&right).unwrap(),
        Some(b"R".to_vec())
    );
}

#[test]
fn later_ordinary_put_wins_after_full_index_rebuild() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create_with_shards(&path, 4).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let key = CanonicalKey::string("ordered-put");
    let subject = subject(heap, &key);

    store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&plan(
            heap,
            atomic(7),
            MutationKind::Create,
            key,
            Some(b"atomic"),
            None,
        ))
        .unwrap();
    store
        .put_subject_bytes(&subject, b"ordinary", DurabilityMode::Durable)
        .unwrap();
    drop(store);
    std::fs::remove_file(path.join("indexes/primary.idx")).unwrap();

    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.get_subject_bytes(&subject).unwrap(),
        Some(b"ordinary".to_vec()),
        "an ordinary write beyond the witnessed shard frontier must outrank the Atomic"
    );
}

#[test]
fn later_ordinary_delete_wins_after_full_index_rebuild() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create_with_shards(&path, 4).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let key = CanonicalKey::string("ordered-delete");
    let subject = subject(heap, &key);

    store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&plan(
            heap,
            atomic(8),
            MutationKind::Create,
            key,
            Some(b"atomic"),
            None,
        ))
        .unwrap();
    store
        .delete_subject_bytes(&subject, DurabilityMode::Durable)
        .unwrap();
    drop(store);
    std::fs::remove_file(path.join("indexes/primary.idx")).unwrap();

    let reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.get_subject_bytes(&subject).unwrap(), None);
}

#[test]
fn overlapping_atomics_replay_in_heap_commit_order() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create_with_shards(&path, 4).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let key = CanonicalKey::string("atomic-chain");
    let subject = subject(heap, &key);

    let first = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&plan(
            heap,
            atomic(9),
            MutationKind::Create,
            key.clone(),
            Some(b"first"),
            None,
        ))
        .unwrap();
    let first_version = store.live_event_id(&subject).unwrap();
    let second = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&plan(
            heap,
            atomic(10),
            MutationKind::Replace,
            key,
            Some(b"second"),
            Some(VersionId::from_bytes(first_version).unwrap()),
        ))
        .unwrap();
    assert_eq!(first.commit_position, Some(1));
    assert_eq!(second.commit_position, Some(2));
    drop(store);
    std::fs::remove_file(path.join("indexes/primary.idx")).unwrap();

    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.get_subject_bytes(&subject).unwrap(),
        Some(b"second".to_vec())
    );
}

#[test]
fn order_witness_without_decision_never_publishes_and_retry_commits_once() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create_with_shards(&path, 4).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let key = CanonicalKey::string("witness-prefix");
    let subject = subject(heap, &key);
    let atomic_plan = plan(
        heap,
        atomic(11),
        MutationKind::Create,
        key,
        Some(b"committed"),
        None,
    );
    arm_failpoint_once("store.atomic.before_decision", FailpointAction::Error);
    let first = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&atomic_plan);
    assert!(matches!(
        first,
        Err(StoreError::Failpoint("store.atomic.before_decision"))
    ));
    assert_eq!(store.get_subject_bytes(&subject).unwrap(), None);
    clear_failpoints();
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.get_subject_bytes(&subject).unwrap(), None);
    let decision = reopened
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&atomic_plan)
        .unwrap();
    assert_eq!(decision.commit_position, Some(1));
    assert_eq!(
        reopened.get_subject_bytes(&subject).unwrap(),
        Some(b"committed".to_vec())
    );
}

#[test]
fn history_merges_committed_atomics_at_the_witnessed_ordinary_gap() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create_with_shards(&path, 4).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let key = CanonicalKey::string("history-order");
    let subject = subject(heap, &key);
    let first = store
        .put_subject_bytes(&subject, b"ordinary-before", DurabilityMode::Durable)
        .unwrap();
    let committed = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&plan(
            heap,
            atomic(12),
            MutationKind::Replace,
            key,
            Some(b"atomic"),
            Some(VersionId::from_bytes(first.event_id).unwrap()),
        ))
        .unwrap();
    let atomic_event = store.live_event_id(&subject).unwrap();
    store
        .put_subject_bytes(&subject, b"ordinary-after", DurabilityMode::Durable)
        .unwrap();

    let assert_history = |store: &Store| {
        let history = store.history_subject_bytes(&subject).unwrap();
        let bodies: Vec<_> = history
            .events
            .iter()
            .map(|event| event.body.as_slice())
            .collect();
        assert_eq!(
            bodies,
            vec![b"ordinary-before".as_slice(), b"atomic", b"ordinary-after"]
        );
        assert_eq!(history.events[1].event_id, atomic_event);
        let selected = store
            .get_payload_version_bytes(&subject, &atomic_event, ReadBudget::unlimited())
            .unwrap();
        assert_eq!(
            selected.selected.unwrap().complete_body(),
            Some(b"atomic".as_slice())
        );
    };
    assert_eq!(committed.commit_position, Some(1));
    assert_history(&store);
    drop(store);

    let reopened = Store::open(&path).unwrap();
    assert_history(&reopened);
}

#[test]
fn read_only_inspection_recovers_committed_values_and_history_without_checkpoint_writes() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut writer = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(writer.store_id()).unwrap();
    let key = CanonicalKey::string("inspect-atomic");
    let subject = subject(heap, &key);
    writer
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&plan(
            heap,
            atomic(13),
            MutationKind::Create,
            key,
            Some(b"visible"),
            None,
        ))
        .unwrap();

    // A read-only open must not touch either derived Atomic sidecar.
    arm_failpoint_once(
        "store.atomic.checkpoint.after_write",
        FailpointAction::Error,
    );
    arm_failpoint_once("store.atomic.coord.after_write", FailpointAction::Error);
    let inspected = Store::open_inspect(&path).unwrap();
    assert_eq!(HeapId::from_bytes(inspected.store_id()).unwrap(), heap);
    assert_eq!(
        inspected.get_subject_bytes(&subject).unwrap(),
        Some(b"visible".to_vec())
    );
    let history = inspected.history_subject_bytes(&subject).unwrap();
    assert_eq!(history.events.len(), 1);
    assert_eq!(history.events[0].body, b"visible");
    drop(inspected);
    drop(writer);
    clear_failpoints();
}

#[test]
fn mandated_decision_publish_ack_crash_prefixes_have_only_legal_recovery_states() {
    let _guard = test_guard();
    clear_failpoints();
    let cuts = [
        ("store.atomic.before_decision", false),
        ("store.atomic.after_decision", true),
        ("store.atomic.before_publish", true),
        ("store.atomic.after_publish", true),
        ("store.atomic.before_ack", true),
    ];

    for (case, (cut, committed)) in cuts.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        let mut store = Store::create_with_shards(&path, 4).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let left = subject(heap, &CanonicalKey::string("left"));
        let right = subject(heap, &CanonicalKey::string("right"));
        let atomic_plan = create_two_plan(heap, atomic(20 + case as u8));
        arm_failpoint_once(cut, FailpointAction::Error);

        let outcome = store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_evidence(&atomic_plan);
        assert!(matches!(outcome, Err(StoreError::Failpoint(name)) if name == cut));
        clear_failpoints();
        drop(store);

        let mut reopened = Store::open(&path).unwrap();
        let observed = (
            reopened.get_subject_bytes(&left).unwrap(),
            reopened.get_subject_bytes(&right).unwrap(),
        );
        assert_eq!(
            observed,
            if committed {
                (Some(b"L".to_vec()), Some(b"R".to_vec()))
            } else {
                (None, None)
            },
            "illegal recovered state at {cut}"
        );
        let replay = reopened
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_evidence(&atomic_plan)
            .unwrap();
        assert_eq!(replay.decision, DecisionCode::Committed);
        assert_eq!(replay.commit_position, Some(1));
        assert_eq!(
            reopened.get_subject_bytes(&left).unwrap(),
            Some(b"L".to_vec())
        );
        assert_eq!(
            reopened.get_subject_bytes(&right).unwrap(),
            Some(b"R".to_vec())
        );
    }
}
