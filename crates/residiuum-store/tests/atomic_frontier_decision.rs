//! ATM-3B serialization-frontier validation and terminal decision proofs.

#![cfg(feature = "legacy-raw-store")]

use residiuum_atomics::{
    encode_exact_scalar_equality, AtomicAbortReason, AtomicId, AtomicOutcome, AtomicPlan,
    AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId, CoordinationScope, DecisionCode,
    ExactScalarEquality, HeapId, LogicalStatus, MaterialStatus, MutationKind, PlanMutation,
    PlanPredicate, PredicateKind, ResourceLimits, ValueEncoding, VersionId,
};
use residiuum_format::{encode_subject_v2, SubjectObjectKind};
use residiuum_store::{
    arm_failpoint_once, clear_failpoints, AtomicStageClass, DurabilityMode, FailpointAction,
    ReadBudget, StageEvidenceClass, Store, StoreError,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};

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
        &key.subject_bytes(),
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
    create_n_plan(heap, atomic_id, 2)
}

fn exact_scalar_plan(
    heap: HeapId,
    atomic_id: AtomicId,
    predicate_key: CanonicalKey,
    expected: &[u8],
    mutation: Option<PlanMutation>,
) -> AtomicPlan {
    let compiled = ExactScalarEquality::new(ValueEncoding::Bytes, expected).unwrap();
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![
            encode_exact_scalar_equality(collection(), predicate_key, &compiled).unwrap(),
        ],
        mutations: mutation.into_iter().collect(),
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

fn create_n_plan(heap: HeapId, atomic_id: AtomicId, count: usize) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: (0..count)
            .map(|index| PlanMutation {
                kind: MutationKind::Create,
                collection_id: collection(),
                key: CanonicalKey::string(if count == 2 {
                    if index == 0 {
                        "left".into()
                    } else {
                        "right".into()
                    }
                } else {
                    format!("member-{index:04}")
                }),
                encoded_value: Some(if count == 2 {
                    if index == 0 {
                        b"L".to_vec()
                    } else {
                        b"R".to_vec()
                    }
                } else {
                    format!("value-{index:04}").into_bytes()
                }),
                if_version: None,
            })
            .collect(),
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
fn unbound_heap_authority_predicate_fails_closed_and_replays_after_restart() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let atomic_id = atomic(31);
    let (heap, atomic_plan, original) = {
        let mut store = Store::create(&path).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let key = CanonicalKey::string("authority-guarded");
        let atomic_plan = AtomicPlan::close(AtomicPlanParts {
            profile: AtomicProfile::LocalHeapV1,
            atomic_id,
            heap_id: heap,
            scope: CoordinationScope::LocalHeap,
            read_frontier: None,
            reads: vec![],
            predicates: vec![PlanPredicate {
                kind: PredicateKind::HeapAuthorityRevision,
                collection_id: None,
                key: None,
                version: None,
                encoded: Some(vec![7; 32]),
            }],
            mutations: vec![PlanMutation {
                kind: MutationKind::Create,
                collection_id: collection(),
                key: key.clone(),
                encoded_value: Some(b"must-not-publish".to_vec()),
                if_version: None,
            }],
            active_rule_revisions: vec![],
            limits: ResourceLimits::hard_local_heap(),
        })
        .unwrap();
        let original = store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_evidence(&atomic_plan)
            .unwrap();
        assert_eq!(original.decision, DecisionCode::NotCommitted);
        assert_eq!(original.commit_position, None);
        assert_eq!(original.abort_reason, Some(AtomicAbortReason::RuleRejected));
        assert_eq!(store.get_subject_bytes(&subject(heap, &key)).unwrap(), None);
        assert_eq!(
            store
                .atomic_stage_for_heap(heap)
                .unwrap()
                .decide_plan_outcome(&atomic_plan)
                .unwrap(),
            AtomicOutcome::NotCommitted {
                atomic_id,
                reason: AtomicAbortReason::RuleRejected,
            }
        );
        (heap, atomic_plan, original)
    };

    let mut reopened = Store::open(&path).unwrap();
    let replay = reopened
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&atomic_plan)
        .unwrap();
    assert_eq!(replay, original);
    assert_eq!(
        reopened
            .get_subject_bytes(&subject(heap, &CanonicalKey::string("authority-guarded")))
            .unwrap(),
        None
    );
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
fn committed_outcome_reports_exact_member_versions_and_replay_identity() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let key_a = CanonicalKey::string("a");
    let key_b = CanonicalKey::string("b");
    let key_c = CanonicalKey::string("c");
    let before_a = store
        .put_subject_bytes(&subject(heap, &key_a), b"old-a", DurabilityMode::Durable)
        .unwrap();
    let before_b = store
        .put_subject_bytes(&subject(heap, &key_b), b"old-b", DurabilityMode::Durable)
        .unwrap();
    let atomic_plan = AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: atomic(32),
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: vec![
            PlanMutation {
                kind: MutationKind::Replace,
                collection_id: collection(),
                key: key_a.clone(),
                encoded_value: Some(b"new-a".to_vec()),
                if_version: Some(VersionId::from_bytes(before_a.event_id).unwrap()),
            },
            PlanMutation {
                kind: MutationKind::Delete,
                collection_id: collection(),
                key: key_b.clone(),
                encoded_value: None,
                if_version: Some(VersionId::from_bytes(before_b.event_id).unwrap()),
            },
            PlanMutation {
                kind: MutationKind::Create,
                collection_id: collection(),
                key: key_c.clone(),
                encoded_value: Some(b"new-c".to_vec()),
                if_version: None,
            },
        ],
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap();

    let first = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_outcome(&atomic_plan)
        .unwrap();
    let AtomicOutcome::Committed(first_receipt) = first else {
        panic!("valid plan did not commit")
    };
    assert!(!first_receipt.replayed);
    assert_eq!(first_receipt.commit_position, 1);
    assert_eq!(first_receipt.members.len(), 3);
    assert!(first_receipt
        .members
        .windows(2)
        .all(|pair| pair[0].key < pair[1].key));
    let member = |key: &CanonicalKey| {
        first_receipt
            .members
            .iter()
            .find(|member| member.key == key.identity_bytes())
            .unwrap()
    };
    let receipt_a = member(&key_a);
    assert_eq!(
        receipt_a.before_version.unwrap().as_bytes(),
        &before_a.event_id
    );
    assert_eq!(receipt_a.after_version, Some(receipt_a.event_id));
    let receipt_b = member(&key_b);
    assert_eq!(
        receipt_b.before_version.unwrap().as_bytes(),
        &before_b.event_id
    );
    assert_eq!(receipt_b.after_version, None);
    let receipt_c = member(&key_c);
    assert_eq!(receipt_c.before_version, None);
    assert_eq!(receipt_c.after_version, Some(receipt_c.event_id));
    assert_eq!(
        store.get_subject_bytes(&subject(heap, &key_a)).unwrap(),
        Some(b"new-a".to_vec())
    );
    assert_eq!(
        store.get_subject_bytes(&subject(heap, &key_b)).unwrap(),
        None
    );
    assert_eq!(
        store.get_subject_bytes(&subject(heap, &key_c)).unwrap(),
        Some(b"new-c".to_vec())
    );
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    let replay = reopened
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_outcome(&atomic_plan)
        .unwrap();
    let AtomicOutcome::Committed(mut replay_receipt) = replay else {
        panic!("exact retry did not replay committed outcome")
    };
    assert!(replay_receipt.replayed);
    replay_receipt.replayed = false;
    assert_eq!(replay_receipt, first_receipt);
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
fn order_witness_without_decision_recovers_not_committed_and_never_reexecutes() {
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
    assert_eq!(reopened.open_report().atomic_stage_recovery_aborts, 1);
    let decision = reopened
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_evidence(&atomic_plan)
        .unwrap();
    assert_eq!(decision.decision, DecisionCode::NotCommitted);
    assert_eq!(decision.commit_position, None);
    assert_eq!(
        decision.abort_reason,
        Some(AtomicAbortReason::RecoveryAbort)
    );
    assert_eq!(reopened.get_subject_bytes(&subject).unwrap(), None);
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
        assert_eq!(
            replay.decision,
            if committed {
                DecisionCode::Committed
            } else {
                DecisionCode::NotCommitted
            }
        );
        assert_eq!(replay.commit_position, committed.then_some(1));
        assert_eq!(
            replay.abort_reason,
            (!committed).then_some(AtomicAbortReason::RecoveryAbort)
        );
        assert_eq!(
            reopened.get_subject_bytes(&left).unwrap(),
            committed.then(|| b"L".to_vec())
        );
        assert_eq!(
            reopened.get_subject_bytes(&right).unwrap(),
            committed.then(|| b"R".to_vec())
        );
    }
}

#[test]
fn concurrent_point_scan_and_history_readers_never_observe_half_a_batch() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let store = Store::create_with_shards(&path, 4).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let left = subject(heap, &CanonicalKey::string("left"));
    let right = subject(heap, &CanonicalKey::string("right"));
    let shared = Arc::new(Mutex::new(store));
    let start = Arc::new(Barrier::new(5));
    let sampled_before = Arc::new(Barrier::new(5));
    let published = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();

    for _ in 0..4 {
        let shared = Arc::clone(&shared);
        let start = Arc::clone(&start);
        let sampled_before = Arc::clone(&sampled_before);
        let published = Arc::clone(&published);
        let left = left.clone();
        let right = right.clone();
        readers.push(std::thread::spawn(move || {
            start.wait();
            let mut saw_before = false;
            let mut saw_after = false;
            for sample in 0..10_000 {
                let store = shared.lock().unwrap();
                let points = (
                    store.get_subject_bytes(&left).unwrap(),
                    store.get_subject_bytes(&right).unwrap(),
                );
                let scan = store.scan_live_logical().unwrap();
                let scan_left = scan.entries.iter().any(|(key, _)| key == &left);
                let scan_right = scan.entries.iter().any(|(key, _)| key == &right);
                let histories = (
                    store.history_subject_bytes(&left).unwrap().events.len(),
                    store.history_subject_bytes(&right).unwrap().events.len(),
                );
                drop(store);

                match points {
                    (None, None) => saw_before = true,
                    (Some(ref l), Some(ref r)) if l == b"L" && r == b"R" => saw_after = true,
                    other => panic!("point reader observed partial Atomic: {other:?}"),
                }
                assert_eq!(scan_left, scan_right, "scan observed partial Atomic");
                assert!(
                    histories == (0, 0) || histories == (1, 1),
                    "history reader observed partial Atomic: {histories:?}"
                );
                if sample == 0 {
                    sampled_before.wait();
                }
                if published.load(Ordering::Acquire) && saw_before && saw_after {
                    break;
                }
                std::thread::yield_now();
            }
            (saw_before, saw_after)
        }));
    }

    start.wait();
    // Readers establish the pre-publication generation before the writer takes
    // the same physical publication guard for the complete decision.
    sampled_before.wait();
    {
        let mut store = shared.lock().unwrap();
        store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_evidence(&create_two_plan(heap, atomic(30)))
            .unwrap();
    }
    published.store(true, Ordering::Release);

    let mut any_before = false;
    let mut any_after = false;
    for reader in readers {
        let (before, after) = reader.join().unwrap();
        any_before |= before;
        any_after |= after;
    }
    assert!(any_before, "test failed to sample the prior generation");
    assert!(any_after, "test failed to sample the committed generation");
}

#[test]
fn whole_plan_commit_uses_two_authoritative_boundaries_not_one_per_member() {
    let _guard = test_guard();
    clear_failpoints();

    let syncs_for = |members: usize, id: u8| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        let mut store = Store::create_with_shards(&path, 4).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let before = store.write_path_stats().authoritative_io.sync_operations;
        let atomic_plan = if members > 1 {
            create_n_plan(heap, atomic(id), members)
        } else {
            plan(
                heap,
                atomic(id),
                MutationKind::Create,
                CanonicalKey::string("one"),
                Some(b"one"),
                None,
            )
        };
        store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_evidence(&atomic_plan)
            .unwrap();
        store
            .write_path_stats()
            .authoritative_io
            .sync_operations
            .saturating_sub(before)
    };

    assert_eq!(syncs_for(1, 40), 2, "one-member Atomic boundaries");
    assert_eq!(syncs_for(256, 41), 2, "256-member Atomic boundaries");
}

#[test]
fn one_over_maximum_caller_plan_is_refused_before_media_append() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create_with_shards(&path, 4).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let atomic_plan = create_n_plan(heap, atomic(42), 257);
    let before = store.write_path_stats().authoritative_io;

    let error = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_outcome(&atomic_plan)
        .unwrap_err();
    assert!(
        matches!(error, StoreError::AtomicStage(ref message) if message.contains("LimitExceeded")),
        "expected structural limit refusal, got {error}"
    );
    assert_eq!(
        store.write_path_stats().authoritative_io,
        before,
        "structural refusal must not append Atomic evidence"
    );
}

#[test]
fn independent_atomic_cohort_shares_exactly_two_boundaries_and_replays_receipts() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create_with_shards(&path, 4).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let plans = vec![
        plan(
            heap,
            atomic(50),
            MutationKind::Create,
            CanonicalKey::string("cohort-a"),
            Some(b"A"),
            None,
        ),
        plan(
            heap,
            atomic(51),
            MutationKind::Create,
            CanonicalKey::string("cohort-b"),
            Some(b"B"),
            None,
        ),
    ];
    let before = store.write_path_stats().authoritative_io.sync_operations;
    let outcomes = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_cohort_outcomes(&plans)
        .unwrap();
    assert_eq!(
        store
            .write_path_stats()
            .authoritative_io
            .sync_operations
            .saturating_sub(before),
        2
    );
    let receipts = outcomes
        .iter()
        .map(|outcome| match outcome.as_ref().unwrap() {
            AtomicOutcome::Committed(receipt) => receipt,
            other => panic!("expected committed cohort member, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(receipts[0].commit_position, 1);
    assert_eq!(receipts[1].commit_position, 2);
    assert!(!receipts[0].replayed && !receipts[1].replayed);
    let original = receipts
        .iter()
        .map(|receipt| (*receipt).clone())
        .collect::<Vec<_>>();
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened
            .get_subject_bytes(&subject(heap, &CanonicalKey::string("cohort-a")))
            .unwrap(),
        Some(b"A".to_vec())
    );
    assert_eq!(
        reopened
            .get_subject_bytes(&subject(heap, &CanonicalKey::string("cohort-b")))
            .unwrap(),
        Some(b"B".to_vec())
    );
    let before_replay = reopened.write_path_stats().authoritative_io.sync_operations;
    let replay = reopened
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_cohort_outcomes(&plans)
        .unwrap();
    assert_eq!(
        reopened.write_path_stats().authoritative_io.sync_operations,
        before_replay,
        "terminal replay must not add a durability boundary"
    );
    for (outcome, mut expected) in replay.into_iter().zip(original) {
        let AtomicOutcome::Committed(receipt) = outcome.unwrap() else {
            panic!("committed cohort retry changed outcome")
        };
        assert!(receipt.replayed);
        expected.replayed = true;
        assert_eq!(receipt, expected);
    }
}

#[test]
fn atomic_cohort_serializes_conflicts_and_keeps_refusals_independent() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create_with_shards(&path, 4).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let first = plan(
        heap,
        atomic(52),
        MutationKind::Create,
        CanonicalKey::string("same"),
        Some(b"first"),
        None,
    );
    let conflicting = plan(
        heap,
        atomic(53),
        MutationKind::Create,
        CanonicalKey::string("same"),
        Some(b"second"),
        None,
    );
    let over_limit = create_n_plan(heap, atomic(54), 257);
    let last = plan(
        heap,
        atomic(55),
        MutationKind::Create,
        CanonicalKey::string("other"),
        Some(b"last"),
        None,
    );
    let plans = vec![first.clone(), first, conflicting, over_limit, last];
    let before = store.write_path_stats().authoritative_io.sync_operations;
    let outcomes = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_cohort_outcomes(&plans)
        .unwrap();
    assert_eq!(
        store
            .write_path_stats()
            .authoritative_io
            .sync_operations
            .saturating_sub(before),
        2
    );

    let AtomicOutcome::Committed(first_receipt) = outcomes[0].as_ref().unwrap() else {
        panic!("first plan did not commit")
    };
    assert_eq!(first_receipt.commit_position, 1);
    assert!(!first_receipt.replayed);
    let AtomicOutcome::Committed(duplicate_receipt) = outcomes[1].as_ref().unwrap() else {
        panic!("exact duplicate did not replay")
    };
    assert!(duplicate_receipt.replayed);
    let mut duplicate_without_flag = duplicate_receipt.clone();
    duplicate_without_flag.replayed = false;
    assert_eq!(&duplicate_without_flag, first_receipt);
    assert_eq!(
        outcomes[2],
        Ok(AtomicOutcome::NotCommitted {
            atomic_id: atomic(53),
            reason: AtomicAbortReason::PreconditionConflict,
        })
    );
    assert_eq!(
        outcomes[3],
        Err(residiuum_atomics::AtomicRefuseReason::LimitExceeded)
    );
    let AtomicOutcome::Committed(last_receipt) = outcomes[4].as_ref().unwrap() else {
        panic!("independent trailing plan did not commit")
    };
    assert_eq!(last_receipt.commit_position, 2);
    assert_eq!(
        store
            .get_subject_bytes(&subject(heap, &CanonicalKey::string("same")))
            .unwrap(),
        Some(b"first".to_vec())
    );
    assert_eq!(
        store
            .get_subject_bytes(&subject(heap, &CanonicalKey::string("other")))
            .unwrap(),
        Some(b"last".to_vec())
    );
}

#[test]
fn exact_scalar_equality_is_checked_at_the_serialization_frontier() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let guarded = CanonicalKey::string("guarded");
    store
        .put_subject_bytes(
            &subject(heap, &guarded),
            b"original",
            DurabilityMode::Durable,
        )
        .unwrap();

    let matching = exact_scalar_plan(
        heap,
        atomic(90),
        guarded.clone(),
        b"original",
        Some(PlanMutation {
            kind: MutationKind::Create,
            collection_id: collection(),
            key: CanonicalKey::string("match-result"),
            encoded_value: Some(b"committed".to_vec()),
            if_version: None,
        }),
    );
    assert!(matches!(
        store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_outcome(&matching)
            .unwrap(),
        AtomicOutcome::Committed(_)
    ));

    let stale = exact_scalar_plan(
        heap,
        atomic(91),
        guarded.clone(),
        b"original",
        Some(PlanMutation {
            kind: MutationKind::Create,
            collection_id: collection(),
            key: CanonicalKey::string("stale-result"),
            encoded_value: Some(b"must-not-publish".to_vec()),
            if_version: None,
        }),
    );
    store
        .put_subject_bytes(
            &subject(heap, &guarded),
            b"ordinary-update",
            DurabilityMode::Durable,
        )
        .unwrap();
    assert_eq!(
        store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_outcome(&stale)
            .unwrap(),
        AtomicOutcome::NotCommitted {
            atomic_id: atomic(91),
            reason: AtomicAbortReason::PreconditionConflict,
        }
    );
    assert_eq!(
        store
            .get_subject_bytes(&subject(heap, &CanonicalKey::string("stale-result")))
            .unwrap(),
        None
    );

    let absent = exact_scalar_plan(
        heap,
        atomic(92),
        CanonicalKey::string("missing"),
        b"x",
        None,
    );
    assert_eq!(
        store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_outcome(&absent)
            .unwrap(),
        AtomicOutcome::NotCommitted {
            atomic_id: atomic(92),
            reason: AtomicAbortReason::PreconditionConflict,
        }
    );
}

#[test]
fn exact_scalar_predicate_reads_earlier_value_in_same_cohort() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let guarded = CanonicalKey::string("cohort-scalar");
    let writer = plan(
        heap,
        atomic(93),
        MutationKind::Create,
        guarded.clone(),
        Some(b"planned"),
        None,
    );
    let sees_planned = exact_scalar_plan(
        heap,
        atomic(94),
        guarded.clone(),
        b"planned",
        Some(PlanMutation {
            kind: MutationKind::Create,
            collection_id: collection(),
            key: CanonicalKey::string("cohort-proof"),
            encoded_value: Some(b"yes".to_vec()),
            if_version: None,
        }),
    );
    let sees_prestate = exact_scalar_plan(heap, atomic(95), guarded, b"old", None);

    let outcomes = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_cohort_outcomes(&[writer, sees_planned, sees_prestate])
        .unwrap();
    assert!(matches!(outcomes[0], Ok(AtomicOutcome::Committed(_))));
    assert!(matches!(outcomes[1], Ok(AtomicOutcome::Committed(_))));
    assert_eq!(
        outcomes[2],
        Ok(AtomicOutcome::NotCommitted {
            atomic_id: atomic(95),
            reason: AtomicAbortReason::PreconditionConflict,
        })
    );
    assert_eq!(
        store
            .get_subject_bytes(&subject(heap, &CanonicalKey::string("cohort-proof")))
            .unwrap(),
        Some(b"yes".to_vec())
    );
}

#[test]
fn atomic_cohort_crash_cuts_recover_only_legal_whole_decisions() {
    let _guard = test_guard();
    clear_failpoints();
    for (case, (cut, committed)) in [
        ("store.atomic.cohort.after_member_boundary", false),
        ("store.atomic.cohort.after_decision_boundary", true),
    ]
    .into_iter()
    .enumerate()
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        let mut store = Store::create_with_shards(&path, 4).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let plans = vec![
            plan(
                heap,
                atomic(60 + case as u8 * 2),
                MutationKind::Create,
                CanonicalKey::string("left"),
                Some(b"L"),
                None,
            ),
            plan(
                heap,
                atomic(61 + case as u8 * 2),
                MutationKind::Create,
                CanonicalKey::string("right"),
                Some(b"R"),
                None,
            ),
        ];
        arm_failpoint_once(cut, FailpointAction::Error);
        let result = store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_cohort_outcomes(&plans);
        assert!(matches!(result, Err(StoreError::Failpoint(name)) if name == cut));
        clear_failpoints();
        drop(store);

        let mut reopened = Store::open(&path).unwrap();
        let observed = (
            reopened
                .get_subject_bytes(&subject(heap, &CanonicalKey::string("left")))
                .unwrap(),
            reopened
                .get_subject_bytes(&subject(heap, &CanonicalKey::string("right")))
                .unwrap(),
        );
        assert_eq!(
            observed,
            if committed {
                (Some(b"L".to_vec()), Some(b"R".to_vec()))
            } else {
                (None, None)
            },
            "illegal cohort state after {cut}"
        );
        let retry = reopened
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_cohort_outcomes(&plans)
            .unwrap();
        assert!(retry.iter().all(|outcome| {
            if committed {
                matches!(outcome, Ok(AtomicOutcome::Committed(_)))
            } else {
                matches!(
                    outcome,
                    Ok(AtomicOutcome::NotCommitted {
                        reason: AtomicAbortReason::RecoveryAbort,
                        ..
                    })
                )
            }
        }));
    }
}

#[test]
fn decision_only_atomic_cohort_uses_one_boundary_and_no_positions() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    for key in ["occupied-a", "occupied-b"] {
        store
            .put_subject_bytes(
                &subject(heap, &CanonicalKey::string(key)),
                b"old",
                DurabilityMode::Durable,
            )
            .unwrap();
    }
    let plans = vec![
        plan(
            heap,
            atomic(70),
            MutationKind::Create,
            CanonicalKey::string("occupied-a"),
            Some(b"new"),
            None,
        ),
        plan(
            heap,
            atomic(71),
            MutationKind::Create,
            CanonicalKey::string("occupied-b"),
            Some(b"new"),
            None,
        ),
    ];
    let before = store.write_path_stats().authoritative_io.sync_operations;
    let outcomes = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_cohort_outcomes(&plans)
        .unwrap();
    assert_eq!(
        store
            .write_path_stats()
            .authoritative_io
            .sync_operations
            .saturating_sub(before),
        1,
        "not-committed decisions need no member-stable boundary"
    );
    assert!(outcomes.iter().all(|outcome| matches!(
        outcome,
        Ok(AtomicOutcome::NotCommitted {
            reason: AtomicAbortReason::PreconditionConflict,
            ..
        })
    )));
    let success = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_outcome(&plan(
            heap,
            atomic(72),
            MutationKind::Create,
            CanonicalKey::string("free"),
            Some(b"value"),
            None,
        ))
        .unwrap();
    let AtomicOutcome::Committed(receipt) = success else {
        panic!("later independent Atomic did not commit")
    };
    assert_eq!(receipt.commit_position, 1);
}

#[test]
fn atomic_status_keeps_logical_and_material_truth_independent() {
    let _guard = test_guard();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();

    let missing = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .atomic_status(atomic(80))
        .unwrap();
    assert_eq!(missing.logical, LogicalStatus::NotFound);
    assert_eq!(missing.material, MaterialStatus::Complete);
    assert!(missing.receipt.is_none());

    let committed_plan = plan(
        heap,
        atomic(81),
        MutationKind::Create,
        CanonicalKey::string("status-committed"),
        Some(b"value"),
        None,
    );
    store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_outcome(&committed_plan)
        .unwrap();
    let committed = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .atomic_status(atomic(81))
        .unwrap();
    assert_eq!(committed.logical, LogicalStatus::Committed);
    assert_eq!(committed.material, MaterialStatus::Complete);
    assert_eq!(committed.receipt.unwrap().commit_position, 1);

    let occupied = CanonicalKey::string("status-occupied");
    store
        .put_subject_bytes(&subject(heap, &occupied), b"old", DurabilityMode::Durable)
        .unwrap();
    let rejected_plan = plan(
        heap,
        atomic(82),
        MutationKind::Create,
        occupied,
        Some(b"new"),
        None,
    );
    store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_outcome(&rejected_plan)
        .unwrap();
    let rejected = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .atomic_status(atomic(82))
        .unwrap();
    assert_eq!(rejected.logical, LogicalStatus::NotCommitted);
    assert_eq!(rejected.material, MaterialStatus::Complete);
    assert!(rejected.receipt.is_none());
}
