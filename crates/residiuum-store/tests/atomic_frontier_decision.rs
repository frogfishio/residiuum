//! ATM-3B serialization-frontier validation and terminal decision proofs.

#![cfg(feature = "legacy-raw-store")]

use residiuum_atomics::{
    AtomicAbortReason, AtomicId, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey,
    CollectionId, CoordinationScope, DecisionCode, HeapId, MutationKind, PlanMutation,
    ResourceLimits, VersionId,
};
use residiuum_format::{encode_subject_v2, SubjectObjectKind};
use residiuum_store::{AtomicStageClass, DurabilityMode, StageEvidenceClass, Store};

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

#[test]
fn stale_create_is_durably_not_committed_and_consumes_no_position() {
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
        Some(b"old".to_vec()),
        "ATM-3B must not expose a member before whole-delta publication"
    );
}

#[test]
fn stale_replace_replays_exact_terminal_decision_after_restart() {
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
