//! ATM-4D.5: collection descriptors and Atomic decisions share one order.

#![cfg(feature = "legacy-raw-store")]

use residiuum_atomics::{
    encode_collection_lifecycle_state, AtomicAbortReason, AtomicId, AtomicPlan, AtomicPlanParts,
    AtomicProfile, CanonicalKey, CollectionId, CollectionLifecycleState, CoordinationScope,
    DecisionCode, HeapId, MutationKind, PlanMutation, ResourceLimits,
};
use residiuum_store::{
    publish_staged_genesis, stage_heap_genesis, DurabilityMode, HeapMetaLayout, Store,
};

fn identity16(first: u8) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[0] = first;
    bytes
}

fn heap(first: u8) -> HeapId {
    HeapId::from_bytes(identity16(first)).unwrap()
}

fn atomic(first: u8) -> AtomicId {
    let mut bytes = [0u8; 32];
    bytes[0] = first;
    AtomicId::from_bytes(bytes).unwrap()
}

fn publish_heap(root: &std::path::Path, heap_id: HeapId, name: &str) {
    let layout = HeapMetaLayout::new(root);
    let staged = stage_heap_genesis(
        &layout,
        identity16(90),
        *heap_id.as_bytes(),
        identity16(heap_id.as_bytes()[0].saturating_add(40)),
        name,
    )
    .unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
}

fn plan(
    heap_id: HeapId,
    atomic_id: AtomicId,
    collection_id: CollectionId,
    expected: CollectionLifecycleState,
    mutation_key: Option<&str>,
) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id,
        heap_id,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![encode_collection_lifecycle_state(collection_id, expected).unwrap()],
        mutations: mutation_key
            .map(|key| PlanMutation {
                kind: MutationKind::Create,
                collection_id,
                key: CanonicalKey::string(key.to_owned()),
                encoded_value: Some(key.as_bytes().to_vec()),
                if_version: None,
            })
            .into_iter()
            .collect(),
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

/// Independent state-machine check: it consumes only the state at a candidate
/// serial position, the declared state, and the recorded decision.
fn lifecycle_decision_is_explained(
    observed: CollectionLifecycleState,
    expected: CollectionLifecycleState,
    decision: &residiuum_atomics::AtomicDecision,
) -> bool {
    if observed == expected {
        decision.decision == DecisionCode::Committed
    } else {
        decision.decision == DecisionCode::NotCommitted
            && decision.abort_reason == Some(AtomicAbortReason::PreconditionConflict)
    }
}

#[test]
fn create_and_retire_invalidate_stale_lifecycle_plans_while_rename_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(&path).unwrap();
    let heap_id = heap(1);
    publish_heap(&path, heap_id, "lifecycle-a");

    let future_id = CollectionId::from_bytes(identity16(20)).unwrap();
    let stale_absent = plan(
        heap_id,
        atomic(1),
        future_id,
        CollectionLifecycleState::Absent,
        None,
    );
    store
        .create_collection_ordered(
            heap_id.as_bytes(),
            *future_id.as_bytes(),
            identity16(21),
            "orders",
        )
        .unwrap();
    let collection_id = future_id;
    assert_eq!(
        store
            .collection_lifecycle_state(heap_id, collection_id)
            .unwrap(),
        CollectionLifecycleState::Active
    );

    let create_conflict = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&stale_absent)
        .unwrap();
    assert_eq!(create_conflict.decision, DecisionCode::NotCommitted);
    assert_eq!(
        create_conflict.abort_reason,
        Some(AtomicAbortReason::PreconditionConflict)
    );
    assert!(lifecycle_decision_is_explained(
        CollectionLifecycleState::Active,
        CollectionLifecycleState::Absent,
        &create_conflict
    ));
    assert!(
        !lifecycle_decision_is_explained(
            CollectionLifecycleState::Absent,
            CollectionLifecycleState::Absent,
            &create_conflict
        ),
        "negative control: removing the winning create makes the abort unjustifiable"
    );

    let before_rename = plan(
        heap_id,
        atomic(2),
        collection_id,
        CollectionLifecycleState::Active,
        Some("after-rename"),
    );
    store
        .rename_collection_ordered(heap_id.as_bytes(), collection_id.as_bytes(), "orders-v2")
        .unwrap();
    assert_eq!(
        store
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_evidence(&before_rename)
            .unwrap()
            .decision,
        DecisionCode::Committed,
        "rename preserves Active and must not create a false lifecycle conflict"
    );

    let before_retire = plan(
        heap_id,
        atomic(3),
        collection_id,
        CollectionLifecycleState::Active,
        Some("must-not-publish"),
    );
    store
        .retire_collection_ordered(heap_id.as_bytes(), collection_id.as_bytes())
        .unwrap();
    let rejected = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&before_retire)
        .unwrap();
    assert_eq!(rejected.decision, DecisionCode::NotCommitted);
    assert_eq!(
        rejected.abort_reason,
        Some(AtomicAbortReason::PreconditionConflict)
    );
    assert!(lifecycle_decision_is_explained(
        CollectionLifecycleState::Retired,
        CollectionLifecycleState::Active,
        &rejected
    ));
    assert!(
        !lifecycle_decision_is_explained(
            CollectionLifecycleState::Active,
            CollectionLifecycleState::Active,
            &rejected
        ),
        "negative control: removing the winning retirement makes the abort unjustifiable"
    );
    assert_eq!(
        store
            .collection_lifecycle_state(heap_id, collection_id)
            .unwrap(),
        CollectionLifecycleState::Retired
    );
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened
            .collection_lifecycle_state(heap_id, collection_id)
            .unwrap(),
        CollectionLifecycleState::Retired
    );
    assert_eq!(
        reopened
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_evidence(&before_retire)
            .unwrap(),
        rejected,
        "restart retry must preserve the terminal lifecycle conflict"
    );
}

#[test]
fn same_collection_identity_has_independent_lifecycle_in_two_heaps() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(&path).unwrap();
    let left = heap(2);
    let right = heap(3);
    publish_heap(&path, left, "left");
    publish_heap(&path, right, "right");
    let collection_id = CollectionId::from_bytes(identity16(33)).unwrap();

    residiuum_store::create_object(
        &HeapMetaLayout::new(&path),
        left.as_bytes(),
        residiuum_store::ObjectKind::Collection,
        *collection_id.as_bytes(),
        identity16(34),
        "shared-id-left",
    )
    .unwrap();
    assert_eq!(
        store
            .collection_lifecycle_state(left, collection_id)
            .unwrap(),
        CollectionLifecycleState::Active
    );
    assert_eq!(
        store
            .collection_lifecycle_state(right, collection_id)
            .unwrap(),
        CollectionLifecycleState::Absent
    );

    let right_absent = plan(
        right,
        atomic(4),
        collection_id,
        CollectionLifecycleState::Absent,
        None,
    );
    assert_eq!(
        store
            .atomic_stage_for_heap(right)
            .unwrap()
            .decide_plan_evidence(&right_absent)
            .unwrap()
            .decision,
        DecisionCode::Committed
    );

    // Keep an ordinary durable write in this test so reopen covers both media
    // authority and descriptor authority in one deployment.
    store
        .set_active_rule_revisions(right, &[], DurabilityMode::Durable)
        .unwrap();
}

#[test]
fn damaged_descriptor_suffix_is_coverage_incomplete_not_an_active_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(&path).unwrap();
    let heap_id = heap(5);
    publish_heap(&path, heap_id, "damaged-lifecycle");
    let collection_id = CollectionId::from_bytes(identity16(55)).unwrap();
    store
        .create_collection_ordered(
            heap_id.as_bytes(),
            *collection_id.as_bytes(),
            identity16(56),
            "damaged",
        )
        .unwrap();
    store
        .rename_collection_ordered(heap_id.as_bytes(), collection_id.as_bytes(), "damaged-v2")
        .unwrap();
    store
        .retire_collection_ordered(heap_id.as_bytes(), collection_id.as_bytes())
        .unwrap();

    let layout = HeapMetaLayout::new(&path);
    let mut frames: Vec<_> = std::fs::read_dir(layout.object_chain_dir(
        heap_id.as_bytes(),
        residiuum_store::ObjectKind::Collection,
        collection_id.as_bytes(),
    ))
    .unwrap()
    .map(|entry| entry.unwrap().path())
    .collect();
    frames.sort();
    std::fs::remove_file(frames.last().unwrap()).unwrap();

    assert!(store
        .collection_lifecycle_state(heap_id, collection_id)
        .is_err());
    let decision = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&plan(
            heap_id,
            atomic(6),
            collection_id,
            CollectionLifecycleState::Active,
            None,
        ))
        .unwrap();
    assert_eq!(decision.decision, DecisionCode::NotCommitted);
    assert_eq!(
        decision.abort_reason,
        Some(AtomicAbortReason::CoverageIncomplete)
    );
}
