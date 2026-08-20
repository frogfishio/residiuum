//! ATM-4D.5: active invariant revisions share the Atomic serialization frontier.

#![cfg(feature = "legacy-raw-store")]

use residiuum_atomics::{
    encode_active_rule_revision_equality, AtomicAbortReason, AtomicId, AtomicPlan, AtomicPlanParts,
    AtomicProfile, CanonicalKey, CollectionId, CoordinationScope, DecisionCode, HeapId,
    MutationKind, PlanMutation, ResourceLimits,
};
use residiuum_store::{DurabilityMode, Store};

fn heap(first: u8) -> HeapId {
    let mut bytes = [0u8; 16];
    bytes[0] = first;
    HeapId::from_bytes(bytes).unwrap()
}

fn atomic(first: u8) -> AtomicId {
    let mut bytes = [0u8; 32];
    bytes[0] = first;
    AtomicId::from_bytes(bytes).unwrap()
}

fn collection() -> CollectionId {
    let mut bytes = [0u8; 16];
    bytes[0] = 31;
    CollectionId::from_bytes(bytes).unwrap()
}

fn rule(first: u8) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[0] = first;
    bytes
}

fn plan(heap_id: HeapId, atomic_id: AtomicId, rules: &[[u8; 32]], key: &str) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id,
        heap_id,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: rules
            .iter()
            .copied()
            .map(encode_active_rule_revision_equality)
            .collect(),
        mutations: vec![PlanMutation {
            kind: MutationKind::Create,
            collection_id: collection(),
            key: CanonicalKey::string(key.to_owned()),
            encoded_value: Some(key.as_bytes().to_vec()),
            if_version: None,
        }],
        active_rule_revisions: rules.to_vec(),
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

#[test]
fn exact_rule_set_survives_restart_and_replays_the_terminal_decision() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(&path).unwrap();
    let heap_id = heap(1);
    let rules = [rule(1), rule(2)];

    assert!(store.active_rule_revisions(heap_id).unwrap().is_empty());
    store
        .set_active_rule_revisions(heap_id, &rules, DurabilityMode::Durable)
        .unwrap();
    assert_eq!(store.active_rule_revisions(heap_id).unwrap(), rules);

    let exact = plan(heap_id, atomic(1), &rules, "exact");
    let decision = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&exact)
        .unwrap();
    assert_eq!(decision.decision, DecisionCode::Committed);
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.active_rule_revisions(heap_id).unwrap(), rules);
    let replay = reopened
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&exact)
        .unwrap();
    assert_eq!(replay, decision);
}

#[test]
fn membership_is_not_enough_and_activation_before_decision_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("store")).unwrap();
    let heap_id = heap(2);
    let first = rule(1);
    let second = rule(2);
    store
        .set_active_rule_revisions(heap_id, &[first, second], DurabilityMode::Durable)
        .unwrap();

    let incomplete = plan(heap_id, atomic(2), &[first], "membership-only");
    let rejected = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&incomplete)
        .unwrap();
    assert_eq!(rejected.decision, DecisionCode::NotCommitted);
    assert_eq!(
        rejected.abort_reason,
        Some(AtomicAbortReason::PreconditionConflict)
    );

    let stale = plan(
        heap_id,
        atomic(3),
        &[first, second],
        "stale-after-activation",
    );
    let third = rule(3);
    store
        .set_active_rule_revisions(heap_id, &[first, second, third], DurabilityMode::Durable)
        .unwrap();
    let rejected = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&stale)
        .unwrap();
    assert_eq!(rejected.decision, DecisionCode::NotCommitted);
    assert_eq!(
        rejected.abort_reason,
        Some(AtomicAbortReason::PreconditionConflict)
    );

    let current = plan(heap_id, atomic(4), &[first, second, third], "current");
    assert_eq!(
        store
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_evidence(&current)
            .unwrap()
            .decision,
        DecisionCode::Committed
    );
}

#[test]
fn active_rule_sets_are_isolated_by_heap() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("store")).unwrap();
    let left = heap(3);
    let right = heap(4);
    store
        .set_active_rule_revisions(left, &[rule(7)], DurabilityMode::Durable)
        .unwrap();

    assert_eq!(store.active_rule_revisions(left).unwrap(), vec![rule(7)]);
    assert!(store.active_rule_revisions(right).unwrap().is_empty());
    assert_eq!(
        store
            .atomic_stage_for_heap(right)
            .unwrap()
            .decide_plan_evidence(&plan(right, atomic(5), &[], "right"))
            .unwrap()
            .decision,
        DecisionCode::Committed
    );
}
