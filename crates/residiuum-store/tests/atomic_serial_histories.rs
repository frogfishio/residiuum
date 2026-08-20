//! ATM-4D.1: independent point-serial history checker and anomaly corpus.

#![cfg(feature = "legacy-raw-store")]

use residiuum_atomics::{
    AtomicAbortReason, AtomicCohortOutcome, AtomicId, AtomicOutcome, AtomicPlan, AtomicPlanParts,
    AtomicProfile, CanonicalKey, CollectionId, CoordinationScope, HeapId, MutationKind,
    PlanMutation, PlanPredicate, PredicateKind, ReadWitness, ResourceLimits, VersionId,
};
use residiuum_format::{encode_subject_v2, SubjectObjectKind};
use residiuum_store::{DurabilityMode, Store};
use std::collections::BTreeMap;

type CellKey = (CollectionId, Vec<u8>);
type SerialState = BTreeMap<CellKey, VersionId>;

fn atomic(first: u8) -> AtomicId {
    let mut bytes = [0u8; 32];
    bytes[0] = first;
    AtomicId::from_bytes(bytes).unwrap()
}

fn collection() -> CollectionId {
    let mut bytes = [0u8; 16];
    bytes[0] = 17;
    CollectionId::from_bytes(bytes).unwrap()
}

fn key(name: &str) -> CanonicalKey {
    CanonicalKey::string(name.to_owned())
}

fn cell_key(collection_id: CollectionId, key: &CanonicalKey) -> CellKey {
    (collection_id, key.identity_bytes())
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

fn close_plan(
    heap: HeapId,
    id: AtomicId,
    reads: Vec<ReadWitness>,
    predicates: Vec<PlanPredicate>,
    mutations: Vec<PlanMutation>,
) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: (!reads.is_empty()).then_some([0xA4; 32]),
        reads,
        predicates,
        mutations,
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

fn replace(name: &str, version: VersionId, value: &[u8]) -> PlanMutation {
    PlanMutation {
        kind: MutationKind::Replace,
        collection_id: collection(),
        key: key(name),
        encoded_value: Some(value.to_vec()),
        if_version: Some(version),
    }
}

fn create(name: &str, value: &[u8]) -> PlanMutation {
    PlanMutation {
        kind: MutationKind::Create,
        collection_id: collection(),
        key: key(name),
        encoded_value: Some(value.to_vec()),
        if_version: None,
    }
}

fn delete(name: &str, version: VersionId) -> PlanMutation {
    PlanMutation {
        kind: MutationKind::Delete,
        collection_id: collection(),
        key: key(name),
        encoded_value: None,
        if_version: Some(version),
    }
}

fn witness(name: &str, version: Option<VersionId>) -> ReadWitness {
    ReadWitness {
        collection_id: collection(),
        key: key(name),
        observed_version: version,
        projection_hash: *blake3::hash(name.as_bytes()).as_bytes(),
    }
}

fn assert_absent(name: &str) -> PlanPredicate {
    PlanPredicate {
        kind: PredicateKind::AssertAbsent,
        collection_id: Some(collection()),
        key: Some(key(name)),
        version: None,
        encoded: None,
    }
}

fn plan_is_valid(plan: &AtomicPlan, state: &SerialState) -> bool {
    for read in plan.reads() {
        if state.get(&cell_key(read.collection_id, &read.key)).copied() != read.observed_version {
            return false;
        }
    }
    for predicate in plan.predicates() {
        if predicate.kind == PredicateKind::HeapAuthorityRevision {
            continue;
        }
        let (Some(collection_id), Some(key)) = (predicate.collection_id, predicate.key.as_ref())
        else {
            return false;
        };
        let observed = state.get(&cell_key(collection_id, key)).copied();
        let valid = match predicate.kind {
            PredicateKind::AssertAbsent => observed.is_none(),
            PredicateKind::AssertPresent => observed.is_some(),
            PredicateKind::AssertVersion => observed == predicate.version,
            _ => false,
        };
        if !valid {
            return false;
        }
    }
    plan.mutations().iter().all(|mutation| {
        let observed = state
            .get(&cell_key(mutation.collection_id, &mutation.key))
            .copied();
        match mutation.kind {
            MutationKind::Create => observed.is_none(),
            MutationKind::Put => true,
            MutationKind::Replace | MutationKind::Delete => observed == mutation.if_version,
        }
    })
}

fn apply_committed(
    plan: &AtomicPlan,
    outcome: &AtomicCohortOutcome,
    state: &mut SerialState,
) -> Option<u64> {
    let AtomicOutcome::Committed(receipt) = outcome.as_ref().ok()? else {
        return None;
    };
    if receipt.atomic_id != plan.atomic_id() || receipt.members.len() != plan.mutations().len() {
        return None;
    }
    for mutation in plan.mutations() {
        let identity = cell_key(mutation.collection_id, &mutation.key);
        let member = receipt.members.iter().find(|member| {
            member.collection_id == mutation.collection_id
                && member.key == mutation.key.identity_bytes()
        })?;
        match mutation.kind {
            MutationKind::Delete => {
                state.remove(&identity);
            }
            _ => {
                state.insert(identity, member.after_version?);
            }
        }
    }
    Some(receipt.commit_position)
}

/// Find a serial order without using production validation or submission order.
fn find_serial_order(
    initial: &SerialState,
    plans: &[AtomicPlan],
    outcomes: &[AtomicCohortOutcome],
) -> Option<Vec<usize>> {
    fn search(
        state: &SerialState,
        plans: &[AtomicPlan],
        outcomes: &[AtomicCohortOutcome],
        used: &mut [bool],
        order: &mut Vec<usize>,
        last_commit: u64,
    ) -> bool {
        if order.len() == plans.len() {
            return true;
        }
        for index in 0..plans.len() {
            if used[index] {
                continue;
            }
            let valid = plan_is_valid(&plans[index], state);
            let mut next = state.clone();
            let next_commit = match &outcomes[index] {
                Ok(AtomicOutcome::Committed(_)) if valid => {
                    let Some(position) =
                        apply_committed(&plans[index], &outcomes[index], &mut next)
                    else {
                        continue;
                    };
                    if position <= last_commit {
                        continue;
                    }
                    position
                }
                Ok(AtomicOutcome::NotCommitted {
                    reason: AtomicAbortReason::PreconditionConflict,
                    ..
                }) if !valid => last_commit,
                _ => continue,
            };
            used[index] = true;
            order.push(index);
            if search(&next, plans, outcomes, used, order, next_commit) {
                return true;
            }
            order.pop();
            used[index] = false;
        }
        false
    }

    if plans.len() != outcomes.len() || plans.len() > 8 {
        return None;
    }
    let mut used = vec![false; plans.len()];
    let mut order = Vec::with_capacity(plans.len());
    search(initial, plans, outcomes, &mut used, &mut order, 0).then_some(order)
}

fn seed(store: &mut Store, heap: HeapId, name: &str, value: &[u8]) -> VersionId {
    let receipt = store
        .put_subject_bytes(&subject(heap, &key(name)), value, DurabilityMode::Durable)
        .unwrap();
    VersionId::from_bytes(receipt.event_id).unwrap()
}

fn run_cohort(store: &mut Store, heap: HeapId, plans: &[AtomicPlan]) -> Vec<AtomicCohortOutcome> {
    store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_cohort_outcomes(plans)
        .unwrap()
}

fn committed_count(outcomes: &[AtomicCohortOutcome]) -> usize {
    outcomes
        .iter()
        .filter(|outcome| matches!(outcome, Ok(AtomicOutcome::Committed(_))))
        .count()
}

#[test]
fn lost_update_has_one_commit_and_an_independent_serial_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let version = seed(&mut store, heap, "counter", b"0");
    let initial = BTreeMap::from([(cell_key(collection(), &key("counter")), version)]);
    let plans = vec![
        close_plan(
            heap,
            atomic(1),
            vec![],
            vec![],
            vec![replace("counter", version, b"1")],
        ),
        close_plan(
            heap,
            atomic(2),
            vec![],
            vec![],
            vec![replace("counter", version, b"2")],
        ),
    ];
    let outcomes = run_cohort(&mut store, heap, &plans);
    assert_eq!(committed_count(&outcomes), 1);
    assert!(find_serial_order(&initial, &plans, &outcomes).is_some());
}

#[test]
fn write_skew_is_rejected_by_declared_read_witnesses() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let left = seed(&mut store, heap, "doctor-a", b"on");
    let right = seed(&mut store, heap, "doctor-b", b"on");
    let initial = BTreeMap::from([
        (cell_key(collection(), &key("doctor-a")), left),
        (cell_key(collection(), &key("doctor-b")), right),
    ]);
    let plans = vec![
        close_plan(
            heap,
            atomic(3),
            vec![witness("doctor-b", Some(right))],
            vec![],
            vec![replace("doctor-a", left, b"off")],
        ),
        close_plan(
            heap,
            atomic(4),
            vec![witness("doctor-a", Some(left))],
            vec![],
            vec![replace("doctor-b", right, b"off")],
        ),
    ];
    let outcomes = run_cohort(&mut store, heap, &plans);
    assert_eq!(committed_count(&outcomes), 1);
    assert!(find_serial_order(&initial, &plans, &outcomes).is_some());

    let witness_omitted = vec![
        close_plan(
            heap,
            atomic(3),
            vec![],
            vec![],
            vec![replace("doctor-a", left, b"off")],
        ),
        close_plan(
            heap,
            atomic(4),
            vec![],
            vec![],
            vec![replace("doctor-b", right, b"off")],
        ),
    ];
    assert!(
        find_serial_order(&initial, &witness_omitted, &outcomes).is_none(),
        "mutation-sensitive negative control: the recorded abort is unjustified when the dependency witnesses are removed"
    );
}

#[test]
fn delete_recreate_never_accepts_the_pre_delete_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let old = seed(&mut store, heap, "aba", b"old");
    let initial = BTreeMap::from([(cell_key(collection(), &key("aba")), old)]);
    let plans = vec![
        close_plan(heap, atomic(5), vec![], vec![], vec![delete("aba", old)]),
        close_plan(heap, atomic(6), vec![], vec![], vec![create("aba", b"new")]),
        close_plan(
            heap,
            atomic(7),
            vec![],
            vec![],
            vec![replace("aba", old, b"stale")],
        ),
    ];
    let outcomes = run_cohort(&mut store, heap, &plans);
    assert_eq!(committed_count(&outcomes), 2);
    assert!(find_serial_order(&initial, &plans, &outcomes).is_some());
    assert_eq!(
        store
            .get_subject_bytes(&subject(heap, &key("aba")))
            .unwrap(),
        Some(b"new".to_vec())
    );
}

#[test]
fn point_absence_predicate_prevents_a_uniqueness_phantom() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let initial = SerialState::new();
    let plans = vec![
        close_plan(
            heap,
            atomic(8),
            vec![],
            vec![assert_absent("unique")],
            vec![create("unique", b"first")],
        ),
        close_plan(
            heap,
            atomic(9),
            vec![],
            vec![assert_absent("unique")],
            vec![create("unique", b"second")],
        ),
    ];
    let outcomes = run_cohort(&mut store, heap, &plans);
    assert_eq!(committed_count(&outcomes), 1);
    assert!(find_serial_order(&initial, &plans, &outcomes).is_some());
}

#[test]
fn disjoint_updates_both_commit_while_still_forming_one_serial_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let left = seed(&mut store, heap, "left", b"0");
    let right = seed(&mut store, heap, "right", b"0");
    let initial = BTreeMap::from([
        (cell_key(collection(), &key("left")), left),
        (cell_key(collection(), &key("right")), right),
    ]);
    let plans = vec![
        close_plan(
            heap,
            atomic(10),
            vec![],
            vec![],
            vec![replace("left", left, b"1")],
        ),
        close_plan(
            heap,
            atomic(11),
            vec![],
            vec![],
            vec![replace("right", right, b"1")],
        ),
    ];
    let outcomes = run_cohort(&mut store, heap, &plans);
    assert_eq!(committed_count(&outcomes), 2);
    assert!(find_serial_order(&initial, &plans, &outcomes).is_some());
}

#[test]
fn ordinary_write_before_atomic_serialization_invalidates_the_stale_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let stale = seed(&mut store, heap, "mixed", b"ordinary-0");
    let plan = close_plan(
        heap,
        atomic(12),
        vec![witness("mixed", Some(stale))],
        vec![],
        vec![replace("mixed", stale, b"atomic")],
    );
    let current = seed(&mut store, heap, "mixed", b"ordinary-1");
    let initial = BTreeMap::from([(cell_key(collection(), &key("mixed")), current)]);
    let plans = vec![plan];
    let outcomes = run_cohort(&mut store, heap, &plans);
    assert_eq!(committed_count(&outcomes), 0);
    assert!(find_serial_order(&initial, &plans, &outcomes).is_some());
    assert_eq!(
        store
            .get_subject_bytes(&subject(heap, &key("mixed")))
            .unwrap(),
        Some(b"ordinary-1".to_vec())
    );
}

#[test]
fn deterministic_randomized_point_histories_all_have_a_checked_serial_order() {
    for campaign in 1u8..=24 {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::create(dir.path().join("s")).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let names = ["random-a", "random-b", "random-c"];
        let mut initial = SerialState::new();
        let mut versions = Vec::new();
        for name in names {
            let version = seed(&mut store, heap, name, b"initial");
            initial.insert(cell_key(collection(), &key(name)), version);
            versions.push(version);
        }

        let mut state = u64::from(campaign).wrapping_mul(0x9E37_79B9);
        let mut plans = Vec::new();
        for ordinal in 0u8..6 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let target = (state as usize) % names.len();
            plans.push(close_plan(
                heap,
                atomic(20 + ordinal),
                vec![],
                vec![],
                vec![replace(
                    names[target],
                    versions[target],
                    &[campaign, ordinal],
                )],
            ));
        }
        let outcomes = run_cohort(&mut store, heap, &plans);
        assert!(
            find_serial_order(&initial, &plans, &outcomes).is_some(),
            "campaign {campaign} has no serial explanation: {outcomes:?}"
        );
        assert!(committed_count(&outcomes) <= names.len());
    }
}
