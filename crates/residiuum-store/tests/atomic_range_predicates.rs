//! ATM-4D.4: exact bounded-range validation at the Heap serialization frontier.

#![cfg(feature = "legacy-raw-store")]

use residiuum_atomics::{
    encode_bounded_key_range_absence, encode_bounded_key_range_presence, AtomicAbortReason,
    AtomicId, AtomicOutcome, AtomicPlan, AtomicPlanParts, AtomicProfile, BoundedKeyRange,
    CanonicalKey, CollectionId, CoordinationScope, HeapId, MutationKind, PlanMutation, RangeEntry,
    ResourceLimits, VersionId,
};
use residiuum_format::{encode_subject_v2, SubjectObjectKind};
use residiuum_store::{DurabilityMode, Store, TierClass, TierMoveMode};

fn atomic(first: u8) -> AtomicId {
    let mut bytes = [0; 32];
    bytes[0] = first;
    AtomicId::from_bytes(bytes).unwrap()
}

fn collection() -> CollectionId {
    let mut bytes = [0; 16];
    bytes[0] = 29;
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

fn put(store: &mut Store, heap: HeapId, key: &CanonicalKey, value: &[u8]) -> VersionId {
    let receipt = store
        .put_subject_bytes(&subject(heap, key), value, DurabilityMode::Durable)
        .unwrap();
    VersionId::from_bytes(receipt.event_id).unwrap()
}

fn mutation(key: CanonicalKey) -> PlanMutation {
    PlanMutation {
        kind: MutationKind::Create,
        collection_id: collection(),
        key,
        encoded_value: Some(b"proof".to_vec()),
        if_version: None,
    }
}

fn range_plan(
    heap: HeapId,
    id: u8,
    range: &BoundedKeyRange,
    presence: bool,
    mutation: Option<PlanMutation>,
) -> AtomicPlan {
    let predicate = if presence {
        encode_bounded_key_range_presence(collection(), range).unwrap()
    } else {
        encode_bounded_key_range_absence(collection(), range).unwrap()
    };
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: atomic(id),
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![predicate],
        mutations: mutation.into_iter().collect(),
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

fn not_committed(outcome: AtomicOutcome, id: u8, reason: AtomicAbortReason) {
    assert_eq!(
        outcome,
        AtomicOutcome::NotCommitted {
            atomic_id: atomic(id),
            reason,
        }
    );
}

#[test]
fn range_absence_rejects_an_in_range_cohort_phantom() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let range = BoundedKeyRange::observed(
        collection(),
        CanonicalKey::string("a"),
        true,
        CanonicalKey::string("m"),
        true,
        100,
        &[],
    )
    .unwrap();
    let writer = AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: atomic(1),
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: vec![mutation(CanonicalKey::string("g"))],
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap();
    let guarded = range_plan(
        heap,
        2,
        &range,
        false,
        Some(mutation(CanonicalKey::string("proof-outside"))),
    );
    let outcomes = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_cohort_outcomes(&[writer, guarded])
        .unwrap();
    assert!(matches!(outcomes[0], Ok(AtomicOutcome::Committed(_))));
    not_committed(
        outcomes[1].clone().unwrap(),
        2,
        AtomicAbortReason::PreconditionConflict,
    );
    assert_eq!(
        store
            .get_subject_bytes(&subject(heap, &CanonicalKey::string("proof-outside")))
            .unwrap(),
        None
    );
}

#[test]
fn outside_range_write_does_not_create_a_false_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let range = BoundedKeyRange::observed(
        collection(),
        CanonicalKey::string("a"),
        true,
        CanonicalKey::string("m"),
        true,
        100,
        &[],
    )
    .unwrap();
    put(&mut store, heap, &CanonicalKey::string("z"), b"outside");
    let plan = range_plan(
        heap,
        3,
        &range,
        false,
        Some(mutation(CanonicalKey::string("result"))),
    );
    assert!(matches!(
        store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_outcome(&plan)
            .unwrap(),
        AtomicOutcome::Committed(_)
    ));
}

#[test]
fn range_presence_binds_every_key_and_establishing_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let key_b = CanonicalKey::string("b");
    let key_d = CanonicalKey::string("d");
    let version_b = put(&mut store, heap, &key_b, b"b0");
    let version_d = put(&mut store, heap, &key_d, b"d0");
    let range = BoundedKeyRange::observed(
        collection(),
        CanonicalKey::string("a"),
        true,
        CanonicalKey::string("e"),
        false,
        100,
        &[
            RangeEntry {
                key: key_d.clone(),
                version: version_d,
            },
            RangeEntry {
                key: key_b.clone(),
                version: version_b,
            },
        ],
    )
    .unwrap();

    put(&mut store, heap, &CanonicalKey::string("z"), b"outside");
    let still_exact = range_plan(heap, 4, &range, true, None);
    let AtomicOutcome::Committed(committed) = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_outcome(&still_exact)
        .unwrap()
    else {
        panic!("exact range did not commit")
    };
    assert!(committed.members.is_empty());
    assert!(!committed.replayed);
    let AtomicOutcome::Committed(replayed) = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_outcome(&still_exact)
        .unwrap()
    else {
        panic!("exact range retry did not replay")
    };
    assert!(replayed.replayed);

    put(&mut store, heap, &key_b, b"b1");
    let stale = range_plan(heap, 5, &range, true, None);
    not_committed(
        store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_outcome(&stale)
            .unwrap(),
        5,
        AtomicAbortReason::PreconditionConflict,
    );
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    let AtomicOutcome::Committed(replayed_after_restart) = reopened
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_outcome(&still_exact)
        .unwrap()
    else {
        panic!("committed predicate-only range changed after restart")
    };
    assert!(replayed_after_restart.replayed);
    not_committed(
        reopened
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_outcome(&stale)
            .unwrap(),
        5,
        AtomicAbortReason::PreconditionConflict,
    );
}

#[test]
fn range_presence_observes_a_prior_cohort_delete() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let key_b = CanonicalKey::string("b");
    let key_d = CanonicalKey::string("d");
    let version_b = put(&mut store, heap, &key_b, b"b0");
    let version_d = put(&mut store, heap, &key_d, b"d0");
    let range = BoundedKeyRange::observed(
        collection(),
        CanonicalKey::string("a"),
        true,
        CanonicalKey::string("e"),
        false,
        100,
        &[
            RangeEntry {
                key: key_b.clone(),
                version: version_b,
            },
            RangeEntry {
                key: key_d,
                version: version_d,
            },
        ],
    )
    .unwrap();
    let delete = AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: atomic(9),
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: vec![PlanMutation {
            kind: MutationKind::Delete,
            collection_id: collection(),
            key: key_b,
            encoded_value: None,
            if_version: Some(version_b),
        }],
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap();
    let guarded = range_plan(heap, 10, &range, true, None);

    let outcomes = store
        .atomic_stage_for_heap(heap)
        .unwrap()
        .decide_plan_cohort_outcomes(&[delete, guarded])
        .unwrap();
    assert!(matches!(outcomes[0], Ok(AtomicOutcome::Committed(_))));
    not_committed(
        outcomes[1].clone().unwrap(),
        10,
        AtomicAbortReason::PreconditionConflict,
    );
}

#[test]
fn integer_range_uses_mathematical_not_subject_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let negative = CanonicalKey::integer(-2);
    let positive = CanonicalKey::integer(2);
    let negative_version = put(&mut store, heap, &negative, b"negative");
    let positive_version = put(&mut store, heap, &positive, b"positive");
    put(&mut store, heap, &CanonicalKey::integer(128), b"outside");
    let range = BoundedKeyRange::observed(
        collection(),
        CanonicalKey::integer(-10),
        true,
        CanonicalKey::integer(10),
        true,
        100,
        &[
            RangeEntry {
                key: positive,
                version: positive_version,
            },
            RangeEntry {
                key: negative,
                version: negative_version,
            },
        ],
    )
    .unwrap();
    assert!(matches!(
        store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_outcome(&range_plan(heap, 6, &range, true, None))
            .unwrap(),
        AtomicOutcome::Committed(_)
    ));
}

#[test]
fn range_refuses_to_claim_absence_beyond_its_work_or_tier_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    put(&mut store, heap, &CanonicalKey::string("x"), b"one");
    put(&mut store, heap, &CanonicalKey::string("y"), b"two");
    let bounded = BoundedKeyRange::observed(
        collection(),
        CanonicalKey::string("a"),
        true,
        CanonicalKey::string("b"),
        true,
        1,
        &[],
    )
    .unwrap();
    not_committed(
        store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_outcome(&range_plan(heap, 7, &bounded, false, None))
            .unwrap(),
        7,
        AtomicAbortReason::CoverageIncomplete,
    );

    store.seal_active().unwrap();
    let segment = store
        .list_segment_summaries()
        .into_iter()
        .find(|summary| summary.item_events > 0)
        .unwrap()
        .segment_id;
    store
        .transfer_segment_to_tier(segment, TierClass::Archive, TierMoveMode::Move)
        .unwrap();
    store.set_tier_available(TierClass::Archive, false).unwrap();
    assert!(store.tier_coverage().is_incomplete());
    let unavailable = BoundedKeyRange::observed(
        collection(),
        CanonicalKey::string("a"),
        true,
        CanonicalKey::string("z"),
        true,
        100,
        &[],
    )
    .unwrap();
    not_committed(
        store
            .atomic_stage_for_heap(heap)
            .unwrap()
            .decide_plan_outcome(&range_plan(heap, 8, &unavailable, false, None))
            .unwrap(),
        8,
        AtomicAbortReason::CoverageIncomplete,
    );
}
