//! ATM-1.2: closed-plan validator vs serial oracle differential.

use residiuum_atomics::{
    encode_assert_absent, encode_assert_present, encode_create, encode_delete, encode_put,
    encode_replace, serialize_canonical_value, validate_closed_plan, AtomicAbortReason, AtomicId,
    AtomicOutcome, AtomicPlan, AtomicPlanParts, AtomicProfile, AtomicRefuseReason, AtomicsError,
    CanonicalKey, CanonicalValue, CollectionId, CoordinationScope, HeapId, LogicalStatus,
    PlanMutation, PlanPredicate, PredicateKind, ResourceLimits, SerialOracle,
};

fn hid(n: u8) -> HeapId {
    let mut b = [0u8; 16];
    b[0] = n;
    HeapId::from_bytes(b).unwrap()
}

fn cid(n: u16) -> CollectionId {
    let mut b = [0u8; 16];
    b[0..2].copy_from_slice(&n.to_be_bytes());
    if n == 0 {
        b[15] = 1;
    }
    CollectionId::from_bytes(b).unwrap()
}

fn aid(n: u32) -> AtomicId {
    let mut b = [0u8; 32];
    b[0..4].copy_from_slice(&n.to_be_bytes());
    b[31] = 1;
    AtomicId::from_bytes(b).unwrap()
}

fn key(s: &str) -> CanonicalKey {
    CanonicalKey::String(s.to_owned())
}

fn val(bytes: &[u8]) -> CanonicalValue {
    serialize_canonical_value(bytes)
}

fn defaults() -> ResourceLimits {
    ResourceLimits::builder_defaults_local_heap()
}

fn hard() -> ResourceLimits {
    ResourceLimits::hard_local_heap()
}

fn close(
    heap: HeapId,
    id: u32,
    scope: CoordinationScope,
    limits: ResourceLimits,
    mutations: Vec<PlanMutation>,
    predicates: Vec<PlanPredicate>,
) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: aid(id),
        heap_id: heap,
        scope,
        read_frontier: None,
        reads: Vec::new(),
        predicates,
        mutations,
        active_rule_revisions: Vec::new(),
        limits,
    })
    .expect("close")
}

fn local(
    heap: HeapId,
    id: u32,
    limits: ResourceLimits,
    mutations: Vec<PlanMutation>,
    predicates: Vec<PlanPredicate>,
) -> AtomicPlan {
    close(
        heap,
        id,
        CoordinationScope::LocalHeap,
        limits,
        mutations,
        predicates,
    )
}

fn creates(heap: HeapId, id: u32, n: u32, limits: ResourceLimits) -> AtomicPlan {
    let mutations = (0..n)
        .map(|i| encode_create(cid(1), key(&format!("k{i}")), val(b"v")))
        .collect();
    local(heap, id, limits, mutations, Vec::new())
}

fn n_collections(heap: HeapId, id: u32, n: u32, limits: ResourceLimits) -> AtomicPlan {
    let mutations = (1..=n)
        .map(|i| encode_create(cid(i as u16), key("same"), val(b"v")))
        .collect();
    local(heap, id, limits, mutations, Vec::new())
}

fn apply_admitted(oracle: &mut SerialOracle, plan: &AtomicPlan) -> AtomicOutcome {
    assert_eq!(validate_closed_plan(plan, oracle.heap_id()), Ok(()));
    oracle
        .apply(plan)
        .expect("admitted plan must not refuse structurally")
}

fn assert_refuse(oracle: &mut SerialOracle, plan: &AtomicPlan, reason: AtomicRefuseReason) {
    assert_eq!(
        validate_closed_plan(plan, oracle.heap_id()),
        Err(AtomicsError::Refused(reason))
    );
    assert_eq!(
        oracle.apply(plan).unwrap_err(),
        AtomicsError::Refused(reason)
    );
    assert_eq!(
        oracle.status(plan.atomic_id()).logical,
        LogicalStatus::NotFound
    );
}

#[test]
fn member_counts_agree_with_oracle() {
    let heap = hid(1);
    for n in [1_u32, 2, 10, 64, 256] {
        let limits = if n <= 64 { defaults() } else { hard() };
        let mut oracle = SerialOracle::new(heap);
        let plan = creates(heap, 1000 + n, n, limits);
        match apply_admitted(&mut oracle, &plan) {
            AtomicOutcome::Committed(receipt) => {
                assert_eq!(receipt.members.len(), n as usize, "n={n}");
            }
            other => panic!("n={n}: {other:?}"),
        }
    }
}

#[test]
fn collection_counts_agree_with_oracle() {
    let heap = hid(1);
    for n in [1_u32, 16, 64] {
        let limits = if n <= 16 { defaults() } else { hard() };
        let mut oracle = SerialOracle::new(heap);
        let plan = n_collections(heap, 2000 + n, n, limits);
        match apply_admitted(&mut oracle, &plan) {
            AtomicOutcome::Committed(_) => {
                assert!(oracle.get(cid(1), &key("same")).is_some());
                assert!(oracle.get(cid(n as u16), &key("same")).is_some());
            }
            other => panic!("n={n}: {other:?}"),
        }
    }
}

#[test]
fn mixed_mutations_agree_with_oracle() {
    let heap = hid(1);
    let mut oracle = SerialOracle::new(heap);
    let seed = local(
        heap,
        1,
        defaults(),
        vec![
            encode_create(cid(1), key("keep"), val(b"1")),
            encode_create(cid(1), key("gone"), val(b"2")),
        ],
        Vec::new(),
    );
    let AtomicOutcome::Committed(_) = apply_admitted(&mut oracle, &seed) else {
        panic!("seed");
    };
    let keep_ver = oracle.get(cid(1), &key("keep")).unwrap().version;
    let gone_ver = oracle.get(cid(1), &key("gone")).unwrap().version;
    let mixed = local(
        heap,
        2,
        defaults(),
        vec![
            encode_create(cid(1), key("new"), val(b"n")),
            encode_put(cid(1), key("blind"), val(b"p")),
            encode_replace(cid(1), key("keep"), keep_ver, val(b"1b")),
            encode_delete(cid(1), key("gone"), gone_ver),
        ],
        Vec::new(),
    );
    match apply_admitted(&mut oracle, &mixed) {
        AtomicOutcome::Committed(_) => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(oracle.get(cid(1), &key("keep")).unwrap().value, b"1b");
    assert!(oracle.get(cid(1), &key("gone")).is_none());
    assert_eq!(oracle.get(cid(1), &key("new")).unwrap().value, b"n");
    assert_eq!(oracle.get(cid(1), &key("blind")).unwrap().value, b"p");
}

#[test]
fn assertion_only_agrees_with_oracle() {
    let heap = hid(1);
    let mut oracle = SerialOracle::new(heap);
    let absent = local(
        heap,
        3,
        defaults(),
        Vec::new(),
        vec![encode_assert_absent(cid(1), key("ghost"))],
    );
    assert!(matches!(
        apply_admitted(&mut oracle, &absent),
        AtomicOutcome::Committed(_)
    ));
    let present = local(
        heap,
        4,
        defaults(),
        Vec::new(),
        vec![encode_assert_present(cid(1), key("ghost"))],
    );
    match apply_admitted(&mut oracle, &present) {
        AtomicOutcome::NotCommitted { reason, .. } => {
            assert_eq!(reason, AtomicAbortReason::PreconditionConflict);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn same_key_across_collections_is_independent() {
    let heap = hid(1);
    let mut oracle = SerialOracle::new(heap);
    let plan = n_collections(heap, 5, 2, defaults());
    apply_admitted(&mut oracle, &plan);
    assert_eq!(oracle.get(cid(1), &key("same")).unwrap().value, b"v");
    assert_eq!(oracle.get(cid(2), &key("same")).unwrap().value, b"v");
}

#[test]
fn same_names_across_two_heaps_do_not_interfere() {
    let a = hid(1);
    let b = hid(2);
    let mut oa = SerialOracle::new(a);
    let mut ob = SerialOracle::new(b);
    let pa = local(
        a,
        6,
        defaults(),
        vec![encode_create(cid(1), key("same"), val(b"A"))],
        Vec::new(),
    );
    let pb = local(
        b,
        7,
        defaults(),
        vec![encode_create(cid(1), key("same"), val(b"B"))],
        Vec::new(),
    );
    apply_admitted(&mut oa, &pa);
    apply_admitted(&mut ob, &pb);
    assert_refuse(&mut oa, &pb, AtomicRefuseReason::CrossHeapCollection);
    assert_eq!(oa.get(cid(1), &key("same")).unwrap().value, b"A");
    assert_eq!(ob.get(cid(1), &key("same")).unwrap().value, b"B");
}

#[test]
fn max_accepted_value_bytes_agree() {
    let heap = hid(1);
    let mut limits = defaults();
    limits.total_proposed_value_bytes = 4;
    let mut oracle = SerialOracle::new(heap);
    let plan = local(
        heap,
        8,
        limits,
        vec![encode_create(cid(1), key("k"), val(b"abcd"))],
        Vec::new(),
    );
    assert!(matches!(
        apply_admitted(&mut oracle, &plan),
        AtomicOutcome::Committed(_)
    ));
}

#[test]
fn one_unit_over_limit_is_refused() {
    let heap = hid(1);
    let mut oracle = SerialOracle::new(heap);
    assert_refuse(
        &mut oracle,
        &creates(heap, 10, 65, defaults()),
        AtomicRefuseReason::LimitExceeded,
    );
    assert_refuse(
        &mut oracle,
        &creates(heap, 11, 257, hard()),
        AtomicRefuseReason::LimitExceeded,
    );
    assert_refuse(
        &mut oracle,
        &n_collections(heap, 12, 17, defaults()),
        AtomicRefuseReason::LimitExceeded,
    );
    assert_refuse(
        &mut oracle,
        &n_collections(heap, 13, 65, hard()),
        AtomicRefuseReason::LimitExceeded,
    );

    let mut tiny_value = defaults();
    tiny_value.total_proposed_value_bytes = 3;
    assert_refuse(
        &mut oracle,
        &local(
            heap,
            14,
            tiny_value,
            vec![encode_create(cid(1), key("k"), val(b"abcd"))],
            Vec::new(),
        ),
        AtomicRefuseReason::LimitExceeded,
    );

    let mut tiny_pred = defaults();
    tiny_pred.predicates = 2;
    assert_refuse(
        &mut oracle,
        &local(
            heap,
            15,
            tiny_pred,
            Vec::new(),
            vec![
                encode_assert_absent(cid(1), key("a")),
                encode_assert_absent(cid(1), key("b")),
                encode_assert_absent(cid(1), key("c")),
            ],
        ),
        AtomicRefuseReason::LimitExceeded,
    );

    let mut tiny_plan = defaults();
    tiny_plan.canonical_plan_bytes = 32;
    assert_refuse(
        &mut oracle,
        &local(
            heap,
            16,
            tiny_plan,
            vec![encode_create(cid(1), key("k"), val(b"v"))],
            Vec::new(),
        ),
        AtomicRefuseReason::LimitExceeded,
    );

    assert_refuse(
        &mut oracle,
        &close(
            heap,
            17,
            CoordinationScope::Key,
            ResourceLimits::hard_key(),
            vec![
                encode_create(cid(1), key("a"), val(b"1")),
                encode_create(cid(1), key("b"), val(b"2")),
            ],
            Vec::new(),
        ),
        AtomicRefuseReason::LimitExceeded,
    );
}

#[test]
fn validator_is_sensitive_to_single_field_flips() {
    let heap = hid(1);
    let mut oracle = SerialOracle::new(heap);
    let good = local(
        heap,
        20,
        defaults(),
        vec![encode_create(cid(1), key("k"), val(b"v"))],
        Vec::new(),
    );
    apply_admitted(&mut oracle, &good);

    let mut unknown = AtomicPlanParts {
        profile: AtomicProfile::from_wire_code(99),
        atomic_id: aid(21),
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: Vec::new(),
        predicates: Vec::new(),
        mutations: vec![encode_create(cid(1), key("u"), val(b"v"))],
        active_rule_revisions: Vec::new(),
        limits: defaults(),
    };
    assert_refuse(
        &mut oracle,
        &AtomicPlan::close(unknown.clone()).unwrap(),
        AtomicRefuseReason::UnsupportedProfile,
    );
    unknown.profile = AtomicProfile::LocalHeapV1;
    apply_admitted(&mut oracle, &AtomicPlan::close(unknown).unwrap());

    let partition = close(
        heap,
        22,
        CoordinationScope::Partition,
        ResourceLimits::hard_partition(),
        vec![encode_create(cid(1), key("p"), val(b"v"))],
        Vec::new(),
    );
    assert_refuse(
        &mut oracle,
        &partition,
        AtomicRefuseReason::ScopeUnavailable,
    );

    let foreign = local(
        hid(9),
        23,
        defaults(),
        vec![encode_create(cid(1), key("x"), val(b"v"))],
        Vec::new(),
    );
    assert_refuse(
        &mut oracle,
        &foreign,
        AtomicRefuseReason::CrossHeapCollection,
    );

    let compiled = local(
        heap,
        24,
        defaults(),
        Vec::new(),
        vec![PlanPredicate {
            kind: PredicateKind::ExactScalarEquality,
            collection_id: Some(cid(1)),
            key: Some(key("k")),
            version: None,
            encoded: Some(b"rql".to_vec()),
        }],
    );
    assert_refuse(
        &mut oracle,
        &compiled,
        AtomicRefuseReason::UnsupportedPredicate,
    );
}

#[test]
fn data_conflict_is_not_a_structural_refusal() {
    let heap = hid(1);
    let mut oracle = SerialOracle::new(heap);
    let first = local(
        heap,
        30,
        defaults(),
        vec![encode_create(cid(1), key("k"), val(b"1"))],
        Vec::new(),
    );
    apply_admitted(&mut oracle, &first);
    let again = local(
        heap,
        31,
        defaults(),
        vec![encode_create(cid(1), key("k"), val(b"2"))],
        Vec::new(),
    );
    match apply_admitted(&mut oracle, &again) {
        AtomicOutcome::NotCommitted { reason, .. } => {
            assert_eq!(reason, AtomicAbortReason::PreconditionConflict);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(oracle.get(cid(1), &key("k")).unwrap().value, b"1");
}
