//! ATM-0.2 / ATM-0.8 properties: builder order, total read/predicate order, frontier.

use residiuum_atomics::{
    decode_canonical_plan, encode_canonical_plan, plan_content_root, AtomicId, AtomicPlan,
    AtomicPlanParts, AtomicProfile, AtomicRefuseReason, AtomicsError, CanonicalKey, CollectionId,
    CoordinationScope, HeapId, MutationKind, PlanMutation, PlanPredicate, PredicateKind,
    ReadWitness, ResourceLimits, VersionId,
};

fn hid(n: u8) -> HeapId {
    let mut b = [0u8; 16];
    b[0] = n;
    HeapId::from_bytes(b).unwrap()
}

fn cid(n: u8) -> CollectionId {
    let mut b = [0u8; 16];
    b[0] = n;
    CollectionId::from_bytes(b).unwrap()
}

fn aid(n: u8) -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = n;
    AtomicId::from_bytes(b).unwrap()
}

fn key(s: &str) -> CanonicalKey {
    CanonicalKey::String(s.to_owned())
}

fn vid(n: u8) -> VersionId {
    let mut b = [0u8; 16];
    b[0] = n;
    VersionId::from_bytes(b).unwrap()
}

fn create(coll: u8, k: &str, val: &[u8]) -> PlanMutation {
    PlanMutation {
        kind: MutationKind::Create,
        collection_id: cid(coll),
        key: key(k),
        encoded_value: Some(val.to_vec()),
        if_version: None,
    }
}

fn parts(heap: u8, mutations: Vec<PlanMutation>) -> AtomicPlanParts {
    AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: aid(9),
        heap_id: hid(heap),
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: Vec::new(),
        predicates: Vec::new(),
        mutations,
        active_rule_revisions: Vec::new(),
        limits: ResourceLimits::builder_defaults_local_heap(),
    }
}

#[test]
fn equivalent_builder_order_same_bytes_and_root() {
    let a = AtomicPlan::close(parts(
        1,
        vec![create(1, "zeta", b"z"), create(1, "alpha", b"a")],
    ))
    .unwrap();
    let b = AtomicPlan::close(parts(
        1,
        vec![create(1, "alpha", b"a"), create(1, "zeta", b"z")],
    ))
    .unwrap();
    let ea = encode_canonical_plan(&a).unwrap();
    let eb = encode_canonical_plan(&b).unwrap();
    assert_eq!(ea, eb);
    assert_eq!(
        plan_content_root(&a).unwrap(),
        plan_content_root(&b).unwrap()
    );
    assert_eq!(a.mutations()[0].key, key("alpha"));
    assert_eq!(a.mutations()[1].key, key("zeta"));
}

#[test]
fn semantic_change_changes_root() {
    let a = AtomicPlan::close(parts(1, vec![create(1, "k", b"one")])).unwrap();
    let b = AtomicPlan::close(parts(1, vec![create(1, "k", b"two")])).unwrap();
    assert_ne!(
        plan_content_root(&a).unwrap(),
        plan_content_root(&b).unwrap()
    );
}

#[test]
fn heap_substitution_changes_root() {
    let a = AtomicPlan::close(parts(1, vec![create(1, "k", b"v")])).unwrap();
    let b = AtomicPlan::close(parts(2, vec![create(1, "k", b"v")])).unwrap();
    assert_ne!(
        encode_canonical_plan(&a).unwrap(),
        encode_canonical_plan(&b).unwrap()
    );
    assert_ne!(
        plan_content_root(&a).unwrap(),
        plan_content_root(&b).unwrap()
    );
}

#[test]
fn collection_substitution_changes_root() {
    let a = AtomicPlan::close(parts(1, vec![create(1, "k", b"v")])).unwrap();
    let b = AtomicPlan::close(parts(1, vec![create(2, "k", b"v")])).unwrap();
    assert_ne!(
        plan_content_root(&a).unwrap(),
        plan_content_root(&b).unwrap()
    );
}

#[test]
fn duplicate_target_is_refused() {
    let err = AtomicPlan::close(parts(
        1,
        vec![create(1, "same", b"1"), create(1, "same", b"2")],
    ))
    .unwrap_err();
    assert_eq!(
        err,
        AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget)
    );
}

#[test]
fn encode_decode_roundtrip() {
    let plan =
        AtomicPlan::close(parts(1, vec![create(3, "b", b"bb"), create(1, "a", b"aa")])).unwrap();
    let bytes = encode_canonical_plan(&plan).unwrap();
    let again = decode_canonical_plan(&bytes).unwrap();
    assert_eq!(plan, again);
    assert_eq!(bytes, encode_canonical_plan(&again).unwrap());
}

#[test]
fn spec_field_numbers_match_codec() {
    let spec: serde_json::Value =
        serde_json::from_str(include_str!("../spec/cbor-v1.json")).unwrap();
    assert_eq!(spec["plan_labels"]["1"]["field"], "profile");
    assert_eq!(spec["plan_labels"]["2"]["field"], "atomic_id");
    assert_eq!(spec["plan_labels"]["8"]["field"], "mutations");
    assert_eq!(spec["plan_labels"]["10"]["field"], "limits");
    assert_eq!(spec["mutation_kinds"]["2"], "put");
    assert_eq!(
        spec["domain_separators"]["content"],
        "RESIDIUUM-ATOMIC-CONTENT-V1"
    );
    assert_eq!(spec["widths"]["atomic_id"], 32);
    assert_eq!(spec["hard_ceilings"]["local_heap"]["caller_mutations"], 256);
    assert_eq!(spec["canonical_read_order"][3], "observed_version");
    assert!(spec["read_frontier_rule"]
        .as_str()
        .unwrap()
        .contains("required"));
}

fn witness(coll: u8, k: &str, version: Option<u8>, proj: u8) -> ReadWitness {
    ReadWitness {
        collection_id: cid(coll),
        key: key(k),
        observed_version: version.map(vid),
        projection_hash: [proj; 32],
    }
}

fn pred(kind: PredicateKind, coll: u8, k: &str, version: Option<u8>) -> PlanPredicate {
    PlanPredicate {
        kind,
        collection_id: Some(cid(coll)),
        key: Some(key(k)),
        version: version.map(vid),
        encoded: None,
    }
}

fn assert_same_canonical(a: AtomicPlanParts, b: AtomicPlanParts) {
    let pa = AtomicPlan::close(a).unwrap();
    let pb = AtomicPlan::close(b).unwrap();
    assert_eq!(
        encode_canonical_plan(&pa).unwrap(),
        encode_canonical_plan(&pb).unwrap()
    );
    assert_eq!(
        plan_content_root(&pa).unwrap(),
        plan_content_root(&pb).unwrap()
    );
}

#[test]
fn read_witness_builder_order_is_canonical() {
    let mut a = parts(1, vec![create(1, "k", b"v")]);
    a.read_frontier = Some([7u8; 32]);
    a.reads = vec![witness(1, "zeta", None, 1), witness(1, "alpha", Some(3), 2)];
    let mut b = a.clone();
    b.reads.reverse();
    assert_same_canonical(a, b);
}

#[test]
fn predicate_builder_order_is_canonical() {
    let mut a = parts(1, vec![create(1, "k", b"v")]);
    a.predicates = vec![
        pred(PredicateKind::AssertPresent, 1, "zeta", None),
        pred(PredicateKind::AssertAbsent, 1, "alpha", None),
    ];
    let mut b = a.clone();
    b.predicates.reverse();
    assert_same_canonical(a, b);
}

#[test]
fn rule_revision_builder_order_is_canonical() {
    let mut a = parts(1, vec![create(1, "k", b"v")]);
    a.active_rule_revisions = vec![[9u8; 32], [1u8; 32]];
    let mut b = a.clone();
    b.active_rule_revisions.reverse();
    assert_same_canonical(a, b);
}

#[test]
fn mixed_builder_permutation_same_root() {
    let mut a = parts(1, vec![create(1, "zeta", b"z"), create(1, "alpha", b"a")]);
    a.read_frontier = Some([7u8; 32]);
    a.reads = vec![witness(1, "r2", None, 2), witness(1, "r1", Some(1), 1)];
    a.predicates = vec![
        pred(PredicateKind::AssertPresent, 2, "lock", None),
        pred(PredicateKind::AssertVersion, 1, "k", Some(4)),
    ];
    a.active_rule_revisions = vec![[3u8; 32], [2u8; 32]];
    let mut b = a.clone();
    b.mutations.reverse();
    b.reads.reverse();
    b.predicates.reverse();
    b.active_rule_revisions.reverse();
    assert_same_canonical(a, b);
}

#[test]
fn duplicate_read_identity_is_refused_even_when_version_differs() {
    let mut p = parts(1, vec![create(1, "k", b"v")]);
    p.read_frontier = Some([7u8; 32]);
    p.reads = vec![
        witness(1, "same", Some(1), 1),
        witness(1, "same", Some(2), 9),
    ];
    assert_eq!(
        AtomicPlan::close(p).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
    );
}

#[test]
fn duplicate_predicate_identity_is_refused_even_when_version_differs() {
    let mut p = parts(1, vec![create(1, "k", b"v")]);
    p.predicates = vec![
        pred(PredicateKind::AssertVersion, 1, "k", Some(1)),
        pred(PredicateKind::AssertVersion, 1, "k", Some(2)),
    ];
    assert_eq!(
        AtomicPlan::close(p).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
    );
}

#[test]
fn prior_read_without_frontier_is_refused() {
    let mut p = parts(1, vec![create(1, "k", b"v")]);
    p.reads = vec![witness(1, "k", None, 1)];
    assert_eq!(
        AtomicPlan::close(p).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
    );
}

#[test]
fn write_only_plan_may_omit_frontier() {
    AtomicPlan::close(parts(1, vec![create(1, "k", b"v")])).unwrap();
}

#[test]
fn unknown_profile_does_not_alias_local_heap_bytes_or_root() {
    let known = AtomicPlan::close(parts(1, vec![create(1, "k", b"v")])).unwrap();
    let mut unknown_parts = parts(1, vec![create(1, "k", b"v")]);
    unknown_parts.profile = AtomicProfile::from_wire_code(99);
    let unknown = AtomicPlan::close(unknown_parts).unwrap();
    assert!(!unknown.profile().execution_supported());
    assert_ne!(
        encode_canonical_plan(&known).unwrap(),
        encode_canonical_plan(&unknown).unwrap()
    );
    assert_ne!(
        plan_content_root(&known).unwrap(),
        plan_content_root(&unknown).unwrap()
    );
    let again = decode_canonical_plan(&encode_canonical_plan(&unknown).unwrap()).unwrap();
    assert!(!again.profile().execution_supported());
    assert_eq!(again.profile().wire_code(), 99);
}
