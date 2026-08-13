//! ATM-0.2 properties: builder order, semantic root, heap/collection substitution.

use residiuum_atomics::{
    decode_canonical_plan, encode_canonical_plan, plan_content_root, AtomicId, AtomicPlan,
    AtomicPlanParts, AtomicProfile, AtomicRefuseReason, AtomicsError, CanonicalKey, CollectionId,
    CoordinationScope, HeapId, MutationKind, PlanMutation, ResourceLimits,
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
        serde_json::from_str(include_str!("../../../spec/atomics/cbor-v1.json")).unwrap();
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
}
