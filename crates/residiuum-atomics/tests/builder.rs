//! ATM-1.3: typed builder, rights, authority binding, cross-Heap negatives.

use residiuum_atomics::{
    admit_closed_plan, plan_content_root, serialize_canonical_value, validate_closed_plan,
    AtomicBuilder, AtomicId, AtomicOptions, AtomicOutcome, AtomicRefuseReason, AtomicsError,
    BoundCollection, CanonicalKey, CanonicalValue, CollectionId, CollectionRights,
    CoordinationScope, HeapId, PredicateKind, ResourceLimits, SerialOracle, TrustedAuthorityView,
    VersionId,
};
use std::time::{Duration, Instant};

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

fn vid(n: u8) -> VersionId {
    let mut b = [0u8; 16];
    b[0] = n;
    VersionId::from_bytes(b).unwrap()
}

fn key(s: &str) -> CanonicalKey {
    CanonicalKey::String(s.to_owned())
}

fn val(bytes: &[u8]) -> CanonicalValue {
    serialize_canonical_value(bytes)
}

fn rev(n: u8) -> [u8; 32] {
    let mut b = [0u8; 32];
    b[0] = n;
    b
}

fn view(heap: u8, authority: u8) -> TrustedAuthorityView {
    TrustedAuthorityView::new(hid(heap), rev(authority))
}

fn coll(heap: u8, collection: u8, rights: CollectionRights, authority: u8) -> BoundCollection {
    let mut trusted = view(heap, authority);
    trusted.grant(cid(collection), rights);
    BoundCollection::from_trusted(&trusted, cid(collection)).unwrap()
}

fn builder(heap: u8, id: u8) -> AtomicBuilder {
    AtomicBuilder::new(hid(heap), AtomicOptions::new(aid(id))).unwrap()
}

#[test]
fn build_commits_on_the_bound_heap_oracle() {
    let heap = hid(1);
    let state = coll(1, 1, CollectionRights::ordinary(), 7);
    let mut b = builder(1, 1);
    b.create(&state, key("a"), val(b"1"))
        .unwrap()
        .put_unconditional(&state, key("b"), val(b"2"))
        .unwrap()
        .assert_absent(&state, key("ghost"))
        .unwrap();
    assert!(b.required_rights().contains(CollectionRights::CREATE));
    assert!(b.required_rights().contains(CollectionRights::PUT));
    assert!(b.required_rights().contains(CollectionRights::READ));
    let plan = b.build().unwrap();
    assert_eq!(plan.heap_id(), heap);
    assert!(plan.active_rule_revisions().is_empty());
    let auth = plan
        .predicates()
        .iter()
        .find(|p| p.kind == PredicateKind::HeapAuthorityRevision)
        .expect("authority predicate");
    assert_eq!(auth.encoded.as_deref(), Some(rev(7).as_slice()));
    validate_closed_plan(&plan, heap).unwrap();
    let mut oracle = SerialOracle::new(heap);
    assert!(matches!(
        oracle.apply(&plan).unwrap(),
        AtomicOutcome::Committed(_)
    ));
}

#[test]
fn cross_heap_collection_is_refused_and_produces_no_plan() {
    let local = coll(1, 1, CollectionRights::ordinary(), 1);
    let foreign = coll(2, 1, CollectionRights::ordinary(), 1);
    let mut b = builder(1, 2);
    b.create(&local, key("ok"), val(b"v")).unwrap();
    assert_eq!(
        b.create(&foreign, key("no"), val(b"v")).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::CrossHeapCollection)
    );
    let plan = b.build().unwrap();
    assert_eq!(plan.mutations().len(), 1);
    let mut foreign_oracle = SerialOracle::new(hid(2));
    assert_eq!(
        foreign_oracle.apply(&plan).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::CrossHeapCollection)
    );
    assert!(foreign_oracle.get(cid(1), &key("ok")).is_none());
    assert!(foreign_oracle.get(cid(1), &key("no")).is_none());
}

#[test]
fn builder_on_foreign_heap_cannot_use_local_collection() {
    let local = coll(1, 1, CollectionRights::ordinary(), 1);
    let mut b = builder(2, 3);
    assert_eq!(
        b.create(&local, key("k"), val(b"v")).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::CrossHeapCollection)
    );
}

#[test]
fn missing_right_is_authorization_failure() {
    let read_only = coll(1, 1, CollectionRights::READ, 1);
    let mut b = builder(1, 4);
    assert_eq!(
        b.create(&read_only, key("k"), val(b"v")).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::AuthorizationFailure)
    );
    b.assert_absent(&read_only, key("k")).unwrap();
    b.build().unwrap();
}

#[test]
fn mismatched_authority_revision_is_stale_or_foreign() {
    let a = coll(1, 1, CollectionRights::ordinary(), 1);
    let stale = coll(1, 2, CollectionRights::ordinary(), 2);
    let mut b = builder(1, 5);
    b.create(&a, key("a"), val(b"1")).unwrap();
    assert_eq!(
        b.create(&stale, key("b"), val(b"2")).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::StaleOrForeignCapability)
    );
}

#[test]
fn expired_deadline_is_refused() {
    let expired = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .expect("instant can step back");
    let err =
        AtomicBuilder::new(hid(1), AtomicOptions::new(aid(6)).with_deadline(expired)).unwrap_err();
    assert_eq!(err, AtomicsError::Refused(AtomicRefuseReason::Deadline));
}

#[test]
fn raised_limits_are_refused() {
    let mut raised = ResourceLimits::hard_local_heap();
    raised.caller_mutations = 257;
    let err =
        AtomicBuilder::new(hid(1), AtomicOptions::new(aid(7)).with_limits(raised)).unwrap_err();
    assert_eq!(
        err,
        AtomicsError::Refused(AtomicRefuseReason::LimitExceeded)
    );
}

#[test]
fn partition_scope_is_unavailable() {
    let err = AtomicBuilder::new(
        hid(1),
        AtomicOptions::new(aid(8)).with_scope(CoordinationScope::Partition),
    )
    .unwrap_err();
    assert_eq!(
        err,
        AtomicsError::Refused(AtomicRefuseReason::ScopeUnavailable)
    );
}

#[test]
fn read_witnesses_bind_frontier_and_versions() {
    let state = coll(1, 1, CollectionRights::ordinary(), 3);
    let mut b = AtomicBuilder::new(
        hid(1),
        AtomicOptions::new(aid(9)).with_read_frontier([9u8; 32]),
    )
    .unwrap();
    b.witness_version(&state, key("seen"), vid(4), [4u8; 32])
        .unwrap()
        .witness_absent(&state, key("gone"), [5u8; 32])
        .unwrap()
        .create(&state, key("new"), val(b"n"))
        .unwrap();
    let plan = b.build().unwrap();
    assert_eq!(plan.read_frontier(), Some([9u8; 32]));
    assert_eq!(plan.reads().len(), 2);
    assert_eq!(plan.reads()[0].observed_version, None);
    assert_eq!(plan.reads()[1].observed_version, Some(vid(4)));
}

#[test]
fn witness_without_frontier_is_malformed() {
    let state = coll(1, 1, CollectionRights::ordinary(), 1);
    let mut b = builder(1, 10);
    b.witness_absent(&state, key("gone"), [1u8; 32]).unwrap();
    assert_eq!(
        b.build().unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
    );
}

#[test]
fn duplicate_mutation_target_is_refused() {
    let state = coll(1, 1, CollectionRights::ordinary(), 1);
    let mut b = builder(1, 11);
    b.create(&state, key("k"), val(b"1")).unwrap();
    assert_eq!(
        b.create(&state, key("k"), val(b"2")).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget)
    );
}

#[test]
fn replace_and_delete_round_trip_through_oracle() {
    let heap = hid(1);
    let state = coll(1, 1, CollectionRights::ordinary(), 1);
    let mut seed = builder(1, 12);
    seed.create(&state, key("keep"), val(b"1"))
        .unwrap()
        .create(&state, key("gone"), val(b"2"))
        .unwrap();
    let mut oracle = SerialOracle::new(heap);
    oracle.apply(&seed.build().unwrap()).unwrap();
    let keep = oracle.get(cid(1), &key("keep")).unwrap().version;
    let gone = oracle.get(cid(1), &key("gone")).unwrap().version;
    let mut next = builder(1, 13);
    next.replace(&state, key("keep"), keep, val(b"1b"))
        .unwrap()
        .delete(&state, key("gone"), gone)
        .unwrap();
    oracle.apply(&next.build().unwrap()).unwrap();
    assert_eq!(oracle.get(cid(1), &key("keep")).unwrap().value, b"1b");
    assert!(oracle.get(cid(1), &key("gone")).is_none());
}

#[test]
fn rule_revisions_stay_off_the_authority_predicate() {
    let state = coll(1, 1, CollectionRights::ordinary(), 7);
    let mut b = builder(1, 40);
    b.create(&state, key("k"), val(b"v"))
        .unwrap()
        .bind_rule_revision([3u8; 32]);
    let plan = b.build().unwrap();
    assert_eq!(plan.active_rule_revisions(), &[[3u8; 32]]);
    let auth = plan
        .predicates()
        .iter()
        .find(|p| p.kind == PredicateKind::HeapAuthorityRevision)
        .unwrap();
    assert_eq!(auth.encoded.as_deref(), Some(rev(7).as_slice()));
}

#[test]
fn authority_change_moves_root_not_rule_revisions() {
    let first = coll(1, 1, CollectionRights::ordinary(), 1);
    let second = coll(1, 1, CollectionRights::ordinary(), 2);
    let mut a = builder(1, 41);
    a.create(&first, key("k"), val(b"v"))
        .unwrap()
        .bind_rule_revision([9u8; 32]);
    let mut b = builder(1, 41);
    b.create(&second, key("k"), val(b"v"))
        .unwrap()
        .bind_rule_revision([9u8; 32]);
    let pa = a.build().unwrap();
    let pb = b.build().unwrap();
    assert_eq!(pa.active_rule_revisions(), pb.active_rule_revisions());
    assert_ne!(
        plan_content_root(&pa).unwrap(),
        plan_content_root(&pb).unwrap()
    );
}

#[test]
fn admit_refuses_stale_authority_without_prepare() {
    let mut trusted = view(1, 1);
    trusted.grant(cid(1), CollectionRights::ordinary());
    let handle = BoundCollection::from_trusted(&trusted, cid(1)).unwrap();
    let mut b = builder(1, 42);
    b.create(&handle, key("k"), val(b"v")).unwrap();
    let plan = b.build().unwrap();
    admit_closed_plan(&plan, &trusted).unwrap();
    let mut later = view(1, 2);
    later.grant(cid(1), CollectionRights::ordinary());
    assert_eq!(
        admit_closed_plan(&plan, &later).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::StaleOrForeignCapability)
    );
}

#[test]
fn admit_refuses_revoked_rights() {
    let mut trusted = view(1, 1);
    trusted.grant(cid(1), CollectionRights::ordinary());
    let handle = BoundCollection::from_trusted(&trusted, cid(1)).unwrap();
    let mut b = builder(1, 43);
    b.create(&handle, key("k"), val(b"v")).unwrap();
    let plan = b.build().unwrap();
    let mut revoked = view(1, 1);
    revoked.grant(cid(1), CollectionRights::READ);
    assert_eq!(
        admit_closed_plan(&plan, &revoked).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::AuthorizationFailure)
    );
}

#[test]
fn ungranted_collection_cannot_be_bound() {
    let trusted = view(1, 1);
    assert_eq!(
        BoundCollection::from_trusted(&trusted, cid(1)).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::AuthorizationFailure)
    );
}
