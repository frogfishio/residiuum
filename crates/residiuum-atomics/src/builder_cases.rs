//! ATM-1.3: typed builder, rights, authority binding, cross-Heap negatives.

use crate::{
    admit_closed_plan, decode_collection_lifecycle_payload, plan_content_root,
    serialize_canonical_value, validate_closed_plan, AtomicBuilder, AtomicId, AtomicOptions,
    AtomicOutcome, AtomicRefuseReason, AtomicsError, BoundCollection, BoundedKeyRange,
    CanonicalKey, CanonicalKeyKind, CanonicalValue, CollectionId, CollectionLifecycleState,
    CollectionRights, ConstructionRead, CoordinationScope, EncodingProfile, HeapId, PredicateKind,
    RangeEntry, ResourceLimits, SerialOracle, TrustedAuthorityView, ValueEncoding, VersionId,
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
    CanonicalValue::from_bytes(bytes)
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
fn lifecycle_binding_requires_read_alongside_mutation_right() {
    let create_only = coll(1, 1, CollectionRights::CREATE, 1);
    let mut b = builder(1, 48);
    assert_eq!(
        b.create(&create_only, key("k"), val(b"v")).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::AuthorizationFailure)
    );

    let create_and_read = coll(
        1,
        1,
        CollectionRights::CREATE.union(CollectionRights::READ),
        1,
    );
    b.create(&create_and_read, key("k"), val(b"v")).unwrap();
    assert!(b.required_rights().contains(CollectionRights::READ));
    assert!(b.required_rights().contains(CollectionRights::CREATE));
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
fn construction_reads_use_planned_values_without_external_witnesses() {
    let state = coll(1, 1, CollectionRights::ordinary(), 1);
    let mut b = builder(1, 30);
    assert_eq!(
        b.read_your_plan(&state, &key("external")).unwrap(),
        ConstructionRead::External
    );
    b.create(&state, key("created"), val(b"planned")).unwrap();
    assert_eq!(
        b.read_your_plan(&state, &key("created")).unwrap(),
        ConstructionRead::Present {
            encoded_value: b"planned".to_vec(),
            mutation_ordinal: 0,
        }
    );
    let plan = b.build().unwrap();
    assert!(plan.reads().is_empty());
    assert_eq!(plan.mutations().len(), 1);
}

#[test]
fn construction_read_after_planned_delete_is_absent() {
    let state = coll(1, 1, CollectionRights::ordinary(), 1);
    let mut b = builder(1, 31);
    b.delete(&state, key("gone"), vid(1)).unwrap();
    assert_eq!(
        b.read_your_plan(&state, &key("gone")).unwrap(),
        ConstructionRead::Absent {
            mutation_ordinal: 0,
        }
    );
    assert!(b.build().unwrap().reads().is_empty());
}

#[test]
fn construction_read_enforces_heap_rights_and_authority_binding() {
    let local = coll(1, 1, CollectionRights::READ, 1);
    let no_read = coll(1, 2, CollectionRights::CREATE, 1);
    let foreign = coll(2, 1, CollectionRights::READ, 1);
    let mut b = builder(1, 32);
    assert_eq!(
        b.read_your_plan(&no_read, &key("k")).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::AuthorizationFailure)
    );
    assert_eq!(
        b.read_your_plan(&foreign, &key("k")).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::CrossHeapCollection)
    );
    assert_eq!(
        b.read_your_plan(&local, &key("k")).unwrap(),
        ConstructionRead::External
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
    let rule = plan
        .predicates()
        .iter()
        .find(|p| p.kind == PredicateKind::ActiveRuleRevisionEquality)
        .unwrap();
    assert!(rule.collection_id.is_none());
    assert!(rule.key.is_none());
    assert!(rule.version.is_none());
    assert_eq!(rule.encoded.as_deref(), Some([3u8; 32].as_slice()));
}

#[test]
fn lifecycle_binding_is_collection_scoped_authorized_and_canonical() {
    let state = coll(1, 9, CollectionRights::READ, 7);
    let mut b = builder(1, 44);
    b.bind_collection_lifecycle(&state, CollectionLifecycleState::Active)
        .unwrap();
    let plan = b.build().unwrap();
    let lifecycle = plan
        .predicates()
        .iter()
        .find(|predicate| predicate.kind == PredicateKind::CollectionLifecycleState)
        .unwrap();
    assert_eq!(lifecycle.collection_id, Some(cid(9)));
    assert!(lifecycle.key.is_none());
    assert_eq!(
        decode_collection_lifecycle_payload(lifecycle.encoded.as_deref().unwrap()).unwrap(),
        CollectionLifecycleState::Active
    );
}

#[test]
fn typed_collection_use_automatically_binds_active_lifecycle_once() {
    let state = coll(1, 9, CollectionRights::ordinary(), 7);
    let mut b = builder(1, 45);
    b.create(&state, key("a"), val(b"1"))
        .unwrap()
        .assert_absent(&state, key("b"))
        .unwrap()
        .bind_collection_lifecycle(&state, CollectionLifecycleState::Active)
        .unwrap();
    let plan = b.build().unwrap();
    let lifecycle: Vec<_> = plan
        .predicates()
        .iter()
        .filter(|predicate| predicate.kind == PredicateKind::CollectionLifecycleState)
        .collect();
    assert_eq!(lifecycle.len(), 1);
    assert_eq!(lifecycle[0].collection_id, Some(cid(9)));
    assert_eq!(
        decode_collection_lifecycle_payload(lifecycle[0].encoded.as_deref().unwrap()).unwrap(),
        CollectionLifecycleState::Active
    );
}

#[test]
fn typed_active_handle_cannot_claim_absent_or_retired_lifecycle() {
    let state = coll(1, 9, CollectionRights::READ, 7);
    for expected in [
        CollectionLifecycleState::Absent,
        CollectionLifecycleState::Retired,
    ] {
        let mut b = builder(1, 46);
        assert_eq!(
            b.bind_collection_lifecycle(&state, expected).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
        );
    }
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

fn coll_encoding(
    heap: u8,
    collection: u8,
    rights: CollectionRights,
    authority: u8,
    encoding: EncodingProfile,
) -> BoundCollection {
    let mut trusted = view(heap, authority);
    trusted.grant_with_encoding(cid(collection), rights, encoding);
    BoundCollection::from_trusted(&trusted, cid(collection)).unwrap()
}

#[test]
fn equivalent_integer_input_shares_bytes_and_root() {
    let state = coll_encoding(
        1,
        1,
        CollectionRights::ordinary(),
        1,
        EncodingProfile::INTEGER,
    );
    let mut a = builder(1, 50);
    a.create(
        &state,
        CanonicalKey::integer(128),
        CanonicalValue::from_integer(128),
    )
    .unwrap();
    let mut b = builder(1, 50);
    b.create(
        &state,
        CanonicalKey::integer_bytes(&[0x00, 0x80]).unwrap(),
        serialize_canonical_value(ValueEncoding::Integer, &[0x00, 0x80]).unwrap(),
    )
    .unwrap();
    let pa = a.build().unwrap();
    let pb = b.build().unwrap();
    assert_eq!(pa.mutations()[0].key, CanonicalKey::integer(128));
    assert_eq!(
        pa.mutations()[0].encoded_value.as_deref(),
        Some(&[0x00, 0x80][..])
    );
    assert_eq!(
        plan_content_root(&pa).unwrap(),
        plan_content_root(&pb).unwrap()
    );
}

#[test]
fn equivalent_decimal_input_shares_bytes_and_root() {
    let state = coll_encoding(
        1,
        1,
        CollectionRights::ordinary(),
        1,
        EncodingProfile::DECIMAL,
    );
    let mut a = builder(1, 51);
    a.create(
        &state,
        CanonicalKey::decimal(10, 1),
        CanonicalValue::from_decimal(10, 1).unwrap(),
    )
    .unwrap();
    let mut b = builder(1, 51);
    b.create(
        &state,
        CanonicalKey::decimal_bytes(&[0x0a], 1).unwrap(),
        CanonicalValue::from_decimal(10, 1).unwrap(),
    )
    .unwrap();
    let pa = a.build().unwrap();
    let pb = b.build().unwrap();
    assert_eq!(
        plan_content_root(&pa).unwrap(),
        plan_content_root(&pb).unwrap()
    );
}

#[test]
fn noncanonical_integer_key_refuses_before_prepare() {
    let state = coll_encoding(
        1,
        1,
        CollectionRights::ordinary(),
        1,
        EncodingProfile::INTEGER,
    );
    assert_eq!(
        CanonicalKey::integer_bytes(&[0x00, 0x01]).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
    );
    let mut b = builder(1, 52);
    assert_eq!(
        b.create(
            &state,
            CanonicalKey::Integer(vec![0x00, 0x01]),
            CanonicalValue::from_integer(1),
        )
        .unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
    );
}

#[test]
fn noncanonical_decimal_coefficient_refuses_before_prepare() {
    let state = coll_encoding(
        1,
        1,
        CollectionRights::ordinary(),
        1,
        EncodingProfile::DECIMAL,
    );
    let mut b = builder(1, 53);
    assert_eq!(
        b.create(
            &state,
            CanonicalKey::Decimal {
                coefficient: vec![0x00, 0x01],
                scale: 0,
            },
            CanonicalValue::from_decimal(1, 0).unwrap(),
        )
        .unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
    );
}

#[test]
fn wrong_schema_key_refuses_before_prepare() {
    let strings = coll(1, 1, CollectionRights::ordinary(), 1);
    let mut b = builder(1, 54);
    assert_eq!(
        b.create(&strings, CanonicalKey::integer(1), val(b"v"))
            .unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
    );
}

#[test]
fn wrong_schema_value_refuses_before_prepare() {
    let ints = coll_encoding(
        1,
        1,
        CollectionRights::ordinary(),
        1,
        EncodingProfile::INTEGER,
    );
    let mut b = builder(1, 55);
    assert_eq!(
        b.create(
            &ints,
            CanonicalKey::integer(1),
            CanonicalValue::from_bytes(&[0x00, 0x01]),
        )
        .unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
    );
    assert_eq!(
        serialize_canonical_value(ValueEncoding::Integer, &[0x00, 0x01]).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
    );

    let decimals = coll_encoding(
        1,
        2,
        CollectionRights::ordinary(),
        1,
        EncodingProfile::DECIMAL,
    );
    let mut d = builder(1, 56);
    assert_eq!(
        d.create(
            &decimals,
            CanonicalKey::decimal(1, 0),
            CanonicalValue::from_bytes(b"hello"),
        )
        .unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
    );
}

#[test]
fn handle_carries_frozen_encoding_profile() {
    let handle = coll_encoding(
        1,
        1,
        CollectionRights::ordinary(),
        1,
        EncodingProfile::INTEGER,
    );
    assert_eq!(handle.encoding(), EncodingProfile::INTEGER);
    assert_eq!(handle.encoding().key_kind(), CanonicalKeyKind::Integer);
    assert_eq!(handle.encoding().value_encoding(), ValueEncoding::Integer);
}

#[test]
fn compiled_exact_scalar_is_typed_authorized_and_canonical() {
    let ints = coll_encoding(
        1,
        1,
        CollectionRights::ordinary(),
        1,
        EncodingProfile::INTEGER,
    );
    let mut b = builder(1, 57);
    b.compiled_exact_scalar_equality(
        &ints,
        CanonicalKey::integer(7),
        CanonicalValue::from_integer(42),
    )
    .unwrap();
    let plan = b.build().unwrap();
    let predicate = plan
        .predicates()
        .iter()
        .find(|predicate| predicate.kind == PredicateKind::ExactScalarEquality)
        .unwrap();
    let compiled =
        crate::decode_exact_scalar_payload(predicate.encoded.as_deref().unwrap()).unwrap();
    assert_eq!(compiled.encoding(), ValueEncoding::Integer);
    assert_eq!(compiled.expected(), &[42]);

    let readless = coll_encoding(1, 2, CollectionRights::CREATE, 1, EncodingProfile::INTEGER);
    let mut denied = builder(1, 58);
    assert_eq!(
        denied
            .compiled_exact_scalar_equality(
                &readless,
                CanonicalKey::integer(7),
                CanonicalValue::from_integer(42),
            )
            .unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::AuthorizationFailure)
    );

    let bytes = coll_encoding(
        1,
        3,
        CollectionRights::ordinary(),
        1,
        EncodingProfile::STRING_BYTES,
    );
    let mut forged = builder(1, 59);
    forged
        .compiled_exact_scalar_equality(
            &bytes,
            CanonicalKey::string("k"),
            CanonicalValue::from_bytes(b"bytes"),
        )
        .unwrap();
    let forged = forged.build().unwrap();
    let mut integer_authority = view(1, 1);
    integer_authority.grant_with_encoding(
        cid(3),
        CollectionRights::ordinary(),
        EncodingProfile::new(CanonicalKeyKind::String, ValueEncoding::Integer),
    );
    assert_eq!(
        admit_closed_plan(&forged, &integer_authority).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
    );
}

#[test]
fn compiled_ranges_are_authorized_typed_and_have_exact_identities() {
    let strings = coll(1, 1, CollectionRights::ordinary(), 1);
    let empty_left = BoundedKeyRange::observed(
        cid(1),
        CanonicalKey::string("a"),
        true,
        CanonicalKey::string("m"),
        true,
        100,
        &[],
    )
    .unwrap();
    let empty_right = BoundedKeyRange::observed(
        cid(1),
        CanonicalKey::string("n"),
        true,
        CanonicalKey::string("z"),
        true,
        100,
        &[],
    )
    .unwrap();
    let mut b = builder(1, 60);
    b.compiled_bounded_key_range_absence(&strings, &empty_right)
        .unwrap()
        .compiled_bounded_key_range_absence(&strings, &empty_left)
        .unwrap();
    let plan = b.build().unwrap();
    assert_eq!(
        plan.predicates()
            .iter()
            .filter(|predicate| predicate.kind == PredicateKind::BoundedKeyRangeAbsence)
            .count(),
        2
    );
    let encoded = crate::encode_canonical_plan(&plan).unwrap();
    assert_eq!(crate::decode_canonical_plan(&encoded).unwrap(), plan);

    let observed = BoundedKeyRange::observed(
        cid(1),
        CanonicalKey::string("a"),
        true,
        CanonicalKey::string("z"),
        true,
        100,
        &[RangeEntry {
            key: CanonicalKey::string("k"),
            version: vid(7),
        }],
    )
    .unwrap();
    let mut present = builder(1, 61);
    present
        .compiled_bounded_key_range_presence(&strings, &observed)
        .unwrap();
    assert!(present.build().is_ok());

    let readless = coll(1, 2, CollectionRights::CREATE, 1);
    let foreign_collection = BoundedKeyRange::observed(
        cid(2),
        CanonicalKey::string("a"),
        true,
        CanonicalKey::string("z"),
        true,
        10,
        &[],
    )
    .unwrap();
    let mut denied = builder(1, 62);
    assert_eq!(
        denied
            .compiled_bounded_key_range_absence(&readless, &foreign_collection)
            .unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::AuthorizationFailure)
    );

    let integers = BoundedKeyRange::observed(
        cid(1),
        CanonicalKey::integer(-1),
        true,
        CanonicalKey::integer(1),
        true,
        10,
        &[],
    )
    .unwrap();
    let mut wrong_kind = builder(1, 63);
    assert_eq!(
        wrong_kind
            .compiled_bounded_key_range_absence(&strings, &integers)
            .unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
    );

    let mut duplicate = builder(1, 64);
    duplicate
        .compiled_bounded_key_range_absence(&strings, &empty_left)
        .unwrap()
        .compiled_bounded_key_range_absence(&strings, &empty_left)
        .unwrap();
    assert_eq!(
        duplicate.build().unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
    );
}
