//! ATM-4D.6: composed authority/lifecycle/rule/data/Atomic histories.

#![cfg(feature = "legacy-raw-store")]

use residiuum_atomics::{
    encode_active_rule_revision_equality, encode_collection_lifecycle_state,
    encode_heap_authority_revision, AtomicAbortReason, AtomicId, AtomicOutcome, AtomicPlan,
    AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId, CollectionLifecycleState,
    CoordinationScope, HeapId as AtomicHeapId, LogicalStatus, MutationKind, PlanMutation,
    PlanPredicate, PredicateKind, ResourceLimits, VersionId,
};
use residiuum_format::{encode_subject_v2, SubjectObjectKind};
use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_store::{
    publish_staged_genesis, stage_heap_genesis, DurabilityMode, HeapMetaLayout, Store, StoreError,
    StoreHost,
};
use std::sync::Arc;

const CHANGE_DATA: u8 = 1;
const CHANGE_RULES: u8 = 2;
const CHANGE_RENAME: u8 = 4;
const CHANGE_RETIRE: u8 = 8;
const CHANGE_AUTHORITY: u8 = 16;

fn id16(first: u8) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[0] = first;
    bytes
}

fn atomic_id(first: u8) -> AtomicId {
    let mut bytes = [0u8; 32];
    bytes[0] = first;
    AtomicId::from_bytes(bytes).unwrap()
}

fn mint_capability_for(heap: HeapId, deployment: DeploymentId) -> residiuum_heap::HeapCap {
    let snapshot = HeapSecuritySnapshot {
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key: [7; 32],
        previous_master_public_key: None,
        security_revision: SecurityRevision::new(1).unwrap(),
        authority_chain_head_hash: [9; 32],
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    };
    let certificate = VerifiedCertificate {
        cose_bytes: vec![1],
        fingerprint: [3; 32],
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4; 32],
        rights: Rights::from_bits_certificate(0x0d).unwrap(),
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: [5; 32],
    };
    mint_capability(
        Arc::new(HeapSlot::new(snapshot)),
        &certificate,
        TrustedInstant {
            unix_s: 1_700_000_000,
        },
    )
    .unwrap()
}

fn composed_plan(
    heap: AtomicHeapId,
    collection: CollectionId,
    id: AtomicId,
    authority: [u8; 32],
    rule: [u8; 32],
    version: VersionId,
) -> AtomicPlan {
    let key = CanonicalKey::string("account".to_owned());
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![
            encode_heap_authority_revision(authority),
            encode_collection_lifecycle_state(collection, CollectionLifecycleState::Active)
                .unwrap(),
            encode_active_rule_revision_equality(rule),
            PlanPredicate {
                kind: PredicateKind::AssertVersion,
                collection_id: Some(collection),
                key: Some(key.clone()),
                version: Some(version),
                encoded: None,
            },
        ],
        mutations: vec![PlanMutation {
            kind: MutationKind::Replace,
            collection_id: collection,
            key,
            encoded_value: Some(b"atomic".to_vec()),
            if_version: Some(version),
        }],
        active_rule_revisions: vec![rule],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExplainedOutcome {
    Committed,
    IssuedConflict,
    RefusedBeforeIssuance,
}

/// Independent checker over only the transitions known to win before issue.
fn expected_outcome(changes: u8) -> ExplainedOutcome {
    if changes & CHANGE_AUTHORITY != 0 {
        ExplainedOutcome::RefusedBeforeIssuance
    } else if changes & (CHANGE_DATA | CHANGE_RULES | CHANGE_RETIRE) != 0 {
        ExplainedOutcome::IssuedConflict
    } else {
        ExplainedOutcome::Committed
    }
}

#[test]
fn independent_classifier_is_sensitive_to_each_semantic_transition() {
    assert_eq!(expected_outcome(0), ExplainedOutcome::Committed);
    for change in [CHANGE_DATA, CHANGE_RULES, CHANGE_RETIRE] {
        assert_eq!(expected_outcome(change), ExplainedOutcome::IssuedConflict);
        assert_ne!(expected_outcome(change), expected_outcome(change & !change));
    }
    assert_eq!(
        expected_outcome(CHANGE_AUTHORITY),
        ExplainedOutcome::RefusedBeforeIssuance
    );
    assert_ne!(
        expected_outcome(CHANGE_AUTHORITY),
        expected_outcome(CHANGE_AUTHORITY & !CHANGE_AUTHORITY)
    );
    assert_eq!(
        expected_outcome(CHANGE_RENAME),
        ExplainedOutcome::Committed,
        "rename is the non-conflicting lifecycle negative control"
    );
}

#[test]
fn deterministic_composed_histories_have_one_exact_explanation_after_restart() {
    // All 32 subsets are covered. A small deterministic shuffle varies the
    // order of independent data/rule/rename transitions; retirement and
    // authority remain last because both intentionally close later surfaces.
    for changes in 0u8..32 {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("store");
        let host = StoreHost::create(&root).unwrap();
        let layout = HeapMetaLayout::new(&root);
        let deployment = DeploymentId::from_bytes_unchecked_nonzero(id16(70)).unwrap();
        let heap = HeapId::from_bytes_unchecked_nonzero(id16(71)).unwrap();
        let collection = CollectionId::from_bytes(id16(72)).unwrap();
        let staged = stage_heap_genesis(
            &layout,
            *deployment.as_bytes(),
            *heap.as_bytes(),
            id16(73),
            "composed",
        )
        .unwrap();
        publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();

        let cap = mint_capability_for(heap, deployment);
        let slot = Arc::clone(cap.slot());
        let authority = slot.load().atomic_authority_revision();
        let atomic_heap = AtomicHeapId::from_bytes(*heap.as_bytes()).unwrap();
        let rule = [0x31; 32];
        {
            let physical = host.physical();
            let mut store = physical.lock().unwrap();
            store
                .create_collection_ordered(heap.as_bytes(), *collection.as_bytes(), id16(74), "c")
                .unwrap();
            store
                .set_active_rule_revisions(atomic_heap, &[rule], DurabilityMode::Durable)
                .unwrap();
        }
        let heap_store = host.open_heap(cap);
        let subject = encode_subject_v2(
            heap.as_bytes(),
            SubjectObjectKind::Collection,
            collection.as_bytes(),
            b"account",
        )
        .unwrap();
        let initial = heap_store.put(&subject, b"initial").unwrap();
        let plan = composed_plan(
            atomic_heap,
            collection,
            atomic_id(changes.saturating_add(1)),
            authority,
            rule,
            VersionId::from_bytes(initial.event_id).unwrap(),
        );

        let mut order = [CHANGE_DATA, CHANGE_RULES, CHANGE_RENAME];
        if changes.count_ones() % 2 == 1 {
            order.swap(0, 2);
        }
        for transition in order {
            if changes & transition == 0 {
                continue;
            }
            match transition {
                CHANGE_DATA => {
                    heap_store.put(&subject, b"ordinary-winner").unwrap();
                }
                CHANGE_RULES => {
                    host.physical()
                        .lock()
                        .unwrap()
                        .set_active_rule_revisions(
                            atomic_heap,
                            &[rule, [0x32; 32]],
                            DurabilityMode::Durable,
                        )
                        .unwrap();
                }
                CHANGE_RENAME => {
                    host.physical()
                        .lock()
                        .unwrap()
                        .rename_collection_ordered(heap.as_bytes(), collection.as_bytes(), "c2")
                        .unwrap();
                }
                _ => unreachable!(),
            }
        }
        if changes & CHANGE_RETIRE != 0 {
            host.physical()
                .lock()
                .unwrap()
                .retire_collection_ordered(heap.as_bytes(), collection.as_bytes())
                .unwrap();
        }
        if changes & CHANGE_AUTHORITY != 0 {
            let mut rotated = (*slot.load()).clone();
            rotated.security_revision = SecurityRevision::new(2).unwrap();
            rotated.authority_chain_head_hash = [0x44; 32];
            slot.store(rotated);
        }

        let expected = expected_outcome(changes);
        let observed = match heap_store.decide_atomic_plan_outcome(&plan) {
            Ok(AtomicOutcome::Committed(_)) => ExplainedOutcome::Committed,
            Ok(AtomicOutcome::NotCommitted {
                reason: AtomicAbortReason::PreconditionConflict,
                ..
            }) => ExplainedOutcome::IssuedConflict,
            Err(StoreError::HeapCapability(_)) => ExplainedOutcome::RefusedBeforeIssuance,
            other => panic!("campaign {changes}: unexplained outcome {other:?}"),
        };
        assert_eq!(observed, expected, "campaign {changes}");
        drop(heap_store);
        drop(host);

        let mut reopened = Store::open(&root).unwrap();
        let status = reopened
            .atomic_stage_for_heap(atomic_heap)
            .unwrap()
            .atomic_status(plan.atomic_id())
            .unwrap();
        match expected {
            ExplainedOutcome::Committed => assert_eq!(status.logical, LogicalStatus::Committed),
            ExplainedOutcome::IssuedConflict => {
                assert_eq!(status.logical, LogicalStatus::NotCommitted)
            }
            ExplainedOutcome::RefusedBeforeIssuance => {
                assert_eq!(status.logical, LogicalStatus::NotFound)
            }
        }
        let body = reopened.get_subject_bytes(&subject).unwrap().unwrap();
        match expected {
            ExplainedOutcome::Committed => assert_eq!(body, b"atomic"),
            _ if changes & CHANGE_DATA != 0 => assert_eq!(body, b"ordinary-winner"),
            _ => assert_eq!(body, b"initial"),
        }
    }
}
