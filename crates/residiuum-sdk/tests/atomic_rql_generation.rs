//! ATM-3D: real embedded SDK/RQL readers bind one guarded collection page.

use residiuum_atomics::{
    AtomicId, AtomicOutcome, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey,
    CollectionId as AtomicCollectionId, CoordinationScope, HeapId as AtomicHeapId, MutationKind,
    PlanMutation, ResourceLimits, VersionId,
};
use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{encode_json, HeapClient, Parameters, QueryRunOptions, ResidiuumDeployment};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use std::sync::{Arc, Barrier};

fn mint_cap_for(heap: HeapId, deployment: DeploymentId) -> residiuum_heap::HeapCap {
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

#[test]
fn concurrent_rql_never_mixes_before_and_after_members() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let deployment_id = DeploymentId::new_random().unwrap();
    let heap_id = HeapId::new_random().unwrap();
    let collection_seed = residiuum_heap::CollectionId::new_random().unwrap();
    let staged = stage_heap_genesis(
        &layout,
        *deployment_id.as_bytes(),
        *heap_id.as_bytes(),
        *collection_seed.as_bytes(),
        "atomic-rql",
    )
    .unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, deployment_id)));
    let mut collection = client.create_collection("docs").unwrap().collection;
    let collection_id = collection.id();
    let left = collection
        .put("left", &serde_json::json!({"n": 0}))
        .unwrap();
    let right = collection
        .put("right", &serde_json::json!({"n": 0}))
        .unwrap();

    let mut readers = (0..4)
        .map(|_| client.open_collection("docs").unwrap())
        .collect::<Vec<_>>();
    let start = Arc::new(Barrier::new(readers.len() + 1));
    let sampled_before = Arc::new(Barrier::new(readers.len() + 1));
    let committed = Arc::new(Barrier::new(readers.len() + 1));
    let mut joins = Vec::new();
    for mut reader in readers.drain(..) {
        let start = Arc::clone(&start);
        let sampled_before = Arc::clone(&sampled_before);
        let committed = Arc::clone(&committed);
        joins.push(std::thread::spawn(move || {
            start.wait();
            let mut saw_old = false;
            let mut saw_new = false;
            for sample in 0..10_000 {
                let page = reader
                    .rql(
                        "from docs page size 10",
                        &Parameters::default(),
                        QueryRunOptions::default(),
                    )
                    .unwrap();
                assert!(page.known_holes.is_empty());
                assert_eq!(page.rows.len(), 2);
                let values = page
                    .rows
                    .iter()
                    .map(|row| row.value["n"].as_u64().unwrap())
                    .collect::<Vec<_>>();
                match values.as_slice() {
                    [0, 0] => saw_old = true,
                    [1, 1] => saw_new = true,
                    mixed => panic!("RQL observed a partial Atomic generation: {mixed:?}"),
                }
                if sample == 0 {
                    sampled_before.wait();
                    committed.wait();
                }
                if saw_old && saw_new {
                    break;
                }
                std::thread::yield_now();
            }
            (saw_old, saw_new)
        }));
    }

    start.wait();
    sampled_before.wait();
    let atomic_heap = AtomicHeapId::from_bytes(*heap_id.as_bytes()).unwrap();
    let atomic_collection = AtomicCollectionId::from_bytes(*collection_id.as_bytes()).unwrap();
    let mut atomic_id = [0; 32];
    atomic_id[0] = 1;
    let plan = AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: AtomicId::from_bytes(atomic_id).unwrap(),
        heap_id: atomic_heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: vec![
            PlanMutation {
                kind: MutationKind::Replace,
                collection_id: atomic_collection,
                key: CanonicalKey::string("left"),
                encoded_value: Some(encode_json(&serde_json::json!({"n": 1})).unwrap()),
                if_version: Some(VersionId::from_bytes(left.version).unwrap()),
            },
            PlanMutation {
                kind: MutationKind::Replace,
                collection_id: atomic_collection,
                key: CanonicalKey::string("right"),
                encoded_value: Some(encode_json(&serde_json::json!({"n": 1})).unwrap()),
                if_version: Some(VersionId::from_bytes(right.version).unwrap()),
            },
        ],
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap();
    let outcome = client.decide_atomic_plan_outcome(&plan).unwrap();
    let AtomicOutcome::Committed(receipt) = outcome else {
        panic!("valid SDK Atomic did not commit")
    };
    assert!(!receipt.replayed);
    assert_eq!(receipt.members.len(), 2);
    assert_eq!(
        receipt.members[0].before_version.unwrap().as_bytes(),
        &left.version
    );
    assert_eq!(
        receipt.members[0].after_version,
        Some(receipt.members[0].event_id)
    );
    assert_eq!(
        receipt.members[1].before_version.unwrap().as_bytes(),
        &right.version
    );
    assert_eq!(
        receipt.members[1].after_version,
        Some(receipt.members[1].event_id)
    );
    committed.wait();

    for join in joins {
        assert_eq!(join.join().unwrap(), (true, true));
    }
}
