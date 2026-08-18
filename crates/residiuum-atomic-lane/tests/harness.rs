//! Shared fixtures for durable-lane integration tests.

#![allow(dead_code)]

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId,
    CoordinationScope, HeapId, MutationKind, ObjectIdentity, PlanMutation, ResourceLimits,
    VersionId,
};

pub const FRONTIER: [u8; 32] = [0xA1; 32];

pub fn hid(n: u8) -> HeapId {
    let mut b = [0u8; 16];
    b[0] = n;
    HeapId::from_bytes(b).unwrap()
}

pub fn cid(n: u8) -> CollectionId {
    let mut b = [0u8; 16];
    b[0] = n;
    CollectionId::from_bytes(b).unwrap()
}

pub fn aid(n: u8) -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = n;
    AtomicId::from_bytes(b).unwrap()
}

pub fn vid(n: u8) -> VersionId {
    let mut b = [0u8; 16];
    b[0] = n;
    VersionId::from_bytes(b).unwrap()
}

pub fn key(s: &str) -> CanonicalKey {
    CanonicalKey::String(s.to_owned())
}

pub fn create_member(id: AtomicId, ordinal: u32, k: &str, payload: &[u8]) -> AtomicMember {
    AtomicMember {
        atomic_id: id,
        ordinal,
        object_identity: ObjectIdentity::new(cid(1), key(k)),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(payload).as_bytes()),
        event_id: vid(ordinal as u8 + 1),
    }
}

pub fn plan_for(heap: HeapId, members: &[AtomicMember], values: &[&[u8]]) -> AtomicPlan {
    assert_eq!(members.len(), values.len());
    let atomic_id = members
        .first()
        .map(|m| m.atomic_id)
        .expect("plan_for requires members; use plan_empty for assertion-only");
    let mutations = members
        .iter()
        .zip(values)
        .map(|(member, value)| PlanMutation {
            kind: member.member_kind,
            collection_id: member.object_identity.collection_id,
            key: member.object_identity.key.clone(),
            encoded_value: match member.member_kind {
                MutationKind::Delete => None,
                _ => Some(value.to_vec()),
            },
            if_version: member.before_version,
        })
        .collect();
    close(
        heap,
        atomic_id,
        mutations,
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

pub fn plan_empty(heap: HeapId, atomic_id: AtomicId) -> AtomicPlan {
    close(
        heap,
        atomic_id,
        Vec::new(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

pub fn close(
    heap: HeapId,
    atomic_id: AtomicId,
    mutations: Vec<PlanMutation>,
    read_frontier: Option<[u8; 32]>,
    reads: Vec<residiuum_atomics::ReadWitness>,
    predicates: Vec<residiuum_atomics::PlanPredicate>,
    rules: Vec<[u8; 32]>,
) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier,
        reads,
        predicates,
        mutations,
        active_rule_revisions: rules,
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}
