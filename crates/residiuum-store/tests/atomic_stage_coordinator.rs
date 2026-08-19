//! CR-ATMR5-004: coordinator sequence is durable, not Atomic-ID sort order.

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId,
    CoordinationScope, HeapId, MutationKind, ObjectIdentity, PlanMutation, ResourceLimits,
    VersionId,
};
use residiuum_store::{atomic_stage_checkpoint_path, Store};

const FRONTIER: [u8; 32] = [0xA1; 32];

fn aid(tag: u8) -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = tag;
    AtomicId::from_bytes(b).unwrap()
}

fn cid() -> CollectionId {
    let mut b = [0u8; 16];
    b[0] = 2;
    CollectionId::from_bytes(b).unwrap()
}

fn vid(tag: u8) -> VersionId {
    let mut b = [0u8; 16];
    b[0] = tag;
    VersionId::from_bytes(b).unwrap()
}

fn member(tag: u8) -> AtomicMember {
    AtomicMember {
        atomic_id: aid(tag),
        ordinal: 0,
        object_identity: ObjectIdentity::new(cid(), CanonicalKey::String(format!("k{tag}"))),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(&[tag]).as_bytes()),
        event_id: vid(tag),
    }
}

fn plan(heap: HeapId, m: &AtomicMember) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: m.atomic_id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: vec![PlanMutation {
            kind: m.member_kind,
            collection_id: m.object_identity.collection_id,
            key: m.object_identity.key.clone(),
            encoded_value: Some(b"secret".to_vec()),
            if_version: m.before_version,
        }],
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

#[test]
fn sequences_survive_reopen_opposite_atomic_id_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let (seq_hi, seq_lo) = {
        let mut store = Store::create(&path).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let mut stage = store.atomic_stage().unwrap();
        let hi = member(2);
        let lo = member(1);
        let (s_hi, _) = stage
            .begin_prepare(&plan(heap, &hi), FRONTIER, std::slice::from_ref(&hi))
            .unwrap();
        let (s_lo, _) = stage
            .begin_prepare(&plan(heap, &lo), FRONTIER, std::slice::from_ref(&lo))
            .unwrap();
        assert!(
            s_hi.as_u64() < s_lo.as_u64(),
            "first prepared must have lower seq"
        );
        assert_eq!(s_hi.as_u64(), 1);
        assert_eq!(s_lo.as_u64(), 2);
        (s_hi, s_lo)
    };
    let mut store = Store::open(&path).unwrap();
    let _ = std::fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    let stage = store.atomic_stage().unwrap();
    assert_eq!(stage.kernel().prepare_seq(aid(2)), Some(seq_hi));
    assert_eq!(stage.kernel().prepare_seq(aid(1)), Some(seq_lo));
    assert!(
        seq_hi.as_u64() < seq_lo.as_u64(),
        "reopen must not reconstruct order from Atomic-ID sort"
    );
}

#[test]
fn same_id_retry_returns_original_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member(9);
    let p = plan(heap, &m);
    let mut stage = store.atomic_stage().unwrap();
    let (first, _) = stage
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    let (again, _) = stage
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    assert_eq!(first, again);
}

#[test]
fn next_sequence_is_strictly_above_durable_high_water() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let mut store = Store::create(&path).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let mut stage = store.atomic_stage().unwrap();
        let a = member(2);
        stage
            .begin_prepare(&plan(heap, &a), FRONTIER, std::slice::from_ref(&a))
            .unwrap();
        let b = member(1);
        stage
            .begin_prepare(&plan(heap, &b), FRONTIER, std::slice::from_ref(&b))
            .unwrap();
    }
    let mut store = Store::open(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let mut stage = store.atomic_stage().unwrap();
    let c = member(3);
    let (seq, _) = stage
        .begin_prepare(&plan(heap, &c), FRONTIER, std::slice::from_ref(&c))
        .unwrap();
    assert_eq!(seq.as_u64(), 3);
}
