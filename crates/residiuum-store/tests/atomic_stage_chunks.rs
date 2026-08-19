//! CR-ATMR5-005: frozen chunk maps and partial chunk bodies are durable.

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, ChunkPlan,
    CollectionId, CoordinationScope, HeapId, MemberPhase, MutationKind, ObjectIdentity,
    PlanMutation, ResourceLimits, VersionId,
};
use residiuum_store::{atomic_stage_checkpoint_path, Store, StoreAtomicStage, StoreError};
use residiuum_store::{StageEvidenceClass, StageEvidenceKind};

const FRONTIER: [u8; 32] = [0xA1; 32];
const PAYLOAD: &[u8] = b"secret";

fn aid() -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = 9;
    AtomicId::from_bytes(b).unwrap()
}

fn cid() -> CollectionId {
    let mut b = [0u8; 16];
    b[0] = 2;
    CollectionId::from_bytes(b).unwrap()
}

fn vid() -> VersionId {
    let mut b = [0u8; 16];
    b[0] = 3;
    VersionId::from_bytes(b).unwrap()
}

fn member() -> AtomicMember {
    AtomicMember {
        atomic_id: aid(),
        ordinal: 0,
        object_identity: ObjectIdentity::new(cid(), CanonicalKey::String("k".into())),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(PAYLOAD).as_bytes()),
        event_id: vid(),
    }
}

fn plan(heap: HeapId, members: &[AtomicMember]) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: members[0].atomic_id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: members
            .iter()
            .map(|m| PlanMutation {
                kind: m.member_kind,
                collection_id: m.object_identity.collection_id,
                key: m.object_identity.key.clone(),
                encoded_value: Some(PAYLOAD.to_vec()),
                if_version: m.before_version,
            })
            .collect(),
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

fn chunks() -> (ChunkPlan, &'static [u8], &'static [u8]) {
    let p0 = b"se";
    let p1 = b"cret";
    (
        ChunkPlan {
            total: 2,
            chunk_hashes: vec![*blake3::hash(p0).as_bytes(), *blake3::hash(p1).as_bytes()],
        },
        p0,
        p1,
    )
}

fn refused(err: StoreError) -> String {
    match err {
        StoreError::AtomicStage(msg) => msg,
        other => panic!("expected AtomicStage, got {other}"),
    }
}

fn assert_duplicate(err: StoreError) {
    let msg = refused(err);
    assert!(
        msg.contains("DuplicateTarget"),
        "expected DuplicateTarget, got {msg}"
    );
}

fn prepare_chunked(store: &mut Store) -> (AtomicMember, ChunkPlan, &'static [u8], &'static [u8]) {
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m));
    let (map, p0, p1) = chunks();
    let mut stage = store.atomic_stage().unwrap();
    stage
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    stage.commit_chunk_manifest(aid(), 0, map.clone()).unwrap();
    drop(stage);
    (m, map, p0, p1)
}

fn prefix_after_chunk0(stage: &StoreAtomicStage<'_>) {
    let plan = stage.kernel().chunk_plan(aid(), 0).expect("durable plan");
    assert_eq!(plan.total, 2);
    let staged = &stage.kernel().inspect_staged(aid()).expect("staged")[0];
    assert!(!staged.payload_complete);
    let chunks = staged.chunks.as_ref().expect("chunk slots");
    assert!(chunks[0].is_some());
    assert!(chunks[1].is_none());
}

#[test]
fn partial_chunk_survives_stage_reopen_and_continues() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let (m, map, p0, p1) = prepare_chunked(&mut store);
    {
        let mut stage = store.atomic_stage().unwrap();
        stage.commit_chunk_manifest(aid(), 0, map.clone()).unwrap();
        stage.append_chunk(m.clone(), 0, p0.to_vec()).unwrap();
        prefix_after_chunk0(&stage);
        assert!(stage.seal_member_boundary(aid()).is_err());
    }
    {
        let mut stage = store.atomic_stage().unwrap();
        prefix_after_chunk0(&stage);
        stage.append_chunk(m.clone(), 0, p0.to_vec()).unwrap();
        stage.append_chunk(m.clone(), 1, p1.to_vec()).unwrap();
        assert!(stage.kernel().inspect_staged(aid()).unwrap()[0].payload_complete);
        stage.seal_member_boundary(aid()).unwrap();
        assert_eq!(
            stage.kernel().lifecycle(aid()).unwrap().members,
            MemberPhase::DurableInvisible
        );
    }
}

#[test]
fn changed_manifest_or_body_after_reopen_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let (m, map, p0, _p1) = prepare_chunked(&mut store);
    {
        let mut stage = store.atomic_stage().unwrap();
        stage.append_chunk(m.clone(), 0, p0.to_vec()).unwrap();
    }
    let mut stage = store.atomic_stage().unwrap();
    let mut other = map;
    other.chunk_hashes[0] = [0x22; 32];
    assert_duplicate(stage.commit_chunk_manifest(aid(), 0, other).unwrap_err());
    assert_duplicate(stage.append_chunk(m, 0, b"xx".to_vec()).unwrap_err());
}

#[test]
fn store_reopen_keeps_partial_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let mut store = Store::create(&path).unwrap();
        let (m, _, p0, _) = prepare_chunked(&mut store);
        let mut stage = store.atomic_stage().unwrap();
        stage.append_chunk(m, 0, p0.to_vec()).unwrap();
    }
    let mut store = Store::open(&path).unwrap();
    let mut stage = store.atomic_stage().unwrap();
    prefix_after_chunk0(&stage);
    let (_, _, p1) = chunks();
    stage.append_chunk(member(), 1, p1.to_vec()).unwrap();
    assert_eq!(
        stage.kernel().inspect_staged(aid()).unwrap()[0].payload,
        PAYLOAD
    );
}

#[test]
fn rebuild_without_checkpoint_recovers_chunk_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let (m, _, p0, _) = prepare_chunked(&mut store);
    {
        let mut stage = store.atomic_stage().unwrap();
        stage.append_chunk(m, 0, p0.to_vec()).unwrap();
    }
    let ckpt = atomic_stage_checkpoint_path(store.paths());
    std::fs::remove_file(&ckpt).unwrap();
    drop(store);
    let mut store = Store::open(&path).unwrap();
    let stage = store.atomic_stage().unwrap();
    prefix_after_chunk0(&stage);
    assert!(stage.findings().records.iter().any(|f| {
        f.kind == StageEvidenceKind::ChunkPlan && f.class == StageEvidenceClass::Valid
    }));
    assert!(stage.findings().records.iter().any(|f| {
        f.kind == StageEvidenceKind::ChunkBody && f.class == StageEvidenceClass::Valid
    }));
}
