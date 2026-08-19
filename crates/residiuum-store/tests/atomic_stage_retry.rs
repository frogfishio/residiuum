//! CR-ATMR5-003: exact same-ID store staging retries are idempotent.

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, ChunkPlan,
    CollectionId, CoordinationScope, HeapId, MemberPhase, MutationKind, ObjectIdentity,
    PlanMutation, ResourceLimits, VersionId,
};
use residiuum_store::{atomic_stage_checkpoint_path, Store, StoreError};
use std::fs;
use std::path::Path;

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

fn vid(tag: u8) -> VersionId {
    let mut b = [0u8; 16];
    b[0] = tag;
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
        event_id: vid(3),
    }
}

fn plan(heap: HeapId, members: &[AtomicMember], value: &[u8]) -> AtomicPlan {
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
                encoded_value: Some(value.to_vec()),
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

fn media_bytes(store: &Store) -> u64 {
    fn walk(dir: &Path) -> u64 {
        let Ok(entries) = fs::read_dir(dir) else {
            return 0;
        };
        entries
            .flatten()
            .map(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path)
                } else {
                    entry.metadata().map(|m| m.len()).unwrap_or(0)
                }
            })
            .sum()
    }
    let segs = walk(&store.paths().segments_dir());
    let ckpt = fs::metadata(atomic_stage_checkpoint_path(store.paths()))
        .map(|m| m.len())
        .unwrap_or(0);
    segs.saturating_add(ckpt)
}

fn snapshot(store: &mut Store) -> (Vec<u8>, MemberPhase) {
    let stage = store.atomic_stage().expect("stage");
    let staged = &stage.kernel().inspect_staged(aid()).expect("staged")[0];
    (
        staged.payload.clone(),
        stage.kernel().lifecycle(aid()).unwrap().members,
    )
}

#[test]
fn unchunked_exact_retry_is_idempotent_in_process_and_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), PAYLOAD);
    {
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage.append_staged(m.clone(), PAYLOAD.to_vec()).unwrap();
        stage.append_staged(m.clone(), PAYLOAD.to_vec()).unwrap();
        assert_eq!(
            stage.kernel().inspect_staged(aid()).unwrap()[0].payload,
            PAYLOAD
        );
    }
    let before = media_bytes(&store);
    {
        let mut stage = store.atomic_stage().unwrap();
        stage.append_staged(m.clone(), PAYLOAD.to_vec()).unwrap();
    }
    assert_eq!(media_bytes(&store), before, "exact retry must add no media");
    drop(store);

    let mut store = Store::open(&path).unwrap();
    {
        let mut stage = store.atomic_stage().unwrap();
        stage.append_staged(m, PAYLOAD.to_vec()).unwrap();
        assert_eq!(
            stage.kernel().inspect_staged(aid()).unwrap()[0].payload,
            PAYLOAD
        );
    }
    assert_eq!(snapshot(&mut store).0, PAYLOAD);
}

#[test]
fn unchunked_one_field_mutations_refuse() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), PAYLOAD);
    let mut stage = store.atomic_stage().unwrap();
    stage
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    stage.append_staged(m.clone(), PAYLOAD.to_vec()).unwrap();

    let mut event = m.clone();
    event.event_id = vid(4);
    assert_duplicate(stage.append_staged(event, PAYLOAD.to_vec()).unwrap_err());

    let mut hash = m.clone();
    hash.after_content_hash = Some([0x11; 32]);
    assert_duplicate(stage.append_staged(hash, PAYLOAD.to_vec()).unwrap_err());

    let mut key = m.clone();
    key.object_identity = ObjectIdentity::new(cid(), CanonicalKey::String("other".into()));
    assert_duplicate(stage.append_staged(key, PAYLOAD.to_vec()).unwrap_err());

    assert_duplicate(
        stage
            .append_staged(m.clone(), b"SECRET".to_vec())
            .unwrap_err(),
    );

    let mut other = m.clone();
    other.after_content_hash = Some(*blake3::hash(b"SECRET").as_bytes());
    assert_duplicate(stage.append_staged(other, b"SECRET".to_vec()).unwrap_err());

    let mut ordinal = m;
    ordinal.ordinal = 1;
    let msg = refused(stage.append_staged(ordinal, PAYLOAD.to_vec()).unwrap_err());
    assert!(
        msg.contains("MalformedInput") || msg.contains("DuplicateTarget"),
        "unknown ordinal must refuse, got {msg}"
    );
}

#[test]
fn chunked_exact_retry_and_conflict_matrix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), PAYLOAD);
    let (plan_chunks, p0, p1) = chunks();
    {
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage
            .commit_chunk_manifest(aid(), 0, plan_chunks.clone())
            .unwrap();
        stage
            .commit_chunk_manifest(aid(), 0, plan_chunks.clone())
            .unwrap();
        let mut other_plan = plan_chunks.clone();
        other_plan.chunk_hashes[0] = [0x22; 32];
        assert_duplicate(
            stage
                .commit_chunk_manifest(aid(), 0, other_plan)
                .unwrap_err(),
        );

        stage.append_chunk(m.clone(), 0, p0.to_vec()).unwrap();
        stage.append_chunk(m.clone(), 0, p0.to_vec()).unwrap();
        assert_duplicate(
            stage
                .append_chunk(m.clone(), 0, b"xx".to_vec())
                .unwrap_err(),
        );

        stage.append_chunk(m.clone(), 1, p1.to_vec()).unwrap();
        stage.append_chunk(m.clone(), 1, p1.to_vec()).unwrap();
        assert_duplicate(
            stage
                .append_chunk(m.clone(), 1, b"XXXX".to_vec())
                .unwrap_err(),
        );

        let mut bad_member = m.clone();
        bad_member.event_id = vid(9);
        assert_duplicate(stage.append_chunk(bad_member, 1, p1.to_vec()).unwrap_err());
        assert_eq!(
            stage.kernel().inspect_staged(aid()).unwrap()[0].payload,
            PAYLOAD
        );
        assert!(stage.kernel().inspect_staged(aid()).unwrap()[0].payload_complete);
    }
    let before = media_bytes(&store);
    {
        let mut stage = store.atomic_stage().unwrap();
        // After reopen the assembled payload is the durable form (CR-ATMR5-005
        // still owns durable chunk prefixes). Exact unchunked replay is the
        // same Atomic identity.
        stage.append_staged(m.clone(), PAYLOAD.to_vec()).unwrap();
    }
    assert_eq!(media_bytes(&store), before);

    drop(store);
    let mut store = Store::open(&path).unwrap();
    let mut stage = store.atomic_stage().unwrap();
    stage.append_staged(m, PAYLOAD.to_vec()).unwrap();
    assert_eq!(
        stage.kernel().inspect_staged(aid()).unwrap()[0].payload,
        PAYLOAD
    );
}

#[test]
fn unchunked_and_chunked_status_match() {
    let dir = tempfile::tempdir().unwrap();
    let unchunked = {
        let path = dir.path().join("u");
        let mut store = Store::create(&path).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let m = member();
        let p = plan(heap, std::slice::from_ref(&m), PAYLOAD);
        {
            let mut stage = store.atomic_stage().unwrap();
            stage
                .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
                .unwrap();
            stage.append_staged(m, PAYLOAD.to_vec()).unwrap();
        }
        snapshot(&mut store)
    };
    let chunked = {
        let path = dir.path().join("c");
        let mut store = Store::create(&path).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let m = member();
        let p = plan(heap, std::slice::from_ref(&m), PAYLOAD);
        let (plan_chunks, p0, p1) = chunks();
        {
            let mut stage = store.atomic_stage().unwrap();
            stage
                .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
                .unwrap();
            stage.commit_chunk_manifest(aid(), 0, plan_chunks).unwrap();
            stage.append_chunk(m.clone(), 0, p0.to_vec()).unwrap();
            stage.append_chunk(m, 1, p1.to_vec()).unwrap();
        }
        snapshot(&mut store)
    };
    assert_eq!(unchunked.0, chunked.0);
    assert_eq!(unchunked.1, chunked.1);
    assert_eq!(unchunked.1, MemberPhase::Staged);
}
