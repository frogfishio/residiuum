//! CR-ATMR3-006: staged Atomic material is invisible on store read surfaces.

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, ChunkPlan,
    CollectionId, CoordinationScope, HeapId, MemberPhase, MutationKind, ObjectIdentity,
    PlanMutation, ResourceLimits, VersionId,
};
use residiuum_format::{read_atomic_evidence, AtomicEvidenceClass, AtomicFrameRole, SafetyLimits};
use residiuum_store::{list_secondary_index_paths, DurabilityMode, Store, StoreError};
use std::fs;

const FRONTIER: [u8; 32] = [0xA1; 32];

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
        after_content_hash: Some(*blake3::hash(b"secret").as_bytes()),
        event_id: vid(),
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

#[test]
fn inspect_cannot_open_atomic_stage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut writer = Store::create(&path).unwrap();
    writer.put("vis", b"ok", DurabilityMode::Durable).unwrap();
    drop(writer);
    let mut inspect = Store::open_inspect(&path).unwrap();
    match inspect.atomic_stage() {
        Err(StoreError::AtomicStage(_)) => {}
        Ok(_) => panic!("inspect must not own the atomic stage"),
        Err(_) => panic!("expected AtomicStage"),
    }
}

#[test]
fn staged_value_is_invisible_on_get_scan_history_and_secondary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store
        .put("ordinary", b"visible", DurabilityMode::Durable)
        .unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap_id, std::slice::from_ref(&m), b"secret");
    {
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage.append_staged(m, b"secret".to_vec()).unwrap();
        stage.seal_member_boundary(aid()).unwrap();
    }

    assert_eq!(
        store.get("ordinary").unwrap().as_deref(),
        Some(b"visible".as_slice())
    );
    assert_eq!(store.get("k").unwrap(), None);
    assert_eq!(store.get("secret").unwrap(), None);

    let scan = store.scan_live_logical().unwrap();
    let subjects: Vec<Vec<u8>> = scan.entries.iter().map(|(s, _)| s.clone()).collect();
    assert!(subjects.iter().any(|s| s == b"ordinary"));
    assert!(!subjects.iter().any(|s| s == b"k" || s == b"secret"));

    let hist = store.history("k").unwrap();
    assert!(hist.events.is_empty(), "staged member must not be history");

    let secondaries = list_secondary_index_paths(store.paths(), "k").unwrap();
    assert!(
        secondaries.is_empty(),
        "staging must not publish a secondary index"
    );
}

#[test]
fn leak_negative_control_is_visible_on_each_surface() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store.put("k", b"secret", DurabilityMode::Durable).unwrap();
    assert_eq!(
        store.get("k").unwrap().as_deref(),
        Some(b"secret".as_slice())
    );
    let scan = store.scan_live_logical().unwrap();
    assert!(scan
        .entries
        .iter()
        .any(|(s, v)| s == b"k" && v == b"secret"));
    assert!(!store.history("k").unwrap().events.is_empty());
}

#[test]
fn store_segment_carries_atomic_evidence_without_publication() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap_id, std::slice::from_ref(&m), b"secret");
    {
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage.append_staged(m, b"secret".to_vec()).unwrap();
    }
    let mut roles = Vec::new();
    let mut any_valid = false;
    for entry in fs::read_dir(store.paths().active_dir()).unwrap() {
        let path = entry.unwrap().path();
        if !path.is_file() {
            continue;
        }
        let bytes = fs::read(&path).unwrap();
        let report = read_atomic_evidence(&bytes, SafetyLimits::draft_defaults());
        any_valid |= report
            .examined
            .iter()
            .any(|e| matches!(e.class, AtomicEvidenceClass::Valid(_)));
        roles.extend(report.valid().map(|l| l.role));
    }
    assert!(
        roles.contains(&AtomicFrameRole::Prepare),
        "expected Atomic prepare on an active segment, got {roles:?}"
    );
    assert!(
        roles.contains(&AtomicFrameRole::Member),
        "expected Atomic member on an active segment, got {roles:?}"
    );
    assert!(any_valid);
    assert_eq!(store.get("k").unwrap(), None);
}

#[test]
fn reopen_keeps_staged_invisible() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let mut store = Store::create(&path).unwrap();
        let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
        let m = member();
        let p = plan(heap_id, std::slice::from_ref(&m), b"secret");
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage.append_staged(m, b"secret".to_vec()).unwrap();
    }
    let store = Store::open(&path).unwrap();
    assert_eq!(store.get("k").unwrap(), None);
    let scan = store.scan_live_logical().unwrap();
    assert!(scan.entries.is_empty());
}

#[test]
fn chunked_stage_survives_reopen_and_stays_invisible() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let p0 = b"se";
    let p1 = b"cret";
    {
        let mut store = Store::create(&path).unwrap();
        let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
        let m = member();
        let p = plan(heap_id, std::slice::from_ref(&m), b"secret");
        {
            let mut stage = store.atomic_stage().unwrap();
            stage
                .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
                .unwrap();
            stage
                .commit_chunk_manifest(
                    aid(),
                    0,
                    ChunkPlan {
                        total: 2,
                        chunk_hashes: vec![
                            *blake3::hash(p0).as_bytes(),
                            *blake3::hash(p1).as_bytes(),
                        ],
                    },
                )
                .unwrap();
            stage.append_chunk(m.clone(), 0, p0.to_vec()).unwrap();
            stage.append_chunk(m, 1, p1.to_vec()).unwrap();
            stage.seal_member_boundary(aid()).unwrap();
        }
        assert_eq!(store.get("k").unwrap(), None);
    }
    let store = Store::open(&path).unwrap();
    assert_eq!(store.get("k").unwrap(), None);
    assert!(store.scan_live_logical().unwrap().entries.is_empty());
}

#[test]
fn peer_lane_is_not_created_or_required() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap_id, std::slice::from_ref(&m), b"secret");
    let lane;
    {
        let mut stage = store.atomic_stage().unwrap();
        lane = stage.lane_root();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage.append_staged(m, b"secret".to_vec()).unwrap();
        stage.seal_member_boundary(aid()).unwrap();
        assert!(!lane.exists(), "store stage must not create a peer lane");
        assert_eq!(
            stage.kernel().lifecycle(aid()).unwrap().members,
            MemberPhase::DurableInvisible
        );
    }
    fs::create_dir_all(&lane).unwrap();
    fs::write(lane.join("junk"), b"not-authority").unwrap();
    drop(store);
    let mut store = Store::open(&path).unwrap();
    assert_eq!(store.get("k").unwrap(), None);
    fs::remove_dir_all(&lane).unwrap();
    {
        let stage = store.atomic_stage().unwrap();
        assert!(stage.kernel().can_resolve(aid()));
        assert_eq!(
            stage.kernel().lifecycle(aid()).unwrap().members,
            MemberPhase::DurableInvisible
        );
    }
}

#[test]
fn same_id_retry_and_final_chunk_close_store_payload() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap_id, std::slice::from_ref(&m), b"secret");
    let p0 = b"se";
    let p1 = b"cret";
    {
        let mut stage = store.atomic_stage().unwrap();
        let first = stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        let again = stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        assert_eq!(first.0, again.0);
        stage
            .commit_chunk_manifest(
                aid(),
                0,
                ChunkPlan {
                    total: 2,
                    chunk_hashes: vec![*blake3::hash(p0).as_bytes(), *blake3::hash(p1).as_bytes()],
                },
            )
            .unwrap();
        stage.append_chunk(m.clone(), 0, p0.to_vec()).unwrap();
        stage.append_chunk(m.clone(), 1, p1.to_vec()).unwrap();
        stage.append_chunk(m, 1, p1.to_vec()).unwrap();
        stage.seal_member_boundary(aid()).unwrap();
        stage.seal_member_boundary(aid()).unwrap();
    }
    drop(store);
    let mut store = Store::open(&path).unwrap();
    let stage = store.atomic_stage().unwrap();
    assert!(stage.kernel().can_resolve(aid()));
    assert_eq!(
        stage.kernel().inspect_staged(aid()).unwrap()[0].payload,
        b"secret"
    );
    assert_eq!(
        stage.kernel().lifecycle(aid()).unwrap().members,
        MemberPhase::DurableInvisible
    );
}
