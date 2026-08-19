//! CR-ATMR5-002: store catalogue does not guess through damage or conflicts.

use residiuum_atomics::{
    encode_prepare, prepare_from_closed_plan, AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts,
    AtomicProfile, CanonicalKey, CollectionId, ContentRoot, CoordinationScope, HeapId,
    MutationKind, ObjectIdentity, PlanMutation, ResourceLimits, VersionId,
};
use residiuum_format::{
    encode_atomic_frame, encode_atomic_prepare_envelope, encode_frame, FrameHeader, FrameKind,
    FrameParts, EMPTY_ENVELOPE,
};
use residiuum_store::{
    atomic_stage_checkpoint_path, StageEvidenceClass, StageEvidenceKind, Store, StoreError,
};
use std::fs;
use std::path::Path;

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

fn write_prepare_file(path: &Path, prepare: &residiuum_atomics::AtomicPrepare, event: u8) {
    let env = encode_atomic_prepare_envelope(
        prepare.heap_id.as_bytes(),
        prepare.atomic_id.as_bytes(),
        prepare.content_root.as_bytes(),
    )
    .unwrap();
    let body = encode_prepare(prepare).unwrap();
    let mut event_id = [0u8; 16];
    event_id[0] = event;
    let bytes = encode_atomic_frame(FrameKind::BatchPrepare, &env, &body, event_id).unwrap();
    fs::write(path, bytes).unwrap();
}

fn write_payload_chunk(path: &Path, body: Vec<u8>, event: u8) {
    let mut event_id = [0u8; 16];
    event_id[0] = event;
    let parts = FrameParts {
        header: FrameHeader::new_draft(
            FrameKind::PayloadChunk,
            EMPTY_ENVELOPE.len() as u32,
            body.len() as u64,
            event_id,
        ),
        envelope: EMPTY_ENVELOPE.to_vec(),
        body,
    };
    fs::write(path, encode_frame(&parts).unwrap()).unwrap();
}

fn two_prepares(
    store_id: [u8; 16],
) -> (
    residiuum_atomics::AtomicPrepare,
    residiuum_atomics::AtomicPrepare,
) {
    let heap = HeapId::from_bytes(store_id).unwrap();
    let m = member();
    let a = prepare_from_closed_plan(
        &plan(heap, std::slice::from_ref(&m), b"secret"),
        FRONTIER,
        std::slice::from_ref(&m),
    )
    .unwrap();
    let other_frontier = [0xB2; 32];
    let b = prepare_from_closed_plan(
        &plan(heap, std::slice::from_ref(&m), b"secret"),
        other_frontier,
        &[m],
    )
    .unwrap();
    assert_ne!(a, b);
    (a, b)
}

#[test]
fn conflicting_prepares_same_result_regardless_of_file_order() {
    let dir = tempfile::tempdir().unwrap();
    for tag in ["ab", "ba"] {
        let root = dir.path().join(tag);
        let mut store = Store::create(&root).unwrap();
        let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
        let (a, b) = two_prepares(store.store_id());
        let (left, right) = if tag == "ab" { (&a, &b) } else { (&b, &a) };
        let segs = store.paths().segments_dir();
        fs::create_dir_all(&segs).unwrap();
        write_prepare_file(&segs.join("aaa.residiuum"), left, 1);
        write_prepare_file(&segs.join("bbb.residiuum"), right, 2);
        let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
        let stage = store.atomic_stage().unwrap();
        assert!(
            stage.kernel().placement(aid()).is_none(),
            "{tag}: conflicting identity must not be installed"
        );
        assert!(
            stage
                .findings()
                .records
                .iter()
                .any(|f| f.class == StageEvidenceClass::Conflict && f.atomic_id == Some(aid())),
            "{tag}: expected Conflict, got {:?}",
            stage.findings().records
        );
        drop(stage);
        let p = plan(heap_id, std::slice::from_ref(&member()), b"secret");
        let mut stage = store.atomic_stage().unwrap();
        match stage.begin_prepare(&p, FRONTIER, &[member()]) {
            Err(StoreError::AtomicStage(msg)) => {
                assert!(
                    msg.contains("blocked"),
                    "{tag}: identity must not be reusable, got {msg}"
                );
            }
            Ok(_) => panic!("{tag}: blocked identity must not prepare"),
            Err(other) => panic!("{tag}: unexpected {other}"),
        }
    }
}

#[test]
fn mutated_prepare_sidecar_is_corrupt_not_absent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    let mut body = b"ATPREP1".to_vec();
    body.extend_from_slice(&[0xFF; 24]);
    write_payload_chunk(&segs.join("bad-prep.residiuum"), body, 7);
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    let stage = store.atomic_stage().unwrap();
    assert!(stage
        .findings()
        .records
        .iter()
        .any(|f| f.kind == StageEvidenceKind::Prepare
            && matches!(
                f.class,
                StageEvidenceClass::Corrupt | StageEvidenceClass::Partial
            )));
    assert!(stage.kernel().placement(aid()).is_none());
}

#[test]
fn foreign_heap_prepare_is_not_resolved_locally() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let mut foreign_id = store.store_id();
    foreign_id[0] ^= 0xFF;
    let foreign_heap = HeapId::from_bytes(foreign_id).unwrap();
    let m = member();
    let foreign_plan = plan(foreign_heap, std::slice::from_ref(&m), b"secret");
    let prepare = prepare_from_closed_plan(&foreign_plan, FRONTIER, &[m]).unwrap();
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    write_prepare_file(&segs.join("foreign.residiuum"), &prepare, 3);
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    let stage = store.atomic_stage().unwrap();
    assert!(stage.kernel().placement(aid()).is_none());
    assert!(stage
        .findings()
        .records
        .iter()
        .any(|f| f.class == StageEvidenceClass::ForeignHeap && f.atomic_id == Some(aid())));
}

#[test]
fn mismatched_seal_root_cannot_seal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), b"secret");
    {
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage.append_staged(m, b"secret".to_vec()).unwrap();
    }
    let mut wrong = [0x11u8; 32];
    wrong[0] = 0x5E;
    let root = ContentRoot::from_bytes(wrong).unwrap();
    let mut body = b"ATSEAL1".to_vec();
    body.extend_from_slice(aid().as_bytes());
    body.extend_from_slice(root.as_bytes());
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    write_payload_chunk(&segs.join("bad-seal.residiuum"), body, 8);
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    let stage = store.atomic_stage().unwrap();
    let life = stage.kernel().lifecycle(aid());
    assert!(
        life.is_none()
            || !matches!(
                life.unwrap().members,
                residiuum_atomics::MemberPhase::DurableInvisible
            ),
        "mismatched seal must not invent the first stable boundary"
    );
    assert!(stage
        .findings()
        .records
        .iter()
        .any(|f| f.kind == StageEvidenceKind::Seal && f.class == StageEvidenceClass::Conflict));
}

#[test]
fn holes_are_reported_not_swallowed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    fs::write(segs.join("garbage.residiuum"), [0u8; 64]).unwrap();
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    let stage = store.atomic_stage().unwrap();
    assert!(
        stage.open_report().holes >= 1,
        "physical holes must be counted, report={:?}",
        stage.open_report()
    );
    assert!(stage
        .findings()
        .records
        .iter()
        .any(|f| f.kind == StageEvidenceKind::Hole));
}
