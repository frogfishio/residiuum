//! CR-ATMR5-006: one authoritative BatchPrepare; no ATPREP1 dual-write.

use residiuum_atomics::{
    encode_member, encode_prepare, prepare_from_closed_plan, AtomicId, AtomicMember, AtomicPlan,
    AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId, CoordinationScope, HeapId,
    MutationKind, ObjectIdentity, PlanMutation, ResourceLimits, VersionId,
};
use residiuum_format::{
    encode_atomic_frame, encode_atomic_member_envelope, encode_frame, examine_atomic_frame,
    scan_forward, AtomicEvidenceClass, AtomicFrameRole, FrameHeader, FrameKind, FrameParts,
    SafetyLimits, EMPTY_ENVELOPE,
};
use residiuum_store::{
    arm_failpoint_once, atomic_stage_checkpoint_path, clear_failpoints, FailpointAction, Store,
    StoreError,
};
use std::fs;
use std::path::Path;
use std::sync::Mutex;

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    guard
}

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

fn media_bytes(store: &Store) -> Vec<u8> {
    let mut out = Vec::new();
    for dir in [
        store.paths().active_dir(),
        store.paths().segments_dir(),
        store.paths().pending_seal_dir(),
    ] {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Ok(bytes) = fs::read(&path) {
                    out.extend_from_slice(&bytes);
                }
            }
        }
    }
    out
}

fn count_prepare_roles(bytes: &[u8]) -> usize {
    scan_forward(bytes, SafetyLimits::draft_defaults())
        .verified_frames()
        .filter(|(_, frame)| {
            matches!(
                examine_atomic_frame(frame),
                Some(AtomicEvidenceClass::Valid(link)) if link.role == AtomicFrameRole::Prepare
            )
        })
        .count()
}

fn count_atprep1(bytes: &[u8]) -> usize {
    bytes.windows(7).filter(|w| *w == b"ATPREP1").count()
}

fn write_legacy_sidecar_and_member(
    dir: &Path,
    prepare: &residiuum_atomics::AtomicPrepare,
    member: &AtomicMember,
) {
    write_legacy_sidecar(&dir.join("legacy-prep.residiuum"), prepare);
    let env = encode_atomic_member_envelope(
        prepare.heap_id.as_bytes(),
        member.atomic_id.as_bytes(),
        u64::from(member.ordinal),
        prepare.content_root.as_bytes(),
        None,
    )
    .unwrap();
    let body = encode_member(member).unwrap();
    let bytes = encode_atomic_frame(
        FrameKind::ItemEvent,
        &env,
        &body,
        member.event_id.to_bytes(),
    )
    .unwrap();
    fs::write(dir.join("legacy-member.residiuum"), bytes).unwrap();
}

fn write_legacy_sidecar(path: &Path, prepare: &residiuum_atomics::AtomicPrepare) {
    let mut body = b"ATPREP1".to_vec();
    body.extend_from_slice(&encode_prepare(prepare).unwrap());
    let parts = FrameParts {
        header: FrameHeader::new_draft(
            FrameKind::PayloadChunk,
            EMPTY_ENVELOPE.len() as u32,
            body.len() as u64,
            [0x51; 16],
        ),
        envelope: EMPTY_ENVELOPE.to_vec(),
        body,
    };
    fs::write(path, encode_frame(&parts).unwrap()).unwrap();
}

#[test]
fn persist_writes_only_batch_prepare() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), b"secret");
    store
        .atomic_stage()
        .unwrap()
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    let bytes = media_bytes(&store);
    assert_eq!(count_atprep1(&bytes), 0, "must not write ATPREP1");
    assert_eq!(count_prepare_roles(&bytes), 1, "exactly one BatchPrepare");
}

#[test]
fn store_reopen_preserves_the_single_batch_prepare() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let mut store = Store::create(&path).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let m = member();
        let p = plan(heap, std::slice::from_ref(&m), b"secret");
        store
            .atomic_stage()
            .unwrap()
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
    }
    let mut store = Store::open(&path).unwrap();
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    let bytes = media_bytes(&store);
    assert_eq!(count_atprep1(&bytes), 0);
    assert_eq!(count_prepare_roles(&bytes), 1);
    let stage = store.atomic_stage().unwrap();
    assert!(stage.kernel().placement(aid()).is_some());
}

#[test]
fn sidecar_only_prefix_is_repaired_to_batch_prepare() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), b"secret");
    let prepare = prepare_from_closed_plan(&p, FRONTIER, std::slice::from_ref(&m)).unwrap();
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    write_legacy_sidecar_and_member(&segs, &prepare, &m);
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    {
        let mut stage = store.atomic_stage().unwrap();
        assert!(stage.kernel().placement(aid()).is_some());
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
    }
    let bytes = media_bytes(&store);
    assert_eq!(count_prepare_roles(&bytes), 1, "repair writes BatchPrepare");
}

#[test]
fn failpoint_before_append_leaves_no_prepare() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), b"secret");
    arm_failpoint_once("store.atomic.prepare.before_append", FailpointAction::Error);
    let err = store
        .atomic_stage()
        .unwrap()
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap_err();
    assert!(matches!(err, StoreError::Failpoint(_)));
    clear_failpoints();
    drop(store);
    let mut store = Store::open(&path).unwrap();
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    let stage = store.atomic_stage().unwrap();
    assert!(stage.kernel().placement(aid()).is_none());
}

#[test]
fn failpoint_after_append_retry_is_durable_and_idempotent() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), b"secret");
    arm_failpoint_once("store.atomic.prepare.after_append", FailpointAction::Error);
    let err = store
        .atomic_stage()
        .unwrap()
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap_err();
    assert!(matches!(err, StoreError::Failpoint(_)));
    clear_failpoints();
    drop(store);
    let mut store = Store::open(&path).unwrap();
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    assert_eq!(
        count_prepare_roles(&media_bytes(&store)),
        1,
        "BatchPrepare must survive the after_append crash prefix"
    );
    {
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        assert!(stage.kernel().placement(aid()).is_some());
    }
    let bytes = media_bytes(&store);
    assert_eq!(count_prepare_roles(&bytes), 1);
    assert_eq!(count_atprep1(&bytes), 0);
}
