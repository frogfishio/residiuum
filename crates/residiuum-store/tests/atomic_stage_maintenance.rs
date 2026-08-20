//! CR-ATMR6-006: freeze staging records; fail-closed maintenance.

use residiuum_atomics::{
    AtomicAbortReason, AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile,
    CanonicalKey, ChunkPlan, CollectionId, CoordinationScope, DecisionCode, HeapId, LogicalStatus,
    MaterialStatus, MutationKind, ObjectIdentity, PlanMutation, ResourceLimits, VersionId,
};
use residiuum_format::{
    encode_frame, encode_subject_v2, examine_atomic_frame, read_atomic_evidence, scan_forward,
    AtomicEvidenceClass, AtomicFrameRole, FrameParts, SafetyLimits, ScanRegion, SubjectObjectKind,
};
use residiuum_store::{
    arm_failpoint_once_current_thread, atomic_coord_path, atomic_stage_checkpoint_path,
    clear_failpoints, outstanding_atomic_evidence, restore_full_backup, CompactOptions,
    DurabilityMode, FailpointAction, RestoreOptions, ScrubOptions, Store, StoreError,
    StoreOpenOptions, TierClass, TierMoveMode, ATOMIC_STAGE_CHECKPOINT_FILE,
};
use std::fs;
use std::sync::{Mutex, OnceLock};

const FRONTIER: [u8; 32] = [0xA1; 32];

fn fp_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn aid() -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = 9;
    AtomicId::from_bytes(b).unwrap()
}

fn second_aid() -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = 10;
    AtomicId::from_bytes(b).unwrap()
}

fn third_aid() -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = 11;
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

fn sibling_member() -> AtomicMember {
    let mut event = [0u8; 16];
    event[0] = 6;
    AtomicMember {
        atomic_id: aid(),
        ordinal: 1,
        object_identity: ObjectIdentity::new(cid(), CanonicalKey::String("sibling".into())),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(b"secret").as_bytes()),
        event_id: VersionId::from_bytes(event).unwrap(),
    }
}

fn second_member() -> AtomicMember {
    let mut event = [0u8; 16];
    event[0] = 4;
    AtomicMember {
        atomic_id: second_aid(),
        ordinal: 0,
        object_identity: ObjectIdentity::new(cid(), CanonicalKey::String("k2".into())),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(b"second").as_bytes()),
        event_id: VersionId::from_bytes(event).unwrap(),
    }
}

fn third_member() -> AtomicMember {
    let mut event = [0u8; 16];
    event[0] = 5;
    AtomicMember {
        atomic_id: third_aid(),
        ordinal: 0,
        object_identity: ObjectIdentity::new(cid(), CanonicalKey::String("k3".into())),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(b"third").as_bytes()),
        event_id: VersionId::from_bytes(event).unwrap(),
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

fn stage_one(store: &mut Store) {
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap_id, std::slice::from_ref(&m), b"secret");
    let mut stage = store.atomic_stage().unwrap();
    stage
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    stage.append_staged(m, b"secret".to_vec()).unwrap();
    stage.seal_member_boundary(aid()).unwrap();
}

fn committed_plan(store: &mut Store) -> (HeapId, AtomicPlan, Vec<u8>) {
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap_id, std::slice::from_ref(&m), b"secret");
    let decision = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&p)
        .unwrap();
    assert_eq!(decision.commit_position, Some(1));
    let subject = encode_subject_v2(
        heap_id.as_bytes(),
        SubjectObjectKind::Collection,
        cid().as_bytes(),
        &CanonicalKey::String("k".into()).subject_bytes(),
    )
    .unwrap();
    assert_eq!(
        store.get_subject_bytes(&subject).unwrap().as_deref(),
        Some(b"secret".as_slice())
    );
    (heap_id, p, subject)
}

fn assert_committed_complete(store: &mut Store, heap_id: HeapId, subject: &[u8]) {
    assert_eq!(
        store.get_subject_bytes(subject).unwrap().as_deref(),
        Some(b"secret".as_slice())
    );
    let status = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .atomic_status(aid())
        .unwrap();
    assert_eq!(status.logical, LogicalStatus::Committed);
    assert_eq!(status.material, MaterialStatus::Complete);
    assert!(status.receipt.is_some());
}

fn subject(heap_id: HeapId, key: &str) -> Vec<u8> {
    encode_subject_v2(
        heap_id.as_bytes(),
        SubjectObjectKind::Collection,
        cid().as_bytes(),
        &CanonicalKey::String(key.into()).subject_bytes(),
    )
    .unwrap()
}

fn media_files(store: &Store) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for dir in [store.paths().active_dir(), store.paths().segments_dir()] {
        if let Ok(entries) = fs::read_dir(dir) {
            files.extend(
                entries
                    .flatten()
                    .map(|entry| entry.path())
                    .filter(|path| path.is_file()),
            );
        }
    }
    files.sort();
    files
}

/// Keep the outer frame authenticated and attributable while making the
/// selected Atomic body undecodable.
fn corrupt_atomic_body(store: &Store, role: AtomicFrameRole) {
    for path in media_files(store) {
        let mut bytes = fs::read(&path).unwrap();
        let scan = scan_forward(&bytes, SafetyLimits::draft_defaults());
        for region in scan.regions {
            let ScanRegion::VerifiedFrame { frame, range } = region else {
                continue;
            };
            let Some(AtomicEvidenceClass::Valid(link)) = examine_atomic_frame(&frame) else {
                continue;
            };
            if link.role != role {
                continue;
            }
            let replacement = encode_frame(&FrameParts {
                header: frame.header,
                envelope: frame.envelope,
                body: vec![0xFF; frame.body.len()],
            })
            .unwrap();
            assert_eq!(replacement.len(), range.end as usize - range.start as usize);
            bytes[range.start as usize..range.end as usize].copy_from_slice(&replacement);
            fs::write(path, bytes).unwrap();
            return;
        }
    }
    panic!("Atomic role {role:?} not found");
}

fn erase_local_material_and_atomic_support(root: &std::path::Path) {
    for entry in fs::read_dir(root.join("segments")).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            fs::remove_file(path).unwrap();
        }
    }
    for path in [
        root.join("store-info/atomic-stage.ckpt"),
        root.join("store-info/atomic-coord.ckpt"),
        root.join("store-info/atomic-tombstones.idx"),
    ] {
        let _ = fs::remove_file(path);
    }
    let authority_dir = root.join("store-info/atomic-authority");
    if authority_dir.is_dir() {
        for entry in fs::read_dir(&authority_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                fs::remove_file(path).unwrap();
            }
        }
    }
}

fn configure_external_cold(root: &std::path::Path, external: &std::path::Path, online: bool) {
    fs::create_dir_all(external).unwrap();
    let state = if online { "online" } else { "offline" };
    let body = format!(
        "# residiuum tier roots v1\n\
         hot online {}\n\
         warm online {}\n\
         cold {state} {}\n\
         archive online {}\n",
        root.join("segments").display(),
        root.join("tiers/warm").display(),
        external.display(),
        root.join("tiers/archive").display(),
    );
    fs::write(root.join("tiers/roots.txt"), body).unwrap();
}

fn compact_output_with_atomic_and_ordinary(
    root: &std::path::Path,
) -> (HeapId, AtomicPlan, Vec<u8>, [u8; 16]) {
    let mut store = Store::create(root).unwrap();
    let (heap_id, plan, atomic_subject) = committed_plan(&mut store);
    store
        .put("ordinary/external", b"ordinary", DurabilityMode::Durable)
        .unwrap();
    store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    let job = store
        .list_compact_jobs()
        .unwrap()
        .into_iter()
        .find(|job| job.reclaim_requested)
        .unwrap();
    let output = residiuum_store::unhex16(&job.output_segment_id).unwrap();
    store.abandon_for_crash_test();
    drop(store);
    (heap_id, plan, atomic_subject, output)
}

fn media_has_prepare_or_member(root: &std::path::Path) -> bool {
    fn walk(dir: &std::path::Path) -> bool {
        let Ok(entries) = fs::read_dir(dir) else {
            return false;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() && walk(&path) {
                return true;
            }
            if !path.is_file() {
                continue;
            }
            let Ok(bytes) = fs::read(&path) else {
                continue;
            };
            let report = read_atomic_evidence(&bytes, SafetyLimits::draft_defaults());
            if report.examined.iter().any(|e| {
                matches!(
                    &e.class,
                    AtomicEvidenceClass::Valid(link)
                        if link.role == AtomicFrameRole::Prepare
                            || link.role == AtomicFrameRole::Member
                )
            }) {
                return true;
            }
        }
        false
    }
    walk(root)
}

fn assert_refused(err: StoreError) {
    match err {
        StoreError::AtomicStage(detail) => {
            assert!(
                detail.contains("outstanding Atomic")
                    || detail.contains("maintenance relocation refused"),
                "unexpected AtomicStage detail: {detail}"
            );
        }
        other => panic!("expected AtomicStage refuse, got {other:?}"),
    }
}

#[test]
fn healthy_terminal_atomic_seals_compacts_without_reclaim_and_replays() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let (heap_id, first_plan, subject) = committed_plan(&mut store);

    store.seal_active().unwrap();
    assert_committed_complete(&mut store, heap_id, &subject);
    let report = store.compact_live().unwrap();
    assert!(report.sources_retained);
    assert_committed_complete(&mut store, heap_id, &subject);
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    assert_committed_complete(&mut reopened, heap_id, &subject);
    let replay = reopened
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&first_plan)
        .unwrap();
    assert_eq!(replay.commit_position, Some(1));
}

#[test]
fn terminal_atomic_segment_move_heals_checkpoint_and_payload_locator() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let (heap_id, _plan, subject) = committed_plan(&mut store);
    store.seal_active().unwrap();
    let segment = *store
        .list_segment_ids()
        .first()
        .expect("sealed Atomic segment");
    let moved = store
        .transfer_segment_to_tier(segment, TierClass::Warm, TierMoveMode::Move)
        .unwrap();
    assert_eq!(moved.source_hash, moved.dest_hash);
    assert_committed_complete(&mut store, heap_id, &subject);
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    assert_committed_complete(&mut reopened, heap_id, &subject);
    assert_eq!(reopened.open_metrics().atomic_stage_publication_degraded, 0);
}

#[test]
fn terminal_atomic_full_backup_restore_preserves_status_value_and_retry() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    let dst = dir.path().join("dst");
    let mut store = Store::create(&src).unwrap();
    let (heap_id, plan, subject) = committed_plan(&mut store);
    let compact = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    assert_eq!(compact.phase, residiuum_store::CompactPhase::Reclaimed);
    store.backup_to(&bak).unwrap();
    drop(store);

    let report = restore_full_backup(&bak, &dst, RestoreOptions::default()).unwrap();
    assert_eq!(report.restored_store_id, heap_id.to_bytes());
    let mut restored = Store::open(&dst).unwrap();
    assert_committed_complete(&mut restored, heap_id, &subject);
    let replay = restored
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&plan)
        .unwrap();
    assert_eq!(replay.commit_position, Some(1));
}

#[test]
fn terminal_atomic_reclaim_installs_authority_generation_and_identity_reassign_still_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    let clone = dir.path().join("clone");
    let mut store = Store::create(&src).unwrap();
    let (heap_id, first_plan, subject) = committed_plan(&mut store);
    let report = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    assert_eq!(report.phase, residiuum_store::CompactPhase::Reclaimed);
    assert!(!report.sources_retained);
    assert!(report.bytes_reclaimed > 0);
    assert!(src.join("store-info").join("atomic-authority").is_dir());
    assert_committed_complete(&mut store, heap_id, &subject);
    drop(store);

    let mut store = Store::open(&src).unwrap();
    assert_committed_complete(&mut store, heap_id, &subject);
    let replay = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&first_plan)
        .unwrap();
    assert_eq!(replay.commit_position, Some(1));
    let second_member = second_member();
    let second_plan = plan(heap_id, std::slice::from_ref(&second_member), b"second");
    let second_decision = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&second_plan)
        .unwrap();
    assert_eq!(
        second_decision.commit_position,
        Some(2),
        "replacement generation must preserve the Heap commit high-water mark"
    );
    let second = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    assert_eq!(second.phase, residiuum_store::CompactPhase::Reclaimed);
    let authority_files = fs::read_dir(src.join("store-info").join("atomic-authority"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_file())
        .count();
    assert_eq!(authority_files, 1, "superseded generations must be pruned");
    assert_committed_complete(&mut store, heap_id, &subject);

    store.backup_to(&bak).unwrap();
    let err = restore_full_backup(
        &bak,
        &clone,
        RestoreOptions {
            reassign_identity: true,
        },
    )
    .unwrap_err();
    assert_refused(err);
    assert!(!clone.join("store-info").is_dir());
}

#[test]
fn not_committed_reclaim_preserves_exact_rejection_and_does_not_consume_position() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let occupied = subject(heap_id, "k");
    store
        .put_subject_bytes(&occupied, b"old", DurabilityMode::Durable)
        .unwrap();
    let rejected_member = member();
    let rejected_plan = plan(heap_id, std::slice::from_ref(&rejected_member), b"secret");
    let rejected = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&rejected_plan)
        .unwrap();
    assert_eq!(rejected.decision, DecisionCode::NotCommitted);
    assert_eq!(rejected.commit_position, None);
    assert_eq!(
        rejected.abort_reason,
        Some(AtomicAbortReason::PreconditionConflict)
    );

    let compact = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    assert_eq!(compact.phase, residiuum_store::CompactPhase::Reclaimed);
    drop(store);

    let mut reopened = Store::open(&root).unwrap();
    let status = reopened
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .atomic_status(aid())
        .unwrap();
    assert_eq!(status.logical, LogicalStatus::NotCommitted);
    assert_eq!(status.material, MaterialStatus::Complete);
    assert!(status.receipt.is_none());
    let replay = reopened
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&rejected_plan)
        .unwrap();
    assert_eq!(replay, rejected);
    assert_eq!(
        reopened.get_subject_bytes(&occupied).unwrap().as_deref(),
        Some(b"old".as_slice())
    );

    let success_member = second_member();
    let success_plan = plan(heap_id, std::slice::from_ref(&success_member), b"second");
    let success = reopened
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&success_plan)
        .unwrap();
    assert_eq!(success.commit_position, Some(1));
}

#[test]
fn chunked_committed_reclaim_preserves_map_payload_status_and_retry() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let closed = plan(heap_id, std::slice::from_ref(&m), b"secret");
    let chunk0 = b"se";
    let chunk1 = b"cret";
    let chunk_plan = ChunkPlan {
        total: 2,
        chunk_hashes: vec![
            *blake3::hash(chunk0).as_bytes(),
            *blake3::hash(chunk1).as_bytes(),
        ],
    };
    {
        let mut stage = store.atomic_stage_for_heap(heap_id).unwrap();
        stage
            .begin_prepare(&closed, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage
            .commit_chunk_manifest(aid(), 0, chunk_plan.clone())
            .unwrap();
        stage.append_chunk(m.clone(), 0, chunk0.to_vec()).unwrap();
        stage.append_chunk(m, 1, chunk1.to_vec()).unwrap();
        stage.seal_member_boundary(aid()).unwrap();
        let decision = stage.persist_committed_decision(aid()).unwrap();
        assert_eq!(decision.commit_position, Some(1));
        let replay = stage.decide_plan_evidence(&closed).unwrap();
        assert_eq!(replay, decision);
    }
    let key_subject = subject(heap_id, "k");
    assert_committed_complete(&mut store, heap_id, &key_subject);

    let compact = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    assert_eq!(compact.phase, residiuum_store::CompactPhase::Reclaimed);
    drop(store);

    let mut reopened = Store::open(&root).unwrap();
    assert_committed_complete(&mut reopened, heap_id, &key_subject);
    let mut stage = reopened.atomic_stage_for_heap(heap_id).unwrap();
    assert_eq!(stage.kernel().chunk_plan(aid(), 0), Some(&chunk_plan));
    let replay = stage.decide_plan_evidence(&closed).unwrap();
    assert_eq!(replay.commit_position, Some(1));
}

#[test]
fn damaged_terminal_atomic_refuses_reclaim_before_job_or_source_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let members = [member(), sibling_member()];
    let closed = plan(heap_id, &members, b"secret");
    let decision = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&closed)
        .unwrap();
    assert_eq!(decision.commit_position, Some(1));
    store.seal_active().unwrap();
    corrupt_atomic_body(&store, AtomicFrameRole::Member);
    fs::remove_file(atomic_stage_checkpoint_path(store.paths())).unwrap();
    drop(store);

    let mut reopened = Store::open(&root).expect("attributable damage must not kneel the store");
    let status = reopened
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .atomic_status(aid())
        .unwrap();
    assert_eq!(status.logical, LogicalStatus::Committed);
    assert_eq!(status.material, MaterialStatus::Partial);
    let sources_before = reopened.list_segment_ids();
    let jobs_before = reopened.list_compact_jobs().unwrap();
    assert_refused(
        reopened
            .compact_live_with(CompactOptions {
                reclaim_sources: true,
                allow_history_loss: true,
                ..CompactOptions::default()
            })
            .unwrap_err(),
    );
    assert_eq!(reopened.list_segment_ids(), sources_before);
    assert_eq!(reopened.list_compact_jobs().unwrap(), jobs_before);
}

#[test]
fn multi_source_reclaim_preserves_every_atomic_and_global_commit_frontier() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let first_member = member();
    let first_plan = plan(heap_id, std::slice::from_ref(&first_member), b"secret");
    let first = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&first_plan)
        .unwrap();
    assert_eq!(first.commit_position, Some(1));
    store.seal_active().unwrap();

    let second_member = second_member();
    let second_plan = plan(heap_id, std::slice::from_ref(&second_member), b"second");
    let second = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&second_plan)
        .unwrap();
    assert_eq!(second.commit_position, Some(2));
    store.seal_active().unwrap();
    assert!(store.list_segment_ids().len() >= 2);

    let compact = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    assert_eq!(compact.phase, residiuum_store::CompactPhase::Reclaimed);
    drop(store);

    let mut reopened = Store::open(&root).unwrap();
    for (id, closed, key, value, position) in [
        (aid(), &first_plan, "k", b"secret".as_slice(), 1),
        (second_aid(), &second_plan, "k2", b"second".as_slice(), 2),
    ] {
        let status = reopened
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .atomic_status(id)
            .unwrap();
        assert_eq!(status.logical, LogicalStatus::Committed);
        assert_eq!(status.material, MaterialStatus::Complete);
        assert_eq!(status.receipt.unwrap().commit_position, position);
        assert_eq!(
            reopened
                .get_subject_bytes(&subject(heap_id, key))
                .unwrap()
                .as_deref(),
            Some(value)
        );
        let replay = reopened
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_evidence(closed)
            .unwrap();
        assert_eq!(replay.commit_position, Some(position));
    }

    let third_member = third_member();
    let third_plan = plan(heap_id, std::slice::from_ref(&third_member), b"third");
    let third = reopened
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&third_plan)
        .unwrap();
    assert_eq!(third.commit_position, Some(3));
}

#[test]
fn terminal_atomic_reclaim_crash_cuts_keep_one_complete_authority_generation() {
    let _guard = fp_lock();
    for failpoint in [
        "store.atomic.authority.before_checkpoint_swap",
        "store.atomic.authority.after_checkpoint_swap",
        "store.recovery_shadow.atomic.before_publish",
        "store.recovery_shadow.atomic.after_publish",
        "store.compact.after_source_delete",
    ] {
        clear_failpoints();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("s");
        let mut store = Store::create(&root).unwrap();
        let (heap_id, plan, subject) = committed_plan(&mut store);
        arm_failpoint_once_current_thread(failpoint, FailpointAction::Error);
        let err = store
            .compact_live_with(CompactOptions {
                reclaim_sources: true,
                allow_history_loss: true,
                ..CompactOptions::default()
            })
            .unwrap_err();
        assert!(err.to_string().contains("failpoint"), "{failpoint}: {err}");
        clear_failpoints();
        store.abandon_for_crash_test();
        drop(store);

        let mut reopened = Store::open(&root).unwrap();
        assert_committed_complete(&mut reopened, heap_id, &subject);
        let replay = reopened
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_evidence(&plan)
            .unwrap();
        assert_eq!(replay.commit_position, Some(1), "{failpoint}");
        let job = reopened
            .list_compact_jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.reclaim_requested)
            .expect("reclaim job");
        let job_id = residiuum_store::unhex16(&job.job_id).unwrap();
        let completed = reopened.reclaim_compact_job(&job_id).unwrap();
        assert_eq!(completed.phase, residiuum_store::CompactPhase::Reclaimed);
        assert_committed_complete(&mut reopened, heap_id, &subject);
        for source in &job.source_segment_ids {
            let source = residiuum_store::unhex16(source).unwrap();
            assert!(
                !root
                    .join("recovery/shadow")
                    .join(format!("{}.rsh", residiuum_store::hex16(&source)))
                    .is_file(),
                "retry must retire the source Shadow after {failpoint}"
            );
        }
        assert!(
            residiuum_store::protected_frontier_gap_free(reopened.paths(), reopened.store_id())
                .unwrap(),
            "retry must retire obsolete frontier membership after {failpoint}"
        );
    }
    clear_failpoints();
}

#[test]
fn terminal_atomic_reclaim_refuses_an_incomplete_replacement_before_source_deletion() {
    let _guard = fp_lock();
    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    let (heap_id, _plan, subject) = committed_plan(&mut store);
    arm_failpoint_once_current_thread(
        "store.atomic.authority.omit_frame",
        FailpointAction::ShortWrite,
    );
    let err = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap_err();
    clear_failpoints();
    assert!(
        err.to_string().contains("not materially identical"),
        "unexpected refusal: {err}"
    );
    let job = store
        .list_compact_jobs()
        .unwrap()
        .into_iter()
        .find(|job| job.reclaim_requested)
        .expect("reclaim job");
    assert_eq!(job.bytes_reclaimed, 0);
    assert!(job.sources_retained);
    assert_committed_complete(&mut store, heap_id, &subject);

    let job_id = residiuum_store::unhex16(&job.job_id).unwrap();
    let completed = store.reclaim_compact_job(&job_id).unwrap();
    assert_eq!(completed.phase, residiuum_store::CompactPhase::Reclaimed);
    assert_committed_complete(&mut store, heap_id, &subject);
}

#[test]
fn recovery_shadow_transition_carries_terminal_atomic_authority() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store.rollback_to_materialized_mode().unwrap();
    let (heap_id, plan, subject) = committed_plan(&mut store);
    store.seal_active().unwrap();
    store.prepare_flip_to_compact_shadow().unwrap();
    assert!(path.join("recovery/shadow/atomic-authority.rsh").is_file());
    store.activate_compact_shadow_mode().unwrap();
    assert_committed_complete(&mut store, heap_id, &subject);
    let replay = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&plan)
        .unwrap();
    assert_eq!(replay.commit_position, Some(1));
    store.rollback_to_materialized_mode().unwrap();
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    assert_committed_complete(&mut reopened, heap_id, &subject);
}

#[test]
fn compact_shadow_only_rebuild_restores_values_atomic_status_retry_and_frontier() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    let (heap_id, first_plan, subject) = committed_plan(&mut store);
    let compact = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    assert_eq!(compact.phase, residiuum_store::CompactPhase::Reclaimed);
    assert!(residiuum_store::protected_frontier_gap_free(store.paths(), store.store_id()).unwrap());
    assert!(root.join("recovery/shadow/atomic-authority.rsh").is_file());
    store.abandon_for_crash_test();
    drop(store);

    erase_local_material_and_atomic_support(&root);

    let mut reopened = Store::open(&root).unwrap();
    assert!(reopened.list_segment_ids().len() >= 1);
    assert_committed_complete(&mut reopened, heap_id, &subject);
    let replay = reopened
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&first_plan)
        .unwrap();
    assert_eq!(replay.commit_position, Some(1));
    let next_member = second_member();
    let next_plan = plan(heap_id, std::slice::from_ref(&next_member), b"second");
    let next = reopened
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&next_plan)
        .unwrap();
    assert_eq!(next.commit_position, Some(2));
}

#[test]
fn compact_shadow_rebuild_preserves_chunked_and_not_committed_authority() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let committed_member = member();
    let committed_plan = plan(heap_id, std::slice::from_ref(&committed_member), b"secret");
    let chunk0 = b"se";
    let chunk1 = b"cret";
    let chunk_plan = ChunkPlan {
        total: 2,
        chunk_hashes: vec![
            *blake3::hash(chunk0).as_bytes(),
            *blake3::hash(chunk1).as_bytes(),
        ],
    };
    {
        let mut stage = store.atomic_stage_for_heap(heap_id).unwrap();
        stage
            .begin_prepare(
                &committed_plan,
                FRONTIER,
                std::slice::from_ref(&committed_member),
            )
            .unwrap();
        stage
            .commit_chunk_manifest(aid(), 0, chunk_plan.clone())
            .unwrap();
        stage
            .append_chunk(committed_member.clone(), 0, chunk0.to_vec())
            .unwrap();
        stage
            .append_chunk(committed_member, 1, chunk1.to_vec())
            .unwrap();
        stage.seal_member_boundary(aid()).unwrap();
        stage.persist_committed_decision(aid()).unwrap();
        stage.decide_plan_evidence(&committed_plan).unwrap();
    }

    let occupied = subject(heap_id, "k2");
    store
        .put_subject_bytes(&occupied, b"occupied", DurabilityMode::Durable)
        .unwrap();
    let rejected_member = second_member();
    let rejected_plan = plan(heap_id, std::slice::from_ref(&rejected_member), b"second");
    let rejected = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&rejected_plan)
        .unwrap();
    assert_eq!(rejected.decision, DecisionCode::NotCommitted);
    assert_eq!(rejected.commit_position, None);

    store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    store.abandon_for_crash_test();
    drop(store);
    erase_local_material_and_atomic_support(&root);

    let mut reopened = Store::open(&root).unwrap();
    assert_committed_complete(&mut reopened, heap_id, &subject(heap_id, "k"));
    let mut stage = reopened.atomic_stage_for_heap(heap_id).unwrap();
    assert_eq!(stage.kernel().chunk_plan(aid(), 0), Some(&chunk_plan));
    assert_eq!(
        stage
            .decide_plan_evidence(&committed_plan)
            .unwrap()
            .commit_position,
        Some(1)
    );
    let rejected_status = stage.atomic_status(second_aid()).unwrap();
    assert_eq!(rejected_status.logical, LogicalStatus::NotCommitted);
    assert_eq!(rejected_status.material, MaterialStatus::Complete);
    assert!(rejected_status.receipt.is_none());
    let rejected_replay = stage.decide_plan_evidence(&rejected_plan).unwrap();
    assert_eq!(rejected_replay, rejected);
    drop(stage);

    let next_member = third_member();
    let next_plan = plan(heap_id, std::slice::from_ref(&next_member), b"third");
    let next = reopened
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_evidence(&next_plan)
        .unwrap();
    assert_eq!(next.commit_position, Some(2));
}

#[test]
fn compact_shadow_restore_crash_cuts_are_idempotent() {
    let _guard = fp_lock();
    for failpoint in [
        "store.recovery_shadow.atomic.before_restore_file",
        "store.recovery_shadow.atomic.after_restore_file",
        "store.recovery_shadow.segment.before_restore",
        "store.recovery_shadow.segment.after_restore",
    ] {
        clear_failpoints();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("s");
        let mut store = Store::create(&root).unwrap();
        let (heap_id, closed, key_subject) = committed_plan(&mut store);
        store
            .compact_live_with(CompactOptions {
                reclaim_sources: true,
                allow_history_loss: true,
                ..CompactOptions::default()
            })
            .unwrap();
        store.abandon_for_crash_test();
        drop(store);
        erase_local_material_and_atomic_support(&root);

        arm_failpoint_once_current_thread(failpoint, FailpointAction::Error);
        let err = Store::open(&root)
            .err()
            .expect("restore cut must interrupt open");
        assert!(err.to_string().contains("failpoint"), "{failpoint}: {err}");
        clear_failpoints();

        let mut reopened = Store::open(&root).unwrap();
        assert_committed_complete(&mut reopened, heap_id, &key_subject);
        let replay = reopened
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_evidence(&closed)
            .unwrap();
        assert_eq!(replay.commit_position, Some(1), "{failpoint}");
    }
    clear_failpoints();
}

#[test]
fn corrupt_atomic_shadow_is_ignored_with_local_authority_and_refused_when_needed() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    let (heap_id, _plan, subject) = committed_plan(&mut store);
    store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    store.abandon_for_crash_test();
    drop(store);

    let bundle = root.join("recovery/shadow/atomic-authority.rsh");
    let mut bytes = fs::read(&bundle).unwrap();
    let last = bytes.last_mut().unwrap();
    *last ^= 0x80;
    fs::write(&bundle, bytes).unwrap();

    let mut healthy = Store::open(&root).expect("unused corrupt Shadow must not kneel authority");
    assert_committed_complete(&mut healthy, heap_id, &subject);
    healthy.abandon_for_crash_test();
    drop(healthy);

    erase_local_material_and_atomic_support(&root);
    let err = Store::open(&root)
        .err()
        .expect("corrupt needed Shadow must fail");
    match err {
        StoreError::AtomicStage(detail) => assert!(detail.contains("commitment mismatch")),
        other => panic!("expected Atomic Shadow refusal, got {other:?}"),
    }
    assert_eq!(
        fs::read_dir(root.join("segments"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_file())
            .count(),
        0,
        "Atomic Shadow must verify before segment restoration mutates media"
    );
}

#[test]
fn compact_shadow_external_tier_restores_in_place_and_inventory_sees_collisions() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let external = dir.path().join("external-cold");
    let (heap_id, closed, atomic_subject, output) = compact_output_with_atomic_and_ordinary(&root);
    configure_external_cold(&root, &external, true);

    let mut store = Store::open(&root).unwrap();
    store
        .transfer_segment_to_tier(output, TierClass::Cold, TierMoveMode::Move)
        .unwrap();
    let hot = root
        .join("segments")
        .join(format!("{}.residiuum", residiuum_store::hex16(&output)));
    let cold = external.join(format!("{}.residiuum", residiuum_store::hex16(&output)));
    assert!(!hot.is_file());
    assert!(cold.is_file());
    store.abandon_for_crash_test();
    drop(store);

    // A missing online external segment is reconstructed at its configured
    // canonical tier path, never silently duplicated into hot storage.
    fs::remove_file(&cold).unwrap();
    let mut restored = Store::open(&root).unwrap();
    assert!(cold.is_file());
    assert!(!hot.is_file());
    assert_committed_complete(&mut restored, heap_id, &atomic_subject);
    assert_eq!(
        restored
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_evidence(&closed)
            .unwrap()
            .commit_position,
        Some(1)
    );
    restored.abandon_for_crash_test();
    drop(restored);

    // Online external media participates in P0 collision truth.
    fs::copy(&cold, &hot).unwrap();
    let err = Store::open(&root)
        .err()
        .expect("hot plus external owner must be a collision");
    assert!(matches!(err, StoreError::SegmentIdCollision { .. }));
}

#[test]
fn atomic_checkpoint_rebuild_discovers_online_external_tier_root() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let external = dir.path().join("external-cold");
    let mut store = Store::create(&root).unwrap();
    let (heap_id, closed, atomic_subject) = committed_plan(&mut store);
    store.seal_active().unwrap();
    let atomic_segment = fs::read_dir(root.join("segments"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            fs::read(path).is_ok_and(|bytes| {
                !read_atomic_evidence(&bytes, SafetyLimits::draft_defaults())
                    .examined
                    .is_empty()
            })
        })
        .and_then(|path| residiuum_store::segment_id_from_filename(&path))
        .expect("Atomic-bearing sealed segment");
    store.abandon_for_crash_test();
    drop(store);
    configure_external_cold(&root, &external, true);

    let mut store = Store::open(&root).unwrap();
    store
        .transfer_segment_to_tier(atomic_segment, TierClass::Cold, TierMoveMode::Move)
        .unwrap();
    let hot = root.join("segments").join(format!(
        "{}.residiuum",
        residiuum_store::hex16(&atomic_segment)
    ));
    let cold = external.join(format!(
        "{}.residiuum",
        residiuum_store::hex16(&atomic_segment)
    ));
    assert!(!hot.is_file());
    assert!(cold.is_file());
    store.abandon_for_crash_test();
    drop(store);

    fs::remove_file(root.join("store-info/atomic-stage.ckpt")).unwrap();
    let mut rebuilt = Store::open(&root).unwrap();
    assert!(!hot.is_file());
    assert!(cold.is_file());
    assert_committed_complete(&mut rebuilt, heap_id, &atomic_subject);
    assert_eq!(
        rebuilt
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_evidence(&closed)
            .unwrap()
            .commit_position,
        Some(1)
    );

    let report = rebuilt
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    assert_eq!(report.phase, residiuum_store::CompactPhase::Reclaimed);
    assert!(
        !cold.is_file(),
        "external source must be physically reclaimed, not merely marked reclaimed"
    );
    rebuilt.abandon_for_crash_test();
    drop(rebuilt);

    let mut reopened = Store::open(&root).unwrap();
    assert_committed_complete(&mut reopened, heap_id, &atomic_subject);
    assert_eq!(
        reopened
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_evidence(&closed)
            .unwrap()
            .commit_position,
        Some(1)
    );
}

#[test]
fn compact_shadow_offline_external_tier_stays_an_explicit_coverage_hole() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let external = dir.path().join("external-cold");
    let (heap_id, closed, atomic_subject, output) = compact_output_with_atomic_and_ordinary(&root);
    configure_external_cold(&root, &external, true);

    let mut store = Store::open(&root).unwrap();
    store
        .transfer_segment_to_tier(output, TierClass::Cold, TierMoveMode::Move)
        .unwrap();
    let hot = root
        .join("segments")
        .join(format!("{}.residiuum", residiuum_store::hex16(&output)));
    let cold = external.join(format!("{}.residiuum", residiuum_store::hex16(&output)));
    assert!(cold.is_file());
    store.abandon_for_crash_test();
    drop(store);
    configure_external_cold(&root, &external, false);

    let mut reopened = Store::open(&root).unwrap();
    assert!(
        !hot.is_file(),
        "offline media must not be resurrected into hot"
    );
    assert!(
        cold.is_file(),
        "offline declaration must not mutate tier media"
    );
    let coverage = reopened.tier_coverage();
    assert!(coverage.is_incomplete());
    assert!(coverage.offline.contains(&TierClass::Cold));
    assert!(coverage.unavailable_segments.contains(&output));
    let ordinary = reopened
        .get_with_tier_coverage("ordinary/external")
        .unwrap();
    assert!(ordinary.value.is_none());
    assert!(!ordinary.absence_proven);

    // The independent Atomic authority generation remains exact even though
    // the tier projection is deliberately unavailable.
    assert_committed_complete(&mut reopened, heap_id, &atomic_subject);
    assert_eq!(
        reopened
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_evidence(&closed)
            .unwrap()
            .commit_position,
        Some(1)
    );
}

#[test]
fn offline_external_atomic_keeps_logical_truth_without_claiming_material() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let external = dir.path().join("external-cold");
    let mut store = Store::create(&root).unwrap();
    let (heap_id, _closed, atomic_subject) = committed_plan(&mut store);
    store.seal_active().unwrap();
    let atomic_segment = fs::read_dir(root.join("segments"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            fs::read(path).is_ok_and(|bytes| {
                !read_atomic_evidence(&bytes, SafetyLimits::draft_defaults())
                    .examined
                    .is_empty()
            })
        })
        .and_then(|path| residiuum_store::segment_id_from_filename(&path))
        .unwrap();
    store.abandon_for_crash_test();
    drop(store);
    configure_external_cold(&root, &external, true);

    let mut store = Store::open(&root).unwrap();
    store
        .transfer_segment_to_tier(atomic_segment, TierClass::Cold, TierMoveMode::Move)
        .unwrap();
    store.abandon_for_crash_test();
    drop(store);
    configure_external_cold(&root, &external, false);

    let mut reopened = Store::open(&root).unwrap();
    let hot = root.join("segments").join(format!(
        "{}.residiuum",
        residiuum_store::hex16(&atomic_segment)
    ));
    assert!(!hot.is_file());
    assert!(reopened
        .get_subject_bytes(&atomic_subject)
        .unwrap()
        .is_none());
    let status = reopened
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .atomic_status(aid())
        .unwrap();
    assert_eq!(status.logical, LogicalStatus::Committed);
    assert_eq!(status.material, MaterialStatus::CoverageIncomplete);
    assert!(status.receipt.is_none());
}

#[test]
fn scrub_is_read_only_for_terminal_atomic_authority() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let (heap_id, _plan, subject) = committed_plan(&mut store);
    store.seal_active().unwrap();
    let before = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .atomic_status(aid())
        .unwrap();
    let report = store.scrub_to_completion(ScrubOptions::default()).unwrap();
    assert!(report.cycle_completed);
    assert!(store.list_scrub_findings().unwrap().is_empty());
    let after = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .atomic_status(aid())
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(
        store.get_subject_bytes(&subject).unwrap().as_deref(),
        Some(b"secret".as_slice())
    );
}

#[test]
fn salvage_keeps_source_atomic_as_inactive_foreign_heap_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let mut store = Store::create(&src).unwrap();
    let (source_heap, _plan, subject) = committed_plan(&mut store);
    store.seal_active().unwrap();
    let report = store.salvage_to(&dst).unwrap();
    assert!(report.frames_copied > 0);
    drop(store);

    let mut salvaged = Store::open_with_options(
        &dst,
        StoreOpenOptions::default().tolerate_unidentified_inventory(),
    )
    .unwrap();
    assert_ne!(salvaged.store_id(), source_heap.to_bytes());
    assert!(salvaged.get_subject_bytes(&subject).unwrap().is_none());
    let status = salvaged
        .atomic_stage_for_heap(source_heap)
        .unwrap()
        .atomic_status(aid())
        .unwrap();
    assert_eq!(status.logical, LogicalStatus::Committed);
    assert_eq!(status.material, MaterialStatus::Complete);
}

#[test]
fn salvage_after_source_reclaim_copies_the_current_authority_generation() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let mut store = Store::create(&src).unwrap();
    let (source_heap, _plan, _subject) = committed_plan(&mut store);
    let compact = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            ..CompactOptions::default()
        })
        .unwrap();
    assert_eq!(compact.phase, residiuum_store::CompactPhase::Reclaimed);
    let report = store.salvage_to(&dst).unwrap();
    assert!(report.frames_copied > 0);
    drop(store);

    let mut salvaged = Store::open_with_options(
        &dst,
        StoreOpenOptions::default().tolerate_unidentified_inventory(),
    )
    .unwrap();
    let status = salvaged
        .atomic_stage_for_heap(source_heap)
        .unwrap()
        .atomic_status(aid())
        .unwrap();
    assert_eq!(status.logical, LogicalStatus::Committed);
    assert_eq!(status.material, MaterialStatus::Complete);
    assert!(status.receipt.is_some());
}

#[test]
fn empty_store_allows_seal_and_compact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store
        .put("ordinary", b"visible", DurabilityMode::Durable)
        .unwrap();
    store.seal_active().unwrap();
    store.compact_live().unwrap();
    assert_eq!(
        store.get("ordinary").unwrap().as_deref(),
        Some(b"visible".as_slice())
    );
}

#[test]
fn empty_stage_checkpoint_remains_quiescent_for_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store
        .put("ordinary", b"visible", DurabilityMode::Durable)
        .unwrap();
    drop(store.atomic_stage().unwrap());
    assert!(!outstanding_atomic_evidence(store.paths()).unwrap());
    drop(store);

    let mut store = Store::open(&path).unwrap();
    drop(store.atomic_stage().unwrap());
    assert!(!outstanding_atomic_evidence(store.paths()).unwrap());
    store.seal_active().unwrap();
    store.compact_live().unwrap();
}

#[test]
fn seal_compact_and_reclaim_refuse_while_outstanding() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store
        .put("ordinary", b"visible", DurabilityMode::Durable)
        .unwrap();
    stage_one(&mut store);
    assert!(outstanding_atomic_evidence(store.paths()).unwrap());

    assert_refused(store.seal_active().unwrap_err());
    assert_refused(store.compact_live().unwrap_err());
    assert_refused(
        store
            .compact_live_with(CompactOptions {
                reclaim_sources: true,
                allow_history_loss: true,
                ..CompactOptions::default()
            })
            .unwrap_err(),
    );
    assert_eq!(
        store.get("ordinary").unwrap().as_deref(),
        Some(b"visible".as_slice())
    );
    assert!(store.get("k").unwrap().is_none());
}

#[test]
fn deleting_checkpoint_cannot_unlock_reclaim_and_media_still_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store
        .put("ordinary", b"visible", DurabilityMode::Durable)
        .unwrap();
    stage_one(&mut store);
    let ckpt = atomic_stage_checkpoint_path(store.paths());
    let coord = atomic_coord_path(store.paths());
    assert!(ckpt.is_file());
    assert!(coord.is_file());
    fs::remove_file(&ckpt).unwrap();
    assert!(outstanding_atomic_evidence(store.paths()).unwrap());
    assert_refused(store.compact_live().unwrap_err());
    assert!(media_has_prepare_or_member(&path));

    drop(store);
    let mut store = Store::open(&path).unwrap();
    {
        let stage = store.atomic_stage().unwrap();
        assert!(stage.kernel().can_resolve(aid()));
    }
    assert_eq!(
        store.get("ordinary").unwrap().as_deref(),
        Some(b"visible".as_slice())
    );
    assert!(store.get("k").unwrap().is_none());
}

#[test]
fn backup_copies_checkpoint_same_identity_restore_keeps_stage() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    let dst = dir.path().join("dst");
    let mut store = Store::create(&src).unwrap();
    store
        .put("ordinary", b"visible", DurabilityMode::Durable)
        .unwrap();
    stage_one(&mut store);
    let sid = store.store_id();
    store.backup_to(&bak).unwrap();

    let ckpt_rel = format!("store-info/{ATOMIC_STAGE_CHECKPOINT_FILE}");
    let store_tree = bak.join("store");
    assert!(
        store_tree
            .join("store-info")
            .join(ATOMIC_STAGE_CHECKPOINT_FILE)
            .is_file(),
        "backup must copy {ckpt_rel}"
    );

    let restored = restore_full_backup(&bak, &dst, RestoreOptions::default()).unwrap();
    assert_eq!(restored.restored_store_id, sid);
    assert!(!restored.identity_reassigned);

    let mut opened = Store::open(&dst).unwrap();
    assert_eq!(opened.store_id(), sid);
    assert_eq!(
        opened.get("ordinary").unwrap().as_deref(),
        Some(b"visible".as_slice())
    );
    assert!(opened.get("k").unwrap().is_none());
    {
        let stage = opened.atomic_stage().unwrap();
        assert!(stage.kernel().can_resolve(aid()));
    }
}

#[test]
fn identity_reassign_clone_refuses_while_outstanding() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    let clone = dir.path().join("clone");
    let mut store = Store::create(&src).unwrap();
    store
        .put("ordinary", b"visible", DurabilityMode::Durable)
        .unwrap();
    stage_one(&mut store);
    store.backup_to(&bak).unwrap();

    match restore_full_backup(
        &bak,
        &clone,
        RestoreOptions {
            reassign_identity: true,
        },
    ) {
        Err(StoreError::AtomicStage(detail)) => {
            assert!(
                detail.contains("identity-reassign") || detail.contains("outstanding Atomic"),
                "{detail}"
            );
        }
        other => panic!("expected clone refuse, got {other:?}"),
    }
    assert!(!clone.join("store-info").is_dir());
}
