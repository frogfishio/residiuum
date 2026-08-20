//! CR-ATMR6-006: freeze staging records; fail-closed maintenance.

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId,
    CoordinationScope, HeapId, LogicalStatus, MaterialStatus, MutationKind, ObjectIdentity,
    PlanMutation, ResourceLimits, VersionId,
};
use residiuum_format::{
    encode_subject_v2, read_atomic_evidence, AtomicEvidenceClass, AtomicFrameRole, SafetyLimits,
    SubjectObjectKind,
};
use residiuum_store::{
    atomic_coord_path, atomic_stage_checkpoint_path, outstanding_atomic_evidence,
    restore_full_backup, CompactOptions, DurabilityMode, RestoreOptions, ScrubOptions, Store,
    StoreError, StoreOpenOptions, TierClass, TierMoveMode, ATOMIC_STAGE_CHECKPOINT_FILE,
};
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
    let (heap_id, plan, subject) = committed_plan(&mut store);

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
        .decide_plan_evidence(&plan)
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
fn terminal_atomic_reclaim_and_identity_reassign_refuse_before_destination_publish() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    let clone = dir.path().join("clone");
    let mut store = Store::create(&src).unwrap();
    let (_heap_id, _plan, _subject) = committed_plan(&mut store);
    let jobs_before = store.list_compact_jobs().unwrap().len();
    assert_refused(
        store
            .compact_live_with(CompactOptions {
                reclaim_sources: true,
                allow_history_loss: true,
                ..CompactOptions::default()
            })
            .unwrap_err(),
    );
    assert_eq!(store.list_compact_jobs().unwrap().len(), jobs_before);

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
fn recovery_shadow_transition_refuses_before_value_only_representation_can_erase_authority() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let (heap_id, _plan, subject) = committed_plan(&mut store);
    store.seal_active().unwrap();
    assert_refused(store.prepare_flip_to_compact_shadow().unwrap_err());
    assert_committed_complete(&mut store, heap_id, &subject);
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    assert_committed_complete(&mut reopened, heap_id, &subject);
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
