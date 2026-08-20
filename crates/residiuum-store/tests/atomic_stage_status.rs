//! CR-ATMR6-005 / ATM-4: surviving prepare is terminally recovery-aborted.

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId,
    CoordinationScope, HeapId, LogicalStatus, MutationKind, ObjectIdentity, PlanMutation,
    ResourceLimits, VersionId,
};
use residiuum_format::{
    examine_atomic_frame, scan_forward, AtomicEvidenceClass, AtomicFrameRole, SafetyLimits,
};
use residiuum_store::{
    arm_failpoint_once, clear_failpoints, AtomicDetailRetentionPolicy, AtomicStageClass,
    FailpointAction, Store, StoreError,
};
use std::fs;
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

fn vid(n: u8) -> VersionId {
    let mut b = [0u8; 16];
    b[0] = n;
    VersionId::from_bytes(b).unwrap()
}

fn member(ordinal: u32, ev: u8, value: &[u8]) -> AtomicMember {
    AtomicMember {
        atomic_id: aid(),
        ordinal,
        object_identity: ObjectIdentity::new(cid(), CanonicalKey::String(format!("k{ordinal}"))),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(value).as_bytes()),
        event_id: vid(ev),
    }
}

fn plan(heap: HeapId, members: &[AtomicMember], values: &[&[u8]]) -> AtomicPlan {
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
            .zip(values.iter())
            .map(|(m, value)| PlanMutation {
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

fn media_has_prepare(store: &Store) -> bool {
    let mut bytes = Vec::new();
    for dir in [store.paths().active_dir(), store.paths().segments_dir()] {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if let Ok(part) = fs::read(entry.path()) {
                bytes.extend_from_slice(&part);
            }
        }
    }
    scan_forward(&bytes, SafetyLimits::draft_defaults())
        .verified_frames()
        .any(|(_, frame)| {
            matches!(
                examine_atomic_frame(frame),
                Some(AtomicEvidenceClass::Valid(link)) if link.role == AtomicFrameRole::Prepare
            )
        })
}

#[test]
fn after_prepare_interrupt_is_recovery_aborted_not_absent() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member(0, 3, b"secret");
    let p = plan(heap, std::slice::from_ref(&m), &[b"secret"]);
    arm_failpoint_once(
        "store.atomic.prepare.after_checkpoint",
        FailpointAction::Error,
    );
    {
        let mut stage = store.atomic_stage().unwrap();
        assert!(stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .is_err());
    }
    clear_failpoints();
    drop(store);
    let mut store = Store::open(&path).unwrap();
    assert!(media_has_prepare(&store));
    let stage = store.atomic_stage().unwrap();
    let st = stage.examine(aid());
    assert_eq!(st.class, AtomicStageClass::NotCommitted);
    assert_eq!(st.present_members, 0);
    assert!(stage.kernel().placement(aid()).is_none());
}

#[test]
fn after_members_without_payload_is_recovery_aborted() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m0 = member(0, 3, b"one");
    let m1 = member(1, 4, b"two");
    let p = plan(heap, &[m0.clone(), m1.clone()], &[b"one", b"two"]);
    {
        let mut stage = store.atomic_stage().unwrap();
        stage.begin_prepare(&p, FRONTIER, &[m0, m1]).unwrap();
        let st = stage.examine(aid());
        assert_eq!(st.class, AtomicStageClass::Prepared);
        assert_eq!(st.intended_members, 2);
        assert_eq!(st.present_members, 2);
        assert_eq!(st.present_payloads, 0);
    }
    drop(store);
    let mut store = Store::open(dir.path().join("s")).unwrap();
    let stage = store.atomic_stage().unwrap();
    let st = stage.examine(aid());
    assert_eq!(st.class, AtomicStageClass::NotCommitted);
    assert_eq!(st.present_members, 2);
}

#[test]
fn incomplete_id_cannot_be_reused() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member(0, 3, b"secret");
    let p = plan(heap, std::slice::from_ref(&m), &[b"secret"]);
    arm_failpoint_once(
        "store.atomic.prepare.after_checkpoint",
        FailpointAction::Error,
    );
    {
        let mut stage = store.atomic_stage().unwrap();
        let _ = stage.begin_prepare(&p, FRONTIER, std::slice::from_ref(&m));
    }
    clear_failpoints();
    drop(store);
    let mut store = Store::open(&path).unwrap();
    let other = [0xB2; 32];
    let p2 = plan(heap, std::slice::from_ref(&m), &[b"secret"]);
    let mut stage = store.atomic_stage().unwrap();
    match stage.begin_prepare(&p2, other, std::slice::from_ref(&m)) {
        Err(StoreError::AtomicStage(msg)) => {
            assert!(
                msg.to_lowercase().contains("conflict") || msg.contains("AtomicId"),
                "incomplete ID must not be reusable, got {msg}"
            );
        }
        Ok(_) => panic!("reusing an issued prepare ID must refuse"),
        Err(other) => panic!("expected AtomicIdConflict, got {other}"),
    }
}

#[test]
fn no_prepare_is_the_only_absence() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let stage = store.atomic_stage().unwrap();
    assert_eq!(stage.examine(aid()).class, AtomicStageClass::Absent);
    assert!(!media_has_prepare(&store));
}

#[test]
fn tombstone_survives_lawful_detail_retirement_restart_and_replay() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member(0, 3, b"secret");
    let p = plan(heap, std::slice::from_ref(&m), &[b"secret"]);
    arm_failpoint_once(
        "store.atomic.prepare.after_checkpoint",
        FailpointAction::Error,
    );
    {
        let mut stage = store.atomic_stage().unwrap();
        let _ = stage.begin_prepare(&p, FRONTIER, std::slice::from_ref(&m));
    }
    clear_failpoints();
    drop(store);

    let mut store = Store::open(&path).unwrap();
    {
        let mut stage = store.atomic_stage().unwrap();
        assert_eq!(
            stage.atomic_status(aid()).unwrap().logical,
            LogicalStatus::NotCommitted
        );
        assert!(
            stage
                .retire_not_committed_detail_at(
                    aid(),
                    u64::MAX,
                    AtomicDetailRetentionPolicy::default(),
                )
                .unwrap()
        );
        assert_eq!(stage.examine(aid()).class, AtomicStageClass::NotCommitted);
        assert_eq!(
            stage.atomic_status(aid()).unwrap().logical,
            LogicalStatus::NotCommitted
        );
        assert!(matches!(
            stage.decide_plan_outcome(&p).unwrap(),
            residiuum_atomics::AtomicOutcome::NotCommitted { .. }
        ));
    }
    drop(store);

    let mut store = Store::open(&path).unwrap();
    let mut stage = store.atomic_stage().unwrap();
    assert_eq!(
        stage.examine(aid()).class,
        AtomicStageClass::NotCommitted,
        "findings={:?}",
        stage.findings()
    );
    assert_eq!(
        stage.atomic_status(aid()).unwrap().logical,
        LogicalStatus::NotCommitted
    );
    assert!(matches!(
        stage.decide_plan_outcome(&p).unwrap(),
        residiuum_atomics::AtomicOutcome::NotCommitted { .. }
    ));
    assert!(
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .is_err(),
        "the low-level prepare surface must not reissue a tombstoned identity"
    );
    let changed_member = member(0, 4, b"changed");
    let changed = plan(heap, std::slice::from_ref(&changed_member), &[b"changed"]);
    assert!(
        stage.decide_plan_outcome(&changed).is_err(),
        "same ID with a different root must conflict after detail retirement"
    );
    drop(stage);
    drop(store);

    // The index is derived. Corruption invalidates the checkpoint and forces
    // reconstruction from ATTOMB1 media; it must never turn the decision into
    // absence.
    let index_path = path.join("store-info/atomic-tombstones.idx");
    let mut bytes = fs::read(&index_path).unwrap();
    bytes[10] ^= 0x80;
    fs::write(&index_path, bytes).unwrap();
    let mut store = Store::open(&path).unwrap();
    let stage = store.atomic_stage().unwrap();
    assert_eq!(
        stage.atomic_status(aid()).unwrap().logical,
        LogicalStatus::NotCommitted
    );
    assert_ne!(stage.examine(aid()).class, AtomicStageClass::Absent);
}

#[test]
fn index_pages_durable_before_checkpoint_leave_the_prior_root_recoverable() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member(0, 3, b"secret");
    let p = plan(heap, std::slice::from_ref(&m), &[b"secret"]);
    arm_failpoint_once(
        "store.atomic.prepare.after_checkpoint",
        FailpointAction::Error,
    );
    {
        let mut stage = store.atomic_stage().unwrap();
        let _ = stage.begin_prepare(&p, FRONTIER, std::slice::from_ref(&m));
    }
    clear_failpoints();
    drop(store);

    arm_failpoint_once(
        "store.atomic.tombstone_index.after_sync",
        FailpointAction::Error,
    );
    assert!(Store::open(&path).is_err());
    clear_failpoints();

    let mut store = Store::open(&path).unwrap();
    let stage = store.atomic_stage().unwrap();
    assert_eq!(
        stage.atomic_status(aid()).unwrap().logical,
        LogicalStatus::NotCommitted
    );
    assert_ne!(stage.examine(aid()).class, AtomicStageClass::Absent);
}

#[test]
fn detail_retirement_obeys_minimum_window_and_legal_hold() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member(0, 3, b"secret");
    let p = plan(heap, std::slice::from_ref(&m), &[b"secret"]);
    arm_failpoint_once(
        "store.atomic.prepare.after_checkpoint",
        FailpointAction::Error,
    );
    {
        let mut stage = store.atomic_stage().unwrap();
        let _ = stage.begin_prepare(&p, FRONTIER, std::slice::from_ref(&m));
    }
    clear_failpoints();
    drop(store);

    let mut store = Store::open(&path).unwrap();
    let mut stage = store.atomic_stage().unwrap();
    assert!(stage
        .retire_not_committed_detail_at(aid(), 0, AtomicDetailRetentionPolicy::default())
        .is_err());
    assert!(stage
        .retire_not_committed_detail_at(
            aid(),
            u64::MAX,
            AtomicDetailRetentionPolicy {
                legal_hold: true,
                ..AtomicDetailRetentionPolicy::default()
            },
        )
        .is_err());
    assert_eq!(stage.examine(aid()).class, AtomicStageClass::NotCommitted);
}

#[test]
fn detail_retention_uses_the_strongest_obligation() {
    let policy = AtomicDetailRetentionPolicy {
        configured_secs: 1,
        heap_history_until_unix_s: 50,
        rre_evidence_until_unix_s: 80,
        backup_until_unix_s: 70,
        legal_hold: false,
    };
    assert_eq!(policy.retain_until(10), Some(10 + 90 * 24 * 60 * 60));

    let stronger = AtomicDetailRetentionPolicy {
        rre_evidence_until_unix_s: 20_000_000,
        ..policy
    };
    assert_eq!(stronger.retain_until(10), Some(20_000_000));
    assert_eq!(
        AtomicDetailRetentionPolicy {
            legal_hold: true,
            ..stronger
        }
        .retain_until(10),
        None
    );
}
