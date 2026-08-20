//! CR-ATMR6-007: crash-media Atomic staging I/O matrix.
//!
//! Each cell panics at a named persist boundary (`FailpointAction::Panic`).
//! `Store::drop` skips orderly close while panicking, so the on-disk image is
//! a crash-media snapshot rather than a clean API-error + close. Optional
//! mutants then remove unsynced effects (omit last active tail, omit published
//! checkpoint, leftover short-write).
//!
//! `Scenario::Member` drives `store.atomic.member.*` during `begin_prepare`
//! (the member `ItemEvent` frame). Payload sidecars are a separate scenario.
//!
//! Every cell has one reviewed projection. Ordinary get/scan stay empty.

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, ChunkPlan,
    CollectionId, CoordinationScope, HeapId, MutationKind, ObjectIdentity, PlanMutation,
    ResourceLimits, VersionId,
};
use residiuum_store::{
    arm_failpoint_once, clear_failpoints, enable_failpoint_hit_proof, require_failpoint_visited,
    AtomicStageClass, FailpointAction, Store, StoreError,
};
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::process::Command;
use std::sync::Mutex;

static SERIAL: Mutex<()> = Mutex::new(());

const FRONTIER: [u8; 32] = [0xA1; 32];
const PAYLOAD: &[u8] = b"secret";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Expect {
    class: AtomicStageClass,
    members: u32,
    payloads: u32,
    plans: u32,
    chunks: u32,
    sealed: bool,
}

impl Expect {
    const fn absent() -> Self {
        Self {
            class: AtomicStageClass::Absent,
            members: 0,
            payloads: 0,
            plans: 0,
            chunks: 0,
            sealed: false,
        }
    }

    const fn prepared(members: u32, payloads: u32) -> Self {
        Self {
            class: AtomicStageClass::Prepared,
            members,
            payloads,
            plans: 0,
            chunks: 0,
            sealed: false,
        }
    }

    const fn staged() -> Self {
        Self {
            class: AtomicStageClass::Staged,
            members: 1,
            payloads: 1,
            plans: 0,
            chunks: 0,
            sealed: false,
        }
    }

    const fn sealed() -> Self {
        Self {
            class: AtomicStageClass::Sealed,
            members: 1,
            payloads: 1,
            plans: 0,
            chunks: 0,
            sealed: true,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Prepare,
    Member,
    Payload,
    ChunkPlan,
    ChunkBody,
    Seal,
    Checkpoint,
    Coordinator,
}

#[derive(Clone, Copy, Debug)]
enum Boundary {
    BeforeWrite,
    AfterWrite,
    AfterFileSync,
    AfterCheckpoint,
}

#[derive(Clone, Copy, Debug)]
enum Mutant {
    Keep,
    OmitUnsyncedTail,
    OmitCheckpointPublish,
    ShortWriteCheckpoint,
}

fn serial() -> std::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    guard
}

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

fn fp_name(scenario: Scenario, boundary: Boundary) -> &'static str {
    match (scenario, boundary) {
        (Scenario::Prepare, Boundary::BeforeWrite) => "store.atomic.prepare.before_append",
        (Scenario::Prepare, Boundary::AfterWrite) => "store.active.write_tail.after_write",
        (Scenario::Prepare, Boundary::AfterFileSync) => "store.active.write_tail.after_sync",
        (Scenario::Prepare, Boundary::AfterCheckpoint) => "store.atomic.prepare.after_checkpoint",
        (Scenario::Member, Boundary::BeforeWrite) => "store.atomic.member.before_append",
        (Scenario::Member, Boundary::AfterWrite) => "store.active.write_tail.after_write",
        (Scenario::Member, Boundary::AfterFileSync) => "store.active.write_tail.after_sync",
        (Scenario::Member, Boundary::AfterCheckpoint) => "store.atomic.member.after_checkpoint",
        (Scenario::Payload, Boundary::BeforeWrite) => "store.atomic.payload.before_append",
        (Scenario::Payload, Boundary::AfterWrite) => "store.active.write_tail.after_write",
        (Scenario::Payload, Boundary::AfterFileSync) => "store.active.write_tail.after_sync",
        (Scenario::Payload, Boundary::AfterCheckpoint) => "store.atomic.payload.after_checkpoint",
        (Scenario::ChunkPlan, Boundary::BeforeWrite) => "store.atomic.chunk_plan.before_append",
        (Scenario::ChunkPlan, Boundary::AfterWrite) => "store.active.write_tail.after_write",
        (Scenario::ChunkPlan, Boundary::AfterFileSync) => "store.active.write_tail.after_sync",
        (Scenario::ChunkPlan, Boundary::AfterCheckpoint) => {
            "store.atomic.chunk_plan.after_checkpoint"
        }
        (Scenario::ChunkBody, Boundary::BeforeWrite) => "store.atomic.chunk_body.before_append",
        (Scenario::ChunkBody, Boundary::AfterWrite) => "store.active.write_tail.after_write",
        (Scenario::ChunkBody, Boundary::AfterFileSync) => "store.active.write_tail.after_sync",
        (Scenario::ChunkBody, Boundary::AfterCheckpoint) => {
            "store.atomic.chunk_body.after_checkpoint"
        }
        (Scenario::Seal, Boundary::BeforeWrite) => "store.atomic.seal.before_append",
        (Scenario::Seal, Boundary::AfterWrite) => "store.active.write_tail.after_write",
        (Scenario::Seal, Boundary::AfterFileSync) => "store.active.write_tail.after_sync",
        (Scenario::Seal, Boundary::AfterCheckpoint) => "store.atomic.seal.after_checkpoint",
        (Scenario::Checkpoint, Boundary::BeforeWrite) => "store.atomic.checkpoint.before_persist",
        (Scenario::Checkpoint, Boundary::AfterWrite) => "store.atomic.checkpoint.after_write",
        (Scenario::Checkpoint, Boundary::AfterFileSync) => {
            "store.atomic.checkpoint.after_file_sync"
        }
        (Scenario::Checkpoint, Boundary::AfterCheckpoint) => {
            "store.atomic.checkpoint.after_dir_sync"
        }
        (Scenario::Coordinator, Boundary::BeforeWrite) => "store.atomic.coord.before_persist",
        (Scenario::Coordinator, Boundary::AfterWrite) => "store.atomic.coord.after_write",
        (Scenario::Coordinator, Boundary::AfterFileSync) => "store.atomic.coord.after_file_sync",
        (Scenario::Coordinator, Boundary::AfterCheckpoint) => "store.atomic.coord.after_dir_sync",
    }
}

fn expected(scenario: Scenario, boundary: Boundary, mutant: Mutant) -> Expect {
    match (scenario, boundary, mutant) {
        (Scenario::Prepare, Boundary::BeforeWrite, _) => Expect::absent(),
        (
            Scenario::Prepare,
            Boundary::AfterWrite | Boundary::AfterFileSync,
            Mutant::OmitUnsyncedTail,
        ) => Expect::absent(),
        (Scenario::Prepare, _, _) => Expect::prepared(0, 0),

        (Scenario::Member, Boundary::BeforeWrite, _) => Expect::prepared(0, 0),
        (
            Scenario::Member,
            Boundary::AfterWrite | Boundary::AfterFileSync,
            Mutant::OmitUnsyncedTail,
        ) => Expect::prepared(0, 0),
        (Scenario::Member, _, _) => Expect::prepared(1, 0),

        (Scenario::Payload, Boundary::BeforeWrite, _) => Expect::prepared(1, 0),
        (Scenario::Payload, Boundary::AfterWrite, Mutant::OmitUnsyncedTail) => {
            Expect::prepared(1, 0)
        }
        (Scenario::Payload, Boundary::AfterWrite, _) => Expect::staged(),
        (Scenario::Payload, Boundary::AfterFileSync, Mutant::OmitUnsyncedTail) => {
            Expect::prepared(1, 0)
        }
        (Scenario::Payload, _, _) => Expect::staged(),

        (Scenario::ChunkPlan, Boundary::BeforeWrite, _) => Expect::prepared(1, 0),
        (Scenario::ChunkPlan, _, Mutant::OmitUnsyncedTail)
            if !matches!(boundary, Boundary::AfterCheckpoint) =>
        {
            Expect::prepared(1, 0)
        }
        (Scenario::ChunkPlan, _, _) => Expect {
            class: AtomicStageClass::Prepared,
            members: 1,
            payloads: 0,
            plans: 1,
            chunks: 0,
            sealed: false,
        },

        (Scenario::ChunkBody, Boundary::BeforeWrite, _) => Expect {
            class: AtomicStageClass::Prepared,
            members: 1,
            payloads: 0,
            plans: 1,
            chunks: 0,
            sealed: false,
        },
        (Scenario::ChunkBody, _, Mutant::OmitUnsyncedTail)
            if !matches!(boundary, Boundary::AfterCheckpoint) =>
        {
            Expect {
                class: AtomicStageClass::Prepared,
                members: 1,
                payloads: 0,
                plans: 1,
                chunks: 0,
                sealed: false,
            }
        }
        (Scenario::ChunkBody, _, _) => Expect {
            class: AtomicStageClass::Prepared,
            members: 1,
            payloads: 0,
            plans: 1,
            chunks: 1,
            sealed: false,
        },

        (Scenario::Seal, Boundary::BeforeWrite, _) => Expect::staged(),
        (
            Scenario::Seal,
            Boundary::AfterWrite | Boundary::AfterFileSync,
            Mutant::OmitUnsyncedTail,
        ) => Expect::staged(),
        (Scenario::Seal, _, _) => Expect::sealed(),

        // Seal is installed in the live catalogue before checkpoint persist.
        // A crash-media reopen at these control-file boundaries still projects
        // Staged: recovery trusts the last authenticated sidecar, which this
        // panic interrupts before the sealed catalogue is the recovered view.
        (Scenario::Checkpoint, _, _) => Expect::staged(),

        (Scenario::Coordinator, _, _) => Expect::staged(),
    }
}

fn assert_no_ordinary_leak(store: &Store) {
    assert_eq!(store.get("k").unwrap(), None);
    let scan = store.scan_live_logical().unwrap();
    assert!(!scan.entries.iter().any(|(s, _)| s == b"k" || s == PAYLOAD));
}

fn classify(store: &mut Store) -> Expect {
    assert_no_ordinary_leak(store);
    let stage = store
        .atomic_stage()
        .expect("atomic stage opens after crash");
    let st = stage.examine(aid());
    Expect {
        class: st.class,
        members: st.present_members,
        payloads: st.present_payloads,
        plans: st.present_chunk_plans,
        chunks: st.present_chunk_bodies,
        sealed: st.sealed,
    }
}

fn setup(scenario: Scenario, store: &mut Store) {
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m));
    let mut stage = store.atomic_stage().unwrap();
    if matches!(scenario, Scenario::Prepare | Scenario::Member) {
        return;
    }
    stage
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    match scenario {
        Scenario::ChunkPlan => {}
        Scenario::ChunkBody => {
            let (map, _, _) = chunks();
            stage.commit_chunk_manifest(aid(), 0, map).unwrap();
        }
        Scenario::Payload => {}
        Scenario::Seal | Scenario::Checkpoint | Scenario::Coordinator => {
            stage.append_staged(m, PAYLOAD.to_vec()).unwrap();
        }
        Scenario::Prepare | Scenario::Member => unreachable!(),
    }
}

fn run_op(scenario: Scenario, store: &mut Store) -> Result<(), StoreError> {
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m));
    let mut stage = store.atomic_stage().unwrap();
    match scenario {
        Scenario::Prepare | Scenario::Member => stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .map(|_| ()),
        Scenario::Payload => stage.append_staged(m, PAYLOAD.to_vec()),
        Scenario::ChunkPlan => {
            let (map, _, _) = chunks();
            stage.commit_chunk_manifest(aid(), 0, map)
        }
        Scenario::ChunkBody => {
            let (_, p0, _) = chunks();
            stage.append_chunk(m, 0, p0.to_vec())
        }
        Scenario::Seal | Scenario::Checkpoint | Scenario::Coordinator => {
            stage.seal_member_boundary(aid())
        }
    }
}

fn apply_mutant(root: &std::path::Path, active_len_before: u64, mutant: Mutant) {
    match mutant {
        Mutant::Keep => {}
        Mutant::OmitUnsyncedTail => {
            if let Some(path) = find_active_file(root) {
                let _ = fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .and_then(|f| f.set_len(active_len_before));
            }
        }
        Mutant::OmitCheckpointPublish => {
            let ckpt = root.join("store-info").join("atomic-stage.ckpt");
            if ckpt.is_file() {
                let _ = fs::remove_file(ckpt);
            }
        }
        Mutant::ShortWriteCheckpoint => {
            let ckpt = root.join("store-info").join("atomic-stage.ckpt");
            if let Ok(meta) = fs::metadata(&ckpt) {
                let n = meta.len().saturating_sub(32);
                let _ = fs::OpenOptions::new()
                    .write(true)
                    .open(&ckpt)
                    .and_then(|f| f.set_len(n));
            }
        }
    }
}

fn find_active_file(root: &std::path::Path) -> Option<std::path::PathBuf> {
    let dir = root.join("active");
    let legacy = dir.join("active.residiuum");
    if legacy.is_file() {
        return Some(legacy);
    }
    let Ok(entries) = fs::read_dir(&dir) else {
        return None;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.extension().and_then(|e| e.to_str()) == Some("residiuum") && p.is_file() {
            return Some(p);
        }
    }
    None
}

fn active_len(root: &std::path::Path) -> u64 {
    find_active_file(root)
        .and_then(|p| fs::metadata(p).ok())
        .map(|m| m.len())
        .unwrap_or(0)
}

fn drive(scenario: Scenario, boundary: Boundary, mutant: Mutant) -> Expect {
    let _g = serial();
    enable_failpoint_hit_proof();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    setup(scenario, &mut store);
    let before = active_len(&path);
    let name = fp_name(scenario, boundary);
    arm_failpoint_once(name, FailpointAction::Panic);
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = run_op(scenario, &mut store);
    }))
    .is_err();
    assert!(
        panicked,
        "{scenario:?} {boundary:?} {mutant:?} must crash at {name}"
    );
    require_failpoint_visited(name);
    clear_failpoints();
    store.abandon_for_crash_test();
    drop(store);
    apply_mutant(&path, before, mutant);
    let mut store = Store::open(&path).expect("reopen crash-media image");
    classify(&mut store)
}

fn prefix_cells() -> Vec<(Scenario, Boundary, Mutant)> {
    let mut out = Vec::new();
    for scenario in [
        Scenario::Prepare,
        Scenario::Member,
        Scenario::Payload,
        Scenario::ChunkPlan,
        Scenario::ChunkBody,
        Scenario::Seal,
        Scenario::Checkpoint,
        Scenario::Coordinator,
    ] {
        for boundary in [
            Boundary::BeforeWrite,
            Boundary::AfterWrite,
            Boundary::AfterFileSync,
            Boundary::AfterCheckpoint,
        ] {
            // begin_prepare emits Prepare and Member in one API call. There is
            // no member-scoped lower-level write hook, so do not mislabel the
            // prepare write as a member write/sync boundary.
            if matches!(scenario, Scenario::Member)
                && matches!(boundary, Boundary::AfterWrite | Boundary::AfterFileSync)
            {
                continue;
            }
            out.push((scenario, boundary, Mutant::Keep));
        }
    }
    out
}

#[test]
fn crash_media_prefix_matrix_has_exact_projections() {
    for (scenario, boundary, mutant) in prefix_cells() {
        let got = drive(scenario, boundary, mutant);
        let want = expected(scenario, boundary, mutant);
        assert_eq!(
            got, want,
            "{scenario:?} {boundary:?} {mutant:?} => {got:?} want {want:?}"
        );
    }
}

#[test]
fn crash_media_matrix_is_deterministic_on_repeat() {
    for (scenario, boundary, mutant) in [
        (Scenario::Prepare, Boundary::AfterFileSync, Mutant::Keep),
        (Scenario::Member, Boundary::AfterCheckpoint, Mutant::Keep),
        (Scenario::Payload, Boundary::AfterFileSync, Mutant::Keep),
        (Scenario::Seal, Boundary::AfterCheckpoint, Mutant::Keep),
    ] {
        let a = drive(scenario, boundary, mutant);
        let b = drive(scenario, boundary, mutant);
        assert_eq!(a, b, "repeat {scenario:?} {boundary:?}");
        assert_eq!(a, expected(scenario, boundary, mutant));
    }
}

#[test]
fn omit_sync_and_short_write_mutants_fail_semantically() {
    assert_eq!(
        drive(
            Scenario::Prepare,
            Boundary::AfterFileSync,
            Mutant::OmitUnsyncedTail
        ),
        Expect::absent()
    );
    assert_eq!(
        drive(Scenario::Prepare, Boundary::AfterFileSync, Mutant::Keep),
        Expect::prepared(0, 0)
    );
    assert_eq!(
        drive(
            Scenario::Payload,
            Boundary::AfterFileSync,
            Mutant::OmitUnsyncedTail
        ),
        Expect::prepared(1, 0)
    );
    assert_eq!(
        drive(Scenario::Payload, Boundary::AfterFileSync, Mutant::Keep),
        Expect::staged()
    );
    assert_eq!(
        drive(
            Scenario::Seal,
            Boundary::AfterFileSync,
            Mutant::OmitUnsyncedTail
        ),
        Expect::staged()
    );
    assert_eq!(
        drive(Scenario::Seal, Boundary::AfterFileSync, Mutant::Keep),
        Expect::sealed()
    );
    assert_eq!(
        drive(
            Scenario::Seal,
            Boundary::AfterCheckpoint,
            Mutant::OmitCheckpointPublish
        ),
        Expect::sealed()
    );
    assert_eq!(
        drive(
            Scenario::Payload,
            Boundary::AfterCheckpoint,
            Mutant::ShortWriteCheckpoint
        ),
        Expect::staged()
    );
}

#[test]
fn payload_after_write_before_sync_omit_tail_is_prepared() {
    assert_eq!(
        drive(
            Scenario::Payload,
            Boundary::AfterWrite,
            Mutant::OmitUnsyncedTail
        ),
        Expect::prepared(1, 0)
    );
}

#[test]
fn compaction_stays_refused_and_does_not_leak() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store
        .put(
            "ordinary",
            b"visible",
            residiuum_store::DurabilityMode::Durable,
        )
        .unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m));
    {
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage.append_staged(m, PAYLOAD.to_vec()).unwrap();
    }
    match store.compact_live() {
        Err(StoreError::AtomicStage(detail)) => {
            assert!(detail.contains("outstanding Atomic"), "{detail}");
        }
        other => panic!("compact must refuse, got {other:?}"),
    }
    assert_eq!(
        store.get("ordinary").unwrap().as_deref(),
        Some(b"visible".as_slice())
    );
    assert_no_ordinary_leak(&store);
}

#[test]
fn member_frame_is_not_the_payload_sidecar() {
    assert_eq!(
        fp_name(Scenario::Member, Boundary::AfterCheckpoint),
        "store.atomic.member.after_checkpoint"
    );
    assert_eq!(
        fp_name(Scenario::Payload, Boundary::AfterFileSync),
        "store.active.write_tail.after_sync"
    );
    assert_eq!(
        drive(Scenario::Member, Boundary::AfterCheckpoint, Mutant::Keep),
        Expect::prepared(1, 0)
    );
}

#[test]
fn subprocess_abort_matches_synced_prepare_projection() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    drop(Store::create(&path).unwrap());

    let bin = std::env::var_os("CARGO_BIN_EXE_residiuum_store_crash_child")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let mut bin = std::env::current_exe().unwrap();
            bin.pop();
            if bin.file_name().and_then(|name| name.to_str()) == Some("deps") {
                bin.pop();
            }
            bin.push("residiuum-store-crash-child");
            bin
        });
    assert!(bin.is_file(), "crash child missing at {}", bin.display());
    let status = Command::new(&bin)
        .env("RESIDIUUM_CRASH_STORE", &path)
        .env("RESIDIUUM_CRASH_OP", "atomic_prepare")
        .env("RESIDIUUM_CRASH_FP", "store.active.write_tail.after_sync")
        .status()
        .unwrap();
    assert!(!status.success(), "abort failpoint must kill child");

    let mut store = Store::open(&path).unwrap();
    assert_eq!(classify(&mut store), Expect::prepared(0, 0));
}
