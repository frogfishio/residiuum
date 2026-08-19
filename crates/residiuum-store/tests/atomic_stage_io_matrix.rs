//! CR-ATMR5-009: store-authority Atomic staging I/O prefixes.

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, ChunkPlan,
    CollectionId, CoordinationScope, HeapId, MutationKind, ObjectIdentity, PlanMutation,
    ResourceLimits, VersionId,
};
use residiuum_store::{
    arm_failpoint_once, clear_failpoints, enable_failpoint_hit_proof, require_failpoint_visited,
    AtomicStageClass, FailpointAction, Store, StoreError,
};
use std::sync::Mutex;

static SERIAL: Mutex<()> = Mutex::new(());

const FRONTIER: [u8; 32] = [0xA1; 32];
const PAYLOAD: &[u8] = b"secret";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Absence,
    PreparedInvisible,
    StagedInvisible,
    DurableInvisible,
    Damage,
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Prepare,
    Member,
    ChunkPlan,
    ChunkBody,
    Seal,
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    BeforeAppend,
    AfterAppend,
    AfterCheckpoint,
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

fn fp_name(scenario: Scenario, phase: Phase) -> &'static str {
    match (scenario, phase) {
        (Scenario::Prepare, Phase::BeforeAppend) => "store.atomic.prepare.before_append",
        (Scenario::Prepare, Phase::AfterAppend) => "store.atomic.prepare.after_append",
        (Scenario::Prepare, Phase::AfterCheckpoint) => "store.atomic.prepare.after_checkpoint",
        (Scenario::Member, Phase::BeforeAppend) => "store.atomic.payload.before_append",
        (Scenario::Member, Phase::AfterAppend) => "store.atomic.payload.after_append",
        (Scenario::Member, Phase::AfterCheckpoint) => "store.atomic.payload.after_checkpoint",
        (Scenario::ChunkPlan, Phase::BeforeAppend) => "store.atomic.chunk_plan.before_append",
        (Scenario::ChunkPlan, Phase::AfterAppend) => "store.atomic.chunk_plan.after_append",
        (Scenario::ChunkPlan, Phase::AfterCheckpoint) => "store.atomic.chunk_plan.after_checkpoint",
        (Scenario::ChunkBody, Phase::BeforeAppend) => "store.atomic.chunk_body.before_append",
        (Scenario::ChunkBody, Phase::AfterAppend) => "store.atomic.chunk_body.after_append",
        (Scenario::ChunkBody, Phase::AfterCheckpoint) => "store.atomic.chunk_body.after_checkpoint",
        (Scenario::Seal, Phase::BeforeAppend) => "store.atomic.seal.before_append",
        (Scenario::Seal, Phase::AfterAppend) => "store.atomic.seal.after_append",
        (Scenario::Seal, Phase::AfterCheckpoint) => "store.atomic.seal.after_checkpoint",
    }
}

fn allowed(scenario: Scenario, phase: Phase) -> &'static [Outcome] {
    use Outcome::*;
    match (scenario, phase) {
        (Scenario::Prepare, Phase::BeforeAppend) => &[Absence],
        (Scenario::Prepare, Phase::AfterAppend | Phase::AfterCheckpoint) => &[PreparedInvisible],
        (Scenario::Member, Phase::BeforeAppend) => &[PreparedInvisible],
        (Scenario::Member, Phase::AfterAppend | Phase::AfterCheckpoint) => &[StagedInvisible],
        (Scenario::ChunkPlan, _) => &[PreparedInvisible],
        (Scenario::ChunkBody, _) => &[PreparedInvisible],
        (Scenario::Seal, Phase::BeforeAppend) => &[StagedInvisible],
        (Scenario::Seal, Phase::AfterAppend | Phase::AfterCheckpoint) => {
            &[DurableInvisible, StagedInvisible]
        }
    }
}

fn assert_no_ordinary_leak(store: &Store) {
    assert_eq!(store.get("k").unwrap(), None);
    let scan = store.scan_live_logical().unwrap();
    assert!(!scan.entries.iter().any(|(s, _)| s == b"k" || s == PAYLOAD));
}

fn classify(store: &mut Store) -> Outcome {
    assert_no_ordinary_leak(store);
    let stage = match store.atomic_stage() {
        Ok(s) => s,
        Err(_) => return Outcome::Damage,
    };
    match stage.examine(aid()).class {
        AtomicStageClass::Absent => Outcome::Absence,
        AtomicStageClass::Prepared => Outcome::PreparedInvisible,
        AtomicStageClass::Staged => Outcome::StagedInvisible,
        AtomicStageClass::Sealed => Outcome::DurableInvisible,
        AtomicStageClass::Blocked => Outcome::Damage,
    }
}

fn setup(scenario: Scenario, store: &mut Store) {
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m));
    let mut stage = store.atomic_stage().unwrap();
    if !matches!(scenario, Scenario::Prepare) {
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
    }
    if matches!(scenario, Scenario::ChunkPlan | Scenario::ChunkBody) {
        let (map, _, _) = chunks();
        if matches!(scenario, Scenario::ChunkBody) {
            stage.commit_chunk_manifest(aid(), 0, map).unwrap();
        }
    }
    if matches!(scenario, Scenario::Seal) {
        stage.append_staged(m, PAYLOAD.to_vec()).unwrap();
    }
}

fn run_op(scenario: Scenario, store: &mut Store) -> Result<(), StoreError> {
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m));
    let mut stage = store.atomic_stage().unwrap();
    match scenario {
        Scenario::Prepare => stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .map(|_| ()),
        Scenario::Member => stage.append_staged(m, PAYLOAD.to_vec()),
        Scenario::ChunkPlan => {
            let (map, _, _) = chunks();
            stage.commit_chunk_manifest(aid(), 0, map)
        }
        Scenario::ChunkBody => {
            let (_, p0, _) = chunks();
            stage.append_chunk(m, 0, p0.to_vec())
        }
        Scenario::Seal => stage.seal_member_boundary(aid()),
    }
}

fn drive(scenario: Scenario, phase: Phase) -> Outcome {
    let _g = serial();
    enable_failpoint_hit_proof();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    setup(scenario, &mut store);
    let name = fp_name(scenario, phase);
    arm_failpoint_once(name, FailpointAction::Error);
    let err = run_op(scenario, &mut store).unwrap_err();
    assert!(
        matches!(err, StoreError::Failpoint(_) | StoreError::AtomicStage(_)),
        "{scenario:?} {phase:?} expected injected failure, got {err}"
    );
    require_failpoint_visited(name);
    clear_failpoints();
    drop(store);
    let mut store = Store::open(&path).unwrap();
    classify(&mut store)
}

fn all_cells() -> Vec<(Scenario, Phase)> {
    let mut out = Vec::new();
    for scenario in [
        Scenario::Prepare,
        Scenario::Member,
        Scenario::ChunkPlan,
        Scenario::ChunkBody,
        Scenario::Seal,
    ] {
        for phase in [
            Phase::BeforeAppend,
            Phase::AfterAppend,
            Phase::AfterCheckpoint,
        ] {
            out.push((scenario, phase));
        }
    }
    out
}

#[test]
fn store_prefix_matrix_has_reviewed_reopen_classes() {
    for (scenario, phase) in all_cells() {
        let got = drive(scenario, phase);
        let allow = allowed(scenario, phase);
        assert!(
            allow.contains(&got),
            "{scenario:?} {phase:?} => {got:?} not in {allow:?}"
        );
    }
}

#[test]
fn store_prefix_sentinels_are_exact() {
    assert_eq!(
        drive(Scenario::Prepare, Phase::BeforeAppend),
        Outcome::Absence
    );
    assert_eq!(
        drive(Scenario::Prepare, Phase::AfterAppend),
        Outcome::PreparedInvisible
    );
    assert_eq!(
        drive(Scenario::Prepare, Phase::AfterCheckpoint),
        Outcome::PreparedInvisible
    );
    assert_eq!(
        drive(Scenario::Member, Phase::BeforeAppend),
        Outcome::PreparedInvisible
    );
    assert_eq!(
        drive(Scenario::Member, Phase::AfterAppend),
        Outcome::StagedInvisible
    );
    assert_eq!(
        drive(Scenario::ChunkPlan, Phase::BeforeAppend),
        Outcome::PreparedInvisible
    );
    assert_eq!(
        drive(Scenario::ChunkBody, Phase::AfterAppend),
        Outcome::PreparedInvisible
    );
    assert_eq!(
        drive(Scenario::Seal, Phase::BeforeAppend),
        Outcome::StagedInvisible
    );
    assert_eq!(
        drive(Scenario::Seal, Phase::AfterCheckpoint),
        Outcome::DurableInvisible
    );
}
