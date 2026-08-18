//! CR-ATMR3-008: phase-indexed I/O failures against real reopened media.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::io_fail::{
    self, IoAction, IoMutant, IoPhase, IoPoint, IoSite,
};
use residiuum_atomic_lane::DurableLane;
use residiuum_atomics::{MemberPhase, StagingHeap};

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
    Seal,
}

fn assert_no_ordinary_leak(heap: &StagingHeap, k: &str) {
    assert!(heap.get(cid(1), &key(k)).is_none());
    assert!(!heap.scan().any(|(_, kk, _)| kk == &key(k)));
}

fn classify(dir: &std::path::Path, id: residiuum_atomics::AtomicId, k: &str) -> Outcome {
    io_fail::reset_process();
    let lane = match DurableLane::open(dir) {
        Ok(l) => l,
        Err(_) => return Outcome::Damage,
    };
    assert_no_ordinary_leak(lane.heap(), k);
    let Some(lc) = lane.heap().lifecycle(id) else {
        return Outcome::Absence;
    };
    if lc.members == MemberPhase::DurableInvisible {
        return Outcome::DurableInvisible;
    }
    match lane.heap().inspect_staged(id) {
        Some(ms) if !ms.is_empty() && ms.iter().all(|s| s.payload_complete) => {
            Outcome::StagedInvisible
        }
        _ => Outcome::PreparedInvisible,
    }
}

fn allowed(scenario: Scenario, site: IoSite, phase: IoPhase) -> &'static [Outcome] {
    use IoPhase::*;
    use IoSite::*;
    use Outcome::*;
    match (scenario, site, phase) {
        (Scenario::Prepare, Plan | Intent, BeforeWrite | ShortWrite | AfterWrite) => {
            &[Absence, Damage]
        }
        (Scenario::Prepare, Plan | Intent, _) => &[Absence, PreparedInvisible, Damage],
        (Scenario::Prepare, Coordinator, BeforeWrite | ShortWrite) => &[Absence, Damage],
        (Scenario::Prepare, Coordinator, _) => &[Absence, PreparedInvisible, Damage],
        (Scenario::Prepare, Ack | Checkpoint, _) => &[PreparedInvisible, Damage],
        (Scenario::Member, Payload, BeforeWrite | ShortWrite) => {
            &[PreparedInvisible, Damage]
        }
        (Scenario::Member, Payload, _) => &[PreparedInvisible, StagedInvisible, Damage],
        (Scenario::Member, Shard, _) => &[PreparedInvisible, StagedInvisible, Damage],
        (Scenario::Member, Ack | Checkpoint, _) => &[StagedInvisible, PreparedInvisible, Damage],
        (Scenario::Seal, Seal, AfterLogSync | BeforeWrite | ShortWrite | AfterWrite | BeforeRename) => {
            &[StagedInvisible, Damage]
        }
        (Scenario::Seal, Seal, _) => &[StagedInvisible, DurableInvisible, Damage],
        (Scenario::Seal, Checkpoint, _) => &[DurableInvisible, StagedInvisible, Damage],
        _ => &[Absence, PreparedInvisible, StagedInvisible, DurableInvisible, Damage],
    }
}

fn exclusive_cells(site: IoSite) -> Vec<(IoPoint, IoAction)> {
    vec![
        (IoPoint::new(site, IoPhase::BeforeWrite), IoAction::Error),
        (IoPoint::new(site, IoPhase::BeforeWrite), IoAction::Kill),
        (IoPoint::new(site, IoPhase::ShortWrite), IoAction::ShortWrite),
        (IoPoint::new(site, IoPhase::AfterWrite), IoAction::Kill),
        (IoPoint::new(site, IoPhase::AfterFileSync), IoAction::Kill),
        (IoPoint::new(site, IoPhase::AfterDirSync), IoAction::Kill),
    ]
}

fn atomic_cells(site: IoSite) -> Vec<(IoPoint, IoAction)> {
    let mut v = exclusive_cells(site);
    v.push((IoPoint::new(site, IoPhase::BeforeRename), IoAction::Error));
    v.push((IoPoint::new(site, IoPhase::BeforeRename), IoAction::Kill));
    v.push((IoPoint::new(site, IoPhase::AfterRename), IoAction::Kill));
    v
}

fn log_cells(site: IoSite) -> Vec<(IoPoint, IoAction)> {
    exclusive_cells(site)
}

fn setup_prefix(scenario: Scenario, lane: &mut DurableLane, n: u8) {
    let m = create_member(aid(n), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    match scenario {
        Scenario::Prepare => {}
        Scenario::Member | Scenario::Seal => {
            lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
                .unwrap();
        }
    }
    if matches!(scenario, Scenario::Seal) {
        let m = create_member(aid(n), 0, "k", b"v");
        lane.append_staged(m, b"v".to_vec()).unwrap();
    }
}

fn run_op(scenario: Scenario, lane: &mut DurableLane, n: u8) {
    let m = create_member(aid(n), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    match scenario {
        Scenario::Prepare => {
            let _ = lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m));
        }
        Scenario::Member => {
            let _ = lane.append_staged(m, b"v".to_vec());
        }
        Scenario::Seal => {
            let _ = lane.seal_member_boundary(aid(n));
        }
    }
}

fn drive(
    scenario: Scenario,
    point: IoPoint,
    action: IoAction,
    n: u8,
) -> Outcome {
    io_fail::reset_process();
    io_fail::clear_visits();
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    io_fail::clear_visits();
    setup_prefix(scenario, &mut lane, n);
    io_fail::arm_once(point, action);
    run_op(scenario, &mut lane, n);
    drop(lane);
    classify(dir.path(), aid(n), "k")
}

#[test]
fn prefix_matrix_never_publishes_and_stays_in_recorded_classes() {
    let mut cases: Vec<(Scenario, IoPoint, IoAction)> = Vec::new();
    for (p, a) in exclusive_cells(IoSite::Plan) {
        cases.push((Scenario::Prepare, p, a));
    }
    for (p, a) in exclusive_cells(IoSite::Intent) {
        cases.push((Scenario::Prepare, p, a));
    }
    for (p, a) in log_cells(IoSite::Coordinator) {
        cases.push((Scenario::Prepare, p, a));
    }
    for (p, a) in atomic_cells(IoSite::Ack) {
        cases.push((Scenario::Prepare, p, a));
    }
    for (p, a) in atomic_cells(IoSite::Checkpoint) {
        cases.push((Scenario::Prepare, p, a));
    }
    for (p, a) in exclusive_cells(IoSite::Payload) {
        cases.push((Scenario::Member, p, a));
    }
    for (p, a) in log_cells(IoSite::Shard) {
        cases.push((Scenario::Member, p, a));
    }
    for (p, a) in atomic_cells(IoSite::Ack) {
        cases.push((Scenario::Member, p, a));
    }
    cases.push((
        Scenario::Seal,
        IoPoint::new(IoSite::Seal, IoPhase::AfterLogSync),
        IoAction::Kill,
    ));
    for (p, a) in atomic_cells(IoSite::Seal) {
        cases.push((Scenario::Seal, p, a));
    }

    for (i, (scenario, point, action)) in cases.into_iter().enumerate() {
        let got = drive(scenario, point, action, (i % 200) as u8 + 1);
        let allow = allowed(scenario, point.site, point.phase);
        assert!(
            allow.contains(&got),
            "{scenario:?} {point:?} {action:?} => {got:?} not in {allow:?}",
        );
    }
}

#[test]
fn sentinel_prefixes_have_exact_outcomes() {
    assert_eq!(
        drive(
            Scenario::Prepare,
            IoPoint::new(IoSite::Plan, IoPhase::BeforeWrite),
            IoAction::Error,
            1,
        ),
        Outcome::Absence,
    );
    assert_eq!(
        drive(
            Scenario::Prepare,
            IoPoint::new(IoSite::Coordinator, IoPhase::AfterDirSync),
            IoAction::Kill,
            2,
        ),
        Outcome::PreparedInvisible,
    );
    assert_eq!(
        drive(
            Scenario::Member,
            IoPoint::new(IoSite::Payload, IoPhase::BeforeWrite),
            IoAction::Error,
            3,
        ),
        Outcome::PreparedInvisible,
    );
    assert_eq!(
        drive(
            Scenario::Member,
            IoPoint::new(IoSite::Shard, IoPhase::AfterDirSync),
            IoAction::Kill,
            4,
        ),
        Outcome::StagedInvisible,
    );
    assert_eq!(
        drive(
            Scenario::Seal,
            IoPoint::new(IoSite::Seal, IoPhase::AfterLogSync),
            IoAction::Kill,
            5,
        ),
        Outcome::StagedInvisible,
    );
    assert_eq!(
        drive(
            Scenario::Seal,
            IoPoint::new(IoSite::Seal, IoPhase::AfterDirSync),
            IoAction::Kill,
            6,
        ),
        Outcome::DurableInvisible,
    );
}

#[test]
fn production_write_visits_required_sync_and_rename_edges() {
    io_fail::reset_process();
    io_fail::clear_visits();
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    io_fail::clear_visits();
    let m = create_member(aid(7), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.append_staged(m, b"v".to_vec()).unwrap();
    lane.seal_member_boundary(aid(7)).unwrap();

    for site in [
        IoSite::Plan,
        IoSite::Intent,
        IoSite::Payload,
        IoSite::Coordinator,
        IoSite::Shard,
    ] {
        io_fail::require_visited(IoPoint::new(site, IoPhase::AfterFileSync));
        io_fail::require_visited(IoPoint::new(site, IoPhase::AfterDirSync));
    }
    for site in [IoSite::Ack, IoSite::Checkpoint, IoSite::Seal] {
        io_fail::require_visited(IoPoint::new(site, IoPhase::AfterFileSync));
        io_fail::require_visited(IoPoint::new(site, IoPhase::AfterRename));
        io_fail::require_visited(IoPoint::new(site, IoPhase::AfterDirSync));
    }
    io_fail::require_visited(IoPoint::new(IoSite::Seal, IoPhase::AfterLogSync));
    assert_no_ordinary_leak(lane.heap(), "k");
}

#[test]
fn mutant_omit_file_sync_is_detected_by_missing_edge() {
    io_fail::reset_process();
    io_fail::clear_visits();
    io_fail::set_mutant(IoMutant::OmitFileSync);
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    io_fail::clear_visits();
    let m = create_member(aid(8), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    assert_eq!(
        io_fail::visit_count(IoPoint::new(IoSite::Plan, IoPhase::AfterFileSync)),
        0,
        "omit-file-sync mutant must not visit AfterFileSync",
    );
    assert_eq!(
        io_fail::visit_count(IoPoint::new(IoSite::Coordinator, IoPhase::AfterFileSync)),
        0,
    );
}

#[test]
fn mutant_omit_rename_leaves_seal_unpublished() {
    io_fail::reset_process();
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(9), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.append_staged(m, b"v".to_vec()).unwrap();
    io_fail::set_mutant(IoMutant::OmitRename);
    lane.seal_member_boundary(aid(9)).unwrap();
    let sealed = dir.path().join("sealed").join(format!("{}", aid(9)));
    assert!(!sealed.exists(), "omit-rename must not publish the seal");
    assert!(
        sealed.with_extension("tmp").exists(),
        "omit-rename must leave the seal temp",
    );
    drop(lane);
    assert_eq!(classify(dir.path(), aid(9), "k"), Outcome::StagedInvisible);
}

#[test]
fn mutant_omit_dir_sync_is_detected_by_missing_edge() {
    io_fail::reset_process();
    io_fail::clear_visits();
    io_fail::set_mutant(IoMutant::OmitDirSync);
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    io_fail::clear_visits();
    let m = create_member(aid(10), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    assert_eq!(
        io_fail::visit_count(IoPoint::new(IoSite::Plan, IoPhase::AfterDirSync)),
        0,
        "omit-dir-sync mutant must not visit AfterDirSync",
    );
}
