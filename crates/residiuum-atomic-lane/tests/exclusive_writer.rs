//! CR-ATMR3-007: one physical writer per lane directory.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::{DurableLane, LaneError};
use std::process::Command;

fn prepare_one(lane: &mut DurableLane, n: u8) {
    let m = create_member(aid(n), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
}

#[test]
fn second_in_process_open_is_writer_held() {
    let dir = tempfile::tempdir().unwrap();
    let lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    assert!(lane.holds_writer_lock());
    match DurableLane::open(dir.path()) {
        Err(LaneError::WriterHeld) => {}
        Ok(_) => panic!("second open must not succeed"),
        Err(_) => panic!("expected WriterHeld"),
    }
    drop(lane);
    let again = DurableLane::open(dir.path()).unwrap();
    assert!(again.holds_writer_lock());
}

#[test]
fn second_in_process_create_is_writer_held() {
    let dir = tempfile::tempdir().unwrap();
    let _lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    match DurableLane::create(dir.path(), hid(2), 1) {
        Err(LaneError::WriterHeld) => {}
        Ok(_) => panic!("second create must not succeed"),
        Err(_) => panic!("expected WriterHeld"),
    }
}

#[test]
fn reopen_releases_and_reacquires() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane, 1);
    let lane = lane.reopen().unwrap();
    assert!(lane.heap().can_resolve(aid(1)));
    assert!(lane.holds_writer_lock());
}

#[test]
fn same_id_conflict_still_holds_after_lock() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane, 2);
    let other = create_member(aid(2), 0, "k", b"other");
    let other_plan = plan_for(hid(1), std::slice::from_ref(&other), &[b"other"]);
    match lane.begin_prepare(&other_plan, FRONTIER, std::slice::from_ref(&other)) {
        Err(LaneError::Kernel(_)) => {}
        Ok(_) => panic!("same-id different plan must conflict"),
        Err(_) => panic!("expected kernel conflict"),
    }
}

#[test]
fn child_process_cannot_open_while_parent_holds() {
    if let Ok(path) = std::env::var("ATMR3_007_LOCK_PATH") {
        match DurableLane::open(path) {
            Err(LaneError::WriterHeld) => return,
            Ok(_) => panic!("child opened a locked lane"),
            Err(_) => panic!("child expected WriterHeld"),
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let _lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let exe = std::env::current_exe().expect("test exe");
    let status = Command::new(exe)
        .env("ATMR3_007_LOCK_PATH", dir.path())
        .args(["--exact", "child_process_cannot_open_while_parent_holds"])
        .status()
        .expect("spawn child");
    assert!(status.success(), "child should exit 0 on WriterHeld");
}
