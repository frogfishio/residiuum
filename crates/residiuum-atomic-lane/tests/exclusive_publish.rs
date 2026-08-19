//! CR-ATMR4-006: exclusive publish must not poison same-ID retry.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::io_fail::{self, IoAction, IoPhase, IoPoint, IoSite};
use residiuum_atomic_lane::{DurableLane, LaneError};
use residiuum_atomics::{AtomicRefuseReason, AtomicsError};
use std::fs;

fn refused(err: LaneError) -> AtomicRefuseReason {
    match err {
        LaneError::Kernel(AtomicsError::Refused(r)) => r,
        other => panic!("expected kernel refusal, got {other:?}"),
    }
}

fn cleanup_failpoints() -> std::sync::MutexGuard<'static, ()> {
    let guard = io_fail::serial_guard();
    io_fail::disarm_all();
    io_fail::clear_visits();
    io_fail::clear_mutants();
    guard
}

#[test]
fn short_plan_write_retry_succeeds() {
    let _serial = cleanup_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let member = create_member(aid(1), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&member), &[b"v"]);
    io_fail::arm_once(
        IoPoint::new(IoSite::Plan, IoPhase::ShortWrite),
        IoAction::ShortWrite,
    );
    assert!(lane
        .begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .is_err());
    io_fail::disarm_all();
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .unwrap();
    assert!(lane.heap().can_resolve(aid(1)));
}

#[test]
fn pre_write_error_retry_succeeds() {
    let _serial = cleanup_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let member = create_member(aid(2), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&member), &[b"v"]);
    io_fail::arm_once(
        IoPoint::new(IoSite::Plan, IoPhase::BeforeWrite),
        IoAction::Error,
    );
    assert!(lane
        .begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .is_err());
    io_fail::disarm_all();
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .unwrap();
    assert!(lane.heap().can_resolve(aid(2)));
}

#[test]
fn empty_torn_sidecar_does_not_conflict() {
    let _serial = cleanup_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let member = create_member(aid(3), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&member), &[b"v"]);
    let sidecar = dir.path().join("plan").join(format!("{}", aid(3)));
    fs::create_dir_all(sidecar.parent().unwrap()).unwrap();
    fs::write(&sidecar, []).unwrap();
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .unwrap();
    assert!(lane.heap().can_resolve(aid(3)));
    assert_ne!(fs::metadata(&sidecar).unwrap().len(), 0);
}

#[test]
fn different_valid_identity_still_conflicts() {
    let _serial = cleanup_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let member = create_member(aid(4), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&member), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .unwrap();
    let other = create_member(aid(4), 0, "k", b"other");
    let other_plan = plan_for(hid(1), std::slice::from_ref(&other), &[b"other"]);
    assert_eq!(
        refused(
            lane.begin_prepare(&other_plan, FRONTIER, std::slice::from_ref(&other))
                .unwrap_err()
        ),
        AtomicRefuseReason::AtomicIdConflict
    );
}

#[test]
fn payload_and_intent_short_write_retry() {
    let _serial = cleanup_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let member = create_member(aid(5), 0, "k", b"payload-bytes");
    let plan = plan_for(hid(1), std::slice::from_ref(&member), &[b"payload-bytes"]);
    io_fail::arm_once(
        IoPoint::new(IoSite::Intent, IoPhase::ShortWrite),
        IoAction::ShortWrite,
    );
    assert!(lane
        .begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .is_err());
    io_fail::disarm_all();
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .unwrap();
    io_fail::arm_once(
        IoPoint::new(IoSite::Payload, IoPhase::ShortWrite),
        IoAction::ShortWrite,
    );
    assert!(lane
        .append_staged(member.clone(), b"payload-bytes".to_vec())
        .is_err());
    io_fail::disarm_all();
    lane.append_staged(member, b"payload-bytes".to_vec())
        .unwrap();
}
