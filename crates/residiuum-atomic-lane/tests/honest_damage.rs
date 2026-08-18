//! CR-ATMR3-003: acknowledged damage is corrupt; unacked torn tails are not.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::{DurableLane, LaneError};
use std::fs;

fn prepare_only() -> (tempfile::TempDir, DurableLane) {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(1), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    (dir, lane)
}

fn prepared_and_staged() -> (tempfile::TempDir, DurableLane) {
    let (dir, mut lane) = prepare_only();
    let m = create_member(aid(1), 0, "k", b"v");
    lane.append_staged(m, b"v".to_vec()).unwrap();
    (dir, lane)
}

fn sealed() -> (tempfile::TempDir, DurableLane) {
    let (dir, mut lane) = prepared_and_staged();
    lane.seal_member_boundary(aid(1)).unwrap();
    (dir, lane)
}

fn is_damage(err: LaneError) -> bool {
    matches!(
        err,
        LaneError::Coverage { .. } | LaneError::Corrupt(_) | LaneError::Kernel(_)
    )
}

#[test]
fn bitflip_of_acknowledged_sole_prepare_is_corrupt() {
    let (dir, lane) = prepare_only();
    drop(lane);
    let log = dir.path().join("coordinator.log");
    let mut bytes = fs::read(&log).unwrap();
    let i = bytes.len() / 2;
    bytes[i] ^= 0xff;
    fs::write(&log, bytes).unwrap();
    assert!(
        DurableLane::open(dir.path()).is_err_and(is_damage),
        "checksum damage of the sole acknowledged prepare must not look empty"
    );
}

#[test]
fn truncate_acknowledged_sole_prepare_is_corrupt() {
    let (dir, lane) = prepare_only();
    drop(lane);
    let log = dir.path().join("coordinator.log");
    let bytes = fs::read(&log).unwrap();
    fs::write(&log, &bytes[..bytes.len() - 1]).unwrap();
    assert!(DurableLane::open(dir.path()).is_err_and(is_damage));
}

#[test]
fn unacknowledged_torn_tail_does_not_invent_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    drop(lane);
    let log = dir.path().join("coordinator.log");
    fs::write(&log, b"RESIDFRM\x00partial-unacked-tail").unwrap();
    let lane = DurableLane::open(dir.path()).unwrap();
    assert!(
        !lane.heap().can_resolve(aid(1)),
        "unacked torn tail must not invent a prepare"
    );
}

#[test]
fn every_prepare_byte_flip_and_truncation_is_explicit() {
    let (dir, lane) = prepare_only();
    drop(lane);
    let log = dir.path().join("coordinator.log");
    let original = fs::read(&log).unwrap();
    assert!(!original.is_empty());
    for i in 0..original.len() {
        let mut flipped = original.clone();
        flipped[i] ^= 0xff;
        fs::write(&log, &flipped).unwrap();
        assert!(
            DurableLane::open(dir.path()).is_err_and(is_damage),
            "prepare flip at {i} must be damage"
        );
    }
    for n in 0..original.len() {
        fs::write(&log, &original[..n]).unwrap();
        assert!(
            DurableLane::open(dir.path()).is_err_and(is_damage),
            "prepare truncate to {n} must be damage"
        );
    }
    fs::write(&log, &original).unwrap();
    assert!(DurableLane::open(dir.path()).is_ok());
}

#[test]
fn every_member_byte_flip_and_truncation_is_explicit() {
    let (dir, lane) = prepared_and_staged();
    drop(lane);
    let log = dir.path().join("shard-00000000.log");
    let original = fs::read(&log).unwrap();
    assert!(!original.is_empty());
    for i in 0..original.len() {
        let mut flipped = original.clone();
        flipped[i] ^= 0xff;
        fs::write(&log, &flipped).unwrap();
        assert!(
            DurableLane::open(dir.path()).is_err_and(is_damage),
            "member flip at {i} must be damage"
        );
    }
    for n in 0..original.len() {
        fs::write(&log, &original[..n]).unwrap();
        assert!(
            DurableLane::open(dir.path()).is_err_and(is_damage),
            "member truncate to {n} must be damage"
        );
    }
    fs::write(&log, &original).unwrap();
    assert!(DurableLane::open(dir.path()).is_ok());
}

#[test]
fn every_seal_byte_flip_and_truncation_is_explicit() {
    let (dir, lane) = sealed();
    drop(lane);
    let path = dir.path().join("sealed").join(format!("{}", aid(1)));
    let original = fs::read(&path).unwrap();
    assert!(!original.is_empty());
    for i in 0..original.len() {
        let mut flipped = original.clone();
        flipped[i] ^= 0xff;
        fs::write(&path, &flipped).unwrap();
        assert!(
            DurableLane::open(dir.path()).is_err_and(is_damage),
            "seal flip at {i} must be damage"
        );
    }
    for n in 0..original.len() {
        fs::write(&path, &original[..n]).unwrap();
        assert!(
            DurableLane::open(dir.path()).is_err_and(is_damage),
            "seal truncate to {n} must be damage"
        );
    }
    fs::write(&path, &original).unwrap();
    assert!(DurableLane::open(dir.path()).is_ok());
}
