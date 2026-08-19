//! CR-ATMR4-007: size-check every recovery sidecar before allocate.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::{DurableLane, LaneError, RecoveryLimits};
use residiuum_atomics::{AtomicRefuseReason, AtomicsError};
use std::fs::{self, OpenOptions};

fn prepare_one(lane: &mut DurableLane) {
    let m = create_member(aid(1), 0, "k", b"secret");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"secret"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.append_staged(m, b"secret".to_vec()).unwrap();
    lane.seal_member_boundary(aid(1)).unwrap();
}

fn open_err(dir: &std::path::Path) -> LaneError {
    match DurableLane::open(dir) {
        Err(e) => e,
        Ok(_) => panic!("open succeeded"),
    }
}

fn limit_exceeded(err: LaneError) {
    match err {
        LaneError::Kernel(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded)) => {}
        other => panic!("expected LimitExceeded, got {other:?}"),
    }
}

fn blow_up(path: &std::path::Path) {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    file.set_len((1 << 30) + 1).unwrap();
}

#[test]
fn oversized_plan_intent_payload_seal_checkpoint_refuse() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane);
    drop(lane);
    let id = format!("{}", aid(1));
    blow_up(&dir.path().join("plan").join(&id));
    limit_exceeded(open_err(dir.path()));

    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane);
    drop(lane);
    blow_up(&dir.path().join("intent").join(format!("{}", aid(1))));
    limit_exceeded(open_err(dir.path()));

    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane);
    drop(lane);
    blow_up(&dir.path().join("payload").join(format!("{}-0", aid(1))));
    limit_exceeded(open_err(dir.path()));

    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane);
    drop(lane);
    blow_up(&dir.path().join("sealed").join(format!("{}", aid(1))));
    limit_exceeded(open_err(dir.path()));

    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane);
    drop(lane);
    blow_up(&dir.path().join("checkpoint"));
    limit_exceeded(open_err(dir.path()));
}

#[test]
fn oversized_chunk_manifest_and_body_refuse() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(2), 0, "k", b"secret");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"secret"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    drop(lane);
    let man_dir = dir.path().join("chunk-manifest");
    fs::create_dir_all(&man_dir).unwrap();
    blow_up(&man_dir.join(format!("{}-0", aid(2))));
    limit_exceeded(open_err(dir.path()));
}

#[test]
fn seal_at_limit_is_read_then_corrupt_not_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane);
    drop(lane);
    let path = dir.path().join("sealed").join(format!("{}", aid(1)));
    fs::write(&path, vec![0u8; 156]).unwrap();
    match DurableLane::open(dir.path()) {
        Err(LaneError::Corrupt(_)) => {}
        Err(LaneError::Kernel(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded))) => {
            panic!("at-limit seal must be read, not refused as LimitExceeded")
        }
        Ok(_) => panic!("expected Corrupt, open succeeded"),
        Err(e) => panic!("expected Corrupt, got {e:?}"),
    }
}

#[test]
fn aggregate_sidecars_cannot_evade_budget() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane);
    drop(lane);
    let mut limits = RecoveryLimits::prototype();
    limits.max_sidecar_bytes = 32;
    match DurableLane::open_with_limits(dir.path(), limits) {
        Err(LaneError::Incomplete {
            what: "sidecar bytes",
            ..
        }) => {}
        Ok(_) => panic!("expected Incomplete sidecar bytes, open succeeded"),
        Err(e) => panic!("expected Incomplete sidecar bytes, got {e:?}"),
    }
}

#[test]
fn ack_over_eight_bytes_is_limit() {
    let dir = tempfile::tempdir().unwrap();
    let lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    drop(lane);
    fs::write(dir.path().join("coordinator.ack"), [0u8; 9]).unwrap();
    limit_exceeded(open_err(dir.path()));
}
