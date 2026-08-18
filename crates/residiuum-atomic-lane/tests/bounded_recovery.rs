//! CR-R2-003: hostile metadata lengths refuse before allocation or path scans.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::{DurableLane, LaneError};
use residiuum_atomics::{AtomicRefuseReason, AtomicsError};
use std::fs;

fn prepared_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(1), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    drop(lane);
    dir
}

fn intent_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("intent").join(format!("{}", aid(1)))
}

fn expect_err(result: Result<DurableLane, LaneError>) -> LaneError {
    match result {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    }
}

fn limit(err: LaneError) {
    match err {
        LaneError::Kernel(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded)) => {}
        other => panic!("expected LimitExceeded, got {other:?}"),
    }
}

fn corrupt(err: LaneError) {
    match err {
        LaneError::Corrupt(_) => {}
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[test]
fn create_refuses_u32_max_shard_count() {
    let dir = tempfile::tempdir().unwrap();
    limit(expect_err(DurableLane::create(
        dir.path(),
        hid(1),
        u32::MAX,
    )));
}

#[test]
fn create_refuses_one_unit_over_shard_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    limit(expect_err(DurableLane::create(dir.path(), hid(1), 65)));
}

#[test]
fn reopen_refuses_u32_max_shard_count_in_meta() {
    let dir = prepared_dir();
    fs::write(dir.path().join("meta"), b"shard_count=4294967295\n").unwrap();
    limit(expect_err(DurableLane::open(dir.path())));
}

#[test]
fn reopen_refuses_u32_max_intent_count() {
    let dir = prepared_dir();
    fs::write(intent_path(&dir), u32::MAX.to_be_bytes()).unwrap();
    limit(expect_err(DurableLane::open(dir.path())));
}

#[test]
fn reopen_refuses_one_unit_over_intent_count() {
    let dir = prepared_dir();
    fs::write(intent_path(&dir), 4097u32.to_be_bytes()).unwrap();
    limit(expect_err(DurableLane::open(dir.path())));
}

#[test]
fn reopen_refuses_u32_max_member_length() {
    let dir = prepared_dir();
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u32.to_be_bytes());
    buf.extend_from_slice(&u32::MAX.to_be_bytes());
    fs::write(intent_path(&dir), buf).unwrap();
    limit(expect_err(DurableLane::open(dir.path())));
}

#[test]
fn reopen_refuses_trailing_intent_bytes() {
    let dir = prepared_dir();
    let path = intent_path(&dir);
    let mut bytes = fs::read(&path).unwrap();
    bytes.push(0);
    fs::write(&path, bytes).unwrap();
    corrupt(expect_err(DurableLane::open(dir.path())));
}

#[test]
fn reopen_refuses_truncated_member_header() {
    let dir = prepared_dir();
    fs::write(intent_path(&dir), 1u32.to_be_bytes()).unwrap();
    corrupt(expect_err(DurableLane::open(dir.path())));
}

#[test]
fn reopen_refuses_truncated_member_body() {
    let dir = prepared_dir();
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u32.to_be_bytes());
    buf.extend_from_slice(&8u32.to_be_bytes());
    buf.extend_from_slice(&[0u8; 3]);
    fs::write(intent_path(&dir), buf).unwrap();
    corrupt(expect_err(DurableLane::open(dir.path())));
}
