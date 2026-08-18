//! CR-R2-001: refused prepare/member mutations leave an identical directory image.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::{DurableLane, LaneError};
use residiuum_atomics::{AtomicRefuseReason, AtomicsError};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let name = entry.file_name();
        if name.to_string_lossy().ends_with(".tmp") {
            continue;
        }
        if path.is_dir() {
            walk(root, &path, out);
        } else {
            out.insert(
                path.strip_prefix(root).unwrap().to_path_buf(),
                fs::read(&path).unwrap(),
            );
        }
    }
}

fn refused(err: LaneError) -> AtomicRefuseReason {
    match err {
        LaneError::Kernel(AtomicsError::Refused(r)) => r,
        other => panic!("expected kernel refusal, got {other:?}"),
    }
}

#[test]
fn malformed_prepare_does_not_touch_the_directory() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let before = snapshot(dir.path());
    let m = create_member(aid(1), 0, "k", b"x");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"x"]);
    let mut bad = m.clone();
    bad.before_version = Some(vid(9));
    assert_eq!(
        refused(
            lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&bad))
                .unwrap_err()
        ),
        AtomicRefuseReason::InvalidValue
    );
    assert_eq!(snapshot(dir.path()), before);
    assert!(!lane.heap().can_resolve(aid(1)));
}

#[test]
fn same_id_different_root_does_not_overwrite_intent() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(2), 0, "k", b"orig");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"orig"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    let before = snapshot(dir.path());
    let other = create_member(aid(2), 0, "k", b"new");
    let other_plan = plan_for(hid(1), std::slice::from_ref(&other), &[b"new"]);
    assert_eq!(
        refused(
            lane.begin_prepare(&other_plan, FRONTIER, std::slice::from_ref(&other))
                .unwrap_err()
        ),
        AtomicRefuseReason::AtomicIdConflict
    );
    assert_eq!(snapshot(dir.path()), before);
    let intent = fs::read(dir.path().join("intent").join(format!("{}", aid(2)))).unwrap();
    assert!(
        !intent.windows(3).any(|w| w == b"new"),
        "rejected prepare must not replace intent bytes"
    );
}

#[test]
fn same_id_same_root_is_idempotent_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(3), 0, "k", b"x");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"x"]);
    let first = lane
        .begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    let before = snapshot(dir.path());
    let second = lane
        .begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    assert_eq!(first.0, second.0);
    assert_eq!(snapshot(dir.path()), before);
}

#[test]
fn append_without_prepare_does_not_create_payload() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let before = snapshot(dir.path());
    let m = create_member(aid(4), 0, "k", b"x");
    assert_eq!(
        refused(lane.append_staged(m, b"x".to_vec()).unwrap_err()),
        AtomicRefuseReason::MalformedInput
    );
    assert_eq!(snapshot(dir.path()), before);
}

#[test]
fn bad_payload_hash_does_not_write_or_replace() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(5), 0, "k", b"good");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"good"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    let before = snapshot(dir.path());
    assert_eq!(
        refused(lane.append_staged(m, b"bad".to_vec()).unwrap_err()),
        AtomicRefuseReason::InvalidValue
    );
    assert_eq!(snapshot(dir.path()), before);
}

#[test]
fn duplicate_ordinal_does_not_overwrite_payload() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(6), 0, "k", b"first");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"first"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.append_staged(m.clone(), b"first".to_vec()).unwrap();
    let before = snapshot(dir.path());
    assert_eq!(
        refused(lane.append_staged(m, b"second".to_vec()).unwrap_err()),
        AtomicRefuseReason::DuplicateTarget
    );
    assert_eq!(snapshot(dir.path()), before);
    let payload = fs::read(dir.path().join("payload").join(format!("{}-0", aid(6)))).unwrap();
    assert_eq!(payload, b"first");
}

#[test]
fn identical_append_is_idempotent_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(7), 0, "k", b"x");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"x"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.append_staged(m.clone(), b"x".to_vec()).unwrap();
    let before = snapshot(dir.path());
    lane.append_staged(m, b"x".to_vec()).unwrap();
    assert_eq!(snapshot(dir.path()), before);
}
