//! CR-R2-003: hostile metadata lengths refuse before allocation or path scans.

use residiuum_atomic_lane::{DurableLane, LaneError};
use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicRefuseReason, AtomicsError, CanonicalKey, CollectionId,
    ContentRoot, HeapId, MutationKind, ObjectIdentity, VersionId,
};
use std::fs;

fn hid(n: u8) -> HeapId {
    let mut b = [0u8; 16];
    b[0] = n;
    HeapId::from_bytes(b).unwrap()
}

fn cid(n: u8) -> CollectionId {
    let mut b = [0u8; 16];
    b[0] = n;
    CollectionId::from_bytes(b).unwrap()
}

fn aid(n: u8) -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = n;
    AtomicId::from_bytes(b).unwrap()
}

fn root(n: u8) -> ContentRoot {
    let mut b = [0u8; 32];
    b[0] = n;
    ContentRoot::from_bytes(b).unwrap()
}

fn vid(n: u8) -> VersionId {
    let mut b = [0u8; 16];
    b[0] = n;
    VersionId::from_bytes(b).unwrap()
}

fn key(s: &str) -> CanonicalKey {
    CanonicalKey::String(s.to_owned())
}

fn create_member(id: AtomicId, ordinal: u32, k: &str, payload: &[u8]) -> AtomicMember {
    AtomicMember {
        atomic_id: id,
        ordinal,
        object_identity: ObjectIdentity::new(cid(1), key(k)),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(payload).as_bytes()),
        event_id: vid(ordinal as u8 + 1),
    }
}

fn prepared_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(1), 0, "k", b"v");
    lane.begin_prepare(aid(1), root(1), std::slice::from_ref(&m))
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
    limit(expect_err(DurableLane::create(dir.path(), hid(1), u32::MAX)));
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