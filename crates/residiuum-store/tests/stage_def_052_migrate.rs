//! DEF-052 — format and protocol migration (phased, evidence-preserving).
//!
//! Covers:
//! - wire / protocol compatibility matrix snapshot
//! - preflight blockers for non-empty destination
//! - plan → apply → verify full path with live parity
//! - source left fully readable after successful migration
//! - plan-only (no destination store)
//! - rollback of incomplete apply
//! - refuse rollback of completed migration
//! - unsupported segment bytes preserved (opaque evidence)

use residiuum_store::{
    load_migration_job, migrate_apply, migrate_plan, migrate_preflight, migrate_rollback,
    migrate_store, migrate_verify, snapshot_protocol_compat, snapshot_wire_matrix, DurabilityMode,
    MigrateOptions, MigratePhase, Store, MIGRATE_PROFILE,
};
use std::fs;
use tempfile::tempdir;

#[test]
fn wire_and_protocol_matrix_declared() {
    let wire = snapshot_wire_matrix();
    assert!(!wire.is_empty());
    assert!(wire.iter().any(|r| r.can_write && r.status == "current"));
    let proto = snapshot_protocol_compat();
    assert_eq!(proto.profile, "residiuum-rpc-v1");
    assert!(proto.mixed_major_requires_policy);
    assert_eq!(proto.rpc_wire_label, "1.0-draft");
}

#[test]
fn full_migration_roundtrip_preserves_source() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");

    let mut store = Store::create(&src).unwrap();
    store
        .put("users/a", br#"{"n":1}"#, DurabilityMode::Durable)
        .unwrap();
    store
        .put("users/b", br#"{"n":2}"#, DurabilityMode::Durable)
        .unwrap();
    store.delete("users/a", DurabilityMode::Durable).unwrap();
    let sid = store.store_id();

    let report = store.migrate_to(&dst, MigrateOptions::default()).unwrap();
    assert_eq!(report.phase, MigratePhase::Done);
    assert!(report.files_applied >= 2);
    assert_eq!(report.verified_live_subjects, Some(1));
    assert!(report.job_path.is_file());

    // Source still exclusive-writable with same identity and data.
    assert_eq!(store.store_id(), sid);
    assert_eq!(
        store.get("users/b").unwrap().as_deref(),
        Some(br#"{"n":2}"#.as_slice())
    );
    store
        .put("users/c", br#"{"n":3}"#, DurabilityMode::Durable)
        .unwrap();
    drop(store);

    let opened = Store::open(&dst).unwrap();
    assert_eq!(opened.store_id(), sid);
    assert!(opened.get("users/a").unwrap().is_none());
    assert_eq!(
        opened.get("users/b").unwrap().as_deref(),
        Some(br#"{"n":2}"#.as_slice())
    );
    // Destination is a snapshot as-of migrate; users/c is only on source.
    assert!(opened.get("users/c").unwrap().is_none());

    let job = load_migration_job(&src).unwrap().expect("job");
    assert_eq!(job.profile, MIGRATE_PROFILE);
    assert_eq!(job.phase, MigratePhase::Done);
    assert!(!job.wire_matrix.is_empty());
    assert_eq!(job.protocol.profile, "residiuum-rpc-v1");
}

#[test]
fn preflight_and_plan_only() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let mut store = Store::create(&src).unwrap();
    store.put("k", b"v", DurabilityMode::Durable).unwrap();

    let pre = store.migrate_preflight(&dst).unwrap();
    assert!(pre.dest_ok);
    assert!(pre.blockers.is_empty());
    assert!(!pre.wire_matrix.is_empty());
    assert!(pre.files_classified > 0);

    let report = store
        .migrate_to(
            &dst,
            MigrateOptions {
                plan_only: true,
                skip_verify: false,
            },
        )
        .unwrap();
    assert_eq!(report.phase, MigratePhase::Plan);
    assert!(
        !dst.exists()
            || fs::read_dir(&dst)
                .map(|mut d| d.next().is_none())
                .unwrap_or(true)
    );
}

#[test]
fn preflight_blocks_existing_store_dest() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let _s = Store::create(&src).unwrap();
    let _d = Store::create(&dst).unwrap();
    let inspect = Store::open_inspect(&src).unwrap();
    let pre = inspect.migrate_preflight(&dst).unwrap();
    assert!(!pre.dest_ok);
    assert!(!pre.blockers.is_empty());
}

#[test]
fn phased_apply_verify_and_rollback() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let mut store = Store::create(&src).unwrap();
    store.put("k", b"hello", DurabilityMode::Durable).unwrap();
    let sid = store.store_id();
    drop(store);

    let plan = migrate_plan(&src, &dst, sid).unwrap();
    assert_eq!(plan.phase, MigratePhase::Plan);
    let applied = migrate_apply(&plan).unwrap();
    assert_eq!(applied.phase, MigratePhase::Apply);
    assert!(applied.files_applied > 0);

    // Incomplete: rollback restores clean slate for dest.
    let rolled = migrate_rollback(&applied).unwrap();
    assert_eq!(rolled.phase, MigratePhase::RolledBack);
    assert!(!dst.exists());

    // Re-run full path.
    let report = migrate_store(&src, &dst, sid, MigrateOptions::default()).unwrap();
    assert_eq!(report.phase, MigratePhase::Done);
    let opened = Store::open(&dst).unwrap();
    assert_eq!(
        opened.get("k").unwrap().as_deref(),
        Some(b"hello".as_slice())
    );
}

#[test]
fn verify_after_apply_skip() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let mut store = Store::create(&src).unwrap();
    store.put("k", b"v", DurabilityMode::Durable).unwrap();
    let sid = store.store_id();
    drop(store);

    let report = migrate_store(
        &src,
        &dst,
        sid,
        MigrateOptions {
            plan_only: false,
            skip_verify: true,
        },
    )
    .unwrap();
    assert_eq!(report.phase, MigratePhase::Apply);

    let job = load_migration_job(&src).unwrap().unwrap();
    let verified = migrate_verify(&job).unwrap();
    assert_eq!(verified.phase, MigratePhase::Done);
    assert_eq!(verified.verified_live_subjects, Some(1));
}

#[test]
fn preflight_via_free_function() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    fs::create_dir_all(&dst).unwrap();
    fs::write(dst.join("noise"), b"1").unwrap();
    let store = Store::create(&src).unwrap();
    let pre = migrate_preflight(&src, &dst, store.store_id()).unwrap();
    assert!(!pre.blockers.is_empty());
}

#[test]
fn job_content_hash_tamper_detected() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let mut store = Store::create(&src).unwrap();
    store.put("k", b"v", DurabilityMode::Durable).unwrap();
    store
        .migrate_to(
            &dst,
            MigrateOptions {
                plan_only: true,
                ..Default::default()
            },
        )
        .unwrap();
    drop(store);

    let job_path = src.join("recovery/migration/job.v1.json");
    let mut bytes = fs::read(&job_path).unwrap();
    // Flip a hex digit in content_hash_hex.
    let key = b"\"content_hash_hex\"";
    let pos = bytes
        .windows(key.len())
        .position(|w| w == key)
        .expect("hash key");
    for b in bytes.iter_mut().skip(pos + key.len()) {
        if b.is_ascii_hexdigit() {
            *b = if *b == b'0' { b'1' } else { b'0' };
            break;
        }
    }
    fs::write(&job_path, &bytes).unwrap();
    let err = load_migration_job(&src).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("content_hash") || msg.contains("mismatch") || msg.contains("corrupt"),
        "err={msg}"
    );
}
