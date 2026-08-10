//! CSQ-8 — derived state / maintenance / backup / migration (first labor cut).
//!
//! Behavioural floors for `CSQ-DER-*`, `CSQ-MNT-*`, `CSQ-BAK-*`, `CSQ-MIG-*`.
//! Permanent regression authorities: DEF-102 (derived), DEF-050 (backup),
//! DEF-051 (scrub), DEF-052 (migrate), DEF-024 (compaction).

use residiuum_store::{
    diagnose_primary_cache, load_and_verify_manifest, load_migration_job, restore_full_backup,
    verify_package_files, BackupConsistency, CompactOptions, CompactPhase, DurabilityMode,
    MigrateOptions, MigratePhase, PrimaryCacheValidation, RestoreOptions, ScrubOptions, Store,
    PRIMARY_CACHE_FILE,
};
use std::fs;
use std::path::Path;
use tempfile::tempdir;

fn seed_live(store: &mut Store) {
    store.put("keep", b"v1", DurabilityMode::Durable).unwrap();
    store.put("drop", b"gone", DurabilityMode::Durable).unwrap();
    store.delete("drop", DurabilityMode::Durable).unwrap();
    store.put("keep", b"v2", DurabilityMode::Durable).unwrap();
    store.put("extra", b"e1", DurabilityMode::Durable).unwrap();
}

fn assert_live_projection(store: &Store) {
    assert_eq!(
        store.get("keep").unwrap().as_deref(),
        Some(b"v2".as_slice())
    );
    assert!(store.get("drop").unwrap().is_none());
    assert_eq!(
        store.get("extra").unwrap().as_deref(),
        Some(b"e1".as_slice())
    );
}

/// CSQ-DER-001/002 — primary cache is never authority; wipe/corrupt is neutral.
#[test]
fn csq_der_primary_cache_never_authority() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    seed_live(&mut store);
    store.persist_index_cache().unwrap();

    let diag = store.primary_cache_diag().unwrap();
    assert!(!diag.authoritative);
    assert!(diag.present);
    assert_eq!(diag.validation, PrimaryCacheValidation::Accepted);
    assert_live_projection(&store);

    // Corrupt cache bytes — logical projection unchanged.
    let cache = root.join("indexes").join(PRIMARY_CACHE_FILE);
    let mut raw = fs::read(&cache).unwrap();
    if raw.len() > 20 {
        let i = raw.len() / 2;
        raw[i] ^= 0xff;
        fs::write(&cache, &raw).unwrap();
    }
    let corrupt = diagnose_primary_cache(&cache, store.store_id(), None, Some(4096));
    assert!(!corrupt.authoritative);
    assert_ne!(corrupt.validation, PrimaryCacheValidation::Accepted);
    assert_live_projection(&store);

    // Delete all derived dirs — reopen rebuilds from authority.
    drop(store);
    for name in ["catalogs", "indexes", "snapshots"] {
        let p = root.join(name);
        if p.exists() {
            fs::remove_dir_all(&p).unwrap();
        }
    }
    let mut reopened = Store::open(&root).unwrap();
    reopened.rebuild_index().unwrap();
    assert_live_projection(&reopened);
    assert_eq!(reopened.live_count(), 2);
}

/// CSQ-DER-003 — rebuild from same authoritative coverage is deterministic.
#[test]
fn csq_der_rebuild_deterministic() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create(&root).unwrap();
        seed_live(&mut store);
        store.seal_active().unwrap();
        store.put("post", b"p1", DurabilityMode::Durable).unwrap();
        store.persist_index_cache().unwrap();
        store.rebuild_catalogs().unwrap();
    }

    let mut a = Store::open(&root).unwrap();
    a.rebuild_index().unwrap();
    let snap_a: Vec<(String, Option<Vec<u8>>)> = ["keep", "drop", "extra", "post"]
        .iter()
        .map(|k| (k.to_string(), a.get(k).unwrap()))
        .collect();
    let count_a = a.live_count();
    drop(a);

    let mut b = Store::open(&root).unwrap();
    b.rebuild_index().unwrap();
    let snap_b: Vec<(String, Option<Vec<u8>>)> = ["keep", "drop", "extra", "post"]
        .iter()
        .map(|k| (k.to_string(), b.get(k).unwrap()))
        .collect();
    assert_eq!(snap_a, snap_b);
    assert_eq!(count_a, b.live_count());
}

/// CSQ-MNT-001/002 — seal + compact preserve live authority; sources retained by default.
#[test]
fn csq_mnt_compact_retains_sources_and_live() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    seed_live(&mut store);
    store.seal_active().unwrap();

    let report = store.compact_live().unwrap();
    assert!(report.sources_retained);
    assert_eq!(report.coverage, "live-projection");
    assert_eq!(report.phase, CompactPhase::Activated);
    assert_eq!(report.bytes_reclaimed, 0);
    assert_live_projection(&store);

    // History remains while sources retained.
    let hist = store.history("keep").unwrap();
    assert!(hist.events.len() >= 2);

    let job = store.load_compact_job(&report.job_id).unwrap().unwrap();
    assert_eq!(job.phase, CompactPhase::Activated);
    assert!(job.sources_retained);
}

/// CSQ-MNT-005 — scrub observes without mutating ordinary get/put authority.
#[test]
fn csq_mnt_scrub_non_mutation() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    seed_live(&mut store);
    store.seal_active().unwrap();
    store
        .put("after-seal", b"ok", DurabilityMode::Durable)
        .unwrap();

    let before_keep = store.get("keep").unwrap();
    let before_extra = store.get("extra").unwrap();
    let before_count = store.live_count();

    let report = store.scrub_to_completion(ScrubOptions::default()).unwrap();
    assert!(report.cycle_completed);
    assert_eq!(report.failures_this_call, 0);

    assert_eq!(store.get("keep").unwrap(), before_keep);
    assert_eq!(store.get("extra").unwrap(), before_extra);
    assert_eq!(store.live_count(), before_count);
    assert_eq!(
        store.get("after-seal").unwrap().as_deref(),
        Some(b"ok".as_slice())
    );

    // New durable write still works after scrub.
    store
        .put("post-scrub", b"new", DurabilityMode::Durable)
        .unwrap();
    assert_eq!(
        store.get("post-scrub").unwrap().as_deref(),
        Some(b"new".as_slice())
    );
}

/// CSQ-BAK-001/002/003 — backup binds declared frontier; restore matches projection;
/// source unchanged.
#[test]
fn csq_bak_backup_restore_source_unchanged() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    let dst = dir.path().join("dst");

    let mut store = Store::create(&src).unwrap();
    seed_live(&mut store);
    let sid = store.store_id();
    let active = src.join("active").join("active.residiuum");
    let before_bytes = fs::read(&active).unwrap();
    let before_mtime = fs::metadata(&active).unwrap().modified().unwrap();

    let report = store.backup_to(&bak).unwrap();
    assert_eq!(report.consistency, BackupConsistency::FlushedExclusive);
    assert_eq!(report.store_id, sid);
    assert!(report.files_copied >= 1);
    assert!(report.manifest_path.is_file());

    // Source bytes/mtime unchanged by backup.
    assert_eq!(fs::read(&active).unwrap(), before_bytes);
    assert_eq!(
        fs::metadata(&active).unwrap().modified().unwrap(),
        before_mtime
    );
    assert_live_projection(&store);

    let manifest = load_and_verify_manifest(&bak).unwrap();
    verify_package_files(&bak, &manifest).unwrap();

    let restored = restore_full_backup(&bak, &dst, RestoreOptions::default()).unwrap();
    assert_eq!(restored.restored_store_id, sid);
    assert_eq!(restored.live_subjects, 2);

    let opened = Store::open(&dst).unwrap();
    assert_eq!(opened.store_id(), sid);
    assert_live_projection(&opened);
}

/// CSQ-BAK-004 — partial/tampered package never masquerades as complete.
#[test]
fn csq_bak_tampered_manifest_rejected() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    {
        let mut store = Store::create(&src).unwrap();
        store.put("k", b"v", DurabilityMode::Durable).unwrap();
        store.backup_to(&bak).unwrap();
    }

    // Tamper a file listed in the package (not the manifest hash alone).
    let store_dir = bak.join("store");
    let mut files: Vec<_> = walk_files(&store_dir);
    files.sort();
    assert!(!files.is_empty());
    let victim = &files[0];
    let mut bytes = fs::read(victim).unwrap();
    if !bytes.is_empty() {
        let i = bytes.len() / 2;
        bytes[i] ^= 0x5a;
        fs::write(victim, &bytes).unwrap();
    }

    let manifest = load_and_verify_manifest(&bak).unwrap();
    let err = verify_package_files(&bak, &manifest).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("hash")
            || msg.contains("mismatch")
            || msg.contains("tamper")
            || msg.contains("blake"),
        "unexpected verify error: {err}"
    );
}

/// CSQ-MIG-001 — migrate preserves source identity/data; destination is snapshot.
#[test]
fn csq_mig_roundtrip_preserves_source() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");

    let mut store = Store::create(&src).unwrap();
    seed_live(&mut store);
    let sid = store.store_id();

    let report = store.migrate_to(&dst, MigrateOptions::default()).unwrap();
    assert_eq!(report.phase, MigratePhase::Done);
    assert!(report.files_applied >= 1);
    assert_eq!(report.verified_live_subjects, Some(2));

    // Source still authoritative and writable.
    assert_eq!(store.store_id(), sid);
    assert_live_projection(&store);
    store
        .put("after-migrate", b"src-only", DurabilityMode::Durable)
        .unwrap();
    drop(store);

    let opened = Store::open(&dst).unwrap();
    assert_eq!(opened.store_id(), sid);
    assert_live_projection(&opened);
    assert!(opened.get("after-migrate").unwrap().is_none());

    let job = load_migration_job(&src).unwrap().expect("migration job");
    assert_eq!(job.phase, MigratePhase::Done);
}

/// CSQ-MIG-002 — preflight/plan leave destination empty; blocked when dest exists.
#[test]
fn csq_mig_preflight_and_plan_only() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let mut store = Store::create(&src).unwrap();
    store.put("k", b"v", DurabilityMode::Durable).unwrap();

    let pre = store.migrate_preflight(&dst).unwrap();
    assert!(pre.dest_ok);
    assert!(pre.blockers.is_empty());
    assert!(!dst.exists() || fs::read_dir(&dst).map(|d| d.count()).unwrap_or(0) == 0);

    // Create dest store so preflight blocks.
    Store::create(&dst).unwrap();
    let blocked = store.migrate_preflight(&dst).unwrap();
    assert!(!blocked.dest_ok || !blocked.blockers.is_empty());
}

/// Compact reclaim refusal without history-loss ack (MNT safety).
#[test]
fn csq_mnt_reclaim_requires_history_loss_ack() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    seed_live(&mut store);
    let err = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: false,
            ..CompactOptions::default()
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("allow_history_loss"),
        "unexpected: {err}"
    );
    assert_live_projection(&store);
}

fn walk_files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if !root.exists() {
        return out;
    }
    fn rec(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        for e in fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                rec(&p, out);
            } else {
                out.push(p);
            }
        }
    }
    rec(root, &mut out);
    out
}
