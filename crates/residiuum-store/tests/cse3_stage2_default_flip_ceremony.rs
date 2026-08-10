//! CSE-3 Stage 2k — CompactShadow default flip ceremony.
//!
//! ```text
//! cargo test -p residiuum-store --features legacy-raw-store \
//!   --test cse3_stage2_default_flip_ceremony -- --test-threads=1
//! ```

use residiuum_store::{recovery_mode_path, DurabilityMode, RecoveryMode, Store, StorePaths};
use std::fs;
use tempfile::tempdir;

#[test]
fn fresh_create_defaults_to_compact_shadow() {
    let dir = tempdir().unwrap();
    let store = Store::create(dir.path()).unwrap();
    assert_eq!(store.recovery_mode(), RecoveryMode::CompactShadow);
    assert!(store.shadow_dual_stream());
    assert!(recovery_mode_path(&StorePaths::new(dir.path())).is_file());
}

#[test]
fn legacy_missing_marker_stays_materialized_on_open() {
    let dir = tempdir().unwrap();
    {
        let mut store =
            Store::create_with_shards_mode(dir.path(), 1, RecoveryMode::Materialized).unwrap();
        store.put("k", b"v", DurabilityMode::Buffered).unwrap();
        store.seal_active().unwrap();
        store.wait_seals_applied().unwrap();
    }
    // Simulate pre-2k tree: no mode marker.
    let path = recovery_mode_path(&StorePaths::new(dir.path()));
    assert!(path.is_file());
    fs::remove_file(&path).unwrap();
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(
        store.recovery_mode(),
        RecoveryMode::Materialized,
        "missing marker must not silently flip to CompactShadow"
    );
    assert!(!store.shadow_dual_stream());
}

#[test]
fn rollback_from_default_compact_is_non_destructive() {
    let dir = tempdir().unwrap();
    let mut store = Store::create_with_shards(dir.path(), 1).unwrap();
    store.set_enrichment_enabled(true);
    store.set_seal_threshold(64 * 1024);
    let payload = vec![0xCCu8; 80];
    for i in 0..20u64 {
        store
            .put(&format!("x{i}"), &payload, DurabilityMode::Buffered)
            .unwrap();
    }
    store.seal_active().unwrap();
    store.wait_seals_applied().unwrap();
    let paths = StorePaths::new(dir.path());
    let rsh_dir = paths.root.join("recovery/shadow");
    let rsh_count = fs::read_dir(&rsh_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("rsh"))
        .count();
    assert!(rsh_count >= 1);

    store.rollback_to_materialized_mode().unwrap();
    assert_eq!(store.recovery_mode(), RecoveryMode::Materialized);
    let rsh_after = fs::read_dir(&rsh_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("rsh"))
        .count();
    assert_eq!(rsh_after, rsh_count, "rollback must not delete Shadows");
    assert_eq!(
        store.get("x0").unwrap().as_deref(),
        Some(payload.as_slice())
    );
}
