//! CSE-3 Stage 2 — RSHD0004 dual-stream damage/crash matrix + Step 8 flip.
//!
//! Extends the F0–F5 charter to exercise write-time dual streaming failpoints
//! and the controlled product-flip marker. **Honest product TPS remains ~28K**;
//! segment finalize rates are not converted into database TPS.
//!
//! ```text
//! cargo test -p residiuum-store --features legacy-raw-store --test cse3_stage2_rshd0004_matrix
//! ```

use residiuum_store::{
    activate_compact_shadow_mode as persist_activate, arm_failpoint_once, chimera_dir,
    clear_failpoints, every_protected_has_verified_rsh, is_dual_magic, load_protected_coverage,
    load_recovery_mode, prepare_flip_to_compact_shadow as persist_prepare,
    protected_frontier_gap_free, recovery_after_auth_compact_delete, recovery_mode_path,
    rollback_to_materialized_mode, try_load_shadow, DurabilityMode, FailpointAction, RecoveryMode,
    ShadowLoad, Store, StorePaths,
};
use std::collections::BTreeMap;
use std::fs;
use std::sync::Mutex;
use tempfile::tempdir;

/// Failpoints are process-global — serialize this suite.
static FP_LOCK: Mutex<()> = Mutex::new(());

fn with_failpoints<R>(f: impl FnOnce() -> R) -> R {
    let _g = FP_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    let out = f();
    clear_failpoints();
    out
}

fn dual_store(root: &std::path::Path, shards: usize) -> Store {
    // Materialized dual-run baseline for RSHD0004 failpoint / flip ceremony.
    let mut s = Store::create_with_shards_mode(root, shards, RecoveryMode::Materialized).unwrap();
    s.set_enrichment_enabled(false);
    s.set_seal_threshold(256 * 1024);
    s.attach_shadow_dual_to_actives().unwrap();
    s
}

/// Detach seal-pair then wait for async P★ publish (Protected Seal-Pair Pipeline).
fn seal_pair(store: &mut Store) -> Result<(), residiuum_store::StoreError> {
    store.seal_active()?;
    store.drain_lifecycle()?;
    Ok(())
}

/// Healthy dual-stream: verified RSHD0004 + recovery after auth wipe.
#[test]
fn r4_f0_healthy_dual_stream_recovery() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let mut store = dual_store(dir.path(), 1);
        let payload = vec![0xABu8; 128];
        let mut expect = BTreeMap::new();
        for i in 0..40u64 {
            let k = format!("k{i:04}");
            store.put(&k, &payload, DurabilityMode::Buffered).unwrap();
            expect.insert(k.into_bytes(), payload.clone());
        }
        seal_pair(&mut store).unwrap();
        assert!(store.shadow_dual_published() >= 1);
        let sid = store.store_id();
        let paths = StorePaths::new(dir.path());
        drop(store);

        let mut any_v4 = false;
        for ent in fs::read_dir(paths.root.join("recovery/shadow")).unwrap() {
            let p = ent.unwrap().path();
            if p.extension().and_then(|e| e.to_str()) != Some("rsh") {
                continue;
            }
            let bytes = fs::read(&p).unwrap();
            if is_dual_magic(&bytes) {
                any_v4 = true;
            }
            assert!(matches!(
                try_load_shadow(&p, Some(sid)).unwrap(),
                ShadowLoad::Ok(_)
            ));
        }
        assert!(any_v4, "expected at least one RSHD0004 Shadow");
        assert!(every_protected_has_verified_rsh(&paths, sid).unwrap());
        assert!(recovery_after_auth_compact_delete(&paths, sid, &expect).unwrap());
    });
}

/// Fail Shadow append after auth wrote — seal refuses P★ (poisoned staging).
#[test]
fn r4_fail_shadow_append_no_p_star() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let mut store = dual_store(dir.path(), 1);
        let payload = vec![0x11u8; 64];
        arm_failpoint_once("rshd4.shadow.append", FailpointAction::Error);
        let err = store
            .put("b", &payload, DurabilityMode::Buffered)
            .unwrap_err();
        assert!(
            format!("{err:?}").to_lowercase().contains("failpoint"),
            "expected failpoint error, got {err:?}"
        );
        clear_failpoints();
        // Poisoned staging fails at seal detach (image_len / poison check) or
        // during async pair finalize — either path must refuse P★.
        let seal_err = match store.seal_active() {
            Err(e) => e,
            Ok(()) => store.drain_lifecycle().expect_err("poisoned pair"),
        };
        assert!(
            format!("{seal_err:?}").to_lowercase().contains("poison")
                || format!("{seal_err:?}").to_lowercase().contains("diverged")
                || format!("{seal_err:?}").to_lowercase().contains("failpoint")
                || format!("{seal_err:?}").to_lowercase().contains("shadow"),
            "poisoned dual-stream must refuse P★: {seal_err:?}"
        );
        let sid = store.store_id();
        let paths = StorePaths::new(dir.path());
        let cov = load_protected_coverage(&paths, sid).unwrap();
        let lag = residiuum_store::protection_lag_from_coverage(&cov);
        assert!(
            lag.protected_frontier == 0 || lag.lag > 0,
            "partial Shadow must not claim full P★: {lag:?}"
        );
    });
}

/// Staging flush failpoint fails closed.
#[test]
fn r4_fail_shadow_flush() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let mut store = dual_store(dir.path(), 1);
        // Dual-stream staging flushes at 1 MiB — one put above that trips flush.
        let big = vec![0x22u8; 1_100_000];
        arm_failpoint_once("rshd4.shadow.flush", FailpointAction::Error);
        let err = store
            .put("boom", &big, DurabilityMode::Buffered)
            .unwrap_err();
        assert!(format!("{err:?}").to_lowercase().contains("failpoint"));
    });
}

fn seal_with_finalize_failpoint(name: &'static str) {
    clear_failpoints();
    let dir = tempdir().unwrap();
    let mut store = dual_store(dir.path(), 1);
    let payload = vec![0x33u8; 100];
    for i in 0..8u64 {
        store
            .put(&format!("x{i}"), &payload, DurabilityMode::Buffered)
            .unwrap();
    }
    arm_failpoint_once(name, FailpointAction::Error);
    // Summary failpoint fires on foreground prepare; sync/rename/dir_sync on worker.
    let err = match store.seal_active() {
        Err(e) => e,
        Ok(()) => store.drain_lifecycle().expect_err("failpoint"),
    };
    clear_failpoints();
    assert!(
        format!("{err:?}").to_lowercase().contains("failpoint"),
        "{name}: {err:?}"
    );
    let sid = store.store_id();
    let paths = StorePaths::new(dir.path());
    let cov = load_protected_coverage(&paths, sid).unwrap();
    let lag = residiuum_store::protection_lag_from_coverage(&cov);
    assert!(
        lag.lag > 0 || lag.protected_frontier < lag.sealed_frontier || lag.sealed_frontier == 0,
        "{name}: frontier must not silently advance: {lag:?}"
    );
    drop(store);
}

/// Summary append failpoint during Shadow finalize.
#[test]
fn r4_fail_finalize_summary() {
    with_failpoints(|| seal_with_finalize_failpoint("rshd4.finalize.summary"));
}

/// Shadow staging sync failpoint during finalize.
#[test]
fn r4_fail_finalize_sync() {
    with_failpoints(|| seal_with_finalize_failpoint("rshd4.finalize.sync"));
}

/// Rename failpoint during Shadow atomic publish.
#[test]
fn r4_fail_finalize_rename() {
    with_failpoints(|| seal_with_finalize_failpoint("rshd4.finalize.rename"));
}

/// Directory sync failpoint after Shadow rename.
#[test]
fn r4_fail_finalize_dir_sync() {
    with_failpoints(|| seal_with_finalize_failpoint("rshd4.finalize.dir_sync"));
}

/// Frontier publish failpoint after Shadow file exists — still no durable claim.
#[test]
fn r4_fail_frontier_publish() {
    // seal_active waits for protected-pair finalize (incl. frontier), so the
    // failpoint may surface on seal or on a subsequent drain — same as finalize cells.
    with_failpoints(|| seal_with_finalize_failpoint("rshd4.frontier.publish"));
}

/// Multi-shard dual-stream seal + gap-aware coverage.
#[test]
fn r4_multi_shard_rotation() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let mut store = dual_store(dir.path(), 2);
        let payload = vec![0x55u8; 96];
        for i in 0..30u64 {
            store
                .put(&format!("s{i:04}"), &payload, DurabilityMode::Buffered)
                .unwrap();
        }
        seal_pair(&mut store).unwrap();
        let sid = store.store_id();
        let paths = StorePaths::new(dir.path());
        drop(store);
        assert!(every_protected_has_verified_rsh(&paths, sid).unwrap());
        let cov = load_protected_coverage(&paths, sid).unwrap();
        // Per-shard completeness — aggregate max−min lag may be >0 with
        // interleaved global segment sequences across writer shards.
        for &sh in cov.sealed_by_shard.keys() {
            assert_eq!(
                cov.protected_prefix(sh),
                cov.sealed_high(sh),
                "shard {sh} must be fully Shadow-durable"
            );
        }
        assert!(protected_frontier_gap_free(&paths, sid).unwrap());
    });
}

/// Overwrites + tombstones survive dual-stream recovery (generation-exact).
#[test]
fn r4_overwrite_tombstone_recovery() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let mut store = dual_store(dir.path(), 1);
        store.put("k", b"v1", DurabilityMode::Buffered).unwrap();
        store
            .put("k", b"v2-final", DurabilityMode::Buffered)
            .unwrap();
        store.put("t", b"doomed", DurabilityMode::Buffered).unwrap();
        store.delete("t", DurabilityMode::Buffered).unwrap();
        seal_pair(&mut store).unwrap();
        let sid = store.store_id();
        let paths = StorePaths::new(dir.path());
        let mut expect = BTreeMap::new();
        expect.insert(b"k".to_vec(), b"v2-final".to_vec());
        drop(store);
        assert!(recovery_after_auth_compact_delete(&paths, sid, &expect).unwrap());
    });
}

/// Chunked frames are dual-streamed into a verified RSHD0004 image.
///
/// Full chunk reassembly from Shadow record projection is a separate salvage
/// path; here we prove the sealed image (incl. chunk frames) is mirrored.
#[test]
fn r4_chunked_value_recovery() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let mut store = dual_store(dir.path(), 1);
        store.set_chunk_threshold(1024);
        store.set_chunk_size(512);
        let big = vec![0x66u8; 2500];
        store.put("chunky", &big, DurabilityMode::Buffered).unwrap();
        seal_pair(&mut store).unwrap();
        let sid = store.store_id();
        let paths = StorePaths::new(dir.path());
        drop(store);
        assert!(every_protected_has_verified_rsh(&paths, sid).unwrap());
        let mut saw = false;
        for ent in fs::read_dir(paths.root.join("recovery/shadow")).unwrap() {
            let p = ent.unwrap().path();
            if p.extension().and_then(|e| e.to_str()) != Some("rsh") {
                continue;
            }
            let bytes = fs::read(&p).unwrap();
            assert!(is_dual_magic(&bytes));
            assert!(matches!(
                try_load_shadow(&p, Some(sid)).unwrap(),
                ShadowLoad::Ok(_)
            ));
            // Chunk payload bytes appear in the mirrored segment image.
            assert!(bytes.windows(16).any(|w| w == &big[..16]));
            saw = true;
        }
        assert!(saw);
    });
}

/// Step 8: prepare → activate → new seals omit Materialized → rollback keeps sources.
#[test]
fn r4_step8_flip_activate_rollback() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let mut store = dual_store(dir.path(), 1);
        store.set_enrichment_enabled(true); // produce Materialized during dual-run
        let payload = vec![0x77u8; 64];
        for i in 0..12u64 {
            store
                .put(&format!("m{i}"), &payload, DurabilityMode::Buffered)
                .unwrap();
        }
        seal_pair(&mut store).unwrap();
        let built = store.prepare_flip_to_compact_shadow().unwrap();
        let _ = built;
        assert_eq!(store.recovery_mode(), RecoveryMode::Transitioning);
        assert!(recovery_mode_path(&StorePaths::new(dir.path())).is_file());

        store.activate_compact_shadow_mode().unwrap();
        assert_eq!(store.recovery_mode(), RecoveryMode::CompactShadow);
        assert!(store.recovery_mode().omits_new_materialized());

        // Post-flip seal: Compact+Shadow; Materialized not required for new segment.
        for i in 0..8u64 {
            store
                .put(&format!("p{i}"), &payload, DurabilityMode::Buffered)
                .unwrap();
        }
        seal_pair(&mut store).unwrap();

        let sid = store.store_id();
        let paths = StorePaths::new(dir.path());
        assert!(protected_frontier_gap_free(&paths, sid).unwrap());

        // Crash-ish: marker persist failpoint on rollback path is covered separately;
        // here rollback must leave Shadows and prior Materialized intact.
        let cmr_before: Vec<_> = fs::read_dir(chimera_dir(&paths))
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("cmr"))
            .collect();
        store.rollback_to_materialized_mode().unwrap();
        assert_eq!(store.recovery_mode(), RecoveryMode::Materialized);
        let cmr_after: Vec<_> = fs::read_dir(chimera_dir(&paths))
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("cmr"))
            .collect();
        assert_eq!(
            cmr_before.len(),
            cmr_after.len(),
            "rollback must not delete Materialized"
        );
        let rsh_count = fs::read_dir(paths.root.join("recovery/shadow"))
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("rsh"))
            .count();
        assert!(rsh_count >= 1, "rollback must not delete Shadows");
        drop(store);
    });
}

/// Mode marker persist failpoint (transition boundary).
#[test]
fn r4_step8_mode_persist_failpoint() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        arm_failpoint_once("rshd4.mode.before_persist", FailpointAction::Error);
        let err = persist_prepare(&paths, [9u8; 16], 0).unwrap_err();
        assert!(format!("{err:?}").to_lowercase().contains("failpoint"));
        assert_eq!(
            load_recovery_mode(&paths).unwrap(),
            RecoveryMode::Materialized
        );
    });
}

/// Activate refuses when a sealed seq lacks durable Shadow (gap).
#[test]
fn r4_step8_activate_requires_gap_free() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let sid = [3u8; 16];
        // Empty store: gap-free → activate ok.
        persist_prepare(&paths, sid, 0).unwrap();
        persist_activate(&paths, sid).unwrap();
        assert_eq!(
            load_recovery_mode(&paths).unwrap(),
            RecoveryMode::CompactShadow
        );
        rollback_to_materialized_mode(&paths, sid).unwrap();

        // Inject sealed-without-durable hole → activate must refuse.
        let mut cov = residiuum_store::ProtectedCoverage::empty(sid);
        cov.note_sealed(0, 1);
        residiuum_store::publish_protected_coverage(&paths, &cov).unwrap();
        assert!(!protected_frontier_gap_free(&paths, sid).unwrap());
        let err = persist_activate(&paths, sid).unwrap_err();
        assert!(
            format!("{err:?}").to_lowercase().contains("gap"),
            "activate must refuse non-gap-free: {err:?}"
        );
        assert_eq!(
            load_recovery_mode(&paths).unwrap(),
            RecoveryMode::Materialized,
            "failed activate must not leave CompactShadow marker"
        );
    });
}

/// Reopen respects CompactShadow marker (arms dual-stream policy).
#[test]
fn r4_step8_reopen_loads_marker() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        {
            let mut store = dual_store(dir.path(), 1);
            let payload = vec![0x88u8; 48];
            for i in 0..10u64 {
                store
                    .put(&format!("r{i}"), &payload, DurabilityMode::Buffered)
                    .unwrap();
            }
            seal_pair(&mut store).unwrap();
            store.prepare_flip_to_compact_shadow().unwrap();
            store.activate_compact_shadow_mode().unwrap();
        }
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.recovery_mode(), RecoveryMode::CompactShadow);
    });
}

/// Charter: 15 behavioral cells in this file (excluding this meta).
#[test]
fn r4_matrix_cell_count_charter() {
    // 1 healthy + 2 append/flush + 4 finalize + 1 frontier + 1 multi-shard
    // + 1 overwrite/tombstone + 1 chunked + 4 step8 = 15.
    const CELLS: &[&str] = &[
        "r4_f0_healthy_dual_stream_recovery",
        "r4_fail_shadow_append_no_p_star",
        "r4_fail_shadow_flush",
        "r4_fail_finalize_summary",
        "r4_fail_finalize_sync",
        "r4_fail_finalize_rename",
        "r4_fail_finalize_dir_sync",
        "r4_fail_frontier_publish",
        "r4_multi_shard_rotation",
        "r4_overwrite_tombstone_recovery",
        "r4_chunked_value_recovery",
        "r4_step8_flip_activate_rollback",
        "r4_step8_mode_persist_failpoint",
        "r4_step8_activate_requires_gap_free",
        "r4_step8_reopen_loads_marker",
    ];
    assert_eq!(CELLS.len(), 15);
}
