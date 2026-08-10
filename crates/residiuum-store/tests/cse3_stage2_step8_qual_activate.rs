//! CSE-3 Stage 2 step 8 — qualification-store activation ceremony.
//!
//! Exercises Materialized → CompactShadow migration on an isolated store.
//! Fresh product `Store::create` defaults to CompactShadow (Stage 2k); this
//! ceremony remains the path for legacy Materialized trees.
//!
//! ```text
//! cargo test -p residiuum-store --features legacy-raw-store --release \
//!   --test cse3_stage2_step8_qual_activate -- --nocapture
//! ```

use residiuum_store::{
    chimera_dir, every_protected_has_verified_rsh, load_protected_coverage,
    protected_frontier_gap_free, segment_id_from_filename, try_load_chimera_layout, DurabilityMode,
    RecoveryMode, Store, StorePaths,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

fn list_cmr(paths: &StorePaths) -> BTreeMap<PathBuf, u64> {
    let mut out = BTreeMap::new();
    let dir = chimera_dir(paths);
    if !dir.is_dir() {
        return out;
    }
    for ent in fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = ent.path();
        if p.extension().and_then(|e| e.to_str()) == Some("cmr") {
            let len = ent.metadata().map(|m| m.len()).unwrap_or(0);
            out.insert(p, len);
        }
    }
    out
}

fn list_rsh(paths: &StorePaths) -> Vec<PathBuf> {
    let dir = paths.root.join("recovery/shadow");
    let mut out = Vec::new();
    if !dir.is_dir() {
        return out;
    }
    for ent in fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = ent.path();
        if p.extension().and_then(|e| e.to_str()) == Some("rsh") {
            out.push(p);
        }
    }
    out
}

fn layout_is_locator_only(path: &Path, store_id: [u8; 16], segment_id: [u8; 16]) -> bool {
    let Some(layout) = try_load_chimera_layout(path, store_id, segment_id)
        .ok()
        .flatten()
    else {
        return false;
    };
    let c = layout.count_by_kind();
    c.segment_frame > 0
        && c.resident == 0
        && c.inline == 0
        && c.point_container == 0
        && c.scan_extent == 0
        && c.large_value_log == 0
}

/// Full Step 8 activation ceremony on an isolated qualification store.
#[test]
fn step8_qual_store_activation_ceremony() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();

    // --- Dual-run Materialized baseline (legacy / migration fixture) ---
    let mut store = Store::create_with_shards_mode(root, 2, RecoveryMode::Materialized).unwrap();
    store.set_enrichment_enabled(true);
    store.set_seal_threshold(256 * 1024);
    assert_eq!(store.recovery_mode(), RecoveryMode::Materialized);

    let payload = vec![0xA5u8; 96];
    for i in 0..40u64 {
        store
            .put(&format!("pre/{i:04}"), &payload, DurabilityMode::Buffered)
            .unwrap();
    }
    store.seal_active().unwrap();
    let sid = store.store_id();
    let paths = StorePaths::new(root);

    let cmr_before_flip = list_cmr(&paths);
    assert!(
        !cmr_before_flip.is_empty(),
        "pre-flip seals must create Materialized .cmr"
    );
    // Materialized payloads embed bodies — not locator-only.
    let mut saw_materialized = false;
    for p in cmr_before_flip.keys() {
        let Some(seg) = segment_id_from_filename(p) else {
            continue;
        };
        if !layout_is_locator_only(p, sid, seg) {
            saw_materialized = true;
        }
    }
    assert!(
        saw_materialized,
        "pre-flip Chimera must include Materialized (non-locator-only) layouts"
    );

    // 1–2. Prepare Shadows for every sealed + gap-free per shard.
    let built = store.prepare_flip_to_compact_shadow().unwrap();
    let _ = built;
    assert_eq!(store.recovery_mode(), RecoveryMode::Transitioning);
    assert!(protected_frontier_gap_free(&paths, sid).unwrap());
    assert!(every_protected_has_verified_rsh(&paths, sid).unwrap());
    let cov = load_protected_coverage(&paths, sid).unwrap();
    for &sh in cov.sealed_by_shard.keys() {
        assert_eq!(
            cov.protected_prefix(sh),
            cov.sealed_high(sh),
            "shard {sh} must be gap-free before activate"
        );
    }

    // 3. Existing Materialized files remain intact through prepare.
    let cmr_after_prepare = list_cmr(&paths);
    assert_eq!(
        cmr_before_flip, cmr_after_prepare,
        "prepare must not rewrite/delete Materialized"
    );

    // 4. Persist RMODE001 = CompactShadow.
    store.activate_compact_shadow_mode().unwrap();
    assert_eq!(store.recovery_mode(), RecoveryMode::CompactShadow);
    assert!(store.recovery_mode().omits_new_materialized());
    drop(store);

    // 5. Restart — mode survives reopen.
    let mut store = Store::open(root).unwrap();
    assert_eq!(store.recovery_mode(), RecoveryMode::CompactShadow);
    store.set_enrichment_enabled(true);
    store.set_seal_threshold(256 * 1024);

    let cmr_pre_postflip = list_cmr(&paths);
    let rsh_pre_postflip = list_rsh(&paths).len();

    // 6. Additional writes under CompactShadow product mode.
    for i in 0..24u64 {
        store
            .put(&format!("post/{i:04}"), &payload, DurabilityMode::Buffered)
            .unwrap();
    }
    store.seal_active().unwrap();
    store.wait_seals_applied().unwrap();
    store.drain_lifecycle().unwrap();
    store
        .drain_enrichment(std::time::Duration::from_secs(60))
        .unwrap();

    let cmr_after = list_cmr(&paths);
    assert!(
        cmr_after.len() > cmr_pre_postflip.len(),
        "post-flip seal must write new Compact Chimera"
    );
    // Prior Materialized byte-identical.
    for (p, len) in &cmr_pre_postflip {
        assert_eq!(
            cmr_after.get(p),
            Some(len),
            "existing Materialized must remain intact: {}",
            p.display()
        );
    }
    // New .cmr are locator-only Compact.
    for (p, _) in &cmr_after {
        if cmr_pre_postflip.contains_key(p) {
            continue;
        }
        let Some(seg) = segment_id_from_filename(p) else {
            panic!("unparseable new chimera {}", p.display());
        };
        assert!(
            layout_is_locator_only(p, sid, seg),
            "new post-flip Chimera must be locator-only Compact: {}",
            p.display()
        );
    }
    assert!(
        list_rsh(&paths).len() > rsh_pre_postflip,
        "post-flip seals must publish new Recovery Shadows"
    );
    assert!(every_protected_has_verified_rsh(&paths, sid).unwrap());
    assert!(protected_frontier_gap_free(&paths, sid).unwrap());

    // Spot-check product get.
    assert_eq!(
        store.get("post/0000").unwrap().as_deref(),
        Some(payload.as_slice())
    );
    assert_eq!(
        store.get("pre/0000").unwrap().as_deref(),
        Some(payload.as_slice())
    );

    let rsh_before_rollback = list_rsh(&paths).len();
    let cmr_before_rollback = list_cmr(&paths);

    // 7. Rollback — neither recovery source deleted.
    store.rollback_to_materialized_mode().unwrap();
    assert_eq!(store.recovery_mode(), RecoveryMode::Materialized);
    assert_eq!(list_rsh(&paths).len(), rsh_before_rollback);
    assert_eq!(list_cmr(&paths), cmr_before_rollback);
    drop(store);

    // Reopen after rollback still Materialized (marker explicit).
    let store = Store::open(root).unwrap();
    assert_eq!(store.recovery_mode(), RecoveryMode::Materialized);
    // Fresh create elsewhere is CompactShadow (Stage 2k product default).
    let other = tempfile::tempdir().unwrap();
    let fresh = Store::create(other.path()).unwrap();
    assert_eq!(
        fresh.recovery_mode(),
        RecoveryMode::CompactShadow,
        "fresh Store::create must default to CompactShadow"
    );
    assert!(fresh.shadow_dual_stream());
}
