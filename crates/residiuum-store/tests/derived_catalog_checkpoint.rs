//! Derived catalog checkpointing: memory SealDone, lagging/rebuildable disk.

use residiuum_store::{DurabilityMode, Store, SEGMENT_CATALOG_FILE, TIER_PLACEMENT_FILE};
use std::fs;
use std::time::Instant;

fn put_until_sealed(store: &mut Store, n_keys: usize, payload: &[u8]) {
    for i in 0..n_keys {
        let key = format!("k{i:06}");
        store.put(&key, payload, DurabilityMode::Buffered).unwrap();
    }
}

#[test]
fn reopen_exact_before_pending_catalog_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store");
    let mut store = Store::create(&store_path).unwrap();
    store.set_seal_threshold(32 * 1024);
    store.set_enrichment_enabled(false);

    let payload = vec![0xCDu8; 2048];
    // A few seals — below coalesce threshold (32) and well under the 2s timer.
    put_until_sealed(&mut store, 40, &payload);
    store.wait_seals_applied().unwrap();

    let sealed = store.lifecycle_diag().unwrap().sealed_segments;
    assert!(sealed >= 1 && sealed < 32, "sealed={sealed}");

    let keys = 40usize;
    // Apply seals to memory only — do not drain (drain flushes catalogs).
    drop(store);

    // Simulate a still-pending / missing checkpoint after crash-style close.
    let catalogs = store_path.join("catalogs");
    let _ = fs::remove_file(catalogs.join(TIER_PLACEMENT_FILE));
    let _ = fs::remove_file(catalogs.join(SEGMENT_CATALOG_FILE));

    let store = Store::open(&store_path).unwrap();
    for i in 0..keys {
        let key = format!("k{i:06}");
        let got = store.get(&key).unwrap().expect("missing after reopen");
        assert_eq!(got.as_slice(), payload.as_slice());
    }
}

#[test]
fn reopen_exact_after_deleting_derived_catalogs() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store");
    let mut store = Store::create(&store_path).unwrap();
    store.set_seal_threshold(32 * 1024);
    store.set_enrichment_enabled(false);

    let payload = vec![0xEFu8; 2048];
    put_until_sealed(&mut store, 60, &payload);
    store.drain_lifecycle().unwrap();
    let keys = 60usize;
    drop(store);

    let catalogs = store_path.join("catalogs");
    fs::remove_file(catalogs.join(TIER_PLACEMENT_FILE)).ok();
    fs::remove_file(catalogs.join(SEGMENT_CATALOG_FILE)).ok();

    let store = Store::open(&store_path).unwrap();
    for i in 0..keys {
        let key = format!("k{i:06}");
        let got = store
            .get(&key)
            .unwrap()
            .expect("missing after catalog wipe");
        assert_eq!(got.as_slice(), payload.as_slice());
    }
}

#[test]
fn catalog_apply_cost_flat_across_segment_counts() {
    // Tiny segments: prove per-rotation catalog_apply stays O(1) / flat as
    // sealed count grows (32 → 256 → 1024). Full 64 MiB campaign is separate.
    let payload = vec![0x11u8; 512];
    let targets = [32usize, 256usize, 1024usize];
    let mut per_rot_ns = Vec::new();

    for &target in &targets {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        let mut store = Store::create(&store_path).unwrap();
        store.set_seal_threshold(8 * 1024);
        store.set_enrichment_enabled(false);

        let t0 = Instant::now();
        let mut i = 0usize;
        loop {
            let key = format!("k{i:08}");
            store.put(&key, &payload, DurabilityMode::Buffered).unwrap();
            i += 1;
            if i % 64 == 0 {
                let _ = store.wait_seals_applied();
                let sealed = store.lifecycle_diag().unwrap().sealed_segments;
                if sealed >= target {
                    break;
                }
            }
            // Safety: avoid runaway if seals stall.
            assert!(i < target.saturating_mul(200), "stuck targeting {target}");
        }
        store.wait_seals_applied().unwrap();
        let totals = store.rotation_stage_totals();
        assert!(
            totals.rotations >= target as u64,
            "target={target} rotations={}",
            totals.rotations
        );
        let avg = totals.catalog_apply_ns / totals.rotations.max(1);
        per_rot_ns.push((target, avg, totals.catalog_apply_ns, t0.elapsed()));
        drop(store);
    }

    // 1024-seg average must stay within 8× of the 32-seg average (allows noise;
    // the old every-seal full rewrite was ~linear in n → ~32× here).
    let avg32 = per_rot_ns[0].1.max(1);
    let avg1024 = per_rot_ns[2].1;
    assert!(
        avg1024 < avg32.saturating_mul(8),
        "catalog_apply grew too much: {:?}",
        per_rot_ns
    );
}
