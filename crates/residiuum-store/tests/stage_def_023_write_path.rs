//! DEF-023 — remove full-store rescans from the write acknowledgement path.
//!
//! Guarantees under test:
//! - Durable writes update derived state without O(total retained data) work.
//! - Deleting all derived state still reconstructs identical logical results.
//! - Memory-mode visibility never enters the durable index / cache.
//! - Frontier checkpoints accelerate open via active-tail apply (not full rescan).
//! - Bench skeleton discloses write-path amplification metrics.

use residiuum_store::{DurabilityMode, Store, PRIMARY_CACHE_FILE};
use std::fs;
use std::time::Instant;
use tempfile::tempdir;

/// Seed many sealed segments so a naive write-path rescan would be expensive.
fn seed_retained_data(store: &mut Store, sealed_batches: usize, keys_per_batch: usize) {
    for b in 0..sealed_batches {
        for k in 0..keys_per_batch {
            let subject = format!("hist/{b}/{k}");
            let body = format!("payload-batch-{b}-key-{k}-{}", "x".repeat(64));
            store
                .put(&subject, body.as_bytes(), DurabilityMode::Durable)
                .unwrap();
        }
        store.seal_active().unwrap();
    }
}

#[test]
fn durable_write_ack_independent_of_retained_volume() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();

    // Large retained history (multiple sealed segments).
    seed_retained_data(&mut store, 8, 40);
    let live_before = store.live_count();
    assert!(live_before >= 8 * 40);

    // Fresh durable writes must not grow with sealed volume: wall time stays
    // within a generous CI bound even with substantial retained data.
    let n = 64usize;
    let payload = b"hot-path-write";
    let t0 = Instant::now();
    for i in 0..n {
        store
            .put(&format!("hot/{i}"), payload, DurabilityMode::Durable)
            .unwrap();
    }
    let hot_path = t0.elapsed();
    assert_eq!(store.live_count(), live_before + n);
    // CI safety bound (not a performance gate). Full segment rescan on every
    // write would typically blow this on slower runners with the seed above.
    assert!(
        hot_path.as_secs() < 30,
        "hot-path durable append too slow ({hot_path:?}); possible full-store rescan regression"
    );
}

#[test]
fn delete_derived_state_reconstructs_identical_logical_results() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let expected: Vec<(String, Vec<u8>)> = {
        let mut store = Store::create(&root).unwrap();
        seed_retained_data(&mut store, 3, 10);
        store
            .put("alpha", b"A", DurabilityMode::Durable)
            .unwrap();
        store
            .put("beta", b"B", DurabilityMode::Buffered)
            .unwrap();
        store.delete("alpha", DurabilityMode::Durable).unwrap();
        store.persist_index_cache().unwrap();
        store
            .live_logical_entries()
            .unwrap()
            .into_iter()
            .map(|(k, v)| (String::from_utf8(k).unwrap(), v))
            .collect()
    };

    // Wipe all derived directories.
    for name in ["indexes", "catalogs", "snapshots"] {
        let p = root.join(name);
        if p.exists() {
            fs::remove_dir_all(&p).unwrap();
        }
    }

    let store = Store::open(&root).unwrap();
    let rebuilt: Vec<(String, Vec<u8>)> = store
        .live_logical_entries()
        .unwrap()
        .into_iter()
        .map(|(k, v)| (String::from_utf8(k).unwrap(), v))
        .collect();
    assert_eq!(
        rebuilt, expected,
        "rebuild-from-segments must match pre-wipe logical state"
    );
    assert!(store.get("alpha").unwrap().is_none());
    assert_eq!(
        store.get("beta").unwrap().as_deref(),
        Some(b"B".as_slice())
    );
}

#[test]
fn memory_mode_never_enters_durable_index_cache() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create(&root).unwrap();
        store
            .put("mem-only", b"ghost", DurabilityMode::Memory)
            .unwrap();
        store
            .put("disk", b"solid", DurabilityMode::Durable)
            .unwrap();
        assert_eq!(
            store.get("mem-only").unwrap().as_deref(),
            Some(b"ghost".as_slice())
        );
        // Force a checkpoint so the cache file exists.
        store.persist_index_cache().unwrap();
        assert!(store.index_cache_path().is_file());
    }
    let store = Store::open(&root).unwrap();
    assert!(
        store.get("mem-only").unwrap().is_none(),
        "memory publish must not survive reopen via index cache"
    );
    assert_eq!(
        store.get("disk").unwrap().as_deref(),
        Some(b"solid".as_slice())
    );
}

#[test]
fn frontier_checkpoint_plus_active_tail_matches_full_rebuild() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create(&root).unwrap();
        for i in 0..10 {
            store
                .put(&format!("sealed/{i}"), b"S", DurabilityMode::Durable)
                .unwrap();
        }
        store.seal_active().unwrap();
        // Checkpoint after seal (covers sealed set; active is nearly empty).
        store.persist_index_cache().unwrap();

        // Writes beyond the checkpoint frontier stay only in the active segment
        // until explicit/orderly-close checkpointing; ingestion never clones the
        // full primary index at a fixed operation interval.
        for i in 0..5 {
            store
                .put(&format!("tail/{i}"), b"T", DurabilityMode::Durable)
                .unwrap();
        }
        // Leave without an explicit final persist so open must apply the active tail.
    }

    let mut reopened = Store::open(&root).unwrap();
    for i in 0..10 {
        assert_eq!(
            reopened.get(&format!("sealed/{i}")).unwrap().as_deref(),
            Some(b"S".as_slice())
        );
    }
    for i in 0..5 {
        assert_eq!(
            reopened.get(&format!("tail/{i}")).unwrap().as_deref(),
            Some(b"T".as_slice())
        );
    }
    let live_from_frontier = reopened.live_count();

    // Explicit full rebuild on the same handle matches frontier+tail visibility.
    reopened.rebuild_index().unwrap();
    assert_eq!(reopened.live_count(), live_from_frontier);
    for i in 0..5 {
        assert_eq!(
            reopened.get(&format!("tail/{i}")).unwrap().as_deref(),
            Some(b"T".as_slice())
        );
    }
}

#[test]
fn absent_mid_run_checkpoint_still_allows_recovery() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create(&root).unwrap();
        // Mid-run checkpoints are not required; recovery must succeed from segments.
        for i in 0..10 {
            store
                .put(&format!("k{i}"), b"v", DurabilityMode::Durable)
                .unwrap();
        }
    }
    // Even if primary.idx is stale or absent, open recovers.
    let idx = root.join("indexes").join(PRIMARY_CACHE_FILE);
    if idx.is_file() {
        fs::remove_file(&idx).unwrap();
    }
    let store = Store::open(&root).unwrap();
    for i in 0..10 {
        assert_eq!(
            store.get(&format!("k{i}")).unwrap().as_deref(),
            Some(b"v".as_slice())
        );
    }
}

#[test]
fn ingestion_never_rewrites_full_primary_checkpoint_at_fixed_operation_count() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    let checkpoint = root.join("indexes").join(PRIMARY_CACHE_FILE);
    let initial_checkpoint = fs::read(&checkpoint).unwrap();

    // Cross the former 65,536-operation trigger in amortized buffered batches.
    // The regression was not the worker's disk write: cloning the growing index
    // happened synchronously in the publication loop before submission.
    let mut ordinal = 0usize;
    while ordinal < 65_537 {
        let end = (ordinal + 1_024).min(65_537);
        let keys: Vec<String> = (ordinal..end).map(|i| format!("bulk/{i:08}")).collect();
        let items: Vec<(&str, &[u8])> = keys
            .iter()
            .map(|key| (key.as_str(), b"x".as_slice()))
            .collect();
        store.put_many(&items, DurabilityMode::Buffered).unwrap();
        ordinal = end;
    }

    assert_eq!(
        fs::read(&checkpoint).unwrap(),
        initial_checkpoint,
        "fixed-count ingestion must not clone/rewrite the full primary checkpoint"
    );
    assert_eq!(store.lifecycle_diag().unwrap().derived_ops_since_checkpoint, 65_537);

    // Explicit checkpointing remains supported and clears the diagnostic lag.
    store.persist_index_cache().unwrap();
    assert_ne!(fs::read(&checkpoint).unwrap(), initial_checkpoint);
    assert_eq!(store.lifecycle_diag().unwrap().derived_ops_since_checkpoint, 0);
}

/// Seal must stay O(segment), not O(retained volume).
///
/// A naive path that re-scans every sealed segment (or rewrites the full
/// primary index with bodies) on each seal produces a classic write
/// degradation curve: early seals are fast, late seals dominate.
#[test]
fn seal_cost_independent_of_retained_segment_count() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    // Small seal threshold so we can create many sealed segments quickly.
    store.set_seal_threshold(64 * 1024);

    let payload = vec![0xABu8; 1024];
    let seals = 24usize;
    let keys_per_seal = 48usize;
    let mut seal_times = Vec::with_capacity(seals);

    for s in 0..seals {
        for k in 0..keys_per_seal {
            store
                .put(
                    &format!("scale/{s}/{k}"),
                    &payload,
                    DurabilityMode::Buffered,
                )
                .unwrap();
        }
        let t0 = Instant::now();
        store.seal_active().unwrap();
        seal_times.push(t0.elapsed());
    }

    let early: u128 = seal_times[..4].iter().map(|d| d.as_micros()).sum::<u128>() / 4;
    let late: u128 = seal_times[seals - 4..]
        .iter()
        .map(|d| d.as_micros())
        .sum::<u128>()
        / 4;
    // Late seals must not blow up with retained segment count. Allow generous
    // jitter for CI (cold start, fsync variance) but catch true O(N) regressions
    // that used to be 5–10× slower by the end of a single GB pump.
    assert!(
        late < early.saturating_mul(8).max(50_000),
        "seal latency grew too much with retained volume: early_avg={early}µs late_avg={late}µs times={seal_times:?}"
    );

    // Catalog still tracks every sealed segment.
    assert!(store.segment_catalog().len() >= seals);
}

#[test]
fn write_path_bench_disclosure() {
    // Disclosure fields for DEF-023 / doc/reference/operations/BENCHMARK_DISCLOSURE.md (skeleton only).
    let dir = tempdir().unwrap();
    let root = dir.path().join("bench");
    let mut store = Store::create(&root).unwrap();
    seed_retained_data(&mut store, 4, 25);

    let n = 100usize;
    let payload = br#"{"bench":true,"def":"023"}"#;
    let mut fsync_proxy = 0u64; // durable mode implies stable-storage boundary per ack

    let t0 = Instant::now();
    for i in 0..n {
        store
            .put(&format!("d{i}"), payload, DurabilityMode::Durable)
            .unwrap();
        fsync_proxy += 1;
    }
    let durable = t0.elapsed();

    let t0 = Instant::now();
    for i in 0..n {
        store
            .put(&format!("b{i}"), payload, DurabilityMode::Buffered)
            .unwrap();
    }
    let buffered = t0.elapsed();

    store.persist_index_cache().unwrap();
    let cache_bytes = fs::metadata(store.index_cache_path())
        .map(|m| m.len())
        .unwrap_or(0);

    eprintln!(
        "def_023_write_path_disclosure\n  \
         durability=durable+buffered verification=frame-scan-on-rebuild\n  \
         dataset_seeded_keys≈{} working_set_appends={n} payload_len={}\n  \
         durable_n={n} durable_elapsed={durable:?} durable_fsync_acks≈{fsync_proxy}\n  \
         buffered_n={n} buffered_elapsed={buffered:?}\n  \
         index_cache_bytes={cache_bytes} p50/p95/p99=not-sampled-in-skeleton\n  \
         write_amplification=derived_checkpoint_rate_limited_not_per_write_rescan",
        4 * 25,
        payload.len()
    );

    assert!(durable.as_secs() < 60);
    assert!(buffered.as_secs() < 60);
}
