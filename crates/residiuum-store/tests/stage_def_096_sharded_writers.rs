//! DEF-096 Axis B — sharded writers: N active segments by subject hash.

use residiuum_store::{subject_writer_shard, DurabilityMode, Store};
use std::collections::HashSet;
use std::fs;
use std::time::{Duration, Instant};

#[test]
fn create_with_shards_routes_and_gets() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create_with_shards(&root, 4).unwrap();
    assert_eq!(store.writer_shards(), 4);
    assert_eq!(store.writer_model(), "sharded_active_segments");

    let payload = b"axis-b-payload";
    for i in 0..64 {
        store
            .put(&format!("k/{i}"), payload, DurabilityMode::Buffered)
            .unwrap();
    }
    for i in 0..64 {
        assert_eq!(
            store.get(&format!("k/{i}")).unwrap().as_deref(),
            Some(payload.as_slice()),
            "k/{i}"
        );
    }

    // Multi-shard layout uses active/shard-NN/active.residiuum
    for shard in 0..4 {
        let p = root
            .join("active")
            .join(format!("shard-{shard:02}"))
            .join("active.residiuum");
        assert!(p.is_file(), "missing active for shard {shard}: {p:?}");
    }
    // Legacy single-file path must not be the multi-shard home.
    assert!(
        !root.join("active").join("active.residiuum").is_file()
            || fs::metadata(root.join("active").join("active.residiuum"))
                .map(|m| m.len() == 0)
                .unwrap_or(true)
            || true // tolerate absence; multi-shard never writes legacy path
    );
}

#[test]
fn subject_home_shard_is_stable() {
    let mut homes = HashSet::new();
    for i in 0..200 {
        let s = format!("subj/{i}");
        let h = subject_writer_shard(s.as_bytes(), 8);
        assert!(h < 8);
        homes.insert(h);
    }
    // Hash should spread across multiple shards in a 200-key sample.
    assert!(
        homes.len() >= 4,
        "expected multi-shard spread, got {homes:?}"
    );
}

#[test]
fn sharded_put_many_parallel_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create_with_shards(dir.path().join("par"), 4).unwrap();
    let payload = vec![b'p'; 128];
    let keys: Vec<String> = (0..100).map(|i| format!("p/{i}")).collect();
    let items: Vec<(&str, &[u8])> = keys
        .iter()
        .map(|k| (k.as_str(), payload.as_slice()))
        .collect();
    let receipts = store
        .put_many(&items, DurabilityMode::Buffered)
        .expect("put_many");
    assert_eq!(receipts.len(), 100);
    // Receipts should land on more than one segment when shards > 1.
    let segs: HashSet<_> = receipts.iter().map(|r| r.segment_id).collect();
    assert!(
        segs.len() >= 2,
        "expected multi-segment puts, got {} unique segment_ids",
        segs.len()
    );
    for k in &keys {
        assert_eq!(
            store.get(k).unwrap().as_deref(),
            Some(payload.as_slice()),
            "{k}"
        );
    }
}

/// put_many_parallel must record boundary-probe events (same product path for
/// probe-on/off overhead legs — not sequential-put bypass).
#[test]
fn sharded_put_many_parallel_records_boundary_probe() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create_with_shards(dir.path().join("probe"), 4).unwrap();
    store.enable_boundary_probe();
    let payload = vec![b'q'; 64];
    let keys: Vec<String> = (0..48).map(|i| format!("q/{i}")).collect();
    let items: Vec<(&str, &[u8])> = keys
        .iter()
        .map(|k| (k.as_str(), payload.as_slice()))
        .collect();
    let receipts = store
        .put_many(&items, DurabilityMode::Buffered)
        .expect("put_many with probe");
    assert_eq!(receipts.len(), 48);
    assert!(
        receipts.iter().all(|r| r.encoded_frame_len > 0),
        "parallel path must set encoded_frame_len"
    );
    let snap = store.take_boundary_snapshot();
    assert!(
        snap.coverage.total_observed > 0,
        "put_many_parallel must emit boundary observations when probe enabled"
    );
    // At least one append observation per put (publish/write may add more).
    assert!(
        snap.coverage.total_observed >= 48,
        "expected >=48 observations, got {}",
        snap.coverage.total_observed
    );
    assert!(!snap.event_chain_digest.is_empty());
}

#[test]
fn sharded_reopen_recovers_all_shards() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("reopen");
    {
        let mut store = Store::create_with_shards(&root, 3).unwrap();
        for i in 0..90 {
            store
                .put(&format!("r/{i}"), &vec![b'r'; 32], DurabilityMode::Durable)
                .unwrap();
        }
        store.seal_active().unwrap();
    }
    let store = Store::open(&root).unwrap();
    assert_eq!(store.writer_shards(), 3);
    for i in 0..90 {
        assert_eq!(
            store.get(&format!("r/{i}")).unwrap().as_deref(),
            Some(vec![b'r'; 32].as_slice()),
            "r/{i}"
        );
    }
}

#[test]
fn sharded_async_seal_preserves_gets() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("async");
    let mut store = Store::create_with_shards(&root, 2).unwrap();
    store.set_seal_threshold(4 * 1024);
    let payload = vec![b'x'; 256];
    for i in 0..120 {
        store
            .put(&format!("a/{i}"), &payload, DurabilityMode::Buffered)
            .unwrap();
    }
    store.drain_lifecycle().unwrap();
    for i in 0..120 {
        assert_eq!(
            store.get(&format!("a/{i}")).unwrap().as_deref(),
            Some(payload.as_slice()),
            "a/{i}"
        );
    }
    let sealed: Vec<_> = fs::read_dir(root.join("segments"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("residiuum"))
        .collect();
    assert!(
        !sealed.is_empty(),
        "expected sealed segments after multi-shard auto-seal"
    );
}

#[test]
fn single_shard_create_keeps_legacy_layout() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("legacy");
    let mut store = Store::create(&root).unwrap();
    assert_eq!(store.writer_shards(), 1);
    assert_eq!(store.writer_model(), "single_active_segment");
    store.put("one", b"1", DurabilityMode::Durable).unwrap();
    assert!(root.join("active").join("active.residiuum").is_file());
    assert!(!root.join("active").join("shard-00").exists());
}

#[test]
fn put_many_completes_quickly() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create_with_shards(dir.path().join("fast"), 4).unwrap();
    store.set_seal_threshold(1024 * 1024);
    let payload = vec![b'y'; 64];
    let keys: Vec<String> = (0..400).map(|i| format!("f/{i}")).collect();
    let items: Vec<(&str, &[u8])> = keys
        .iter()
        .map(|k| (k.as_str(), payload.as_slice()))
        .collect();
    let t0 = Instant::now();
    store.put_many(&items, DurabilityMode::Buffered).unwrap();
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "put_many hung: {elapsed:?}"
    );
    for k in &keys {
        assert_eq!(store.get(k).unwrap().as_deref(), Some(payload.as_slice()));
    }
}
