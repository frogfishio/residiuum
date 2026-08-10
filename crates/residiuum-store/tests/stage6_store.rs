//! Stage 6 store exit criteria: catalogs, history, chunks, compaction, salvage.

use residiuum_store::{DurabilityMode, EventKind, PayloadResult, Store, DEFAULT_CHUNK_SIZE};
use std::fs;
use tempfile::tempdir;

#[test]
fn wipe_indexes_and_catalogs_rebuild_same_content() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let mut store = Store::create(&path).unwrap();
        store.put("a", b"one", DurabilityMode::Durable).unwrap();
        store.put("b", b"two", DurabilityMode::Durable).unwrap();
        store.delete("a", DurabilityMode::Durable).unwrap();
        assert_eq!(store.get("b").unwrap().as_deref(), Some(b"two".as_slice()));
        assert!(store.get("a").unwrap().is_none());
        // Ensure catalog/cache exist (path may be written lazily by rebuild).
        store.rebuild_catalogs().unwrap();
        let _ = store.index_cache_path();
    }

    // Wipe derived dirs.
    let store = Store::open(&path).unwrap();
    for d in store.derived_dirs() {
        if d.exists() {
            fs::remove_dir_all(&d).unwrap();
        }
    }
    drop(store);

    let mut store = Store::open(&path).unwrap();
    store.rebuild_index().unwrap();
    assert_eq!(store.get("b").unwrap().as_deref(), Some(b"two".as_slice()));
    assert!(store.get("a").unwrap().is_none());
    assert_eq!(store.live_count(), 1);
}

#[test]
fn subject_history_orders_puts_and_deletes() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.put("user", b"v1", DurabilityMode::Durable).unwrap();
    store.put("user", b"v2", DurabilityMode::Durable).unwrap();
    store.delete("user", DurabilityMode::Durable).unwrap();
    store.put("user", b"v3", DurabilityMode::Durable).unwrap();

    let hist = store.history("user").unwrap();
    assert_eq!(hist.events.len(), 4);
    assert_eq!(hist.events[0].kind, EventKind::Put);
    assert_eq!(hist.events[0].body, b"v1");
    assert_eq!(hist.events[1].body, b"v2");
    assert_eq!(hist.events[2].kind, EventKind::Delete);
    assert_eq!(hist.events[3].body, b"v3");
    assert!(!hist.has_known_holes);
}

#[test]
fn chunked_put_get_and_partial_missing_middle() {
    use residiuum_format::{decode_chunk_body, scan_forward, FrameKind, SafetyLimits};
    use residiuum_store::{decode_chunk_manifest, reassemble_with_manifest, resolve_piece};

    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(20);
    store.set_chunk_size(8);

    let payload: Vec<u8> = (0u8..60).collect();
    store.put("big", &payload, DurabilityMode::Durable).unwrap();
    assert_eq!(
        store.get("big").unwrap().as_deref(),
        Some(payload.as_slice())
    );

    // Index stores a chunk manifest, not the full body.
    let raw = store
        .live_entries()
        .find(|(k, _)| *k == b"big".as_slice())
        .map(|(_, b)| b.to_vec())
        .unwrap();
    assert!(residiuum_store::is_chunk_manifest(&raw));
    let manifest = decode_chunk_manifest(&raw).unwrap();
    assert!(manifest.chunks.len() >= 3);

    match store.get_payload("big").unwrap() {
        Some(PayloadResult::Complete { body }) => assert_eq!(body, payload),
        other => panic!("expected complete: {other:?}"),
    }

    // Collect verified chunk pieces with event ids, drop the middle index, reassemble.
    store.seal_active().unwrap();
    let mut resolved = Vec::new();
    for path in fs::read_dir(store.path().join("segments")).unwrap() {
        let p = path.unwrap().path();
        let bytes = fs::read(&p).unwrap();
        let report = scan_forward(&bytes, SafetyLimits::default());
        for (off, frame) in report.verified_frames() {
            if frame.header.known_kind() != Some(FrameKind::PayloadChunk) {
                continue;
            }
            if let Some(piece) = decode_chunk_body(&frame.body) {
                resolved.push(resolve_piece(frame.header.event_id, piece, [0u8; 16], off));
            }
        }
    }
    assert!(resolved.len() >= 3);
    resolved.sort_by_key(|r| r.piece.index);
    let middle = resolved[1].piece.index;
    let item_id = resolved[0].piece.item_id;
    let surviving: Vec<_> = resolved
        .into_iter()
        .filter(|r| r.piece.index != middle)
        .collect();
    match reassemble_with_manifest(item_id, &manifest, &surviving) {
        PayloadResult::Partial {
            missing, extents, ..
        } => {
            assert_eq!(missing, vec![middle]);
            assert_eq!(extents.len(), manifest.chunks.len());
            assert!(!extents[middle as usize].present);
            // Completeness never overstated: ordinary get would reject partial.
            assert!(!matches!(
                reassemble_with_manifest(item_id, &manifest, &surviving),
                PayloadResult::Complete { .. }
            ));
        }
        other => panic!("expected partial missing middle: {other:?}"),
    }
    let _ = DEFAULT_CHUNK_SIZE;
}

#[test]
fn compact_live_preserves_current_values() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.put("x", b"1", DurabilityMode::Durable).unwrap();
    store.put("y", b"2", DurabilityMode::Durable).unwrap();
    store.delete("x", DurabilityMode::Durable).unwrap();
    let report = store.compact_live().unwrap();
    assert!(report.sources_retained);
    assert_eq!(report.coverage, "live-projection");
    assert_eq!(report.live_subjects_written, 1);
    assert_eq!(store.get("y").unwrap().as_deref(), Some(b"2".as_slice()));
    assert!(store.get("x").unwrap().is_none());
    // History still present (sources retained).
    let hist = store.history("x").unwrap();
    assert!(hist.events.len() >= 2);
}

#[test]
fn checkpoint_declares_coverage() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.put("k", b"v", DurabilityMode::Durable).unwrap();
    let (meta, path) = store.checkpoint("hot-tier-only").unwrap();
    assert_eq!(meta.coverage, "hot-tier-only");
    assert_eq!(meta.live_subjects, 1);
    assert_eq!(meta.projection, "primary-live-v1");
    let loaded = store.load_checkpoint(&path).unwrap().unwrap();
    assert_eq!(loaded.0.coverage, "hot-tier-only");
    assert_eq!(loaded.1.len(), 1);
}

#[test]
fn secondary_index_create_lookup_delete() {
    use residiuum_store::{IndexState, SecondaryIndex};

    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    // Subjects as raw strings for store-level index file test.
    store
        .put("users/1", b"{}", DurabilityMode::Durable)
        .unwrap();

    let mut idx = SecondaryIndex::new_building("users", "by-email", vec!["email".into()]);
    idx.insert(b"a@x.com".to_vec(), b"users/1".to_vec());
    let fp = store.segment_fingerprint().unwrap();
    idx.mark_ready(fp);
    store.write_secondary_index(&idx).unwrap();

    let loaded = store
        .load_secondary_index("users", "by-email")
        .unwrap()
        .unwrap();
    assert_eq!(loaded.meta.state, IndexState::Ready);
    assert_eq!(loaded.lookup(b"a@x.com"), &[b"users/1".to_vec()]);

    store.delete_secondary_index("users", "by-email").unwrap();
    assert!(store
        .load_secondary_index("users", "by-email")
        .unwrap()
        .is_none());
    // Authoritative data untouched.
    assert_eq!(
        store.get("users/1").unwrap().as_deref(),
        Some(b"{}".as_slice())
    );
}
