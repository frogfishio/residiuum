//! DEF-010 write dedup, DEF-012 offline-tier scan honesty, DEF-013 catalog frontier.

use residiuum_store::{
    collections_catalog_path, content_identity, try_load_collection_catalog, DurabilityMode,
    EventKind, Store, TierClass, TierMoveMode,
};

#[test]
fn def_013_memory_then_durable_does_not_contaminate_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create(&root).unwrap();
        // Memory-only subject under collection "memonly" via Stage 4 subject encoding
        // is not used here — put takes raw subjects. Use a collection-shaped subject
        // so catalog extraction sees it: 0x01 || coll_len || coll || key.
        let mem_subject = stage4_subject("ghost", "a");
        store
            .put(&mem_subject, b"memory-only", DurabilityMode::Memory)
            .unwrap();
        assert_eq!(
            store.get(&mem_subject).unwrap().as_deref(),
            Some(b"memory-only".as_slice())
        );

        let durable_subject = stage4_subject("real", "b");
        store
            .put(&durable_subject, b"durable", DurabilityMode::Durable)
            .unwrap();

        // Visibility catalog may include memory-mode collections.
        let names = store.list_collections();
        assert!(
            names.iter().any(|n| n == "ghost") || names.iter().any(|n| n == "real"),
            "in-process visibility should list known collections: {names:?}"
        );

        // On-disk catalog must not include the memory-only collection.
        let fp = store.segment_fingerprint().unwrap();
        let cat_path = collections_catalog_path(&store.paths().catalogs_dir());
        let on_disk = try_load_collection_catalog(&cat_path, store.store_id(), fp)
            .unwrap()
            .expect("catalog file after durable write");
        let disk_names: Vec<_> = on_disk.names().map(|s| s.to_string()).collect();
        assert!(
            !disk_names.iter().any(|n| n == "ghost"),
            "persisted catalog must not list memory-only collection: {disk_names:?}"
        );
        assert!(
            disk_names.iter().any(|n| n == "real"),
            "persisted catalog must list durable collection: {disk_names:?}"
        );
    }

    // Crash/reopen: only durable data survives.
    let store = Store::open(&root).unwrap();
    let mem_subject = stage4_subject("ghost", "a");
    let durable_subject = stage4_subject("real", "b");
    assert!(store.get(&mem_subject).unwrap().is_none());
    assert_eq!(
        store.get(&durable_subject).unwrap().as_deref(),
        Some(b"durable".as_slice())
    );
    let names = store.list_collections();
    assert!(
        !names.iter().any(|n| n == "ghost"),
        "reopen must not resurrect memory-only collection: {names:?}"
    );
    assert!(names.iter().any(|n| n == "real"), "{names:?}");
}

#[test]
fn def_013_memory_not_flushed_by_later_durable_write() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create(&root).unwrap();
        store.put("only-mem", b"A", DurabilityMode::Memory).unwrap();
        store
            .put("only-disk", b"B", DurabilityMode::Durable)
            .unwrap();
        assert_eq!(
            store.get("only-mem").unwrap().as_deref(),
            Some(b"A".as_slice())
        );
        assert_eq!(
            store.get("only-disk").unwrap().as_deref(),
            Some(b"B".as_slice())
        );
    }
    let store = Store::open(&root).unwrap();
    assert!(
        store.get("only-mem").unwrap().is_none(),
        "memory publish must not become durable via a later write"
    );
    assert_eq!(
        store.get("only-disk").unwrap().as_deref(),
        Some(b"B".as_slice())
    );
}

#[test]
fn def_010_write_dedup_exact_retry_and_content_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let op = [0x11u8; 16];
    let body = b"hello";
    let hash = content_identity("put", "users", "alice", body);
    let receipt = store
        .put(
            // Use opaque subject; content identity is independent of subject encoding.
            "users\0alice",
            body,
            DurabilityMode::Durable,
        )
        .unwrap();
    store.record_write_dedup(op, hash, &receipt).unwrap();

    // Exact retry.
    let again = store.resolve_write_dedup(&op, &hash).unwrap().unwrap();
    assert_eq!(again.event_id, receipt.event_id);
    assert_eq!(again.segment_id, receipt.segment_id);

    // Content mismatch.
    let other = content_identity("put", "users", "alice", b"different");
    let err = store.resolve_write_dedup(&op, &other).unwrap_err();
    assert!(
        matches!(
            err,
            residiuum_store::StoreError::OperationIdentityConflict
                | residiuum_store::StoreError::ConsistencyViolation(_)
        ),
        "{err:?}"
    );

    // Survives reopen.
    drop(store);
    let store = Store::open(dir.path().join("s")).unwrap();
    let path = store
        .paths()
        .store_info()
        .join(residiuum_store::WRITE_DEDUP_JOURNAL_FILE);
    assert!(path.is_file(), "dedup file must persist");
    let prior = store.resolve_write_dedup(&op, &hash).unwrap().unwrap();
    assert_eq!(prior.event_id, receipt.event_id);

    // History still has a single put event for the subject.
    let hist = store.history("users\0alice").unwrap();
    let puts: Vec<_> = hist
        .events
        .iter()
        .filter(|e| e.kind == EventKind::Put)
        .collect();
    assert_eq!(puts.len(), 1, "dedup must not double-append");
}

#[test]
fn def_012_offline_tier_makes_ordinary_scan_incomplete() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_seal_threshold(64 * 1024 * 1024);
    store
        .put("only/on/archive", b"cold-bytes", DurabilityMode::Durable)
        .unwrap();
    store.seal_active().unwrap();
    let seg = store
        .list_segment_summaries()
        .into_iter()
        .find(|s| s.item_events > 0)
        .expect("data-bearing sealed segment")
        .segment_id;
    store
        .transfer_segment_to_tier(seg, TierClass::Archive, TierMoveMode::Move)
        .unwrap();
    store.set_tier_available(TierClass::Archive, false).unwrap();

    assert!(store.tier_coverage().is_incomplete());
    let scan = store.scan_live_logical().unwrap();
    assert!(scan.tier_coverage_incomplete);
    assert!(!scan.complete);

    let err = store.live_logical_entries().unwrap_err();
    assert!(
        matches!(err, residiuum_store::StoreError::CoverageIncomplete(_)),
        "{err:?}"
    );
}

/// Build a Stage 4 draft subject: `0x01 || coll_len:u16 LE || collection || key`.
fn stage4_subject(collection: &str, key: &str) -> String {
    let mut s = vec![0x01];
    s.extend_from_slice(&(collection.len() as u16).to_le_bytes());
    s.extend_from_slice(collection.as_bytes());
    s.extend_from_slice(key.as_bytes());
    String::from_utf8(s).expect("stage4 subject is utf-8")
}

#[test]
fn def_010_content_identity_helper_stable() {
    let a = content_identity("put", "c", "k", b"1");
    let b = content_identity("put", "c", "k", b"1");
    assert_eq!(a, b);
    assert_ne!(a, content_identity("delete", "c", "k", b""));
}
