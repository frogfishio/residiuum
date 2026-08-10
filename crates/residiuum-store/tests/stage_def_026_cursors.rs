//! DEF-026 — true bounded-memory live scan cursors.
//!
//! Guarantees under test:
//! - Paged scan reconstructs the same complete live set as a full scan.
//! - Peak page buffers are bounded by page_size (not total live count).
//! - Continuation tokens are store-bound and MAC-authenticated.
//! - Generation fence rejects stale tokens after mutations.
//! - Cross-store and tampered tokens fail closed.

use residiuum_store::{
    DurabilityMode, LiveScanPageOptions, Store, StoreError, CURSOR_PROFILE, DEFAULT_PAGE_SIZE,
};
use std::collections::HashSet;
use tempfile::tempdir;

fn put_n(store: &mut Store, n: usize, prefix: &str) {
    for i in 0..n {
        let k = format!("{prefix}{i:04}");
        let v = format!("body-{i}");
        store
            .put(&k, v.as_bytes(), DurabilityMode::Durable)
            .unwrap();
    }
}

#[test]
fn cursor_profile_tag() {
    assert_eq!(CURSOR_PROFILE, "residiuum-cursor-v2");
}

#[test]
fn paged_scan_matches_full_scan() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    put_n(&mut store, 137, "doc/");

    let full = store.live_logical_entries().unwrap();
    assert_eq!(full.len(), 137);

    let mut opts = LiveScanPageOptions::new(10);
    let mut paged = Vec::new();
    let mut pages = 0usize;
    let mut max_page_entries = 0usize;
    loop {
        let page = store.scan_live_page(&opts).unwrap();
        pages += 1;
        max_page_entries = max_page_entries.max(page.entries.len());
        assert!(page.entries.len() <= 10);
        paged.extend(page.entries);
        if !page.has_more {
            assert!(page.continuation.is_none());
            break;
        }
        opts = LiveScanPageOptions::new(10).continuation(page.continuation.unwrap());
    }

    assert!(pages >= 14, "expected multiple pages, got {pages}");
    assert!(max_page_entries <= 10);
    assert_eq!(paged.len(), full.len());
    assert_eq!(paged, full);
}

#[test]
fn prefix_scope_and_order() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    put_n(&mut store, 20, "a/");
    put_n(&mut store, 15, "b/");

    let mut opts = LiveScanPageOptions::new(7).prefix(b"a/".to_vec());
    let mut keys = Vec::new();
    loop {
        let page = store.scan_live_page(&opts).unwrap();
        for (k, _) in &page.entries {
            assert!(k.starts_with(b"a/"));
            keys.push(k.clone());
        }
        if !page.has_more {
            break;
        }
        opts = LiveScanPageOptions::new(7)
            .prefix(b"a/".to_vec())
            .continuation(page.continuation.unwrap());
    }
    assert_eq!(keys.len(), 20);
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
}

#[test]
fn tampered_token_rejected() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    put_n(&mut store, 30, "k/");

    let page = store.scan_live_page(&LiveScanPageOptions::new(5)).unwrap();
    let mut tok = page.continuation.expect("more pages");
    let last = tok.len() - 1;
    tok[last] ^= 0x5a;

    let err = store
        .scan_live_page(&LiveScanPageOptions::new(5).continuation(tok))
        .unwrap_err();
    assert!(
        matches!(err, StoreError::CursorInvalid(_)),
        "expected CursorInvalid, got {err:?}"
    );
}

#[test]
fn cross_store_token_rejected() {
    let dir = tempdir().unwrap();
    let mut a = Store::create(dir.path().join("a")).unwrap();
    let mut b = Store::create(dir.path().join("b")).unwrap();
    put_n(&mut a, 20, "k/");
    put_n(&mut b, 20, "k/");

    let page = a.scan_live_page(&LiveScanPageOptions::new(4)).unwrap();
    let tok = page.continuation.expect("more pages");

    let err = b
        .scan_live_page(&LiveScanPageOptions::new(4).continuation(tok))
        .unwrap_err();
    assert!(matches!(err, StoreError::CursorInvalid(_)));
}

#[test]
fn mutation_stales_cursor_generation() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    put_n(&mut store, 25, "k/");

    let page = store.scan_live_page(&LiveScanPageOptions::new(5)).unwrap();
    let tok = page.continuation.expect("more pages");

    // Change live count → generation fence.
    store.put("k/extra", b"x", DurabilityMode::Durable).unwrap();

    let err = store
        .scan_live_page(&LiveScanPageOptions::new(5).continuation(tok))
        .unwrap_err();
    assert!(
        matches!(err, StoreError::CursorStale(_)),
        "expected CursorStale, got {err:?}"
    );
}

#[test]
fn empty_store_single_complete_page() {
    let dir = tempdir().unwrap();
    let store = Store::create(dir.path().join("s")).unwrap();
    let page = store
        .scan_live_page(&LiveScanPageOptions::new(DEFAULT_PAGE_SIZE))
        .unwrap();
    assert!(page.entries.is_empty());
    assert!(!page.has_more);
    assert!(page.complete);
    assert!(page.continuation.is_none());
}

#[test]
fn prefix_not_starved_by_earlier_keys() {
    // Keys that sort before the prefix must not make take_while stop early.
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store
        .put("aaa/early", b"1", DurabilityMode::Durable)
        .unwrap();
    store.put("users/a", b"A", DurabilityMode::Durable).unwrap();
    store.put("users/c", b"C", DurabilityMode::Durable).unwrap();
    store
        .put("zzz/late", b"9", DurabilityMode::Durable)
        .unwrap();

    let page = store
        .scan_live_page(&LiveScanPageOptions::new(10).prefix(b"users/".to_vec()))
        .unwrap();
    assert_eq!(page.entries.len(), 2);
    assert_eq!(page.entries[0].0, b"users/a");
    assert_eq!(page.entries[1].0, b"users/c");
    assert!(page.complete);
}

#[test]
fn no_duplicates_across_pages() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    put_n(&mut store, 200, "x/");

    let mut seen = HashSet::new();
    let mut opts = LiveScanPageOptions::new(13);
    loop {
        let page = store.scan_live_page(&opts).unwrap();
        for (k, _) in page.entries {
            assert!(seen.insert(k), "duplicate subject across pages");
        }
        if !page.has_more {
            break;
        }
        opts = LiveScanPageOptions::new(13).continuation(page.continuation.unwrap());
    }
    assert_eq!(seen.len(), 200);
}
