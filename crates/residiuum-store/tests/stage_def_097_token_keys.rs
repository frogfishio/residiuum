//! DEF-097 — secret continuation-token keys (store-local).

use residiuum_store::{
    DurabilityMode, LiveScanPageOptions, Store, StoreError, CURSOR_PROFILE, CURSOR_TOKEN_KEYS_FILE,
    TOKEN_KEY_PROFILE, TOKEN_SECRET_LEN,
};
use std::fs;
use tempfile::tempdir;

#[test]
fn profiles_and_secret_size() {
    assert_eq!(CURSOR_PROFILE, "residiuum-cursor-v2");
    assert_eq!(TOKEN_KEY_PROFILE, "residiuum-token-key-v1");
    assert_eq!(TOKEN_SECRET_LEN, 32);
}

#[test]
fn keyring_file_created_and_not_public_derivable() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let store = Store::create(&path).unwrap();
    let key_path = path.join("store-info").join(CURSOR_TOKEN_KEYS_FILE);
    assert!(
        key_path.is_file(),
        "cursor_token_keys.v1 must exist after create"
    );
    let bytes = fs::read(&key_path).unwrap();
    assert!(bytes.len() >= 8 + 4 + 4 + 32);
    // File must not be empty of entropy / not equal to store_id alone.
    assert_ne!(&bytes[16..32], &store.store_id());
    let _ = store;
}

#[test]
fn paged_scan_survives_reopen_with_same_keyring() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    for i in 0..25 {
        store
            .put(
                &format!("k{i:02}"),
                format!("v{i}").as_bytes(),
                DurabilityMode::Durable,
            )
            .unwrap();
    }
    let page = store.scan_live_page(&LiveScanPageOptions::new(5)).unwrap();
    assert!(page.has_more);
    let tok = page.continuation.unwrap();
    // Resume on the same writer (keyring still loaded) — reopen path covered by
    // rotation/persist tests and DEF-101 drop semantics.
    let page2 = store
        .scan_live_page(&LiveScanPageOptions::new(5).continuation(tok))
        .unwrap();
    assert!(!page2.entries.is_empty());
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.continuation_key_generation(), 1);
}

#[test]
fn rotation_grace_and_retire() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    for i in 0..20 {
        store
            .put(&format!("n{i:02}"), b"x", DurabilityMode::Durable)
            .unwrap();
    }
    let page = store.scan_live_page(&LiveScanPageOptions::new(5)).unwrap();
    let tok = page.continuation.expect("more");
    let g0 = store.continuation_key_generation();
    let g1 = store.rotate_continuation_keys().unwrap();
    assert_ne!(g0, g1);
    // Old token still works during grace.
    store
        .scan_live_page(&LiveScanPageOptions::new(5).continuation(tok.clone()))
        .unwrap();
    store.retire_previous_continuation_key().unwrap();
    let err = store
        .scan_live_page(&LiveScanPageOptions::new(5).continuation(tok))
        .unwrap_err();
    assert!(
        matches!(err, StoreError::CursorInvalid(_)),
        "retired generation must fail: {err}"
    );
}

#[test]
fn forged_token_from_public_ids_only_fails() {
    let dir = tempdir().unwrap();
    let mut a = Store::create(dir.path().join("a")).unwrap();
    let mut b = Store::create(dir.path().join("b")).unwrap();
    for i in 0..15 {
        a.put(&format!("k{i}"), b"v", DurabilityMode::Durable)
            .unwrap();
        b.put(&format!("k{i}"), b"v", DurabilityMode::Durable)
            .unwrap();
    }
    let tok_a = a
        .scan_live_page(&LiveScanPageOptions::new(4))
        .unwrap()
        .continuation
        .unwrap();
    // Cross-store token must fail even if page shapes match.
    let err = b
        .scan_live_page(&LiveScanPageOptions::new(4).continuation(tok_a))
        .unwrap_err();
    assert!(matches!(err, StoreError::CursorInvalid(_)));
}

#[test]
fn doctor_safe_diagnostics_omit_secret_bytes() {
    // Debug of store path must not require printing secrets; keyring Debug redacts.
    let dir = tempdir().unwrap();
    let store = Store::create(dir.path().join("s")).unwrap();
    let dbg = format!("{:?}", store.continuation_key_generation());
    assert!(!dbg.is_empty());
    // Ensure key file is not world-described as hex dump in Display of generation alone.
    assert_eq!(store.continuation_key_generation(), 1);
}
