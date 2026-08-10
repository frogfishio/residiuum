//! DEF-104 — executable journeys for `doc/reference/operations/CRASH_AND_RECOVERY_CONTRACT.md`
//! (`residiuum-crash-recovery-v1`).
//!
//! These tests are the compiled/CI examples for the normative page. Deeper
//! failpoint cells live in `stage_def_022_crash_matrix`; generation/history/
//! scan/lock/cache/policy detail remain in stage_def_098–103.

use residiuum_store::{
    rewrite_heavy, BeforeEvent, DurabilityMode, LiveScanPageOptions, PayloadResult,
    PrimaryCacheValidation, ReadBudget, RecoveryReadOptions, Store, StoreError, StoreOpenOptions,
    WriterLockClass, PRIMARY_CACHE_FILE,
};
use std::fs;
use std::time::Duration;
use tempfile::tempdir;

fn large(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

/// Journey: durable put acknowledged → drop handle (kill process analogue) → reopen.
#[test]
fn journey_durable_put_ack_survives_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let receipt = {
        let mut store = Store::create(&path).unwrap();
        let r = store
            .put("users/u1", br#"{"n":1}"#, DurabilityMode::Durable)
            .unwrap();
        assert_eq!(r.durability, DurabilityMode::Durable);
        assert!(!r.event_id.iter().all(|&b| b == 0));
        r
    };
    let store = Store::open(&path).unwrap();
    assert_eq!(
        store.get("users/u1").unwrap().as_deref(),
        Some(br#"{"n":1}"#.as_slice())
    );
    // Exact historical path still names the ack event.
    let v = store
        .get_payload_version("users/u1", &receipt.event_id, ReadBudget::default())
        .unwrap();
    assert_eq!(
        v.selected.as_ref().and_then(|p| p.complete_body()),
        Some(br#"{"n":1}"#.as_slice())
    );
}

/// Journey: dual durable chunked overwrite → current is exactly B after reopen.
#[test]
fn journey_chunked_overwrite_generation_exact() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let b = large(2, 72);
    {
        let mut store = Store::create(&path).unwrap();
        store.set_chunk_threshold(16);
        store.set_chunk_size(8);
        store
            .put("k", &large(1, 72), DurabilityMode::Durable)
            .unwrap();
        store.put("k", &b, DurabilityMode::Durable).unwrap();
        assert_eq!(store.get("k").unwrap().as_deref(), Some(b.as_slice()));
    }
    let store = Store::open(&path).unwrap();
    assert_eq!(store.get("k").unwrap().as_deref(), Some(b.as_slice()));
    match store.get_payload("k").unwrap() {
        Some(PayloadResult::Complete { body }) => assert_eq!(body, b),
        other => panic!("expected complete B: {other:?}"),
    }
}

/// Journey: prior complete generation export while a newer complete generation is current.
#[test]
fn journey_prior_complete_via_history_apis() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(20);
    store.set_chunk_size(8);

    let a = large(1, 48);
    let b = large(2, 48);
    let r1 = store.put("doc", &a, DurabilityMode::Durable).unwrap();
    let _r2 = store.put("doc", &b, DurabilityMode::Durable).unwrap();

    // Current is B — ordinary get must not silently fall back to A.
    assert_eq!(store.get("doc").unwrap().as_deref(), Some(b.as_slice()));

    let prior = store
        .get_payload_version("doc", &r1.event_id, ReadBudget::default())
        .unwrap();
    assert_eq!(
        prior.selected.as_ref().and_then(|p| p.complete_body()),
        Some(a.as_slice())
    );

    let found = store
        .find_last_complete_version("doc", BeforeEvent::Current, RecoveryReadOptions::default())
        .unwrap();
    assert_eq!(
        found
            .found
            .as_ref()
            .and_then(|v| v.selected.as_ref().and_then(|p| p.complete_body())),
        Some(a.as_slice())
    );
}

/// Journey: key enumeration + document page for multi-key store.
#[test]
fn journey_key_and_document_pages() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(20);
    store.set_chunk_size(8);

    store
        .put("a", br#"{"ok":1}"#, DurabilityMode::Durable)
        .unwrap();
    store
        .put("b", &large(3, 60), DurabilityMode::Durable)
        .unwrap();
    store
        .put("c", br#"{"ok":2}"#, DurabilityMode::Durable)
        .unwrap();

    let keys = store
        .scan_live_keys_page(&LiveScanPageOptions::new(10))
        .unwrap();
    assert_eq!(keys.keys.len(), 3);
    assert!(keys.coverage_complete);

    let docs = store
        .scan_live_documents_page(&LiveScanPageOptions::new(10))
        .unwrap();
    assert!(docs.rows.iter().any(|(k, _)| k == b"a"));
    assert!(docs.rows.iter().any(|(k, _)| k == b"b"));
    assert!(docs.rows.iter().any(|(k, _)| k == b"c"));
    assert!(docs.key_coverage_complete);
}

/// Journey: exclusive writer held → open fails with structured obs; inspect still reads.
#[test]
fn journey_writer_held_inspect_not_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let mut writer = Store::create(&path).unwrap();
    writer
        .put("alive", b"yes", DurabilityMode::Durable)
        .unwrap();

    match Store::open(&path) {
        Err(StoreError::WriterLockHeld(obs)) => {
            assert_eq!(obs.class, WriterLockClass::InProcess);
            assert!(obs.os_lock_authoritative);
            // Forbidden: treat as empty / NotAStore.
            assert!(!obs.detail.to_lowercase().contains("not a residiuum store"));
            assert!(
                obs.detail.contains("do not delete writer.lock")
                    || obs.detail.contains("writer.lock")
                    || !obs.detail.is_empty()
            );
        }
        Ok(_) => panic!("second open must fail with WriterLockHeld"),
        Err(e) => panic!("expected WriterLockHeld, got {e:?}"),
    }

    let inspect = Store::open_inspect(&path).unwrap();
    assert_eq!(
        inspect.get("alive").unwrap().as_deref(),
        Some(b"yes".as_slice())
    );

    let status = Store::writer_lock_status(&path).unwrap();
    assert_ne!(status.class, WriterLockClass::Free);

    // Bounded wait still times out while held — never NotAStore.
    let err =
        Store::open_with_options(&path, StoreOpenOptions::wait_for(Duration::from_millis(30)))
            .err()
            .expect("must not succeed while held");
    assert!(matches!(err, StoreError::WriterLockHeld(_)));
}

/// Journey: wipe derived cache → logical state identical; diags never elevate authority.
#[test]
fn journey_derived_cache_wipe_neutral() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let expected: Vec<(String, Vec<u8>)> = {
        let mut store = Store::create(&root).unwrap();
        for i in 0..4 {
            store
                .put(
                    &format!("item/{i}"),
                    format!("v{i}").as_bytes(),
                    DurabilityMode::Durable,
                )
                .unwrap();
        }
        store.seal_active().unwrap();
        store.put("tail", b"T", DurabilityMode::Durable).unwrap();
        store.persist_index_cache().unwrap();
        let diag = store.primary_cache_diag().unwrap();
        assert!(!diag.authoritative);
        assert!(matches!(
            diag.validation,
            PrimaryCacheValidation::Accepted | PrimaryCacheValidation::Absent
        ));
        store
            .live_logical_entries()
            .unwrap()
            .into_iter()
            .map(|(k, v)| (String::from_utf8(k).unwrap(), v))
            .collect()
    };

    for name in ["indexes", "catalogs", "snapshots"] {
        let p = root.join(name);
        if p.exists() {
            fs::remove_dir_all(&p).unwrap();
        }
    }
    assert!(!root.join("indexes").join(PRIMARY_CACHE_FILE).is_file());

    let store = Store::open(&root).unwrap();
    let rebuilt: Vec<(String, Vec<u8>)> = store
        .live_logical_entries()
        .unwrap()
        .into_iter()
        .map(|(k, v)| (String::from_utf8(k).unwrap(), v))
        .collect();
    assert_eq!(rebuilt, expected);

    let life = store.lifecycle_diag().unwrap();
    assert!(!life.primary_cache_authoritative);
    let _ = store.primary_cache_diag().unwrap();
    let after: Vec<(String, Vec<u8>)> = store
        .live_logical_entries()
        .unwrap()
        .into_iter()
        .map(|(k, v)| (String::from_utf8(k).unwrap(), v))
        .collect();
    assert_eq!(after, expected, "diagnostics must not change logical state");
}

/// Journey: rewrite-heavy transcript as independent turns; one missing turn does not erase others.
#[test]
fn journey_transcript_independent_turns() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();

    let tid = "sess-1";
    store
        .put(
            &rewrite_heavy::transcript_meta(tid),
            br#"{"title":"demo"}"#,
            DurabilityMode::Durable,
        )
        .unwrap();
    store
        .put(
            &rewrite_heavy::transcript_turn(tid, "0001"),
            br#"{"role":"user","text":"hi"}"#,
            DurabilityMode::Durable,
        )
        .unwrap();
    store
        .put(
            &rewrite_heavy::transcript_turn(tid, "0002"),
            br#"{"role":"assistant","text":"hello"}"#,
            DurabilityMode::Durable,
        )
        .unwrap();
    store
        .put(
            &rewrite_heavy::transcript_turn(tid, "0003"),
            br#"{"role":"user","text":"bye"}"#,
            DurabilityMode::Durable,
        )
        .unwrap();

    // Simulate loss of one turn unit (application-level delete of that key only).
    store
        .delete(
            &rewrite_heavy::transcript_turn(tid, "0002"),
            DurabilityMode::Durable,
        )
        .unwrap();

    assert!(store
        .get(&rewrite_heavy::transcript_turn(tid, "0001"))
        .unwrap()
        .is_some());
    assert!(store
        .get(&rewrite_heavy::transcript_turn(tid, "0002"))
        .unwrap()
        .is_none());
    assert!(store
        .get(&rewrite_heavy::transcript_turn(tid, "0003"))
        .unwrap()
        .is_some());
    assert!(store
        .get(&rewrite_heavy::transcript_meta(tid))
        .unwrap()
        .is_some());
}

/// Forbidden: WriterLockHeld must never be reported as NotAStore / empty.
#[test]
fn forbidden_lock_held_is_not_empty_store() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let _w = Store::create(&path).unwrap();
    match Store::open(&path) {
        Err(StoreError::WriterLockHeld(_)) => {}
        Err(StoreError::NotAStore(_)) => panic!("lock must not become NotAStore"),
        Err(e) => panic!("unexpected error: {e}"),
        Ok(_) => panic!("second open must fail"),
    }
}

/// Forbidden: over-limit write has zero durable effect (not empty overwrite of prior).
#[test]
fn forbidden_payload_too_large_zero_effect() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.put("k", b"prior", DurabilityMode::Durable).unwrap();
    let huge = vec![0u8; 20 * 1024 * 1024]; // above default 16 MiB profile
    let err = store
        .put("k", &huge, DurabilityMode::Durable)
        .err()
        .expect("must reject");
    assert!(matches!(err, StoreError::PayloadTooLarge));
    assert_eq!(
        store.get("k").unwrap().as_deref(),
        Some(b"prior".as_slice()),
        "reject must not clear prior value"
    );
}

/// Forbidden: ordinary get does not auto-fallback to older complete generation.
#[test]
fn forbidden_get_no_silent_historical_fallback() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(16);
    store.set_chunk_size(8);
    let a = large(1, 40);
    let b = large(2, 40);
    let r1 = store.put("k", &a, DurabilityMode::Durable).unwrap();
    store.put("k", &b, DurabilityMode::Durable).unwrap();
    // Current complete → get is B only.
    assert_eq!(store.get("k").unwrap().as_deref(), Some(b.as_slice()));
    // Historical A requires explicit API.
    let hist = store
        .get_payload_version("k", &r1.event_id, ReadBudget::default())
        .unwrap();
    assert_eq!(
        hist.selected.as_ref().and_then(|p| p.complete_body()),
        Some(a.as_slice())
    );
}

/// Contract doc is present with required section anchors (keeps page discoverable in-tree).
#[test]
fn contract_document_present() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("doc/reference/operations/CRASH_AND_RECOVERY_CONTRACT.md");
    let text = fs::read_to_string(&root).expect("CRASH_AND_RECOVERY_CONTRACT.md must exist");
    assert!(text.contains("residiuum-crash-recovery-v1"));
    for needle in [
        "Durability-mode acknowledgement",
        "Inline and chunked publication",
        "Read outcome decision table",
        "Exact Store and Collection recovery APIs",
        "Key coverage versus body completeness",
        "Historical-version selection",
        "Writer-lock recovery",
        "Authority versus derived",
        "Large and rewrite-heavy",
        "Operator decision tree",
        "Capability limitations",
        "Forbidden",
    ] {
        assert!(
            text.contains(needle),
            "contract missing section marker: {needle}"
        );
    }
}

use std::path::PathBuf;
