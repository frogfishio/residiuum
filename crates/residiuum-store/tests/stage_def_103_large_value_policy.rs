//! DEF-103 — large-value policy admission and rewrite-heavy layout contract.

use residiuum_store::{
    rewrite_heavy, DurabilityMode, LargeValuePolicy, PayloadLayout, Store, StoreError,
    DEFAULT_CHUNK_THRESHOLD, LARGE_VALUE_PROFILE_ID,
};
use tempfile::tempdir;

#[test]
fn threshold_minus_exact_plus_one_layout() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let thr = store.large_value_policy().chunk_threshold_bytes;

    let below = vec![1u8; thr.saturating_sub(1).max(1)];
    let exact = vec![2u8; thr];
    let over = vec![3u8; thr + 1];

    let r0 = store.put("a", &below, DurabilityMode::Durable).unwrap();
    assert_eq!(r0.layout, PayloadLayout::Inline);
    assert_eq!(r0.chunk_count, 0);
    assert_eq!(r0.profile_id, LARGE_VALUE_PROFILE_ID);

    let r1 = store.put("b", &exact, DurabilityMode::Durable).unwrap();
    assert_eq!(r1.layout, PayloadLayout::Inline);

    let r2 = store.put("c", &over, DurabilityMode::Durable).unwrap();
    assert_eq!(r2.layout, PayloadLayout::Chunked);
    assert!(r2.chunk_count >= 1);
    assert_eq!(r2.logical_len, over.len() as u64);

    assert_eq!(store.get("a").unwrap().as_deref(), Some(below.as_slice()));
    assert_eq!(store.get("b").unwrap().as_deref(), Some(exact.as_slice()));
    assert_eq!(store.get("c").unwrap().as_deref(), Some(over.as_slice()));
    let _ = DEFAULT_CHUNK_THRESHOLD;
}

#[test]
fn over_max_logical_rejects_with_zero_effect() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let max = store.large_value_policy().max_logical_payload_bytes as usize;
    store.put("ok", b"v", DurabilityMode::Durable).unwrap();
    let before = store.live_count();

    let too_big = vec![9u8; max + 1];
    let err = match store.put("big", &too_big, DurabilityMode::Durable) {
        Ok(_) => panic!("expected PayloadTooLarge"),
        Err(e) => e,
    };
    assert!(matches!(err, StoreError::PayloadTooLarge));
    assert_eq!(store.live_count(), before);
    assert!(store.get("big").unwrap().is_none());
    assert_eq!(store.get("ok").unwrap().as_deref(), Some(b"v".as_slice()));
}

#[test]
fn tighter_policy_keeps_old_data_readable() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(32);
    store.set_chunk_size(16);
    let body = vec![7u8; 80];
    store.put("k", &body, DurabilityMode::Durable).unwrap();

    let mut tight = LargeValuePolicy::application_v1();
    tight.max_logical_payload_bytes = 40;
    tight.max_reassembly_bytes = 40;
    tight.max_write_peak_memory = 80;
    store.set_large_value_policy(tight).unwrap();

    // Old value still readable.
    assert_eq!(store.get("k").unwrap().as_deref(), Some(body.as_slice()));
    // New over-limit write rejected.
    assert!(matches!(
        store.put("k2", &vec![1u8; 50], DurabilityMode::Durable),
        Err(StoreError::PayloadTooLarge)
    ));
}

#[test]
fn invalid_policy_rejected_before_apply() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let mut bad = LargeValuePolicy::application_v1();
    bad.chunk_payload_bytes = 0;
    assert!(store.set_large_value_policy(bad).is_err());
    // Store still uses defaults.
    assert_eq!(
        store.large_value_policy().profile_id,
        LARGE_VALUE_PROFILE_ID
    );
}

#[test]
fn effective_with_chooses_tightest() {
    let p = LargeValuePolicy::application_v1();
    assert_eq!(p.effective_with(1024), 1024);
    assert_eq!(
        p.effective_with(p.max_logical_payload_bytes + 100),
        p.max_logical_payload_bytes
    );
}

#[test]
fn rewrite_heavy_key_helpers() {
    assert_eq!(
        rewrite_heavy::transcript_turn("t1", "42"),
        "transcript/t1/turn/42"
    );
    assert_eq!(rewrite_heavy::transcript_meta("t1"), "transcript/t1/meta");
    assert!(rewrite_heavy::transcript_snapshot("t1", "g3").contains("snapshot"));
}

#[test]
fn independent_turns_survive_sibling_damage_model() {
    // Recommended model: independent keys; one missing body does not erase others.
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let tid = "chat-1";
    store
        .put(
            &rewrite_heavy::transcript_meta(tid),
            br#"{"title":"demo"}"#,
            DurabilityMode::Durable,
        )
        .unwrap();
    store
        .put(
            &rewrite_heavy::transcript_turn(tid, "1"),
            br#"{"text":"hello"}"#,
            DurabilityMode::Durable,
        )
        .unwrap();
    store
        .put(
            &rewrite_heavy::transcript_turn(tid, "2"),
            br#"{"text":"world"}"#,
            DurabilityMode::Durable,
        )
        .unwrap();
    // "Damage" one turn by deleting it; siblings remain.
    store
        .delete(
            &rewrite_heavy::transcript_turn(tid, "1"),
            DurabilityMode::Durable,
        )
        .unwrap();
    assert!(store
        .get(&rewrite_heavy::transcript_turn(tid, "1"))
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .get(&rewrite_heavy::transcript_turn(tid, "2"))
            .unwrap()
            .as_deref(),
        Some(br#"{"text":"world"}"#.as_slice())
    );
    assert!(store
        .get(&rewrite_heavy::transcript_meta(tid))
        .unwrap()
        .is_some());
}
