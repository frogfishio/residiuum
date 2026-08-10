//! DEF-098 — generation-exact chunk reassembly.
//!
//! Regression shape:
//!   put(K, large A, durable) → ack
//!   put(K, large B, durable) → ack
//!   get(K) → exactly B (not PayloadPartial / Conflicting)

use residiuum_store::{DurabilityMode, PayloadResult, Store};
use tempfile::tempdir;

fn large(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

#[test]
fn chunked_overwrite_returns_latest_generation() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(20);
    store.set_chunk_size(8);

    let a = large(1, 60);
    let b = large(2, 60);
    store.put("k", &a, DurabilityMode::Durable).unwrap();
    store.put("k", &b, DurabilityMode::Durable).unwrap();

    assert_eq!(store.get("k").unwrap().as_deref(), Some(b.as_slice()));
    match store.get_payload("k").unwrap() {
        Some(PayloadResult::Complete { body }) => assert_eq!(body, b),
        other => panic!("expected complete B after dual chunked put: {other:?}"),
    }
}

#[test]
fn chunked_overwrite_survives_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let b = large(7, 80);
    {
        let mut store = Store::create(&path).unwrap();
        store.set_chunk_threshold(16);
        store.set_chunk_size(8);
        store
            .put("k", &large(3, 80), DurabilityMode::Durable)
            .unwrap();
        store.put("k", &b, DurabilityMode::Durable).unwrap();
        assert_eq!(store.get("k").unwrap().as_deref(), Some(b.as_slice()));
    }
    let store = Store::open(&path).unwrap();
    assert_eq!(store.get("k").unwrap().as_deref(), Some(b.as_slice()));
    match store.get_payload("k").unwrap() {
        Some(PayloadResult::Complete { body }) => assert_eq!(body, b),
        other => panic!("expected complete B after reopen: {other:?}"),
    }
}

#[test]
fn same_size_larger_and_smaller_chunked_replacements() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(10);
    store.set_chunk_size(6);

    let same = large(1, 48);
    let larger = large(2, 72);
    let smaller = large(3, 24);

    store.put("k", &same, DurabilityMode::Durable).unwrap();
    assert_eq!(store.get("k").unwrap().as_deref(), Some(same.as_slice()));

    store.put("k", &larger, DurabilityMode::Durable).unwrap();
    assert_eq!(store.get("k").unwrap().as_deref(), Some(larger.as_slice()));

    store.put("k", &smaller, DurabilityMode::Durable).unwrap();
    assert_eq!(store.get("k").unwrap().as_deref(), Some(smaller.as_slice()));
}

#[test]
fn inline_to_chunked_to_inline() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(20);
    store.set_chunk_size(8);

    let inline = b"small".to_vec();
    let chunked = large(9, 64);
    let inline2 = b"tiny".to_vec();

    store.put("k", &inline, DurabilityMode::Durable).unwrap();
    assert_eq!(store.get("k").unwrap().as_deref(), Some(inline.as_slice()));

    store.put("k", &chunked, DurabilityMode::Durable).unwrap();
    assert_eq!(store.get("k").unwrap().as_deref(), Some(chunked.as_slice()));

    store.put("k", &inline2, DurabilityMode::Durable).unwrap();
    assert_eq!(store.get("k").unwrap().as_deref(), Some(inline2.as_slice()));

    store.put("k", &chunked, DurabilityMode::Durable).unwrap();
    assert_eq!(store.get("k").unwrap().as_deref(), Some(chunked.as_slice()));
}

#[test]
fn many_consecutive_chunked_generations() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(12);
    store.set_chunk_size(5);

    let mut last = Vec::new();
    for gen in 0u8..12 {
        last = large(gen, 40 + gen as usize);
        store.put("k", &last, DurabilityMode::Durable).unwrap();
    }
    assert_eq!(store.get("k").unwrap().as_deref(), Some(last.as_slice()));
}

#[test]
fn different_key_chunked_values_isolated() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(15);
    store.set_chunk_size(7);

    let a = large(1, 50);
    let b = large(2, 50);
    store.put("a", &a, DurabilityMode::Durable).unwrap();
    store.put("b", &b, DurabilityMode::Durable).unwrap();
    store
        .put("a", &large(3, 55), DurabilityMode::Durable)
        .unwrap();

    let a2 = large(3, 55);
    assert_eq!(store.get("a").unwrap().as_deref(), Some(a2.as_slice()));
    assert_eq!(store.get("b").unwrap().as_deref(), Some(b.as_slice()));
}
