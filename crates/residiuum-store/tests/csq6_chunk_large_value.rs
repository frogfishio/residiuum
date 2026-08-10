//! CSQ-6 — chunk / large-value qualification (first labor cut).
//!
//! Covers `CSQ-CHK-001`…`CSQ-CHK-007` behavioural floors and permanently links
//! DEF-098 + DEF-103 regression authorities. Full package accept still requires
//! principal review of predecessors CSQ-3…CSQ-5 and residual damage diagnostics
//! that CSQ-7 deepens.

use residiuum_format::{decode_chunk_body, scan_forward, FrameKind, SafetyLimits};
use residiuum_store::{
    decode_chunk_manifest, is_chunk_manifest, reassemble_with_manifest, resolve_piece,
    DurabilityMode, PayloadLayout, PayloadResult, ReadBudget, Store, StoreError,
    LARGE_VALUE_PROFILE_ID,
};
use std::fs;
use tempfile::tempdir;

fn large(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

fn assert_complete(store: &Store, key: &str, expected: &[u8]) {
    assert_eq!(store.get(key).unwrap().as_deref(), Some(expected));
    match store.get_payload(key).unwrap() {
        Some(PayloadResult::Complete { body }) => assert_eq!(body, expected),
        other => panic!("expected Complete for {key}: {other:?}"),
    }
}

/// CSQ-CHK-001/002/006 — bounded generation kernel around threshold edges.
#[test]
fn csq_chk_generation_kernel_threshold_edges() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(32);
    store.set_chunk_size(8);

    for len in [1usize, 31, 32, 33, 40, 64, 65, 96, 128] {
        let body = large((len % 200) as u8, len);
        let key = format!("k{len}");
        let rc = store.put(&key, &body, DurabilityMode::Durable).unwrap();
        assert_eq!(rc.logical_len, len as u64);
        assert_eq!(rc.profile_id, LARGE_VALUE_PROFILE_ID);
        if len > 32 {
            assert_eq!(rc.layout, PayloadLayout::Chunked);
            assert!(rc.chunk_count >= 1, "len={len} chunks={}", rc.chunk_count);
            let expected_chunks = len.div_ceil(8) as u32;
            assert_eq!(rc.chunk_count, expected_chunks, "len={len}");
        } else {
            assert_eq!(rc.layout, PayloadLayout::Inline);
            assert_eq!(rc.chunk_count, 0);
        }
        assert_complete(&store, &key, &body);
    }
}

/// CSQ-CHK-002/004/006 — many rewrites keep only the latest generation.
#[test]
fn csq_chk_current_generation_exact_after_rewrites() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(16);
    store.set_chunk_size(6);

    let mut last = Vec::new();
    for gen in 0u8..20 {
        last = large(gen, 48 + gen as usize);
        let rc = store.put("target", &last, DurabilityMode::Durable).unwrap();
        assert_eq!(rc.layout, PayloadLayout::Chunked);
        assert_eq!(
            store.get("target").unwrap().as_deref(),
            Some(last.as_slice())
        );
    }
    assert_complete(&store, "target", &last);

    // Reopen must not resurrect an intermediate generation.
    drop(store);
    let store = Store::open(dir.path().join("s")).unwrap();
    assert_complete(&store, "target", &last);
}

/// CSQ-CHK-004 — same-lineage older chunks never poison current get.
#[test]
fn csq_chk_old_generation_does_not_poison_current() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(12);
    store.set_chunk_size(5);

    let a = large(1, 55);
    let b = large(2, 70);
    store.put("k", &a, DurabilityMode::Durable).unwrap();
    store.put("k", &b, DurabilityMode::Durable).unwrap();
    assert_complete(&store, "k", &b);
    assert_ne!(a, b);

    // Sibling keys keep their own lineage.
    let c = large(3, 60);
    store.put("other", &c, DurabilityMode::Durable).unwrap();
    assert_complete(&store, "k", &b);
    assert_complete(&store, "other", &c);
}

/// CSQ-CHK-001/002 — rebuild locators from segments then reassemble.
#[test]
fn csq_chk_locator_rebuild_preserves_complete() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let body = large(9, 90);
    {
        let mut store = Store::create(&path).unwrap();
        store.set_chunk_threshold(20);
        store.set_chunk_size(10);
        store.put("k", &body, DurabilityMode::Durable).unwrap();
        store.rebuild_index().unwrap();
        assert_complete(&store, "k", &body);
    }
    let mut store = Store::open(&path).unwrap();
    store.rebuild_index().unwrap();
    assert_complete(&store, "k", &body);
    store.rebuild_catalogs().unwrap();
    assert_complete(&store, "k", &body);
}

/// CSQ-CHK-007 — point-read completeness unaffected by unrelated store growth.
///
/// Versioned payload projection for the target event must not grow with
/// filler volume (bounded by manifest/referenced work, not total dataset).
#[test]
fn csq_chk_unrelated_growth_does_not_poison_point_read() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(16);
    store.set_chunk_size(8);

    let target = large(11, 80);
    store
        .put("target", &target, DurabilityMode::Durable)
        .unwrap();
    let hist = store.history("target").unwrap();
    let event_id = hist.events.last().expect("put event").event_id;
    let baseline = store
        .get_payload_version("target", &event_id, ReadBudget::unlimited())
        .unwrap();
    let base_bytes = baseline.bytes_examined;
    assert!(
        baseline
            .selected
            .as_ref()
            .map(|p| p.is_complete())
            .unwrap_or(false),
        "baseline must be complete"
    );

    // Flood the store with unrelated chunked payloads.
    for i in 0..40 {
        let filler = large((i % 200) as u8, 64 + (i % 17));
        store
            .put(&format!("filler-{i}"), &filler, DurabilityMode::Durable)
            .unwrap();
    }

    assert_complete(&store, "target", &target);

    let after = store
        .get_payload_version(
            "target",
            &event_id,
            ReadBudget {
                max_events_examined: 64,
                max_bytes_examined: 256 * 1024,
            },
        )
        .unwrap();
    assert!(
        after
            .selected
            .as_ref()
            .map(|p| p.is_complete())
            .unwrap_or(false),
        "after filler flood must still complete"
    );
    // Work for the target event must not scale with filler volume.
    assert!(
        after.bytes_examined <= base_bytes.saturating_mul(4).max(base_bytes + 4096),
        "bytes_examined grew too much: base={base_bytes} after={}",
        after.bytes_examined
    );
}

/// CSQ-CHK-003 — partial reassembly surfaces exact missing extents (pure path).
#[test]
fn csq_chk_partial_reassembly_reports_missing_extents() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(16);
    store.set_chunk_size(8);
    let body = large(5, 40);
    store.put("k", &body, DurabilityMode::Durable).unwrap();
    store.seal_active().unwrap();

    // Read current manifest from the live index path.
    let hist = store.history("k").unwrap();
    let put_ev = hist
        .events
        .iter()
        .rev()
        .find(|e| is_chunk_manifest(&e.body))
        .expect("chunked put history");
    let manifest = decode_chunk_manifest(&put_ev.body).expect("decode manifest");

    let mut resolved = Vec::new();
    for entry in fs::read_dir(store.path().join("segments")).unwrap() {
        let p = entry.unwrap().path();
        let bytes = fs::read(&p).unwrap();
        let report = scan_forward(&bytes, SafetyLimits::default());
        for (off, frame) in report.verified_frames() {
            if frame.header.known_kind() != Some(FrameKind::PayloadChunk) {
                continue;
            }
            if let Some(piece) = decode_chunk_body(&frame.body) {
                if piece.item_id == put_ev.item_id {
                    resolved.push(resolve_piece(frame.header.event_id, piece, [0u8; 16], off));
                }
            }
        }
    }
    assert!(resolved.len() >= 2, "need multi-chunk payload");
    resolved.sort_by_key(|r| r.piece.index);
    let drop_idx = resolved[0].piece.index;
    let surviving: Vec<_> = resolved
        .into_iter()
        .filter(|r| r.piece.index != drop_idx)
        .collect();

    match reassemble_with_manifest(put_ev.item_id, &manifest, &surviving) {
        PayloadResult::Partial {
            missing, extents, ..
        } => {
            assert!(missing.contains(&drop_idx));
            assert_eq!(extents.len(), manifest.chunks.len());
            assert!(!extents[drop_idx as usize].present);
        }
        other => panic!("expected Partial, got {other:?}"),
    }
}

/// DEF-103 linkage — rewrite-heavy profile still yields exact latest body.
#[test]
fn csq_def103_rewrite_heavy_latest_wins() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    // Force chunked for modest bodies.
    store.set_chunk_threshold(24);
    store.set_chunk_size(12);

    let mut last = large(0, 50);
    for gen in 0u8..8 {
        last = large(gen, 50 + gen as usize * 3);
        let rc = store.put("hot", &last, DurabilityMode::Durable).unwrap();
        assert_eq!(rc.layout, PayloadLayout::Chunked);
    }
    assert_complete(&store, "hot", &last);

    // Over-max admission still fails closed with zero effect on hot key.
    let max = store.large_value_policy().max_logical_payload_bytes as usize;
    let too_big = vec![0xABu8; max + 1];
    let err = store
        .put("hot", &too_big, DurabilityMode::Durable)
        .unwrap_err();
    assert!(matches!(err, StoreError::PayloadTooLarge));
    assert_complete(&store, "hot", &last);
}

/// DEF-098 registry linkage: dual durable chunked put → latest complete only.
#[test]
fn csq_def098_dual_chunked_put_latest_only() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(20);
    store.set_chunk_size(8);
    let a = large(1, 60);
    let b = large(2, 60);
    store.put("k", &a, DurabilityMode::Durable).unwrap();
    store.put("k", &b, DurabilityMode::Durable).unwrap();
    assert_complete(&store, "k", &b);
    match store.get_payload("k").unwrap() {
        Some(PayloadResult::Complete { body }) => assert_eq!(body, b),
        Some(PayloadResult::Partial { .. })
        | Some(PayloadResult::Conflicting { .. })
        | Some(PayloadResult::Unavailable { .. })
        | None => panic!("DEF-098: expected complete latest generation"),
    }
}

/// Transcript survival: durable chunked value remains after seal + reopen.
#[test]
fn csq_transcript_survival_after_seal_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let body = large(42, 100);
    {
        let mut store = Store::create(&path).unwrap();
        store.set_chunk_threshold(20);
        store.set_chunk_size(16);
        store
            .put("survive", &body, DurabilityMode::Durable)
            .unwrap();
        store.seal_active().unwrap();
    }
    let store = Store::open(&path).unwrap();
    assert_complete(&store, "survive", &body);
}
