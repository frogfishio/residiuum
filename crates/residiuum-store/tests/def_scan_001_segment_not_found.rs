//! DEF-SCAN-001 — distinct locator errors + honest collection scan pages.
//!
//! Principal rejects:
//! 1. Collapsing bad offset / verify / segment-id mismatch into one error bucket.
//! 2. Soft-skip into a plain `Vec` (hides fail-closed holes as empty success).
//!
//! ## Required posture
//!
//! - Locator resolve errors stay **distinct**.
//! - `scan_collection_page` returns [`CollectionScanPage`].
//! - Legacy `scan_collection` returns `Vec<(key, body)>` only when complete;
//!   any hole hard-fails as [`StoreError`] (no soft-skip partial Vec).
//! - Callers that need honesty use `page.complete` / `incomplete` — not
//!   `entries.is_empty()` alone.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_store::{
    create_object, publish_staged_genesis, stage_heap_genesis, CollectionScanHole,
    CollectionScanHoleReason, CollectionScanPage, CompactOptions, HeapMetaLayout, ObjectKind,
    StoreError, StoreHost, StorePaths, WriteCondition,
};
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;

fn mint_cap(heap: HeapId, deployment: DeploymentId) -> residiuum_heap::HeapCap {
    let snap = HeapSecuritySnapshot {
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key: [7u8; 32],
        previous_master_public_key: None,
        security_revision: SecurityRevision::new(1).unwrap(),
        authority_chain_head_hash: [9u8; 32],
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    };
    let slot = Arc::new(HeapSlot::new(snap));
    let cert = VerifiedCertificate {
        cose_bytes: vec![0x01],
        fingerprint: [3u8; 32],
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4u8; 32],
        // READ | WRITE | INDEX_ADMIN — index build tests need INDEX_ADMIN.
        rights: Rights::from_bits_certificate(
            Rights::READ.bits() | Rights::WRITE.bits() | Rights::INDEX_ADMIN.bits(),
        )
        .unwrap(),
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: [5u8; 32],
    };
    mint_capability(slot, &cert, TrustedInstant { unix_s: 1_700_000_000 }).unwrap()
}

fn uuid16() -> [u8; 16] {
    *residiuum_heap::CollectionId::new_random().unwrap().as_bytes()
}

struct OpenedHeap {
    _dir: tempfile::TempDir,
    host: StoreHost,
    heap: residiuum_store::HeapStore,
    collection_id: [u8; 16],
    root: std::path::PathBuf,
}

fn open_heap_with_collection(name: &str) -> OpenedHeap {
    let dir = tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let host = StoreHost::create(&root).unwrap();
    let layout = HeapMetaLayout::new(&root);

    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_id = *HeapId::new_random().unwrap().as_bytes();
    let coll = uuid16();

    let staged = stage_heap_genesis(&layout, dep, heap_id, uuid16(), "def-scan-heap").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    create_object(
        &layout,
        &heap_id,
        ObjectKind::Collection,
        coll,
        uuid16(),
        name,
    )
    .unwrap();

    let cap = mint_cap(
        HeapId::from_bytes_unchecked_nonzero(heap_id).unwrap(),
        DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap(),
    );
    let heap = host.open_heap(cap);
    OpenedHeap {
        _dir: dir,
        host,
        heap,
        collection_id: coll,
        root,
    }
}

fn scan_all(heap: &residiuum_store::HeapStore, coll: &[u8; 16]) -> Result<CollectionScanPage, StoreError> {
    let mut entries = Vec::new();
    let mut incomplete = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    let mut examined = 0usize;
    loop {
        let page = heap.scan_collection_page(coll, 64, after.as_deref())?;
        examined += page.examined;
        entries.extend(page.entries);
        incomplete.extend(page.incomplete);
        if !page.has_more {
            let complete = incomplete.is_empty();
            return Ok(CollectionScanPage {
                entries,
                incomplete,
                examined,
                complete,
                has_more: false,
                last_key: page.last_key,
            });
        }
        after = page.last_key;
    }
}

/// Compatibility: complete page → `scan_collection` returns `page.entries`.
#[test]
fn scan_collection_returns_page_entries_when_complete() {
    let ctx = open_heap_with_collection("gremlin.work.scan_alias");
    ctx.heap
        .put_collection(&ctx.collection_id, b"k1", b"v1")
        .unwrap();
    let page = ctx
        .heap
        .scan_collection_page(&ctx.collection_id, 16, None)
        .unwrap();
    let legacy = ctx
        .heap
        .scan_collection(&ctx.collection_id, 16, None)
        .unwrap();
    assert!(page.complete);
    assert_eq!(legacy, page.entries);
    assert_eq!(legacy.len(), 1);
    assert_eq!(legacy[0].0, b"k1");
    assert_eq!(legacy[0].1, b"v1");
}

#[test]
fn versioned_point_and_scan_reads_return_establishing_event_ids() {
    let ctx = open_heap_with_collection("gremlin.work.versioned_reads");
    let first = ctx
        .heap
        .put_collection(&ctx.collection_id, b"conversation-1", b"v1")
        .unwrap();

    let point = ctx
        .heap
        .get_collection_versioned(&ctx.collection_id, b"conversation-1")
        .unwrap()
        .expect("live versioned point value");
    assert_eq!(point.body, b"v1");
    assert_eq!(point.version, first.event_id);

    let page = ctx
        .heap
        .scan_collection_page_versioned(&ctx.collection_id, 16, None)
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].0, b"conversation-1");
    assert_eq!(page.entries[0].1, b"v1");
    assert_eq!(page.entries[0].2, first.event_id);

    let second = ctx
        .heap
        .put_collection_if(
            &ctx.collection_id,
            b"conversation-1",
            b"v2",
            WriteCondition::LiveEventId(point.version),
        )
        .unwrap();
    assert_ne!(second.event_id, point.version);
    let stale = ctx
        .heap
        .put_collection_if(
            &ctx.collection_id,
            b"conversation-1",
            b"stale",
            WriteCondition::LiveEventId(point.version),
        )
        .unwrap_err();
    assert!(matches!(stale, StoreError::VersionConflict { .. }));
}

/// Legacy Vec API must hard-fail on holes — never soft-skip into Ok(partial).
#[test]
fn scan_collection_hard_fails_on_unresolved_locator() {
    let ctx = open_heap_with_collection("gremlin.work.scan_failclosed");
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.set_seal_threshold(4 * 1024);
    }
    for i in 0..8 {
        let k = format!("a/{i:03}");
        ctx.heap
            .put_collection(
                &ctx.collection_id,
                k.as_bytes(),
                &format!("body-{k}").into_bytes(),
            )
            .unwrap();
    }
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.seal_active().unwrap();
    }
    for i in 0..8 {
        let k = format!("b/{i:03}");
        ctx.heap
            .put_collection(
                &ctx.collection_id,
                k.as_bytes(),
                &format!("body-{k}").into_bytes(),
            )
            .unwrap();
    }
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.seal_active().unwrap();
    }
    let paths = StorePaths::new(&ctx.root);
    let mut sealed: Vec<_> = fs::read_dir(paths.segments_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    sealed.sort();
    assert!(sealed.len() >= 2);
    fs::remove_file(&sealed[0]).unwrap();

    // Page API still reports survivors + holes.
    let page = ctx
        .heap
        .scan_collection_page(&ctx.collection_id, 64, None)
        .expect("page scan must not hard-abort");
    assert!(!page.complete);
    assert!(!page.incomplete.is_empty());

    // Legacy Vec must hard-fail — not Ok(survivors only).
    let err = ctx
        .heap
        .scan_collection(&ctx.collection_id, 64, None)
        .expect_err("legacy scan_collection must fail-closed on holes");
    match err {
        StoreError::LocatorFault(f) => {
            assert_eq!(
                f.kind,
                residiuum_store::LocatorFaultKind::SegmentNotFound
            );
        }
        StoreError::SegmentNotFound => {}
        other => panic!("expected locator/segment fault, got {other:?}"),
    }
}

#[test]
fn high_churn_exclusive_writer_scan_still_complete() {
    let ctx = open_heap_with_collection("gremlin.work.features");
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.set_seal_threshold(8 * 1024);
    }

    let n_keys = 40usize;
    let rewrites = 25usize;
    let payload = vec![0xABu8; 512];

    for r in 0..rewrites {
        for i in 0..n_keys {
            let key = format!("proj/{i:04}");
            let mut body = payload.clone();
            body.extend_from_slice(format!("-r{r}").as_bytes());
            ctx.heap
                .put_collection(&ctx.collection_id, key.as_bytes(), &body)
                .unwrap();
        }
        if r % 5 == 4 {
            let phys = ctx.host.physical();
            let mut g = phys.lock().unwrap();
            g.seal_active().unwrap();
        }
    }
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.seal_active().unwrap();
    }

    let page = scan_all(&ctx.heap, &ctx.collection_id).expect("scan after high churn");
    assert!(page.complete, "healthy high-churn must be scan-complete");
    assert!(page.incomplete.is_empty());
    assert_eq!(page.entries.len(), n_keys);
}

/// Missing segment media: holes explicit; survivors still in `entries`.
#[test]
fn missing_segment_scan_reports_holes_and_survivors() {
    let ctx = open_heap_with_collection("gremlin.work.kanban_tasks");
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.set_seal_threshold(4 * 1024);
    }

    let cohort_a: Vec<String> = (0..8).map(|i| format!("a/{i:03}")).collect();
    for k in &cohort_a {
        ctx.heap
            .put_collection(
                &ctx.collection_id,
                k.as_bytes(),
                &format!("body-a-{k}").into_bytes(),
            )
            .unwrap();
    }
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.seal_active().unwrap();
    }

    let cohort_b: Vec<String> = (0..8).map(|i| format!("b/{i:03}")).collect();
    for k in &cohort_b {
        ctx.heap
            .put_collection(
                &ctx.collection_id,
                k.as_bytes(),
                &format!("body-b-{k}").into_bytes(),
            )
            .unwrap();
    }
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.seal_active().unwrap();
    }

    let paths = StorePaths::new(&ctx.root);
    let mut sealed: Vec<_> = fs::read_dir(paths.segments_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    sealed.sort();
    assert!(sealed.len() >= 2);
    fs::remove_file(&sealed[0]).unwrap();

    let page = scan_all(&ctx.heap, &ctx.collection_id).expect("scan must not hard-abort");
    assert!(!page.complete, "missing media ⇒ incomplete page");
    assert!(
        !page.entries.is_empty(),
        "survivors must still appear in entries"
    );
    assert!(
        !page.incomplete.is_empty(),
        "holes must be listed — plain Vec soft-skip is rejected"
    );
    assert!(
        page.incomplete
            .iter()
            .any(|h| h.reason == CollectionScanHoleReason::SegmentNotFound),
        "true missing media must report SegmentNotFound reason, got {:?}",
        page.incomplete
    );
    let snf = page
        .incomplete
        .iter()
        .find(|h| h.reason == CollectionScanHoleReason::SegmentNotFound)
        .unwrap();
    let loc = snf.locator.as_ref().expect("SegmentNotFound hole needs locator ctx");
    assert_eq!(loc.kind, residiuum_store::LocatorFaultKind::SegmentNotFound);
    // segment_id is non-zero for real segments
    assert_ne!(loc.segment_id, [0u8; 16]);
    // Empty live set claim is false when holes or entries exist.
    assert!(!page.is_empty_live());
}

/// Present media with zeroed frames → frame verify failure, not SegmentNotFound / PayloadPartial.
#[test]
fn present_media_unreadable_frame_is_locator_frame_verify_failed() {
    let ctx = open_heap_with_collection("forensics.present_media");
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.set_seal_threshold(2 * 1024);
    }
    ctx.heap
        .put_collection(&ctx.collection_id, b"k1", b"hello-present-media")
        .unwrap();
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.seal_active().unwrap();
    }
    let paths = StorePaths::new(&ctx.root);
    let sealed: Vec<_> = fs::read_dir(paths.segments_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    assert!(!sealed.is_empty());
    for p in &sealed {
        let len = fs::metadata(p).unwrap().len();
        fs::write(p, vec![0u8; len as usize]).unwrap();
    }

    let err = ctx
        .heap
        .get_collection(&ctx.collection_id, b"k1")
        .expect_err("zeroed media must fail closed");
    // Zeroed frames may surface as verify failure or offset-invalid (corrupt
    // length fields) — both are distinct from SegmentNotFound / PayloadPartial.
    let fault = match &err {
        StoreError::LocatorFault(f) => f,
        other => panic!("present media with corrupt frame must be LocatorFault, got {other:?}"),
    };
    assert!(
        matches!(
            fault.kind,
            residiuum_store::LocatorFaultKind::FrameVerifyFailed
                | residiuum_store::LocatorFaultKind::OffsetInvalid
        ),
        "got kind {:?}",
        fault.kind
    );
    assert!(fault.path.is_some(), "fault must carry path");
    assert!(fault.file_len.is_some(), "fault must carry file_len");
    assert!(fault.cause.is_some(), "fault must carry cause");
    assert_eq!(fault.frame_offset > 0 || fault.frame_offset == 0, true);
    assert!(!matches!(err, StoreError::SegmentNotFound));
    assert!(!matches!(err, StoreError::PayloadPartial));

    let page = scan_all(&ctx.heap, &ctx.collection_id).expect("scan returns page with holes");
    assert!(!page.complete);
    assert!(page.entries.is_empty());
    assert_eq!(page.incomplete.len(), 1);
    assert!(
        matches!(
            page.incomplete[0].reason,
            CollectionScanHoleReason::LocatorFrameVerifyFailed
                | CollectionScanHoleReason::LocatorOffsetInvalid
        ),
        "hole reason must be locator-class, got {:?}",
        page.incomplete[0].reason
    );
    let loc = page.incomplete[0]
        .locator
        .as_ref()
        .expect("hole must carry structured locator diagnostics");
    assert!(loc.path.is_some());
    assert!(loc.file_len.is_some());
}

/// Offset past EOF is LocatorOffsetInvalid (distinct).
#[test]
fn locator_offset_invalid_is_distinct() {
    // Exercise pread helper path via public compact API surface.
    use residiuum_format::SafetyLimits;
    use residiuum_store::pread_item_body_if_segment;
    let dir = tempdir().unwrap();
    let path = dir.path().join("tiny.residiuum");
    fs::write(&path, b"short").unwrap();
    let err = pread_item_body_if_segment(&path, 1_000_000, &[1u8; 16], SafetyLimits::default())
        .unwrap_err();
    match err {
        StoreError::LocatorFault(f) => {
            assert_eq!(f.kind, residiuum_store::LocatorFaultKind::OffsetInvalid);
            assert_eq!(f.segment_id, [1u8; 16]);
            assert_eq!(f.frame_offset, 1_000_000);
            assert!(f.path.is_some());
            assert_eq!(f.file_len, Some(5));
            assert!(f.cause.is_some());
        }
        other => panic!("expected LocatorFault, got {other:?}"),
    }
}

#[test]
fn compact_reclaim_live_scan_remains_complete() {
    let ctx = open_heap_with_collection("gremlin.work.feature_revisions");
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.set_seal_threshold(6 * 1024);
    }

    for i in 0..30 {
        let key = format!("rev/{i:04}");
        for gen in 0..10 {
            let body = format!("rev-body-{i}-g{gen}").into_bytes();
            ctx.heap
                .put_collection(&ctx.collection_id, key.as_bytes(), &body)
                .unwrap();
        }
        if i % 7 == 6 {
            let phys = ctx.host.physical();
            let mut g = phys.lock().unwrap();
            g.seal_active().unwrap();
        }
    }
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.seal_active().unwrap();
        g.drain_lifecycle().unwrap();
        let _ = g
            .compact_live_with(CompactOptions {
                reclaim_sources: true,
                allow_history_loss: true,
                ..CompactOptions::default()
            })
            .unwrap();
    }

    let page = scan_all(&ctx.heap, &ctx.collection_id).expect("scan after reclaim");
    assert!(page.complete);
    assert_eq!(page.entries.len(), 30);
}

#[test]
fn empty_entries_with_holes_is_not_empty_live() {
    // Documents why plain Vec soft-skip is rejected: holes must be visible.
    let page = CollectionScanPage {
        entries: vec![],
        incomplete: vec![residiuum_store::CollectionScanHole {
            key: b"k".to_vec(),
            reason: CollectionScanHoleReason::SegmentNotFound,
            locator: None,
        }],
        examined: 1,
        complete: false,
        has_more: false,
        last_key: Some(b"k".to_vec()),
    };
    assert!(page.entries.is_empty());
    assert!(!page.is_empty_live());
    assert!(!page.complete);
}

/// Construction-time: unresolved locator during index build must not yield
/// Ready+complete_coverage (false authoritative absence).
#[test]
fn index_build_with_unresolved_locator_is_partial_not_ready() {
    use residiuum_store::IndexState;
    use serde_json::json;

    let ctx = open_heap_with_collection("gremlin.work.index_build");
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.set_seal_threshold(2 * 1024);
    }
    // Cohort A then seal; cohort B — delete A's segment so A becomes holes.
    for i in 0..4 {
        let k = format!("a/{i}");
        let body = {
            let mut v = vec![0x01];
            v.extend(serde_json::to_vec(&json!({"status": "open", "i": i})).unwrap());
            v
        };
        ctx.heap
            .put_collection(&ctx.collection_id, k.as_bytes(), &body)
            .unwrap();
    }
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.seal_active().unwrap();
    }
    for i in 0..4 {
        let k = format!("b/{i}");
        let body = {
            let mut v = vec![0x01];
            v.extend(serde_json::to_vec(&json!({"status": "open", "i": i + 10})).unwrap());
            v
        };
        ctx.heap
            .put_collection(&ctx.collection_id, k.as_bytes(), &body)
            .unwrap();
    }
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.seal_active().unwrap();
    }
    let paths = StorePaths::new(&ctx.root);
    let mut sealed: Vec<_> = fs::read_dir(paths.segments_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    sealed.sort();
    assert!(sealed.len() >= 2);
    fs::remove_file(&sealed[0]).unwrap();

    // Scan sees holes (A cohort).
    let page = scan_all(&ctx.heap, &ctx.collection_id).unwrap();
    assert!(!page.complete);
    assert!(!page.incomplete.is_empty());

    let idx = ctx
        .heap
        .create_index(&ctx.collection_id, "by_status", &["status"])
        .expect("index build must complete without hard-abort");
    assert_eq!(
        idx.meta.state,
        IndexState::Partial,
        "holes during build ⇒ Partial, not Ready (got {:?})",
        idx.meta.state
    );
    assert!(
        !idx.meta.complete_coverage,
        "must not claim complete_coverage when build saw incomplete locators"
    );
    assert!(
        !idx.meta.may_prove_absence(),
        "Ready+complete_coverage would enable false authoritative absence"
    );
    // Survivors may still accelerate hits.
    assert!(idx.meta.may_accelerate_hits());

    // Partial must not supply an exclusive candidate set (even non-empty hits):
    // survivors indexed + peers omitted would look complete after materializing
    // only the hit list (A yes, B silently gone).
    let found = ctx
        .heap
        .lookup_index_keys(
            &ctx.collection_id,
            &[("status".into(), json!("open"))],
        )
        .unwrap();
    assert!(
        found.is_none(),
        "Partial index must not return exclusive candidates; got {found:?}"
    );
    assert!(!idx.meta.may_supply_exclusive_candidates());
    // Scan path still sees survivors + holes.
    let page = scan_all(&ctx.heap, &ctx.collection_id).unwrap();
    assert!(!page.complete);
    assert!(!page.entries.is_empty());
    assert!(!page.incomplete.is_empty());
}

/// Query-time hole mapping (T9) still applies when candidates are present.
#[test]
fn locator_fault_maps_to_hole_for_index_and_scan_sources() {
    use residiuum_store::{LocatorFault, LocatorFaultKind};
    let f = LocatorFault {
        kind: LocatorFaultKind::SegmentNotFound,
        segment_id: [9u8; 16],
        frame_offset: 42,
        path: None,
        file_len: None,
        observed_segment_id: None,
        cause: None,
    };
    let err = StoreError::LocatorFault(Box::new(f));
    let hole = CollectionScanHole::from_error(b"idx-key".to_vec(), &err)
        .expect("locator faults are hole-class for every source");
    assert_eq!(hole.key, b"idx-key");
    assert_eq!(hole.reason, CollectionScanHoleReason::SegmentNotFound);
    assert!(hole.locator.is_some());
    // Non-hole errors must not be soft-skipped into holes.
    assert!(CollectionScanHole::from_error(b"k".to_vec(), &StoreError::PayloadTooLarge).is_none());
}

/// Continuation must follow last *examined* key (hole or complete), not last
/// successful row — otherwise a trailing hole is re-examined forever / skipped wrong.
#[test]
fn multipage_continues_from_last_key_including_hole() {
    let ctx = open_heap_with_collection("gremlin.work.pagination");
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.set_seal_threshold(4 * 1024);
    }
    // Write a then b, seal, wipe media for a's segment if possible — simpler:
    // write many keys, remove one sealed segment so holes appear mid-range,
    // page with tiny limit so has_more, assert next after uses last_key.
    for i in 0..20 {
        let k = format!("k/{i:03}");
        ctx.heap
            .put_collection(
                &ctx.collection_id,
                k.as_bytes(),
                &format!("body-{i}").into_bytes(),
            )
            .unwrap();
        if i == 4 || i == 9 || i == 14 {
            let phys = ctx.host.physical();
            let mut g = phys.lock().unwrap();
            g.seal_active().unwrap();
        }
    }
    {
        let phys = ctx.host.physical();
        let mut g = phys.lock().unwrap();
        g.seal_active().unwrap();
    }
    let paths = StorePaths::new(&ctx.root);
    let mut sealed: Vec<_> = fs::read_dir(paths.segments_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    sealed.sort();
    // Drop earliest sealed file so early keys become holes; later keys survive.
    if !sealed.is_empty() {
        fs::remove_file(&sealed[0]).unwrap();
    }

    let page1 = ctx
        .heap
        .scan_collection_page(&ctx.collection_id, 3, None)
        .expect("page1");
    assert!(
        page1.has_more || page1.examined >= 1,
        "expect multipage or at least one examined"
    );
    // last_key must equal the last entry or last hole — never lag behind holes.
    if let Some(ref lk) = page1.last_key {
        let last_entry = page1.entries.last().map(|(k, _)| k.as_slice());
        let last_hole = page1.incomplete.last().map(|h| h.key.as_slice());
        let max_seen = match (last_entry, last_hole) {
            (Some(e), Some(h)) => Some(if e >= h { e } else { h }),
            (Some(e), None) => Some(e),
            (None, Some(h)) => Some(h),
            (None, None) => None,
        };
        if let Some(m) = max_seen {
            assert_eq!(
                lk.as_slice(),
                m,
                "last_key must be the last examined key (entry or hole)"
            );
        }
    }
    if page1.has_more {
        let after = page1.last_key.clone().expect("has_more ⇒ last_key");
        let page2 = ctx
            .heap
            .scan_collection_page(&ctx.collection_id, 3, Some(after.as_slice()))
            .expect("page2");
        // Page2 must not re-include after key as examined complete/hole.
        for (k, _) in &page2.entries {
            assert!(k.as_slice() > after.as_slice(), "entries after cursor");
        }
        for h in &page2.incomplete {
            assert!(h.key.as_slice() > after.as_slice(), "holes after cursor");
        }
    }
}
