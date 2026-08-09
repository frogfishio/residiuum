//! CSQ-7 — damage / salvage / deterministic recovery (first labor cut).
//!
//! Behavioural floors for `CSQ-DMG-001`…`005` and `CSQ-REC-001`…`005`.
//! Deep multi-topology damage campaigns and encryption-unavailable cases remain
//! residual depth for later cuts; this package links DEF-011 salvage honesty.

use residiuum_format::{scan_forward, FrameKind, SafetyLimits};
use residiuum_store::{
    try_load_recovery_manifest, DurabilityMode, SalvageMode, Store, StoreError, StorePaths,
};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

fn seed_two_segment_store(path: &Path) {
    let mut store = Store::create(path).unwrap();
    store
        .put("early", b"early-v1", DurabilityMode::Durable)
        .unwrap();
    store
        .put("keep", b"keep-v1", DurabilityMode::Durable)
        .unwrap();
    store.seal_active().unwrap();
    store
        .put("late", b"late-v1", DurabilityMode::Durable)
        .unwrap();
    store
        .put("keep", b"keep-v2", DurabilityMode::Durable)
        .unwrap();
}

fn sealed_segment_file(root: &Path) -> PathBuf {
    let segments = root.join("segments");
    let mut sealed: Vec<_> = fs::read_dir(&segments)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("residiuum"))
        .collect();
    sealed.sort();
    sealed.into_iter().next().expect("sealed segment")
}

fn corrupt_middle_bytes(path: &Path, xor: u8) {
    let mut bytes = fs::read(path).unwrap();
    if bytes.len() > 100 {
        let mid = bytes.len() / 2;
        let end = mid + 40.min(bytes.len() - mid);
        for b in &mut bytes[mid..end] {
            *b ^= xor;
        }
        fs::write(path, &bytes).unwrap();
    }
}

fn collect_verified_item_bodies(root: &Path) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut files = Vec::new();
    let active = root.join("active").join("active.residiuum");
    if active.is_file() {
        files.push(active);
    }
    let segs = root.join("segments");
    if segs.is_dir() {
        let mut sealed: Vec<_> = fs::read_dir(&segs)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("residiuum"))
            .collect();
        sealed.sort();
        files.extend(sealed);
    }
    for f in files {
        let bytes = fs::read(&f).unwrap();
        let report = scan_forward(&bytes, SafetyLimits::default());
        for (_off, frame) in report.verified_frames() {
            if frame.header.known_kind() == Some(FrameKind::ItemEvent) {
                out.push(frame.body.clone());
            }
        }
    }
    out
}

/// CSQ-DMG-001/002 — corrupted sealed region never invents a verified payload;
/// healthy later units remain authoritative.
#[test]
fn csq_dmg_corrupt_bytes_never_verified_survivors_readable() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    seed_two_segment_store(&path);

    let seg = sealed_segment_file(&path);
    corrupt_middle_bytes(&seg, 0x5a);

    let store = Store::open(&path).unwrap();
    // Active-segment "late" and latest "keep" must remain exact.
    assert_eq!(
        store.get("late").unwrap().as_deref(),
        Some(b"late-v1".as_slice())
    );
    assert_eq!(
        store.get("keep").unwrap().as_deref(),
        Some(b"keep-v2".as_slice())
    );

    // Salvage must find verified islands and report holes without fabricating early.
    let report = store.salvage().unwrap();
    assert!(report.verified_frames >= 1);
    assert!(report.live_subjects >= 1);
    // Corrupted early may be lost; absence/damage is never "success with wrong body".
    match store.get("early") {
        Ok(Some(body)) => assert_eq!(
            body.as_slice(),
            b"early-v1",
            "if early is readable it must be exact, not corrupted garbage"
        ),
        Ok(None) | Err(StoreError::LocatorFault(_)) => {}
        Err(error) => panic!("unexpected damage classification: {error}"),
    }
}

/// CSQ-DMG-003/004 — multi-fault (hole + incomplete tail) still terminates and
/// records hole evidence; survivors stay discoverable.
#[test]
fn csq_dmg_multi_fault_holes_and_incomplete_tail() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let dst = dir.path().join("dst");
    seed_two_segment_store(&path);

    let seg = sealed_segment_file(&path);
    corrupt_middle_bytes(&seg, 0xa5);

    // Incomplete tail on active segment.
    let active = path.join("active").join("active.residiuum");
    let mut f = OpenOptions::new().append(true).open(&active).unwrap();
    f.write_all(b"RESIDFRM").unwrap();
    f.write_all(&[0u8; 40]).unwrap();
    f.sync_all().unwrap();

    let src = Store::open_inspect(&path).unwrap();
    let report = src.salvage_to(&dst).unwrap();
    assert_eq!(report.mode, SalvageMode::Evidence);
    assert!(
        report.holes_recorded > 0 || report.source.holes > 0,
        "multi-fault must surface hole evidence"
    );
    assert!(report.subjects_copied >= 1);

    let dest = Store::open_with_options(
        &dst,
        residiuum_store::StoreOpenOptions::default().tolerate_unidentified_inventory(),
    )
    .unwrap();
    assert_eq!(
        dest.get("late").unwrap().as_deref(),
        Some(b"late-v1".as_slice())
    );
    assert_eq!(
        dest.get("keep").unwrap().as_deref(),
        Some(b"keep-v2".as_slice())
    );

    let manifest = try_load_recovery_manifest(&StorePaths::new(&dst))
        .unwrap()
        .expect("recovery manifest");
    let total_holes: u64 = manifest.files.iter().map(|f| f.holes.len() as u64).sum();
    assert_eq!(total_holes, report.holes_recorded);
    assert!(!manifest.content_hash_hex.is_empty());
}

/// CSQ-REC-001/002 — reopening identical bytes yields identical conclusions;
/// repeated salvage is idempotent on verified item bodies.
#[test]
fn csq_rec_reopen_identical_and_resalvage_idempotent() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let mid = dir.path().join("mid");
    let dst = dir.path().join("dst");
    {
        let mut store = Store::create(&src).unwrap();
        store.put("a", b"1", DurabilityMode::Durable).unwrap();
        store.put("b", b"2", DurabilityMode::Durable).unwrap();
        store.delete("a", DurabilityMode::Durable).unwrap();
        store.put("a", b"3", DurabilityMode::Durable).unwrap();
    }

    let open1 = Store::open_inspect(&src).unwrap();
    let g1_a = open1.get("a").unwrap();
    let g1_b = open1.get("b").unwrap();
    let h1 = open1.history("a").unwrap().events.len();
    drop(open1);

    let open2 = Store::open_inspect(&src).unwrap();
    assert_eq!(open2.get("a").unwrap(), g1_a);
    assert_eq!(open2.get("b").unwrap(), g1_b);
    assert_eq!(open2.history("a").unwrap().events.len(), h1);

    open2.salvage_to(&mid).unwrap();
    Store::open_inspect(&mid).unwrap().salvage_to(&dst).unwrap();
    assert_eq!(
        collect_verified_item_bodies(&mid),
        collect_verified_item_bodies(&dst),
        "re-salvage must be deterministic on verified item bodies"
    );

    let dest = Store::open_with_options(
        &dst,
        residiuum_store::StoreOpenOptions::default().tolerate_unidentified_inventory(),
    )
    .unwrap();
    assert_eq!(dest.get("a").unwrap().as_deref(), Some(b"3".as_slice()));
    assert_eq!(dest.get("b").unwrap().as_deref(), Some(b"2".as_slice()));
}

/// CSQ-REC-003 — live get, clean reopen, cacheless rebuild, and salvage_to agree
/// on authoritative live observations.
#[test]
fn csq_rec_live_reopen_rebuild_salvage_agree() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let salvaged = dir.path().join("salvaged");
    {
        let mut store = Store::create(&path).unwrap();
        store.put("x", b"1", DurabilityMode::Durable).unwrap();
        store.put("y", b"2", DurabilityMode::Durable).unwrap();
        store.delete("x", DurabilityMode::Durable).unwrap();
        store.put("z", b"3", DurabilityMode::Durable).unwrap();
        store.rebuild_catalogs().unwrap();
    }

    let live = Store::open(&path).unwrap();
    let live_x = live.get("x").unwrap();
    let live_y = live.get("y").unwrap();
    let live_z = live.get("z").unwrap();
    let live_count = live.live_count();
    drop(live);

    // Wipe derived caches then reopen + rebuild.
    for name in ["catalogs", "indexes", "snapshots"] {
        let p = path.join(name);
        if p.exists() {
            fs::remove_dir_all(&p).unwrap();
        }
    }
    let mut rebuilt = Store::open(&path).unwrap();
    rebuilt.rebuild_index().unwrap();
    assert_eq!(rebuilt.get("x").unwrap(), live_x);
    assert_eq!(rebuilt.get("y").unwrap(), live_y);
    assert_eq!(rebuilt.get("z").unwrap(), live_z);
    assert_eq!(rebuilt.live_count(), live_count);

    Store::open_inspect(&path)
        .unwrap()
        .salvage_to(&salvaged)
        .unwrap();
    let dest = Store::open_with_options(
        &salvaged,
        residiuum_store::StoreOpenOptions::default().tolerate_unidentified_inventory(),
    )
    .unwrap();
    assert_eq!(dest.get("x").unwrap(), live_x);
    assert_eq!(dest.get("y").unwrap(), live_y);
    assert_eq!(dest.get("z").unwrap(), live_z);
    assert_eq!(dest.live_count(), live_count);
}

/// CSQ-REC-004 — salvage_to never mutates source bytes (mtime / size stable).
#[test]
fn csq_rec_salvage_never_writes_source() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    {
        let mut store = Store::create(&src).unwrap();
        store.put("k", b"v", DurabilityMode::Durable).unwrap();
    }
    let active = src.join("active").join("active.residiuum");
    let before_meta = fs::metadata(&active).unwrap();
    let before_len = before_meta.len();
    let before_mtime = before_meta.modified().unwrap();
    let before_bytes = fs::read(&active).unwrap();

    Store::open_inspect(&src).unwrap().salvage_to(&dst).unwrap();

    let after_meta = fs::metadata(&active).unwrap();
    assert_eq!(after_meta.len(), before_len);
    assert_eq!(after_meta.modified().unwrap(), before_mtime);
    assert_eq!(fs::read(&active).unwrap(), before_bytes);
}

/// CSQ-REC-005 — after recoverable damage, healthy work continues and damage
/// evidence remains via salvage/manifest.
#[test]
fn csq_rec_healthy_work_continues_after_damage() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    seed_two_segment_store(&path);
    let seg = sealed_segment_file(&path);
    corrupt_middle_bytes(&seg, 0x3c);

    let mut store = Store::open(&path).unwrap();
    // Continue healthy writes on the live path.
    store
        .put("post-damage", b"new-work", DurabilityMode::Durable)
        .unwrap();
    assert_eq!(
        store.get("post-damage").unwrap().as_deref(),
        Some(b"new-work".as_slice())
    );
    assert_eq!(
        store.get("late").unwrap().as_deref(),
        Some(b"late-v1".as_slice())
    );

    let report = store.salvage().unwrap();
    assert!(
        report.holes > 0 || report.verified_frames >= 1,
        "damage evidence or surviving frames must remain observable"
    );
}

/// Incomplete tail must not poison earlier complete frames (DMG locality).
#[test]
fn csq_dmg_incomplete_tail_locality() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut store = Store::create(&path).unwrap();
        store
            .put("keep", b"alive", DurabilityMode::Durable)
            .unwrap();
        store.put("also", b"ok", DurabilityMode::Durable).unwrap();
    }
    let active = path.join("active").join("active.residiuum");
    let mut f = OpenOptions::new().append(true).open(&active).unwrap();
    f.write_all(b"RESIDFRM").unwrap();
    f.write_all(&[0u8; 40]).unwrap();
    f.sync_all().unwrap();

    let store = Store::open(&path).unwrap();
    assert_eq!(
        store.get("keep").unwrap().as_deref(),
        Some(b"alive".as_slice())
    );
    assert_eq!(
        store.get("also").unwrap().as_deref(),
        Some(b"ok".as_slice())
    );
    let report = store.salvage().unwrap();
    assert!(report.item_events >= 2);
    assert_eq!(report.live_subjects, 2);
}

/// Live-state export is a distinct recovery mode (new lineage, no hole manifest).
#[test]
fn csq_rec_live_export_vs_evidence_differential() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let evidence = dir.path().join("evidence");
    let live_export = dir.path().join("live");
    {
        let mut store = Store::create(&src).unwrap();
        store.put("k", b"v1", DurabilityMode::Durable).unwrap();
        store.put("k", b"v2", DurabilityMode::Durable).unwrap();
        store.put("gone", b"x", DurabilityMode::Durable).unwrap();
        store.delete("gone", DurabilityMode::Durable).unwrap();
    }
    let inspect = Store::open_inspect(&src).unwrap();
    let src_hist_len = inspect.history("k").unwrap().events.len();
    let src_event = inspect.history("k").unwrap().events[0].event_id;

    let ev_report = inspect.salvage_to(&evidence).unwrap();
    assert_eq!(ev_report.mode, SalvageMode::Evidence);
    assert!(ev_report.frames_copied > 0);
    assert!(ev_report.manifest_path.is_some());

    let live_report = inspect.export_live_state(&live_export).unwrap();
    assert_eq!(live_report.mode, SalvageMode::LiveStateExport);
    assert_eq!(live_report.frames_copied, 0);
    assert!(live_report.manifest_path.is_none());

    let ev = Store::open_inspect(&evidence).unwrap();
    assert_eq!(ev.history("k").unwrap().events.len(), src_hist_len);
    assert_eq!(ev.get("k").unwrap().as_deref(), Some(b"v2".as_slice()));

    let live = Store::open_inspect(&live_export).unwrap();
    assert_eq!(live.get("k").unwrap().as_deref(), Some(b"v2".as_slice()));
    assert_eq!(live.history("k").unwrap().events.len(), 1);
    assert_ne!(
        live.history("k").unwrap().events[0].event_id,
        src_event,
        "live export mints new lineage"
    );
    assert!(live.history("gone").unwrap().events.is_empty());
}
