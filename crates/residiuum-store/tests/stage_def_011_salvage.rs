//! DEF-011: evidence-preserving salvage vs live-state export.

use residiuum_format::{scan_forward, SafetyLimits};
use residiuum_store::{try_load_recovery_manifest, DurabilityMode, SalvageMode, Store, StorePaths};
use std::fs::{self, OpenOptions};
use std::io::Write;
use tempfile::tempdir;

#[test]
fn salvage_preserves_history_tombstones_and_event_ids() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    {
        let mut store = Store::create(&src).unwrap();
        store
            .put("users/a", b"{\"v\":1}", DurabilityMode::Durable)
            .unwrap();
        store
            .put("users/a", b"{\"v\":2}", DurabilityMode::Durable)
            .unwrap();
        store.delete("users/a", DurabilityMode::Durable).unwrap();
        store
            .put("users/a", b"{\"v\":3}", DurabilityMode::Durable)
            .unwrap();
        store
            .put("users/b", b"keep", DurabilityMode::Durable)
            .unwrap();
        store.delete("users/b", DurabilityMode::Durable).unwrap();
    }

    let src_store = Store::open_inspect(&src).unwrap();
    let src_hist = src_store.history("users/a").unwrap();
    assert_eq!(src_hist.events.len(), 4);
    let src_event_ids: Vec<_> = src_hist.events.iter().map(|e| e.event_id).collect();

    let report = src_store.salvage_to(&dst).unwrap();
    assert_eq!(report.mode, SalvageMode::Evidence);
    assert!(report.frames_copied > 0);
    assert_eq!(report.subjects_copied, 1); // only users/a live
    assert!(report.manifest_path.as_ref().unwrap().is_file());

    let dest = Store::open_inspect(&dst).unwrap();
    assert_eq!(
        dest.get("users/a").unwrap().as_deref(),
        Some(b"{\"v\":3}".as_slice())
    );
    assert!(dest.get("users/b").unwrap().is_none());

    let dest_hist = dest.history("users/a").unwrap();
    assert_eq!(dest_hist.events.len(), 4);
    let dest_event_ids: Vec<_> = dest_hist.events.iter().map(|e| e.event_id).collect();
    assert_eq!(
        src_event_ids, dest_event_ids,
        "event identities must survive evidence salvage"
    );

    let paths = StorePaths::new(&dst);
    let manifest = try_load_recovery_manifest(&paths)
        .unwrap()
        .expect("manifest");
    assert_eq!(manifest.mode, SalvageMode::Evidence);
    assert!(!manifest.content_hash_hex.is_empty());
    assert_eq!(manifest.frames_copied, report.frames_copied);
}

#[test]
fn salvage_copies_verified_frames_byte_identical() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    {
        let mut store = Store::create(&src).unwrap();
        store
            .put("k", b"payload-bytes", DurabilityMode::Durable)
            .unwrap();
    }

    let src_bytes = fs::read(src.join("active").join("active.residiuum")).unwrap();
    let src_report = scan_forward(&src_bytes, SafetyLimits::default());
    let mut src_frame_slices = Vec::new();
    for region in &src_report.regions {
        if let residiuum_format::ScanRegion::VerifiedFrame { range, .. } = region {
            src_frame_slices.push(src_bytes[range.start as usize..range.end as usize].to_vec());
        }
    }
    assert!(!src_frame_slices.is_empty());

    Store::open_inspect(&src).unwrap().salvage_to(&dst).unwrap();

    // Destination sealed segment(s) must contain each source verified frame verbatim.
    let segs = dst.join("segments");
    let mut dest_pool = Vec::new();
    for entry in fs::read_dir(&segs).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) == Some("residiuum") {
            dest_pool.extend_from_slice(&fs::read(&path).unwrap());
        }
    }
    for frame in &src_frame_slices {
        assert!(
            dest_pool
                .windows(frame.len())
                .any(|w| w == frame.as_slice()),
            "verified frame missing byte-identically in destination"
        );
    }
}

#[test]
fn salvage_records_holes_and_still_recovers_survivors() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let dst = dir.path().join("dst");
    let early_segment;
    {
        let mut store = Store::create(&path).unwrap();
        early_segment = store
            .put("early", b"1", DurabilityMode::Durable)
            .unwrap()
            .segment_id;
        store.seal_active().unwrap();
        store.put("late", b"2", DurabilityMode::Durable).unwrap();
    }

    // Corrupt `early` specifically. Orderly close also seals `late`, so an
    // arbitrary directory entry no longer identifies the damage target.
    let segment_hex: String = early_segment
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let seg_file = path
        .join("segments")
        .join(format!("{segment_hex}.residiuum"));
    let mut bytes = fs::read(&seg_file).unwrap();
    if bytes.len() > 80 {
        for b in &mut bytes[40..80] {
            *b ^= 0x5a;
        }
        fs::write(&seg_file, &bytes).unwrap();
    }

    let report = Store::open_inspect(&path)
        .unwrap()
        .salvage_to(&dst)
        .unwrap();
    assert!(report.holes_recorded > 0 || report.source.holes > 0);

    let dest = Store::open_with_options(
        &dst,
        residiuum_store::StoreOpenOptions::default().tolerate_unidentified_inventory(),
    )
    .unwrap();
    // At least the uncorrupted orderly-sealed "late" value must survive.
    assert_eq!(dest.get("late").unwrap().as_deref(), Some(b"2".as_slice()));

    let manifest = try_load_recovery_manifest(&StorePaths::new(&dst))
        .unwrap()
        .expect("manifest");
    let total_holes: u64 = manifest.files.iter().map(|f| f.holes.len() as u64).sum();
    assert_eq!(total_holes, report.holes_recorded);
}

#[test]
fn export_live_state_uses_new_lineage() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    {
        let mut store = Store::create(&src).unwrap();
        store.put("k", b"v", DurabilityMode::Durable).unwrap();
        store.put("gone", b"x", DurabilityMode::Durable).unwrap();
        store.delete("gone", DurabilityMode::Durable).unwrap();
    }

    let src_hist = Store::open_inspect(&src).unwrap().history("k").unwrap();
    let src_event = src_hist.events[0].event_id;

    let report = Store::open_inspect(&src)
        .unwrap()
        .export_live_state(&dst)
        .unwrap();
    assert_eq!(report.mode, SalvageMode::LiveStateExport);
    assert_eq!(report.subjects_copied, 1);
    assert_eq!(report.frames_copied, 0);
    assert!(report.manifest_path.is_none());

    let dest = Store::open_inspect(&dst).unwrap();
    assert_eq!(dest.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
    assert!(dest.get("gone").unwrap().is_none());
    let dest_hist = dest.history("k").unwrap();
    assert_eq!(dest_hist.events.len(), 1);
    assert_ne!(
        dest_hist.events[0].event_id, src_event,
        "live export must mint new event lineage"
    );
    // Tombstone history for gone is not preserved under live export.
    let gone_hist = dest.history("gone").unwrap();
    assert!(gone_hist.events.is_empty());
}

#[test]
fn re_salvage_is_deterministic_on_frame_bytes() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let mid = dir.path().join("mid");
    let dst = dir.path().join("dst");
    {
        let mut store = Store::create(&src).unwrap();
        store.put("a", b"1", DurabilityMode::Durable).unwrap();
        store.put("b", b"2", DurabilityMode::Durable).unwrap();
    }

    Store::open_inspect(&src).unwrap().salvage_to(&mid).unwrap();
    Store::open_inspect(&mid).unwrap().salvage_to(&dst).unwrap();

    let collect_item_bodies = |root: &std::path::Path| -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let segs = root.join("segments");
        if segs.is_dir() {
            let mut files: Vec<_> = fs::read_dir(&segs)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("residiuum"))
                .collect();
            files.sort();
            for f in files {
                let bytes = fs::read(&f).unwrap();
                let report = scan_forward(&bytes, SafetyLimits::default());
                for (_off, frame) in report.verified_frames() {
                    if frame.header.known_kind() == Some(residiuum_format::FrameKind::ItemEvent) {
                        out.push(frame.body.clone());
                    }
                }
            }
        }
        out
    };

    let mid_bodies = collect_item_bodies(&mid);
    let dst_bodies = collect_item_bodies(&dst);
    assert_eq!(mid_bodies, dst_bodies);
}

#[test]
fn incomplete_tail_does_not_poison_salvage_to() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let dst = dir.path().join("dst");
    {
        let mut store = Store::create(&path).unwrap();
        store
            .put("keep", b"alive", DurabilityMode::Durable)
            .unwrap();
    }
    let active = path.join("active").join("active.residiuum");
    let mut f = OpenOptions::new().append(true).open(&active).unwrap();
    f.write_all(b"RESIDFRM").unwrap();
    f.write_all(&[0u8; 40]).unwrap();
    f.sync_all().unwrap();

    let report = Store::open_inspect(&path)
        .unwrap()
        .salvage_to(&dst)
        .unwrap();
    assert!(report.holes_recorded > 0 || report.source.holes > 0);
    let dest = Store::open_with_options(
        &dst,
        residiuum_store::StoreOpenOptions::default().tolerate_unidentified_inventory(),
    )
    .unwrap();
    assert_eq!(
        dest.get("keep").unwrap().as_deref(),
        Some(b"alive".as_slice())
    );
}
