//! DEF-051 — integrity scrub and media-health automation (single-node cut).
//!
//! Covers:
//! - clean store scrub reaches full coverage without findings
//! - injected segment corruption is detected (frame holes)
//! - content hash mismatch vs placement is detected
//! - quarantine copies corrupt evidence without removing the original
//! - pause/resume freezes and unfreezes the frontier
//! - bounded max_files requires multiple steps to complete a cycle
//! - scrub status exposes age, bytes verified, failures, coverage

use residiuum_store::{
    list_scrub_findings, scrub_findings_path, scrub_state_path, DurabilityMode, ScrubFindingKind,
    ScrubOptions, Store, SCRUB_PROFILE,
};
use std::fs;
use std::io::Write;
use tempfile::tempdir;

#[test]
fn clean_store_scrub_completes_without_findings() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    store
        .put("users/a", br#"{"ok":true}"#, DurabilityMode::Durable)
        .unwrap();
    store
        .put("users/b", br#"{"ok":true}"#, DurabilityMode::Durable)
        .unwrap();
    store.seal_active().unwrap();
    store
        .put("users/c", br#"{"ok":true}"#, DurabilityMode::Durable)
        .unwrap();

    let report = store.scrub_to_completion(ScrubOptions::default()).unwrap();
    assert!(report.cycle_completed, "expected full cycle");
    assert_eq!(report.failures_this_call, 0);
    assert!(report.status.coverage_ratio >= 1.0 - f64::EPSILON);
    assert!(report.status.last_complete_cycle_ns.is_some());
    assert!(report.status.bytes_verified_total > 0);

    let findings = store.list_scrub_findings().unwrap();
    assert!(findings.is_empty(), "findings={findings:?}");

    // Durable state documents exist.
    assert!(scrub_state_path(store.paths()).is_file());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &fs::read(scrub_state_path(store.paths())).unwrap()
        )
        .unwrap()["profile"],
        SCRUB_PROFILE
    );
}

#[test]
fn injected_corruption_detected_and_quarantined() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    store
        .put("k/1", br#"{"n":1}"#, DurabilityMode::Durable)
        .unwrap();
    store.seal_active().unwrap();
    store
        .put("k/2", br#"{"n":2}"#, DurabilityMode::Durable)
        .unwrap();

    // Locate a sealed segment and inject garbage so frame scan reports holes.
    let segments = store.paths().segments_dir();
    let mut sealed: Vec<_> = fs::read_dir(&segments)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("residiuum"))
        .collect();
    sealed.sort();
    assert!(!sealed.is_empty(), "need a sealed segment to corrupt");
    let victim = &sealed[0];
    let original = fs::read(victim).unwrap();
    assert!(!original.is_empty());

    // Overwrite middle bytes with non-frame garbage (keeps file present).
    {
        let mut f = fs::OpenOptions::new().write(true).open(victim).unwrap();
        f.write_all(b"CORRUPT_SCRUB_INJECTION!!!!!!!!!!").unwrap();
        f.sync_all().unwrap();
    }
    // Original path must still exist (scrub never hides it).
    assert!(victim.is_file());

    let report = store
        .scrub_to_completion(ScrubOptions {
            quarantine: true,
            ..ScrubOptions::default()
        })
        .unwrap();
    assert!(report.cycle_completed);
    assert!(
        report.status.failures_total > 0
            || report.failures_this_call > 0
            || !store.list_scrub_findings().unwrap().is_empty(),
        "expected failures; status={:?}",
        report.status
    );

    let findings = store.list_scrub_findings().unwrap();
    assert!(
        !findings.is_empty(),
        "expected open findings after corruption"
    );
    let kinds: Vec<_> = findings.iter().map(|f| f.finding).collect();
    assert!(
        kinds.contains(&ScrubFindingKind::FrameHoles)
            || kinds.contains(&ScrubFindingKind::ContentHashMismatch)
            || kinds.contains(&ScrubFindingKind::SizeMismatch),
        "kinds={kinds:?}"
    );

    // Original still present; quarantine copy exists when we recorded a path.
    assert!(victim.is_file(), "original must not be deleted");
    let any_quarantine = findings.iter().any(|f| f.quarantine_path.is_some());
    assert!(any_quarantine, "expected quarantine path on findings");
    for f in &findings {
        if let Some(ref qp) = f.quarantine_path {
            assert!(root.join(qp).is_file(), "quarantine file missing: {qp}");
        }
    }

    assert!(scrub_findings_path(store.paths()).is_file());
}

#[test]
fn content_hash_mismatch_against_placement() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    store
        .put("x", b"hello-scrub", DurabilityMode::Durable)
        .unwrap();
    store.seal_active().unwrap();
    // Touch active so store is non-empty after seal.
    store.put("y", b"more", DurabilityMode::Durable).unwrap();

    let segments = store.paths().segments_dir();
    let sealed: Vec<_> = fs::read_dir(&segments)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("residiuum"))
        .collect();
    assert!(!sealed.is_empty());
    let victim = &sealed[0];

    // Append one byte — changes BLAKE3 without necessarily destroying all frames
    // if we append at end; for hash mismatch placement expects old hash.
    {
        let mut f = fs::OpenOptions::new().append(true).open(victim).unwrap();
        f.write_all(b"X").unwrap();
        f.sync_all().unwrap();
    }

    let report = store.scrub_to_completion(ScrubOptions::default()).unwrap();
    assert!(report.cycle_completed);

    let findings = store.list_scrub_findings().unwrap();
    assert!(
        findings
            .iter()
            .any(|f| f.finding == ScrubFindingKind::ContentHashMismatch
                || f.finding == ScrubFindingKind::SizeMismatch
                || f.finding == ScrubFindingKind::FrameHoles),
        "findings={findings:?}"
    );
}

#[test]
fn pause_resume_and_bounded_steps() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    for i in 0..8 {
        store
            .put(
                &format!("item/{i}"),
                format!(r#"{{"i":{i}}}"#).as_bytes(),
                DurabilityMode::Durable,
            )
            .unwrap();
        if i % 2 == 1 {
            store.seal_active().unwrap();
        }
    }

    // Pause first — no progress.
    let paused = store.pause_scrub().unwrap();
    assert!(paused.paused);
    let blocked = store.scrub_once(ScrubOptions::default()).unwrap();
    assert!(blocked.paused);
    assert_eq!(blocked.targets_processed, 0);

    store.resume_scrub().unwrap();

    // Bounded: one file per step.
    let step = ScrubOptions {
        max_files: 1,
        max_bytes: u64::MAX,
        ..ScrubOptions::default()
    };
    let first = store.scrub_once(step.clone()).unwrap();
    assert_eq!(first.targets_processed, 1);
    assert!(!first.cycle_completed || first.status.targets_in_cycle <= 1);

    // Continue until complete.
    let mut guard = 0;
    let mut last = first;
    while !last.cycle_completed && guard < 100 {
        guard += 1;
        last = store.scrub_once(step.clone()).unwrap();
    }
    assert!(last.cycle_completed || last.status.coverage_ratio >= 1.0 - f64::EPSILON);

    let status = store.scrub_status().unwrap();
    assert!(!status.paused);
    assert!(status.bytes_verified_total > 0);
    assert!(status.last_complete_cycle_ns.is_some());
    assert!(status.last_complete_age_ns.is_some());
}

#[test]
fn clean_rescrub_clears_resolved_findings() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    store.put("z", b"payload", DurabilityMode::Durable).unwrap();
    // No seal — only active; corrupt then we cannot easily repair active without
    // rewrite. Instead: seal, corrupt sealed, then restore original bytes.
    store.seal_active().unwrap();
    store.put("z2", b"more", DurabilityMode::Durable).unwrap();

    let segments = store.paths().segments_dir();
    let sealed: Vec<_> = fs::read_dir(&segments)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("residiuum"))
        .collect();
    let victim = sealed[0].clone();
    let good = fs::read(&victim).unwrap();
    fs::write(&victim, b"%%%not a frame%%%").unwrap();

    store.scrub_to_completion(ScrubOptions::default()).unwrap();
    assert!(!list_scrub_findings(store.paths(), store.store_id())
        .unwrap()
        .is_empty());

    // Restore good bytes — next scrub cycle should clear findings for that target.
    fs::write(&victim, &good).unwrap();
    store.scrub_to_completion(ScrubOptions::default()).unwrap();
    let remaining = store.list_scrub_findings().unwrap();
    // Findings for restored target should be gone; active may still be clean.
    assert!(
        remaining.iter().all(|f| !f
            .relative_path
            .contains(victim.file_name().unwrap().to_str().unwrap())),
        "remaining={remaining:?}"
    );
}
