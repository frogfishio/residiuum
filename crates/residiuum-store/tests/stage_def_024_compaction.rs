//! DEF-024 — phased compaction with safe source reclaim.
//!
//! Guarantees under test:
//! - Default compact activates a verified generation and retains sources.
//! - Job records survive under recovery/compaction/ through every phase.
//! - Reclaim requires allow_history_loss for live-projection coverage.
//! - Crash/failpoint after create still recovers to activated or clean cancel.
//! - Reclaimed bytes are measurable; live gets remain correct after reclaim.
//! - History remains available while sources are retained.

use residiuum_store::{
    arm_failpoint_once, clear_failpoints, CompactOptions, CompactPhase, DurabilityMode,
    FailpointAction, Store,
};
use std::fs;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

/// Failpoints are process-global; serialize tests that arm them.
fn fp_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

fn seed_history(store: &mut Store) {
    store.put("keep", b"v1", DurabilityMode::Durable).unwrap();
    store.put("drop", b"gone", DurabilityMode::Durable).unwrap();
    store.put("keep", b"v2", DurabilityMode::Durable).unwrap();
    store.delete("drop", DurabilityMode::Durable).unwrap();
    store.seal_active().unwrap();
    store.put("extra", b"e1", DurabilityMode::Durable).unwrap();
    store.seal_active().unwrap();
}

#[test]
fn default_compact_retains_sources_and_history() {
    let _guard = fp_lock();
    clear_failpoints();
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    seed_history(&mut store);

    let report = store.compact_live().unwrap();
    assert!(report.sources_retained);
    assert_eq!(report.coverage, "live-projection");
    assert_eq!(report.phase, CompactPhase::Activated);
    assert_eq!(report.bytes_reclaimed, 0);
    assert!(report.bytes_estimated_read > 0);
    assert!(report.bytes_written > 0);
    assert_eq!(report.live_subjects_written, 2); // keep + extra

    assert_eq!(
        store.get("keep").unwrap().as_deref(),
        Some(b"v2".as_slice())
    );
    assert!(store.get("drop").unwrap().is_none());
    assert_eq!(
        store.get("extra").unwrap().as_deref(),
        Some(b"e1".as_slice())
    );

    let hist = store.history("keep").unwrap();
    assert!(
        hist.events.len() >= 2,
        "history must remain while sources retained"
    );

    let job = store.load_compact_job(&report.job_id).unwrap().unwrap();
    assert_eq!(job.phase, CompactPhase::Activated);
    assert!(job.sources_retained);
    assert_eq!(job.recovery_generation, 1);
}

#[test]
fn reclaim_requires_history_loss_ack() {
    let _guard = fp_lock();
    clear_failpoints();
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    seed_history(&mut store);

    let err = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: false,
            ..CompactOptions::default()
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("allow_history_loss"),
        "unexpected: {err}"
    );
}

#[test]
fn reclaim_after_activate_measures_bytes_and_keeps_live() {
    let _guard = fp_lock();
    clear_failpoints();
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    seed_history(&mut store);

    let report = store
        .compact_live_with(CompactOptions {
            reclaim_sources: true,
            allow_history_loss: true,
            tombstone_horizon_ns: Some(1),
            dedup_horizon: Some("test-horizon".into()),
        })
        .unwrap();

    assert!(!report.sources_retained);
    assert_eq!(report.phase, CompactPhase::Reclaimed);
    assert!(
        report.bytes_reclaimed > 0,
        "reclaimed bytes must be measurable"
    );
    // Output segment remains under segments/.
    let segs = root.join("segments");
    let remaining: usize = fs::read_dir(&segs)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("residiuum"))
        .count();
    assert_eq!(
        remaining, 1,
        "only the compacted output segment should remain"
    );
    assert_eq!(
        store.get("keep").unwrap().as_deref(),
        Some(b"v2".as_slice())
    );
    assert_eq!(
        store.get("extra").unwrap().as_deref(),
        Some(b"e1".as_slice())
    );
    assert!(store.get("drop").unwrap().is_none());

    // History for pre-reclaim events may be gone (live-projection reclaim).
    let hist = store.history("keep").unwrap();
    assert_eq!(
        hist.events.len(),
        1,
        "after reclaim only compacted live put remains"
    );

    let job = store.load_compact_job(&report.job_id).unwrap().unwrap();
    assert_eq!(job.phase, CompactPhase::Reclaimed);
    assert_eq!(job.dedup_horizon.as_deref(), Some("test-horizon"));
    assert_eq!(job.tombstone_horizon_ns, Some(1));
}

#[test]
fn two_phase_reclaim_via_explicit_api() {
    let _guard = fp_lock();
    clear_failpoints();
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    seed_history(&mut store);

    let report = store
        .compact_live_with(CompactOptions {
            reclaim_sources: false,
            allow_history_loss: true, // plan allows later reclaim
            ..CompactOptions::default()
        })
        .unwrap();
    assert_eq!(report.phase, CompactPhase::Activated);
    assert!(report.sources_retained);

    let reclaimed = store.reclaim_compact_job(&report.job_id).unwrap();
    assert_eq!(reclaimed.phase, CompactPhase::Reclaimed);
    assert!(reclaimed.bytes_reclaimed > 0);
    assert_eq!(
        store.get("keep").unwrap().as_deref(),
        Some(b"v2".as_slice())
    );
}

#[test]
fn failpoint_after_create_recovers_on_reopen() {
    let _guard = fp_lock();
    clear_failpoints();
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create(&root).unwrap();
        seed_history(&mut store);
        arm_failpoint_once("store.compact.after_create", FailpointAction::Error);
        let err = store.compact_live().unwrap_err();
        assert!(
            err.to_string().contains("failpoint") || err.to_string().contains("compact"),
            "unexpected: {err}"
        );
        clear_failpoints();
    }

    // Reopen must recover: verify+activate incomplete job or leave live correct.
    let store = Store::open(&root).unwrap();
    assert_eq!(
        store.get("keep").unwrap().as_deref(),
        Some(b"v2".as_slice())
    );
    assert_eq!(
        store.get("extra").unwrap().as_deref(),
        Some(b"e1".as_slice())
    );

    let jobs = store.list_compact_jobs().unwrap();
    assert!(!jobs.is_empty());
    // After recover, job should be Activated (finish from Created) or Failed.
    let phase = jobs[0].phase;
    assert!(
        matches!(
            phase,
            CompactPhase::Activated | CompactPhase::Failed | CompactPhase::Cancelled
        ),
        "unexpected recover phase {phase:?}"
    );
}

#[test]
fn cancel_before_activate_removes_output() {
    let _guard = fp_lock();
    clear_failpoints();
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    seed_history(&mut store);

    arm_failpoint_once("store.compact.after_create", FailpointAction::Error);
    let _ = store.compact_live();
    clear_failpoints();

    let jobs = store.list_compact_jobs().unwrap();
    let job = jobs
        .into_iter()
        .find(|j| matches!(j.phase, CompactPhase::Created | CompactPhase::Failed))
        .expect("created or failed job");
    let job_id = residiuum_store::unhex16(&job.job_id).expect("job id hex");

    // Create writes the segment then hits after_create failpoint → Created.
    if job.phase == CompactPhase::Created {
        let out_name = format!("{}.residiuum", job.output_segment_id);
        let out_path = root.join("segments").join(&out_name);
        assert!(
            out_path.is_file(),
            "output segment should exist before cancel"
        );
        let cancelled = store.cancel_compact_job(&job_id).unwrap();
        assert_eq!(cancelled.phase, CompactPhase::Cancelled);
        assert!(
            !out_path.is_file(),
            "cancelled job should drop unactivated output"
        );
    }
}

#[test]
fn refuse_cancel_after_activate() {
    let _guard = fp_lock();
    clear_failpoints();
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    seed_history(&mut store);
    let report = store.compact_live().unwrap();
    let err = store.cancel_compact_job(&report.job_id).unwrap_err();
    assert!(
        err.to_string().contains("cannot cancel"),
        "unexpected: {err}"
    );
}
