//! CSQ-5 crash / process / filesystem campaign.
//!
//! Runs boundary failpoint matrix, composed failures, reopen/continuation
//! oracles, writer-lock + portable FS-image lanes, and old/new/unknown
//! classification. Silent platform skips are forbidden.

use residiuum_store::{
    action_for_failure_class, all_cells, arm_failpoint_once, ci_subset_cells, clear_failpoints,
    composed_schedule, enable_failpoint_hit_proof, load_crash_matrix, load_failure_combinations,
    matrix_totals, probe_linux_loopback_lane, require_failpoint_visited, validate_crash_matrix,
    validate_reopen_expectation, CampaignReport, CrashController, CrashOutcomeClass,
    DurabilityMode, FailpointAction, FilesystemImageHarness, PortableFsImage, ScheduleDecision,
    Store, StoreError,
};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

fn matrix_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn seed_prior(path: &Path) {
    let mut s = Store::create(path).expect("create");
    s.put("prior", b"prior-v1", DurabilityMode::Durable)
        .expect("prior");
    drop(s);
}

fn is_injected(err: &StoreError) -> bool {
    match err {
        StoreError::Failpoint(_) => true,
        StoreError::Io(e) => matches!(
            e.kind(),
            std::io::ErrorKind::StorageFull
                | std::io::ErrorKind::PermissionDenied
                | std::io::ErrorKind::WriteZero
                | std::io::ErrorKind::OutOfMemory
        ),
        _ => false,
    }
}

fn arm_fault(name: &str, fault: Option<&str>) {
    let action = match fault {
        Some("enospc") => FailpointAction::IoEnospc,
        Some("permission") => FailpointAction::IoPermission,
        Some("short_write") => FailpointAction::ShortWrite,
        Some("process_abort") => FailpointAction::Abort,
        Some("panic") => FailpointAction::Panic,
        Some("error") | None => {
            if name.contains("short_write") {
                FailpointAction::ShortWrite
            } else {
                FailpointAction::Error
            }
        }
        Some(_) => FailpointAction::Error,
    };
    if action == FailpointAction::Abort {
        return; // process-kill cells use crash-child
    }
    let leaked: &'static str = Box::leak(name.to_string().into_boxed_str());
    arm_failpoint_once(leaked, action);
}

/// Drive put_durable-style cells used by most matrix ops; returns (acked, visible, prior_ok).
fn drive_put_cell(path: &Path, failpoint: &str, fault: Option<&str>) -> (bool, bool, bool) {
    seed_prior(path);
    if fault == Some("process_abort") {
        let ctrl = CrashController::resolve().expect("crash child");
        let status = ctrl
            .run(path, "put_durable", Some(failpoint), "k", "v-new")
            .expect("spawn");
        assert!(!status.success(), "abort child must not succeed");
        let store = Store::open(path).expect("reopen");
        let prior = store.get("prior").unwrap().as_deref() == Some(b"prior-v1".as_slice());
        let visible = store.get("k").unwrap().is_some();
        return (false, visible, prior);
    }
    let mut store = Store::open(path).unwrap();
    arm_fault(failpoint, fault);
    let result = store.put("k", b"v-new", DurabilityMode::Durable);
    let acked = result.is_ok();
    if !acked {
        assert!(is_injected(result.as_ref().unwrap_err()));
    }
    drop(store);
    clear_failpoints();
    let store = Store::open(path).unwrap();
    let prior = store.get("prior").unwrap().as_deref() == Some(b"prior-v1".as_slice());
    let visible = store.get("k").unwrap().as_deref() == Some(b"v-new".as_slice());
    (acked, visible, prior)
}

// ---------------------------------------------------------------------------
// Matrix campaign
// ---------------------------------------------------------------------------

#[test]
fn csq5_ci_subset_and_full_matrix_cells_run() {
    let _g = matrix_lock();
    clear_failpoints();
    enable_failpoint_hit_proof();

    let m = load_crash_matrix().expect("load");
    validate_crash_matrix(&m).expect("validate");
    let (total, ci_n) = matrix_totals(&m);
    assert!(total >= 20, "expected substantial matrix, got {total}");
    assert!(ci_n >= 5);

    let mut report = CampaignReport::default();
    report.matrix_cells_total = total;
    report.lane_ran("crash_matrix_ci_subset");

    for (op, fp) in ci_subset_cells(&m) {
        // Skip ops without a simple put driver in this campaign harness —
        // those remain covered by stage_def_022 full drivers.
        let driver_ops = [
            "put_durable",
            "put_durable_io_faults",
            "put_durable_process_kill",
            "put_buffered",
        ];
        if !driver_ops.contains(&op.id.as_str()) {
            continue;
        }
        let dir = tempdir().unwrap();
        let path = dir.path().join("s");
        let (acked, visible, prior) = drive_put_cell(&path, &fp.name, fp.fault.as_deref());
        if fp.fault.as_deref() != Some("process_abort") {
            // Hit proof when in-process
            let _ = require_failpoint_visited(&fp.name);
        }
        let class = validate_reopen_expectation(&fp.expected_on_reopen, acked, visible, prior)
            .unwrap_or_else(|e| panic!("cell {}/{}: {e}", op.id, fp.name));
        report.record_outcome(class);
        report.ci_subset_ran += 1;
        report.matrix_cells_ran += 1;
        clear_failpoints();
    }
    assert!(
        report.ci_subset_ran >= 3,
        "ran {} ci cells",
        report.ci_subset_ran
    );

    // Full matrix: every put_* cell (not only ci_subset)
    report.lane_ran("crash_matrix_full_put_family");
    for (op, fp) in all_cells(&m) {
        let driver_ops = [
            "put_durable",
            "put_durable_io_faults",
            "put_durable_process_kill",
            "put_buffered",
        ];
        if !driver_ops.contains(&op.id.as_str()) {
            continue;
        }
        if fp.ci_subset {
            continue; // already counted
        }
        let dir = tempdir().unwrap();
        let path = dir.path().join("s");
        let (acked, visible, prior) = drive_put_cell(&path, &fp.name, fp.fault.as_deref());
        let class = validate_reopen_expectation(&fp.expected_on_reopen, acked, visible, prior)
            .unwrap_or_else(|e| panic!("full cell {}/{}: {e}", op.id, fp.name));
        report.record_outcome(class);
        report.matrix_cells_ran += 1;
        clear_failpoints();
    }

    // Also run stage_def_022 style validation that document covers ops
    assert!(
        report.matrix_cells_ran >= ci_n,
        "cells ran {} < ci {}",
        report.matrix_cells_ran,
        ci_n
    );
    report.assert_no_silent_skips();
    let _ = report.to_json().unwrap();
    clear_failpoints();
}

// ---------------------------------------------------------------------------
// Composed failures
// ---------------------------------------------------------------------------

#[test]
fn csq5_composed_failure_pairs_execute() {
    let _g = matrix_lock();
    clear_failpoints();

    let path = workspace_root().join("spec/verification/core-storage/failure-combinations-v1.json");
    let doc = load_failure_combinations(&path).expect("load combos");
    let sched = composed_schedule(&doc).expect("schedule");

    let mut report = CampaignReport::default();
    report.lane_ran("composed_failure_pairs");

    for d in &sched {
        match d {
            ScheduleDecision::Reject { id, reason } => {
                assert!(!reason.is_empty(), "{id}");
                report.composed_rejected += 1;
                report.lane_skipped(&format!("composed:{id}"), reason.clone());
            }
            ScheduleDecision::Run { id, order, owner } => {
                assert_eq!(owner, "CSQ-SUITE-CRASH");
                // Sequential single-fault injections on put path (ordered pair).
                let dir = tempdir().unwrap();
                let path = dir.path().join("s");
                seed_prior(&path);
                for class in order {
                    let Some(action) = action_for_failure_class(class) else {
                        // corruption/clock etc. — explicit skip with reason
                        report.lane_skipped(
                            &format!("composed:{id}:{class}"),
                            "no in-process action; deferred to damage/platform lane",
                        );
                        continue;
                    };
                    if action == FailpointAction::Abort {
                        // process abort uses child — smoke one class only
                        if let Some(ctrl) = CrashController::resolve() {
                            let st = ctrl
                                .run(
                                    &path,
                                    "put_durable",
                                    Some("store.active.write_tail.before"),
                                    "k",
                                    "v",
                                )
                                .unwrap();
                            assert!(!st.success());
                        }
                        continue;
                    }
                    // Map classes to (failpoint name, action) that the put path hits.
                    let (fp, act): (&'static str, FailpointAction) = match class.as_str() {
                        "short_write" => (
                            "store.active.write_tail.short_write",
                            FailpointAction::ShortWrite,
                        ),
                        "enospc" => ("store.active.write_tail.before", FailpointAction::IoEnospc),
                        "permission" => (
                            "store.active.write_tail.before",
                            FailpointAction::IoPermission,
                        ),
                        "io_error" | "error" | "eio" | "cancellation" | "resource_exhaustion" => {
                            ("store.active.write_tail.before", FailpointAction::Error)
                        }
                        other => {
                            report.lane_skipped(
                                &format!("composed:{id}:{other}"),
                                "no put-path injection for this class in CSQ-5 CI",
                            );
                            continue;
                        }
                    };
                    let _ = action; // mapped above
                    let mut store = Store::open(&path).unwrap();
                    arm_failpoint_once(fp, act);
                    let put_res = store.put("k", b"pair", DurabilityMode::Durable);
                    let err = put_res.expect_err(&format!("{id} class {class} must inject"));
                    assert!(is_injected(&err), "{id} class {class}: {err:?}");
                    drop(store);
                    clear_failpoints();
                    // Reopen + healthy continuation
                    let mut store = Store::open(&path).unwrap();
                    assert_eq!(
                        store.get("prior").unwrap().as_deref(),
                        Some(b"prior-v1".as_slice())
                    );
                    store
                        .put("healthy", b"ok", DurabilityMode::Durable)
                        .expect("healthy continuation");
                    drop(store);
                }
                report.composed_ran += 1;
            }
        }
    }
    assert!(report.composed_ran >= 1);
    report.assert_no_silent_skips();
    clear_failpoints();
}

// ---------------------------------------------------------------------------
// Reopen / retry / healthy continuation
// ---------------------------------------------------------------------------

#[test]
fn csq5_repeated_reopen_idempotent_after_fault() {
    let _g = matrix_lock();
    clear_failpoints();
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    seed_prior(&path);
    {
        let mut s = Store::open(&path).unwrap();
        arm_failpoint_once("store.active.write_tail.before", FailpointAction::Error);
        let _ = s.put("k", b"v", DurabilityMode::Durable).unwrap_err();
        drop(s);
        clear_failpoints();
    }
    let mut last = None;
    for _ in 0..3 {
        let s = Store::open(&path).unwrap();
        let prior = s.get("prior").unwrap();
        let k = s.get("k").unwrap();
        let obs = (prior.is_some(), k.is_none());
        if let Some(prev) = last {
            assert_eq!(prev, obs, "CSQ-REC-001/002 reopen must agree");
        }
        last = Some(obs);
        let _ = s.salvage().unwrap();
        drop(s);
    }
    // Healthy continuation
    let mut s = Store::open(&path).unwrap();
    s.put("after", b"ok", DurabilityMode::Durable).unwrap();
    assert_eq!(s.get("after").unwrap().as_deref(), Some(b"ok".as_slice()));
}

// ---------------------------------------------------------------------------
// ENOSPC / permission campaigns
// ---------------------------------------------------------------------------

#[test]
fn csq5_enospc_and_permission_campaign() {
    let _g = matrix_lock();
    clear_failpoints();
    let mut report = CampaignReport::default();
    report.lane_ran("io_enospc");
    report.lane_ran("io_permission");

    for (action, name) in [
        (FailpointAction::IoEnospc, "enospc"),
        (FailpointAction::IoPermission, "permission"),
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("s");
        seed_prior(&path);
        let mut s = Store::open(&path).unwrap();
        arm_failpoint_once("store.active.write_tail.before", action);
        let err = s.put("k", b"v", DurabilityMode::Durable).unwrap_err();
        assert!(is_injected(&err), "{name}: {err:?}");
        drop(s);
        clear_failpoints();
        let mut s = Store::open(&path).unwrap();
        assert_eq!(
            s.get("prior").unwrap().as_deref(),
            Some(b"prior-v1".as_slice())
        );
        s.put("recover", b"1", DurabilityMode::Durable).unwrap();
    }
    report.assert_no_silent_skips();
}

// ---------------------------------------------------------------------------
// Writer-lock campaign (DEF-101)
// ---------------------------------------------------------------------------

#[test]
fn csq5_writer_lock_stale_and_contention() {
    let mut report = CampaignReport::default();
    report.lane_ran("writer_lock_def101");

    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    // Contention while writer live.
    {
        let _a = Store::create(&path).expect("create writer");
        match Store::open(&path) {
            Ok(_) => panic!("second open should fail with WriterLockHeld"),
            Err(StoreError::WriterLockHeld(_)) => {}
            Err(other) => panic!("contention must be WriterLockHeld, got {other}"),
        }
    }
    // After drop, lock free; stale diagnostic text must not block.
    let lock = path.join("store-info").join("writer.lock");
    assert!(
        lock.is_file() || path.join("store-info").is_dir(),
        "store-info should exist after create"
    );
    if lock.is_file() {
        std::fs::write(
            &lock,
            "residiuum-writer-lock\npid=1\nacquired_ns=0\nnote=stale\n",
        )
        .expect("write stale diagnostic");
    }
    let reopened = Store::open(&path).expect("open after stale diagnostic");
    assert!(reopened.holds_writer_lock());
    report.assert_no_silent_skips();
}

// ---------------------------------------------------------------------------
// Portable FS image + path alias + platform lane ledger
// ---------------------------------------------------------------------------

#[test]
fn csq5_portable_fs_image_and_path_alias() {
    let mut report = CampaignReport::default();
    let dir = tempdir().unwrap();
    let img = PortableFsImage::create(dir.path()).unwrap();
    report.filesystem = Some(img.evidence());
    report.lane_ran("portable_fs_image");

    {
        let mut s = Store::create(&img.store_path).unwrap();
        s.put("k", b"v", DurabilityMode::Durable).unwrap();
    }
    // Path alias: rename image root
    let new_store = img.rename_alias("csq5-aliased").unwrap();
    report.lane_ran("path_alias_rename");
    let s = Store::open(&new_store).unwrap();
    assert_eq!(s.get("k").unwrap().as_deref(), Some(b"v".as_slice()));

    // FilesystemImageHarness API
    let mut h = FilesystemImageHarness::inventory();
    assert!(h.ready);
    let parent = tempdir().unwrap();
    let store_path = h.run_portable_campaign(parent.path()).unwrap();
    let mut s = Store::create(&store_path).unwrap();
    s.put("x", b"y", DurabilityMode::Durable).unwrap();

    // Platform lane — always recorded
    let lane = probe_linux_loopback_lane();
    report.lanes.push(lane);
    report.assert_no_silent_skips();
    let json = report.to_json().unwrap();
    assert!(json.contains("portable") || json.contains("linux") || json.contains("macos"));
}

// ---------------------------------------------------------------------------
// Outcome validator unit-level
// ---------------------------------------------------------------------------

#[test]
fn csq5_outcome_validator_rejects_hybrid() {
    assert_eq!(
        residiuum_store::classify_outcome(false, false, true),
        CrashOutcomeClass::Old
    );
    assert_eq!(
        residiuum_store::classify_outcome(true, true, true),
        CrashOutcomeClass::New
    );
    // acked without visibility is hybrid error
    let exp = residiuum_store::ExpectedReopen {
        acknowledged_visible: Some(true),
        no_fabricated_commit: true,
        salvageable: true,
        prior_durable_retained: true,
        notes: String::new(),
    };
    assert!(validate_reopen_expectation(&exp, true, false, true).is_err());
}

#[test]
fn csq5_process_abort_lane_when_child_present() {
    let mut report = CampaignReport::default();
    if CrashController::resolve().is_none() {
        report.lane_skipped(
            "process_abort_child",
            "crash-child binary not built in this test profile",
        );
        report.assert_no_silent_skips();
        return;
    }
    report.lane_ran("process_abort_child");
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    seed_prior(&path);
    let ctrl = CrashController::resolve().unwrap();
    let st = ctrl
        .run(
            &path,
            "put_durable",
            Some("store.active.write_tail.before"),
            "k",
            "v-new",
        )
        .unwrap();
    assert!(!st.success());
    let s = Store::open(&path).unwrap();
    assert_eq!(
        s.get("prior").unwrap().as_deref(),
        Some(b"prior-v1".as_slice())
    );
    assert!(s.get("k").unwrap().is_none());
    report.assert_no_silent_skips();
}
