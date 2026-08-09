//! DEF-022 crash-consistency matrix (hardened beyond skeleton).
//!
//! - Validates the embedded machine-readable matrix.
//! - Runs the **CI subset** of failpoint cells on every test run.
//! - When `RESIDIUUM_CRASH_MATRIX_FULL=1`, runs every matrix cell (nightly).
//! - I/O fault injection: ENOSPC, permission-denied, short-write.
//! - Multi-process `process::abort` via `residiuum-store-crash-child`.
//!
//! Process-local cells: arm failpoint → drive op → drop → reopen → assert.
//! Process-kill cells: parent seeds store → child aborts mid-op → parent reopens.

use residiuum_store::{
    all_cells, arm_failpoint_once, ci_subset_cells, clear_failpoints, load_crash_matrix,
    validate_crash_matrix, write_atomic, DurabilityMode, FailpointAction, Store, StoreError,
    CRASH_MATRIX_JSON,
};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

/// Failpoints are process-global; serialize matrix drivers across test threads.
fn matrix_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

#[test]
fn crash_matrix_document_is_valid() {
    assert!(
        CRASH_MATRIX_JSON.contains("\"version\": 1"),
        "embedded matrix must be version 1"
    );
    let m = load_crash_matrix().expect("parse crash_matrix.v1.json");
    validate_crash_matrix(&m).expect("structural validation");
    assert!(
        !ci_subset_cells(&m).is_empty(),
        "CI subset must list at least one failpoint"
    );
    for id in [
        "store_create",
        "put_durable",
        "put_durable_io_faults",
        "put_durable_process_kill",
        "delete_durable",
        "seal_active",
        "chunked_put_durable",
        "write_dedup_persist",
    ] {
        assert!(
            m.operations.iter().any(|o| o.id == id),
            "matrix missing operation {id}"
        );
    }
    // Hardened cells present.
    assert!(CRASH_MATRIX_JSON.contains("short_write"));
    assert!(CRASH_MATRIX_JSON.contains("process_abort"));
    assert!(CRASH_MATRIX_JSON.contains("enospc"));
}

#[test]
fn ci_subset_failpoints_respect_reopen_invariants() {
    let _guard = matrix_lock();
    let m = load_crash_matrix().expect("load");
    validate_crash_matrix(&m).expect("validate");
    for (op, fp) in ci_subset_cells(&m) {
        run_cell(
            op.id.as_str(),
            fp.name.as_str(),
            fp.fault.as_deref(),
            &fp.expected_on_reopen,
        );
    }
}

#[test]
fn full_matrix_when_env_set() {
    if std::env::var_os("RESIDIUUM_CRASH_MATRIX_FULL").is_none() {
        eprintln!("skip full matrix (set RESIDIUUM_CRASH_MATRIX_FULL=1 for nightly)");
        return;
    }
    let _guard = matrix_lock();
    let m = load_crash_matrix().expect("load");
    validate_crash_matrix(&m).expect("validate");
    for (op, fp) in all_cells(&m) {
        run_cell(
            op.id.as_str(),
            fp.name.as_str(),
            fp.fault.as_deref(),
            &fp.expected_on_reopen,
        );
    }
}

/// Explicit I/O fault + permission-loss + short-write smoke (always on CI).
#[test]
fn io_fault_injection_suite() {
    let _guard = matrix_lock();
    clear_failpoints();

    // ENOSPC before any new bytes.
    {
        let dir = tempdir().unwrap();
        let path = dir.path().join("s");
        seed_prior(&path);
        let mut store = Store::open(&path).unwrap();
        arm_failpoint_once("store.active.write_tail.before", FailpointAction::IoEnospc);
        let err = store
            .put("k", b"v-new", DurabilityMode::Durable)
            .unwrap_err();
        match err {
            StoreError::Io(e) => assert_eq!(e.kind(), ErrorKind::StorageFull),
            other => panic!("expected StorageFull Io, got {other}"),
        }
        drop(store);
        clear_failpoints();
        let store = Store::open(&path).unwrap();
        assert_eq!(
            store.get("prior").unwrap().as_deref(),
            Some(b"prior-v1".as_slice())
        );
        assert!(store.get("k").unwrap().is_none());
    }

    // Permission-denied injection at boundary.
    {
        let dir = tempdir().unwrap();
        let path = dir.path().join("s");
        seed_prior(&path);
        let mut store = Store::open(&path).unwrap();
        arm_failpoint_once(
            "store.active.write_tail.before",
            FailpointAction::IoPermission,
        );
        let err = store
            .put("k", b"v-new", DurabilityMode::Durable)
            .unwrap_err();
        match err {
            StoreError::Io(e) => assert_eq!(e.kind(), ErrorKind::PermissionDenied),
            other => panic!("expected PermissionDenied Io, got {other}"),
        }
        drop(store);
        clear_failpoints();
        let store = Store::open(&path).unwrap();
        assert_eq!(
            store.get("prior").unwrap().as_deref(),
            Some(b"prior-v1".as_slice())
        );
        assert!(store.get("k").unwrap().is_none());
    }

    // Short-write mid-append: torn frame must not become live.
    {
        let dir = tempdir().unwrap();
        let path = dir.path().join("s");
        seed_prior(&path);
        let mut store = Store::open(&path).unwrap();
        arm_failpoint_once(
            "store.active.write_tail.short_write",
            FailpointAction::ShortWrite,
        );
        let err = store
            .put(
                "k",
                b"v-short-write-payload-xxxxxxxx",
                DurabilityMode::Durable,
            )
            .unwrap_err();
        match err {
            StoreError::Io(e) => assert_eq!(e.kind(), ErrorKind::WriteZero),
            other => panic!("expected WriteZero Io, got {other}"),
        }
        drop(store);
        clear_failpoints();
        let store = Store::open(&path).unwrap();
        assert_eq!(
            store.get("prior").unwrap().as_deref(),
            Some(b"prior-v1".as_slice())
        );
        assert!(
            store.get("k").unwrap().is_none(),
            "short-written put must not appear live"
        );
        let report = store.salvage().unwrap();
        let _ = (report.verified_frames, report.holes);
    }

    // Atomic control-doc short write: published path unchanged.
    {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ctrl.json");
        fs::write(&path, b"{\"ok\":true}").unwrap();
        arm_failpoint_once("atomic.tmp.short_write", FailpointAction::ShortWrite);
        let err = write_atomic(&path, br#"{"ok":false,"bigger":true}"#).unwrap_err();
        match err {
            StoreError::Io(e) => assert_eq!(e.kind(), ErrorKind::WriteZero),
            other => panic!("expected WriteZero, got {other}"),
        }
        clear_failpoints();
        assert_eq!(fs::read(&path).unwrap(), b"{\"ok\":true}");
    }

    // Real OS permission loss on active segment file (best-effort POSIX).
    {
        let dir = tempdir().unwrap();
        let path = dir.path().join("s");
        seed_prior(&path);
        let active_files: Vec<PathBuf> = {
            let active = path.join("active");
            fs::read_dir(&active)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.is_file())
                .collect()
        };
        if active_files.is_empty() {
            eprintln!("skip OS permission test: no active file");
        } else {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let target = &active_files[0];
                // Already-open FDs often ignore later chmod; model permission
                // loss by revoking mode, then opening a *new* writer handle.
                let original = fs::metadata(target).unwrap().permissions();
                let mut perms = original.clone();
                perms.set_mode(0o000);
                fs::set_permissions(target, perms).unwrap();
                let open_or_put = (|| -> Result<(), StoreError> {
                    let mut store = Store::open(&path)?;
                    store.put("k", b"v-new", DurabilityMode::Durable)?;
                    Ok(())
                })();
                // Restore so reopen/cleanup can touch the file.
                let mut restore = original.clone();
                restore.set_mode(0o644);
                let _ = fs::set_permissions(target, restore);
                assert!(
                    open_or_put.is_err(),
                    "open/put with mode-000 active should fail, got {open_or_put:?}"
                );
                let store = Store::open(&path).unwrap();
                assert_eq!(
                    store.get("prior").unwrap().as_deref(),
                    Some(b"prior-v1".as_slice())
                );
            }
            #[cfg(not(unix))]
            {
                eprintln!("skip OS permission chmod on non-unix");
            }
        }
    }

    clear_failpoints();
}

/// Multi-process abort harness (always on CI for the before-write cell).
#[test]
fn multiprocess_abort_before_write() {
    let _guard = matrix_lock();
    clear_failpoints();
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    seed_prior(&path);

    let status = run_crash_child(
        &path,
        "put_durable",
        Some("store.active.write_tail.before"),
        "k",
        "v-new",
    );
    // Abort typically yields a signal exit (not 0). On some platforms status may be None.
    assert!(
        !status.success(),
        "child should not exit cleanly when aborting before write; status={status:?}"
    );

    let store = Store::open(&path).expect("reopen after child abort");
    assert_eq!(
        store.get("prior").unwrap().as_deref(),
        Some(b"prior-v1".as_slice()),
        "prior durable must survive process kill before write"
    );
    assert!(
        store.get("k").unwrap().is_none(),
        "unacked put must not appear after kill before write"
    );
    let _ = store.salvage().unwrap();
    clear_failpoints();
}

#[test]
fn multiprocess_abort_after_sync_full_or_ci() {
    // Always run: post-sync abort leaves durable bytes without a receipt.
    let _guard = matrix_lock();
    clear_failpoints();
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    seed_prior(&path);

    let status = run_crash_child(
        &path,
        "put_durable",
        Some("store.active.write_tail.after_sync"),
        "k",
        "v-new",
    );
    assert!(
        !status.success(),
        "child should abort after sync; status={status:?}"
    );

    let store = Store::open(&path).expect("reopen after child abort post-sync");
    assert_eq!(
        store.get("prior").unwrap().as_deref(),
        Some(b"prior-v1".as_slice())
    );
    // Frame was fsynced before abort; recovery should see it as durable media
    // even though the child never returned a receipt to a caller.
    assert_eq!(
        store.get("k").unwrap().as_deref(),
        Some(b"v-new".as_slice()),
        "post-sync abort should leave the put visible on reopen"
    );
    clear_failpoints();
}

fn crash_child_bin() -> PathBuf {
    // Prefer Cargo's integration-test env (hyphens → underscores in the name).
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_residiuum_store_crash_child") {
        return PathBuf::from(p);
    }
    // Fallback: same profile dir as this test binary.
    let mut exe = std::env::current_exe().expect("current_exe");
    exe.pop(); // deps/
    if exe.file_name().and_then(|s| s.to_str()) == Some("deps") {
        exe.pop();
    }
    exe.push("residiuum-store-crash-child");
    if exe.is_file() {
        return exe;
    }
    // Last resort: target/{debug,release} next to the workspace.
    let mut alt = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    alt.pop(); // crates
    alt.pop(); // workspace
    alt.push("target");
    alt.push(if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    });
    alt.push("residiuum-store-crash-child");
    alt
}

fn run_crash_child(
    store: &Path,
    op: &str,
    failpoint: Option<&str>,
    key: &str,
    val: &str,
) -> std::process::ExitStatus {
    let bin = crash_child_bin();
    assert!(
        bin.is_file(),
        "crash child binary missing at {} (build residiuum-store-crash-child first)",
        bin.display()
    );
    let mut cmd = Command::new(&bin);
    cmd.env("RESIDIUUM_CRASH_STORE", store)
        .env("RESIDIUUM_CRASH_OP", op)
        .env("RESIDIUUM_CRASH_KEY", key)
        .env("RESIDIUUM_CRASH_VAL", val);
    if let Some(fp) = failpoint {
        cmd.env("RESIDIUUM_CRASH_FP", fp);
    }
    cmd.status()
        .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()))
}

/// Drive one matrix cell for a known operation id + failpoint name.
fn run_cell(
    op_id: &str,
    failpoint: &str,
    fault: Option<&str>,
    expected: &residiuum_store::ExpectedReopen,
) {
    clear_failpoints();
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("s");

    match op_id {
        "store_create" => run_store_create(&path, failpoint, expected),
        "put_durable" => run_put_durable(&path, failpoint, fault, expected),
        "put_durable_io_faults" => run_put_durable(&path, failpoint, fault, expected),
        "put_durable_process_kill" => run_put_process_kill(&path, failpoint, expected),
        "put_buffered" => run_put_buffered(&path, failpoint, expected),
        "delete_durable" => run_delete_durable(&path, failpoint, expected),
        "chunked_put_durable" => run_chunked_put(&path, failpoint, expected),
        "seal_active" => run_seal(&path, failpoint, expected),
        "write_dedup_persist" => run_dedup(&path, failpoint, expected),
        "catalog_refresh" => run_catalog(&path, failpoint, expected),
        "checkpoint" => run_checkpoint(&path, failpoint, expected),
        "tier_move" => run_tier_move(&path, failpoint, expected),
        "compact_live" => run_compact(&path, failpoint, expected),
        other => panic!("no driver for operation {other}"),
    }

    clear_failpoints();
}

fn leak_name(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

fn arm_for_fault(name: &str, fault: Option<&str>) {
    let action = match fault {
        Some("enospc") => FailpointAction::IoEnospc,
        Some("permission") => FailpointAction::IoPermission,
        Some("short_write") => FailpointAction::ShortWrite,
        Some("process_abort") => FailpointAction::Abort,
        Some("panic") => FailpointAction::Panic,
        Some("error") | None => {
            if name.ends_with(".short_write") || name.contains("short_write") {
                FailpointAction::ShortWrite
            } else {
                FailpointAction::Error
            }
        }
        Some(other) => panic!("unknown fault class {other}"),
    };
    // Abort must only be used from the crash-child process.
    if action == FailpointAction::Abort {
        return;
    }
    arm_failpoint_once(leak_name(name), action);
}

fn arm_error(name: &str) {
    arm_failpoint_once(leak_name(name), FailpointAction::Error);
}

#[allow(dead_code)] // retained for process-local panic cells / nightly drivers
fn arm_panic(name: &str) {
    arm_failpoint_once(leak_name(name), FailpointAction::Panic);
}

fn assert_prior_ok(store: &Store, expected: &residiuum_store::ExpectedReopen) {
    if expected.prior_durable_retained {
        assert_eq!(
            store.get("prior").unwrap().as_deref(),
            Some(b"prior-v1".as_slice()),
            "prior durable key must survive crash at failpoint"
        );
    }
    if expected.salvageable {
        let report = store.salvage().expect("salvage must run");
        let _ = (report.files_scanned, report.verified_frames, report.holes);
    }
}

fn seed_prior(path: &Path) {
    let mut s = Store::create(path).expect("create");
    s.put("prior", b"prior-v1", DurabilityMode::Durable)
        .expect("prior put");
    drop(s);
}

fn is_injected_failure(err: &StoreError) -> bool {
    match err {
        StoreError::Failpoint(_) => true,
        StoreError::Io(e) => matches!(
            e.kind(),
            ErrorKind::StorageFull | ErrorKind::PermissionDenied | ErrorKind::WriteZero
        ),
        _ => false,
    }
}

fn run_store_create(path: &Path, failpoint: &str, expected: &residiuum_store::ExpectedReopen) {
    arm_error(failpoint);
    let result = Store::create(path);
    assert!(
        matches!(result, Err(StoreError::Failpoint(_))),
        "create should hit failpoint {failpoint}, got err={}",
        result.err().map(|e| e.to_string()).unwrap_or_default()
    );
    clear_failpoints();
    if let Ok(s) = Store::open(path) {
        assert!(
            s.get("ghost").unwrap().is_none(),
            "must not fabricate subjects"
        );
        if expected.salvageable {
            let _ = s.salvage();
        }
    }
}

fn run_put_durable(
    path: &Path,
    failpoint: &str,
    fault: Option<&str>,
    expected: &residiuum_store::ExpectedReopen,
) {
    seed_prior(path);
    let mut store = Store::open(path).expect("open");
    arm_for_fault(failpoint, fault);
    let result = store.put("k", b"v-new", DurabilityMode::Durable);
    let acknowledged = result.is_ok();
    if !acknowledged {
        let err = result.as_ref().unwrap_err();
        assert!(
            is_injected_failure(err),
            "expected injected failure, got {err:?}"
        );
    }
    drop(store);
    clear_failpoints();

    let store = Store::open(path).expect("reopen");
    assert_prior_ok(&store, expected);
    match expected.acknowledged_visible {
        Some(true) => {
            assert_eq!(
                store.get("k").unwrap().as_deref(),
                Some(b"v-new".as_slice()),
                "expected put visible after failpoint {failpoint}"
            );
        }
        Some(false) => {
            if !acknowledged {
                assert!(
                    store.get("k").unwrap().is_none(),
                    "unacked put must not appear after reopen at {failpoint}"
                );
            }
        }
        None => {
            let _ = store.get("k");
        }
    }
}

fn run_put_process_kill(path: &Path, failpoint: &str, expected: &residiuum_store::ExpectedReopen) {
    seed_prior(path);
    let status = run_crash_child(path, "put_durable", Some(failpoint), "k", "v-new");
    assert!(
        !status.success(),
        "process_abort cell must kill child; status={status:?}"
    );
    let store = Store::open(path).expect("reopen after kill");
    assert_prior_ok(&store, expected);
    match expected.acknowledged_visible {
        Some(true) => {
            assert_eq!(
                store.get("k").unwrap().as_deref(),
                Some(b"v-new".as_slice()),
                "post-boundary kill should leave durable put at {failpoint}"
            );
        }
        Some(false) => {
            assert!(
                store.get("k").unwrap().is_none(),
                "pre-write kill must not fabricate put at {failpoint}"
            );
        }
        None => {}
    }
}

fn run_put_buffered(path: &Path, failpoint: &str, expected: &residiuum_store::ExpectedReopen) {
    seed_prior(path);
    let mut store = Store::open(path).expect("open");
    arm_error(failpoint);
    let result = store.put("k", b"buf", DurabilityMode::Buffered);
    let acknowledged = result.is_ok();
    drop(store);
    clear_failpoints();
    let store = Store::open(path).expect("reopen");
    assert_prior_ok(&store, expected);
    if expected.acknowledged_visible == Some(false)
        && !acknowledged
        && failpoint.ends_with(".before")
    {
        assert!(store.get("k").unwrap().is_none());
    }
}

fn run_delete_durable(path: &Path, failpoint: &str, expected: &residiuum_store::ExpectedReopen) {
    seed_prior(path);
    {
        let mut s = Store::open(path).unwrap();
        s.put("victim", b"alive", DurabilityMode::Durable).unwrap();
    }
    let mut store = Store::open(path).unwrap();
    arm_error(failpoint);
    let result = store.delete("victim", DurabilityMode::Durable);
    let acknowledged = result.is_ok();
    drop(store);
    clear_failpoints();
    let store = Store::open(path).unwrap();
    assert_prior_ok(&store, expected);
    match expected.acknowledged_visible {
        Some(false) if !acknowledged => {
            assert_eq!(
                store.get("victim").unwrap().as_deref(),
                Some(b"alive".as_slice()),
                "unacked delete must leave prior live value"
            );
        }
        Some(true) => {
            assert!(
                store.get("victim").unwrap().is_none(),
                "durable delete tombstone should apply after {failpoint}"
            );
        }
        _ => {}
    }
}

fn run_chunked_put(path: &Path, failpoint: &str, expected: &residiuum_store::ExpectedReopen) {
    seed_prior(path);
    let mut store = Store::open(path).unwrap();
    store.set_chunk_threshold(20);
    store.set_chunk_size(8);
    let payload: Vec<u8> = (0u8..80).collect();
    arm_error(failpoint);
    let result = store.put("big", &payload, DurabilityMode::Durable);
    let acknowledged = result.is_ok();
    drop(store);
    clear_failpoints();
    let store = Store::open(path).unwrap();
    assert_prior_ok(&store, expected);
    if expected.acknowledged_visible == Some(false)
        && !acknowledged
        && failpoint.ends_with(".before")
    {
        assert!(store.get("big").unwrap().is_none() || store.get("big").is_err());
    }
    if expected.acknowledged_visible == Some(true) {
        assert_eq!(
            store.get("big").unwrap().as_deref(),
            Some(payload.as_slice())
        );
    }
}

fn run_seal(path: &Path, failpoint: &str, expected: &residiuum_store::ExpectedReopen) {
    seed_prior(path);
    {
        let mut store = Store::open(path).unwrap();
        store
            .put("sealed-key", b"sealed-val", DurabilityMode::Durable)
            .unwrap();
        // Prefer Error over Panic: CompactShadow finalize runs on a worker thread;
        // worker panics are not caught by catch_unwind on the writer.
        arm_error(failpoint);
        let result = store.seal_active();
        assert!(
            result.is_err(),
            "seal must fail at failpoint {failpoint}, got {result:?}"
        );
        drop(store);
    }
    clear_failpoints();
    let store = Store::open(path).expect("reopen after seal crash");
    assert_prior_ok(&store, expected);
    assert_eq!(
        store.get("sealed-key").unwrap().as_deref(),
        Some(b"sealed-val".as_slice()),
        "data written before seal must survive failpoint {failpoint}"
    );
}

fn run_dedup(path: &Path, failpoint: &str, expected: &residiuum_store::ExpectedReopen) {
    seed_prior(path);
    let mut store = Store::open(path).unwrap();
    let receipt = store
        .put("dedup-k", b"dedup-v", DurabilityMode::Durable)
        .unwrap();
    let op_id = [7u8; 16];
    let content = residiuum_store::content_identity("put", "", "dedup-k", b"dedup-v");
    arm_error(failpoint);
    let result = store.record_write_dedup(op_id, content, &receipt);
    drop(store);
    clear_failpoints();
    let store = Store::open(path).unwrap();
    assert_prior_ok(&store, expected);
    assert_eq!(
        store.get("dedup-k").unwrap().as_deref(),
        Some(b"dedup-v".as_slice())
    );
    let _ = result;
}

fn run_catalog(path: &Path, failpoint: &str, expected: &residiuum_store::ExpectedReopen) {
    seed_prior(path);
    let mut store = Store::open(path).unwrap();
    store
        .put("c/users/1", b"{}", DurabilityMode::Durable)
        .unwrap();
    arm_error(failpoint);
    let _ = store.rebuild_catalogs();
    drop(store);
    clear_failpoints();
    let store = Store::open(path).unwrap();
    assert_prior_ok(&store, expected);
    assert!(store.get("c/users/1").unwrap().is_some());
}

fn run_checkpoint(path: &Path, failpoint: &str, expected: &residiuum_store::ExpectedReopen) {
    seed_prior(path);
    let store = Store::open(path).unwrap();
    arm_error(failpoint);
    let _ = store.checkpoint("test-coverage");
    drop(store);
    clear_failpoints();
    let store = Store::open(path).unwrap();
    assert_prior_ok(&store, expected);
}

fn run_tier_move(path: &Path, failpoint: &str, expected: &residiuum_store::ExpectedReopen) {
    seed_prior(path);
    let mut store = Store::open(path).unwrap();
    store
        .put("tier-k", b"tier-v", DurabilityMode::Durable)
        .unwrap();
    store.seal_active().unwrap();
    let ids: Vec<_> = store
        .tier_placement()
        .entries()
        .map(|p| p.segment_id)
        .collect();
    if ids.is_empty() {
        clear_failpoints();
        return;
    }
    arm_error(failpoint);
    let _ = store.transfer_segment_to_tier(
        ids[0],
        residiuum_store::TierClass::Warm,
        residiuum_store::TierMoveMode::Copy,
    );
    drop(store);
    clear_failpoints();
    let store = Store::open(path).unwrap();
    assert_prior_ok(&store, expected);
    assert_eq!(
        store.get("tier-k").unwrap().as_deref(),
        Some(b"tier-v".as_slice())
    );
}

fn run_compact(path: &Path, failpoint: &str, expected: &residiuum_store::ExpectedReopen) {
    seed_prior(path);
    let mut store = Store::open(path).unwrap();
    store.put("c1", b"one", DurabilityMode::Durable).unwrap();
    arm_error(failpoint);
    let _ = store.compact_live();
    drop(store);
    clear_failpoints();
    let store = Store::open(path).unwrap();
    assert_prior_ok(&store, expected);
    assert_eq!(store.get("c1").unwrap().as_deref(), Some(b"one".as_slice()));
}
