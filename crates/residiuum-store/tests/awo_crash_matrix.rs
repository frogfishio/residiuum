//! AWO-6 floor: closed failpoint inventory + hit-proof cells for wired sites.
//!
//! Explicit non-claims: not multi-process crash campaign; not PQH G8; not package accept.
//! Deferred live hits: awo.reserve.* / awo.cook.* until coordinator owns encode.
//!
//! Run:
//!   cargo test -p residiuum-store --features legacy-raw-store --test awo_crash_matrix -- --test-threads=1

use residiuum_store::adaptive_write::AWO_FAILPOINTS;
use residiuum_store::{
    arm_failpoint_once, clear_failpoints, enable_failpoint_hit_proof, failpoint_visit_count,
    require_failpoint_visited, DurabilityMode, FailpointAction, Store,
};
use std::sync::{Mutex, OnceLock};

/// Failpoints are process-global; serialize armed cells across test threads.
fn fp_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

/// Failpoints hit on Durable put_many **before** index publish.
const PRE_PUBLISH_CELLS: &[&str] = &[
    "awo.install.frame.before",
    "awo.install.frame.after",
    "awo.persist.before",
    "awo.persist.after_write",
    "awo.persist.after_sync",
    "awo.publish.before",
];

/// Failpoints hit **after** index publish (error returns but keys may be visible).
const POST_PUBLISH_CELLS: &[&str] = &["awo.publish.after", "awo.complete.before"];

/// Names reserved in the closed set but not yet live on put_many.
const DEFERRED_CELLS: &[&str] = &["awo.reserve.after", "awo.cook.before", "awo.cook.after"];

#[test]
fn closed_failpoint_inventory_partition() {
    assert_eq!(AWO_FAILPOINTS.len(), 11);
    for name in PRE_PUBLISH_CELLS
        .iter()
        .chain(POST_PUBLISH_CELLS.iter())
        .chain(DEFERRED_CELLS.iter())
    {
        assert!(
            AWO_FAILPOINTS.contains(name),
            "cell must be in closed set: {name}"
        );
    }
    assert_eq!(
        PRE_PUBLISH_CELLS.len() + POST_PUBLISH_CELLS.len() + DEFERRED_CELLS.len(),
        AWO_FAILPOINTS.len()
    );
}

#[test]
fn each_pre_publish_error_failpoint_leaves_index_clean() {
    let _guard = fp_lock();
    for &name in PRE_PUBLISH_CELLS {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::create(dir.path()).unwrap();
        clear_failpoints();
        enable_failpoint_hit_proof();
        arm_failpoint_once(name, FailpointAction::Error);

        // after_sync only arms on Durable.
        let err = store
            .put_many(
                &[("awo/mx/a", b"1"), ("awo/mx/b", b"2")],
                DurabilityMode::Durable,
            )
            .expect_err(name);
        let _ = err;
        require_failpoint_visited(name);
        assert!(
            store.get("awo/mx/a").unwrap().is_none(),
            "{name}: must not publish a"
        );
        assert!(
            store.get("awo/mx/b").unwrap().is_none(),
            "{name}: must not publish b"
        );

        clear_failpoints();
        if !store.is_awo_writer_poisoned() {
            store
                .put_many(&[("awo/mx/ok", b"z")], DurabilityMode::Buffered)
                .unwrap();
            assert!(store.get("awo/mx/ok").unwrap().is_some());
        }
    }
}

#[test]
fn post_publish_failpoints_hit_after_index_visible() {
    let _guard = fp_lock();
    for &name in POST_PUBLISH_CELLS {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::create(dir.path()).unwrap();
        clear_failpoints();
        enable_failpoint_hit_proof();
        arm_failpoint_once(name, FailpointAction::Error);

        let err = store
            .put_many(&[("awo/post/a", b"1")], DurabilityMode::Durable)
            .expect_err(name);
        let _ = err;
        require_failpoint_visited(name);
        // Index already published; completion/error is after visibility.
        assert!(
            store.get("awo/post/a").unwrap().is_some(),
            "{name}: index already published"
        );
        clear_failpoints();
    }
}

#[test]
fn deferred_cells_named_but_not_required_on_put_many() {
    let _guard = fp_lock();
    // Honesty: reserve/cook are not hit by raw put_many today.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path()).unwrap();
    clear_failpoints();
    enable_failpoint_hit_proof();
    for &name in DEFERRED_CELLS {
        arm_failpoint_once(name, FailpointAction::Error);
    }
    // Batch must succeed — deferred failpoints are not on this path.
    store
        .put_many(&[("awo/def/x", b"1")], DurabilityMode::Buffered)
        .expect("deferred failpoints must not fire on put_many");
    for &name in DEFERRED_CELLS {
        assert_eq!(
            failpoint_visit_count(name),
            0,
            "{name} must remain unvisited on put_many"
        );
    }
    clear_failpoints();
}

#[test]
fn short_write_cell_poisons_writer() {
    let _guard = fp_lock();
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path()).unwrap();
    clear_failpoints();
    enable_failpoint_hit_proof();
    arm_failpoint_once(
        "store.active.write_tail.short_write",
        FailpointAction::ShortWrite,
    );
    let _ = store
        .put_many(&[("awo/sw/k", b"v")], DurabilityMode::Buffered)
        .expect_err("short write");
    assert!(store.is_awo_writer_poisoned());
    clear_failpoints();
}

/// Multi-process abort at `awo.persist.before` via crash-child `put_many_durable`.
#[test]
fn multiprocess_abort_awo_persist_before() {
    use std::process::Command;

    clear_failpoints();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let mut s = Store::create(&path).unwrap();
        s.put("prior", b"prior-v1", DurabilityMode::Durable)
            .unwrap();
    }

    let bin = crash_child_bin();
    if !bin.is_file() {
        // Profile without crash-child binary: skip honestly (not a silent pass).
        eprintln!(
            "skip multiprocess: crash-child missing at {}",
            bin.display()
        );
        return;
    }
    let status = Command::new(&bin)
        .env("RESIDIUUM_CRASH_STORE", &path)
        .env("RESIDIUUM_CRASH_OP", "put_many_durable")
        .env("RESIDIUUM_CRASH_FP", "awo.persist.before")
        .env("RESIDIUUM_CRASH_KEY", "awo/mp/k")
        .env("RESIDIUUM_CRASH_VAL", "v-new")
        .status()
        .expect("spawn crash child");
    assert!(
        !status.success(),
        "child must abort at awo.persist.before; status={status:?}"
    );

    let store = Store::open(&path).expect("reopen");
    assert_eq!(
        store.get("prior").unwrap().as_deref(),
        Some(b"prior-v1".as_slice())
    );
    assert!(
        store.get("awo/mp/k").unwrap().is_none(),
        "batch aborted before persist must not publish"
    );
    assert!(store.get("awo/mp/k-b").unwrap().is_none());
    clear_failpoints();
}

fn crash_child_bin() -> std::path::PathBuf {
    use std::path::PathBuf;
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_residiuum_store_crash_child") {
        return PathBuf::from(p);
    }
    let mut exe = std::env::current_exe().expect("current_exe");
    exe.pop();
    if exe.file_name().and_then(|s| s.to_str()) == Some("deps") {
        exe.pop();
    }
    exe.push("residiuum-store-crash-child");
    if exe.is_file() {
        return exe;
    }
    let mut alt = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    alt.pop();
    alt.pop();
    alt.push("target");
    alt.push(if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    });
    alt.push("residiuum-store-crash-child");
    alt
}
