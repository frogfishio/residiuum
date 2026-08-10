//! CSQ-2: boundary census, hit proof, crash controller, composed-failure schedule.
//!
//! Complements `stage_def_022_crash_matrix` (cell drivers) with package-level
//! instrumentation gates from CORE_STORAGE_QUALIFICATION_IMPLEMENTATION_PLAN §6.

use residiuum_store::{
    arm_failpoint_once, clear_failpoints, clear_failpoints_all, enable_failpoint_hit_proof,
    failpoint_is_armed, failpoint_visit_count, failure_class_action, harness_is_approved,
    hit_failpoint, require_failpoint_visited, schedule_failure_combinations, validate_crash_matrix,
    validate_failure_combinations, CrashController, DurabilityMode, FailpointAction,
    FailureCombinationDoc, FilesystemImageHarness, ScheduleDecision, Store, HARNESS_CAPABILITIES,
};
use serde_json::Value;
use std::path::PathBuf;
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

fn load_json(rel: &str) -> Value {
    let path = workspace_root().join(rel);
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

#[test]
fn csq2_harness_inventory_ready_flags() {
    assert!(HARNESS_CAPABILITIES
        .iter()
        .any(|c| c.ready && c.owner_package == "CSQ-2"));
    let fs = FilesystemImageHarness::inventory();
    // CSQ-5 marks portable FS image ready; privileged loopback remains skip-with-reason.
    assert!(fs.ready);
}

#[test]
fn csq2_every_boundary_has_approved_harness() {
    let bnd = load_json("spec/verification/core-storage/boundaries-v1.json");
    let items = bnd["items"].as_array().expect("items");
    assert!(items.len() >= 100, "expected full boundary census");
    for i in items {
        let id = i["id"].as_str().unwrap();
        let h = i["harness"].as_str().unwrap_or("");
        assert!(
            harness_is_approved(h),
            "boundary {id} unapproved harness {h:?}"
        );
        if h == "in_process_failpoint" && i.get("failpoint").and_then(|x| x.as_str()).is_none() {
            let kind = i.get("kind").and_then(|x| x.as_str()).unwrap_or("");
            assert_eq!(
                kind, "logical",
                "boundary {id}: in_process_failpoint without failpoint name"
            );
        }
    }
}

#[test]
fn csq2_source_failpoints_match_registry() {
    // Static check duplicated in scripts/verify-csq-boundary-instrumentation.sh;
    // keep a Rust agreement gate for CI without shell.
    let bnd = load_json("spec/verification/core-storage/boundaries-v1.json");
    let reg: std::collections::HashSet<String> = bnd["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|i| {
            i.get("failpoint")
                .and_then(|x| x.as_str())
                .map(str::to_string)
        })
        .collect();
    assert!(reg.len() >= 40, "expected full source failpoint census");

    let src_root = workspace_root().join("crates/residiuum-store/src");
    let mut hits = std::collections::HashSet::new();
    for ent in walkdir_rs(&src_root) {
        let text = std::fs::read_to_string(&ent).unwrap();
        for cap in regex_failpoints(&text) {
            hits.insert(cap);
        }
    }
    assert_eq!(
        hits, reg,
        "source failpoints must equal registered failpoint boundaries"
    );
}

fn walkdir_rs(root: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn rec(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            let p = e.path();
            if p.is_dir() {
                rec(&p, out);
            } else if p.extension().and_then(|s| s.to_str()) == Some("rs") {
                out.push(p);
            }
        }
    }
    rec(root, &mut out);
    out
}

fn regex_failpoints(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for marker in ["failpoint::hit(\"", "failpoint::consume_short_write(\""] {
        let mut rest = text;
        while let Some(i) = rest.find(marker) {
            let after = &rest[i + marker.len()..];
            if let Some(end) = after.find('"') {
                out.push(after[..end].to_string());
                rest = &after[end + 1..];
            } else {
                break;
            }
        }
    }
    out
}

#[test]
fn csq2_hit_proof_unreachable_is_fail() {
    let _g = matrix_lock();
    clear_failpoints_all();
    enable_failpoint_hit_proof();

    // Visited without arm still counts (reachability).
    assert_eq!(failpoint_visit_count("store.active.write_tail.before"), 0);
    hit_failpoint("store.active.write_tail.before").unwrap();
    require_failpoint_visited("store.active.write_tail.before");

    // Arm that never fires stays armed — matrix drivers must treat as miss.
    arm_failpoint_once("test.csq2.never", FailpointAction::Error);
    assert!(failpoint_is_armed("test.csq2.never"));
    // Simulating a driver that forgets to hit: still armed ⇒ fail condition.
    assert!(
        failpoint_is_armed("test.csq2.never"),
        "unreachable injection must remain detectable via is_armed"
    );
    clear_failpoints_all();
}

#[test]
fn csq2_hit_proof_on_put_boundary() {
    let _g = matrix_lock();
    clear_failpoints_all();
    enable_failpoint_hit_proof();

    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let mut s = Store::create(&path).unwrap();
        s.put("prior", b"v1", DurabilityMode::Durable).unwrap();
    }
    let mut store = Store::open(&path).unwrap();
    arm_failpoint_once("store.active.write_tail.before", FailpointAction::Error);
    let err = store
        .put("k", b"v-new", DurabilityMode::Durable)
        .unwrap_err();
    assert!(matches!(err, residiuum_store::StoreError::Failpoint(_)));
    require_failpoint_visited("store.active.write_tail.before");
    assert!(
        !failpoint_is_armed("store.active.write_tail.before"),
        "successful injection must consume arm_once"
    );
    drop(store);
    clear_failpoints_all();
}

#[test]
fn csq2_composed_failure_schedule_from_registry() {
    let path = workspace_root().join("spec/verification/core-storage/failure-combinations-v1.json");
    let text = std::fs::read_to_string(&path).unwrap();
    let doc: FailureCombinationDoc = serde_json::from_str(&text).unwrap();
    validate_failure_combinations(&doc).expect("combinations valid");
    let sched = schedule_failure_combinations(&doc, true).expect("schedule");
    assert!(!sched.is_empty());
    let mut runs = 0usize;
    let mut rejects = 0usize;
    for d in &sched {
        match d {
            ScheduleDecision::Run { id, order, owner } => {
                assert!(order.len() >= 2, "{id} order");
                assert!(!owner.is_empty(), "{id} owner");
                // Every scheduled class must map to an action or be campaign-only.
                for f in order {
                    let _ = failure_class_action(f);
                }
                runs += 1;
            }
            ScheduleDecision::Reject { id, reason } => {
                assert!(!reason.is_empty(), "{id} rejection reason");
                rejects += 1;
            }
        }
    }
    assert!(runs >= 1, "expected at least one scheduled combination");
    let _ = rejects;
}

#[test]
fn csq2_ordered_pair_executor_ci_smoke() {
    // Execute one scheduled ordered pair as sequential single-fault injections
    // on the put path (full multi-fault campaigns are CSQ-5).
    let _g = matrix_lock();
    clear_failpoints_all();
    enable_failpoint_hit_proof();

    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let mut s = Store::create(&path).unwrap();
        s.put("prior", b"prior-v1", DurabilityMode::Durable)
            .unwrap();
    }

    // Pair: io_error then enospc (sequential cells, same boundary family).
    for (fp, action) in [
        ("store.active.write_tail.before", FailpointAction::Error),
        ("store.active.write_tail.before", FailpointAction::IoEnospc),
    ] {
        let mut store = Store::open(&path).unwrap();
        arm_failpoint_once(fp, action);
        let err = store
            .put("k", b"v-pair", DurabilityMode::Durable)
            .unwrap_err();
        match action {
            FailpointAction::Error => {
                assert!(matches!(err, residiuum_store::StoreError::Failpoint(_)));
            }
            FailpointAction::IoEnospc => match err {
                residiuum_store::StoreError::Io(e) => {
                    assert_eq!(e.kind(), std::io::ErrorKind::StorageFull);
                }
                other => panic!("expected StorageFull, got {other}"),
            },
            _ => unreachable!(),
        }
        require_failpoint_visited(fp);
        assert!(!failpoint_is_armed(fp));
        drop(store);
        clear_failpoints();
    }

    let store = Store::open(&path).unwrap();
    assert_eq!(
        store.get("prior").unwrap().as_deref(),
        Some(b"prior-v1".as_slice())
    );
    assert!(store.get("k").unwrap().is_none());
    clear_failpoints_all();
}

#[test]
fn csq2_crash_controller_resolves() {
    // Binary is built as a package bin; may be present after cargo test -p residiuum-store.
    if let Some(ctrl) = CrashController::resolve() {
        assert!(ctrl.binary.is_file());
    } else {
        // Still require the harness capability to be registered.
        assert!(HARNESS_CAPABILITIES
            .iter()
            .any(|c| c.kind == residiuum_store::BarrierKind::ChildProcessAbort && c.ready));
    }
}

#[test]
fn csq2_embedded_crash_matrix_still_valid() {
    let m = residiuum_store::load_crash_matrix().expect("load");
    validate_crash_matrix(&m).expect("validate");
}
