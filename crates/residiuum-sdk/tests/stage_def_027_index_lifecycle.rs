//! DEF-027: online, truthful secondary index lifecycle.
//!
//! - Durable build state (building / ready / stale / partial / failed / rebuilding)
//! - Resume after failpoints at plan / mid / before_ready
//! - Stale and partial never prove absence
//! - Dropping indexes never changes correctness
//! - Indexed vs force-scan results match

use residiuum_sdk::{json, Filter, IndexState, QueryOptions, Residiuum};

use residiuum_store::{
    arm_failpoint, clear_failpoints, disarm_failpoint, FailpointAction,
    IndexState as StoreIndexState, Store,
};
use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;
use tempfile::tempdir;

/// Process-global failpoints require serializing tests that arm them.
static FAILPOINT_LOCK: Mutex<()> = Mutex::new(());

const FP_AFTER_PLAN: &str = "index.build.after_plan";
const FP_MID: &str = "index.build.mid";
const FP_BEFORE_READY: &str = "index.build.before_ready";

struct FailpointGuard;
impl Drop for FailpointGuard {
    fn drop(&mut self) {
        clear_failpoints();
    }
}

fn with_failpoints<R>(f: impl FnOnce() -> R) -> R {
    let _lock = FAILPOINT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    let _guard = FailpointGuard;
    f()
}

#[test]
fn create_reaches_ready_with_build_id() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    let mut users = db.collection("users").unwrap();
    users.put("u1", &json!({"email": "a@x.com"})).unwrap();
    users.put("u2", &json!({"email": "b@x.com"})).unwrap();
    let info = users
        .indexes()
        .unwrap()
        .create("by-email", &["email"])
        .unwrap();
    assert_eq!(info.state, IndexState::Ready);
    assert!(info.complete_coverage);
    assert!(!info.build_id_hex.is_empty());
    assert!(info.failure_reason.is_empty());
    let rows = users.find(&Filter::field("email").eq("b@x.com")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "u2");
}

#[test]
fn write_marks_ready_stale_and_miss_falls_back_to_scan() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    let mut users = db.collection("users").unwrap();
    users.put("u1", &json!({"email": "a@x.com"})).unwrap();
    users
        .indexes()
        .unwrap()
        .create("by-email", &["email"])
        .unwrap();
    users.put("u2", &json!({"email": "b@x.com"})).unwrap();
    let listed = users.list_indexes().unwrap();
    assert_eq!(listed[0].state, IndexState::Stale);
    assert!(!listed[0].complete_coverage);
    let rows = users.find(&Filter::field("email").eq("b@x.com")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "u2");
    let none = users
        .find(&Filter::field("email").eq("missing@x.com"))
        .unwrap();
    assert!(none.is_empty());
}

#[test]
fn resume_after_plan_failpoint() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let path = dir.path().join("app.residiuum");
        {
            let mut db = Residiuum::open(&path).unwrap();
            let mut c = db.collection("docs").unwrap();
            for i in 0..10 {
                c.put(&format!("k{i}"), &json!({"n": i})).unwrap();
            }
            arm_failpoint(FP_AFTER_PLAN, FailpointAction::Error);
            let err = c.indexes().unwrap().create("by-n", &["n"]).unwrap_err();
            assert!(matches!(
                err,
                residiuum_sdk::Error::Store(residiuum_store::StoreError::Failpoint(_))
            ));
            disarm_failpoint(FP_AFTER_PLAN);
            drop(c);
            drop(db);
            let store = Store::open_inspect(&path).unwrap();
            let idx = store.load_secondary_index("docs", "by-n").unwrap().unwrap();
            assert!(matches!(
                idx.meta.state,
                StoreIndexState::Building | StoreIndexState::Rebuilding
            ));
            assert!(!idx.meta.build_id.iter().all(|&b| b == 0));
        }
        let mut db = Residiuum::open(&path).unwrap();
        let mut c = db.collection("docs").unwrap();
        let info = c.indexes().unwrap().create("by-n", &["n"]).unwrap();
        assert_eq!(info.state, IndexState::Ready);
        assert!(info.entry_count >= 10);
        let rows = c.find(&Filter::field("n").eq(7)).unwrap();
        assert_eq!(rows.len(), 1);
    });
}

#[test]
fn resume_after_mid_build_failpoint() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let path = dir.path().join("app.residiuum");
        {
            let mut db = Residiuum::open(&path).unwrap();
            let mut c = db.collection("docs").unwrap();
            for i in 0..80 {
                c.put(&format!("k{i:03}"), &json!({"n": i})).unwrap();
            }
            arm_failpoint(FP_MID, FailpointAction::Error);
            let err = c.indexes().unwrap().create("by-n", &["n"]);
            assert!(err.is_err());
            disarm_failpoint(FP_MID);
            drop(c);
            drop(db);
            let store = Store::open_inspect(&path).unwrap();
            let idx = store.load_secondary_index("docs", "by-n").unwrap().unwrap();
            assert!(idx.is_build_in_progress());
            assert!(!idx.meta.resume_after_subject.is_empty());
            assert!(idx.meta.entry_count > 0);
        }
        let mut db = Residiuum::open(&path).unwrap();
        let mut c = db.collection("docs").unwrap();
        let info = c.indexes().unwrap().continue_build("by-n").unwrap();
        assert_eq!(info.state, IndexState::Ready);
        assert!(info.entry_count >= 80);
        let rows = c.find(&Filter::field("n").eq(55)).unwrap();
        assert_eq!(rows.len(), 1);
    });
}

#[test]
fn resume_after_before_ready_failpoint() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let path = dir.path().join("app.residiuum");
        {
            let mut db = Residiuum::open(&path).unwrap();
            let mut c = db.collection("docs").unwrap();
            for i in 0..5 {
                c.put(&format!("k{i}"), &json!({"n": i})).unwrap();
            }
            arm_failpoint(FP_BEFORE_READY, FailpointAction::Error);
            assert!(c.indexes().unwrap().create("by-n", &["n"]).is_err());
            disarm_failpoint(FP_BEFORE_READY);
            drop(c);
            drop(db);
            let store = Store::open_inspect(&path).unwrap();
            let idx = store.load_secondary_index("docs", "by-n").unwrap().unwrap();
            assert!(idx.is_build_in_progress());
            assert!(idx.meta.entry_count >= 5);
        }
        let mut db = Residiuum::open(&path).unwrap();
        let info = db
            .collection("docs")
            .unwrap()
            .indexes()
            .unwrap()
            .create("by-n", &["n"])
            .unwrap();
        assert_eq!(info.state, IndexState::Ready);
    });
}

#[test]
fn panic_failpoint_leaves_building_and_resume_works() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let path = dir.path().join("app.residiuum");
        {
            let mut db = Residiuum::open(&path).unwrap();
            let mut c = db.collection("docs").unwrap();
            for i in 0..40 {
                c.put(&format!("k{i:02}"), &json!({"n": i})).unwrap();
            }
            arm_failpoint(FP_MID, FailpointAction::Panic);
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = c.indexes().unwrap().create("by-n", &["n"]);
            }));
            assert!(result.is_err());
            disarm_failpoint(FP_MID);
            drop(c);
            drop(db);
        }
        let mut db = Residiuum::open(&path).unwrap();
        let mut c = db.collection("docs").unwrap();
        {
            let store_view = Store::open_inspect(&path).unwrap();
            let mid = store_view
                .load_secondary_index("docs", "by-n")
                .unwrap()
                .unwrap();
            assert!(mid.is_build_in_progress());
        }
        let info = c.indexes().unwrap().create("by-n", &["n"]).unwrap();
        assert_eq!(info.state, IndexState::Ready);
    });
}

#[test]
fn partial_index_never_proves_absence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let mut db = Residiuum::open(&path).unwrap();
        let mut c = db.collection("users").unwrap();
        c.put("u1", &json!({"email": "a@x.com"})).unwrap();
        c.indexes().unwrap().create("by-email", &["email"]).unwrap();
        // Add a second doc via SDK while index is ready → becomes Stale.
        c.put("u2", &json!({"email": "b@x.com"})).unwrap();
        assert_eq!(c.list_indexes().unwrap()[0].state, IndexState::Stale);
    }
    // Force Partial (with only u1 postings) while u2 lives in the store.
    {
        let store = Store::open(&path).unwrap();
        let mut idx = store
            .load_secondary_index("users", "by-email")
            .unwrap()
            .unwrap();
        // Drop all postings so b@x.com is a miss on the index.
        idx.clear_entries();
        idx.mark_partial([1u8; 32], "test partial after write");
        store.write_secondary_index(&idx).unwrap();
    }
    let mut db = Residiuum::open(&path).unwrap();
    let mut c = db.collection("users").unwrap();
    let listed = c.list_indexes().unwrap();
    assert_eq!(listed[0].state, IndexState::Partial);
    assert!(!listed[0].complete_coverage);
    // Partial must not hide live docs: empty index miss → fall through to scan.
    let rows = c.find(&Filter::field("email").eq("b@x.com")).unwrap();
    assert_eq!(rows.len(), 1, "partial must not hide live docs");
    assert_eq!(rows[0].0, "u2");
}

#[test]
fn drop_all_indexes_preserves_query_correctness() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    let mut c = db.collection("items").unwrap();
    for i in 0..20 {
        c.put(
            &format!("k{i}"),
            &json!({"tag": if i % 2 == 0 { "even" } else { "odd" }, "n": i}),
        )
        .unwrap();
    }
    c.indexes().unwrap().create("by-tag", &["tag"]).unwrap();
    let with_index: BTreeSet<_> = c
        .find(&Filter::field("tag").eq("even"))
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    c.indexes().unwrap().drop("by-tag").unwrap();
    assert!(c.list_indexes().unwrap().is_empty());
    let without: BTreeSet<_> = c
        .find(&Filter::field("tag").eq("even"))
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(with_index, without);
    assert_eq!(with_index.len(), 10);
}

#[test]
fn indexed_matches_force_scan() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    let mut c = db.collection("docs").unwrap();
    for i in 0..30 {
        c.put(
            &format!("k{i:02}"),
            &json!({"n": i % 5, "label": format!("L{i}")}),
        )
        .unwrap();
    }
    c.indexes().unwrap().create("by-n", &["n"]).unwrap();
    let filter = Filter::field("n").eq(3);
    let indexed: BTreeSet<_> = c
        .find(&filter)
        .unwrap()
        .into_iter()
        .map(|(k, v)| (k, v["label"].as_str().unwrap().to_string()))
        .collect();
    let scanned: BTreeSet<_> = c
        .find_with(&filter, QueryOptions::new().force_scan())
        .unwrap()
        .into_iter()
        .map(|(k, v)| (k, v["label"].as_str().unwrap().to_string()))
        .collect();
    assert_eq!(indexed, scanned);
    assert_eq!(indexed.len(), 6);
}

#[test]
fn rebuild_uses_rebuilding_then_ready() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    let mut c = db.collection("users").unwrap();
    c.put("u1", &json!({"email": "a@x.com"})).unwrap();
    c.indexes().unwrap().create("by-email", &["email"]).unwrap();
    c.put("u2", &json!({"email": "b@x.com"})).unwrap();
    assert_eq!(c.list_indexes().unwrap()[0].state, IndexState::Stale);
    let rebuilt = c.indexes().unwrap().rebuild("by-email").unwrap();
    assert_eq!(rebuilt.state, IndexState::Ready);
    assert!(rebuilt.entry_count >= 2);
    let rows = c.find(&Filter::field("email").eq("b@x.com")).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn concurrent_writes_during_build_do_not_block() {
    with_failpoints(|| {
        let dir = tempdir().unwrap();
        let path = dir.path().join("app.residiuum");
        {
            let mut db = Residiuum::open(&path).unwrap();
            let mut c = db.collection("docs").unwrap();
            for i in 0..40 {
                c.put(&format!("k{i:02}"), &json!({"n": i})).unwrap();
            }
            arm_failpoint(FP_MID, FailpointAction::Error);
            let err = c.indexes().unwrap().create("by-n", &["n"]);
            assert!(err.is_err());
            disarm_failpoint(FP_MID);
            // Writes must succeed while index is Building.
            c.put("k99", &json!({"n": 99})).unwrap();
            assert_eq!(c.get("k99").unwrap().unwrap()["n"], 99);
        }
        let mut db = Residiuum::open(&path).unwrap();
        let mut c = db.collection("docs").unwrap();
        let info = c.indexes().unwrap().create("by-n", &["n"]).unwrap();
        assert_eq!(info.state, IndexState::Ready);
        let rows = c.find(&Filter::field("n").eq(99)).unwrap();
        assert_eq!(rows.len(), 1);
    });
}
