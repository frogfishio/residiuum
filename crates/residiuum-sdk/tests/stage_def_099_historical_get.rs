//! DEF-099 — Collection historical get / last-complete (embedded).

use residiuum_sdk::{json, Residiuum};
use residiuum_store::{BeforeEvent, RecoveryReadOptions};
use tempfile::tempdir;

#[test]
fn collection_get_version_and_find_last_complete() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("db")).unwrap();
    let mut coll = db.collection("docs").unwrap();

    coll.put("k", &json!({"n": 1})).unwrap();
    let hist = coll.history("k").unwrap();
    assert_eq!(hist.versions.len(), 1);
    let eid = unhex16(&hist.versions[0].event_id);

    coll.put("k", &json!({"n": 2})).unwrap();
    assert_eq!(coll.get("k").unwrap().unwrap()["n"], 2);

    let ver = coll.get_version("k", &eid).unwrap();
    assert!(!ver.is_tombstone);
    let body = ver
        .selected
        .as_ref()
        .and_then(|p| p.complete_body())
        .expect("complete historical body");
    // Historical logical body is typed JSON encoding; current get still n=2.
    assert!(body.len() > 1);
    assert_eq!(coll.get("k").unwrap().unwrap()["n"], 2);

    let found = coll
        .find_last_complete("k", RecoveryReadOptions::default())
        .unwrap();
    assert!(found.found.is_some());
    assert_eq!(found.found.as_ref().unwrap().selected_event_id, eid);
    let _ = BeforeEvent::Current;
}

fn unhex16(s: &str) -> [u8; 16] {
    residiuum_store::unhex16(s).expect("hex event id")
}
