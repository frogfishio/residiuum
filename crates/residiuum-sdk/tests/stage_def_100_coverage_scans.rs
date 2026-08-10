//! DEF-100 — Collection coverage-aware key / document scans (embedded).

use residiuum_sdk::{json, Error, Residiuum};
use tempfile::tempdir;

#[test]
fn scan_keys_lists_verified_keys_around_chunked_values() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("db")).unwrap();
    let mut coll = db.collection("chats").unwrap();

    coll.put("a", &json!({"t": 1})).unwrap();
    let big: Vec<u8> = (0u8..80).collect();
    coll.put_bytes("big", &big).unwrap();
    coll.put("c", &json!({"t": 2})).unwrap();

    let keys = coll.scan_keys().unwrap();
    assert_eq!(
        keys,
        vec!["a".to_string(), "big".to_string(), "c".to_string()]
    );
}

#[test]
fn scan_keys_page_and_partial_document_page() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("db")).unwrap();
    let mut coll = db.collection("docs").unwrap();

    for i in 0..5 {
        coll.put(&format!("k{i}"), &json!({"i": i})).unwrap();
    }
    // Bytes body — complete for store, undecodable as JSON on partial page.
    coll.put_bytes("raw", b"not-json").unwrap();

    let page = coll.scan_keys_page(3, None).unwrap();
    assert_eq!(page.keys.len(), 3);
    assert!(page.has_more);
    assert!(!page.coverage_complete); // not final page
    let page2 = coll
        .scan_keys_page(3, page.continuation.as_deref())
        .unwrap();
    assert!(!page2.has_more);
    assert!(page2.coverage_complete);

    let doc = coll.scan_json_partial_page(16, None).unwrap();
    assert!(doc.rows.iter().any(|(k, _)| k == "k0"));
    assert!(
        doc.undecodable.iter().any(|u| u.key == "raw"),
        "expected undecodable raw: {:?}",
        doc.undecodable
    );
    assert!(doc.incomplete.is_empty());
    assert!(doc.key_coverage_complete);
}

#[test]
fn scan_json_page_still_succeeds_when_complete() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("db")).unwrap();
    let mut coll = db.collection("ok").unwrap();
    coll.put("x", &json!(1)).unwrap();
    let page = coll.scan_json_page(8, None).unwrap();
    assert_eq!(page.rows.len(), 1);
    assert!(page.complete);
}

#[test]
fn scan_keys_empty_collection_complete() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("db")).unwrap();
    let mut coll = db.collection("empty").unwrap();
    let keys = coll.scan_keys().unwrap();
    assert!(keys.is_empty());
    assert!(!matches!(
        coll.scan_keys().map(|_| ()),
        Err(Error::CoverageIncomplete(_))
    ));
}
