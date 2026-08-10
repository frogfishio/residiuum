//! Stage 4a–4d: open, put/get/delete, bytes, scan, filters, error codes.

use residiuum_sdk::{
    json, DurabilityMode, ErrorCode, Filter, PutOptions, QueryOptions, Residiuum, SortOrder,
};

use serde::Deserialize;
use tempfile::tempdir;

#[test]
fn open_put_get_delete_json() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    let mut db = Residiuum::open(&path).unwrap();
    let store_id = db.store_id();
    {
        let mut users = db.collection("users").unwrap();
        let receipt = users
            .put("user-42", &json!({ "name": "Alice", "status": "active" }))
            .unwrap();
        assert_eq!(receipt.key, "user-42");
        assert_eq!(receipt.acknowledgement, DurabilityMode::Durable);
        assert!(receipt.committed);
        assert_eq!(receipt.store_id, store_id);

        let alice = users.get("user-42").unwrap().unwrap();
        assert_eq!(alice["name"], "Alice");
        assert_eq!(alice["status"], "active");

        let del = users.delete("user-42").unwrap();
        assert!(del.removed);
        assert!(users.get("user-42").unwrap().is_none());

        let del2 = users.delete("user-42").unwrap();
        assert!(!del2.removed);
    }
}

#[test]
fn create_or_open_and_reopen_persists() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let mut db = Residiuum::open(&path).unwrap();
        db.collection("users")
            .unwrap()
            .put("u1", &json!({"n": 1}))
            .unwrap();
    }
    let mut db = Residiuum::open(&path).unwrap();
    let mut users = db.collection("users").unwrap();
    assert_eq!(users.get("u1").unwrap().unwrap()["n"], 1);
}

#[test]
fn collections_are_isolated() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    db.collection("a")
        .unwrap()
        .put("k", &json!("from-a"))
        .unwrap();
    db.collection("b")
        .unwrap()
        .put("k", &json!("from-b"))
        .unwrap();
    assert_eq!(
        db.collection("a").unwrap().get("k").unwrap().unwrap(),
        json!("from-a")
    );
    assert_eq!(
        db.collection("b").unwrap().get("k").unwrap().unwrap(),
        json!("from-b")
    );
}

#[test]
fn overwrite_updates_current_value() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    let mut c = db.collection("docs").unwrap();
    c.put("x", &json!(1)).unwrap();
    c.put("x", &json!(2)).unwrap();
    assert_eq!(c.get("x").unwrap().unwrap(), json!(2));
}

#[test]
fn get_as_typed_struct() {
    #[derive(Debug, Deserialize, PartialEq)]
    struct User {
        name: String,
        age: u32,
    }

    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    let mut users = db.collection("users").unwrap();
    users
        .put("ada", &json!({"name": "Ada", "age": 36}))
        .unwrap();
    let u: User = users.get_as("ada").unwrap().unwrap();
    assert_eq!(
        u,
        User {
            name: "Ada".into(),
            age: 36
        }
    );
}

#[test]
fn bytes_roundtrip() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    let mut arts = db.collection("artifacts").unwrap();
    let payload = b"\x00hello\xff".as_slice();
    arts.put_bytes("build-19", payload).unwrap();
    assert_eq!(
        arts.get_bytes("build-19").unwrap().as_deref(),
        Some(payload)
    );
    // JSON get on bytes payload is a typed error, not silent corruption.
    assert!(arts
        .get("build-19")
        .unwrap_err()
        .to_string()
        .contains("mismatch"));
}

#[test]
fn scan_keys_and_json() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    {
        let mut users = db.collection("users").unwrap();
        users.put("b", &json!({"i": 2})).unwrap();
        users.put("a", &json!({"i": 1})).unwrap();
        users.put("c", &json!({"i": 3})).unwrap();
        users.delete("b").unwrap();
    }
    // Other collection must not appear in scan.
    db.collection("other")
        .unwrap()
        .put("a", &json!(true))
        .unwrap();

    let mut users = db.collection("users").unwrap();
    assert_eq!(users.scan_keys().unwrap(), vec!["a", "c"]);
    let rows = users.scan_json().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "a");
    assert_eq!(rows[0].1["i"], 1);
    assert_eq!(rows[1].0, "c");
}

#[test]
fn memory_mode_not_visible_after_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let mut db = Residiuum::open(&path).unwrap();
        let mut c = db.collection("t").unwrap();
        c.put_with("disk", &json!(1), PutOptions::durable())
            .unwrap();
        c.put_with("mem", &json!(2), PutOptions::memory()).unwrap();
        assert!(c.get("mem").unwrap().is_some());
    }
    let mut db = Residiuum::open(&path).unwrap();
    let mut c = db.collection("t").unwrap();
    assert_eq!(c.get("disk").unwrap().unwrap(), json!(1));
    assert!(c.get("mem").unwrap().is_none());
}

#[test]
fn rejects_empty_collection_and_key() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    assert!(db.collection("").is_err());
    let mut c = db.collection("ok").unwrap();
    assert!(c.put("", &json!(1)).is_err());
}

#[test]
fn readme_happy_path() {
    // DX_SPEC / README journey: open + store JSON in under one minute.
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    let mut users = db.collection("users").unwrap();
    users
        .put(
            "user-42",
            &json!({
                "name": "Alice",
                "status": "active"
            }),
        )
        .unwrap();
    let alice = users.get("user-42").unwrap().unwrap();
    assert_eq!(alice["name"], "Alice");
}

#[test]
fn find_object_filter_and_builder() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    {
        let mut users = db.collection("users").unwrap();
        users
            .put(
                "a",
                &json!({"name": "Ada", "status": "active", "age": 36, "country": "TH"}),
            )
            .unwrap();
        users
            .put(
                "b",
                &json!({"name": "Bob", "status": "active", "age": 17, "country": "SG"}),
            )
            .unwrap();
        users
            .put(
                "c",
                &json!({"name": "Cyd", "status": "paused", "age": 40, "country": "TH"}),
            )
            .unwrap();
        users
            .put(
                "d",
                &json!({"name": "Dan", "status": "active", "age": 22, "country": "US"}),
            )
            .unwrap();
    }

    let mut users = db.collection("users").unwrap();

    // DX_SPEC §7.1 object filter
    let rows = users
        .find_json(&json!({
            "status": "active",
            "age": { "$gte": 18 },
            "country": { "$in": ["TH", "SG"] }
        }))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "a");
    assert_eq!(rows[0].1["name"], "Ada");

    // Predicate AST
    let f = Filter::and([
        Filter::field("status").eq("active"),
        Filter::field("age").gte(18),
    ]);
    let rows = users.find(&f).unwrap();
    assert_eq!(
        rows.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
        vec!["a", "d"]
    );

    // Fluent builder + limit + order
    let rows = users
        .query()
        .where_eq("status", "active")
        .where_gte("age", 18)
        .order_by("age", SortOrder::Desc)
        .limit(1)
        .collect()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "a"); // age 36 > 22
    assert_eq!(rows[0].1["age"], 36);

    // Order asc with limit
    let rows = users
        .find_with(
            &Filter::field("status").eq("active"),
            QueryOptions::new().order_by("age", SortOrder::Asc).limit(2),
        )
        .unwrap();
    assert_eq!(rows[0].0, "b"); // 17
    assert_eq!(rows[1].0, "d"); // 22
}

#[test]
fn scan_json_iter_streams() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    {
        let mut c = db.collection("docs").unwrap();
        for i in 0..20 {
            c.put(&format!("k{i:02}"), &json!({"i": i})).unwrap();
        }
    }
    let mut c = db.collection("docs").unwrap();
    let mut n = 0;
    for row in c.scan_json_iter().unwrap() {
        let (k, v) = row.unwrap();
        assert!(k.starts_with('k'));
        assert!(v["i"].as_i64().unwrap() >= 0);
        n += 1;
    }
    assert_eq!(n, 20);
}

#[test]
fn error_codes_are_stable() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    let err = match db.collection("") {
        Ok(_) => panic!("expected validation error"),
        Err(e) => e,
    };
    assert_eq!(err.code(), ErrorCode::ValidationFailed);
    assert!(err.is_validation());
    assert_eq!(err.code().as_str(), "validation_failed");

    let mut c = db.collection("ok").unwrap();
    let err = c.put("", &json!(1)).unwrap_err();
    assert_eq!(err.code(), ErrorCode::ValidationFailed);

    c.put_bytes("bin", b"raw").unwrap();
    let err = c.get("bin").unwrap_err();
    assert_eq!(err.code(), ErrorCode::TypeMismatch);
    assert_eq!(err.code().as_str(), "type_mismatch");

    let err = Filter::from_json(&json!({"age": {"$nope": 1}})).unwrap_err();
    assert_eq!(err.code(), ErrorCode::QueryInvalid);
    assert!(err.is_query_invalid());
}
