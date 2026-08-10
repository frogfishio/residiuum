//! Stage 6 SDK: secondary indexes, history, query budget, catalog rebuild.

use residiuum_sdk::{
    json, ErrorCode, Filter, IndexState, QueryBudget, QueryOptions, Residiuum, SortOrder,
};

use std::fs;
use tempfile::tempdir;

#[test]
fn secondary_index_create_query_drop() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    {
        let mut users = db.collection("users").unwrap();
        users
            .put("u1", &json!({"email": "a@x.com", "status": "active"}))
            .unwrap();
        users
            .put("u2", &json!({"email": "b@x.com", "status": "active"}))
            .unwrap();
        users
            .put("u3", &json!({"email": "c@x.com", "status": "idle"}))
            .unwrap();

        let info = users
            .indexes()
            .unwrap()
            .create("by-email", &["email"])
            .unwrap();
        assert_eq!(info.state, IndexState::Ready);
        assert_eq!(info.name, "by-email");
        assert!(info.entry_count >= 3);

        let rows = users.find(&Filter::field("email").eq("b@x.com")).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "u2");

        // Drop index; query still correct via scan.
        users.indexes().unwrap().drop("by-email").unwrap();
        let rows = users.find(&Filter::field("email").eq("b@x.com")).unwrap();
        assert_eq!(rows.len(), 1);
    }
}

#[test]
fn index_states_visible_and_stale_after_write() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    {
        let mut users = db.collection("users").unwrap();
        users.put("u1", &json!({"email": "a@x.com"})).unwrap();
        let info = users
            .indexes()
            .unwrap()
            .create("by-email", &["email"])
            .unwrap();
        assert_eq!(info.state, IndexState::Ready);

        users.put("u2", &json!({"email": "b@x.com"})).unwrap();
        let after = users.list_indexes().unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].state, IndexState::Stale);

        // Rebuild restores ready.
        let rebuilt = users.indexes().unwrap().rebuild("by-email").unwrap();
        assert_eq!(rebuilt.state, IndexState::Ready);
        assert!(rebuilt.entry_count >= 2);
    }
}

#[test]
fn query_budget_required_on_tight_scan() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    {
        let mut c = db.collection("docs").unwrap();
        for i in 0..10 {
            c.put(&format!("k{i}"), &json!({"n": i})).unwrap();
        }
        // No index; force scan with tiny budget.
        let err = c
            .find_with(
                &Filter::field("n").eq(9),
                QueryOptions::new()
                    .budget(QueryBudget::max_docs(3))
                    .force_scan(),
            )
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::QueryBudgetRequired);
    }
}

#[test]
fn history_for_key() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    {
        let mut users = db.collection("users").unwrap();
        users.put("u1", &json!({"v": 1})).unwrap();
        users.put("u1", &json!({"v": 2})).unwrap();
        users.delete("u1").unwrap();
        users.put("u1", &json!({"v": 3})).unwrap();

        let hist = users.history("u1").unwrap();
        assert_eq!(hist.key, "u1");
        assert_eq!(hist.versions.len(), 4);
        assert_eq!(hist.versions[0].kind, "put");
        assert_eq!(hist.versions[0].json.as_ref().unwrap()["v"], 1);
        assert_eq!(hist.versions[2].kind, "delete");
        assert_eq!(hist.versions[3].json.as_ref().unwrap()["v"], 3);
    }
}

#[test]
fn wipe_catalogs_indexes_same_logical_content() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let mut db = Residiuum::open(&path).unwrap();
        db.collection("users")
            .unwrap()
            .put("u1", &json!({"n": 1}))
            .unwrap();
        db.collection("orders")
            .unwrap()
            .put("o1", &json!({"n": 2}))
            .unwrap();
        db.collection("users")
            .unwrap()
            .indexes()
            .unwrap()
            .create("by-n", &["n"])
            .unwrap();
        let cols = db.list_collections().unwrap();
        assert!(cols.contains(&"users".to_string()));
        assert!(cols.contains(&"orders".to_string()));
    }

    // Wipe derived dirs.
    for name in ["catalogs", "indexes", "snapshots"] {
        let p = path.join(name);
        if p.exists() {
            fs::remove_dir_all(&p).unwrap();
        }
    }

    let mut db = Residiuum::open(&path).unwrap();
    db.rebuild_index().unwrap();
    db.rebuild_catalogs().unwrap();
    assert_eq!(
        db.collection("users").unwrap().get("u1").unwrap().unwrap()["n"],
        1
    );
    assert_eq!(
        db.collection("orders").unwrap().get("o1").unwrap().unwrap()["n"],
        2
    );
    let cols = db.list_collections().unwrap();
    assert!(cols.contains(&"users".to_string()));
    assert!(cols.contains(&"orders".to_string()));
}

#[test]
fn chunked_large_bytes_roundtrip() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    // Force chunking with low threshold on underlying store.
    db.store_mut().unwrap().set_chunk_threshold(32);
    db.store_mut().unwrap().set_chunk_size(16);
    {
        let mut c = db.collection("blobs").unwrap();
        let data: Vec<u8> = (0u8..100).collect();
        c.put_bytes("big", &data).unwrap();
        assert_eq!(
            c.get_bytes("big").unwrap().as_deref(),
            Some(data.as_slice())
        );
    }
}

#[test]
fn find_still_orders_and_limits() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    {
        let mut c = db.collection("t").unwrap();
        c.put("a", &json!({"age": 30})).unwrap();
        c.put("b", &json!({"age": 20})).unwrap();
        c.put("c", &json!({"age": 40})).unwrap();
        let rows = c
            .find_with(
                &Filter::always(),
                QueryOptions::new().order_by("age", SortOrder::Asc).limit(2),
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1["age"], 20);
        assert_eq!(rows[1].1["age"], 30);
    }
}
