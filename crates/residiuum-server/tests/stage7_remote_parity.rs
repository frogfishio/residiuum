//! Stage 7 remote parity: history, indexes, get_payload, server-side find.

use residiuum_sdk::{
    json, ErrorCode, Filter, IndexState, PayloadResult, QueryBudget, QueryOptions, Residiuum,
};

use residiuum_server::{serve_store_with, ServeOptions};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

fn free_bind() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("127.0.0.1:{port}")
}

fn wait_for(bind: &str) {
    for _ in 0..40 {
        if std::net::TcpStream::connect(bind).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("server did not accept on {bind}");
}

fn spawn_server(path: std::path::PathBuf, bind: &str) {
    let path_c = path;
    let bind_c = bind.to_string();
    thread::spawn(move || {
        // Stage-7 open path: explicit legacy (HAR-4 product default is HeapKey).
        let _ = serve_store_with(path_c, &bind_c, ServeOptions::new().legacy_token_server());
    });
    wait_for(bind);
}

#[test]
fn remote_history_matches_embedded() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let mut db = Residiuum::open(&path).unwrap();
        let mut docs = db.collection("docs").unwrap();
        docs.put("k", &json!({"v": 1})).unwrap();
        docs.put("k", &json!({"v": 2})).unwrap();
        docs.delete("k").unwrap();
        docs.put("k", &json!({"v": 3})).unwrap();
    }

    let bind = free_bind();
    spawn_server(path, &bind);

    let url = format!("residiuum://{bind}/app");
    let mut remote = Residiuum::connect(&url).expect("connect");
    assert!(remote.is_remote());

    let hist = remote.collection("docs").unwrap().history("k").unwrap();
    assert_eq!(hist.key, "k");
    assert_eq!(hist.versions.len(), 4);
    assert_eq!(hist.versions[0].kind, "put");
    assert_eq!(hist.versions[0].json.as_ref().unwrap()["v"], 1);
    assert_eq!(hist.versions[2].kind, "delete");
    assert_eq!(hist.versions[3].json.as_ref().unwrap()["v"], 3);
    assert!(!hist.versions[0].event_id.is_empty());
    assert_eq!(hist.versions[0].event_id.len(), 32);
}

#[test]
fn remote_indexes_create_list_stale_rebuild_drop() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let mut db = Residiuum::open(&path).unwrap();
        let mut users = db.collection("users").unwrap();
        users
            .put("u1", &json!({"email": "a@x.com", "status": "active"}))
            .unwrap();
        users
            .put("u2", &json!({"email": "b@x.com", "status": "idle"}))
            .unwrap();
    }

    let bind = free_bind();
    spawn_server(path.clone(), &bind);

    let url = format!("residiuum://{bind}/app");
    let mut remote = Residiuum::connect(&url).unwrap();
    {
        let mut users = remote.collection("users").unwrap();
        let info = users
            .indexes()
            .unwrap()
            .create("by-email", &["email"])
            .unwrap();
        assert_eq!(info.state, IndexState::Ready);
        assert_eq!(info.name, "by-email");
        assert!(info.entry_count >= 2);

        let listed = users.list_indexes().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].fields, vec!["email".to_string()]);

        // Server-side find uses the remote index when ready.
        let rows = users.find(&Filter::field("email").eq("b@x.com")).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "u2");

        // Remote write marks index stale on server.
        users.put("u3", &json!({"email": "c@x.com"})).unwrap();
        let after = users.list_indexes().unwrap();
        assert_eq!(after[0].state, IndexState::Stale);

        let rebuilt = users.indexes().unwrap().rebuild("by-email").unwrap();
        assert_eq!(rebuilt.state, IndexState::Ready);
        assert!(rebuilt.entry_count >= 3);

        users.indexes().unwrap().drop("by-email").unwrap();
        assert!(users.list_indexes().unwrap().is_empty());
    }

    // Read-only inspect while the server still holds the exclusive writer lock
    // (DEF-020): index gone and data intact without competing for ownership.
    let mut local = Residiuum::open_inspect(&path).unwrap();
    assert!(local
        .collection("users")
        .unwrap()
        .list_indexes()
        .unwrap()
        .is_empty());
    assert_eq!(
        local
            .collection("users")
            .unwrap()
            .get("u3")
            .unwrap()
            .unwrap()["email"],
        "c@x.com"
    );
}

#[test]
fn remote_get_payload_complete_and_absent() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    let data: Vec<u8> = (0u8..100).collect();
    {
        let mut db = Residiuum::open(&path).unwrap();
        // Force chunking so get_payload reassembly is exercised.
        db.store_mut().unwrap().set_chunk_threshold(32);
        db.store_mut().unwrap().set_chunk_size(16);
        let mut c = db.collection("blobs").unwrap();
        c.put_bytes("big", &data).unwrap();
        c.put("small", &json!({"n": 1})).unwrap();
    }

    let bind = free_bind();
    spawn_server(path, &bind);

    let url = format!("residiuum://{bind}/app");
    let mut remote = Residiuum::connect(&url).unwrap();
    let mut blobs = remote.collection("blobs").unwrap();

    match blobs.get_payload("big").unwrap() {
        Some(PayloadResult::Complete { body }) => {
            // Store body is typed: 0x02 || raw bytes.
            assert_eq!(body.first(), Some(&0x02));
            assert_eq!(&body[1..], data.as_slice());
        }
        other => panic!("expected complete chunked payload: {other:?}"),
    }
    // Ordinary get_bytes still works (strips type tag).
    assert_eq!(
        blobs.get_bytes("big").unwrap().as_deref(),
        Some(data.as_slice())
    );

    match blobs.get_payload("small").unwrap() {
        Some(PayloadResult::Complete { body }) => {
            // JSON-encoded body: 0x01 || UTF-8 JSON.
            assert_eq!(body.first(), Some(&0x01));
            assert!(!body.is_empty());
        }
        other => panic!("expected complete small payload: {other:?}"),
    }

    assert!(blobs.get_payload("missing").unwrap().is_none());
}

#[test]
fn remote_find_index_accelerated_under_budget() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let mut db = Residiuum::open(&path).unwrap();
        let mut docs = db.collection("docs").unwrap();
        for i in 0..20 {
            docs.put(&format!("k{i}"), &json!({"n": i, "tag": format!("t{i}")}))
                .unwrap();
        }
        docs.indexes().unwrap().create("by-n", &["n"]).unwrap();
    }

    let bind = free_bind();
    spawn_server(path, &bind);

    let url = format!("residiuum://{bind}/app");
    let mut remote = Residiuum::connect(&url).unwrap();
    let mut docs = remote.collection("docs").unwrap();

    // Tight budget: full scan of 20 docs would fail; index probe must succeed.
    let rows = docs
        .find_with(
            &Filter::field("n").eq(17),
            QueryOptions::new().budget(QueryBudget::max_docs(3)),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "k17");
    assert_eq!(rows[0].1["tag"], "t17");

    // Force scan with the same budget fails.
    let err = docs
        .find_with(
            &Filter::field("n").eq(17),
            QueryOptions::new()
                .budget(QueryBudget::max_docs(3))
                .force_scan(),
        )
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::QueryBudgetRequired);
}

#[test]
fn remote_put_is_idempotent_with_operation_id() {
    use residiuum_sdk::{client_handshake, read_frame, write_frame, DEFAULT_MAX_FRAME_BYTES};

    use residiuum_server::handle_connection_with;

    use residiuum_store::{EventKind, Store};
    use std::io::BufReader;
    use std::net::{TcpListener, TcpStream};

    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let _ = Store::create(&path).unwrap();
    }

    // Same operation_id twice over the wire → one history event.
    let op = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let path_c = path.clone();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut store = Store::open(path_c).unwrap();
        let _ = handle_connection_with(
            &mut store,
            stream,
            ServeOptions::default().legacy_token_server(),
        );
    });

    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    client_handshake(&mut reader, &mut stream).unwrap();
    let put_body = format!(
        r#"{{"id":1,"op":"put","collection":"docs","key":"k","json":{{"n":1}},"durability":"durable","operation_id":"{op}"}}"#
    );
    write_frame(&mut stream, put_body.as_bytes()).unwrap();
    let resp1 = String::from_utf8(
        read_frame(&mut reader, DEFAULT_MAX_FRAME_BYTES)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(resp1.contains("\"ok\":true"), "{resp1}");

    // Exact retry with same operation_id.
    write_frame(&mut stream, put_body.as_bytes()).unwrap();
    let resp2 = String::from_utf8(
        read_frame(&mut reader, DEFAULT_MAX_FRAME_BYTES)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(resp2.contains("\"ok\":true"), "{resp2}");
    let e1 = extract_field(&resp1, "event_id");
    let e2 = extract_field(&resp2, "event_id");
    assert_eq!(e1, e2, "exact retry must return original event_id");

    // Content mismatch reuses id → consistency_violation.
    let bad = format!(
        r#"{{"id":3,"op":"put","collection":"docs","key":"k","json":{{"n":2}},"durability":"durable","operation_id":"{op}"}}"#
    );
    write_frame(&mut stream, bad.as_bytes()).unwrap();
    let resp3 = String::from_utf8(
        read_frame(&mut reader, DEFAULT_MAX_FRAME_BYTES)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(
        resp3.contains("consistency_violation") || resp3.contains("\"ok\":false"),
        "{resp3}"
    );

    drop(stream);
    // Inspect path does not need exclusive writer ownership (server may still hold it).
    let store = Store::open_inspect(&path).unwrap();
    let subject = residiuum_sdk::encode_subject("docs", "k").unwrap();
    let subject_str = std::str::from_utf8(&subject).unwrap();
    let hist = store.history(subject_str).unwrap();
    let puts: Vec<_> = hist
        .events
        .iter()
        .filter(|e| e.kind == EventKind::Put)
        .collect();
    assert_eq!(puts.len(), 1, "only one put event after idempotent retry");
}

fn extract_field(json_line: &str, field: &str) -> String {
    let needle = format!("\"{field}\":\"");
    let start = json_line.find(&needle).expect("field present") + needle.len();
    let rest = &json_line[start..];
    let end = rest.find('"').expect("closing quote");
    rest[..end].to_string()
}
