//! DEF-030: bounded server architecture.
//!
//! Proves concurrent clients are not serialized on the accept loop, connection
//! admission enforces limits with typed overload responses, one coordinated
//! store owner is used, and graceful shutdown drains in-flight work.

use residiuum_sdk::{
    client_handshake, json, read_frame, write_frame, ErrorCode, Residiuum, DEFAULT_MAX_FRAME_BYTES,
};

use residiuum_server::{
    serve_store_with, ServeOptions, ServerLimits, DEFAULT_MAX_CONNECTIONS, SERVER_PROFILE,
};
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn free_bind() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("127.0.0.1:{port}")
}

fn wait_for(bind: &str) {
    for _ in 0..80 {
        if TcpStream::connect(bind).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("server did not accept on {bind}");
}

fn spawn_server(path: std::path::PathBuf, bind: &str, options: ServeOptions) -> Arc<AtomicBool> {
    let shutdown = options
        .shutdown
        .clone()
        .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let flag = Arc::clone(&shutdown);
    let opts = if options.shutdown.is_some() {
        options
    } else {
        options.shutdown_flag(Arc::clone(&shutdown))
    };
    let bind_c = bind.to_string();
    thread::spawn(move || {
        let _ = serve_store_with(path, &bind_c, opts);
    });
    wait_for(bind);
    flag
}

/// Complete framed handshake; returns a buffered reader for subsequent frames.
fn framed_connect(bind: &str) -> (TcpStream, BufReader<TcpStream>) {
    let mut stream = TcpStream::connect(bind).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    client_handshake(&mut reader, &mut stream).expect("handshake");
    (stream, reader)
}

fn rpc_frame(stream: &mut TcpStream, reader: &mut BufReader<TcpStream>, json_body: &str) -> String {
    write_frame(stream, json_body.as_bytes()).unwrap();
    let bytes = read_frame(reader, DEFAULT_MAX_FRAME_BYTES)
        .unwrap()
        .expect("response frame");
    String::from_utf8(bytes).unwrap()
}

#[test]
fn server_profile_constant() {
    assert_eq!(SERVER_PROFILE, "residiuum-server-v1");
    assert_eq!(DEFAULT_MAX_CONNECTIONS, 64);
}

#[test]
fn concurrent_clients_progress_independently() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let mut db = Residiuum::open(&path).unwrap();
        db.collection("docs")
            .unwrap()
            .put("seed", &json!({"n": 0}))
            .unwrap();
    }

    let bind = free_bind();
    let shutdown = spawn_server(
        path,
        &bind,
        ServeOptions::new()
            .legacy_token_server()
            .max_connections(16),
    );

    // Hold an idle TCP connection open (slow client) without sending RPCs.
    let _slow = TcpStream::connect(&bind).expect("slow client connect");

    // Concurrent writers must still make progress (accept loop is not blocked).
    let mut handles = Vec::new();
    for i in 0..8 {
        let bind_c = bind.clone();
        handles.push(thread::spawn(move || {
            let url = format!("residiuum://{bind_c}/app");
            let mut db = Residiuum::connect(&url).expect("connect");
            db.collection("docs")
                .unwrap()
                .put(&format!("k{i}"), &json!({"i": i}))
                .unwrap();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let url = format!("residiuum://{bind}/app");
    let mut db = Residiuum::connect(&url).unwrap();
    let keys = db.collection("docs").unwrap().scan_keys().unwrap();
    assert!(keys.len() >= 9, "expected seed + 8 puts, got {keys:?}");

    shutdown.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(200));
}

#[test]
fn connection_limit_returns_resource_limit() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let _ = Residiuum::open(&path).unwrap();
    }

    let bind = free_bind();
    let shutdown = spawn_server(
        path,
        &bind,
        ServeOptions::new()
            .legacy_token_server()
            .server_limits(ServerLimits {
                max_connections: 2,
                idle_timeout: Duration::from_secs(30),
                drain_timeout: Duration::from_secs(5),
            }),
    );

    // Hold two live workers (confirmed via ping) so further connects overload.
    let mut held = Vec::new();
    for i in 0..2 {
        let (mut s, mut r) = framed_connect(&bind);
        let resp = rpc_frame(&mut s, &mut r, &format!(r#"{{"id":{i},"op":"ping"}}"#));
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["ok"], true, "held connection {i} must be a live worker");
        held.push(s);
    }

    // Retry a few times: reject is racy only if a slot frees mid-test.
    // Overload path emits an unsolicited framed handshake reject.
    let mut got = String::new();
    for _ in 0..10 {
        let over = TcpStream::connect(&bind).expect("overload connect");
        over.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let mut reader = BufReader::new(over);
        match read_frame(&mut reader, DEFAULT_MAX_FRAME_BYTES) {
            Ok(Some(bytes)) => {
                got = String::from_utf8(bytes).unwrap();
                break;
            }
            Ok(None) | Err(_) => {
                thread::sleep(Duration::from_millis(50));
                continue;
            }
        }
    }

    assert!(
        !got.is_empty(),
        "expected overload reject frame after holding max connections"
    );
    let v: serde_json::Value = serde_json::from_str(&got).expect("json overload");
    assert_eq!(v["msg"], "reject");
    assert_eq!(v["code"], "resource_limit");
    let err = v["error"].as_str().unwrap_or("");
    assert!(
        err.contains("connection limit") || err.contains("draining"),
        "unexpected error: {err}"
    );

    drop(held);
    shutdown.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(150));
}

#[test]
fn graceful_shutdown_drains_and_stops_accept() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let _ = Residiuum::open(&path).unwrap();
    }

    let bind = free_bind();
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = spawn_server(
        path,
        &bind,
        ServeOptions::new()
            .legacy_token_server()
            .max_connections(8)
            .drain_timeout(Duration::from_secs(5))
            .shutdown_flag(Arc::clone(&shutdown)),
    );
    assert!(Arc::ptr_eq(&flag, &shutdown));

    let url = format!("residiuum://{bind}/app");
    let mut db = Residiuum::connect(&url).unwrap();
    db.collection("docs")
        .unwrap()
        .put("before", &json!({"ok": true}))
        .unwrap();

    shutdown.store(true, Ordering::SeqCst);

    // Wait until the accept loop has exited (connect should fail).
    let start = Instant::now();
    let mut stopped = false;
    while start.elapsed() < Duration::from_secs(5) {
        thread::sleep(Duration::from_millis(50));
        // Server may still hold the port until drain completes; try an RPC on a
        // fresh connection — once fully stopped, connect fails.
        match TcpStream::connect(&bind) {
            Ok(mut s) => {
                s.set_read_timeout(Some(Duration::from_millis(200)))
                    .unwrap();
                let mut r = BufReader::new(s.try_clone().unwrap());
                // May get framed reject (draining/limit) or handshake/RPC failure.
                match client_handshake(&mut r, &mut s) {
                    Ok(_) => {
                        let _ = write_frame(&mut s, br#"{"id":1,"op":"ping"}"#);
                        match read_frame(&mut r, DEFAULT_MAX_FRAME_BYTES) {
                            Ok(Some(bytes)) => {
                                let line = String::from_utf8_lossy(&bytes);
                                if line.contains("draining") || line.contains("resource_limit") {
                                    continue;
                                }
                            }
                            Ok(None) | Err(_) => {
                                stopped = true;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("draining") || msg.contains("resource_limit") {
                            continue;
                        }
                        // Connection refused mid-handshake / closed ⇒ stopped.
                        stopped = true;
                        break;
                    }
                }
            }
            Err(_) => {
                stopped = true;
                break;
            }
        }
    }
    assert!(stopped, "server did not stop accepting after shutdown");
}

#[test]
fn single_store_owner_survives_many_clients() {
    // Opening Store::open per connection would fail with WriterLockHeld under
    // concurrency. This test hammers concurrent puts to ensure one owner works.
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let _ = Residiuum::open(&path).unwrap();
    }

    let bind = free_bind();
    let shutdown = spawn_server(
        path,
        &bind,
        ServeOptions::new()
            .legacy_token_server()
            .max_connections(32),
    );

    let mut handles = Vec::new();
    for i in 0..20 {
        let bind_c = bind.clone();
        handles.push(thread::spawn(move || {
            let url = format!("residiuum://{bind_c}/app");
            let mut db = Residiuum::connect(&url).unwrap();
            db.collection("c")
                .unwrap()
                .put(&format!("k{i}"), &json!({"i": i}))
                .unwrap();
            let got = db.collection("c").unwrap().get(&format!("k{i}")).unwrap();
            assert_eq!(got.unwrap()["i"], i);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    shutdown.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(150));
}

#[test]
fn remote_error_code_for_limit_is_stable() {
    // Sanity: ErrorCode string used on the wire matches DX surface.
    assert_eq!(ErrorCode::ResourceLimit.as_str(), "resource_limit");
}

#[test]
fn idle_timeout_is_configurable_on_options() {
    let opts = ServeOptions::new()
        .legacy_token_server()
        .idle_timeout(Duration::from_secs(3));
    assert_eq!(opts.server_limits.idle_timeout, Duration::from_secs(3));
}

/// Direct low-level check that an admitted worker answers ping while another
/// connection is idle (accept loop free + shared store).
#[test]
fn raw_ping_while_peer_idle() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let _ = Residiuum::open(&path).unwrap();
    }
    let bind = free_bind();
    let shutdown = spawn_server(path, &bind, ServeOptions::new().legacy_token_server());

    let _idle = TcpStream::connect(&bind).unwrap();
    let (mut active, mut reader) = framed_connect(&bind);
    let resp = rpc_frame(&mut active, &mut reader, r#"{"id":7,"op":"ping"}"#);
    let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["id"], 7);

    shutdown.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(100));
}
