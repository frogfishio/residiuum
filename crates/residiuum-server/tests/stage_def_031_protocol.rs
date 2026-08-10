//! DEF-031: versioned, framed network protocol.
//!
//! Covers handshake negotiation, clear legacy/old-version failures, oversized
//! frame refusal before allocation, diagnostic line profile, and golden
//! fixtures under `tests/fixtures/protocol/`.

use residiuum_sdk::{
    client_handshake, encode_frame, json, read_frame, write_frame, write_json_frame,
    write_reject_frame, ConnectOptions, ErrorCode, Handshake, HandshakeMsg, Residiuum,
    DEFAULT_MAX_FRAME_BYTES, FEATURE_JSON_RPC_V1, HANDSHAKE_MAX_FRAME_BYTES, PROTOCOL_MAJOR,
    PROTOCOL_PROFILE, REQUIRED_FEATURES, REQUIRED_WRITE_RECEIPT_FIELDS, RPC_WIRE_LABEL,
};

use residiuum_server::{serve_store_with, ServeOptions, SERVER_PROFILE};
use std::io::{BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/protocol")
}

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

fn spawn_server(path: PathBuf, bind: &str, options: ServeOptions) -> Arc<AtomicBool> {
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

#[test]
fn protocol_profile_constants() {
    assert_eq!(PROTOCOL_PROFILE, "residiuum-rpc-v1");
    assert_eq!(RPC_WIRE_LABEL, "1.0-draft");
    assert_eq!(PROTOCOL_MAJOR, 1);
    assert_eq!(SERVER_PROFILE, "residiuum-server-v1");
    assert!(REQUIRED_FEATURES.contains(&FEATURE_JSON_RPC_V1));
    assert!(REQUIRED_WRITE_RECEIPT_FIELDS.contains(&"event_id"));
    assert!(REQUIRED_WRITE_RECEIPT_FIELDS.contains(&"committed"));
}

#[test]
fn golden_fixtures_parse_and_frame() {
    let dir = fixtures_dir();
    for name in [
        "hello.v1.json",
        "welcome.v1.json",
        "reject_version.v1.json",
        "ping_request.v1.json",
        "ping_response.v1.json",
    ] {
        let raw = std::fs::read_to_string(dir.join(name)).unwrap_or_else(|e| {
            panic!("missing fixture {name}: {e}");
        });
        let v: serde_json::Value = serde_json::from_str(&raw).expect(name);
        assert!(v.is_object(), "{name} must be a JSON object");
        // Framing must not allocate beyond declared length.
        let framed = encode_frame(raw.trim().as_bytes()).unwrap();
        assert_eq!(
            u32::from_be_bytes(framed[..4].try_into().unwrap()) as usize,
            raw.trim().len()
        );
        let mut cur = std::io::Cursor::new(framed);
        let round = read_frame(&mut cur, DEFAULT_MAX_FRAME_BYTES)
            .unwrap()
            .unwrap();
        let again: serde_json::Value = serde_json::from_slice(&round).unwrap();
        assert_eq!(again, v, "fixture {name} frame round-trip");
    }

    // Structural checks on handshake fixtures.
    let hello: Handshake =
        serde_json::from_str(&std::fs::read_to_string(dir.join("hello.v1.json")).unwrap()).unwrap();
    assert_eq!(hello.msg, HandshakeMsg::Hello);
    assert_eq!(hello.v, 1);
    assert_eq!(hello.protocol_profile.as_deref(), Some(PROTOCOL_PROFILE));

    let welcome: Handshake =
        serde_json::from_str(&std::fs::read_to_string(dir.join("welcome.v1.json")).unwrap())
            .unwrap();
    assert_eq!(welcome.msg, HandshakeMsg::Welcome);
    let fields = welcome.required_receipt_fields.as_ref().unwrap();
    for f in REQUIRED_WRITE_RECEIPT_FIELDS {
        assert!(fields.iter().any(|x| x == *f), "missing receipt field {f}");
    }
}

#[test]
fn framed_handshake_and_ping_over_serve() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let _ = Residiuum::open(&path).unwrap();
    }
    let bind = free_bind();
    let shutdown = spawn_server(path, &bind, ServeOptions::new().legacy_token_server());

    let mut stream = TcpStream::connect(&bind).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let session = client_handshake(&mut reader, &mut stream).unwrap();
    assert_eq!(session.protocol_major, 1);
    assert!(session.has_feature(FEATURE_JSON_RPC_V1));
    assert!(session.max_frame <= DEFAULT_MAX_FRAME_BYTES);

    write_frame(&mut stream, br#"{"id":42,"op":"ping"}"#).unwrap();
    let resp = read_frame(&mut reader, session.max_frame).unwrap().unwrap();
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["id"], 42);

    // SDK client path also handshakes.
    let url = format!("residiuum://{bind}/app");
    let mut db = Residiuum::connect(&url).unwrap();
    db.collection("docs")
        .unwrap()
        .put("k", &json!({"n": 1}))
        .unwrap();

    shutdown.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(150));
}

#[test]
fn legacy_line_client_fails_clearly() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let _ = Residiuum::open(&path).unwrap();
    }
    let bind = free_bind();
    let shutdown = spawn_server(path, &bind, ServeOptions::new().legacy_token_server());

    let mut stream = TcpStream::connect(&bind).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    // Old client: raw line JSON without handshake.
    stream.write_all(br#"{"id":1,"op":"ping"}"#).unwrap();
    stream.write_all(b"\n").unwrap();
    stream.flush().unwrap();

    // Server detects leading `{` and either closes or sends a framed reject.
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    match read_frame(&mut reader, HANDSHAKE_MAX_FRAME_BYTES) {
        Ok(Some(bytes)) => {
            let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(v["msg"], "reject");
            let code = v["code"].as_str().unwrap_or("");
            assert!(
                code == "protocol_violation" || code == "protocol_version_unsupported",
                "code={code} body={}",
                String::from_utf8_lossy(&bytes)
            );
        }
        Ok(None) => {
            // Clean close after detecting legacy is acceptable.
        }
        Err(e) => {
            // Peer reset after reject is also fine.
            let s = e.to_string();
            assert!(
                s.contains("protocol")
                    || s.contains("closed")
                    || s.contains("reset")
                    || s.contains("Connection"),
                "unexpected error: {s}"
            );
        }
    }

    shutdown.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(100));
}

#[test]
fn unsupported_protocol_major_rejected() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let _ = Residiuum::open(&path).unwrap();
    }
    let bind = free_bind();
    let shutdown = spawn_server(path, &bind, ServeOptions::new().legacy_token_server());

    let mut stream = TcpStream::connect(&bind).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut hello = Handshake::hello();
    hello.v = 99;
    hello.protocol_major = Some(99);
    write_json_frame(&mut stream, &hello).unwrap();
    let bytes = read_frame(&mut reader, HANDSHAKE_MAX_FRAME_BYTES)
        .unwrap()
        .unwrap();
    let hs: Handshake = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(hs.msg, HandshakeMsg::Reject);
    assert_eq!(hs.code.as_deref(), Some("protocol_version_unsupported"));

    shutdown.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(100));
}

#[test]
fn oversized_frame_refused_without_full_read() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    {
        let _ = Residiuum::open(&path).unwrap();
    }
    let bind = free_bind();
    let shutdown = spawn_server(path, &bind, ServeOptions::new().legacy_token_server());

    let mut stream = TcpStream::connect(&bind).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    client_handshake(&mut reader, &mut stream).unwrap();

    // Claim a huge payload length; do not send the body.
    let huge = (DEFAULT_MAX_FRAME_BYTES as u32).saturating_add(1);
    stream.write_all(&huge.to_be_bytes()).unwrap();
    stream.flush().unwrap();

    // Server should answer with resource_limit (or close) without requiring
    // the client to push gigabytes.
    match read_frame(&mut reader, HANDSHAKE_MAX_FRAME_BYTES) {
        Ok(Some(bytes)) => {
            let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(v["ok"], false);
            assert_eq!(v["code"], "resource_limit");
        }
        Ok(None) => {}
        Err(e) => {
            assert!(
                e.code() == ErrorCode::ResourceLimit
                    || e.code() == ErrorCode::Io
                    || e.code() == ErrorCode::DeadlineExceeded
                    || e.code() == ErrorCode::ProtocolViolation,
                "unexpected: {e:?}"
            );
        }
    }

    shutdown.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(100));
}

#[test]
fn diagnostic_line_protocol_opt_in() {
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
            .diagnostic_line_protocol(true),
    );

    // Raw line client works only against diagnostic servers.
    let mut stream = TcpStream::connect(&bind).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(br#"{"id":9,"op":"ping"}"#).unwrap();
    stream.write_all(b"\n").unwrap();
    stream.flush().unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    use std::io::BufRead;
    reader.read_line(&mut line).unwrap();
    let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["id"], 9);

    // Matching diagnostic client options.
    let url = format!("residiuum://{bind}/app");
    let mut db =
        Residiuum::connect_with(&url, ConnectOptions::new().diagnostic_line_protocol(true))
            .unwrap();
    db.collection("c")
        .unwrap()
        .put("x", &json!({"ok": true}))
        .unwrap();

    shutdown.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(100));
}

#[test]
fn unsolicited_reject_frame_is_parseable() {
    // Overload path writes a framed reject without a prior hello.
    let mut buf = Vec::new();
    write_reject_frame(
        &mut buf,
        "resource_limit",
        "connection limit exceeded (max 2)",
    )
    .unwrap();
    let mut cur = std::io::Cursor::new(buf);
    let bytes = read_frame(&mut cur, HANDSHAKE_MAX_FRAME_BYTES)
        .unwrap()
        .unwrap();
    let hs: Handshake = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(hs.msg, HandshakeMsg::Reject);
    assert_eq!(hs.code.as_deref(), Some("resource_limit"));
}

#[test]
fn hello_fixture_matches_builder() {
    let dir = fixtures_dir();
    let fixture: Handshake =
        serde_json::from_str(&std::fs::read_to_string(dir.join("hello.v1.json")).unwrap()).unwrap();
    let built = Handshake::hello();
    assert_eq!(fixture.msg, built.msg);
    assert_eq!(fixture.v, built.v);
    assert_eq!(fixture.protocol_profile, built.protocol_profile);
    assert_eq!(fixture.wire_label, built.wire_label);
    let ff = fixture.features.as_ref().unwrap();
    let bf = built.features.as_ref().unwrap();
    for f in REQUIRED_FEATURES {
        assert!(ff.iter().any(|x| x == *f));
        assert!(bf.iter().any(|x| x == *f));
    }
}

#[test]
fn read_trait_eof_is_none() {
    let mut empty: &[u8] = &[];
    assert!(read_frame(&mut empty, 64).unwrap().is_none());
}
