//! DEF-060: structured process logging.
//!
//! Proves:
//! - stable event names + `residiuum-log-v1` profile
//! - RPC completion logs carry correlation fields (operation_id, request id,
//!   principal, guarantees, latency, error code)
//! - credentials and request/response payloads never appear in NDJSON
//! - connection rejects log `connection.rejected` without secrets
//! - end-to-end SDK traffic emits correlated `rpc.complete` lines

use residiuum_sdk::{
    client_handshake, json, read_frame, write_frame, ConnectOptions, Residiuum,
    DEFAULT_MAX_FRAME_BYTES,
};
use residiuum_server::{
    log_events, serve_store_with, AdmissionController, AdmissionLimits, LogSink, Logger,
    MemorySink, ServeOptions, LOG_PROFILE,
};
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_for_server(bind: &str) {
    for _ in 0..100 {
        if TcpStream::connect(bind).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("server did not accept on {bind}");
}

fn start_server(
    dir: &TempDir,
    logger: Arc<Logger>,
    options: ServeOptions,
) -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let store_path = dir.path().join("log.residiuum");
    drop(Residiuum::open(&store_path).unwrap());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let bind_c = bind.clone();
    let path_c = store_path.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    let opts = options
        .logger(logger)
        .shutdown_flag(Arc::clone(&stop))
        .suppress_startup_report(true);
    let handle = thread::spawn(move || {
        let _ = serve_store_with(path_c, &bind_c, opts);
    });
    wait_for_server(&bind);
    thread::sleep(Duration::from_millis(40));
    (bind, stop_c, handle)
}

fn framed_connect(bind: &str) -> (TcpStream, BufReader<TcpStream>) {
    let mut stream = TcpStream::connect(bind).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    client_handshake(&mut reader, &mut stream).expect("handshake");
    (stream, reader)
}

fn rpc_raw(
    stream: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    body: &serde_json::Value,
) -> serde_json::Value {
    write_frame(stream, body.to_string().as_bytes()).unwrap();
    let bytes = read_frame(reader, DEFAULT_MAX_FRAME_BYTES)
        .unwrap()
        .expect("response");
    serde_json::from_slice(&bytes).unwrap()
}

fn event_names(sink: &MemorySink) -> Vec<String> {
    sink.json_lines()
        .iter()
        .filter_map(|v| v["event"].as_str().map(|s| s.to_string()))
        .collect()
}

fn assert_no_payload_or_secret_keys(v: &serde_json::Value) {
    for forbidden in [
        "token",
        "json",
        "value",
        "bytes_b64",
        "password",
        "secret",
        "authorization",
        "auth_token",
    ] {
        assert!(
            v.get(forbidden).is_none(),
            "log line must not contain payload/secret key {forbidden}: {v}"
        );
    }
    let line = v.to_string();
    assert!(
        !line.contains("super-secret-token-value"),
        "credential leaked into log: {line}"
    );
    assert!(
        !line.contains("123-45-6789"),
        "document payload leaked into log: {line}"
    );
}

#[test]
fn profile_and_stable_event_names() {
    assert_eq!(LOG_PROFILE, "residiuum-log-v1");
    assert_eq!(log_events::RPC_COMPLETE, "rpc.complete");
    assert_eq!(log_events::GUARANTEE_FAILED, "guarantee.failed");
    assert_eq!(log_events::SERVER_START, "server.start");
    assert_eq!(log_events::CONNECTION_REJECTED, "connection.rejected");
}

#[test]
fn put_rpc_emits_correlated_ndjson_without_payloads() {
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(MemorySink::new(64));
    let logger = Logger::with_sink(Arc::clone(&sink) as Arc<dyn LogSink>)
        .store(dir.path().join("log.residiuum").display().to_string())
        .mode("serve")
        .shared();

    let secret = "super-secret-token-value";
    let (bind, stop, handle) = start_server(
        &dir,
        logger,
        ServeOptions::new().legacy_token_server().auth_token(secret),
    );

    let op_id = "aabbccddeeff00112233445566778899";
    {
        let (mut stream, mut reader) = framed_connect(&bind);
        let req = json!({
            "id": 7u64,
            "op": "put",
            "collection": "docs",
            "key": "k1",
            "json": {"ssn": "123-45-6789", "name": "alice"},
            "durability": "durable",
            "token": secret,
            "operation_id": op_id,
        });
        let resp = rpc_raw(&mut stream, &mut reader, &req);
        assert_eq!(resp["ok"], true, "put failed: {resp}");
        assert_eq!(resp["id"], 7);
    }

    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();

    let lines = sink.json_lines();
    assert!(
        !lines.is_empty(),
        "expected structured log lines from serve path"
    );

    let rpc = lines
        .iter()
        .find(|v| v["event"] == log_events::RPC_COMPLETE && v["op"] == "put")
        .unwrap_or_else(|| {
            panic!(
                "missing rpc.complete for put; events={:?}",
                event_names(&sink)
            )
        });

    assert_eq!(rpc["profile"], LOG_PROFILE);
    assert_eq!(rpc["request_id"], 7);
    assert_eq!(rpc["operation_id"], op_id);
    assert_eq!(rpc["collection"], "docs");
    assert_eq!(rpc["ok"], true);
    assert_eq!(rpc["guarantee_requested"], "durable");
    // Achieved guarantee / committed come from the receipt when present.
    assert!(rpc.get("latency_ms").and_then(|x| x.as_u64()).is_some());
    assert_eq!(rpc["mode"], "serve");
    assert!(rpc["store"].as_str().is_some());
    // Principal is the synthetic superuser label for shared-token auth.
    assert!(rpc.get("principal_id").is_some());

    for v in &lines {
        assert_no_payload_or_secret_keys(v);
    }
}

#[test]
fn auth_failure_logs_error_code_not_token() {
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(MemorySink::new(32));
    let logger = Logger::with_sink(Arc::clone(&sink) as Arc<dyn LogSink>)
        .mode("serve")
        .shared();

    let (bind, stop, handle) = start_server(
        &dir,
        logger,
        ServeOptions::new()
            .legacy_token_server()
            .auth_token("expected-token"),
    );

    {
        let (mut stream, mut reader) = framed_connect(&bind);
        let req = json!({
            "id": 3u64,
            "op": "ping",
            "token": "super-secret-token-value",
        });
        let resp = rpc_raw(&mut stream, &mut reader, &req);
        assert_eq!(resp["ok"], false);
    }

    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();

    let rpc = sink
        .json_lines()
        .into_iter()
        .find(|v| v["event"] == log_events::RPC_COMPLETE)
        .expect("rpc.complete for failed auth");
    assert_eq!(rpc["ok"], false);
    assert!(rpc["error_code"].as_str().is_some());
    assert_no_payload_or_secret_keys(&rpc);
}

#[test]
fn connection_churn_reject_emits_connection_rejected() {
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(MemorySink::new(32));
    let logger = Logger::with_sink(Arc::clone(&sink) as Arc<dyn LogSink>)
        .mode("serve")
        .shared();

    // Small window budget; wait_for_server + probes will exhaust it.
    let mut limits = AdmissionLimits::draft_defaults();
    limits.max_connects_per_window = 2;
    limits.connect_window = Duration::from_secs(60);
    let admission = AdmissionController::new(limits);

    let store_path = dir.path().join("log.residiuum");
    drop(Residiuum::open(&store_path).unwrap());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let bind_c = bind.clone();
    let path_c = store_path.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .admission(admission)
        .logger(logger)
        .shutdown_flag(Arc::clone(&stop))
        .suppress_startup_report(true);
    let handle = thread::spawn(move || {
        let _ = serve_store_with(path_c, &bind_c, opts);
    });
    wait_for_server(&bind);
    thread::sleep(Duration::from_millis(40));

    // Burn remaining admission slots; at least one accept must hit churn reject.
    for _ in 0..6 {
        match TcpStream::connect(&bind) {
            Ok(s) => {
                // Brief hold so accept loop processes the fd before drop.
                thread::sleep(Duration::from_millis(30));
                drop(s);
            }
            Err(_) => break,
        }
    }
    thread::sleep(Duration::from_millis(80));

    stop_c.store(true, Ordering::SeqCst);
    let _ = handle.join();

    let names = event_names(&sink);
    assert!(
        names.iter().any(|n| n == log_events::CONNECTION_REJECTED),
        "expected connection.rejected; got {names:?}"
    );
    for v in sink.json_lines() {
        assert_no_payload_or_secret_keys(&v);
        if v["event"] == log_events::CONNECTION_REJECTED {
            assert_eq!(v["error_code"], "resource_limit");
        }
    }
}

#[test]
fn sdk_put_get_emits_rpc_complete() {
    // End-to-end through the public SDK client.
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(MemorySink::new(64));
    let logger = Logger::with_sink(Arc::clone(&sink) as Arc<dyn LogSink>)
        .store("sdk-e2e-store")
        .mode("serve")
        .shared();

    let (bind, stop, handle) =
        start_server(&dir, logger, ServeOptions::new().legacy_token_server());

    let uri = format!("residiuum://{bind}/app");
    let mut db = Residiuum::connect_with(&uri, ConnectOptions::new()).expect("connect");
    let mut col = db.collection("docs").unwrap();
    col.put("item", &json!({"n": 1})).unwrap();
    let got = col.get("item").unwrap();
    assert!(got.is_some());

    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();

    let puts: Vec<_> = sink
        .json_lines()
        .into_iter()
        .filter(|v| v["event"] == log_events::RPC_COMPLETE && v["op"] == "put")
        .collect();
    assert!(!puts.is_empty(), "expected put rpc.complete lines");
    for v in puts {
        assert_eq!(v["profile"], LOG_PROFILE);
        assert_eq!(v["ok"], true);
        assert_eq!(v["collection"], "docs");
        assert_eq!(v["store"], "sdk-e2e-store");
        // SDK clients always send operation_id on mutations.
        assert!(
            v["operation_id"].as_str().is_some(),
            "put should log operation_id for correlation: {v}"
        );
        assert_no_payload_or_secret_keys(&v);
    }
}
