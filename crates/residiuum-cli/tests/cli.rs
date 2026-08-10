//! Stage 7 CLI integration tests: put/get/list, doctor, salvage, serve+connect.

use residiuum_sdk::{client_handshake, read_frame, write_frame, DEFAULT_MAX_FRAME_BYTES};
use std::fs;
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

/// Wait until a residiuum serve process completes a framed handshake + ping.
///
/// A bare TCP connect is not enough: production servers (DEF-031) require the
/// `residiuum-rpc-v1` hello/welcome exchange before application RPCs. Any framed
/// JSON response (including auth failure on a later store_info) means live.
fn wait_for_residiuum_ping(bind: &str) {
    for _ in 0..100 {
        if let Ok(mut stream) = TcpStream::connect(bind) {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(400)));
            let _ = stream.set_write_timeout(Some(Duration::from_millis(400)));
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            if client_handshake(&mut reader, &mut stream).is_ok()
                && write_frame(&mut stream, br#"{"id":1,"op":"ping"}"#).is_ok()
            {
                if let Ok(Some(bytes)) = read_frame(&mut reader, DEFAULT_MAX_FRAME_BYTES) {
                    if bytes.contains(&b'{') {
                        return;
                    }
                }
            }
        }
        thread::sleep(Duration::from_millis(30));
    }
    panic!("residiuum serve did not answer ping on {bind}");
}

fn residiuum_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_residiuum"))
}

fn run_ok(args: &[&str]) -> String {
    let output = residiuum_bin().args(args).output().expect("run residiuum");
    assert!(
        output.status.success(),
        "cmd {:?} failed\nstdout:\n{}\nstderr:\n{}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn license_prints_notice() {
    let out = run_ok(&["--license"]);
    assert!(out.contains("Alexander R. Croft"), "license={out}");
    assert!(
        out.contains("AGPL") || out.contains("Affero"),
        "residiuum CLI must advertise AGPL, license={out}"
    );
    assert!(
        !out.contains("MIT License") || out.contains("multi-licensed"),
        "residiuum must not claim pure MIT, license={out}"
    );
}

#[test]
fn version_and_help() {
    let out = run_ok(&["--version"]);
    assert!(out.contains("-build"), "version={out}");
    let help = run_ok(&["--help"]);
    assert!(help.contains("doctor"));
    assert!(help.contains("salvage"));
    assert!(help.contains("backup"));
    assert!(help.contains("restore"));
    assert!(help.contains("migrate"));
    assert!(help.contains("scrub"));
    assert!(help.contains("config"));
    assert!(help.contains("serve"));
    assert!(help.contains("serve-cluster"));
    assert!(
        help.contains("experimental") || help.contains("development"),
        "top-level help should qualify serve maturity, help={help}"
    );
    let serve_help = run_ok(&["serve", "--help"]);
    assert!(
        serve_help.contains("allow-insecure-bind"),
        "serve help={serve_help}"
    );
    assert!(
        serve_help.contains("config"),
        "serve should accept --config (DEF-054), help={serve_help}"
    );
    assert!(
        serve_help.contains("legacy-token-server"),
        "serve should document --legacy-token-server (HAR-4), help={serve_help}"
    );
    assert!(
        serve_help.contains("qualified-heap-key"),
        "serve should document --qualified-heap-key (HAR-4), help={serve_help}"
    );
    let sc_help = run_ok(&["serve-cluster", "--help"]);
    assert!(
        sc_help.contains("experimental-network-cluster"),
        "serve-cluster help={sc_help}"
    );
    let cfg_help = run_ok(&["config", "--help"]);
    assert!(cfg_help.contains("validate"), "config help={cfg_help}");
    assert!(cfg_help.contains("show"), "config help={cfg_help}");
}

#[test]
fn put_get_list_delete_roundtrip() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("app.residiuum");
    let store_s = store.to_str().unwrap();

    run_ok(&[
        "put",
        store_s,
        "users/user-42",
        "--json",
        r#"{"name":"Alice","status":"active"}"#,
    ]);
    let got = run_ok(&["get", store_s, "users/user-42"]);
    assert!(got.contains("Alice"));

    let list_cols = run_ok(&["list", store_s]);
    assert!(list_cols.contains("users"));

    let list_keys = run_ok(&["list", store_s, "users"]);
    assert!(list_keys.contains("user-42"));

    let json_get = run_ok(&["--json-out", "get", store_s, "users/user-42"]);
    assert!(json_get.contains(r#""found":true"#) || json_get.contains(r#""found": true"#));

    run_ok(&["delete", store_s, "users/user-42"]);
    let missing = residiuum_bin()
        .args(["get", store_s, "users/user-42"])
        .output()
        .unwrap();
    assert!(!missing.status.success());
}

#[test]
fn put_bytes_roundtrip() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("app.residiuum");
    let store_s = store.to_str().unwrap();
    let bin = dir.path().join("blob.bin");
    fs::write(&bin, b"\x00\xffhello").unwrap();

    run_ok(&[
        "put-bytes",
        store_s,
        "artifacts/build-19",
        bin.to_str().unwrap(),
    ]);
    // list keys
    let keys = run_ok(&["list", store_s, "artifacts"]);
    assert!(keys.contains("build-19"));
}

#[test]
fn doctor_is_read_only_on_healthy_store() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("app.residiuum");
    let store_s = store.to_str().unwrap();
    run_ok(&["put", store_s, "users/u1", "--json", r#"{"n":1}"#]);

    // Snapshot mtimes of authoritative paths.
    let active = store.join("active").join("active.residiuum");
    let before = fs::metadata(&active).unwrap().modified().unwrap();

    let out = run_ok(&["doctor", store_s]);
    assert!(out.contains("read_only: true"));
    assert!(out.contains("healthy: true"));
    // DEF-102: primary.idx is derived; doctor classifies without size-as-health.
    assert!(
        out.contains("primary_cache:"),
        "doctor should report primary_cache"
    );
    assert!(out.contains("authoritative=false"));
    assert!(out.contains("lifecycle:"), "doctor should report lifecycle");

    let after = fs::metadata(&active).unwrap().modified().unwrap();
    assert_eq!(before, after, "doctor must not rewrite active segment");
}

#[test]
fn salvage_to_new_path_preserves_live_data() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("damaged.residiuum");
    let dst = dir.path().join("recovered.residiuum");
    let src_s = src.to_str().unwrap();
    let dst_s = dst.to_str().unwrap();

    run_ok(&["put", src_s, "users/alice", "--json", r#"{"name":"Alice"}"#]);
    run_ok(&["put", src_s, "users/bob", "--json", r#"{"name":"Bob"}"#]);

    // Wipe derived state on source (catalogs/indexes) — salvage must still work.
    for name in ["catalogs", "indexes", "snapshots"] {
        let p = src.join(name);
        if p.exists() {
            fs::remove_dir_all(&p).unwrap();
        }
    }

    let report = run_ok(&["salvage", src_s, "--output", dst_s]);
    assert!(report.contains("source immutable: true"));
    assert!(report.contains("subjects_copied: 2"));
    assert!(report.contains("evidence"));
    assert!(report.contains("frames_copied:"));
    assert!(report.contains("manifest:"));

    // Source still readable via open (segments intact).
    let src_get = run_ok(&["get", src_s, "users/alice"]);
    assert!(src_get.contains("Alice"));

    // Destination has recovered values.
    let dst_get = run_ok(&["get", dst_s, "users/bob"]);
    assert!(dst_get.contains("Bob"));
}

#[test]
fn export_live_is_distinct_from_salvage() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src.residiuum");
    let dst = dir.path().join("export.residiuum");
    let src_s = src.to_str().unwrap();
    let dst_s = dst.to_str().unwrap();

    run_ok(&["put", src_s, "users/alice", "--json", r#"{"name":"Alice"}"#]);
    let report = run_ok(&["export-live", src_s, "--output", dst_s]);
    assert!(report.contains("live_state_export") || report.contains("subjects_copied: 1"));
    assert!(report.contains("frames_copied: 0"));
    let dst_get = run_ok(&["get", dst_s, "users/alice"]);
    assert!(dst_get.contains("Alice"));
}

#[test]
fn backup_and_restore_roundtrip() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("app.residiuum");
    let bak = dir.path().join("app.bak");
    let dst = dir.path().join("restored.residiuum");
    let src_s = src.to_str().unwrap();
    let bak_s = bak.to_str().unwrap();
    let dst_s = dst.to_str().unwrap();

    run_ok(&["put", src_s, "users/alice", "--json", r#"{"name":"Alice"}"#]);
    run_ok(&["put", src_s, "users/bob", "--json", r#"{"name":"Bob"}"#]);

    let report = run_ok(&["backup", src_s, "--output", bak_s]);
    assert!(report.contains("full_backup"), "report={report}");
    assert!(report.contains("files_copied:"), "report={report}");
    assert!(
        report.contains("flushed_exclusive") || report.contains("on_disk_inspect"),
        "report={report}"
    );
    assert!(bak.join("backup-manifest.v1.json").is_file());

    let restore = run_ok(&["restore", bak_s, "--output", dst_s]);
    assert!(restore.contains("restore"), "restore={restore}");
    assert!(restore.contains("live_subjects: 2"), "restore={restore}");
    assert!(
        restore.contains("identity_reassigned: false"),
        "restore={restore}"
    );

    let alice = run_ok(&["get", dst_s, "users/alice"]);
    assert!(alice.contains("Alice"), "alice={alice}");
    let bob = run_ok(&["get", dst_s, "users/bob"]);
    assert!(bob.contains("Bob"), "bob={bob}");
}

#[test]
fn restore_reassign_identity_clone() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("app.residiuum");
    let bak = dir.path().join("app.bak");
    let clone = dir.path().join("clone.residiuum");
    let src_s = src.to_str().unwrap();
    let bak_s = bak.to_str().unwrap();
    let clone_s = clone.to_str().unwrap();

    run_ok(&["put", src_s, "k/x", "--json", r#"{"v":1}"#]);
    run_ok(&["backup", src_s, "--output", bak_s]);
    let restore = run_ok(&["restore", bak_s, "--output", clone_s, "--reassign-identity"]);
    assert!(
        restore.contains("identity_reassigned: true"),
        "restore={restore}"
    );
    let got = run_ok(&["get", clone_s, "k/x"]);
    assert!(got.contains(r#""v":1"#) || got.contains("1"), "got={got}");
}

#[test]
fn scrub_clean_store_and_status() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("app.residiuum");
    let store_s = store.to_str().unwrap();

    run_ok(&["put", store_s, "users/a", "--json", r#"{"n":1}"#]);
    run_ok(&["put", store_s, "users/b", "--json", r#"{"n":2}"#]);

    let report = run_ok(&["scrub", store_s]);
    assert!(report.contains("scrub"), "report={report}");
    assert!(
        report.contains("cycle_completed: true") || report.contains("coverage_ratio: 1"),
        "report={report}"
    );
    assert!(report.contains("open_findings: 0"), "report={report}");

    let status = run_ok(&["scrub", store_s, "--status"]);
    assert!(status.contains("scrub_status"), "status={status}");
    assert!(status.contains("bytes_verified_total:"), "status={status}");

    let paused = run_ok(&["scrub", store_s, "--pause"]);
    assert!(paused.contains("paused: true"), "paused={paused}");
    let resumed = run_ok(&["scrub", store_s, "--resume"]);
    assert!(resumed.contains("paused: false"), "resumed={resumed}");
}

#[test]
fn config_validate_show_and_unsafe_reject() {
    let dir = tempdir().unwrap();
    let good = dir.path().join("good.json");
    fs::write(
        &good,
        r#"{
          "format": "residiuum-config-v1",
          "format_version": 1,
          "store": { "path": "/tmp/residiuum-store", "durability_default": "durable" },
          "serve": {
            "bind": "127.0.0.1:7434",
            "max_connections": 8,
            "token_env": "RESIDIUUM_TEST_CLI_NO_TOKEN",
            "admission": { "global_max_rps": 50 }
          }
        }"#,
    )
    .unwrap();
    let good_s = good.to_str().unwrap();

    let out = run_ok(&["config", "validate", good_s, "--mode", "serve"]);
    assert!(out.contains("config ok"), "out={out}");
    assert!(
        out.contains("residiuum-config-v1") || out.contains("profile="),
        "out={out}"
    );

    let show = run_ok(&["--json-out", "config", "show", good_s, "--mode", "serve"]);
    assert!(show.contains("residiuum-config-v1"), "show={show}");
    assert!(
        show.contains("[redacted]") || show.contains("<unset>"),
        "show={show}"
    );
    assert!(show.contains("serve.bind"), "show={show}");

    let bad = dir.path().join("bad.json");
    fs::write(
        &bad,
        r#"{
          "format": "residiuum-config-v1",
          "format_version": 1,
          "cluster": {
            "root": "/tmp/c",
            "expected_node_count": 1,
            "claim_replication": true
          },
          "serve": { "experimental_network_cluster": true }
        }"#,
    )
    .unwrap();
    let bad_s = bad.to_str().unwrap();
    let output = residiuum_bin()
        .args(["config", "validate", bad_s, "--mode", "serve-cluster"])
        .output()
        .expect("run");
    assert!(
        !output.status.success(),
        "unsafe config must fail validation"
    );
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        err.contains("unsafe") || err.contains("replication"),
        "stderr={err}"
    );
}

#[test]
fn migrate_roundtrip_and_status() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("app.residiuum");
    let dst = dir.path().join("migrated.residiuum");
    let src_s = src.to_str().unwrap();
    let dst_s = dst.to_str().unwrap();

    run_ok(&["put", src_s, "users/alice", "--json", r#"{"name":"Alice"}"#]);
    run_ok(&["put", src_s, "users/bob", "--json", r#"{"name":"Bob"}"#]);

    let pre = run_ok(&["migrate", src_s, "--output", dst_s, "--preflight"]);
    assert!(pre.contains("migrate_preflight"), "pre={pre}");
    assert!(pre.contains("dest_ok: true"), "pre={pre}");

    let report = run_ok(&["migrate", src_s, "--output", dst_s]);
    assert!(report.contains("migrate"), "report={report}");
    assert!(report.contains("phase: done"), "report={report}");
    assert!(
        report.contains("verified_live_subjects: 2"),
        "report={report}"
    );

    let alice = run_ok(&["get", dst_s, "users/alice"]);
    assert!(alice.contains("Alice"), "alice={alice}");
    // Source still readable.
    let bob_src = run_ok(&["get", src_s, "users/bob"]);
    assert!(bob_src.contains("Bob"), "bob_src={bob_src}");

    let status = run_ok(&["migrate", src_s, "--status"]);
    assert!(status.contains("migrate_status"), "status={status}");
    assert!(status.contains("phase: done"), "status={status}");
}

#[test]
fn serve_and_sdk_connect_parity() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("app.residiuum");
    let store_s = store.to_str().unwrap();

    // Seed via CLI.
    run_ok(&["put", store_s, "users/seed", "--json", r#"{"from":"cli"}"#]);

    // Pick a free port.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let bind = format!("127.0.0.1:{port}");

    let mut child = residiuum_bin()
        .args(["serve", "--legacy-token-server", store_s, "--bind", &bind])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn residiuum serve");

    wait_for_residiuum_ping(&bind);

    let url = format!("residiuum://{bind}/app");
    let mut db = residiuum_sdk::Residiuum::connect(&url).expect("connect");
    assert!(db.is_remote());

    {
        let mut users = db.collection("users").unwrap();
        let seed = users.get("seed").unwrap().unwrap();
        assert_eq!(seed["from"], "cli");
        users
            .put("remote", &serde_json::json!({"from": "sdk"}))
            .unwrap();
        assert_eq!(users.get("remote").unwrap().unwrap()["from"], "sdk");
    }

    // Stop the server so exclusive writer ownership is released (DEF-020),
    // then reopen embedded and confirm the remote write is durable.
    let _ = child.kill();
    let _ = child.wait();

    let mut local = residiuum_sdk::Residiuum::open(&store).unwrap();
    let v = local
        .collection("users")
        .unwrap()
        .get("remote")
        .unwrap()
        .unwrap();
    assert_eq!(v["from"], "sdk");
}

#[test]
fn history_command() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("app.residiuum");
    let store_s = store.to_str().unwrap();
    run_ok(&["put", store_s, "docs/k", "--json", r#"{"v":1}"#]);
    run_ok(&["put", store_s, "docs/k", "--json", r#"{"v":2}"#]);
    let hist = run_ok(&["history", store_s, "docs/k"]);
    assert!(hist.contains("history"));
    assert!(hist.contains("versions="));
}

#[test]
fn serve_auth_token_required() {
    use residiuum_sdk::{ConnectOptions, ErrorCode};

    let dir = tempdir().unwrap();
    let store = dir.path().join("app.residiuum");
    let store_s = store.to_str().unwrap();
    run_ok(&["put", store_s, "users/seed", "--json", r#"{"from":"cli"}"#]);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let bind = format!("127.0.0.1:{port}");

    let mut child = residiuum_bin()
        .args([
            "serve",
            "--legacy-token-server",
            store_s,
            "--bind",
            &bind,
            "--token",
            "s3cret",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn residiuum serve");

    wait_for_residiuum_ping(&bind);

    let url = format!("residiuum://{bind}/app");

    // No token → authentication_failed.
    let err = match residiuum_sdk::Residiuum::connect(&url) {
        Ok(_) => panic!("must reject missing token"),
        Err(e) => e,
    };
    assert_eq!(err.code(), ErrorCode::AuthenticationFailed);

    // Wrong token → authentication_failed.
    let err = match residiuum_sdk::Residiuum::connect_with(
        &url,
        ConnectOptions::new().auth_token("wrong"),
    ) {
        Ok(_) => panic!("must reject wrong token"),
        Err(e) => e,
    };
    assert_eq!(err.code(), ErrorCode::AuthenticationFailed);

    // Correct token → put/get works; receipts carry non-zero event ids.
    let mut db = residiuum_sdk::Residiuum::connect_with(
        &url,
        ConnectOptions::new()
            .auth_token("s3cret")
            .max_connect_attempts(5)
            .request_timeout(Duration::from_secs(5)),
    )
    .expect("connect with token");
    assert_ne!(db.store_id(), [0u8; 16]);
    {
        let mut users = db.collection("users").unwrap();
        let receipt = users
            .put("authed", &serde_json::json!({"ok": true}))
            .unwrap();
        assert_ne!(receipt.event_id, [0u8; 16]);
        assert_ne!(receipt.store_id, [0u8; 16]);
        assert_eq!(users.get("authed").unwrap().unwrap()["ok"], true);
    }

    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn connect_retry_deadline_to_closed_port() {
    use residiuum_sdk::{ConnectOptions, ErrorCode};
    use std::time::Instant;

    // Nothing listening: connect should fail after retries within a deadline.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let url = format!("residiuum://127.0.0.1:{port}/app");
    let t0 = Instant::now();
    let err = match residiuum_sdk::Residiuum::connect_with(
        &url,
        ConnectOptions::new()
            .connect_timeout(Duration::from_millis(50))
            .max_connect_attempts(3)
            .retry_backoff(Duration::from_millis(10)),
    ) {
        Ok(_) => panic!("closed port must fail"),
        Err(e) => e,
    };
    let elapsed = t0.elapsed();
    // Three 50ms attempts + small backoffs should finish well under 5s.
    assert!(elapsed < Duration::from_secs(5), "elapsed={elapsed:?}");
    // IO or deadline depending on platform timeout reporting.
    assert!(
        matches!(
            err.code(),
            ErrorCode::Io | ErrorCode::DeadlineExceeded | ErrorCode::Internal
        ),
        "code={:?}",
        err.code()
    );
}

#[test]
fn serve_cluster_missing_root_fails_fast() {
    let dir = tempdir().unwrap();
    let missing = dir.path().join("no-such-cluster");
    let out = residiuum_bin()
        .args([
            "serve-cluster",
            missing.to_str().unwrap(),
            "--node",
            "0",
            "--bind",
            "127.0.0.1:0",
            "--experimental-network-cluster",
        ])
        .output()
        .expect("run serve-cluster");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("cluster") || err.contains("error"),
        "stderr={err}"
    );
}

#[test]
fn serve_cluster_requires_experimental_flag() {
    let dir = tempdir().unwrap();
    let out = residiuum_bin()
        .args([
            "serve-cluster",
            dir.path().to_str().unwrap(),
            "--node",
            "0",
            "--bind",
            "127.0.0.1:0",
        ])
        .output()
        .expect("run serve-cluster without flag");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("experimental-network-cluster") || err.contains("DEF-002"),
        "stderr={err}"
    );
}

#[test]
fn serve_refuses_public_plaintext_bind_without_override() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("app.residiuum");
    let store_s = store.to_str().unwrap();
    // Create a store first so failure is about bind policy, not missing path.
    run_ok(&["put", store_s, "t/k", "--json", r#"{"ok":true}"#]);
    let out = residiuum_bin()
        .args([
            "serve",
            "--legacy-token-server",
            store_s,
            "--bind",
            "0.0.0.0:17434",
        ])
        .output()
        .expect("run serve with public bind");
    assert!(!out.status.success(), "public bind must fail closed");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("non-loopback")
            || err.contains("allow-insecure-bind")
            || err.contains("DEF-002"),
        "stderr={err}"
    );
}

#[test]
fn serve_loopback_bind_is_allowed() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("app.residiuum");
    let store_s = store.to_str().unwrap();
    run_ok(&["put", store_s, "t/k", "--json", r#"{"ok":true}"#]);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let bind = format!("127.0.0.1:{port}");

    let mut child = residiuum_bin()
        .args(["serve", "--legacy-token-server", store_s, "--bind", &bind])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn residiuum serve");
    wait_for_residiuum_ping(&bind);

    // Startup report must not claim network quorum durability.
    // Kill after a successful ping; capture any stderr already written.
    let _ = child.kill();
    let output = child.wait_with_output().expect("wait serve");
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        err.contains("startup status") || err.contains("replication: none"),
        "expected structured startup report, stderr={err}"
    );
    assert!(
        !err.to_lowercase().contains("replicated durability"),
        "must not claim replicated durability, stderr={err}"
    );
}

#[test]
fn capability_matrix_document_present() {
    // DEF-001: release-facing capability matrix must stay in-tree.
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let matrix = root.join("doc/wip/status/CAPABILITY_MATRIX.md");
    let text = fs::read_to_string(&matrix).expect("doc/wip/status/CAPABILITY_MATRIX.md must exist");
    assert!(
        text.contains("Experimental network cluster is not production")
            || text.contains("not a production release claim")
            || text.contains("do not provide replicated durability")
            || text
                .contains("Three `serve-cluster` processes do not provide replicated durability"),
        "matrix must keep honest maturity labels for network multi-node"
    );
    assert!(text.contains("DEF-002") || text.contains("allow-insecure-bind"));
    let readme = fs::read_to_string(root.join("README.md")).expect("README");
    assert!(
        readme.contains("Not production-ready")
            || readme.contains("not production-ready")
            || readme.contains("not yet production-ready"),
        "README must state production maturity honestly"
    );
    assert!(
        !readme.contains("Extreme speed"),
        "README must not lead with unqualified extreme-speed marketing"
    );
}

#[test]
fn serve_public_bind_allowed_with_insecure_override() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("app.residiuum");
    let store_s = store.to_str().unwrap();
    run_ok(&["put", store_s, "t/k", "--json", r#"{"ok":true}"#]);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    // Bind all interfaces on an ephemeral port with explicit override (DEF-002).
    let bind = format!("0.0.0.0:{port}");

    let mut child = residiuum_bin()
        .args([
            "serve",
            "--legacy-token-server",
            store_s,
            "--bind",
            &bind,
            "--allow-insecure-bind",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn residiuum serve with insecure bind");
    // Connect via loopback to the same port.
    let connect = format!("127.0.0.1:{port}");
    wait_for_residiuum_ping(&connect);
    let _ = child.kill();
    let output = child.wait_with_output().expect("wait serve");
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        err.contains("allow_insecure_bind: yes") || err.contains("development override"),
        "stderr={err}"
    );
}

#[test]
fn serve_cluster_advertises_placement_and_endpoints() {
    use residiuum_sdk::{ClusterConfig, RemoteClient};
    use residiuum_server::{serve_cluster_node, ServeOptions};

    let dir = tempdir().unwrap();
    let root = dir.path().join("c");
    // Build a 3-node cluster on disk, then serve node 0 in-process over TCP.
    let mut db = residiuum_sdk::Residiuum::create_cluster(
        ClusterConfig::dependable_local(&root).with_virtual_partitions(8),
    )
    .expect("create cluster");
    db.collection("users")
        .unwrap()
        .put("seed", &serde_json::json!({"n": 1}))
        .unwrap();
    drop(db);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let bind = format!("127.0.0.1:{port}");

    let root_thread = root.clone();
    let bind_thread = bind.clone();
    let handle = thread::spawn(move || {
        let _ = serve_cluster_node(
            &root_thread,
            0,
            &bind_thread,
            ServeOptions::new()
                .legacy_token_server()
                .experimental_network_cluster(true),
        );
    });

    wait_for_residiuum_ping(&bind);

    let url = format!("residiuum://{bind}/c");
    let mut client = RemoteClient::connect(&bind, url.clone()).expect("connect to cluster node");
    let snap = client.fetch_directory().expect("directory");
    assert_eq!(snap.virtual_partitions, 8);
    assert_eq!(snap.assignments.len(), 8);
    assert!(snap.assignments.iter().all(|a| a.replicas.len() == 3));
    assert_eq!(
        snap.endpoints.get(&0).map(String::as_str),
        Some(bind.as_str()),
        "endpoints={:?}",
        snap.endpoints
    );
    // Ping/get on the same open connection (server is single-threaded).
    assert!(client.store_info().is_ok());
    drop(client);

    let ep = fs::read_to_string(root.join("endpoints.json")).unwrap();
    assert!(ep.contains(&bind));

    // Second client after the first connection is closed.
    let mut db = residiuum_sdk::Residiuum::connect(&url).unwrap();
    assert!(db.is_remote());
    let _ = db.collection("users").unwrap().get("seed");
    drop(db);

    // serve_cluster_node loops forever; thread ends with the test process.
    let _ = handle.thread().id();
}
