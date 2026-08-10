//! DEF-033: authorization privileges + tamper-evident audit (no secret leakage).

use residiuum_sdk::{
    client_handshake, json, read_frame, write_frame, ConnectOptions, Error, ErrorCode, Residiuum,
};

use residiuum_server::{
    serve_store_with, AuditDecision, AuditLog, AuthzPolicy, Privilege, PrivilegeSet, ServeOptions,
    AUTHZ_PROFILE, FORCE_RECONFIG_CONFIRM, PURGE_CONFIRM,
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
    options: ServeOptions,
) -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let store_path = dir.path().join("authz.residiuum");
    // Create the store (open creates on missing path), then drop so serve can lock.
    drop(Residiuum::open(&store_path).unwrap());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let bind_c = bind.clone();
    let path_c = store_path.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    let opts = options
        .shutdown_flag(Arc::clone(&stop))
        .suppress_startup_report(true);
    let handle = thread::spawn(move || {
        let _ = serve_store_with(path_c, &bind_c, opts);
    });
    wait_for_server(&bind);
    thread::sleep(Duration::from_millis(30));
    (bind, stop_c, handle)
}

fn rpc(
    bind: &str,
    token: Option<&str>,
    op: &str,
    collection: Option<&str>,
    confirm: Option<&str>,
) -> serde_json::Value {
    let mut stream = TcpStream::connect(bind).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    client_handshake(&mut reader, &mut stream).expect("handshake");

    let mut req = serde_json::json!({
        "id": 1u64,
        "op": op,
    });
    if let Some(t) = token {
        req["token"] = serde_json::Value::String(t.into());
    }
    if let Some(c) = collection {
        req["collection"] = serde_json::Value::String(c.into());
    }
    if let Some(c) = confirm {
        req["confirm"] = serde_json::Value::String(c.into());
    }
    if op == "put" {
        req["key"] = serde_json::Value::String("k1".into());
        req["json"] = json!({"n": 1});
        req["operation_id"] = serde_json::Value::String(format!("{:032x}", 1u128));
    }
    if op == "get" {
        req["key"] = serde_json::Value::String("k1".into());
    }
    if op == "index_create" {
        req["key"] = serde_json::Value::String("by_n".into());
        req["fields"] = serde_json::json!(["n"]);
    }

    let body = serde_json::to_string(&req).unwrap();
    write_frame(&mut stream, body.as_bytes()).unwrap();
    let bytes = read_frame(&mut reader, 4 * 1024 * 1024)
        .unwrap()
        .expect("response frame");
    serde_json::from_slice(&bytes).expect("json response")
}

fn writer_policy() -> (AuthzPolicy, Arc<AuditLog>) {
    let policy = AuthzPolicy::new()
        .with_principal("writer", "writer-secret", PrivilegeSet::writer())
        .unwrap()
        .with_principal("root", "root-secret", PrivilegeSet::superuser())
        .unwrap()
        .with_principal("reader", "reader-secret", PrivilegeSet::reader())
        .unwrap();
    (policy, Arc::new(AuditLog::new()))
}

#[test]
fn authz_profile_tag() {
    assert_eq!(AUTHZ_PROFILE, "residiuum-authz-v1");
    assert!(PrivilegeSet::writer().contains(Privilege::Write));
    assert!(!PrivilegeSet::writer().contains(Privilege::Purge));
    assert!(!PrivilegeSet::writer().contains(Privilege::Admin));
    assert!(!PrivilegeSet::writer().contains(Privilege::Salvage));
    assert!(!PrivilegeSet::writer().contains(Privilege::TierMove));
    assert!(!PrivilegeSet::writer().contains(Privilege::ForceReconfig));
    assert!(!PrivilegeSet::writer().contains(Privilege::IndexAdmin));
}

#[test]
fn writer_can_put_but_not_admin_or_purge() {
    let dir = TempDir::new().unwrap();
    let (policy, audit) = writer_policy();
    let (bind, stop, handle) = start_server(
        &dir,
        ServeOptions::new()
            .legacy_token_server()
            .authz(policy)
            .audit(Arc::clone(&audit)),
    );

    let put = rpc(&bind, Some("writer-secret"), "put", Some("docs"), None);
    assert_eq!(put["ok"], true, "put: {put}");

    let get = rpc(&bind, Some("writer-secret"), "get", Some("docs"), None);
    assert_eq!(get["ok"], true, "get: {get}");

    let admin = rpc(&bind, Some("writer-secret"), "admin_stats", None, None);
    assert_eq!(admin["ok"], false);
    assert_eq!(admin["code"], "permission_denied");

    let purge = rpc(
        &bind,
        Some("writer-secret"),
        "purge",
        Some("docs"),
        Some(PURGE_CONFIRM),
    );
    assert_eq!(purge["ok"], false);
    assert_eq!(purge["code"], "permission_denied");

    for op in ["salvage_export", "tier_move", "force_reconfig"] {
        let confirm = if op == "force_reconfig" {
            Some(FORCE_RECONFIG_CONFIRM)
        } else {
            None
        };
        let r = rpc(&bind, Some("writer-secret"), op, Some("docs"), confirm);
        assert_eq!(r["ok"], false, "op {op}: {r}");
        assert_eq!(r["code"], "permission_denied");
    }

    let idx = rpc(
        &bind,
        Some("writer-secret"),
        "index_create",
        Some("docs"),
        None,
    );
    assert_eq!(idx["ok"], false);
    assert_eq!(idx["code"], "permission_denied");

    stop.store(true, Ordering::SeqCst);
    let _ = handle;

    assert!(audit.len() >= 4);
    audit.verify_chain().unwrap();
    assert!(!audit.contains_literal("writer-secret"));
    assert!(!audit.contains_literal("root-secret"));
    let denials: Vec<_> = audit
        .records()
        .into_iter()
        .filter(|r| r.decision == AuditDecision::Deny)
        .collect();
    assert!(denials.iter().any(|r| r.op == "purge"));
    assert!(denials.iter().any(|r| r.op == "admin_stats"));
    assert!(denials.iter().all(|r| r.principal_id == "writer"));
}

#[test]
fn root_purge_requires_confirm_then_succeeds() {
    let dir = TempDir::new().unwrap();
    let (policy, audit) = writer_policy();
    let (bind, stop, handle) = start_server(
        &dir,
        ServeOptions::new()
            .legacy_token_server()
            .authz(policy)
            .audit(Arc::clone(&audit)),
    );

    let missing = rpc(&bind, Some("root-secret"), "purge", Some("docs"), None);
    assert_eq!(missing["ok"], false);
    assert_eq!(missing["code"], "permission_denied");

    let wrong = rpc(
        &bind,
        Some("root-secret"),
        "purge",
        Some("docs"),
        Some("please"),
    );
    assert_eq!(wrong["ok"], false);
    assert_eq!(wrong["code"], "permission_denied");

    let ok = rpc(
        &bind,
        Some("root-secret"),
        "purge",
        Some("docs"),
        Some(PURGE_CONFIRM),
    );
    assert_eq!(ok["ok"], true, "purge: {ok}");

    let reconfig = rpc(
        &bind,
        Some("root-secret"),
        "force_reconfig",
        None,
        Some(FORCE_RECONFIG_CONFIRM),
    );
    assert_eq!(reconfig["ok"], true);

    stop.store(true, Ordering::SeqCst);
    let _ = handle;

    audit.verify_chain().unwrap();
    assert!(!audit.contains_literal("root-secret"));
    let allows: Vec<_> = audit
        .records()
        .into_iter()
        .filter(|r| r.decision == AuditDecision::Allow && r.op == "purge")
        .collect();
    assert_eq!(allows.len(), 1);
    assert_eq!(allows[0].principal_id, "root");
}

#[test]
fn reader_cannot_write() {
    let dir = TempDir::new().unwrap();
    let (policy, audit) = writer_policy();
    let (bind, stop, handle) = start_server(
        &dir,
        ServeOptions::new()
            .legacy_token_server()
            .authz(policy)
            .audit(Arc::clone(&audit)),
    );

    let put = rpc(&bind, Some("reader-secret"), "put", Some("docs"), None);
    assert_eq!(put["ok"], false);
    assert_eq!(put["code"], "permission_denied");

    stop.store(true, Ordering::SeqCst);
    let _ = handle;
    assert!(audit
        .records()
        .iter()
        .any(|r| r.op == "put" && r.decision == AuditDecision::Deny));
}

#[test]
fn bad_token_is_authentication_failed_not_permission() {
    let dir = TempDir::new().unwrap();
    let (policy, audit) = writer_policy();
    let (bind, stop, handle) = start_server(
        &dir,
        ServeOptions::new()
            .legacy_token_server()
            .authz(policy)
            .audit(Arc::clone(&audit)),
    );

    let r = rpc(&bind, Some("nope"), "store_info", None, None);
    assert_eq!(r["ok"], false);
    assert_eq!(r["code"], "authentication_failed");
    assert!(!audit.contains_literal("nope"));
    assert!(!audit.contains_literal("writer-secret"));

    stop.store(true, Ordering::SeqCst);
    let _ = handle;
}

#[test]
fn legacy_shared_token_is_superuser() {
    let dir = TempDir::new().unwrap();
    let audit = Arc::new(AuditLog::new());
    let (bind, stop, handle) = start_server(
        &dir,
        ServeOptions::new()
            .legacy_token_server()
            .auth_token("legacy-token")
            .audit(Arc::clone(&audit)),
    );

    let put = rpc(&bind, Some("legacy-token"), "put", Some("docs"), None);
    assert_eq!(put["ok"], true);

    let purge = rpc(
        &bind,
        Some("legacy-token"),
        "purge",
        Some("docs"),
        Some(PURGE_CONFIRM),
    );
    assert_eq!(purge["ok"], true);

    {
        let mut db = Residiuum::connect_with(
            format!("residiuum://{bind}"),
            ConnectOptions::new().auth_token("legacy-token"),
        )
        .unwrap();
        let mut c = db.collection("docs").unwrap();
        c.put("sdk", &json!({"v": 1})).unwrap();
        assert!(c.get("sdk").unwrap().is_some());
    }

    stop.store(true, Ordering::SeqCst);
    // Do not join: accept loop may wait on idle timeout after shutdown flag.
    let _ = handle;
    assert!(!audit.contains_literal("legacy-token"));
}

#[test]
fn permission_denied_error_code_maps() {
    let e = Error::PermissionDenied("nope".into());
    assert_eq!(e.code(), ErrorCode::PermissionDenied);
    assert_eq!(e.code().as_str(), "permission_denied");
}
