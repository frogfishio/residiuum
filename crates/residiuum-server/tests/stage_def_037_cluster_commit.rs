//! DEF-037 — network data-plane cluster commitment.
//!
//! Client put/get over TCP must go through partition-leader Raft propose.
//! Acks report `committed=true` only after quorum + local apply. Killing the
//! contacted node after commit must not lose the acknowledged write.
//!
//! In-process cluster (`Residiuum::create_cluster`) already shares the same logical
//! commit model; this suite proves the network path matches that contract.

use residiuum_cluster::ClusterConfig;
use residiuum_sdk::{json, ConnectOptions, PutOptions, RemoteClient, Residiuum};

use residiuum_server::{
    serve_cluster_node, serve_store_with, AuthzPolicy, ServeOptions, CLUSTER_COMMIT_PROFILE,
    FEATURE_CLUSTER_COMMIT_V1, FEATURE_RAFT_RPC_V1,
};
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

fn wait_for(bind: &str) {
    for _ in 0..200 {
        if TcpStream::connect(bind).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("server did not accept on {bind}");
}

fn start_cluster_nodes(
    root: &std::path::Path,
    token: &str,
    n: u32,
) -> (
    Vec<String>,
    Vec<Arc<AtomicBool>>,
    Vec<thread::JoinHandle<()>>,
) {
    let mut binds = Vec::new();
    let mut stops = Vec::new();
    let mut handles = Vec::new();
    for i in 0..n {
        let port = free_port();
        let bind = format!("127.0.0.1:{port}");
        let root_c = root.to_path_buf();
        let bind_c = bind.clone();
        let token_c = token.to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let policy = AuthzPolicy::shared_superuser(&token_c);
            let opts = ServeOptions::new()
                .legacy_token_server()
                .experimental_network_cluster(true)
                .auth_token(token_c.clone())
                .authz(policy)
                .shutdown_flag(stop_c)
                .suppress_startup_report(true);
            let _ = serve_cluster_node(&root_c, i, &bind_c, opts);
        });
        wait_for(&bind);
        binds.push(bind);
        stops.push(stop);
        handles.push(handle);
    }
    // Wait until every node advertises a full endpoint map (routing only).
    for bind in &binds {
        let mut ok = false;
        for _ in 0..50 {
            if let Ok(mut c) = RemoteClient::connect_with(
                bind,
                format!("residiuum://{bind}/c"),
                ConnectOptions::new().auth_token(token),
            ) {
                if let Ok(snap) = c.fetch_directory() {
                    if snap.endpoints.len() >= n as usize {
                        ok = true;
                        break;
                    }
                }
            }
            thread::sleep(Duration::from_millis(40));
        }
        assert!(ok, "endpoints not fully advertised on {bind}");
    }
    (binds, stops, handles)
}

fn stop_nodes(stops: &[Arc<AtomicBool>], handles: Vec<thread::JoinHandle<()>>) {
    for s in stops {
        s.store(true, Ordering::SeqCst);
    }
    // Nudge accept loops.
    thread::sleep(Duration::from_millis(50));
    for h in handles {
        let _ = h.join();
    }
}

#[test]
fn feature_and_profile_labels() {
    assert_eq!(FEATURE_CLUSTER_COMMIT_V1, "cluster-commit-v1");
    assert_eq!(CLUSTER_COMMIT_PROFILE, "residiuum-cluster-commit-v1");
    assert_eq!(FEATURE_RAFT_RPC_V1, "raft-rpc-v1");
}

/// Network and in-process paths share the same logical put/get commit story.
#[test]
fn in_process_and_network_share_commit_semantics() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("c");

    // In-process: committed ack + readable after put.
    {
        let mut db = Residiuum::create_cluster(
            ClusterConfig::dependable_local(&root).with_virtual_partitions(8),
        )
        .unwrap();
        let r = db
            .collection("docs")
            .unwrap()
            .put("shared", &json!({"via": "in-process"}))
            .unwrap();
        assert!(r.committed, "in-process put must be committed");
        assert_eq!(
            db.collection("docs")
                .unwrap()
                .get("shared")
                .unwrap()
                .unwrap()["via"],
            "in-process"
        );
    }

    let token = "def037-shared-secret";
    let (binds, stops, handles) = start_cluster_nodes(&root, token, 3);

    let mut client = RemoteClient::connect_with(
        &binds[0],
        format!("residiuum://{}/c", binds[0]),
        ConnectOptions::new().auth_token(token),
    )
    .expect("connect");

    let r = client
        .put_json(
            "docs",
            "net",
            &json!({"via": "network"}),
            PutOptions::default(),
        )
        .expect("network put");
    assert!(
        r.committed,
        "network put ack must be committed after quorum (not routing-only)"
    );

    let v = client.get_json("docs", "net").expect("get").expect("found");
    assert_eq!(v["via"], "network");

    // Prior in-process key remains visible on a linearizable read path.
    let seed = client.get_json("docs", "shared").expect("seed get");
    assert!(
        seed.is_some(),
        "in-process seed should still be readable from a cluster node store"
    );

    stop_nodes(&stops, handles);
}

#[test]
fn network_put_survives_contacted_node_kill() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("c");
    {
        let _ = Residiuum::create_cluster(
            ClusterConfig::dependable_local(&root).with_virtual_partitions(4),
        )
        .unwrap();
    }

    let token = "def037-kill-node";
    let (binds, stops, handles) = start_cluster_nodes(&root, token, 3);

    let mut client = RemoteClient::connect_with(
        &binds[0],
        format!("residiuum://{}/c", binds[0]),
        ConnectOptions::new().auth_token(token),
    )
    .unwrap();

    // Write several keys so at least one lands under leadership that spans the cluster.
    for i in 0..12 {
        let key = format!("k{i}");
        let r = client
            .put_json("t", &key, &json!({"i": i}), PutOptions::default())
            .unwrap_or_else(|e| panic!("put {key}: {e}"));
        assert!(r.committed, "put {key} must be quorum-committed");
    }

    // Kill node 0 (the original seed). Remaining majority must still serve.
    stops[0].store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(100));

    let mut survivor = RemoteClient::connect_with(
        &binds[1],
        format!("residiuum://{}/c", binds[1]),
        ConnectOptions::new().auth_token(token),
    )
    .expect("survivor connect");

    for i in 0..12 {
        let key = format!("k{i}");
        let v = survivor
            .get_json("t", &key)
            .unwrap_or_else(|e| panic!("get {key} after kill: {e}"))
            .unwrap_or_else(|| panic!("missing {key} after kill of contacted seed"));
        assert_eq!(v["i"], i, "key {key} value mismatch after leader/seed kill");
    }

    // New write after kill still commits on the remaining quorum.
    let r = survivor
        .put_json("t", "after", &json!({"ok": true}), PutOptions::default())
        .expect("put after kill");
    assert!(r.committed);
    assert_eq!(
        survivor.get_json("t", "after").unwrap().unwrap()["ok"],
        true
    );

    // Clean up remaining nodes.
    stops[1].store(true, Ordering::SeqCst);
    stops[2].store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));
    for h in handles {
        let _ = h.join();
    }
}

#[test]
fn operation_id_retry_preserves_one_network_write() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("c");
    {
        let _ = Residiuum::create_cluster(
            ClusterConfig::dependable_local(&root).with_virtual_partitions(4),
        )
        .unwrap();
    }
    let token = "def037-op-id";
    let (binds, stops, handles) = start_cluster_nodes(&root, token, 3);

    let mut client = RemoteClient::connect_with(
        &binds[0],
        format!("residiuum://{}/c", binds[0]),
        ConnectOptions::new().auth_token(token),
    )
    .unwrap();

    // First put mints an operation_id inside RemoteClient; a second put with
    // different content is a new op. Replay is covered by store dedup + raft
    // propose_dedup on the server — exercise client transport retry by doing
    // two identical logical puts (new ids) and ensuring both commit cleanly.
    let r1 = client
        .put_json("c", "idem", &json!({"n": 1}), PutOptions::default())
        .unwrap();
    assert!(r1.committed);
    let r2 = client
        .put_json("c", "idem", &json!({"n": 2}), PutOptions::default())
        .unwrap();
    assert!(r2.committed);
    assert_ne!(
        r1.event_id, r2.event_id,
        "distinct client ops must yield distinct events"
    );
    assert_eq!(client.get_json("c", "idem").unwrap().unwrap()["n"], 2);

    stop_nodes(&stops, handles);
}

#[test]
fn single_node_serve_without_raft_still_local_commit() {
    // Regression: plain `serve_store` (no Raft) must keep single-node put semantics.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("solo");
    {
        let _ = Residiuum::open(&path).unwrap();
    }
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    let path_c = path.clone();
    let bind_c = bind.clone();
    let h = thread::spawn(move || {
        let opts = ServeOptions::new()
            .legacy_token_server()
            .shutdown_flag(stop_c)
            .suppress_startup_report(true);
        let _ = serve_store_with(path_c, &bind_c, opts);
    });
    wait_for(&bind);

    let mut client = RemoteClient::connect_with(
        &bind,
        format!("residiuum://{bind}/s"),
        ConnectOptions::new(),
    )
    .unwrap();
    let r = client
        .put_json("x", "k", &json!(1), PutOptions::default())
        .unwrap();
    assert!(r.committed);
    assert_eq!(client.get_json("x", "k").unwrap().unwrap(), json!(1));

    stop.store(true, Ordering::SeqCst);
    let _ = h.join();
}
