//! DEF-036 — framed `raft_*` RPCs over TCP between three servers.
//!
//! Proves the control-plane path: authenticated RequestVote / AppendEntries /
//! ReadIndex on independent listeners with shared cluster token. Data-plane
//! collection writes over Raft propose remain DEF-037.

use residiuum_cluster::{
    ClusterId, LogCommand, NodeId, PartitionId, PlacementEpoch, RequestVoteRequest,
};
use residiuum_sdk::{ConnectOptions, RemoteClient, Residiuum};

use residiuum_server::{
    serve_store_with, AuthzPolicy, RaftServerState, ServeOptions, SharedRaftState,
    TcpRaftTransport, FEATURE_RAFT_RPC_V1, SDK_RAFT_RPC_PROFILE,
};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
    for _ in 0..150 {
        if TcpStream::connect(bind).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("server did not accept on {bind}");
}

fn start_peer(
    store_dir: &TempDir,
    name: &str,
    raft: SharedRaftState,
    token: &str,
) -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let store_path = store_dir.path().join(name);
    drop(Residiuum::open(&store_path).unwrap());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let bind_c = bind.clone();
    let path_c = store_path.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    let token = token.to_string();
    let handle = thread::spawn(move || {
        let policy = AuthzPolicy::shared_superuser(&token);
        let opts = ServeOptions::new()
            .legacy_token_server()
            .auth_token(token)
            .authz(policy)
            .raft(raft)
            .shutdown_flag(Arc::clone(&stop_c))
            .suppress_startup_report(true);
        let _ = serve_store_with(path_c, &bind_c, opts);
    });
    wait_for_server(&bind);
    thread::sleep(Duration::from_millis(40));
    (bind, stop, handle)
}

#[test]
fn feature_and_profile_labels() {
    assert_eq!(FEATURE_RAFT_RPC_V1, "raft-rpc-v1");
    assert_eq!(SDK_RAFT_RPC_PROFILE, "residiuum-raft-rpc-v1");
}

#[test]
fn tcp_request_vote_and_append_across_three_peers() {
    let token = "cluster-secret-def036";
    let cluster_id = ClusterId::from_seed(b"tcp-036");
    let p = PartitionId(0);
    let voters: Vec<NodeId> = (0..3).map(NodeId::new).collect();

    // Build three in-memory raft states, then wire endpoints after binds.
    let states: Vec<SharedRaftState> = voters
        .iter()
        .map(|v| {
            Arc::new(Mutex::new(RaftServerState::for_test(
                cluster_id,
                *v,
                p,
                voters.clone(),
                PlacementEpoch(1),
                HashMap::new(),
                Some(token.into()),
            )))
        })
        .collect();

    let d0 = TempDir::new().unwrap();
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();
    let (b0, s0, h0) = start_peer(&d0, "n0", Arc::clone(&states[0]), token);
    let (b1, s1, h1) = start_peer(&d1, "n1", Arc::clone(&states[1]), token);
    let (b2, s2, h2) = start_peer(&d2, "n2", Arc::clone(&states[2]), token);

    let mut endpoints = HashMap::new();
    endpoints.insert(0, b0.clone());
    endpoints.insert(1, b1.clone());
    endpoints.insert(2, b2.clone());
    for st in &states {
        st.lock().unwrap().set_endpoints(endpoints.clone());
    }

    // Campaign node 0 using TCP transport to peers 1 and 2.
    {
        let transport = TcpRaftTransport {
            endpoints: endpoints.clone(),
            token: Some(token.into()),
        };
        let online = voters.clone();
        let mut g = states[0].lock().unwrap();
        let (leader, term) = g
            .node_mut(p.0)
            .unwrap()
            .campaign(&transport, &online)
            .expect("tcp campaign");
        assert_eq!(leader.index(), 0);
        assert!(term.0 >= 1);

        let res = g
            .node_mut(p.0)
            .unwrap()
            .propose(
                LogCommand::Put {
                    subject: "tcp/k".into(),
                    value: b"v".to_vec(),
                },
                &transport,
                &online,
                Some("op-tcp036bbbbbbbbbbbbbbbbbbbbbbbbbb"),
            )
            .expect("tcp propose");
        assert!(res.committed, "3-node TCP quorum must commit");
        assert!(res.replica_acks >= 2);
    }

    // ReadIndex via framed client against the leader endpoint.
    let mut client =
        RemoteClient::connect_with(&b0, b0.clone(), ConnectOptions::new().auth_token(token))
            .unwrap();
    let body = serde_json::json!({
        "cluster_id": cluster_id.to_hex(),
        "partition": p.0,
        "placement_epoch": 1,
        "term": 0,
        "from_id": 9,
    });
    let mut req = residiuum_sdk::RpcRequest {
        id: 0,
        op: "raft_read_index".into(),
        collection: None,
        key: None,
        json: Some(body),
        bytes_b64: None,
        limit: None,
        durability: None,
        token: Some(token.into()),
        fields: None,
        force_scan: None,
        order_field: None,
        order_dir: None,
        max_docs_scanned: None,
        max_bytes_scanned: None,
        max_result_bytes: None,
        operation_id: None,
        confirm: None,
    };
    let resp = client.call_rpc(req).unwrap();
    assert!(resp.ok);
    let v = resp.value.expect("read_index payload");
    assert_eq!(v["is_leader"], true);

    // Wrong epoch is fenced (not granted by knowing the endpoint).
    let mut client1 =
        RemoteClient::connect_with(&b1, b1.clone(), ConnectOptions::new().auth_token(token))
            .unwrap();
    let bad = RequestVoteRequest {
        cluster_id: cluster_id.to_hex(),
        partition: p.0,
        placement_epoch: 42,
        term: 99,
        candidate_id: 0,
        last_log_index: 0,
        last_log_term: 0,
    };
    req = residiuum_sdk::RpcRequest {
        id: 0,
        op: "raft_request_vote".into(),
        collection: None,
        key: None,
        json: Some(serde_json::to_value(&bad).unwrap()),
        bytes_b64: None,
        limit: None,
        durability: None,
        token: Some(token.into()),
        fields: None,
        force_scan: None,
        order_field: None,
        order_dir: None,
        max_docs_scanned: None,
        max_bytes_scanned: None,
        max_result_bytes: None,
        operation_id: None,
        confirm: None,
    };
    match client1.call_rpc(req) {
        Ok(resp) => assert!(!resp.ok, "stale epoch must not grant a vote"),
        Err(e) => {
            // Fail-closed transport/application error is also acceptable.
            assert!(
                e.code() == residiuum_sdk::ErrorCode::ConsistencyViolation
                    || format!("{e}").contains("epoch")
                    || format!("{e}").contains("fenced"),
                "expected fence error, got {e}"
            );
        }
    }

    s0.store(true, Ordering::SeqCst);
    s1.store(true, Ordering::SeqCst);
    s2.store(true, Ordering::SeqCst);
    let _ = h0.join();
    let _ = h1.join();
    let _ = h2.join();
}

#[test]
fn unauthenticated_raft_rpc_rejected_when_token_required() {
    let token = "need-auth";
    let cluster_id = ClusterId::from_seed(b"auth-036");
    let p = PartitionId(0);
    let voters = vec![NodeId::new(0)];
    let raft = Arc::new(Mutex::new(RaftServerState::for_test(
        cluster_id,
        NodeId::new(0),
        p,
        voters,
        PlacementEpoch(1),
        HashMap::new(),
        Some(token.into()),
    )));
    let dir = TempDir::new().unwrap();
    let (bind, stop, handle) = start_peer(&dir, "solo", raft, token);

    let client = RemoteClient::connect_with(
        &bind,
        bind.clone(),
        ConnectOptions::new(), // no token
    );
    // connect itself may fail on store_info without token.
    match client {
        Err(e) => {
            let c = e.code();
            assert!(
                c == residiuum_sdk::ErrorCode::AuthenticationFailed
                    || c == residiuum_sdk::ErrorCode::Internal
                    || format!("{e}").contains("auth"),
                "expected auth failure, got {e}"
            );
        }
        Ok(mut c) => {
            let req = residiuum_sdk::RpcRequest {
                id: 1,
                op: "raft_read_index".into(),
                collection: None,
                key: None,
                json: Some(serde_json::json!({
                    "cluster_id": cluster_id.to_hex(),
                    "partition": 0,
                    "placement_epoch": 1,
                    "term": 0,
                    "from_id": 0,
                })),
                bytes_b64: None,
                limit: None,
                durability: None,
                token: None,
                fields: None,
                force_scan: None,
                order_field: None,
                order_dir: None,
                max_docs_scanned: None,
                max_bytes_scanned: None,
                max_result_bytes: None,
                operation_id: None,
                confirm: None,
            };
            let resp = c.call_rpc(req);
            if let Ok(r) = resp {
                assert!(!r.ok);
            }
        }
    }

    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();
}
