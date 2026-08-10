//! Server-side network Raft runtime (DEF-036 control plane + DEF-037 data plane).
//!
//! Holds per-partition [`NetworkRaftNode`] state for one process and dispatches
//! inbound control-plane RPCs. Outbound RPCs use [`TcpRaftTransport`] over the
//! framed application protocol (`raft_*` ops).
//!
//! **Data plane (DEF-037):** collection put/delete on a node with Raft state
//! attached must go through [`RaftServerState::propose_and_apply`] so acks are
//! only returned after quorum commit and local state-machine apply.

use residiuum_cluster::raft_rpc::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    NetworkRaftNode, RaftTransport, ReadIndexRequest, ReadIndexResponse, RequestVoteRequest,
    RequestVoteResponse,
};
use residiuum_cluster::{
    ClusterId, ClusterMeta, LogCommand, NodeId, PartitionDirectory, PartitionId, PlacementEpoch,
    ProposeError, ProposeResult, RaftPeerStore, RaftRole,
};
use residiuum_sdk::DirectorySnapshot;
use residiuum_sdk::Error;
use residiuum_sdk::{ConnectOptions, RemoteClient, RpcRequest};
use residiuum_store::{DurabilityMode, Store, WriteReceipt as StoreWriteReceipt};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Feature token advertised when a server speaks network Raft RPC.
pub const FEATURE_RAFT_RPC_V1: &str = "raft-rpc-v1";

/// Feature token when data-plane collection ops use Raft propose (DEF-037).
pub const FEATURE_CLUSTER_COMMIT_V1: &str = "cluster-commit-v1";

/// Re-export profile for capability matrices.
pub use residiuum_cluster::RAFT_RPC_PROFILE as SDK_RAFT_RPC_PROFILE;

/// Profile tag for data-plane cluster commitment over network Raft (DEF-037).
pub const CLUSTER_COMMIT_PROFILE: &str = "residiuum-cluster-commit-v1";

/// Per-process Raft control-plane state for one cluster node.
#[derive(Debug)]
pub struct RaftServerState {
    /// Cluster identity.
    pub cluster_id: ClusterId,
    /// Dense node index of this process.
    pub local: NodeId,
    /// Optional shared auth token for peer RPCs.
    pub peer_token: Option<String>,
    /// partition → local network node.
    nodes: HashMap<u32, NetworkRaftNode>,
    /// Endpoint map node_index → host:port (routing hints only).
    endpoints: HashMap<u32, String>,
}

impl RaftServerState {
    /// Build from a cluster root for `node_index`, attaching durable peer stores.
    pub fn open(
        cluster_root: &Path,
        node_index: u32,
        peer_token: Option<String>,
    ) -> Result<Self, Error> {
        let meta = ClusterMeta::load(cluster_root)
            .map_err(|e| Error::Internal(format!("cluster meta: {e}")))?;
        if node_index >= meta.node_count {
            return Err(Error::ValidationMsg(format!(
                "node {node_index} out of range ({} nodes)",
                meta.node_count
            )));
        }
        let cluster_id = ClusterId::from_hex(&meta.cluster_id).ok_or_else(|| {
            Error::Internal(format!(
                "invalid cluster_id hex in meta: {}",
                meta.cluster_id
            ))
        })?;
        let directory = PartitionDirectory::load(cluster_root)
            .map_err(|e| Error::Internal(format!("placement: {e}")))?
            .ok_or_else(|| Error::Internal("missing placement.json".into()))?;
        let endpoints = residiuum_cluster::load_endpoints(cluster_root).unwrap_or_default();
        let local = NodeId::new(node_index);
        let mut nodes = HashMap::new();
        for a in &directory.assignments {
            if !a.replicas.contains(&local) {
                continue;
            }
            let store = RaftPeerStore::open(cluster_root, local, a.partition)
                .map_err(|e| Error::Internal(format!("raft store: {e}")))?;
            let mut node = NetworkRaftNode::new(
                cluster_id,
                a.partition,
                local,
                a.replicas.clone(),
                a.placement_epoch,
            );
            node.attach_store(store)
                .map_err(|e| Error::Internal(format!("raft attach: {e}")))?;
            nodes.insert(a.partition.0, node);
        }
        Ok(Self {
            cluster_id,
            local,
            peer_token,
            nodes,
            endpoints,
        })
    }

    /// In-memory (no disk) state for tests — one partition, given voters.
    pub fn for_test(
        cluster_id: ClusterId,
        local: NodeId,
        partition: PartitionId,
        voters: Vec<NodeId>,
        placement_epoch: PlacementEpoch,
        endpoints: HashMap<u32, String>,
        peer_token: Option<String>,
    ) -> Self {
        let node = NetworkRaftNode::new(cluster_id, partition, local, voters, placement_epoch);
        let mut nodes = HashMap::new();
        nodes.insert(partition.0, node);
        Self {
            cluster_id,
            local,
            peer_token,
            nodes,
            endpoints,
        }
    }

    /// Update endpoint routing hints (not write authority).
    pub fn set_endpoints(&mut self, endpoints: HashMap<u32, String>) {
        self.endpoints = endpoints;
    }

    /// Refresh endpoints from cluster root.
    pub fn reload_endpoints(&mut self, cluster_root: &Path) {
        if let Ok(eps) = residiuum_cluster::load_endpoints(cluster_root) {
            self.endpoints = eps;
        }
    }

    /// Borrow a partition node.
    pub fn node_mut(&mut self, partition: u32) -> Option<&mut NetworkRaftNode> {
        self.nodes.get_mut(&partition)
    }

    /// Build a transport that dials peers by endpoint map.
    pub fn transport(&self) -> TcpRaftTransport {
        TcpRaftTransport {
            endpoints: self.endpoints.clone(),
            token: self.peer_token.clone(),
        }
    }

    /// Online peers we have an endpoint for (routing hint; not authority).
    pub fn online_hint(&self) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self.endpoints.keys().copied().map(NodeId::new).collect();
        if !v.contains(&self.local) {
            v.push(self.local);
        }
        v.sort_by_key(|n| n.index());
        v
    }

    /// Campaign for leadership on `partition`.
    pub fn campaign(&mut self, partition: u32) -> Result<(NodeId, residiuum_cluster::Term), Error> {
        let online = self.online_hint();
        let transport = self.transport();
        let node = self
            .nodes
            .get_mut(&partition)
            .ok_or_else(|| Error::ValidationMsg(format!("no raft state for p{partition}")))?;
        node.campaign(&transport, &online)
            .map_err(|e| map_elect_err(partition, e))
    }

    /// Whether this process holds Raft state for `partition`.
    pub fn has_partition(&self, partition: u32) -> bool {
        self.nodes.contains_key(&partition)
    }

    /// Whether the local peer is leader for `partition`.
    pub fn is_leader(&self, partition: u32) -> bool {
        self.nodes
            .get(&partition)
            .map(|n| n.is_leader())
            .unwrap_or(false)
    }

    /// Local term for `partition` (0 if unknown).
    pub fn term(&self, partition: u32) -> u64 {
        self.nodes.get(&partition).map(|n| n.term().0).unwrap_or(0)
    }

    /// Overlay live leader/term from Raft onto a directory snapshot (routing hints).
    ///
    /// Placement epoch and replica sets remain authoritative from disk; only
    /// leader and term fields that this process can prove are updated.
    pub fn overlay_live_leaders(&self, snap: &mut DirectorySnapshot) {
        for a in &mut snap.assignments {
            if let Some(n) = self.nodes.get(&a.partition) {
                if n.is_leader() {
                    a.leader = self.local.index();
                    a.term = n.term().0;
                } else if let Some(known) = n.group().peer(n.local).and_then(|p| {
                    if p.role == RaftRole::Follower {
                        p.voted_for
                    } else {
                        None
                    }
                }) {
                    // Best-effort: last known leader from vote/append. Not write
                    // authority — clients still refresh on fence errors.
                    a.leader = known.index();
                    a.term = n.term().0.max(a.term);
                }
            }
        }
    }

    /// Ensure local leadership for `partition` (campaign when needed).
    pub fn ensure_leader(
        &mut self,
        partition: u32,
    ) -> Result<(NodeId, residiuum_cluster::Term), Error> {
        if self.is_leader(partition) {
            let t = self.term(partition);
            return Ok((self.local, residiuum_cluster::Term(t)));
        }
        self.campaign(partition)
    }

    /// Propose `command` on `partition` and wait for quorum commit evidence.
    ///
    /// Campaigns if this node is not yet leader. Returns [`ProposeError::NotLeader`]
    /// mapped to [`Error::StaleRoute`] when a concurrent term change steals leadership.
    pub fn propose(
        &mut self,
        partition: u32,
        command: LogCommand,
        operation_id: Option<&str>,
    ) -> Result<ProposeResult, Error> {
        self.ensure_leader(partition)?;
        let online = self.online_hint();
        let transport = self.transport();
        let node = self
            .nodes
            .get_mut(&partition)
            .ok_or_else(|| Error::StaleRoute {
                partition,
                message: "no raft state for partition".into(),
            })?;
        node.propose(command, &transport, &online, operation_id)
            .map_err(|e| map_propose_err(partition, e))
    }

    /// Apply committed-but-not-yet-applied log entries for `partition` to `store`.
    ///
    /// Used after a successful propose (leader) and after AppendEntries
    /// (followers) so state machines stay caught up with the commit index.
    pub fn apply_committed(
        &mut self,
        partition: u32,
        store: &mut Store,
        mode: DurabilityMode,
    ) -> Result<Vec<StoreWriteReceipt>, Error> {
        let local = self.local;
        let node = self
            .nodes
            .get_mut(&partition)
            .ok_or_else(|| Error::StaleRoute {
                partition,
                message: "no raft state for partition".into(),
            })?;
        let batch = node.group_mut().take_apply_batch(local);
        let mut receipts = Vec::with_capacity(batch.len());
        for entry in batch {
            let receipt = match entry.command {
                LogCommand::Put { subject, value } => store.put(&subject, &value, mode)?,
                LogCommand::Delete { subject } => store.delete(&subject, mode)?,
            };
            receipts.push(receipt);
        }
        Ok(receipts)
    }

    /// Apply committed entries for every partition this process owns.
    pub fn apply_all_committed(
        &mut self,
        store: &mut Store,
        mode: DurabilityMode,
    ) -> Result<(), Error> {
        let partitions: Vec<u32> = self.nodes.keys().copied().collect();
        for p in partitions {
            let _ = self.apply_committed(p, store, mode)?;
        }
        Ok(())
    }

    /// Propose a data-plane write, require quorum commit, apply locally, return receipt.
    ///
    /// A routing-only or minority ack is never returned as success: if commit
    /// evidence is missing, this returns [`Error::DurabilityUnavailable`].
    pub fn propose_and_apply(
        &mut self,
        partition: u32,
        command: LogCommand,
        store: &mut Store,
        mode: DurabilityMode,
        operation_id: Option<&str>,
    ) -> Result<(ProposeResult, StoreWriteReceipt), Error> {
        let propose = self.propose(partition, command, operation_id)?;
        if !propose.committed {
            return Err(Error::DurabilityUnavailable(format!(
                "quorum not reached for partition {partition}: got {} replica acks (need majority)",
                propose.replica_acks
            )));
        }
        let receipts = self.apply_committed(partition, store, mode)?;
        let receipt = if let Some(r) = receipts.into_iter().last() {
            r
        } else {
            // Committed earlier (operation_id / response-loss retry) and already
            // applied: re-execute once so the caller gets a store receipt. Store
            // write-dedup at the dispatch layer should usually short-circuit first.
            match &propose.entry.command {
                LogCommand::Put { subject, value } => store.put(subject, value, mode)?,
                LogCommand::Delete { subject } => store.delete(subject, mode)?,
            }
        };
        Ok((propose, receipt))
    }

    /// Linearizable read barrier: ensure leadership and catch up apply index.
    pub fn read_index_barrier(&mut self, partition: u32, store: &mut Store) -> Result<(), Error> {
        self.ensure_leader(partition)?;
        // Apply everything committed so far before serving the read.
        let _ = self.apply_committed(partition, store, DurabilityMode::Durable)?;
        Ok(())
    }

    /// Dispatch an inbound raft RPC by op name and JSON body.
    pub fn dispatch_json(&mut self, op: &str, body: &JsonValue) -> Result<JsonValue, Error> {
        match op {
            "raft_request_vote" => {
                let req: RequestVoteRequest = serde_json::from_value(body.clone())
                    .map_err(|e| Error::ProtocolViolation(format!("request_vote: {e}")))?;
                let node = self
                    .nodes
                    .get_mut(&req.partition)
                    .ok_or_else(|| Error::ValidationMsg("unknown partition".into()))?;
                let resp = node.handle_request_vote(&req).map_err(raft_err)?;
                Ok(serde_json::to_value(resp).expect("serialize"))
            }
            "raft_append_entries" => {
                let req: AppendEntriesRequest = serde_json::from_value(body.clone())
                    .map_err(|e| Error::ProtocolViolation(format!("append_entries: {e}")))?;
                let node = self
                    .nodes
                    .get_mut(&req.partition)
                    .ok_or_else(|| Error::ValidationMsg("unknown partition".into()))?;
                let resp = node.handle_append_entries(&req).map_err(raft_err)?;
                Ok(serde_json::to_value(resp).expect("serialize"))
            }
            "raft_install_snapshot" => {
                let req: InstallSnapshotRequest = serde_json::from_value(body.clone())
                    .map_err(|e| Error::ProtocolViolation(format!("install_snapshot: {e}")))?;
                let node = self
                    .nodes
                    .get_mut(&req.partition)
                    .ok_or_else(|| Error::ValidationMsg("unknown partition".into()))?;
                let resp = node.handle_install_snapshot(&req).map_err(raft_err)?;
                Ok(serde_json::to_value(resp).expect("serialize"))
            }
            "raft_read_index" => {
                let req: ReadIndexRequest = serde_json::from_value(body.clone())
                    .map_err(|e| Error::ProtocolViolation(format!("read_index: {e}")))?;
                let node = self
                    .nodes
                    .get_mut(&req.partition)
                    .ok_or_else(|| Error::ValidationMsg("unknown partition".into()))?;
                let resp = node.handle_read_index(&req).map_err(raft_err)?;
                Ok(serde_json::to_value(resp).expect("serialize"))
            }
            _ => Err(Error::ValidationMsg(format!("unknown raft op {op}"))),
        }
    }
}

fn raft_err(e: residiuum_cluster::RaftRpcError) -> Error {
    match e {
        residiuum_cluster::RaftRpcError::Unauthorized(s) => Error::AuthenticationFailed(s),
        residiuum_cluster::RaftRpcError::Fenced(s) => Error::ConsistencyViolation(s),
        residiuum_cluster::RaftRpcError::Protocol(s) => Error::ProtocolViolation(s),
        other => Error::Internal(other.to_string()),
    }
}

fn map_propose_err(partition: u32, e: ProposeError) -> Error {
    match e {
        ProposeError::NotLeader => Error::StaleRoute {
            partition,
            message: "not leader (code=not_leader)".into(),
        },
        ProposeError::SteppedDown(t) => Error::StaleRoute {
            partition,
            message: format!("leader stepped down at term {} (code=not_leader)", t.0),
        },
        ProposeError::PersistFailed => {
            Error::DurabilityUnavailable("raft log persist failed before replication".into())
        }
    }
}

fn map_elect_err(partition: u32, e: residiuum_cluster::ElectError) -> Error {
    match e {
        residiuum_cluster::ElectError::NoQuorum { votes, need } => Error::StaleRoute {
            partition,
            message: format!("election no quorum votes={votes} need={need} (code=not_leader)"),
        },
        residiuum_cluster::ElectError::NotAVoter => Error::StaleRoute {
            partition,
            message: "not a voter for partition (code=not_leader)".into(),
        },
        residiuum_cluster::ElectError::CandidateOffline => Error::PartitionUnavailable {
            partition,
            reason: "candidate offline",
        },
        other => Error::Internal(format!("campaign: {other:?}")),
    }
}

/// Shared handle placed on [`crate::remote::ServeOptions`].
pub type SharedRaftState = Arc<Mutex<RaftServerState>>;

/// TCP transport: dials peer endpoints and issues `raft_*` application RPCs.
#[derive(Debug, Clone)]
pub struct TcpRaftTransport {
    /// Routing hints only (node_index → host:port).
    pub endpoints: HashMap<u32, String>,
    /// Shared auth token for peer calls.
    pub token: Option<String>,
}

impl TcpRaftTransport {
    fn call_json(&self, to: NodeId, op: &str, body: &JsonValue) -> Result<JsonValue, Error> {
        let addr = self
            .endpoints
            .get(&to.index())
            .ok_or_else(|| {
                Error::Internal(format!(
                    "no endpoint for {to} (routing hint missing; not write authority)"
                ))
            })?
            .clone();
        let mut opts = ConnectOptions::new();
        if let Some(t) = &self.token {
            opts = opts.auth_token(t.clone());
        }
        // endpoint label is diagnostic only; addr is the dial target.
        let mut client = RemoteClient::connect_with(&addr, addr.clone(), opts)?;
        let req = RpcRequest {
            id: 1,
            op: op.into(),
            collection: None,
            key: None,
            json: Some(body.clone()),
            bytes_b64: None,
            limit: None,
            durability: None,
            token: self.token.clone(),
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
        let resp = client.call_rpc(req)?;
        if !resp.ok {
            return Err(Error::Internal(
                resp.error
                    .unwrap_or_else(|| format!("raft rpc {op} failed")),
            ));
        }
        resp.value
            .ok_or_else(|| Error::ProtocolViolation(format!("{op} missing value payload")))
    }
}

impl RaftTransport for TcpRaftTransport {
    fn request_vote(
        &self,
        to: NodeId,
        req: &RequestVoteRequest,
    ) -> Result<RequestVoteResponse, residiuum_cluster::RaftRpcError> {
        let body = serde_json::to_value(req).map_err(|e| {
            residiuum_cluster::RaftRpcError::Protocol(format!("encode request_vote: {e}"))
        })?;
        let v = self
            .call_json(to, "raft_request_vote", &body)
            .map_err(|e| residiuum_cluster::RaftRpcError::Unavailable(e.to_string()))?;
        serde_json::from_value(v)
            .map_err(|e| residiuum_cluster::RaftRpcError::Protocol(format!("decode: {e}")))
    }

    fn append_entries(
        &self,
        to: NodeId,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, residiuum_cluster::RaftRpcError> {
        let body = serde_json::to_value(req).map_err(|e| {
            residiuum_cluster::RaftRpcError::Protocol(format!("encode append_entries: {e}"))
        })?;
        let v = self
            .call_json(to, "raft_append_entries", &body)
            .map_err(|e| residiuum_cluster::RaftRpcError::Unavailable(e.to_string()))?;
        serde_json::from_value(v)
            .map_err(|e| residiuum_cluster::RaftRpcError::Protocol(format!("decode: {e}")))
    }

    fn install_snapshot(
        &self,
        to: NodeId,
        req: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, residiuum_cluster::RaftRpcError> {
        let body = serde_json::to_value(req).map_err(|e| {
            residiuum_cluster::RaftRpcError::Protocol(format!("encode install_snapshot: {e}"))
        })?;
        let v = self
            .call_json(to, "raft_install_snapshot", &body)
            .map_err(|e| residiuum_cluster::RaftRpcError::Unavailable(e.to_string()))?;
        serde_json::from_value(v)
            .map_err(|e| residiuum_cluster::RaftRpcError::Protocol(format!("decode: {e}")))
    }

    fn read_index(
        &self,
        to: NodeId,
        req: &ReadIndexRequest,
    ) -> Result<ReadIndexResponse, residiuum_cluster::RaftRpcError> {
        let body = serde_json::to_value(req).map_err(|e| {
            residiuum_cluster::RaftRpcError::Protocol(format!("encode read_index: {e}"))
        })?;
        let v = self
            .call_json(to, "raft_read_index", &body)
            .map_err(|e| residiuum_cluster::RaftRpcError::Unavailable(e.to_string()))?;
        serde_json::from_value(v)
            .map_err(|e| residiuum_cluster::RaftRpcError::Protocol(format!("decode: {e}")))
    }
}

/// Helper: open raft state and wrap in a shared mutex.
pub fn shared_raft_state(
    cluster_root: impl AsRef<Path>,
    node_index: u32,
    peer_token: Option<String>,
) -> Result<SharedRaftState, Error> {
    let state = RaftServerState::open(cluster_root.as_ref(), node_index, peer_token)?;
    Ok(Arc::new(Mutex::new(state)))
}
