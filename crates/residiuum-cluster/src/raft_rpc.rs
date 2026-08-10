//! Network Raft RPC types, transport, and single-peer node (DEF-036).
//!
//! The in-process [`crate::raft::PartitionRaft`] keeps multi-peer state in one
//! address space for tests. Production multi-process clusters need the same
//! RPCs over the wire: RequestVote, AppendEntries, InstallSnapshot, and
//! leadership/read-index probes.
//!
//! # Authority rules (DEF-036)
//!
//! - Endpoint maps are **routing hints only** — never write authority.
//! - Writes are fenced by **term**, **membership (voter set)**, and
//!   **placement epoch**.
//! - Control-plane RPCs (votes / appends / snapshots) are distinct from the
//!   data-plane collection API; a node may answer Raft RPCs while refusing
//!   client writes, and vice versa when degraded.
//! - Persist-before-ack (DEF-035) still applies on each peer before acking.
//!
//! Profile tag: [`RAFT_RPC_PROFILE`].

use crate::error::ClusterError;
use crate::id::{ClusterId, LogPosition, NodeId, PartitionId, PlacementEpoch, Term};
use crate::raft::{
    ElectError, LogCommand, LogEntry, PartitionRaft, ProposeError, ProposeResult, RaftRole,
};
use crate::raft_persist::{snapshot_meta_for, RaftPeerStore};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Profile tag for network Raft RPC wire documents (DEF-036).
pub const RAFT_RPC_PROFILE: &str = "residiuum-raft-rpc-v1";

/// Default max entries per AppendEntries batch (bounded batching).
pub const DEFAULT_MAX_APPEND_BATCH: usize = 64;

/// Default base backoff when a peer is unreachable (milliseconds).
pub const DEFAULT_PEER_BACKOFF_MS: u64 = 25;

/// Maximum peer backoff (milliseconds).
pub const DEFAULT_PEER_BACKOFF_MAX_MS: u64 = 2_000;

/// Maximum replication attempts per follower during one propose.
pub const DEFAULT_REPLICATE_ATTEMPTS: u32 = 32;

// ---------------------------------------------------------------------------
// Wire messages
// ---------------------------------------------------------------------------

/// RequestVote RPC (Raft §5.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    /// Cluster identity fence (reject cross-cluster).
    pub cluster_id: String,
    /// Partition this vote applies to.
    pub partition: u32,
    /// Placement epoch fence.
    pub placement_epoch: u64,
    /// Candidate's term.
    pub term: u64,
    /// Candidate dense node index.
    pub candidate_id: u32,
    /// Index of candidate's last log entry.
    pub last_log_index: u64,
    /// Term of candidate's last log entry.
    pub last_log_term: u64,
}

/// RequestVote response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// Receiver's current term (may be higher).
    pub term: u64,
    /// Whether the vote was granted.
    pub vote_granted: bool,
}

/// AppendEntries RPC (Raft §5.3) — also used as heartbeat when `entries` empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    /// Cluster identity fence.
    pub cluster_id: String,
    /// Partition.
    pub partition: u32,
    /// Placement epoch fence.
    pub placement_epoch: u64,
    /// Leader's term.
    pub term: u64,
    /// Leader dense node index.
    pub leader_id: u32,
    /// Index of log entry immediately preceding new ones.
    pub prev_log_index: u64,
    /// Term of `prev_log_index` entry.
    pub prev_log_term: u64,
    /// Log entries to store (empty for heartbeat); bounded by batching.
    pub entries: Vec<LogEntry>,
    /// Leader's commit index.
    pub leader_commit: u64,
}

/// AppendEntries response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// Receiver's current term.
    pub term: u64,
    /// True if follower contained entry matching prevLogIndex/prevLogTerm.
    pub success: bool,
    /// Hint for leader next_index backoff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_index: Option<u64>,
    /// Follower's last log index after processing (diagnostics / flow control).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_index: Option<u64>,
}

/// InstallSnapshot RPC (Raft §7) — single-shot for this profile (blob in body).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    /// Cluster identity fence.
    pub cluster_id: String,
    /// Partition.
    pub partition: u32,
    /// Placement epoch fence.
    pub placement_epoch: u64,
    /// Leader's term.
    pub term: u64,
    /// Leader dense node index.
    pub leader_id: u32,
    /// Snapshot last-included index.
    pub last_included_index: u64,
    /// Snapshot last-included term.
    pub last_included_term: u64,
    /// BLAKE3-256 hex of snapshot blob (`SnapshotMeta::blob_checksum`).
    pub blob_checksum: String,
    /// Snapshot payload (base64 on the outer RPC; raw bytes here in-process).
    #[serde(with = "serde_bytes_b64")]
    pub blob: Vec<u8>,
}

/// InstallSnapshot response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    /// Receiver's current term.
    pub term: u64,
    /// Whether the snapshot was installed.
    pub success: bool,
    /// Error detail when rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Leadership / read-index probe (linearizable-read helper).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadIndexRequest {
    /// Cluster identity fence.
    pub cluster_id: String,
    /// Partition.
    pub partition: u32,
    /// Placement epoch fence.
    pub placement_epoch: u64,
    /// Caller's observed term (optional; 0 = ignore).
    #[serde(default)]
    pub term: u64,
    /// Dense node index of the caller (leader probing self, or client via leader).
    pub from_id: u32,
}

/// ReadIndex / leadership response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadIndexResponse {
    /// Receiver's current term.
    pub term: u64,
    /// Whether this node believes it is the leader for the partition.
    pub is_leader: bool,
    /// Commit index if leader (else 0).
    pub commit_index: u64,
    /// Leader dense node index if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_id: Option<u32>,
}

// Minimal base64 helpers so wire blobs are JSON-safe without an extra crate.
mod serde_bytes_b64 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&b64_encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        b64_decode(&s).map_err(serde::de::Error::custom)
    }

    fn b64_encode(data: &[u8]) -> String {
        const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
        for chunk in data.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
            let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
            let n = (b0 << 16) | (b1 << 8) | b2;
            out.push(T[((n >> 18) & 63) as usize] as char);
            out.push(T[((n >> 12) & 63) as usize] as char);
            out.push(if chunk.len() > 1 {
                T[((n >> 6) & 63) as usize] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                T[(n & 63) as usize] as char
            } else {
                '='
            });
        }
        out
    }

    fn b64_decode(s: &str) -> Result<Vec<u8>, &'static str> {
        fn val(c: u8) -> Result<u8, &'static str> {
            match c {
                b'A'..=b'Z' => Ok(c - b'A'),
                b'a'..=b'z' => Ok(c - b'a' + 26),
                b'0'..=b'9' => Ok(c - b'0' + 52),
                b'+' => Ok(62),
                b'/' => Ok(63),
                _ => Err("invalid base64"),
            }
        }
        let bytes = s.as_bytes();
        if !bytes.len().is_multiple_of(4) {
            return Err("invalid base64 length");
        }
        let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
        for chunk in bytes.chunks(4) {
            let pad = chunk.iter().filter(|&&c| c == b'=').count();
            let n = ((val(chunk[0])? as u32) << 18)
                | ((val(if chunk[1] == b'=' { b'A' } else { chunk[1] })? as u32) << 12)
                | ((val(if chunk[2] == b'=' { b'A' } else { chunk[2] })? as u32) << 6)
                | (val(if chunk[3] == b'=' { b'A' } else { chunk[3] })? as u32);
            out.push(((n >> 16) & 0xff) as u8);
            if pad < 2 {
                out.push(((n >> 8) & 0xff) as u8);
            }
            if pad < 1 {
                out.push((n & 0xff) as u8);
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

/// Errors from network Raft RPCs (distinct from data-plane SDK errors).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftRpcError {
    /// Peer unreachable / transport failure.
    Unavailable(String),
    /// Authentication or authorization failed.
    Unauthorized(String),
    /// Cluster / partition / epoch fence rejected the call.
    Fenced(String),
    /// Protocol or payload error.
    Protocol(String),
    /// Local persistence failure (fail closed).
    Persist(String),
    /// Timed out waiting for a peer.
    Timeout(String),
}

impl std::fmt::Display for RaftRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(s) => write!(f, "raft rpc unavailable: {s}"),
            Self::Unauthorized(s) => write!(f, "raft rpc unauthorized: {s}"),
            Self::Fenced(s) => write!(f, "raft rpc fenced: {s}"),
            Self::Protocol(s) => write!(f, "raft rpc protocol: {s}"),
            Self::Persist(s) => write!(f, "raft rpc persist: {s}"),
            Self::Timeout(s) => write!(f, "raft rpc timeout: {s}"),
        }
    }
}

impl std::error::Error for RaftRpcError {}

impl RaftRpcError {
    /// Stable machine code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unavailable(_) => "raft_unavailable",
            Self::Unauthorized(_) => "raft_unauthorized",
            Self::Fenced(_) => "raft_fenced",
            Self::Protocol(_) => "raft_protocol",
            Self::Persist(_) => "raft_persist",
            Self::Timeout(_) => "raft_timeout",
        }
    }
}

/// Outbound Raft RPC transport (control plane).
///
/// Implementations MUST NOT treat endpoint discovery alone as write authority;
/// the receiver applies term/membership/epoch fences.
pub trait RaftTransport: Send + Sync {
    /// RequestVote to `to`.
    fn request_vote(
        &self,
        to: NodeId,
        req: &RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftRpcError>;

    /// AppendEntries to `to`.
    fn append_entries(
        &self,
        to: NodeId,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftRpcError>;

    /// InstallSnapshot to `to`.
    fn install_snapshot(
        &self,
        to: NodeId,
        req: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftRpcError>;

    /// ReadIndex / leadership probe to `to`.
    fn read_index(
        &self,
        to: NodeId,
        req: &ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftRpcError>;
}

// ---------------------------------------------------------------------------
// Single-peer network node
// ---------------------------------------------------------------------------

/// Flow-control / retry knobs for a network Raft node.
#[derive(Debug, Clone)]
pub struct RaftRpcConfig {
    /// Max log entries per AppendEntries.
    pub max_append_batch: usize,
    /// Base backoff for unreachable peers.
    pub peer_backoff: Duration,
    /// Cap on peer backoff.
    pub peer_backoff_max: Duration,
    /// Max replicate attempts per follower per propose.
    pub replicate_attempts: u32,
}

impl Default for RaftRpcConfig {
    fn default() -> Self {
        Self {
            max_append_batch: DEFAULT_MAX_APPEND_BATCH,
            peer_backoff: Duration::from_millis(DEFAULT_PEER_BACKOFF_MS),
            peer_backoff_max: Duration::from_millis(DEFAULT_PEER_BACKOFF_MAX_MS),
            replicate_attempts: DEFAULT_REPLICATE_ATTEMPTS,
        }
    }
}

/// One voting replica that owns **only local** Raft state and speaks RPCs.
///
/// Remote peers are contacted exclusively through a [`RaftTransport`]. This is
/// the multi-process counterpart to in-process [`PartitionRaft`].
#[derive(Debug)]
pub struct NetworkRaftNode {
    /// Cluster this node belongs to.
    pub cluster_id: ClusterId,
    /// Local dense node id.
    pub local: NodeId,
    /// In-process group holding the local peer (and only that peer's state).
    group: PartitionRaft,
    /// Optional durable store for the local peer.
    ///
    /// Stored inside `group` via `attach_store`; kept here for path diagnostics.
    #[allow(dead_code)]
    has_store: bool,
    /// RPC flow-control config.
    pub config: RaftRpcConfig,
    /// Propose dedupe: operation_id → committed log index (one event identity).
    propose_dedup: HashMap<String, u64>,
    /// Per-peer consecutive failure count (for backoff).
    peer_failures: HashMap<u32, u32>,
}

impl NetworkRaftNode {
    /// Create a node with empty local log for `local` among `voters`.
    pub fn new(
        cluster_id: ClusterId,
        partition: PartitionId,
        local: NodeId,
        voters: Vec<NodeId>,
        placement_epoch: PlacementEpoch,
    ) -> Self {
        assert!(voters.contains(&local), "local node must be a voter");
        // Full voter set for quorum math; only the local peer's log/hard state
        // is used. Remotes are contacted exclusively via [`RaftTransport`].
        let group = PartitionRaft::new(partition, voters, placement_epoch);
        Self {
            cluster_id,
            local,
            group,
            has_store: false,
            config: RaftRpcConfig::default(),
            propose_dedup: HashMap::new(),
            peer_failures: HashMap::new(),
        }
    }

    /// Attach durable store for the local peer and restore state (Follower).
    pub fn attach_store(&mut self, store: RaftPeerStore) -> Result<(), ClusterError> {
        self.group.attach_store(store);
        self.group.restore_peer_from_store(self.local)?;
        self.group.persist_membership()?;
        self.has_store = true;
        Ok(())
    }

    /// Partition id.
    pub fn partition(&self) -> PartitionId {
        self.group.partition
    }

    /// Placement epoch.
    pub fn placement_epoch(&self) -> PlacementEpoch {
        self.group.placement_epoch
    }

    /// Configured voters.
    pub fn voters(&self) -> &[NodeId] {
        &self.group.voters
    }

    /// Quorum size.
    pub fn quorum(&self) -> u32 {
        self.group.quorum()
    }

    /// Local peer role.
    pub fn role(&self) -> RaftRole {
        self.group
            .peer(self.local)
            .map(|p| p.role)
            .unwrap_or(RaftRole::Follower)
    }

    /// Local current term.
    pub fn term(&self) -> Term {
        self.group
            .peer(self.local)
            .map(|p| p.current_term)
            .unwrap_or(Term(0))
    }

    /// Local commit index.
    pub fn commit_index(&self) -> u64 {
        self.group
            .peer(self.local)
            .map(|p| p.commit_index)
            .unwrap_or(0)
    }

    /// Whether this node is leader.
    pub fn is_leader(&self) -> bool {
        self.role() == RaftRole::Leader
    }

    /// Borrow the inner group (tests / apply path).
    pub fn group(&self) -> &PartitionRaft {
        &self.group
    }

    /// Mutable group (apply batches).
    pub fn group_mut(&mut self) -> &mut PartitionRaft {
        &mut self.group
    }

    fn cluster_hex(&self) -> String {
        self.cluster_id.to_hex()
    }

    fn check_fence(
        &self,
        cluster_id: &str,
        partition: u32,
        placement_epoch: u64,
    ) -> Result<(), RaftRpcError> {
        if cluster_id != self.cluster_hex() {
            return Err(RaftRpcError::Fenced(format!(
                "cluster_id mismatch: got {cluster_id}, want {}",
                self.cluster_hex()
            )));
        }
        if partition != self.group.partition.0 {
            return Err(RaftRpcError::Fenced(format!(
                "partition mismatch: got {partition}, want {}",
                self.group.partition.0
            )));
        }
        if placement_epoch != self.group.placement_epoch.0 {
            return Err(RaftRpcError::Fenced(format!(
                "placement_epoch mismatch: got {placement_epoch}, want {}",
                self.group.placement_epoch.0
            )));
        }
        Ok(())
    }

    /// Handle inbound RequestVote.
    pub fn handle_request_vote(
        &mut self,
        req: &RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftRpcError> {
        self.check_fence(&req.cluster_id, req.partition, req.placement_epoch)?;
        let res = self.group.handle_request_vote(
            self.local,
            NodeId::new(req.candidate_id),
            Term(req.term),
            req.last_log_index,
            Term(req.last_log_term),
        );
        Ok(RequestVoteResponse {
            term: res.term.0,
            vote_granted: res.vote_granted,
        })
    }

    /// Handle inbound AppendEntries.
    pub fn handle_append_entries(
        &mut self,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftRpcError> {
        self.check_fence(&req.cluster_id, req.partition, req.placement_epoch)?;
        let res = self.group.append_entries(
            self.local,
            NodeId::new(req.leader_id),
            Term(req.term),
            req.prev_log_index,
            Term(req.prev_log_term),
            &req.entries,
            req.leader_commit,
        );
        let match_index = if res.success {
            self.group.peer(self.local).map(|p| p.last_log_index())
        } else {
            None
        };
        Ok(AppendEntriesResponse {
            term: res.term.0,
            success: res.success,
            conflict_index: res.conflict_index,
            match_index,
        })
    }

    /// Handle inbound InstallSnapshot.
    pub fn handle_install_snapshot(
        &mut self,
        req: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftRpcError> {
        self.check_fence(&req.cluster_id, req.partition, req.placement_epoch)?;
        let peer_term = self.term();
        if req.term < peer_term.0 {
            return Ok(InstallSnapshotResponse {
                term: peer_term.0,
                success: false,
                error: Some("stale leader term".into()),
            });
        }
        // Step down to follower on valid leader term.
        if let Some(p) = self.group.peer_mut(self.local) {
            if req.term > p.current_term.0 || p.role != RaftRole::Follower {
                p.current_term = Term(req.term);
                p.role = RaftRole::Follower;
                p.voted_for = Some(NodeId::new(req.leader_id));
            }
        }
        let recomputed = snapshot_meta_for(
            req.last_included_index,
            Term(req.last_included_term),
            &req.blob,
            "network-install",
        );
        if !req.blob_checksum.is_empty() && recomputed.blob_checksum != req.blob_checksum {
            return Ok(InstallSnapshotResponse {
                term: self.term().0,
                success: false,
                error: Some("snapshot blob checksum mismatch".into()),
            });
        }
        // Prefer the ordinary install path when the index is already in the
        // local log (post-replication). Otherwise force-apply the snapshot
        // base so a lagging follower can catch up (Raft §7).
        let has_index = self
            .group
            .peer(self.local)
            .and_then(|p| p.term_at(req.last_included_index))
            .is_some();
        let result = if has_index {
            self.group.install_local_snapshot(
                self.local,
                req.last_included_index,
                &req.blob,
                "network-install",
            )
        } else {
            self.force_install_snapshot(
                req.last_included_index,
                Term(req.last_included_term),
                &req.blob,
            )
        };
        match result {
            Ok(()) => Ok(InstallSnapshotResponse {
                term: self.term().0,
                success: true,
                error: None,
            }),
            Err(e) => Ok(InstallSnapshotResponse {
                term: self.term().0,
                success: false,
                error: Some(e.to_string()),
            }),
        }
    }

    /// Install a snapshot when the last-included index is not yet in the local log.
    fn force_install_snapshot(
        &mut self,
        last_included_index: u64,
        last_included_term: Term,
        blob: &[u8],
    ) -> Result<(), ClusterError> {
        let Some(peer) = self.group.peer_mut(self.local) else {
            return Err(ClusterError::CorruptMeta("snapshot: unknown peer"));
        };
        peer.log.clear();
        if last_included_index > 0 {
            peer.log.push(LogEntry {
                term: last_included_term,
                index: last_included_index,
                command: LogCommand::Delete {
                    subject: "__residiuum_snapshot_base__".into(),
                },
            });
        }
        peer.commit_index = peer.commit_index.max(last_included_index);
        peer.last_applied = peer.last_applied.max(last_included_index);
        // Persist via install_local_snapshot now that the index is present.
        let _ = peer;
        self.group.install_local_snapshot(
            self.local,
            last_included_index,
            blob,
            "network-force-install",
        )
    }

    /// Handle inbound ReadIndex / leadership probe.
    pub fn handle_read_index(
        &mut self,
        req: &ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftRpcError> {
        self.check_fence(&req.cluster_id, req.partition, req.placement_epoch)?;
        let term = self.term();
        let is_leader = self.is_leader();
        Ok(ReadIndexResponse {
            term: term.0,
            is_leader,
            commit_index: if is_leader { self.commit_index() } else { 0 },
            leader_id: if is_leader {
                Some(self.local.index())
            } else {
                None
            },
        })
    }

    /// Campaign for leadership using `transport` for remote RequestVote.
    ///
    /// `online` is the set of peers this coordinator believes reachable; it is
    /// **not** write authority — votes still require successful RPC + fences.
    pub fn campaign(
        &mut self,
        transport: &dyn RaftTransport,
        online: &[NodeId],
    ) -> Result<(NodeId, Term), ElectError> {
        if !self.group.voters.contains(&self.local) {
            return Err(ElectError::NotAVoter);
        }
        if !online.contains(&self.local) {
            return Err(ElectError::CandidateOffline);
        }

        let (new_term, last_idx, last_term) = {
            let peer = self
                .group
                .peer_mut(self.local)
                .ok_or(ElectError::NotAVoter)?;
            peer.current_term = Term(peer.current_term.0 + 1);
            peer.role = RaftRole::Candidate;
            peer.voted_for = Some(self.local);
            (
                peer.current_term,
                peer.last_log_index(),
                peer.last_log_term(),
            )
        };
        if self.group.flush_peer(self.local).is_err() {
            if let Some(p) = self.group.peer_mut(self.local) {
                p.role = RaftRole::Follower;
            }
            return Err(ElectError::PersistFailed);
        }

        let mut votes = 1u32;
        let mut saw_higher = None;
        let voters = self.group.voters.clone();
        let req = RequestVoteRequest {
            cluster_id: self.cluster_hex(),
            partition: self.group.partition.0,
            placement_epoch: self.group.placement_epoch.0,
            term: new_term.0,
            candidate_id: self.local.index(),
            last_log_index: last_idx,
            last_log_term: last_term.0,
        };

        for voter in voters {
            if voter == self.local {
                continue;
            }
            if !online.contains(&voter) {
                continue;
            }
            match transport.request_vote(voter, &req) {
                Ok(res) => {
                    self.peer_failures.insert(voter.index(), 0);
                    if res.term > new_term.0 {
                        saw_higher = Some(Term(res.term));
                        break;
                    }
                    if res.vote_granted {
                        votes += 1;
                    }
                }
                Err(_) => {
                    let c = self.peer_failures.entry(voter.index()).or_insert(0);
                    *c = c.saturating_add(1);
                }
            }
        }

        if let Some(higher) = saw_higher {
            if let Some(p) = self.group.peer_mut(self.local) {
                p.current_term = higher;
                p.role = RaftRole::Follower;
                p.voted_for = None;
            }
            let _ = self.group.flush_peer(self.local);
            return Err(ElectError::HigherTerm(higher));
        }

        if votes >= self.quorum() {
            let voters = self.group.voters.clone();
            if let Some(peer) = self.group.peer_mut(self.local) {
                peer.current_term = new_term;
                peer.role = RaftRole::Leader;
                // Initialize leader volatile state.
                let next = peer.last_log_index() + 1;
                peer.next_index.clear();
                peer.match_index.clear();
                for v in &voters {
                    peer.next_index.insert(v.index(), next);
                    peer.match_index.insert(v.index(), 0);
                }
                peer.match_index
                    .insert(self.local.index(), peer.last_log_index());
                peer.next_index
                    .insert(self.local.index(), peer.last_log_index() + 1);
            }
            let _ = self.group.flush_peer(self.local);
            Ok((self.local, new_term))
        } else {
            if let Some(peer) = self.group.peer_mut(self.local) {
                peer.role = RaftRole::Follower;
            }
            let _ = self.group.flush_peer(self.local);
            Err(ElectError::NoQuorum {
                votes,
                need: self.quorum(),
            })
        }
    }

    /// Leader proposes `command` and replicates via `transport`.
    ///
    /// When `operation_id` is set, a successful prior propose with the same id
    /// returns the prior commit evidence (response-loss retry safety).
    pub fn propose(
        &mut self,
        command: LogCommand,
        transport: &dyn RaftTransport,
        online: &[NodeId],
        operation_id: Option<&str>,
    ) -> Result<ProposeResult, ProposeError> {
        if let Some(oid) = operation_id {
            if let Some(&idx) = self.propose_dedup.get(oid) {
                if let Some(peer) = self.group.peer(self.local) {
                    if peer.role == RaftRole::Leader && peer.commit_index >= idx {
                        if let Some(entry) = peer.entry_at(idx).cloned() {
                            return Ok(ProposeResult {
                                entry: entry.clone(),
                                term: entry.term,
                                position: LogPosition(idx),
                                replica_acks: self.quorum(),
                                acked_by: vec![self.local],
                                committed: true,
                                commit_index: peer.commit_index,
                            });
                        }
                    }
                }
            }
        }

        let (term, new_index, entry) = {
            let peer = self
                .group
                .peer_mut(self.local)
                .ok_or(ProposeError::NotLeader)?;
            if peer.role != RaftRole::Leader {
                return Err(ProposeError::NotLeader);
            }
            let term = peer.current_term;
            let new_index = peer.last_log_index() + 1;
            let entry = LogEntry {
                term,
                index: new_index,
                command,
            };
            peer.log.push(entry.clone());
            peer.match_index.insert(self.local.index(), new_index);
            peer.next_index.insert(self.local.index(), new_index + 1);
            (term, new_index, entry)
        };
        if self.group.flush_peer(self.local).is_err() {
            if let Some(peer) = self.group.peer_mut(self.local) {
                if peer.last_log_index() == new_index {
                    peer.log.pop();
                }
                peer.match_index
                    .insert(self.local.index(), peer.last_log_index());
                peer.next_index
                    .insert(self.local.index(), peer.last_log_index() + 1);
            }
            return Err(ProposeError::PersistFailed);
        }

        let mut acked = vec![self.local];
        let followers: Vec<NodeId> = self
            .group
            .voters
            .iter()
            .copied()
            .filter(|v| *v != self.local && online.iter().any(|n| n == v))
            .collect();

        for follower in followers {
            match self.replicate_to(follower, transport) {
                Ok(()) => {
                    let match_idx = self
                        .group
                        .peer(self.local)
                        .and_then(|p| p.match_index.get(&follower.index()).copied())
                        .unwrap_or(0);
                    if match_idx >= new_index {
                        acked.push(follower);
                    }
                }
                Err(ProposeError::SteppedDown(t)) => return Err(ProposeError::SteppedDown(t)),
                Err(_) => {}
            }
        }

        self.advance_commit();
        let commit_index = self.commit_index();
        let committed = commit_index >= new_index;

        if committed {
            for follower in self.group.voters.clone() {
                if follower == self.local {
                    continue;
                }
                if !online.contains(&follower) {
                    continue;
                }
                let _ = self.replicate_to(follower, transport);
            }
            if let Some(oid) = operation_id {
                self.propose_dedup.insert(oid.to_string(), new_index);
            }
        }

        Ok(ProposeResult {
            entry,
            term,
            position: LogPosition(new_index),
            replica_acks: acked.len() as u32,
            acked_by: acked,
            committed,
            commit_index,
        })
    }

    fn replicate_to(
        &mut self,
        follower: NodeId,
        transport: &dyn RaftTransport,
    ) -> Result<(), ProposeError> {
        for _ in 0..self.config.replicate_attempts {
            let (term, prev_idx, prev_term, entries, leader_commit) = {
                let peer = self.group.peer(self.local).ok_or(ProposeError::NotLeader)?;
                if peer.role != RaftRole::Leader {
                    return Err(ProposeError::NotLeader);
                }
                let next = *peer.next_index.get(&follower.index()).unwrap_or(&1);
                let prev_idx = next.saturating_sub(1);
                let prev_term = peer.term_at(prev_idx).unwrap_or(Term(0));
                let mut entries: Vec<LogEntry> = peer
                    .log
                    .iter()
                    .filter(|e| e.index >= next)
                    .cloned()
                    .collect();
                if entries.len() > self.config.max_append_batch {
                    entries.truncate(self.config.max_append_batch);
                }
                (
                    peer.current_term,
                    prev_idx,
                    prev_term,
                    entries,
                    peer.commit_index,
                )
            };

            let req = AppendEntriesRequest {
                cluster_id: self.cluster_hex(),
                partition: self.group.partition.0,
                placement_epoch: self.group.placement_epoch.0,
                term: term.0,
                leader_id: self.local.index(),
                prev_log_index: prev_idx,
                prev_log_term: prev_term.0,
                entries: entries.clone(),
                leader_commit,
            };

            let res = match transport.append_entries(follower, &req) {
                Ok(r) => {
                    self.peer_failures.insert(follower.index(), 0);
                    r
                }
                Err(_) => {
                    let c = self.peer_failures.entry(follower.index()).or_insert(0);
                    *c = c.saturating_add(1);
                    return Ok(());
                }
            };

            if res.term > term.0 {
                if let Some(p) = self.group.peer_mut(self.local) {
                    p.current_term = Term(res.term);
                    p.role = RaftRole::Follower;
                    p.voted_for = None;
                }
                let _ = self.group.flush_peer(self.local);
                return Err(ProposeError::SteppedDown(Term(res.term)));
            }

            if res.success {
                let match_idx = res.match_index.unwrap_or_else(|| {
                    if entries.is_empty() {
                        prev_idx
                    } else {
                        entries.last().map(|e| e.index).unwrap_or(prev_idx)
                    }
                });
                if let Some(p) = self.group.peer_mut(self.local) {
                    p.match_index.insert(follower.index(), match_idx);
                    p.next_index.insert(follower.index(), match_idx + 1);
                }
                return Ok(());
            }

            if let Some(p) = self.group.peer_mut(self.local) {
                let next = p.next_index.entry(follower.index()).or_insert(1);
                let hint = res.conflict_index.unwrap_or(next.saturating_sub(1));
                *next = hint.max(1);
                if *next > 1 {
                    *next = (*next).saturating_sub(1).max(1);
                }
            }
        }
        Ok(())
    }

    fn advance_commit(&mut self) {
        let Some(peer) = self.group.peer(self.local) else {
            return;
        };
        if peer.role != RaftRole::Leader {
            return;
        }
        let term = peer.current_term;
        let last = peer.last_log_index();
        let mut new_commit = peer.commit_index;
        let voters = self.group.voters.clone();
        let quorum = self.quorum();

        for n in (peer.commit_index + 1..=last).rev() {
            let entry_term = match peer.term_at(n) {
                Some(t) => t,
                None => continue,
            };
            if entry_term != term {
                continue;
            }
            let mut count = 0u32;
            for v in &voters {
                let m = peer.match_index.get(&v.index()).copied().unwrap_or(0);
                if m >= n {
                    count += 1;
                }
            }
            if count >= quorum {
                new_commit = n;
                break;
            }
        }

        if new_commit > peer.commit_index {
            if let Some(p) = self.group.peer_mut(self.local) {
                p.commit_index = new_commit;
            }
            let _ = self.group.flush_peer(self.local);
        }
    }

    /// Send a local snapshot to a follower over the transport.
    pub fn send_snapshot(
        &mut self,
        follower: NodeId,
        blob: Vec<u8>,
        transport: &dyn RaftTransport,
    ) -> Result<InstallSnapshotResponse, RaftRpcError> {
        if !self.is_leader() {
            return Err(RaftRpcError::Fenced("not leader".into()));
        }
        let (term, last_idx, last_term) = {
            let peer = self
                .group
                .peer(self.local)
                .ok_or_else(|| RaftRpcError::Protocol("missing local peer".into()))?;
            (
                peer.current_term,
                peer.last_log_index(),
                peer.last_log_term(),
            )
        };
        let meta = snapshot_meta_for(last_idx, last_term, &blob, "network-send");
        let req = InstallSnapshotRequest {
            cluster_id: self.cluster_hex(),
            partition: self.group.partition.0,
            placement_epoch: self.group.placement_epoch.0,
            term: term.0,
            leader_id: self.local.index(),
            last_included_index: last_idx,
            last_included_term: last_term.0,
            blob_checksum: meta.blob_checksum,
            blob,
        };
        transport.install_snapshot(follower, &req)
    }
}

// ---------------------------------------------------------------------------
// In-process multi-node transport (tests + single-process multi-root sim)
// ---------------------------------------------------------------------------

/// Shared registry of network Raft nodes used by [`MemoryRaftTransport`].
///
/// Each peer is an independent `Mutex` so a leader can hold its own lock while
/// the transport locks remote peers (no global lock across RPCs).
#[derive(Clone, Default)]
pub struct MemoryRaftNetwork {
    inner: Arc<Mutex<MemoryRaftNetworkInner>>,
}

#[derive(Default)]
struct MemoryRaftNetworkInner {
    /// partition → node_index → node
    nodes: HashMap<u32, HashMap<u32, Arc<Mutex<NetworkRaftNode>>>>,
    /// Nodes marked offline (RPCs fail as Unavailable).
    offline: HashSet<u32>,
}

impl MemoryRaftNetwork {
    /// Empty network.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a node for its partition.
    pub fn register(&self, node: NetworkRaftNode) {
        let mut g = self.inner.lock().expect("memory raft network");
        let p = node.partition().0;
        let idx = node.local.index();
        g.nodes
            .entry(p)
            .or_default()
            .insert(idx, Arc::new(Mutex::new(node)));
    }

    /// Mark a dense node index offline (all partitions).
    pub fn mark_offline(&self, node: NodeId) {
        self.inner
            .lock()
            .expect("memory raft network")
            .offline
            .insert(node.index());
    }

    /// Mark a node online again.
    pub fn mark_online(&self, node: NodeId) {
        self.inner
            .lock()
            .expect("memory raft network")
            .offline
            .remove(&node.index());
    }

    /// Whether a node is offline.
    pub fn is_offline(&self, node: NodeId) -> bool {
        self.inner
            .lock()
            .expect("memory raft network")
            .offline
            .contains(&node.index())
    }

    /// Clone the `Arc` for one peer (drops the registry lock before use).
    fn peer_arc(
        &self,
        partition: PartitionId,
        node: NodeId,
    ) -> Option<Arc<Mutex<NetworkRaftNode>>> {
        let g = self.inner.lock().expect("memory raft network");
        g.nodes
            .get(&partition.0)
            .and_then(|m| m.get(&node.index()).cloned())
    }

    /// Online voter list for a partition (excludes offline).
    pub fn online_voters(&self, partition: PartitionId) -> Vec<NodeId> {
        let g = self.inner.lock().expect("memory raft network");
        let Some(part) = g.nodes.get(&partition.0) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for (idx, arc) in part {
            if g.offline.contains(idx) {
                continue;
            }
            // Prefer the map key (always correct) over locking.
            out.push(NodeId::new(*idx));
            let _ = arc;
        }
        out
    }

    /// Mutate a node (locks only that peer).
    pub fn with_node_mut<R>(
        &self,
        partition: PartitionId,
        node: NodeId,
        f: impl FnOnce(&mut NetworkRaftNode) -> R,
    ) -> Option<R> {
        let arc = self.peer_arc(partition, node)?;
        let mut n = arc.lock().expect("raft peer");
        Some(f(&mut n))
    }

    /// Read a node (locks only that peer).
    pub fn with_node<R>(
        &self,
        partition: PartitionId,
        node: NodeId,
        f: impl FnOnce(&NetworkRaftNode) -> R,
    ) -> Option<R> {
        let arc = self.peer_arc(partition, node)?;
        let n = arc.lock().expect("raft peer");
        Some(f(&n))
    }

    /// Campaign `candidate` on `partition` using this network as transport.
    pub fn campaign(
        &self,
        partition: PartitionId,
        candidate: NodeId,
    ) -> Result<(NodeId, Term), ElectError> {
        let online = self.online_voters(partition);
        let transport = MemoryRaftTransport {
            network: self.clone(),
            partition,
        };
        self.with_node_mut(partition, candidate, |n| n.campaign(&transport, &online))
            .ok_or(ElectError::NotAVoter)?
    }

    /// Propose on the current leader (or `leader` if provided).
    pub fn propose(
        &self,
        partition: PartitionId,
        leader: NodeId,
        command: LogCommand,
        operation_id: Option<&str>,
    ) -> Result<ProposeResult, ProposeError> {
        let online = self.online_voters(partition);
        let transport = MemoryRaftTransport {
            network: self.clone(),
            partition,
        };
        self.with_node_mut(partition, leader, |n| {
            n.propose(command, &transport, &online, operation_id)
        })
        .ok_or(ProposeError::NotLeader)?
    }

    /// Find a leader for the partition among online nodes.
    pub fn current_leader(&self, partition: PartitionId) -> Option<(NodeId, Term)> {
        let online = self.online_voters(partition);
        for n in online {
            if let Some((is_leader, term, local)) = self.with_node(partition, n, |node| {
                (node.is_leader(), node.term(), node.local)
            }) {
                if is_leader {
                    return Some((local, term));
                }
            }
        }
        None
    }
}

/// Transport that routes RPCs into a [`MemoryRaftNetwork`].
pub struct MemoryRaftTransport {
    network: MemoryRaftNetwork,
    partition: PartitionId,
}

impl MemoryRaftTransport {
    /// Build a transport for one partition on a shared network.
    pub fn new(network: MemoryRaftNetwork, partition: PartitionId) -> Self {
        Self { network, partition }
    }
}

impl RaftTransport for MemoryRaftTransport {
    fn request_vote(
        &self,
        to: NodeId,
        req: &RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftRpcError> {
        if self.network.is_offline(to) {
            return Err(RaftRpcError::Unavailable(format!("{to} offline")));
        }
        self.network
            .with_node_mut(self.partition, to, |n| n.handle_request_vote(req))
            .ok_or_else(|| RaftRpcError::Unavailable(format!("{to} missing")))?
    }

    fn append_entries(
        &self,
        to: NodeId,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftRpcError> {
        if self.network.is_offline(to) {
            return Err(RaftRpcError::Unavailable(format!("{to} offline")));
        }
        self.network
            .with_node_mut(self.partition, to, |n| n.handle_append_entries(req))
            .ok_or_else(|| RaftRpcError::Unavailable(format!("{to} missing")))?
    }

    fn install_snapshot(
        &self,
        to: NodeId,
        req: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftRpcError> {
        if self.network.is_offline(to) {
            return Err(RaftRpcError::Unavailable(format!("{to} offline")));
        }
        self.network
            .with_node_mut(self.partition, to, |n| n.handle_install_snapshot(req))
            .ok_or_else(|| RaftRpcError::Unavailable(format!("{to} missing")))?
    }

    fn read_index(
        &self,
        to: NodeId,
        req: &ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftRpcError> {
        if self.network.is_offline(to) {
            return Err(RaftRpcError::Unavailable(format!("{to} offline")));
        }
        self.network
            .with_node_mut(self.partition, to, |n| n.handle_read_index(req))
            .ok_or_else(|| RaftRpcError::Unavailable(format!("{to} missing")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{ClusterId, NodeId, PartitionId, PlacementEpoch};
    use crate::raft::LogCommand;
    use tempfile::tempdir;

    fn three_node_net(partition: PartitionId) -> (MemoryRaftNetwork, ClusterId, Vec<NodeId>) {
        let cluster_id = ClusterId::from_seed(b"def-036-test");
        let voters: Vec<NodeId> = (0..3).map(NodeId::new).collect();
        let net = MemoryRaftNetwork::new();
        for v in &voters {
            let node =
                NetworkRaftNode::new(cluster_id, partition, *v, voters.clone(), PlacementEpoch(1));
            net.register(node);
        }
        (net, cluster_id, voters)
    }

    #[test]
    fn network_election_and_quorum_commit() {
        let p = PartitionId(0);
        let (net, _, voters) = three_node_net(p);
        let (leader, term) = net.campaign(p, voters[0]).expect("elect");
        assert_eq!(leader, voters[0]);
        assert!(term.0 >= 1);

        let res = net
            .propose(
                p,
                leader,
                LogCommand::Put {
                    subject: "k".into(),
                    value: b"v1".to_vec(),
                },
                Some("op-aaaa1111bbbb2222cccc3333dddd4444"),
            )
            .expect("propose");
        assert!(res.committed, "majority of 3 must commit");
        assert!(res.replica_acks >= 2);

        // All online followers should have commit advanced.
        for v in &voters {
            let ci = net.with_node(p, *v, |n| n.commit_index()).expect("node");
            assert!(ci >= res.position.0, "node {v} commit {ci}");
        }
    }

    #[test]
    fn minority_cannot_commit() {
        let p = PartitionId(0);
        let (net, _, voters) = three_node_net(p);
        net.mark_offline(voters[1]);
        net.mark_offline(voters[2]);
        // Single node cannot win election (need 2 of 3).
        let err = net.campaign(p, voters[0]).expect_err("no quorum");
        assert!(matches!(err, ElectError::NoQuorum { .. }));
    }

    #[test]
    fn old_leader_cannot_commit_after_new_term() {
        let p = PartitionId(0);
        let (net, _, voters) = three_node_net(p);
        let (old_leader, old_term) = net.campaign(p, voters[0]).expect("elect0");
        net.mark_offline(old_leader);

        // Remaining majority elects a new leader.
        let (new_leader, new_term) = net.campaign(p, voters[1]).expect("elect1");
        assert_ne!(new_leader, old_leader);
        assert!(new_term.0 > old_term.0);

        // New leader commits.
        let res = net
            .propose(
                p,
                new_leader,
                LogCommand::Put {
                    subject: "k".into(),
                    value: b"new".to_vec(),
                },
                None,
            )
            .expect("propose new");
        assert!(res.committed);

        // Old leader comes back; its term is stale — campaign or propose must not
        // clobber the higher term without stepping down.
        net.mark_online(old_leader);
        // Stale leader still thinks it is leader until it hears a higher term.
        // A propose that reaches a follower with higher term must step down.
        // Simulate: old leader tries append; followers reject / return higher term.
        // Force old leader propose — followers have higher term from new election.
        let stale = net.propose(
            p,
            old_leader,
            LogCommand::Put {
                subject: "k".into(),
                value: b"stale".to_vec(),
            },
            None,
        );
        // Either NotLeader (if we stepped it down) or SteppedDown or uncommitted.
        match stale {
            Err(ProposeError::NotLeader) | Err(ProposeError::SteppedDown(_)) => {}
            Ok(r) => assert!(
                !r.committed,
                "stale leader must not commit against higher term majority"
            ),
            Err(e) => panic!("unexpected {e:?}"),
        }

        // Committed value on new majority remains the new one.
        let committed_on_new = net
            .with_node(p, new_leader, |n| {
                n.group()
                    .peer(new_leader)
                    .and_then(|peer| peer.entry_at(r_index_of_put(peer, b"new")))
                    .map(|e| e.command.clone())
            })
            .flatten();
        assert!(committed_on_new.is_some());
    }

    fn r_index_of_put(peer: &crate::raft::RaftPeer, body: &[u8]) -> u64 {
        peer.log
            .iter()
            .rev()
            .find(|e| {
                matches!(
                    &e.command,
                    LogCommand::Put { value, .. } if value == body
                )
            })
            .map(|e| e.index)
            .unwrap_or(0)
    }

    #[test]
    fn operation_id_retry_preserves_one_event() {
        let p = PartitionId(0);
        let (net, _, voters) = three_node_net(p);
        let (leader, _) = net.campaign(p, voters[0]).unwrap();
        let oid = "op-11112222333344445555666677778888";
        let r1 = net
            .propose(
                p,
                leader,
                LogCommand::Put {
                    subject: "x".into(),
                    value: b"once".to_vec(),
                },
                Some(oid),
            )
            .unwrap();
        assert!(r1.committed);
        let r2 = net
            .propose(
                p,
                leader,
                LogCommand::Put {
                    subject: "x".into(),
                    value: b"once".to_vec(),
                },
                Some(oid),
            )
            .unwrap();
        assert!(r2.committed);
        assert_eq!(r1.position, r2.position);
        assert_eq!(r1.entry.index, r2.entry.index);
    }

    #[test]
    fn epoch_fence_rejects_stale_placement() {
        let p = PartitionId(0);
        let (net, cluster_id, voters) = three_node_net(p);
        let bad = RequestVoteRequest {
            cluster_id: cluster_id.to_hex(),
            partition: p.0,
            placement_epoch: 99,
            term: 1,
            candidate_id: voters[0].index(),
            last_log_index: 0,
            last_log_term: 0,
        };
        let err = net
            .with_node_mut(p, voters[1], |n| n.handle_request_vote(&bad))
            .unwrap()
            .expect_err("epoch fence");
        assert_eq!(err.code(), "raft_fenced");
    }

    #[test]
    fn durable_network_nodes_recover_votes() {
        let dir = tempdir().unwrap();
        let cluster_id = ClusterId::from_seed(b"durable-net");
        let p = PartitionId(3);
        let voters: Vec<NodeId> = (0..3).map(NodeId::new).collect();
        let net = MemoryRaftNetwork::new();
        for v in &voters {
            let store = RaftPeerStore::open(dir.path(), *v, p).unwrap();
            let mut node =
                NetworkRaftNode::new(cluster_id, p, *v, voters.clone(), PlacementEpoch(1));
            node.attach_store(store).unwrap();
            net.register(node);
        }
        let (leader, _) = net.campaign(p, voters[0]).unwrap();
        let res = net
            .propose(
                p,
                leader,
                LogCommand::Put {
                    subject: "d".into(),
                    value: b"1".to_vec(),
                },
                None,
            )
            .unwrap();
        assert!(res.committed);

        // Reload each store — terms/votes/logs durable.
        for v in &voters {
            let store = RaftPeerStore::open(dir.path(), *v, p).unwrap();
            let peer = store.load_peer().unwrap();
            assert!(peer.current_term.0 >= 1);
            // Log and hard state must reopen without inventing an empty peer.
            assert!(peer.last_log_index() >= 1 || peer.commit_index >= 1);
        }
    }

    #[test]
    fn snapshot_rpc_installs_on_follower() {
        let p = PartitionId(0);
        let (net, _, voters) = three_node_net(p);
        let (leader, _) = net.campaign(p, voters[0]).unwrap();
        let _ = net
            .propose(
                p,
                leader,
                LogCommand::Put {
                    subject: "s".into(),
                    value: b"snap".to_vec(),
                },
                None,
            )
            .unwrap();

        let transport = MemoryRaftTransport::new(net.clone(), p);
        let blob = b"sm-bytes".to_vec();
        let resp = net
            .with_node_mut(p, leader, |n| {
                n.send_snapshot(voters[2], blob.clone(), &transport)
            })
            .unwrap()
            .unwrap();
        assert!(resp.success);
    }

    #[test]
    fn read_index_reports_leader() {
        let p = PartitionId(0);
        let (net, cluster_id, voters) = three_node_net(p);
        let (leader, term) = net.campaign(p, voters[0]).unwrap();
        let req = ReadIndexRequest {
            cluster_id: cluster_id.to_hex(),
            partition: p.0,
            placement_epoch: 1,
            term: term.0,
            from_id: 0,
        };
        let resp = net
            .with_node_mut(p, leader, |n| n.handle_read_index(&req))
            .unwrap()
            .unwrap();
        assert!(resp.is_leader);
        assert_eq!(resp.leader_id, Some(leader.index()));
    }
}
