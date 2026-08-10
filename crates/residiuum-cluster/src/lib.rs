//! Residiuum cluster federation (Stage 8 + product freezes).
//!
//! A cluster is a **federation of independently recoverable partitions and
//! segments** ([CLUSTER_SPEC](../../CLUSTER_SPEC.md)). Consensus decides who may
//! coordinate writes; it does not give bytes their meaning.
//!
//! ## Stage 8 surface
//!
//! - Deterministic virtual partition map ([`PartitionMap`])
//! - Consistency / read / commit modes
//! - [`Coverage`] on every multi-partition result
//! - Placement [`PartitionDirectory`]
//! - In-process multi-node [`Cluster`] (development + dependable-local)
//! - Per-partition Raft-equivalent elections, log matching, and commit evidence
//!   ([`raft`] module; CLUSTER_SPEC §10)
//! - Durable Raft hard state / log / membership / snapshots
//!   ([`raft_persist`]; DEF-035 / `residiuum-raft-persist-v1`)
//! - Network Raft RPC types + single-peer node over a transport
//!   ([`raft_rpc`]; DEF-036 / `residiuum-raft-rpc-v1`)
//! - Convergent-append dual-accept + reconcile ([`convergent`]; CLUSTER_SPEC §9.2)
//! - Distributed find/scan with coverage honesty (Stage 8e; CLUSTER_SPEC §17)
//! - Multi-page distributed query continuation + deterministic merge
//!   ([`QUERY_CONTINUATION_PROFILE`]; DEF-040 / CLUSTER_SPEC §17.3–§17.4)
//! - Interruptible partition rebalance (Stage 8f; CLUSTER_SPEC §14)
//! - Durable rebalance jobs + joint membership restore (DEF-038 /
//!   `residiuum-rebalance-control-v1`)
//! - Anti-entropy inventory + replica repair (DEF-039 /
//!   `residiuum-anti-entropy-v1`)
//! - Deterministic verification harness: seeded faults, history, linearizability
//!   ([`sim`]; DEF-041 / `residiuum-cluster-verify-v1`)
//! - Multi-process OS chaos + short soak history dumps
//!   ([`multiproc`]; DEF-041-N / `residiuum-cluster-multiproc-v1`)
//! - Multi-partition batch put ([`Cluster::put_many`]; product capacity path /
//!   WORK_HORIZON S3 — independent partition leaders, honest acks)
//! - Node-local salvage without cluster software
//! - Network advertise path: [`endpoints`] + `residiuum serve-cluster`
//!
//! **Product freeze:** [`CLUSTER_PROFILE_VERSION`] labels the Stage 8
//! conformance profile (no cross-partition atomic writes; CLUSTER_SPEC).

#![deny(missing_docs)]

/// Cluster federation profile freeze label (DELIVERY_PLAN §7: Cluster profile v1).
///
/// Stage 8a–8f conformance is locked: partitions, coverage, Raft-equivalent
/// leadership, convergent-append, find coverage, rebalance. Network multi-hop
/// continues to harden on the same rules without changing this label.
pub const CLUSTER_PROFILE_VERSION: &str = "v1";

mod ack;
mod cluster;
mod config;
mod convergent;
mod coverage;
mod directory;
mod endpoints;
mod error;
mod id;
mod modes;
pub mod multiproc;
mod partition;
pub mod raft;
pub mod raft_persist;
pub mod raft_rpc;
mod rebalance;
mod repair;
pub mod sim;

pub use ack::ClusterWriteAck;
pub use cluster::{Cluster, ClusterHealth};
pub use config::{node_store_path, ClusterConfig, ClusterMeta};
pub use convergent::{
    body_content_hash, ConvergentIdentity, ReconcileReport, SubjectConflict, SubjectVariant,
};
pub use coverage::{
    Coverage, FindResult, GetResult, PartitionFrontier, QueryContinuation, ScanOptions, ScanResult,
    DEFAULT_FIND_PAGE_SIZE, MAX_FIND_PAGE_SIZE, QUERY_CONTINUATION_PROFILE,
};
pub use directory::{PartitionAssignment, PartitionDirectory};
pub use endpoints::{
    load_endpoints, save_endpoints, upsert_endpoint, upsert_endpoint_authenticated, EndpointsFile,
    ENDPOINTS_FILE,
};
pub use error::ClusterError;
pub use id::{ClusterId, LogPosition, NodeId, PartitionId, PlacementEpoch, Term};
pub use modes::{CommitStatus, ConsistencyMode, DeploymentProfile, ReadMode};
pub use multiproc::{hex16 as multiproc_hex16, MultiprocHistory, MultiprocOp, MULTIPROC_PROFILE};
pub use partition::{
    default_partition_key, PartitionMap, DEFAULT_VIRTUAL_PARTITIONS, HASH_PROFILE_BLAKE3_MOD,
};
pub use raft::{
    CommitEvidence, ElectError, LogCommand, LogEntry, PartitionRaft, ProposeError, ProposeResult,
    RaftPeer, RaftRole,
};
pub use raft_persist::{
    snapshot_meta_for, ConsensusEvidenceClass, HardState, MembershipState, RaftPeerStore, Snapshot,
    SnapshotMeta, RAFT_PERSIST_PROFILE,
};
pub use raft_rpc::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    MemoryRaftNetwork, MemoryRaftTransport, NetworkRaftNode, RaftRpcConfig, RaftRpcError,
    RaftTransport, ReadIndexRequest, ReadIndexResponse, RequestVoteRequest, RequestVoteResponse,
    DEFAULT_MAX_APPEND_BATCH, DEFAULT_PEER_BACKOFF_MAX_MS, DEFAULT_PEER_BACKOFF_MS,
    DEFAULT_REPLICATE_ATTEMPTS, RAFT_RPC_PROFILE,
};
pub use rebalance::{
    RebalanceJob, RebalanceJobsFile, RebalancePhase, RebalanceReport, REBALANCE_CONTROL_PROFILE,
    REBALANCE_JOBS_FILE,
};
pub use repair::{
    hash_hex, select_repair_source, ClusterInventory, NodeSubjectView, PartitionInventory,
    RepairActionKind, RepairAuditEntry, RepairAuditFile, RepairOptions, RepairReport,
    ReplicaObservation, SourceSelectError, SubjectInventory, ANTI_ENTROPY_PROFILE,
    REPAIR_AUDIT_FILE,
};
pub use sim::{
    check_convergent_preserved, check_partition_linearizable, run_conformance_matrix, CaseReport,
    ClientOp, ConformanceCase, ConvergentVariant, FaultModel, HistoryEntry, LinError, OpOutcome,
    SeedRng, SimConfig, SimEvent, SimWorld, VERIFY_PROFILE,
};

/// Re-export durability modes used on cluster acks.
pub use residiuum_store::DurabilityMode;
