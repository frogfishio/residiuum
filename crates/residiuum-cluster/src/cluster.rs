//! In-process multi-node cluster (Stage 8a–8f).
//!
//! Each node is an ordinary [`residiuum_store::Store`]. Writes in
//! `partition-linearizable` mode go through a per-partition Raft-equivalent
//! group (elections, log matching, majority commit), then apply committed
//! commands to replica stores.
//!
//! Stage 8a used static primary leadership from the placement directory.
//! Stage 8b elects leaders dynamically when the prior leader is offline and
//! a majority of configured voters remains reachable.
//! Stage 8c adds `convergent-append`: any online replica may accept unique
//! events without quorum; dual-accept across a split is allowed; reconcile
//! merges by subject + content hash and reports conflicts explicitly.
//! Stage 8e adds distributed find with coverage honesty on partial queries.
//! Stage 8f adds interruptible partition rebalance (CLUSTER_SPEC §14).
//! DEF-039 adds anti-entropy inventory and integrity-based replica repair.
//! DEF-040 adds multi-page find with integrity-tagged continuation and
//! deterministic merge independent of worker visit order. Attacker-resistant
//! token authentication remains DEF-097.

use crate::ack::ClusterWriteAck;
use crate::config::{node_store_path, ClusterConfig, ClusterMeta};
use crate::convergent::{body_content_hash, ReconcileReport, SubjectConflict, SubjectVariant};
use crate::coverage::{
    Coverage, FindResult, GetResult, QueryContinuation, ScanOptions, ScanResult,
    DEFAULT_FIND_PAGE_SIZE, MAX_FIND_PAGE_SIZE,
};
use crate::directory::PartitionDirectory;
use crate::error::ClusterError;
use crate::id::{ClusterId, LogPosition, NodeId, PartitionId, PlacementEpoch, Term};
use crate::modes::{CommitStatus, ConsistencyMode, DeploymentProfile, ReadMode};
use crate::partition::{default_partition_key, PartitionMap};
use crate::raft::{ElectError, LogCommand, PartitionRaft, ProposeError};
use crate::raft_persist::RaftPeerStore;
use crate::rebalance::{
    RebalanceJob, RebalanceJobsFile, RebalancePhase, RebalanceReport, REBALANCE_CONTROL_PROFILE,
};
use crate::repair::{
    hash_hex, select_repair_source, ClusterInventory, NodeSubjectView, PartitionInventory,
    RepairActionKind, RepairAuditEntry, RepairAuditFile, RepairOptions, RepairReport,
    ReplicaObservation, SourceSelectError, SubjectInventory, ANTI_ENTROPY_PROFILE,
};
use residiuum_store::{DurabilityMode, SalvageReport, Store};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Open cluster handle: federation of independently salvageable stores.
pub struct Cluster {
    root: PathBuf,
    cluster_id: ClusterId,
    profile: DeploymentProfile,
    consistency_mode: ConsistencyMode,
    partition_map: PartitionMap,
    directory: PartitionDirectory,
    /// Expected node count from `cluster.json` (DEF-038 degraded open).
    expected_node_count: u32,
    /// Live nodes keyed by dense index. Absent / offline nodes are missing.
    nodes: HashMap<u32, Store>,
    /// Offline node ids (explicitly marked down for tests / ops).
    offline: Vec<u32>,
    /// Node indexes expected but missing store paths at open (DEF-038).
    missing_nodes: Vec<u32>,
    /// Per-partition Raft groups (Stage 8b; unused for convergent writes).
    raft: HashMap<u32, PartitionRaft>,
    /// Partition-local accept counters for convergent-append (not Raft index).
    convergent_pos: HashMap<u32, u64>,
    /// In-flight rebalance jobs keyed by partition id (Stage 8f / DEF-038).
    rebalance_jobs: HashMap<u32, RebalanceJob>,
    /// Secret continuation-token keyring (DEF-097). Control-plane only.
    token_keyring: residiuum_store::ContinuationKeyring,
}

/// Explicit health / degraded-state snapshot after open or during operation (DEF-038).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterHealth {
    /// Expected node count from cluster metadata.
    pub expected_nodes: u32,
    /// Currently online nodes.
    pub online: Vec<NodeId>,
    /// Explicitly offline (marked down).
    pub offline: Vec<NodeId>,
    /// Expected indexes with no on-disk store at last open.
    pub missing_store_paths: Vec<NodeId>,
    /// In-flight rebalance jobs (partition → phase name).
    pub rebalance_phases: Vec<(PartitionId, RebalancePhase)>,
    /// True when any expected node is offline or missing.
    pub degraded: bool,
}

impl Cluster {
    /// Create a new cluster at `cfg.root` (must not already exist).
    pub fn create(cfg: ClusterConfig) -> Result<Self, ClusterError> {
        let root = cfg.root.clone();
        if root.exists() {
            if root.join("cluster.json").is_file() {
                return Err(ClusterError::AlreadyExists(root.display().to_string()));
            }
        } else {
            std::fs::create_dir_all(&root)?;
        }

        let cluster_id = match cfg.cluster_id {
            Some(id) => id,
            None => ClusterId::generate()?,
        };

        let meta = ClusterMeta::from_config(&cfg, cluster_id);
        meta.write(&root)?;

        let node_count = cfg.profile.default_node_count();
        std::fs::create_dir_all(root.join("nodes"))?;

        let mut nodes = HashMap::new();
        for i in 0..node_count {
            let path = node_store_path(&root, i);
            let store = Store::create(&path)?;
            nodes.insert(i, store);
        }

        let directory = PartitionDirectory::balanced(
            cfg.partition_map.clone(),
            node_count,
            PlacementEpoch(meta.placement_epoch),
        );
        directory.save(&root)?;

        let mut raft = Self::build_raft_groups(&directory);
        Self::attach_and_seed_raft_stores(&root, &directory, &mut raft)?;

        // Empty durable rebalance control plane at create.
        RebalanceJobsFile::new().save(&root)?;
        // DEF-097: mint cluster continuation secrets (control plane).
        let token_keyring = residiuum_store::ContinuationKeyring::mint_new().map_err(|e| {
            ClusterError::Io(std::io::Error::other(format!("token keyring mint: {e}")))
        })?;
        token_keyring.save_cluster(&root).map_err(|e| {
            ClusterError::Io(std::io::Error::other(format!("token keyring save: {e}")))
        })?;

        Ok(Self {
            root,
            cluster_id,
            profile: cfg.profile,
            consistency_mode: cfg.consistency_mode,
            partition_map: cfg.partition_map,
            directory,
            expected_node_count: node_count,
            nodes,
            offline: Vec::new(),
            missing_nodes: Vec::new(),
            raft,
            convergent_pos: HashMap::new(),
            rebalance_jobs: HashMap::new(),
            token_keyring,
        })
    }

    /// Open an existing cluster root, bringing all on-disk nodes online.
    ///
    /// Restores durable rebalance jobs and joint membership (DEF-038). Missing
    /// expected node stores are recorded in [`Cluster::health`]; open still
    /// succeeds so operators can inspect degraded state rather than failing
    /// silently with a full-looking directory.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ClusterError> {
        let root = root.as_ref().to_path_buf();
        let meta = ClusterMeta::load(&root)?;
        let cluster_id = ClusterId::from_hex(&meta.cluster_id)
            .ok_or(ClusterError::CorruptMeta("bad cluster_id hex"))?;
        let profile = match meta.profile.as_str() {
            "development" => DeploymentProfile::Development,
            "dependable-local" => DeploymentProfile::DependableLocal,
            _ => return Err(ClusterError::CorruptMeta("unknown profile")),
        };
        let consistency_mode = ConsistencyMode::parse(&meta.consistency_mode)
            .ok_or(ClusterError::CorruptMeta("unknown consistency mode"))?;
        let partition_map = PartitionMap {
            virtual_partitions: meta.virtual_partitions,
            hash_profile: meta.hash_profile,
        };
        let directory = match PartitionDirectory::load(&root)? {
            Some(d) => d,
            None => {
                // Refuse silent synthetic placement when meta claimed a prior
                // epoch with expected nodes — expose via missing placement.
                if meta.placement_epoch > 1 || meta.node_count > 1 {
                    return Err(ClusterError::CorruptMeta(
                        "placement.json missing; reconstruct or restore control plane",
                    ));
                }
                let d = PartitionDirectory::balanced(
                    partition_map.clone(),
                    meta.node_count,
                    PlacementEpoch(meta.placement_epoch),
                );
                d.save(&root)?;
                d
            }
        };

        let mut nodes = HashMap::new();
        let mut missing_nodes = Vec::new();
        for i in 0..meta.node_count {
            let path = node_store_path(&root, i);
            if path.exists() {
                let store = Store::open(&path)?;
                nodes.insert(i, store);
            } else {
                missing_nodes.push(i);
            }
        }

        let jobs_file = RebalanceJobsFile::load(&root)?;
        let mut rebalance_jobs = HashMap::new();
        for job in jobs_file.jobs {
            rebalance_jobs.insert(job.partition.get(), job);
        }

        let mut raft = Self::build_raft_groups(&directory);
        Self::attach_and_restore_raft_stores(&root, &directory, &mut raft)?;
        Self::restore_rebalance_raft_state(&root, &rebalance_jobs, &mut raft)?;

        let token_keyring = residiuum_store::ContinuationKeyring::load_or_mint_cluster(&root)
            .map_err(|e| {
                ClusterError::Io(std::io::Error::other(format!("token keyring load: {e}")))
            })?;

        Ok(Self {
            root,
            cluster_id,
            profile,
            consistency_mode,
            partition_map,
            directory,
            expected_node_count: meta.node_count,
            nodes,
            offline: Vec::new(),
            missing_nodes,
            raft,
            convergent_pos: HashMap::new(),
            rebalance_jobs,
            token_keyring,
        })
    }

    fn build_raft_groups(directory: &PartitionDirectory) -> HashMap<u32, PartitionRaft> {
        let mut raft = HashMap::new();
        for a in &directory.assignments {
            raft.insert(
                a.partition.get(),
                PartitionRaft::new(a.partition, a.replicas.clone(), a.placement_epoch),
            );
        }
        raft
    }

    /// Attach empty durable stores and seed membership (cluster create).
    fn attach_and_seed_raft_stores(
        root: &Path,
        directory: &PartitionDirectory,
        raft: &mut HashMap<u32, PartitionRaft>,
    ) -> Result<(), ClusterError> {
        for a in &directory.assignments {
            let Some(group) = raft.get_mut(&a.partition.get()) else {
                continue;
            };
            for node in &a.replicas {
                let store = RaftPeerStore::open(root, *node, a.partition)?;
                group.attach_store(store);
            }
            group.persist_membership()?;
            group.flush_all()?;
        }
        Ok(())
    }

    /// Attach stores and restore hard state / logs after reopen (DEF-035).
    fn attach_and_restore_raft_stores(
        root: &Path,
        directory: &PartitionDirectory,
        raft: &mut HashMap<u32, PartitionRaft>,
    ) -> Result<(), ClusterError> {
        for a in &directory.assignments {
            let Some(group) = raft.get_mut(&a.partition.get()) else {
                continue;
            };
            for node in &a.replicas {
                let store = RaftPeerStore::open(root, *node, a.partition)?;
                group.attach_store(store);
                group.restore_peer_from_store(*node)?;
            }
            // Directory placement is authoritative unless a durable joint
            // rebalance job overrides it in [`restore_rebalance_raft_state`].
            // Prefer durable joint membership when peer store recorded it.
            if group.joint {
                // Keep restored joint voters from membership.json.
            } else {
                group.set_voters(a.replicas.clone(), a.placement_epoch);
            }
        }
        Ok(())
    }

    /// Re-apply durable rebalance jobs onto Raft groups after open (DEF-038).
    ///
    /// Attaches destination learner stores, restores joint voter sets, and
    /// ensures old placement stays authoritative in the directory until epoch
    /// activation (directory is not rewritten here).
    fn restore_rebalance_raft_state(
        root: &Path,
        jobs: &HashMap<u32, RebalanceJob>,
        raft: &mut HashMap<u32, PartitionRaft>,
    ) -> Result<(), ClusterError> {
        for job in jobs.values() {
            let Some(group) = raft.get_mut(&job.partition.get()) else {
                continue;
            };
            // Learners / destinations need peer stores even before activation.
            for d in &job.destinations {
                let store = RaftPeerStore::open(root, *d, job.partition)?;
                group.attach_store(store);
                group.ensure_peer(*d);
                let _ = group.restore_peer_from_store(*d);
            }
            for n in &job.old_replicas {
                if group.peer(*n).is_none() {
                    let store = RaftPeerStore::open(root, *n, job.partition)?;
                    group.attach_store(store);
                    group.ensure_peer(*n);
                    let _ = group.restore_peer_from_store(*n);
                }
            }
            if job.phase.is_joint() || job.joint {
                group.set_joint_voters(
                    job.old_replicas.clone(),
                    job.new_replicas.clone(),
                    job.plan_epoch,
                );
            } else if job.phase.new_placement_authoritative() {
                let epoch = job.activated_epoch.unwrap_or(job.plan_epoch);
                group.set_voters(job.new_replicas.clone(), epoch);
            } else {
                // Old placement still sole authoritative membership.
                group.set_voters(job.old_replicas.clone(), job.plan_epoch);
            }
        }
        Ok(())
    }

    fn persist_rebalance_jobs(&self) -> Result<(), ClusterError> {
        let jobs: Vec<RebalanceJob> = self.rebalance_jobs.values().cloned().collect();
        RebalanceJobsFile::from_jobs(jobs).save(&self.root)
    }

    /// Cluster root path.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Cluster identifier.
    pub fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    /// Secret continuation keyring for this cluster (DEF-097).
    ///
    /// Not for logging; used by control-plane token encode/decode and tests.
    pub fn continuation_keyring(&self) -> &residiuum_store::ContinuationKeyring {
        &self.token_keyring
    }

    /// Rotate cluster continuation-token secrets (DEF-097).
    pub fn rotate_continuation_keys(&mut self) -> Result<u32, ClusterError> {
        let id = self.token_keyring.rotate().map_err(|e| {
            ClusterError::Io(std::io::Error::other(format!("token keyring rotate: {e}")))
        })?;
        self.token_keyring.save_cluster(&self.root).map_err(|e| {
            ClusterError::Io(std::io::Error::other(format!("token keyring save: {e}")))
        })?;
        Ok(id)
    }

    /// Deployment profile.
    pub fn profile(&self) -> DeploymentProfile {
        self.profile
    }

    /// Consistency mode.
    pub fn consistency_mode(&self) -> ConsistencyMode {
        self.consistency_mode
    }

    /// Partition map.
    pub fn partition_map(&self) -> &PartitionMap {
        &self.partition_map
    }

    /// Placement directory (derived routing; leader fields refreshed by Raft).
    pub fn directory(&self) -> &PartitionDirectory {
        &self.directory
    }

    /// Raft group for a partition (Stage 8b diagnostics / tests).
    pub fn raft_group(&self, partition: PartitionId) -> Option<&PartitionRaft> {
        self.raft.get(&partition.get())
    }

    /// Whether this profile can claim replicated durability.
    pub fn replicated_durability_available(&self) -> bool {
        self.profile != DeploymentProfile::Development && self.online_node_count() >= 2
    }

    /// Write quorum for the configured profile (floor(N/2)+1).
    pub fn write_quorum(&self) -> u32 {
        self.profile.write_quorum()
    }

    /// Number of nodes currently online.
    pub fn online_node_count(&self) -> u32 {
        self.nodes.len() as u32
    }

    /// Online node ids (sorted).
    pub fn online_nodes(&self) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self.nodes.keys().copied().map(NodeId::new).collect();
        v.sort();
        v
    }

    /// Path of a node store directory (exists even if offline).
    pub fn node_path(&self, node: NodeId) -> PathBuf {
        node_store_path(&self.root, node.index())
    }

    /// Read a subject directly from one online node's store (no Raft / coverage).
    ///
    /// Used by anti-entropy diagnostics and tests that inject per-replica faults.
    pub fn store_get_local(
        &self,
        node: NodeId,
        subject: &str,
    ) -> Result<Option<Vec<u8>>, ClusterError> {
        let store = self
            .nodes
            .get(&node.index())
            .ok_or(ClusterError::PartitionUnavailable {
                partition: 0,
                reason: "node offline for local get",
            })?;
        Ok(store.get(subject)?)
    }

    /// Write a subject directly to one online node's store (bypasses quorum).
    ///
    /// For repair inject tests and operator-controlled single-replica writes.
    /// Does **not** update Raft logs — subsequent anti-entropy may repair peers.
    pub fn store_put_local(
        &mut self,
        node: NodeId,
        subject: &str,
        value: &[u8],
        mode: DurabilityMode,
    ) -> Result<(), ClusterError> {
        let store =
            self.nodes
                .get_mut(&node.index())
                .ok_or(ClusterError::PartitionUnavailable {
                    partition: 0,
                    reason: "node offline for local put",
                })?;
        store.put(subject, value, mode)?;
        Ok(())
    }

    /// Delete a subject on one online node's store (bypasses quorum).
    pub fn store_delete_local(
        &mut self,
        node: NodeId,
        subject: &str,
        mode: DurabilityMode,
    ) -> Result<(), ClusterError> {
        let store =
            self.nodes
                .get_mut(&node.index())
                .ok_or(ClusterError::PartitionUnavailable {
                    partition: 0,
                    reason: "node offline for local delete",
                })?;
        store.delete(subject, mode)?;
        Ok(())
    }

    /// List live subject keys on one online node (UTF-8 subjects only).
    pub fn store_live_subjects_local(&self, node: NodeId) -> Result<Vec<String>, ClusterError> {
        let store = self
            .nodes
            .get(&node.index())
            .ok_or(ClusterError::PartitionUnavailable {
                partition: 0,
                reason: "node offline for local list",
            })?;
        let mut out = Vec::new();
        for (k, _) in store.live_entries() {
            if let Ok(s) = std::str::from_utf8(k) {
                out.push(s.to_string());
            }
        }
        out.sort();
        Ok(out)
    }

    /// Mark a node offline (simulates failure; CLUSTER_SPEC §15 tests).
    pub fn mark_offline(&mut self, node: NodeId) -> Result<(), ClusterError> {
        let idx = node.index();
        // Drop the open store handle so the directory is idle for salvage.
        self.nodes.remove(&idx);
        if !self.offline.contains(&idx) {
            self.offline.push(idx);
            self.offline.sort();
        }
        // Step down any Raft leadership held by this node.
        for group in self.raft.values_mut() {
            if let Some((leader, _)) = group.current_leader() {
                if leader == node {
                    if let Some(p) = group.peer_mut(node) {
                        p.role = crate::raft::RaftRole::Follower;
                    }
                }
            }
        }
        Ok(())
    }

    /// Bring a previously offline node back online by reopening its store.
    pub fn mark_online(&mut self, node: NodeId) -> Result<(), ClusterError> {
        let idx = node.index();
        self.offline.retain(|&x| x != idx);
        if !self.nodes.contains_key(&idx) {
            let path = node_store_path(&self.root, idx);
            let store = Store::open(&path)?;
            self.nodes.insert(idx, store);
        }
        Ok(())
    }

    /// Whether a node is currently online.
    pub fn is_online(&self, node: NodeId) -> bool {
        self.nodes.contains_key(&node.index())
    }

    /// Explicit cluster health / degraded state (DEF-038).
    ///
    /// Missing expected stores, offline marks, and in-flight rebalance phases
    /// are visible to operators — open never pretends a partial cluster is full.
    pub fn health(&self) -> ClusterHealth {
        let online = self.online_nodes();
        let offline: Vec<NodeId> = self.offline.iter().copied().map(NodeId::new).collect();
        let missing_store_paths: Vec<NodeId> = self
            .missing_nodes
            .iter()
            .copied()
            .map(NodeId::new)
            .collect();
        let mut rebalance_phases: Vec<(PartitionId, RebalancePhase)> = self
            .rebalance_jobs
            .values()
            .map(|j| (j.partition, j.phase))
            .collect();
        rebalance_phases.sort_by_key(|(p, _)| p.get());
        let degraded = !missing_store_paths.is_empty()
            || !offline.is_empty()
            || (online.len() as u32) < self.expected_node_count;
        ClusterHealth {
            expected_nodes: self.expected_node_count,
            online,
            offline,
            missing_store_paths,
            rebalance_phases,
            degraded,
        }
    }

    /// Profile tag for durable rebalance control plane (DEF-038).
    pub fn rebalance_control_profile() -> &'static str {
        REBALANCE_CONTROL_PROFILE
    }

    /// Profile tag for anti-entropy / repair (DEF-039).
    pub fn anti_entropy_profile() -> &'static str {
        ANTI_ENTROPY_PROFILE
    }

    /// Resolve the virtual partition for a subject (default partition key).
    pub fn partition_for_subject(&self, subject: &str) -> PartitionId {
        self.partition_map
            .partition_of(default_partition_key(subject))
    }

    /// Ensure a live Raft leader for `partition`, updating the placement directory.
    ///
    /// When no leader is online yet, prefers the placement-directory primary
    /// (`partition_id % node_count` under balanced layout) so multi-node clusters
    /// spread leadership for product capacity (WORK_HORIZON S3). Sticky leadership
    /// still wins if a live leader already holds the partition.
    pub fn ensure_partition_leader(
        &mut self,
        partition: PartitionId,
    ) -> Result<(NodeId, Term), ClusterError> {
        let online = self.online_nodes();
        // Placement primary before election (balanced map spreads by partition id).
        let preferred = self.directory.get(partition).map(|a| a.leader);
        let group = self
            .raft
            .get_mut(&partition.get())
            .ok_or(ClusterError::NoLeader(partition.get()))?;

        let (leader, term) =
            group
                .ensure_leader_preferring(&online, preferred)
                .map_err(|e| match e {
                    ElectError::NoQuorum { .. }
                    | ElectError::NoOnlineVoters
                    | ElectError::CandidateOffline
                    | ElectError::PersistFailed => ClusterError::PartitionUnavailable {
                        partition: partition.get(),
                        reason: "no leader elected (quorum unavailable)",
                    },
                    ElectError::NotAVoter => ClusterError::NoLeader(partition.get()),
                    ElectError::HigherTerm(_) => ClusterError::PartitionUnavailable {
                        partition: partition.get(),
                        reason: "election observed higher term",
                    },
                })?;

        self.directory.set_leader(partition, leader, term);
        Ok((leader, term))
    }

    /// Put a subject on its partition.
    ///
    /// - `partition-linearizable`: Raft quorum commit + store apply.
    /// - `convergent-append`: any online replica set may accept (no quorum);
    ///   dual-accept across a network split is allowed (CLUSTER_SPEC §9.2).
    pub fn put(
        &mut self,
        subject: &str,
        value: &[u8],
        mode: DurabilityMode,
    ) -> Result<ClusterWriteAck, ClusterError> {
        self.write_event(subject, value, mode, false)
    }

    /// Batch put that **groups by virtual partition** then writes each group.
    ///
    /// Product capacity path (WORK_HORIZON S3): multi-partition ingest fans out
    /// to independent partition leaders / node stores rather than treating the
    /// testrig `--stores N` harness as multi-tenant product sharding. Each item
    /// still receives a full [`ClusterWriteAck`] with honest
    /// `replica_acks` / `committed` / `leader` fields (CLUSTER_SPEC §11.2).
    ///
    /// Order of acks matches `items`. Within a partition, writes are sequential
    /// (Raft log order). Across partitions, groups are processed in ascending
    /// partition id so the batch is deterministic under a fixed partition map.
    ///
    /// Empty `items` returns an empty ack list. On the first error the batch
    /// stops; prior acks in the return value are not produced (fail-closed).
    pub fn put_many(
        &mut self,
        items: &[(&str, &[u8])],
        mode: DurabilityMode,
    ) -> Result<Vec<ClusterWriteAck>, ClusterError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        // Group item indices by partition for deterministic multi-leader fan-out.
        let mut by_partition: HashMap<u32, Vec<usize>> = HashMap::new();
        for (i, (subject, _)) in items.iter().enumerate() {
            let p = self.partition_for_subject(subject).get();
            by_partition.entry(p).or_default().push(i);
        }

        let mut partition_order: Vec<u32> = by_partition.keys().copied().collect();
        partition_order.sort_unstable();

        let mut slots: Vec<Option<ClusterWriteAck>> = (0..items.len()).map(|_| None).collect();
        for p in partition_order {
            let indices = by_partition.get(&p).expect("key from map");
            // Ensure leadership once per partition group (product path: one
            // election cost per partition, not per key).
            let _ = self.ensure_partition_leader(PartitionId::new(p))?;
            for &i in indices {
                let (subject, value) = items[i];
                let ack = self.write_event(subject, value, mode, false)?;
                debug_assert_eq!(ack.partition.get(), p);
                slots[i] = Some(ack);
            }
        }

        let mut out = Vec::with_capacity(items.len());
        for slot in slots {
            out.push(slot.ok_or(ClusterError::CorruptMeta("put_many missing ack slot"))?);
        }
        Ok(out)
    }

    /// Delete a subject (tombstone) with the same routing/replication path.
    pub fn delete(
        &mut self,
        subject: &str,
        mode: DurabilityMode,
    ) -> Result<ClusterWriteAck, ClusterError> {
        self.write_event(subject, &[], mode, true)
    }

    /// Accept a convergent append on one specific online node (ingest endpoint).
    ///
    /// Used for dual-accept tests and direct routing to a known ingest node.
    /// Requires [`ConsistencyMode::ConvergentAppend`]. Does not require quorum
    /// or Raft leadership.
    pub fn append_local(
        &mut self,
        node: NodeId,
        subject: &str,
        value: &[u8],
        mode: DurabilityMode,
    ) -> Result<ClusterWriteAck, ClusterError> {
        if self.consistency_mode != ConsistencyMode::ConvergentAppend {
            return Err(ClusterError::ConsistencyViolation(
                "append_local requires convergent-append mode".into(),
            ));
        }
        if !self.is_online(node) {
            return Err(ClusterError::PartitionUnavailable {
                partition: self.partition_for_subject(subject).get(),
                reason: "target ingest node offline",
            });
        }
        self.write_convergent_on(&[node], subject, value, mode, false)
    }

    /// Reconcile convergent state across all currently online replicas.
    ///
    /// Copies missing `(subject, body)` pairs by content identity. When the same
    /// subject has different live bodies on different nodes, both variants are
    /// retained on each participant (history keeps both; live value is not
    /// silently chosen) and the conflict is reported (CLUSTER_SPEC §9.2, §15.2).
    pub fn reconcile(&mut self) -> Result<ReconcileReport, ClusterError> {
        if self.consistency_mode != ConsistencyMode::ConvergentAppend {
            return Err(ClusterError::ConsistencyViolation(
                "reconcile is for convergent-append mode".into(),
            ));
        }

        let participants = self.online_nodes();
        let mut report = ReconcileReport {
            events_replicated: 0,
            conflicts: Vec::new(),
            participants: participants.clone(),
        };

        // Collect live (subject -> (node, body)) from every online node.
        // Balanced placement: every node may hold every subject.
        let mut by_subject: HashMap<String, Vec<(NodeId, Vec<u8>)>> = HashMap::new();
        for node in &participants {
            let store = match self.nodes.get(&node.index()) {
                Some(s) => s,
                None => continue,
            };
            for (subj_bytes, _body) in store.live_entries() {
                let Ok(subject) = std::str::from_utf8(subj_bytes) else {
                    continue;
                };
                let live = match store.get(subject) {
                    Ok(Some(v)) => v,
                    Ok(None) => continue,
                    Err(residiuum_store::StoreError::PayloadPartial) => continue,
                    Err(e) => return Err(e.into()),
                };
                by_subject
                    .entry(subject.to_string())
                    .or_default()
                    .push((*node, live));
            }
        }

        // Detect conflicts: same subject, differing bodies.
        let mut conflict_subjects: HashSet<String> = HashSet::new();
        for (subject, variants) in &by_subject {
            let mut hashes: HashSet<[u8; 32]> = HashSet::new();
            for (_, body) in variants {
                hashes.insert(body_content_hash(body));
            }
            if hashes.len() > 1 {
                conflict_subjects.insert(subject.clone());
                let partition = self.partition_for_subject(subject);
                // Dedup variants by content hash for the report.
                let mut seen = HashSet::new();
                let mut unique = Vec::new();
                for (node, body) in variants {
                    let h = body_content_hash(body);
                    if seen.insert(h) {
                        unique.push(SubjectVariant {
                            node: *node,
                            body: body.clone(),
                            content_hash: h,
                        });
                    }
                }
                report.conflicts.push(SubjectConflict {
                    subject: subject.clone(),
                    partition,
                    variants: unique,
                });
            }
        }

        // Fan-out: for each distinct (subject, body) seen, ensure every online
        // node that is a replica of the partition holds that body in history.
        // For non-conflicts, missing keys get a put. For conflicts, put each
        // missing variant onto nodes that lack that content so both sides
        // survive (history) without inventing a single live winner beyond the
        // last put — we put only onto nodes that do not already have that hash
        // as their live value *and* lack it in a simple live check.
        //
        // Practical rule: if node N's live body hash != H for variant H of
        // subject S, append variant H onto N (store put). This preserves both
        // event_ids in history; the final live value is the last put applied
        // (deterministic: sort by content hash then apply).
        for (subject, variants) in &by_subject {
            let partition = self.partition_for_subject(subject);
            let Some(assignment) = self.directory.get(partition).cloned() else {
                continue;
            };

            // Unique bodies for this subject.
            let mut unique_bodies: Vec<Vec<u8>> = Vec::new();
            let mut seen_h = HashSet::new();
            for (_, body) in variants {
                let h = body_content_hash(body);
                if seen_h.insert(h) {
                    unique_bodies.push(body.clone());
                }
            }
            // Deterministic order for conflict multi-put.
            unique_bodies.sort_by_key(|a| body_content_hash(a));

            let is_conflict = conflict_subjects.contains(subject);

            for node in &participants {
                if !assignment.replicas.contains(node) {
                    continue;
                }
                let store = match self.nodes.get_mut(&node.index()) {
                    Some(s) => s,
                    None => continue,
                };
                let current = store.get(subject)?;
                let current_hash = current.as_ref().map(|b| body_content_hash(b));

                if !is_conflict {
                    // Single variant: copy if missing or different (should not differ).
                    let body = &unique_bodies[0];
                    let h = body_content_hash(body);
                    if current_hash != Some(h) {
                        store.put(subject, body, DurabilityMode::Durable)?;
                        report.events_replicated += 1;
                    }
                } else {
                    // Conflict: ensure every variant is applied so history holds
                    // both; final live is last in hash-sorted order (explicit,
                    // not silent "newest wall clock wins").
                    for body in &unique_bodies {
                        let h = body_content_hash(body);
                        // Skip if already live with this body.
                        if current_hash == Some(h) {
                            // Still may need other variants in history — put others only.
                            continue;
                        }
                        // Always append other variants so history has both.
                        store.put(subject, body, DurabilityMode::Durable)?;
                        report.events_replicated += 1;
                    }
                }
            }
        }

        report.conflicts.sort_by(|a, b| a.subject.cmp(&b.subject));
        Ok(report)
    }

    fn write_event(
        &mut self,
        subject: &str,
        value: &[u8],
        mode: DurabilityMode,
        is_delete: bool,
    ) -> Result<ClusterWriteAck, ClusterError> {
        if self.consistency_mode == ConsistencyMode::ConvergentAppend {
            return self.write_convergent(subject, value, mode, is_delete);
        }
        self.write_linearizable(subject, value, mode, is_delete)
    }

    /// Convergent-append: accept on all currently online replicas of the
    /// partition. No Raft, no quorum — minority and split sides may accept.
    fn write_convergent(
        &mut self,
        subject: &str,
        value: &[u8],
        mode: DurabilityMode,
        is_delete: bool,
    ) -> Result<ClusterWriteAck, ClusterError> {
        let partition = self.partition_for_subject(subject);
        let assignment = self
            .directory
            .get(partition)
            .cloned()
            .ok_or(ClusterError::NoLeader(partition.get()))?;

        let targets: Vec<NodeId> = assignment
            .replicas
            .iter()
            .copied()
            .filter(|n| self.is_online(*n))
            .collect();
        if targets.is_empty() {
            return Err(ClusterError::PartitionUnavailable {
                partition: partition.get(),
                reason: "no online replica for convergent append",
            });
        }
        self.write_convergent_on(&targets, subject, value, mode, is_delete)
    }

    fn write_convergent_on(
        &mut self,
        targets: &[NodeId],
        subject: &str,
        value: &[u8],
        mode: DurabilityMode,
        is_delete: bool,
    ) -> Result<ClusterWriteAck, ClusterError> {
        let partition = self.partition_for_subject(subject);
        let assignment = self
            .directory
            .get(partition)
            .cloned()
            .ok_or(ClusterError::NoLeader(partition.get()))?;
        let epoch = assignment.placement_epoch;

        if targets.is_empty() {
            return Err(ClusterError::PartitionUnavailable {
                partition: partition.get(),
                reason: "no ingest targets",
            });
        }
        for n in targets {
            if !self.is_online(*n) {
                return Err(ClusterError::PartitionUnavailable {
                    partition: partition.get(),
                    reason: "ingest target offline",
                });
            }
            if !assignment.replicas.contains(n) {
                return Err(ClusterError::ReplicationRejected(format!(
                    "node {n} is not a replica of partition {}",
                    partition.get()
                )));
            }
        }

        let pos = {
            let e = self.convergent_pos.entry(partition.get()).or_insert(0);
            *e += 1;
            LogPosition(*e)
        };

        let ingest = targets[0];
        let mut first_receipt = None;
        let mut acks = 0u32;

        for node in targets {
            let store =
                self.nodes
                    .get_mut(&node.index())
                    .ok_or(ClusterError::PartitionUnavailable {
                        partition: partition.get(),
                        reason: "ingest store missing",
                    })?;
            let receipt = if is_delete {
                store.delete(subject, mode)?
            } else {
                store.put(subject, value, mode)?
            };
            acks += 1;
            if first_receipt.is_none() {
                first_receipt = Some(receipt);
            }
        }

        let receipt = first_receipt.expect("at least one target");

        // Convergent accepts are local/durable on ingest nodes but do not claim
        // linearizable commitment (CLUSTER_SPEC §9.2).
        Ok(ClusterWriteAck {
            cluster_id: self.cluster_id,
            consistency_mode: ConsistencyMode::ConvergentAppend,
            durability_mode: mode,
            partition,
            term: Term(0),
            position: pos,
            placement_epoch: epoch,
            replica_acks: acks,
            committed: false,
            commit_status: CommitStatus::Prepared,
            leader: ingest,
            event_id: receipt.event_id,
            store_id: receipt.store_id,
            segment_id: receipt.segment_id,
        })
    }

    fn write_linearizable(
        &mut self,
        subject: &str,
        value: &[u8],
        mode: DurabilityMode,
        is_delete: bool,
    ) -> Result<ClusterWriteAck, ClusterError> {
        let partition = self.partition_for_subject(subject);
        let assignment = self
            .directory
            .get(partition)
            .cloned()
            .ok_or(ClusterError::NoLeader(partition.get()))?;
        let epoch = assignment.placement_epoch;

        let (leader, _term) = self.ensure_partition_leader(partition)?;

        let command = if is_delete {
            LogCommand::Delete {
                subject: subject.to_string(),
            }
        } else {
            LogCommand::Put {
                subject: subject.to_string(),
                value: value.to_vec(),
            }
        };

        let online = self.online_nodes();
        let propose = {
            let group = self
                .raft
                .get_mut(&partition.get())
                .ok_or(ClusterError::NoLeader(partition.get()))?;
            group
                .propose(leader, command, &online)
                .map_err(|e| match e {
                    ProposeError::NotLeader => ClusterError::NoLeader(partition.get()),
                    ProposeError::SteppedDown(_) => ClusterError::PartitionUnavailable {
                        partition: partition.get(),
                        reason: "leader stepped down during propose",
                    },
                    ProposeError::PersistFailed => ClusterError::DurabilityUnavailable(
                        "raft log persist failed before replication".into(),
                    ),
                })?
        };

        if !propose.committed {
            return Err(ClusterError::DurabilityUnavailable(format!(
                "quorum not reached: got {} of {} for partition {}",
                propose.replica_acks,
                self.write_quorum(),
                partition.get()
            )));
        }

        // Apply committed entries to every online replica that has them.
        let mut leader_receipt = None;
        for node in online {
            let batch = self
                .raft
                .get_mut(&partition.get())
                .map(|g| {
                    g.sync_follower_commit(leader, node);
                    g.take_apply_batch(node)
                })
                .unwrap_or_default();
            if batch.is_empty() {
                continue;
            }
            let store = match self.nodes.get_mut(&node.index()) {
                Some(s) => s,
                None => continue,
            };
            for entry in batch {
                let receipt = match &entry.command {
                    LogCommand::Put { subject, value } => store.put(subject, value, mode)?,
                    LogCommand::Delete { subject } => store.delete(subject, mode)?,
                };
                if node == leader {
                    leader_receipt = Some(receipt);
                }
            }
        }

        let receipt = leader_receipt.ok_or(ClusterError::PartitionUnavailable {
            partition: partition.get(),
            reason: "leader apply missing receipt",
        })?;

        Ok(ClusterWriteAck {
            cluster_id: self.cluster_id,
            consistency_mode: self.consistency_mode,
            durability_mode: mode,
            partition,
            term: propose.term,
            position: propose.position,
            placement_epoch: epoch,
            replica_acks: propose.replica_acks,
            committed: propose.committed,
            commit_status: CommitStatus::Committed,
            leader,
            event_id: receipt.event_id,
            store_id: receipt.store_id,
            segment_id: receipt.segment_id,
        })
    }

    /// Get a subject under the given read mode, with coverage.
    pub fn get(&mut self, subject: &str, mode: ReadMode) -> Result<GetResult, ClusterError> {
        let partition = self.partition_for_subject(subject);
        let mut coverage = Coverage::single(partition);
        if !self.replicated_durability_available() {
            coverage.note("development profile: replicated durability unavailable");
        }
        if self.consistency_mode == ConsistencyMode::ConvergentAppend {
            coverage.note("convergent-append: not linearizable");
        }

        let assignment = match self.directory.get(partition) {
            Some(a) => a.clone(),
            None => {
                coverage.mark_unavailable(partition);
                return Ok(GetResult {
                    value: None,
                    coverage,
                    absence_proven: false,
                });
            }
        };

        match mode {
            ReadMode::Linearizable => {
                if self.consistency_mode == ConsistencyMode::ConvergentAppend {
                    return Err(ClusterError::ConsistencyViolation(
                        "linearizable reads are not available in convergent-append mode".into(),
                    ));
                }
                // Need a live leader (may re-elect after failure).
                let (leader, term) = match self.ensure_partition_leader(partition) {
                    Ok(lt) => lt,
                    Err(e) => {
                        coverage.mark_unavailable(partition);
                        return Err(e);
                    }
                };
                let store =
                    self.nodes
                        .get(&leader.index())
                        .ok_or(ClusterError::PartitionUnavailable {
                            partition: partition.get(),
                            reason: "leader handle missing",
                        })?;
                let value = store.get(subject)?;
                let pos = self
                    .raft
                    .get(&partition.get())
                    .map(|g| LogPosition(g.max_commit_index()))
                    .unwrap_or(LogPosition(0));
                coverage.mark_completed(partition, term, pos, Some(leader.index()));
                Ok(GetResult {
                    value,
                    coverage,
                    absence_proven: true,
                })
            }
            ReadMode::Available | ReadMode::Salvage => {
                // Prefer current Raft leader, then any online replica.
                let mut order = Vec::new();
                if let Some((leader, _)) = self
                    .raft
                    .get(&partition.get())
                    .and_then(|g| g.current_leader())
                {
                    order.push(leader);
                }
                order.push(assignment.leader);
                for r in &assignment.replicas {
                    if !order.contains(r) {
                        order.push(*r);
                    }
                }
                for node in order {
                    if !self.is_online(node) {
                        continue;
                    }
                    let store = match self.nodes.get(&node.index()) {
                        Some(s) => s,
                        None => continue,
                    };
                    match store.get(subject) {
                        Ok(value) => {
                            let (term, pos) = self
                                .raft
                                .get(&partition.get())
                                .map(|g| {
                                    let term =
                                        g.current_leader().map(|(_, t)| t).unwrap_or(Term(0));
                                    (term, LogPosition(g.max_commit_index()))
                                })
                                .unwrap_or((Term(0), LogPosition(0)));
                            coverage.mark_completed(partition, term, pos, Some(node.index()));
                            return Ok(GetResult {
                                value,
                                coverage,
                                absence_proven: false,
                            });
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                coverage.mark_unavailable(partition);
                Ok(GetResult {
                    value: None,
                    coverage,
                    absence_proven: false,
                })
            }
        }
    }

    /// Scan all live subjects across partitions that are currently reachable.
    ///
    /// Incomplete coverage is always reported; never claims an offline partition
    /// as an empty success (CLUSTER_SPEC §6.7).
    pub fn scan_all(&mut self) -> Result<ScanResult, ClusterError> {
        let find = self.scan_with(ScanOptions::default())?;
        Ok(ScanResult {
            entries: find.entries,
            coverage: find.coverage,
        })
    }

    /// Distributed scan/find with coverage (CLUSTER_SPEC §17, Stage 8e, DEF-040).
    ///
    /// Returns matching subjects from completed partitions only. Always attach
    /// honest coverage: unavailable partitions are listed, never treated as
    /// empty success. Resource budgets set `coverage.resource_limit_reached`.
    ///
    /// **Paging (DEF-040):** set [`ScanOptions::page_size`] and/or
    /// [`ScanOptions::continuation`]. Every page carries full coverage plus an
    /// continuation state so a replacement coordinator can resume
    /// without silent duplicates or omissions. Merge order is always
    /// subject-ascending and independent of partition visit / worker order.
    pub fn scan_with(&mut self, options: ScanOptions) -> Result<FindResult, ClusterError> {
        // Resolve continuation first so scope / read mode / budgets resume.
        let cont = if let Some(ref tok) = options.continuation {
            Some(QueryContinuation::decode(
                self.cluster_id,
                &self.token_keyring,
                tok,
            )?)
        } else {
            None
        };

        let read_mode = cont
            .as_ref()
            .map(|c| c.read_mode)
            .unwrap_or(options.read_mode);

        let subject_prefix = cont
            .as_ref()
            .and_then(|c| c.prefix.clone())
            .or_else(|| options.subject_prefix.clone());

        let after_subject = cont
            .as_ref()
            .map(|c| c.after_subject.clone())
            .or_else(|| options.after_subject.clone());

        let page_size = cont
            .as_ref()
            .map(|c| c.page_size)
            .or(options.page_size)
            .map(|n| n.clamp(1, MAX_FIND_PAGE_SIZE));

        let limit = cont
            .as_ref()
            .and_then(|c| c.remaining_limit)
            .or(options.limit);
        let max_docs = cont
            .as_ref()
            .and_then(|c| c.remaining_max_docs)
            .or(options.max_docs_scanned);

        let requested: Vec<PartitionId> = if let Some(ref c) = cont {
            c.partitions.clone()
        } else {
            match &options.partitions {
                Some(p) => {
                    let mut v = p.clone();
                    v.sort();
                    v.dedup();
                    v
                }
                None => self.partition_map.all_partitions().collect(),
            }
        };

        let query_id = if let Some(ref c) = cont {
            c.query_id.clone()
        } else {
            FindResult::make_query_id(&requested, subject_prefix.as_deref(), limit)
        };

        // Visit order affects only contact sequencing; merge is always by subject.
        let visit: Vec<PartitionId> = if let Some(ref order) = options.visit_order {
            // Validate permutation of scope.
            let mut sorted_order: Vec<PartitionId> = order.clone();
            sorted_order.sort();
            sorted_order.dedup();
            if sorted_order != requested {
                return Err(ClusterError::ConsistencyViolation(
                    "visit_order must be a permutation of the partition scope".into(),
                ));
            }
            order.clone()
        } else {
            requested.clone()
        };

        let mut coverage = Coverage::for_partitions(requested.iter().copied());
        coverage.with_read_mode(read_mode);
        coverage.use_index("primary-scan");
        coverage.search_tier("hot");
        if !self.replicated_durability_available() {
            coverage.note("development profile: replicated durability unavailable");
        }
        if cont.is_some() {
            coverage.note(format!("resumed via continuation (query_id={query_id})"));
        }

        let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
        let mut seen_subjects = HashSet::new();
        let mut docs_scanned = 0usize;
        let mut budget_stopped = false;

        for partition in visit {
            if budget_stopped {
                // Remaining requested partitions are incomplete due to budget.
                if !coverage.completed.contains(&partition)
                    && !coverage.unavailable.contains(&partition)
                {
                    coverage.note(format!(
                        "partition {} not examined (resource budget)",
                        partition.get()
                    ));
                }
                continue;
            }

            let Some(assignment) = self.directory.get(partition).cloned() else {
                coverage.mark_unavailable(partition);
                continue;
            };

            let mut batch = Vec::new();
            let served = match self.contact_partition(
                partition,
                &assignment,
                read_mode,
                &mut batch,
                &mut seen_subjects,
                subject_prefix.as_deref(),
                after_subject.as_deref(),
                &mut docs_scanned,
                max_docs,
            )? {
                ContactOutcome::Served { term, pos, node } => {
                    coverage.mark_completed(partition, term, pos, Some(node.index()));
                    true
                }
                ContactOutcome::BudgetExhausted { term, pos, node } => {
                    coverage.mark_completed(partition, term, pos, Some(node.index()));
                    coverage.mark_resource_limit(format!(
                        "max_docs_scanned budget reached after {docs_scanned} subjects"
                    ));
                    budget_stopped = true;
                    true
                }
                ContactOutcome::Unavailable => false,
            };

            if !served {
                coverage.mark_unavailable(partition);
            } else {
                entries.extend(batch);
            }
        }

        // Deterministic merge: subject ascending, never worker completion order.
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let mut truncated = false;
        let mut has_more = false;
        let mut continuation = None;

        if let Some(ps) = page_size {
            // Paged mode (DEF-040): emit at most `ps` rows (also bound by remaining limit).
            let cap = match limit {
                Some(l) => l.min(ps),
                None => ps,
            };
            if entries.len() > cap {
                has_more = true;
                truncated = true;
                entries.truncate(cap);
                coverage.note(format!("page truncated to {cap} (page_size={ps})"));
            }

            if has_more {
                if let Some(last) = entries.last() {
                    let remaining_limit = limit.map(|l| l.saturating_sub(entries.len()));
                    // Exhausted overall limit → no further pages.
                    if remaining_limit == Some(0) {
                        has_more = false;
                    } else {
                        let remaining_max_docs = max_docs.map(|m| m.saturating_sub(docs_scanned));
                        let next = QueryContinuation {
                            query_id: query_id.clone(),
                            after_subject: last.0.clone(),
                            page_size: ps,
                            prefix: subject_prefix.clone(),
                            partitions: requested.clone(),
                            read_mode,
                            remaining_limit,
                            remaining_max_docs,
                        };
                        continuation = Some(next.encode(self.cluster_id, &self.token_keyring)?);
                    }
                }
            }
        } else if let Some(lim) = limit {
            // One-shot mode with limit (Stage 8e).
            if entries.len() > lim {
                entries.truncate(lim);
                truncated = true;
                coverage.note(format!("result truncated to limit {lim}"));
            }
        }

        Ok(FindResult {
            entries,
            coverage,
            query_id,
            truncated,
            has_more,
            continuation,
        })
    }

    /// Convenience find: subjects matching optional prefix under scan options.
    pub fn find(&mut self, options: ScanOptions) -> Result<FindResult, ClusterError> {
        self.scan_with(options)
    }

    /// Fetch one page of a distributed find, starting or resuming (DEF-040).
    ///
    /// Equivalent to [`Self::scan_with`] with [`ScanOptions::page_size`] set
    /// (default [`DEFAULT_FIND_PAGE_SIZE`] when neither page size nor
    /// continuation provides one).
    pub fn scan_page(&mut self, mut options: ScanOptions) -> Result<FindResult, ClusterError> {
        if options.page_size.is_none() && options.continuation.is_none() {
            options.page_size = Some(DEFAULT_FIND_PAGE_SIZE);
        } else if options.page_size.is_none() {
            // Continuation carries page_size; leave unset so decode supplies it.
        }
        self.scan_with(options)
    }

    /// Begin an interruptible rebalance of one partition to `new_replicas`
    /// (CLUSTER_SPEC §14, Stage 8f). Advances only to `PlanCommitted`.
    pub fn begin_rebalance(
        &mut self,
        partition: PartitionId,
        new_replicas: Vec<NodeId>,
    ) -> Result<RebalanceJob, ClusterError> {
        if new_replicas.is_empty() {
            return Err(ClusterError::Rebalance(
                "new replica set must be non-empty".into(),
            ));
        }
        // Destinations must exist as node stores (online or offline on disk).
        for n in &new_replicas {
            let path = self.node_path(*n);
            if !path.exists() && !self.is_online(*n) {
                return Err(ClusterError::Rebalance(format!(
                    "destination node {} has no store path",
                    n.index()
                )));
            }
        }
        if self.rebalance_jobs.contains_key(&partition.get()) {
            return Err(ClusterError::Rebalance(format!(
                "rebalance already in progress for partition {}",
                partition.get()
            )));
        }
        let old = self
            .directory
            .get(partition)
            .ok_or_else(|| {
                ClusterError::Rebalance(format!("no assignment for partition {}", partition.get()))
            })?
            .replicas
            .clone();
        let plan_epoch = self.directory.placement_epoch;
        let job_id = format!(
            "rb-{}-{}",
            partition.get(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        );
        let job = RebalanceJob::plan(job_id, partition, old, new_replicas, plan_epoch);
        self.rebalance_jobs.insert(partition.get(), job.clone());
        self.persist_rebalance_jobs()?;
        Ok(job)
    }

    /// Advance the in-flight rebalance for `partition` by one phase.
    ///
    /// Persists job phase (and joint membership when applicable) before return
    /// so coordinator restart cannot lose operation state (DEF-038).
    pub fn advance_rebalance(
        &mut self,
        partition: PartitionId,
    ) -> Result<RebalanceJob, ClusterError> {
        let job = self
            .rebalance_jobs
            .get(&partition.get())
            .cloned()
            .ok_or_else(|| {
                ClusterError::Rebalance(format!(
                    "no in-flight rebalance for partition {}",
                    partition.get()
                ))
            })?;
        if job.phase == RebalancePhase::Reclaimed {
            return Err(ClusterError::Rebalance("rebalance already complete".into()));
        }
        let next = job
            .phase
            .next()
            .ok_or_else(|| ClusterError::Rebalance("no next phase".into()))?;

        let updated = match next {
            RebalancePhase::LearnersAdded => self.rb_learners_added(job)?,
            RebalancePhase::SegmentsCopied => self.rb_segments_copied(job)?,
            RebalancePhase::LogCaughtUp => self.rb_log_caught_up(job)?,
            RebalancePhase::MembershipChanged => self.rb_membership_changed(job)?,
            RebalancePhase::EpochActivated => self.rb_epoch_activated(job)?,
            RebalancePhase::SafetyWindow => self.rb_safety_window(job)?,
            RebalancePhase::Reclaimed => self.rb_reclaimed(job)?,
            RebalancePhase::PlanCommitted => {
                return Err(ClusterError::Rebalance("cannot re-enter plan".into()));
            }
        };
        if updated.phase == RebalancePhase::Reclaimed {
            self.rebalance_jobs.remove(&partition.get());
        } else {
            self.rebalance_jobs.insert(partition.get(), updated.clone());
        }
        self.persist_rebalance_jobs()?;
        Ok(updated)
    }

    /// Run all remaining rebalance steps to completion.
    pub fn rebalance_partition(
        &mut self,
        partition: PartitionId,
        new_replicas: Vec<NodeId>,
    ) -> Result<RebalanceReport, ClusterError> {
        let mut phases = vec![RebalancePhase::PlanCommitted];
        let mut job = self.begin_rebalance(partition, new_replicas)?;
        while job.phase != RebalancePhase::Reclaimed {
            job = self.advance_rebalance(partition)?;
            phases.push(job.phase);
        }
        Ok(RebalanceReport {
            job,
            phases_completed: phases,
        })
    }

    /// Current in-flight rebalance job for a partition, if any.
    pub fn rebalance_job(&self, partition: PartitionId) -> Option<&RebalanceJob> {
        self.rebalance_jobs.get(&partition.get())
    }

    /// Reconstruct a placement directory from live node stores after control-
    /// plane loss (CLUSTER_SPEC §12.3 / §22 item 10, simplified).
    ///
    /// Scans every online node for subjects, maps them to partitions, and
    /// builds a balanced-style directory with those nodes as replicas. Does
    /// not invent commitment evidence.
    pub fn reconstruct_directory_from_stores(
        &mut self,
    ) -> Result<PartitionDirectory, ClusterError> {
        let online = self.online_nodes();
        if online.is_empty() {
            return Err(ClusterError::Rebalance(
                "cannot reconstruct directory with no online nodes".into(),
            ));
        }
        let epoch = PlacementEpoch(self.directory.placement_epoch.0 + 1);
        let mut directory = PartitionDirectory::balanced(
            self.partition_map.clone(),
            online.iter().map(|n| n.index()).max().unwrap_or(0) + 1,
            epoch,
        );
        // Restrict each partition's replicas to online nodes that currently
        // hold at least one subject for that partition (or all online if none).
        let mut holders: HashMap<u32, HashSet<u32>> = HashMap::new();
        for node in &online {
            let store = match self.nodes.get(&node.index()) {
                Some(s) => s,
                None => continue,
            };
            for (subj_bytes, _) in store.live_entries() {
                let Ok(subject) = std::str::from_utf8(subj_bytes) else {
                    continue;
                };
                let p = self.partition_for_subject(subject).get();
                holders.entry(p).or_default().insert(node.index());
            }
        }
        for a in &mut directory.assignments {
            let p = a.partition.get();
            if let Some(set) = holders.get(&p) {
                let mut reps: Vec<NodeId> = set.iter().copied().map(NodeId::new).collect();
                reps.sort();
                if !reps.is_empty() {
                    a.replicas = reps.clone();
                    a.leader = reps[0];
                }
            } else {
                // Empty partition: any online node may host it.
                a.replicas = online.clone();
                a.leader = online[0];
            }
            a.placement_epoch = epoch;
        }
        directory.placement_epoch = epoch;
        self.directory = directory.clone();
        let mut raft = Self::build_raft_groups(&self.directory);
        Self::attach_and_restore_raft_stores(&self.root, &self.directory, &mut raft)?;
        self.raft = raft;
        self.directory.save(&self.root)?;
        Ok(directory)
    }

    fn rb_learners_added(&mut self, mut job: RebalanceJob) -> Result<RebalanceJob, ClusterError> {
        // Register peer slots for destinations (non-voting until membership).
        let partition = job.partition;
        let destinations = job.destinations.clone();
        if let Some(group) = self.raft.get_mut(&partition.get()) {
            for d in &destinations {
                group.ensure_peer(*d);
                let store = RaftPeerStore::open(&self.root, *d, partition)?;
                group.attach_store(store);
            }
        }
        // Ensure destination stores are open when on disk.
        for d in &job.destinations {
            if !self.is_online(*d) {
                let path = self.node_path(*d);
                if path.exists() {
                    let store = Store::open(&path)?;
                    self.nodes.insert(d.index(), store);
                    self.offline.retain(|&x| x != d.index());
                } else {
                    return Err(ClusterError::Rebalance(format!(
                        "learner destination {} offline with no store",
                        d.index()
                    )));
                }
            }
        }
        job.phase = RebalancePhase::LearnersAdded;
        Ok(job)
    }

    fn rb_segments_copied(&mut self, mut job: RebalanceJob) -> Result<RebalanceJob, ClusterError> {
        // Pick a source: first online old replica with data.
        let source = job
            .old_replicas
            .iter()
            .copied()
            .find(|n| self.is_online(*n))
            .ok_or_else(|| {
                ClusterError::Rebalance(
                    "no online source replica during segment copy (CLUSTER_SPEC §22.13)".into(),
                )
            })?;

        let subjects: Vec<(String, Vec<u8>)> = {
            let store = self
                .nodes
                .get(&source.index())
                .ok_or_else(|| ClusterError::Rebalance("source store missing".into()))?;
            let mut out = Vec::new();
            for (subj_bytes, _) in store.live_entries() {
                let Ok(subject) = std::str::from_utf8(subj_bytes) else {
                    continue;
                };
                if self.partition_for_subject(subject) != job.partition {
                    continue;
                }
                if let Ok(Some(body)) = store.get(subject) {
                    out.push((subject.to_string(), body));
                }
            }
            out
        };

        for dest in &job.destinations {
            if !self.is_online(*dest) {
                return Err(ClusterError::Rebalance(format!(
                    "destination {} offline during segment copy",
                    dest.index()
                )));
            }
            let store = self
                .nodes
                .get_mut(&dest.index())
                .ok_or_else(|| ClusterError::Rebalance("dest store missing".into()))?;
            for (subject, body) in &subjects {
                store.put(subject, body, DurabilityMode::Durable)?;
                job.subjects_copied += 1;
            }
        }
        job.phase = RebalancePhase::SegmentsCopied;
        Ok(job)
    }

    fn rb_log_caught_up(&mut self, mut job: RebalanceJob) -> Result<RebalanceJob, ClusterError> {
        let group = self
            .raft
            .get_mut(&job.partition.get())
            .ok_or_else(|| ClusterError::Rebalance("raft group missing".into()))?;
        let leader = group
            .current_leader()
            .map(|(n, _)| n)
            .or_else(|| {
                job.old_replicas
                    .iter()
                    .copied()
                    .find(|n| group.peer(*n).is_some())
            })
            .ok_or_else(|| ClusterError::Rebalance("no leader/source for log catch-up".into()))?;
        for dest in &job.destinations {
            let n = group.stream_log_to(leader, *dest);
            job.log_entries_streamed += n;
        }
        job.phase = RebalancePhase::LogCaughtUp;
        Ok(job)
    }

    fn rb_membership_changed(
        &mut self,
        mut job: RebalanceJob,
    ) -> Result<RebalanceJob, ClusterError> {
        // Joint configuration: old ∪ new (durable via membership.json + job file).
        let epoch = job.plan_epoch;
        if let Some(group) = self.raft.get_mut(&job.partition.get()) {
            group.set_joint_voters(job.old_replicas.clone(), job.new_replicas.clone(), epoch);
        }
        job.joint = true;
        job.phase = RebalancePhase::MembershipChanged;
        Ok(job)
    }

    fn rb_epoch_activated(&mut self, mut job: RebalanceJob) -> Result<RebalanceJob, ClusterError> {
        let new_epoch = PlacementEpoch(job.plan_epoch.0 + 1);
        if let Some(group) = self.raft.get_mut(&job.partition.get()) {
            group.set_voters(job.new_replicas.clone(), new_epoch);
        }
        self.directory
            .set_replicas(job.partition, job.new_replicas.clone(), new_epoch);
        // Prefer a live leader from the new set.
        let _ = self.ensure_partition_leader(job.partition);
        self.directory.save(&self.root)?;
        // Bump meta epoch for open() compatibility.
        if let Ok(mut meta) = ClusterMeta::load(&self.root) {
            meta.placement_epoch = new_epoch.0;
            meta.format = ClusterMeta::FORMAT.to_string();
            let _ = meta.write(&self.root);
        }
        job.activated_epoch = Some(new_epoch);
        job.joint = false;
        job.phase = RebalancePhase::EpochActivated;
        Ok(job)
    }

    fn rb_safety_window(&mut self, mut job: RebalanceJob) -> Result<RebalanceJob, ClusterError> {
        // Old replicas retain data; nothing reclaimed yet.
        job.phase = RebalancePhase::SafetyWindow;
        Ok(job)
    }

    fn rb_reclaimed(&mut self, mut job: RebalanceJob) -> Result<RebalanceJob, ClusterError> {
        // Soft reclaim: note only. Physical purge of partition subjects on
        // removed nodes is optional and left for ops; we do not invent deletes
        // that would destroy salvage evidence.
        if !job.removals.is_empty() {
            // no-op data plane; placement already excludes them
        }
        job.phase = RebalancePhase::Reclaimed;
        Ok(job)
    }

    #[allow(clippy::too_many_arguments)] // query contact state is intentionally expanded
    fn contact_partition(
        &mut self,
        partition: PartitionId,
        assignment: &crate::directory::PartitionAssignment,
        read_mode: ReadMode,
        entries: &mut Vec<(String, Vec<u8>)>,
        seen: &mut HashSet<String>,
        prefix: Option<&str>,
        after_subject: Option<&str>,
        docs_scanned: &mut usize,
        max_docs: Option<usize>,
    ) -> Result<ContactOutcome, ClusterError> {
        // Linearizable prefers electable leader; available/salvage any replica.
        if read_mode == ReadMode::Linearizable
            && self.consistency_mode != ConsistencyMode::ConvergentAppend
        {
            if let Ok((leader, term)) = self.ensure_partition_leader(partition) {
                if let Some(store) = self.nodes.get(&leader.index()) {
                    let hit_budget = self.collect_partition_entries(
                        store,
                        partition,
                        entries,
                        seen,
                        prefix,
                        after_subject,
                        docs_scanned,
                        max_docs,
                    )?;
                    let pos = self
                        .raft
                        .get(&partition.get())
                        .map(|g| LogPosition(g.max_commit_index()))
                        .unwrap_or(LogPosition(0));
                    return Ok(if hit_budget {
                        ContactOutcome::BudgetExhausted {
                            term,
                            pos,
                            node: leader,
                        }
                    } else {
                        ContactOutcome::Served {
                            term,
                            pos,
                            node: leader,
                        }
                    });
                }
            }
        }

        for r in &assignment.replicas {
            if !self.is_online(*r) {
                continue;
            }
            if let Some(store) = self.nodes.get(&r.index()) {
                let hit_budget = self.collect_partition_entries(
                    store,
                    partition,
                    entries,
                    seen,
                    prefix,
                    after_subject,
                    docs_scanned,
                    max_docs,
                )?;
                let (term, pos) = self
                    .raft
                    .get(&partition.get())
                    .map(|g| {
                        (
                            g.current_leader()
                                .map(|(_, t)| t)
                                .unwrap_or(assignment.term),
                            LogPosition(g.max_commit_index()),
                        )
                    })
                    .unwrap_or((assignment.term, LogPosition(0)));
                return Ok(if hit_budget {
                    ContactOutcome::BudgetExhausted {
                        term,
                        pos,
                        node: *r,
                    }
                } else {
                    ContactOutcome::Served {
                        term,
                        pos,
                        node: *r,
                    }
                });
            }
        }
        Ok(ContactOutcome::Unavailable)
    }

    /// Collect live entries for one partition. Returns true if a docs budget stopped early.
    ///
    /// When `after_subject` is set, only subjects strictly greater than that
    /// value are returned (DEF-040 page resume).
    #[allow(clippy::too_many_arguments)] // page scan state stays explicit
    fn collect_partition_entries(
        &self,
        store: &Store,
        partition: PartitionId,
        entries: &mut Vec<(String, Vec<u8>)>,
        seen: &mut HashSet<String>,
        prefix: Option<&str>,
        after_subject: Option<&str>,
        docs_scanned: &mut usize,
        max_docs: Option<usize>,
    ) -> Result<bool, ClusterError> {
        for (subj_bytes, body) in store.live_entries() {
            let Ok(subject) = std::str::from_utf8(subj_bytes) else {
                continue;
            };
            if self.partition_for_subject(subject) != partition {
                continue;
            }
            if let Some(p) = prefix {
                if !subject.starts_with(p) {
                    continue;
                }
            }
            if let Some(after) = after_subject {
                if subject.as_bytes() <= after.as_bytes() {
                    continue;
                }
            }
            if !seen.insert(subject.to_string()) {
                continue;
            }
            *docs_scanned += 1;
            if let Some(max) = max_docs {
                if *docs_scanned > max {
                    return Ok(true);
                }
            }
            match store.get(subject) {
                Ok(Some(v)) => entries.push((subject.to_string(), v)),
                Ok(None) => {}
                Err(residiuum_store::StoreError::PayloadPartial) => {
                    let _ = body;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(false)
    }

    /// Per-subject history from the current partition leader's store.
    ///
    /// Used by the SDK cluster backend (Stage 8d). Elects a leader when needed.
    pub fn subject_history(
        &mut self,
        subject: &str,
    ) -> Result<residiuum_store::SubjectHistory, ClusterError> {
        let partition = self.partition_for_subject(subject);
        let (leader, _) = self.ensure_partition_leader(partition)?;
        let store = self
            .nodes
            .get(&leader.index())
            .ok_or(ClusterError::PartitionUnavailable {
                partition: partition.get(),
                reason: "leader handle missing",
            })?;
        Ok(store.history(subject)?)
    }

    // ── DEF-039 anti-entropy / repair ─────────────────────────────────────

    /// Build a verified hierarchical inventory for one partition.
    ///
    /// Collects per-replica subject digests (content hash), log frontier, and
    /// store segment fingerprints. Source selection uses integrity + majority
    /// (never mtime). See [`select_repair_source`].
    pub fn inventory_partition(
        &mut self,
        partition: PartitionId,
    ) -> Result<PartitionInventory, ClusterError> {
        let assignment = self
            .directory
            .get(partition)
            .cloned()
            .ok_or(ClusterError::NoLeader(partition.get()))?;

        let online_replicas: Vec<NodeId> = assignment
            .replicas
            .iter()
            .copied()
            .filter(|n| self.is_online(*n))
            .collect();

        let leader = self
            .raft
            .get(&partition.get())
            .and_then(|g| g.current_leader().map(|(n, _)| n));
        let log_frontier = self
            .raft
            .get(&partition.get())
            .map(|g| LogPosition(g.max_commit_index()))
            .unwrap_or(LogPosition(0));

        // Segment fingerprints per online replica (hierarchical inventory).
        let mut segment_fingerprints = Vec::new();
        for n in &online_replicas {
            if let Some(store) = self.nodes.get(&n.index()) {
                match store.segment_fingerprint() {
                    Ok(fp) => segment_fingerprints.push((*n, hash_hex(&fp))),
                    Err(_) => segment_fingerprints.push((*n, String::new())),
                }
            }
        }

        // Union of subjects observed on any online replica for this partition.
        let mut subject_set: HashSet<String> = HashSet::new();
        for n in &online_replicas {
            let store = match self.nodes.get(&n.index()) {
                Some(s) => s,
                None => continue,
            };
            for (subj_bytes, _) in store.live_entries() {
                let Ok(subject) = std::str::from_utf8(subj_bytes) else {
                    continue;
                };
                if self.partition_for_subject(subject) != partition {
                    continue;
                }
                subject_set.insert(subject.to_string());
            }
        }

        let mut subjects: Vec<SubjectInventory> = Vec::new();
        let mut needs_repair = 0u64;
        let mut irrecoverable = 0u64;
        let mut conflicts = 0u64;

        let mut subject_list: Vec<String> = subject_set.into_iter().collect();
        subject_list.sort();

        for subject in subject_list {
            let mut views = Vec::new();
            for n in &online_replicas {
                let store = match self.nodes.get(&n.index()) {
                    Some(s) => s,
                    None => continue,
                };
                let view = match store.get(&subject) {
                    Ok(Some(body)) => {
                        let h = body_content_hash(&body);
                        NodeSubjectView {
                            node: *n,
                            observation: ReplicaObservation::Healthy,
                            content_hash_hex: hash_hex(&h),
                            body: Some(body),
                        }
                    }
                    Ok(None) => NodeSubjectView {
                        node: *n,
                        observation: ReplicaObservation::Missing,
                        content_hash_hex: String::new(),
                        body: None,
                    },
                    Err(residiuum_store::StoreError::PayloadPartial)
                    | Err(residiuum_store::StoreError::PayloadConflict) => NodeSubjectView {
                        node: *n,
                        observation: ReplicaObservation::Corrupt,
                        content_hash_hex: String::new(),
                        body: None,
                    },
                    Err(e) => return Err(e.into()),
                };
                views.push(view);
            }

            let mut inv = SubjectInventory {
                subject: subject.clone(),
                partition,
                views: views.clone(),
                target_hash_hex: None,
                source_node: None,
                conflicting: false,
                irrecoverable: false,
            };

            match select_repair_source(&views, leader) {
                Ok((hash, src)) => {
                    let target_hex = hash_hex(&hash);
                    inv.target_hash_hex = Some(target_hex.clone());
                    inv.source_node = Some(src);
                    // Mark divergent vs target; count repair need.
                    let mut subject_needs = false;
                    for v in &mut inv.views {
                        match v.observation {
                            ReplicaObservation::Healthy | ReplicaObservation::Divergent => {
                                if v.content_hash_hex != target_hex {
                                    v.observation = ReplicaObservation::Divergent;
                                    subject_needs = true;
                                }
                            }
                            ReplicaObservation::Missing | ReplicaObservation::Corrupt => {
                                subject_needs = true;
                            }
                        }
                    }
                    if subject_needs {
                        needs_repair += 1;
                    }
                }
                Err(SourceSelectError::Conflict) => {
                    inv.conflicting = true;
                    conflicts += 1;
                    needs_repair += 1;
                }
                Err(SourceSelectError::Irrecoverable) => {
                    inv.irrecoverable = true;
                    irrecoverable += 1;
                    needs_repair += 1;
                }
                Err(SourceSelectError::AllMissing) => {
                    // Should not happen if subject came from live_entries.
                }
            }
            subjects.push(inv);
        }

        Ok(PartitionInventory {
            partition,
            replicas: assignment.replicas,
            online_replicas,
            log_frontier,
            leader,
            segment_fingerprints,
            subjects,
            needs_repair,
            irrecoverable,
            conflicts,
        })
    }

    /// Inventory every partition in the placement directory.
    pub fn inventory_cluster(&mut self) -> Result<ClusterInventory, ClusterError> {
        let partitions: Vec<PartitionId> = self
            .directory
            .assignments
            .iter()
            .map(|a| a.partition)
            .collect();
        let mut out = ClusterInventory::default();
        for p in partitions {
            let inv = self.inventory_partition(p)?;
            out.needs_repair += inv.needs_repair;
            out.irrecoverable += inv.irrecoverable;
            out.conflicts += inv.conflicts;
            out.partitions.push(inv);
        }
        Ok(out)
    }

    /// Run anti-entropy repair for one partition (DEF-039).
    ///
    /// Copies verified bodies from integrity-selected sources onto lagging or
    /// divergent online replicas. Corrupt observations are audited and never
    /// used as sources. Conflicts and irrecoverable holes stay explicit.
    /// Rate limits isolate repair from unbounded foreground impact.
    pub fn repair_partition(
        &mut self,
        partition: PartitionId,
        options: RepairOptions,
    ) -> Result<RepairReport, ClusterError> {
        let inv = self.inventory_partition(partition)?;
        let report = self.apply_repair_from_inventory(&inv, &options)?;
        if !report.audit.is_empty() && !options.dry_run {
            RepairAuditFile::append_and_save(&self.root, &report.audit)?;
        }
        Ok(report)
    }

    /// Run anti-entropy repair across all partitions (bounded by options).
    pub fn repair_cluster(&mut self, options: RepairOptions) -> Result<RepairReport, ClusterError> {
        let inv = self.inventory_cluster()?;
        let mut report = RepairReport {
            needs_repair_before: inv.needs_repair,
            ..RepairReport::default()
        };
        // Track remaining byte budget across partitions.
        let mut bytes_remaining = options.max_bytes;
        let mut subjects_remaining = options.max_subjects;
        for part in &inv.partitions {
            if report.budget_exhausted {
                report.budget_remaining_subjects = report
                    .budget_remaining_subjects
                    .saturating_add(part.needs_repair);
                continue;
            }
            let part_opts = RepairOptions {
                max_subjects: subjects_remaining,
                max_bytes: bytes_remaining,
                dry_run: options.dry_run,
            };
            let sub = self.apply_repair_from_inventory(part, &part_opts)?;
            report.subjects_repaired += sub.subjects_repaired;
            report.copies_written += sub.copies_written;
            report.corrupt_quarantined += sub.corrupt_quarantined;
            report.conflicts_preserved += sub.conflicts_preserved;
            report.irrecoverable_holes += sub.irrecoverable_holes;
            report.audit.extend(sub.audit);
            if sub.budget_exhausted {
                report.budget_exhausted = true;
                report.budget_remaining_subjects += sub.budget_remaining_subjects;
            }
            if let Some(max) = subjects_remaining {
                subjects_remaining = Some(max.saturating_sub(sub.subjects_repaired as usize));
                if subjects_remaining == Some(0) {
                    report.budget_exhausted = true;
                }
            }
            // Coarse byte accounting: reduce by copies when max_bytes set.
            if let Some(ref mut rem) = bytes_remaining {
                // apply_repair enforces the cap; clear remaining when exhausted.
                if sub.budget_exhausted && options.max_bytes.is_some() {
                    *rem = 0;
                }
            }
        }
        if !report.audit.is_empty() && !options.dry_run {
            RepairAuditFile::append_and_save(&self.root, &report.audit)?;
        }
        Ok(report)
    }

    /// One-shot cluster anti-entropy: inventory + repair with default options.
    pub fn anti_entropy_once(&mut self) -> Result<RepairReport, ClusterError> {
        self.repair_cluster(RepairOptions::unlimited())
    }

    /// Load durable repair audit log (DEF-039).
    pub fn repair_audit(&self) -> Result<RepairAuditFile, ClusterError> {
        RepairAuditFile::load(&self.root)
    }

    fn apply_repair_from_inventory(
        &mut self,
        inv: &PartitionInventory,
        options: &RepairOptions,
    ) -> Result<RepairReport, ClusterError> {
        let mut report = RepairReport {
            needs_repair_before: inv.needs_repair,
            ..RepairReport::default()
        };
        let mut subjects_used = 0usize;
        let mut bytes_used = 0u64;
        let mut remaining_after_budget = 0u64;

        for subj in &inv.subjects {
            // Count corrupt observations for quarantine audit.
            for v in &subj.views {
                if v.observation == ReplicaObservation::Corrupt {
                    report.corrupt_quarantined += 1;
                    report.audit.push(RepairAuditEntry {
                        seq: 0,
                        subject: subj.subject.clone(),
                        partition: inv.partition,
                        action: RepairActionKind::QuarantinedCorrupt,
                        source: None,
                        destination: Some(v.node),
                        content_hash_hex: String::new(),
                        reason: "payload unreadable; never used as repair source".into(),
                    });
                }
            }

            if subj.conflicting {
                report.conflicts_preserved += 1;
                report.audit.push(RepairAuditEntry {
                    seq: 0,
                    subject: subj.subject.clone(),
                    partition: inv.partition,
                    action: RepairActionKind::PreservedConflict,
                    source: None,
                    destination: None,
                    content_hash_hex: String::new(),
                    reason: "equal-vote content split; no mtime winner".into(),
                });
                continue;
            }

            if subj.irrecoverable {
                report.irrecoverable_holes += 1;
                report.audit.push(RepairAuditEntry {
                    seq: 0,
                    subject: subj.subject.clone(),
                    partition: inv.partition,
                    action: RepairActionKind::IrrecoverableHole,
                    source: None,
                    destination: None,
                    content_hash_hex: String::new(),
                    reason: "no healthy replica holds a verified body".into(),
                });
                continue;
            }

            let (Some(target_hex), Some(source_node)) =
                (subj.target_hash_hex.as_ref(), subj.source_node)
            else {
                continue;
            };

            // Destinations that need the target body.
            let mut dests: Vec<NodeId> = Vec::new();
            for v in &subj.views {
                match v.observation {
                    ReplicaObservation::Missing | ReplicaObservation::Corrupt => {
                        dests.push(v.node);
                    }
                    ReplicaObservation::Divergent => dests.push(v.node),
                    ReplicaObservation::Healthy => {
                        if &v.content_hash_hex != target_hex {
                            dests.push(v.node);
                        }
                    }
                }
            }
            dests.retain(|d| *d != source_node);
            if dests.is_empty() {
                continue;
            }

            // Rate limit: subject budget.
            if let Some(max) = options.max_subjects {
                if subjects_used >= max {
                    remaining_after_budget += 1;
                    report.budget_exhausted = true;
                    continue;
                }
            }

            // Obtain source body (prefer in-memory view; re-read if needed).
            let body = {
                let from_view = subj
                    .views
                    .iter()
                    .find(|v| v.node == source_node)
                    .and_then(|v| v.body.clone());
                if let Some(b) = from_view {
                    b
                } else if let Some(store) = self.nodes.get(&source_node.index()) {
                    match store.get(&subj.subject)? {
                        Some(b) => b,
                        None => {
                            // Source lost the body between inventory and repair.
                            report.irrecoverable_holes += 1;
                            report.audit.push(RepairAuditEntry {
                                seq: 0,
                                subject: subj.subject.clone(),
                                partition: inv.partition,
                                action: RepairActionKind::IrrecoverableHole,
                                source: Some(source_node),
                                destination: None,
                                content_hash_hex: target_hex.clone(),
                                reason: "source lost body after inventory".into(),
                            });
                            continue;
                        }
                    }
                } else {
                    continue;
                }
            };

            // Verify source body still matches selected hash (integrity).
            let actual = hash_hex(&body_content_hash(&body));
            if &actual != target_hex {
                report.audit.push(RepairAuditEntry {
                    seq: 0,
                    subject: subj.subject.clone(),
                    partition: inv.partition,
                    action: RepairActionKind::QuarantinedCorrupt,
                    source: Some(source_node),
                    destination: None,
                    content_hash_hex: actual,
                    reason: "source body hash drifted after inventory; skipped".into(),
                });
                report.corrupt_quarantined += 1;
                continue;
            }

            let body_len = body.len() as u64;
            if let Some(max_b) = options.max_bytes {
                if bytes_used.saturating_add(body_len.saturating_mul(dests.len() as u64)) > max_b
                    && bytes_used > 0
                {
                    remaining_after_budget += 1;
                    report.budget_exhausted = true;
                    continue;
                }
            }

            if options.dry_run {
                subjects_used += 1;
                report.subjects_repaired += 1;
                for d in &dests {
                    report.copies_written += 1;
                    report.audit.push(RepairAuditEntry {
                        seq: 0,
                        subject: subj.subject.clone(),
                        partition: inv.partition,
                        action: RepairActionKind::Copied,
                        source: Some(source_node),
                        destination: Some(*d),
                        content_hash_hex: target_hex.clone(),
                        reason: "dry-run majority/integrity source".into(),
                    });
                }
                continue;
            }

            let mut wrote_any = false;
            for d in &dests {
                if !self.is_online(*d) {
                    continue;
                }
                // Byte budget per write.
                if let Some(max_b) = options.max_bytes {
                    if bytes_used.saturating_add(body_len) > max_b && bytes_used > 0 {
                        remaining_after_budget += 1;
                        report.budget_exhausted = true;
                        break;
                    }
                }
                let store = match self.nodes.get_mut(&d.index()) {
                    Some(s) => s,
                    None => continue,
                };
                // Skip if destination already matches (race).
                if let Ok(Some(cur)) = store.get(&subj.subject) {
                    if hash_hex(&body_content_hash(&cur)) == *target_hex {
                        report.audit.push(RepairAuditEntry {
                            seq: 0,
                            subject: subj.subject.clone(),
                            partition: inv.partition,
                            action: RepairActionKind::AlreadyMatched,
                            source: Some(source_node),
                            destination: Some(*d),
                            content_hash_hex: target_hex.clone(),
                            reason: "destination already matched target".into(),
                        });
                        continue;
                    }
                }
                store.put(&subj.subject, &body, DurabilityMode::Durable)?;
                // Verify destination after copy (DEF-039).
                let verify = store.get(&subj.subject)?;
                let ok = verify
                    .as_ref()
                    .map(|b| hash_hex(&body_content_hash(b)) == *target_hex)
                    .unwrap_or(false);
                if !ok {
                    return Err(ClusterError::ReplicationRejected(format!(
                        "repair verify failed for subject {} on node {}",
                        subj.subject,
                        d.index()
                    )));
                }
                bytes_used = bytes_used.saturating_add(body_len);
                report.copies_written += 1;
                wrote_any = true;
                report.audit.push(RepairAuditEntry {
                    seq: 0,
                    subject: subj.subject.clone(),
                    partition: inv.partition,
                    action: RepairActionKind::Copied,
                    source: Some(source_node),
                    destination: Some(*d),
                    content_hash_hex: target_hex.clone(),
                    reason: "majority/integrity source; verified after put".into(),
                });
            }
            if wrote_any {
                subjects_used += 1;
                report.subjects_repaired += 1;
            }
        }

        report.budget_remaining_subjects = remaining_after_budget;
        Ok(report)
    }

    /// Run ordinary store salvage on one node without using cluster metadata.
    ///
    /// Proves CLUSTER_SPEC §6.1 / §22 item 19: node salvage without cluster
    /// software yields ordinary segments.
    pub fn salvage_node(&self, node: NodeId) -> Result<SalvageReport, ClusterError> {
        let path = self.node_path(node);
        if let Some(store) = self.nodes.get(&node.index()) {
            return Ok(store.salvage()?);
        }
        let store = Store::open_inspect(&path)?;
        Ok(store.salvage()?)
    }

    /// Salvage a node store path with **no** cluster handle (standalone API).
    pub fn salvage_node_path(path: impl AsRef<Path>) -> Result<SalvageReport, ClusterError> {
        let store = Store::open_inspect(path)?;
        Ok(store.salvage()?)
    }
}

enum ContactOutcome {
    Served {
        term: Term,
        pos: LogPosition,
        node: NodeId,
    },
    BudgetExhausted {
        term: Term,
        pos: LogPosition,
        node: NodeId,
    },
    Unavailable,
}
