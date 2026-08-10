//! Client-side partition directory cache (CLUSTER_SPEC §13, Stage 8d).
//!
//! Clients MAY cache partition → leader/replica routes and refresh on stale
//! placement epochs. A stale route may cost a redirect; it MUST NOT authorize
//! an obsolete writer (CLUSTER_SPEC §13).
//!
//! This module is deliberately free of `residiuum-cluster` so remote routing stays
//! available on the MPL embedded/remote SDK path. Wire types use plain integers;
//! in-process cluster code converts [`residiuum_cluster`] directories under the
//! `cluster` feature (see [`DirectorySnapshot::from_cluster_directory`]).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Published hash profile name for partition-key → virtual partition mapping.
pub const HASH_PROFILE_BLAKE3_MOD: &str = "blake3-mod-v1";

/// Dense node index within a cluster (wire form of `residiuum_cluster::NodeId`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(pub u32);

impl NodeId {
    /// Construct from a dense node index.
    pub fn new(index: u32) -> Self {
        Self(index)
    }

    /// Dense index used for endpoint maps.
    pub fn index(self) -> u32 {
        self.0
    }
}

/// Virtual partition identifier (wire form of `residiuum_cluster::PartitionId`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionId(pub u32);

impl PartitionId {
    /// Wrap a virtual partition index.
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Raw index.
    pub fn get(self) -> u32 {
        self.0
    }
}

/// Leadership term (wire form).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct Term(pub u64);

/// Placement generation (wire form).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct PlacementEpoch(pub u64);

/// Parameters of the deterministic partition map for one store generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionMap {
    /// Number of virtual partitions (must be ≥ 1).
    pub virtual_partitions: u32,
    /// Published hash profile identifier.
    pub hash_profile: String,
}

impl PartitionMap {
    /// Create with an explicit virtual partition count.
    pub fn new(virtual_partitions: u32) -> Self {
        assert!(
            virtual_partitions >= 1,
            "need at least one virtual partition"
        );
        Self {
            virtual_partitions,
            hash_profile: HASH_PROFILE_BLAKE3_MOD.to_string(),
        }
    }

    /// Map a partition key to a virtual partition (CLUSTER_SPEC §8.1–§8.2).
    pub fn partition_of(&self, partition_key: &[u8]) -> PartitionId {
        let hash = blake3::hash(partition_key);
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&hash.as_bytes()[..8]);
        let n = u64::from_le_bytes(buf);
        let id = (n % u64::from(self.virtual_partitions)) as u32;
        PartitionId::new(id)
    }
}

/// Snapshot of one partition route held by a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedRoute {
    /// Virtual partition.
    pub partition: PartitionId,
    /// Cached leader / primary for linearizable writes.
    pub leader: NodeId,
    /// Leadership term observed when this entry was cached.
    pub term: Term,
    /// Placement epoch for this assignment.
    pub placement_epoch: PlacementEpoch,
    /// Voting replicas (may be used for available reads / convergent ingest).
    pub replicas: Vec<NodeId>,
}

/// Wire-friendly directory snapshot for RPC / multi-seed connect (Stage 8d).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectorySnapshot {
    /// Virtual partition count.
    pub virtual_partitions: u32,
    /// Published hash profile name.
    pub hash_profile: String,
    /// Whole-directory placement epoch.
    pub placement_epoch: u64,
    /// Per-partition assignments.
    pub assignments: Vec<AssignmentWire>,
    /// Optional node id → `host:port` endpoints for direct routing.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub endpoints: HashMap<u32, String>,
}

/// One assignment on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentWire {
    /// Partition index.
    pub partition: u32,
    /// Replica node indexes.
    pub replicas: Vec<u32>,
    /// Leader node index.
    pub leader: u32,
    /// Leadership term.
    pub term: u64,
    /// Placement epoch for this assignment.
    pub placement_epoch: u64,
}

impl DirectorySnapshot {
    /// Build from a live `residiuum-cluster` placement directory and optional endpoints.
    ///
    /// Available only when the `cluster` feature is enabled (AGPL `residiuum-cluster`).
    #[cfg(feature = "cluster")]
    pub fn from_cluster_directory(
        dir: &residiuum_cluster::PartitionDirectory,
        endpoints: HashMap<u32, String>,
    ) -> Self {
        Self {
            virtual_partitions: dir.map.virtual_partitions,
            hash_profile: dir.map.hash_profile.clone(),
            placement_epoch: dir.placement_epoch.0,
            assignments: dir
                .assignments
                .iter()
                .map(|a| AssignmentWire {
                    partition: a.partition.get(),
                    replicas: a.replicas.iter().map(|n| n.index()).collect(),
                    leader: a.leader.index(),
                    term: a.term.0,
                    placement_epoch: a.placement_epoch.0,
                })
                .collect(),
            endpoints,
        }
    }

    /// Compatibility alias used by serve / cluster glue under the `cluster` feature.
    #[cfg(feature = "cluster")]
    pub fn from_directory(
        dir: &residiuum_cluster::PartitionDirectory,
        endpoints: HashMap<u32, String>,
    ) -> Self {
        Self::from_cluster_directory(dir, endpoints)
    }
}

/// Bounded client cache of partition routes (CLUSTER_SPEC §13).
///
/// Hot path: hash subject → partition → cached leader, then open a connection
/// to that node. On `stale_epoch` / misrouting, refresh the affected entry
/// (or the whole directory) and retry with the same event identity.
#[derive(Debug, Clone)]
pub struct ClientDirectoryCache {
    map: PartitionMap,
    placement_epoch: PlacementEpoch,
    /// partition index → route
    routes: HashMap<u32, CachedRoute>,
    /// node index → `host:port` (empty for in-process cluster handles)
    endpoints: HashMap<u32, String>,
    /// Partitions marked stale until the next successful refresh of that entry.
    stale: HashMap<u32, bool>,
    /// How many full-directory refreshes have been applied (diagnostics/tests).
    refresh_count: u64,
    /// How many single-entry refreshes have been applied.
    entry_refresh_count: u64,
}

impl ClientDirectoryCache {
    /// Empty cache (must [`Self::replace_from_snapshot`] before routing).
    pub fn empty() -> Self {
        Self {
            map: PartitionMap::new(1),
            placement_epoch: PlacementEpoch(0),
            routes: HashMap::new(),
            endpoints: HashMap::new(),
            stale: HashMap::new(),
            refresh_count: 0,
            entry_refresh_count: 0,
        }
    }

    /// Build a cache from a `residiuum-cluster` placement directory (no network endpoints).
    #[cfg(feature = "cluster")]
    pub fn from_directory(dir: &residiuum_cluster::PartitionDirectory) -> Self {
        let snap = DirectorySnapshot::from_cluster_directory(dir, HashMap::new());
        Self::from_snapshot(&snap)
    }

    /// Build from a wire snapshot (includes optional endpoints).
    pub fn from_snapshot(snap: &DirectorySnapshot) -> Self {
        let mut cache = Self::empty();
        cache.replace_from_snapshot(snap);
        cache
    }

    /// Replace the entire cache from a wire snapshot.
    pub fn replace_from_snapshot(&mut self, snap: &DirectorySnapshot) {
        self.map = PartitionMap {
            virtual_partitions: snap.virtual_partitions.max(1),
            hash_profile: snap.hash_profile.clone(),
        };
        self.placement_epoch = PlacementEpoch(snap.placement_epoch);
        self.routes.clear();
        for a in &snap.assignments {
            self.routes.insert(
                a.partition,
                CachedRoute {
                    partition: PartitionId::new(a.partition),
                    leader: NodeId::new(a.leader),
                    term: Term(a.term),
                    placement_epoch: PlacementEpoch(a.placement_epoch),
                    replicas: a.replicas.iter().copied().map(NodeId::new).collect(),
                },
            );
        }
        self.endpoints = snap.endpoints.clone();
        self.stale.clear();
        self.refresh_count = self.refresh_count.saturating_add(1);
    }

    /// Replace the entire cache from a `residiuum-cluster` directory + endpoints.
    #[cfg(feature = "cluster")]
    pub fn replace(
        &mut self,
        dir: &residiuum_cluster::PartitionDirectory,
        endpoints: HashMap<u32, String>,
    ) {
        let snap = DirectorySnapshot::from_cluster_directory(dir, endpoints);
        self.replace_from_snapshot(&snap);
    }

    /// Refresh a single partition assignment from wire fields.
    pub fn refresh_entry_wire(&mut self, assignment: &AssignmentWire) {
        self.routes.insert(
            assignment.partition,
            CachedRoute {
                partition: PartitionId::new(assignment.partition),
                leader: NodeId::new(assignment.leader),
                term: Term(assignment.term),
                placement_epoch: PlacementEpoch(assignment.placement_epoch),
                replicas: assignment
                    .replicas
                    .iter()
                    .copied()
                    .map(NodeId::new)
                    .collect(),
            },
        );
        self.stale.remove(&assignment.partition);
        let epoch = PlacementEpoch(assignment.placement_epoch);
        if epoch > self.placement_epoch {
            self.placement_epoch = epoch;
        }
        self.entry_refresh_count = self.entry_refresh_count.saturating_add(1);
    }

    /// Refresh a single partition assignment (CLUSTER_SPEC §13 step 4).
    #[cfg(feature = "cluster")]
    pub fn refresh_entry(&mut self, assignment: &residiuum_cluster::PartitionAssignment) {
        self.refresh_entry_wire(&AssignmentWire {
            partition: assignment.partition.get(),
            replicas: assignment.replicas.iter().map(|n| n.index()).collect(),
            leader: assignment.leader.index(),
            term: assignment.term.0,
            placement_epoch: assignment.placement_epoch.0,
        });
    }

    /// Mark a partition route as stale (forces refresh on next route lookup
    /// when using [`Self::route_checked`]).
    pub fn mark_stale(&mut self, partition: PartitionId) {
        self.stale.insert(partition.get(), true);
    }

    /// Poison the cached leader for tests (simulates stale placement).
    #[doc(hidden)]
    pub fn poison_leader(&mut self, partition: PartitionId, fake_leader: NodeId) {
        if let Some(r) = self.routes.get_mut(&partition.get()) {
            r.leader = fake_leader;
            // Keep epoch so the caller can observe a leader mismatch without
            // an epoch bump; production servers use epoch and/or not_leader.
        }
    }

    /// Partition map used for hashing.
    pub fn partition_map(&self) -> &PartitionMap {
        &self.map
    }

    /// Whole-directory placement epoch.
    pub fn placement_epoch(&self) -> PlacementEpoch {
        self.placement_epoch
    }

    /// Number of full-directory refreshes applied.
    pub fn refresh_count(&self) -> u64 {
        self.refresh_count
    }

    /// Number of single-entry refreshes applied.
    pub fn entry_refresh_count(&self) -> u64 {
        self.entry_refresh_count
    }

    /// Endpoint for a node, if known.
    pub fn endpoint(&self, node: NodeId) -> Option<&str> {
        self.endpoints.get(&node.index()).map(|s| s.as_str())
    }

    /// All known endpoints.
    pub fn endpoints(&self) -> &HashMap<u32, String> {
        &self.endpoints
    }

    /// Map subject bytes to a virtual partition.
    pub fn partition_of(&self, partition_key: &[u8]) -> PartitionId {
        self.map.partition_of(partition_key)
    }

    /// Lookup a cached route for a partition (no staleness check).
    pub fn get(&self, partition: PartitionId) -> Option<&CachedRoute> {
        self.routes.get(&partition.get())
    }

    /// Whether the partition entry is marked stale.
    pub fn is_stale(&self, partition: PartitionId) -> bool {
        self.stale.get(&partition.get()).copied().unwrap_or(false)
    }

    /// Route a partition key (subject) to a cached leader route.
    ///
    /// Returns `None` if the partition is unknown or marked stale.
    pub fn route_checked(&self, partition_key: &[u8]) -> Option<&CachedRoute> {
        let p = self.partition_of(partition_key);
        if self.is_stale(p) {
            return None;
        }
        self.get(p)
    }

    /// Route without staleness gate (may return a known-stale entry).
    pub fn route(&self, partition_key: &[u8]) -> Option<&CachedRoute> {
        let p = self.partition_of(partition_key);
        self.get(p)
    }

    /// True when `observed` is strictly newer than the cached epoch for that
    /// partition (or the directory epoch when the partition is unknown).
    pub fn is_observed_epoch_newer(
        &self,
        partition: PartitionId,
        observed: PlacementEpoch,
    ) -> bool {
        let cached = self
            .get(partition)
            .map(|r| r.placement_epoch)
            .unwrap_or(self.placement_epoch);
        observed > cached
    }

    /// Snapshot for wire / debugging.
    pub fn snapshot(&self) -> DirectorySnapshot {
        let mut assignments: Vec<AssignmentWire> = self
            .routes
            .values()
            .map(|r| AssignmentWire {
                partition: r.partition.get(),
                replicas: r.replicas.iter().map(|n| n.index()).collect(),
                leader: r.leader.index(),
                term: r.term.0,
                placement_epoch: r.placement_epoch.0,
            })
            .collect();
        assignments.sort_by_key(|a| a.partition);
        DirectorySnapshot {
            virtual_partitions: self.map.virtual_partitions,
            hash_profile: self.map.hash_profile.clone(),
            placement_epoch: self.placement_epoch.0,
            assignments,
            endpoints: self.endpoints.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn balanced_snapshot(
        virtual_partitions: u32,
        node_count: u32,
        epoch: u64,
    ) -> DirectorySnapshot {
        assert!(node_count >= 1);
        let replicas: Vec<u32> = (0..node_count).collect();
        let mut assignments = Vec::with_capacity(virtual_partitions as usize);
        for p in 0..virtual_partitions {
            assignments.push(AssignmentWire {
                partition: p,
                replicas: replicas.clone(),
                leader: p % node_count,
                term: 1,
                placement_epoch: epoch,
            });
        }
        DirectorySnapshot {
            virtual_partitions,
            hash_profile: HASH_PROFILE_BLAKE3_MOD.to_string(),
            placement_epoch: epoch,
            assignments,
            endpoints: HashMap::new(),
        }
    }

    #[test]
    fn route_is_stable_for_subject() {
        let snap = balanced_snapshot(8, 3, 1);
        let cache = ClientDirectoryCache::from_snapshot(&snap);
        let a = cache.route(b"users/alice").unwrap().clone();
        let b = cache.route(b"users/alice").unwrap().clone();
        assert_eq!(a, b);
        assert_eq!(a.partition, cache.partition_of(b"users/alice"));
    }

    #[test]
    fn stale_gate_hides_route_until_refresh() {
        let snap = balanced_snapshot(4, 2, 1);
        let mut cache = ClientDirectoryCache::from_snapshot(&snap);
        let p = cache.partition_of(b"k");
        cache.mark_stale(p);
        assert!(cache.route_checked(b"k").is_none());
        let assignment = snap
            .assignments
            .iter()
            .find(|a| a.partition == p.get())
            .unwrap()
            .clone();
        cache.refresh_entry_wire(&assignment);
        assert!(cache.route_checked(b"k").is_some());
        assert_eq!(cache.entry_refresh_count(), 1);
    }

    #[test]
    fn snapshot_roundtrip() {
        let mut snap = balanced_snapshot(4, 2, 3);
        snap.endpoints.insert(0, "127.0.0.1:7400".into());
        snap.endpoints.insert(1, "127.0.0.1:7401".into());
        let cache = ClientDirectoryCache::from_snapshot(&snap);
        assert_eq!(cache.placement_epoch().0, 3);
        assert_eq!(cache.endpoint(NodeId::new(0)), Some("127.0.0.1:7400"));
        assert_eq!(cache.routes.len(), 4);
    }
}
