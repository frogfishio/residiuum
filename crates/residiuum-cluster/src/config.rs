//! Cluster configuration and on-disk cluster descriptor.

use crate::error::ClusterError;
use crate::id::ClusterId;
use crate::modes::{ConsistencyMode, DeploymentProfile};
use crate::partition::{PartitionMap, DEFAULT_VIRTUAL_PARTITIONS, HASH_PROFILE_BLAKE3_MOD};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// In-memory configuration used to create a cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Root directory for cluster metadata and node stores.
    pub root: PathBuf,
    /// Deployment profile.
    pub profile: DeploymentProfile,
    /// Virtual partition map.
    pub partition_map: PartitionMap,
    /// Write consistency mode.
    pub consistency_mode: ConsistencyMode,
    /// Optional fixed cluster id (otherwise generated).
    pub cluster_id: Option<ClusterId>,
}

impl ClusterConfig {
    /// Development profile: one node under `root`.
    pub fn development(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            profile: DeploymentProfile::Development,
            partition_map: PartitionMap::new(DEFAULT_VIRTUAL_PARTITIONS),
            consistency_mode: ConsistencyMode::PartitionLinearizable,
            cluster_id: None,
        }
    }

    /// Dependable local: three voting nodes under `root`.
    pub fn dependable_local(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            profile: DeploymentProfile::DependableLocal,
            partition_map: PartitionMap::new(DEFAULT_VIRTUAL_PARTITIONS),
            consistency_mode: ConsistencyMode::PartitionLinearizable,
            cluster_id: None,
        }
    }

    /// Override virtual partition count.
    pub fn with_virtual_partitions(mut self, n: u32) -> Self {
        self.partition_map = PartitionMap::new(n);
        self
    }

    /// Override consistency mode.
    pub fn with_consistency_mode(mut self, mode: ConsistencyMode) -> Self {
        self.consistency_mode = mode;
        self
    }
}

/// Persistent cluster descriptor (`cluster.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMeta {
    /// Format tag for this draft descriptor.
    pub format: String,
    /// Cluster id.
    pub cluster_id: String,
    /// Profile name.
    pub profile: String,
    /// Consistency mode name.
    pub consistency_mode: String,
    /// Virtual partition count.
    pub virtual_partitions: u32,
    /// Hash profile name.
    pub hash_profile: String,
    /// Node count.
    pub node_count: u32,
    /// Placement epoch at create time.
    pub placement_epoch: u64,
    /// Optional BLAKE3 hex of the endpoint registration secret (DEF-038).
    ///
    /// When set, [`crate::endpoints::upsert_endpoint_authenticated`] requires a
    /// matching plaintext secret. Absent ⇒ unauthenticated upserts allowed
    /// (development / existing clusters).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration_token_hash: Option<String>,
}

impl ClusterMeta {
    pub(crate) const FORMAT: &'static str = "residiuum-cluster-8f";
    /// Formats accepted on open (8a–8f).
    pub(crate) const FORMAT_COMPAT: &'static [&'static str] = &[
        "residiuum-cluster-8f",
        "residiuum-cluster-8e",
        "residiuum-cluster-8c",
        "residiuum-cluster-8b",
        "residiuum-cluster-8a",
    ];

    pub(crate) fn from_config(cfg: &ClusterConfig, cluster_id: ClusterId) -> Self {
        Self {
            format: Self::FORMAT.to_string(),
            cluster_id: cluster_id.to_hex(),
            profile: cfg.profile.as_str().to_string(),
            consistency_mode: cfg.consistency_mode.as_str().to_string(),
            virtual_partitions: cfg.partition_map.virtual_partitions,
            hash_profile: cfg.partition_map.hash_profile.clone(),
            node_count: cfg.profile.default_node_count(),
            placement_epoch: 1,
            registration_token_hash: None,
        }
    }

    /// BLAKE3 hex of a registration secret (for `registration_token_hash`).
    pub fn hash_registration_secret(secret: &str) -> String {
        blake3::hash(secret.as_bytes()).to_hex().to_string()
    }

    /// Persist a registration secret requirement (DEF-038 endpoint auth).
    ///
    /// Subsequent authenticated endpoint upserts must present the same secret.
    pub fn set_registration_secret(root: &Path, secret: &str) -> Result<(), ClusterError> {
        let mut meta = Self::load(root)?;
        meta.registration_token_hash = Some(Self::hash_registration_secret(secret));
        meta.write(root)
    }

    /// Clear registration secret requirement (dev only).
    pub fn clear_registration_secret(root: &Path) -> Result<(), ClusterError> {
        let mut meta = Self::load(root)?;
        meta.registration_token_hash = None;
        meta.write(root)
    }

    /// Verify a plaintext registration secret against this meta (if configured).
    pub fn check_registration_secret(&self, secret: &str) -> Result<(), ClusterError> {
        match &self.registration_token_hash {
            None => Ok(()),
            Some(expect) => {
                let got = Self::hash_registration_secret(secret);
                if &got == expect {
                    Ok(())
                } else {
                    Err(ClusterError::ReplicationRejected(
                        "endpoint registration secret mismatch".into(),
                    ))
                }
            }
        }
    }

    pub(crate) fn write(&self, root: &Path) -> Result<(), ClusterError> {
        let path = root.join("cluster.json");
        let json = serde_json::to_string_pretty(self)
            .map_err(|_| ClusterError::CorruptMeta("serialize cluster.json"))?;
        // Keep previous generation: cluster identity is not trivially rebuilt (DEF-021).
        residiuum_store::write_atomic_keep_previous(&path, json.as_bytes())?;
        Ok(())
    }

    /// Load `cluster.json` from a cluster root.
    ///
    /// Falls back to `cluster.json.prev` when the primary is corrupt (DEF-021).
    pub fn load(root: &Path) -> Result<Self, ClusterError> {
        let path = root.join("cluster.json");
        if let Some(meta) = Self::try_parse(&path)? {
            return Ok(meta);
        }
        let prev = residiuum_store::previous_path(&path);
        if let Some(meta) = Self::try_parse(&prev)? {
            return Ok(meta);
        }
        if !path.is_file() && !prev.is_file() {
            return Err(ClusterError::NotACluster(format!(
                "missing cluster.json at {}",
                root.display()
            )));
        }
        Err(ClusterError::CorruptMeta(
            "cluster.json unreadable; restore cluster.json.prev or recreate the cluster root",
        ))
    }

    fn try_parse(path: &Path) -> Result<Option<Self>, ClusterError> {
        if !path.is_file() {
            return Ok(None);
        }
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(_) => return Ok(None),
        };
        let meta: Self = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(_) => return Ok(None),
        };
        if !Self::FORMAT_COMPAT.contains(&meta.format.as_str()) {
            return Ok(None);
        }
        if meta.hash_profile != HASH_PROFILE_BLAKE3_MOD {
            return Ok(None);
        }
        Ok(Some(meta))
    }
}

/// Path helpers for a cluster root.
pub fn node_store_path(root: &Path, node_index: u32) -> PathBuf {
    root.join("nodes").join(format!("node-{node_index}"))
}
