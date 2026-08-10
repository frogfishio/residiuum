//! Node endpoint map for network multi-node serve (post–Stage 8 follow-on).
//!
//! In-process clusters do not need host:port maps. Process-per-node TCP serve
//! records `node_index → host:port` in `endpoints.json` so clients and
//! `directory` RPC responses can advertise real routes (CLUSTER_SPEC §13).
//!
//! Writes are atomic + durable (DEF-021): temp → sync → keep `.prev` → rename
//! → dirsync. Concurrent registrations take a process-local + OS advisory lock
//! so one upsert cannot clobber unrelated endpoints.

use crate::error::ClusterError;
use blake3::Hasher;
use residiuum_store::{previous_path, write_atomic_keep_previous};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

/// Filename under the cluster root.
pub const ENDPOINTS_FILE: &str = "endpoints.json";

/// Advisory lock file next to endpoints (not the document itself).
const ENDPOINTS_LOCK: &str = "endpoints.lock";

/// On-disk endpoints document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointsFile {
    /// Format tag.
    #[serde(default = "default_format")]
    pub format: String,
    /// Monotonic generation; increments on every successful save (DEF-021).
    #[serde(default)]
    pub generation: u64,
    /// BLAKE3-256 hex of the canonical endpoints map (keys sorted).
    #[serde(default)]
    pub content_blake3: String,
    /// Node dense index → `host:port` (or other connect string).
    #[serde(default)]
    pub endpoints: HashMap<String, String>,
}

fn default_format() -> String {
    "residiuum-cluster-endpoints-1".into()
}

fn process_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

impl EndpointsFile {
    /// Empty map.
    pub fn new() -> Self {
        Self {
            format: default_format(),
            generation: 0,
            content_blake3: String::new(),
            endpoints: HashMap::new(),
        }
    }

    /// Convert to `u32 → host:port` map (invalid keys skipped).
    pub fn as_u32_map(&self) -> HashMap<u32, String> {
        let mut out = HashMap::new();
        for (k, v) in &self.endpoints {
            if let Ok(idx) = k.parse::<u32>() {
                out.insert(idx, v.clone());
            }
        }
        out
    }

    /// Build from a `u32 → host:port` map.
    pub fn from_u32_map(map: &HashMap<u32, String>) -> Self {
        let mut endpoints = HashMap::new();
        for (k, v) in map {
            endpoints.insert(k.to_string(), v.clone());
        }
        let mut file = Self {
            format: default_format(),
            generation: 0,
            content_blake3: String::new(),
            endpoints,
        };
        file.refresh_checksum();
        file
    }

    /// Set one node endpoint.
    pub fn set(&mut self, node_index: u32, hostport: impl Into<String>) {
        self.endpoints
            .insert(node_index.to_string(), hostport.into());
    }

    /// Recompute [`Self::content_blake3`] from the endpoints map.
    pub fn refresh_checksum(&mut self) {
        self.content_blake3 = endpoints_content_hash(&self.endpoints);
    }

    /// Validate format + checksum when a hash is present.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.format != default_format() && !self.format.is_empty() {
            // Accept only the known format tag when set.
            if self.format != "residiuum-cluster-endpoints-1" {
                return Err(ClusterError::CorruptMeta("unsupported endpoints format"));
            }
        }
        if !self.content_blake3.is_empty() {
            let expect = endpoints_content_hash(&self.endpoints);
            if self.content_blake3 != expect {
                return Err(ClusterError::CorruptMeta(
                    "endpoints.json content_blake3 mismatch",
                ));
            }
        }
        Ok(())
    }

    /// Load from `root/endpoints.json`, or empty if missing.
    ///
    /// Falls back to `endpoints.json.prev` when the primary is corrupt (DEF-021).
    pub fn load(root: &Path) -> Result<Self, ClusterError> {
        let path = root.join(ENDPOINTS_FILE);
        if let Some(file) = try_parse_endpoints(&path)? {
            return Ok(file);
        }
        let prev = previous_path(&path);
        if let Some(file) = try_parse_endpoints(&prev)? {
            return Ok(file);
        }
        if path.is_file() || prev.is_file() {
            return Err(ClusterError::CorruptMeta(
                "endpoints.json unreadable; restore .prev or re-register nodes",
            ));
        }
        Ok(Self::new())
    }

    /// Persist under the cluster root (atomic durable; keeps previous generation).
    pub fn save(&self, root: &Path) -> Result<(), ClusterError> {
        let path = root.join(ENDPOINTS_FILE);
        let mut out = self.clone();
        out.generation = self.generation.saturating_add(1).max(1);
        out.refresh_checksum();
        let json = serde_json::to_string_pretty(&out)
            .map_err(|_| ClusterError::CorruptMeta("serialize endpoints.json"))?;
        write_atomic_keep_previous(&path, json.as_bytes())?;
        Ok(())
    }
}

fn endpoints_content_hash(endpoints: &HashMap<String, String>) -> String {
    let mut keys: Vec<_> = endpoints.keys().cloned().collect();
    keys.sort();
    let mut h = Hasher::new();
    for k in keys {
        h.update(k.as_bytes());
        h.update(&[0]);
        if let Some(v) = endpoints.get(&k) {
            h.update(v.as_bytes());
        }
        h.update(&[0]);
    }
    hex::encode_blake3(h.finalize().as_bytes())
}

/// Local hex without pulling a hex crate.
mod hex {
    pub fn encode_blake3(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

fn try_parse_endpoints(path: &Path) -> Result<Option<EndpointsFile>, ClusterError> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };
    let file: EndpointsFile = match serde_json::from_slice(&bytes) {
        Ok(f) => f,
        Err(_) => return Ok(None),
    };
    if file.validate().is_err() {
        return Ok(None);
    }
    Ok(Some(file))
}

/// Load endpoints as a dense-index map (missing file → empty).
pub fn load_endpoints(root: &Path) -> Result<HashMap<u32, String>, ClusterError> {
    Ok(EndpointsFile::load(root)?.as_u32_map())
}

/// Save endpoints from a dense-index map.
pub fn save_endpoints(root: &Path, map: &HashMap<u32, String>) -> Result<(), ClusterError> {
    let _guard = process_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _os = endpoints_os_lock(root)?;
    EndpointsFile::from_u32_map(map).save(root)
}

/// Upsert one node address and rewrite `endpoints.json`.
///
/// Holds a process-local mutex and an OS advisory lock so concurrent
/// registrations cannot drop unrelated endpoints (DEF-021 acceptance).
///
/// When the cluster has a registration secret configured
/// ([`crate::ClusterMeta::set_registration_secret`]), prefer
/// [`upsert_endpoint_authenticated`]; this unauthenticated path still works
/// for development clusters without a secret.
pub fn upsert_endpoint(
    root: &Path,
    node_index: u32,
    hostport: impl Into<String>,
) -> Result<HashMap<u32, String>, ClusterError> {
    // Refuse silent unauthenticated upserts when a secret is required (DEF-038).
    let meta = crate::config::ClusterMeta::load(root);
    if let Ok(meta) = meta {
        if meta.registration_token_hash.is_some() {
            return Err(ClusterError::ReplicationRejected(
                "endpoint registration requires authentication (use upsert_endpoint_authenticated)"
                    .into(),
            ));
        }
    }
    upsert_endpoint_unlocked(root, node_index, hostport)
}

/// Authenticated endpoint registration (DEF-038).
///
/// Atomically upserts `node_index → hostport` after verifying `secret` against
/// `cluster.json`'s `registration_token_hash`. When no secret is configured,
/// the call succeeds (same as unauthenticated upsert) so existing clusters keep
/// working.
pub fn upsert_endpoint_authenticated(
    root: &Path,
    node_index: u32,
    hostport: impl Into<String>,
    secret: &str,
) -> Result<HashMap<u32, String>, ClusterError> {
    let meta = crate::config::ClusterMeta::load(root)?;
    meta.check_registration_secret(secret)?;
    upsert_endpoint_unlocked(root, node_index, hostport)
}

fn upsert_endpoint_unlocked(
    root: &Path,
    node_index: u32,
    hostport: impl Into<String>,
) -> Result<HashMap<u32, String>, ClusterError> {
    let _guard = process_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _os = endpoints_os_lock(root)?;
    let mut file = EndpointsFile::load(root)?;
    file.set(node_index, hostport);
    file.save(root)?;
    Ok(file.as_u32_map())
}

struct EndpointsOsLock {
    file: File,
}

impl Drop for EndpointsOsLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            const LOCK_UN: i32 = 8;
            let _ = unsafe { flock(self.file.as_raw_fd(), LOCK_UN) };
        }
    }
}

fn endpoints_os_lock(root: &Path) -> Result<EndpointsOsLock, ClusterError> {
    std::fs::create_dir_all(root)?;
    let path: PathBuf = root.join(ENDPOINTS_LOCK);
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)?;
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        // flock exclusive, blocking — serializes multi-process registration.
        const LOCK_EX: i32 = 2;
        let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX) };
        if rc != 0 {
            return Err(ClusterError::Io(std::io::Error::last_os_error()));
        }
    }
    Ok(EndpointsOsLock { file })
}

#[cfg(unix)]
extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_endpoints_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = HashMap::new();
        map.insert(0, "127.0.0.1:7434".into());
        map.insert(1, "127.0.0.1:7435".into());
        save_endpoints(dir.path(), &map).unwrap();
        let loaded = load_endpoints(dir.path()).unwrap();
        assert_eq!(loaded.get(&0).map(String::as_str), Some("127.0.0.1:7434"));
        assert_eq!(loaded.get(&1).map(String::as_str), Some("127.0.0.1:7435"));
        let after = upsert_endpoint(dir.path(), 2, "127.0.0.1:7436").unwrap();
        assert_eq!(after.len(), 3);
        // Generation + checksum present.
        let raw = std::fs::read_to_string(dir.path().join(ENDPOINTS_FILE)).unwrap();
        assert!(raw.contains("\"generation\""));
        assert!(raw.contains("content_blake3"));
        // Previous generation retained.
        assert!(previous_path(&dir.path().join(ENDPOINTS_FILE)).is_file());
    }

    #[test]
    fn upsert_preserves_unrelated_endpoints() {
        let dir = tempfile::tempdir().unwrap();
        upsert_endpoint(dir.path(), 0, "127.0.0.1:1").unwrap();
        upsert_endpoint(dir.path(), 1, "127.0.0.1:2").unwrap();
        upsert_endpoint(dir.path(), 0, "127.0.0.1:1b").unwrap();
        let m = load_endpoints(dir.path()).unwrap();
        assert_eq!(m.get(&0).map(String::as_str), Some("127.0.0.1:1b"));
        assert_eq!(m.get(&1).map(String::as_str), Some("127.0.0.1:2"));
    }

    #[test]
    fn corrupt_primary_falls_back_to_prev() {
        let dir = tempfile::tempdir().unwrap();
        upsert_endpoint(dir.path(), 0, "127.0.0.1:9").unwrap();
        upsert_endpoint(dir.path(), 1, "127.0.0.1:10").unwrap();
        let path = dir.path().join(ENDPOINTS_FILE);
        std::fs::write(&path, b"{not json").unwrap();
        let m = load_endpoints(dir.path()).unwrap();
        assert!(m.contains_key(&0) || m.contains_key(&1));
    }
}
