//! In-process cluster backend for the collection SDK (Stage 8d).
//!
//! Wraps [`residiuum_cluster::Cluster`] with a [`ClientDirectoryCache`] so the
//! ordinary collection API routes via a client-visible partition directory.
//! Stale routes are refreshed and the operation is retried (CLUSTER_SPEC §13).

use crate::directory_cache::ClientDirectoryCache;
use crate::error::Error;
use crate::filter::{Filter, QueryOptions};
use crate::history::KeyHistory;
use crate::receipt::{DeleteReceipt, PutOptions, WriteReceipt};
use crate::subject::{collection_prefix, decode_subject, encode_subject, validate_key};
use crate::value::{decode_bytes, decode_json, encode_bytes, encode_json};
use residiuum_cluster::{
    Cluster, ClusterConfig, ClusterError, ClusterWriteAck, Coverage, FindResult, PartitionId,
    ReadMode, ScanOptions,
};
use residiuum_store::DurabilityMode;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str;

/// Maximum misroute retries after a directory refresh (bounded staleness).
const MAX_ROUTE_RETRIES: u32 = 3;

/// Cluster find outcome with coverage (Stage 8e / DEF-040, CLUSTER_SPEC §17).
#[derive(Debug, Clone)]
pub struct ClusterFindResult {
    /// Matching application keys and JSON values from completed partitions.
    ///
    /// Ordered by application key ascending (deterministic merge).
    pub rows: Vec<(String, JsonValue)>,
    /// Distributed coverage; inspect before treating empty as absence.
    /// Present on every page.
    pub coverage: Coverage,
    /// Stable query identity for pagination / coordinator replacement.
    pub query_id: String,
    /// Whether the underlying scan truncated by limit/page before filtering.
    pub truncated: bool,
    /// True when a further page is available via [`Self::continuation`].
    pub has_more: bool,
    /// Authenticated continuation for the next distributed page (DEF-040).
    pub continuation: Option<Vec<u8>>,
}

/// SDK handle over an in-process multi-node [`Cluster`].
pub struct ClusterBackend {
    cluster: Cluster,
    cache: ClientDirectoryCache,
    /// Last store_id observed on a write (for Residiuum::store_id when no writes yet).
    last_store_id: [u8; 16],
}

impl ClusterBackend {
    /// Create a new cluster and wrap it.
    pub fn create(cfg: ClusterConfig) -> Result<Self, Error> {
        let cluster = Cluster::create(cfg).map_err(map_cluster)?;
        Ok(Self::from_cluster(cluster))
    }

    /// Open an existing cluster root and wrap it.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, Error> {
        let cluster = Cluster::open(root).map_err(map_cluster)?;
        Ok(Self::from_cluster(cluster))
    }

    fn from_cluster(cluster: Cluster) -> Self {
        let cache = ClientDirectoryCache::from_directory(cluster.directory());
        Self {
            cluster,
            cache,
            last_store_id: [0u8; 16],
        }
    }

    /// Cluster filesystem root.
    pub fn root(&self) -> &Path {
        self.cluster.root()
    }

    /// Underlying cluster (ops / tests).
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// Mutable cluster (failover injection in tests).
    pub fn cluster_mut(&mut self) -> &mut Cluster {
        &mut self.cluster
    }

    /// Client directory cache.
    pub fn cache(&self) -> &ClientDirectoryCache {
        &self.cache
    }

    /// Mutable cache (tests: poison routes, mark stale).
    pub fn cache_mut(&mut self) -> &mut ClientDirectoryCache {
        &mut self.cache
    }

    /// Refresh the entire client cache from the live placement directory.
    pub fn refresh_directory(&mut self) {
        // Ensure leaders are current in the cluster directory first.
        let partitions: Vec<PartitionId> = self.cluster.partition_map().all_partitions().collect();
        for p in partitions {
            let _ = self.cluster.ensure_partition_leader(p);
        }
        self.cache.replace(self.cluster.directory(), HashMap::new());
    }

    /// Sync cache with live directory without forcing elections.
    pub fn sync_cache_from_cluster(&mut self) {
        self.cache.replace(self.cluster.directory(), HashMap::new());
    }

    /// Cluster id bytes.
    pub fn store_id(&self) -> [u8; 16] {
        if self.last_store_id != [0u8; 16] {
            return self.last_store_id;
        }
        self.cluster.cluster_id().0
    }

    /// Path buffer for diagnostics.
    pub fn path_buf(&self) -> PathBuf {
        self.cluster.root().to_path_buf()
    }

    /// Online node count.
    pub fn live_count_approx(&mut self) -> Result<usize, Error> {
        let scan = self.cluster.scan_all().map_err(map_cluster)?;
        Ok(scan.entries.len())
    }

    /// List collection names derived from live subjects.
    pub fn list_collections(&mut self) -> Result<Vec<String>, Error> {
        let scan = self.cluster.scan_all().map_err(map_cluster)?;
        let mut names = std::collections::BTreeSet::new();
        for (subject, _) in scan.entries {
            if let Some((coll, _)) = decode_subject(subject.as_bytes()) {
                names.insert(coll.to_string());
            }
        }
        Ok(names.into_iter().collect())
    }

    /// Put JSON under collection/key with client-side routing + stale retry.
    pub fn put_json(
        &mut self,
        collection: &str,
        key: &str,
        value: &JsonValue,
        options: PutOptions,
    ) -> Result<WriteReceipt, Error> {
        let limits = crate::resource::host_limits();
        crate::resource::check_json_depth(value, &limits)?;
        let subject = subject_str(collection, key)?;
        let body = encode_json(value)?;
        crate::resource::check_payload_len(body.len(), &limits)?;
        let ack = self.write_routed(&subject, &body, options.durability, false)?;
        self.last_store_id = ack.store_id;
        Ok(write_receipt_from_ack(key, &ack, options.durability))
    }

    /// Put raw bytes.
    pub fn put_bytes(
        &mut self,
        collection: &str,
        key: &str,
        bytes: &[u8],
        options: PutOptions,
    ) -> Result<WriteReceipt, Error> {
        let limits = crate::resource::host_limits();
        crate::resource::check_payload_len(bytes.len().saturating_add(1), &limits)?;
        let subject = subject_str(collection, key)?;
        let body = encode_bytes(bytes);
        let ack = self.write_routed(&subject, &body, options.durability, false)?;
        self.last_store_id = ack.store_id;
        Ok(write_receipt_from_ack(key, &ack, options.durability))
    }

    /// Get JSON.
    pub fn get_json(&mut self, collection: &str, key: &str) -> Result<Option<JsonValue>, Error> {
        let subject = subject_str(collection, key)?;
        match self.get_routed(&subject)? {
            None => Ok(None),
            Some(body) => Ok(Some(decode_json(&body)?)),
        }
    }

    /// Get bytes.
    pub fn get_bytes(&mut self, collection: &str, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let subject = subject_str(collection, key)?;
        match self.get_routed(&subject)? {
            None => Ok(None),
            Some(body) => Ok(Some(decode_bytes(&body)?)),
        }
    }

    /// Delete key.
    pub fn delete(
        &mut self,
        collection: &str,
        key: &str,
        options: PutOptions,
    ) -> Result<DeleteReceipt, Error> {
        let subject = subject_str(collection, key)?;
        let existed = self.get_routed(&subject)?.is_some();
        let ack = self.write_routed(&subject, &[], options.durability, true)?;
        self.last_store_id = ack.store_id;
        Ok(DeleteReceipt {
            key: key.to_string(),
            removed: existed,
            event_id: ack.event_id,
            version: ack.event_id,
            acknowledgement: options.durability,
            committed: ack.committed,
            store_id: ack.store_id,
            segment_id: ack.segment_id,
        })
    }

    /// History from the current partition leader's store.
    pub fn history(&mut self, collection: &str, key: &str) -> Result<KeyHistory, Error> {
        let subject = subject_str(collection, key)?;
        self.ensure_route_fresh(subject.as_bytes())?;
        let hist = self
            .cluster
            .subject_history(&subject)
            .map_err(map_cluster)?;
        let partition = self.cluster.partition_for_subject(&subject);
        self.sync_cache_entry(partition);
        KeyHistory::from_store(key.to_string(), hist)
    }

    /// Scan application keys in a collection.
    pub fn scan_keys(&mut self, collection: &str) -> Result<Vec<String>, Error> {
        let prefix = collection_prefix(collection)?;
        let scan = self.cluster.scan_all().map_err(map_cluster)?;
        self.sync_cache_from_cluster();
        let mut keys = Vec::new();
        for (subject, _) in scan.entries {
            let bytes = subject.as_bytes();
            if !bytes.starts_with(&prefix) {
                continue;
            }
            if let Some((coll, key)) = decode_subject(bytes) {
                if coll == collection {
                    keys.push(key.to_string());
                }
            }
        }
        keys.sort();
        Ok(keys)
    }

    /// Scan / find JSON documents with full coverage honesty (Stage 8e).
    ///
    /// Partial results remain valid data. When coverage is incomplete and
    /// [`QueryOptions::allow_partial_coverage`] is false, returns
    /// [`Error::CoverageIncomplete`] so an empty match list is never mistaken
    /// for a complete empty collection (CLUSTER_SPEC §17.2).
    pub fn find(
        &mut self,
        collection: &str,
        filter: &Filter,
        options: &QueryOptions,
    ) -> Result<Vec<(String, JsonValue)>, Error> {
        let covered = self.find_with_coverage(collection, filter, options)?;
        Ok(covered.rows)
    }

    /// Find with explicit coverage record (Stage 8e).
    pub fn find_with_coverage(
        &mut self,
        collection: &str,
        filter: &Filter,
        options: &QueryOptions,
    ) -> Result<ClusterFindResult, Error> {
        let prefix_bytes = collection_prefix(collection)?;
        let prefix = str::from_utf8(&prefix_bytes)
            .map_err(|_| Error::Internal("collection prefix is not UTF-8".into()))?
            .to_string();

        let mut scan_opts = ScanOptions::new().prefix(prefix);
        if let Some(budget) = &options.budget {
            if let Some(max) = budget.max_docs_scanned {
                scan_opts = scan_opts.max_docs_scanned(max);
            }
        }
        // Fetch more than limit so filter can still fill the limit; final
        // truncation is done by finish_query.
        if let Some(lim) = options.limit {
            scan_opts = scan_opts.limit(lim.saturating_mul(8).max(lim));
        }

        let find: FindResult = self.cluster.find(scan_opts).map_err(map_cluster)?;
        self.sync_cache_from_cluster();

        if find.coverage.is_incomplete() && !options.allow_partial_coverage {
            if find.coverage.resource_limit_reached {
                return Err(Error::QueryBudgetRequired(format!(
                    "cluster scan hit resource budget before full coverage \
                     (query_id={}); raise budget or set allow_partial_coverage",
                    find.query_id
                )));
            }
            return Err(Error::CoverageIncomplete(format!(
                "cluster find coverage incomplete: {} unavailable partition(s), \
                 completed={}/{}; set allow_partial_coverage to accept partial \
                 matches (query_id={})",
                find.coverage.unavailable.len(),
                find.coverage.completed.len(),
                find.coverage.requested.len(),
                find.query_id
            )));
        }

        let mut out = Vec::new();
        for (subject, body) in find.entries {
            let bytes = subject.as_bytes();
            let Some((coll, key)) = decode_subject(bytes) else {
                continue;
            };
            if coll != collection {
                continue;
            }
            let value = match decode_json(&body) {
                Ok(v) => v,
                Err(Error::TypeMismatch { .. }) => continue,
                Err(e) => return Err(e),
            };
            if !filter.matches(&value) {
                continue;
            }
            out.push((key.to_string(), value));
        }
        let rows = crate::collection::finish_query_pub(out, options)?;
        Ok(ClusterFindResult {
            rows,
            coverage: find.coverage,
            query_id: find.query_id,
            truncated: find.truncated,
            has_more: find.has_more,
            continuation: find.continuation,
        })
    }

    /// Full multi-partition scan with coverage (Stage 8e / DEF-040).
    pub fn scan_with_coverage(&mut self, options: ScanOptions) -> Result<FindResult, Error> {
        let find = self.cluster.find(options).map_err(map_cluster)?;
        self.sync_cache_from_cluster();
        Ok(find)
    }

    /// One page of a distributed scan with coverage and continuation (DEF-040).
    pub fn scan_page(&mut self, options: ScanOptions) -> Result<FindResult, Error> {
        let find = self.cluster.scan_page(options).map_err(map_cluster)?;
        self.sync_cache_from_cluster();
        Ok(find)
    }

    // --- routing core -------------------------------------------------------

    fn write_routed(
        &mut self,
        subject: &str,
        body: &[u8],
        mode: DurabilityMode,
        is_delete: bool,
    ) -> Result<ClusterWriteAck, Error> {
        let mut last_err: Option<Error> = None;
        for attempt in 0..MAX_ROUTE_RETRIES {
            if let Err(e) = self.ensure_route_fresh(subject.as_bytes()) {
                last_err = Some(e);
                self.refresh_directory();
                continue;
            }
            // Client-visible route decision (hot path uses the cache).
            let route = self
                .cache
                .route(subject.as_bytes())
                .cloned()
                .ok_or_else(|| Error::Internal("no cached route after refresh".into()))?;

            // Compare with live directory: stale placement → refresh + retry.
            let live = self
                .cluster
                .directory()
                .get(cluster_partition(route.partition))
                .cloned();
            if let Some(ref live_a) = live {
                if live_a.leader.index() != route.leader.index()
                    || live_a.placement_epoch.0 != route.placement_epoch.0
                    || live_a.term.0 != route.term.0
                {
                    self.cache.mark_stale(route.partition);
                    self.cache.refresh_entry(live_a);
                    last_err = Some(Error::StaleRoute {
                        partition: route.partition.get(),
                        message: format!(
                            "cached leader {:?} epoch {} != live leader {:?} epoch {} (attempt {attempt})",
                            route.leader,
                            route.placement_epoch.0,
                            live_a.leader,
                            live_a.placement_epoch.0
                        ),
                    });
                    continue;
                }
            }

            let result = if is_delete {
                self.cluster.delete(subject, mode)
            } else {
                self.cluster.put(subject, body, mode)
            };

            match result {
                Ok(ack) => {
                    // Refresh entry from post-write directory (term may advance).
                    if let Some(a) = self.cluster.directory().get(ack.partition).cloned() {
                        self.cache.refresh_entry(&a);
                    }
                    return Ok(ack);
                }
                Err(ClusterError::NoLeader(_)) | Err(ClusterError::PartitionUnavailable { .. }) => {
                    self.cache.mark_stale(route.partition);
                    self.refresh_directory();
                    last_err = Some(map_cluster(result.err().unwrap()));
                }
                Err(e) => return Err(map_cluster(e)),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            Error::Internal("route retries exhausted without a specific error".into())
        }))
    }

    fn get_routed(&mut self, subject: &str) -> Result<Option<Vec<u8>>, Error> {
        let mut last_err: Option<Error> = None;
        for attempt in 0..MAX_ROUTE_RETRIES {
            if let Err(e) = self.ensure_route_fresh(subject.as_bytes()) {
                last_err = Some(e);
                self.refresh_directory();
                continue;
            }
            let route = match self.cache.route(subject.as_bytes()).cloned() {
                Some(r) => r,
                None => {
                    self.refresh_directory();
                    continue;
                }
            };

            let live = self
                .cluster
                .directory()
                .get(cluster_partition(route.partition))
                .cloned();
            if let Some(ref live_a) = live {
                if live_a.leader.index() != route.leader.index()
                    || live_a.placement_epoch.0 != route.placement_epoch.0
                {
                    self.cache.refresh_entry(live_a);
                    last_err = Some(Error::StaleRoute {
                        partition: route.partition.get(),
                        message: format!("stale get route attempt {attempt}"),
                    });
                    continue;
                }
            }

            match self.cluster.get(subject, ReadMode::Linearizable) {
                Ok(got) => {
                    if let Some(a) = self
                        .cluster
                        .directory()
                        .get(cluster_partition(route.partition))
                        .cloned()
                    {
                        self.cache.refresh_entry(&a);
                    }
                    return Ok(got.value);
                }
                Err(ClusterError::ConsistencyViolation(_)) => {
                    // Convergent mode: fall back to available read.
                    let got = self
                        .cluster
                        .get(subject, ReadMode::Available)
                        .map_err(map_cluster)?;
                    return Ok(got.value);
                }
                Err(ClusterError::NoLeader(_)) | Err(ClusterError::PartitionUnavailable { .. }) => {
                    self.cache.mark_stale(route.partition);
                    self.refresh_directory();
                    last_err = Some(Error::PartitionUnavailable {
                        partition: route.partition.get(),
                        reason: "leader unavailable; refreshed directory",
                    });
                }
                Err(e) => return Err(map_cluster(e)),
            }
        }
        Err(last_err.unwrap_or_else(|| Error::Internal("get route retries exhausted".into())))
    }

    fn ensure_route_fresh(&mut self, partition_key: &[u8]) -> Result<(), Error> {
        if self.cache.route_checked(partition_key).is_some() {
            return Ok(());
        }
        // Stale or missing: refresh whole directory (bounded).
        self.refresh_directory();
        if self.cache.route_checked(partition_key).is_some() {
            Ok(())
        } else {
            let p = self.cache.partition_of(partition_key);
            Err(Error::PartitionUnavailable {
                partition: p.get(),
                reason: "no route after directory refresh",
            })
        }
    }

    fn sync_cache_entry(&mut self, partition: PartitionId) {
        if let Some(a) = self.cluster.directory().get(partition).cloned() {
            self.cache.refresh_entry(&a);
        }
    }
}

fn subject_str(collection: &str, key: &str) -> Result<String, Error> {
    validate_key(key)?;
    let bytes = encode_subject(collection, key)?;
    str::from_utf8(&bytes)
        .map(|s| s.to_string())
        .map_err(|_| Error::Internal("subject is not UTF-8".into()))
}

fn cluster_partition(p: crate::directory_cache::PartitionId) -> PartitionId {
    PartitionId::new(p.get())
}

fn write_receipt_from_ack(
    key: &str,
    ack: &ClusterWriteAck,
    requested: DurabilityMode,
) -> WriteReceipt {
    WriteReceipt {
        key: key.to_string(),
        event_id: ack.event_id,
        version: ack.event_id,
        acknowledgement: requested,
        committed: ack.committed,
        store_id: ack.store_id,
        segment_id: ack.segment_id,
    }
}

pub(crate) fn map_cluster(e: ClusterError) -> Error {
    match e {
        ClusterError::Store(s) => Error::Store(s),
        ClusterError::PartitionUnavailable { partition, reason } => {
            Error::PartitionUnavailable { partition, reason }
        }
        ClusterError::CoverageIncomplete(m) => Error::CoverageIncomplete(m),
        ClusterError::DurabilityUnavailable(m) => Error::DurabilityUnavailable(m),
        ClusterError::NoLeader(p) => Error::PartitionUnavailable {
            partition: p,
            reason: "no leader",
        },
        ClusterError::ConsistencyViolation(m) => Error::ConsistencyViolation(m),
        ClusterError::Rebalance(m) => Error::Internal(format!("rebalance: {m}")),
        ClusterError::ContinuationInvalid(m) => {
            Error::ValidationMsg(format!("distributed query continuation invalid: {m}"))
        }
        ClusterError::AlreadyExists(p) => {
            Error::ValidationMsg(format!("cluster already exists: {p}"))
        }
        ClusterError::NotACluster(p) => Error::ValidationMsg(format!("not a cluster: {p}")),
        ClusterError::CorruptMeta(m) => Error::Internal(format!("corrupt cluster meta: {m}")),
        ClusterError::ReplicationRejected(m) => {
            Error::Internal(format!("replication rejected: {m}"))
        }
        ClusterError::Io(e) => Error::from_io(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory_cache::NodeId;
    use residiuum_cluster::ClusterConfig;
    use serde_json::json;

    #[test]
    fn open_cluster_put_get_via_cache() {
        let dir = tempfile::tempdir().unwrap();
        let mut backend = ClusterBackend::create(
            ClusterConfig::dependable_local(dir.path().join("c")).with_virtual_partitions(8),
        )
        .unwrap();
        assert!(backend.cache().refresh_count() >= 1);
        backend
            .put_json("users", "alice", &json!({"n": 1}), PutOptions::default())
            .unwrap();
        let v = backend.get_json("users", "alice").unwrap().unwrap();
        assert_eq!(v["n"], 1);
    }

    #[test]
    fn poisoned_route_refreshes_and_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let mut backend = ClusterBackend::create(
            ClusterConfig::dependable_local(dir.path().join("c")).with_virtual_partitions(8),
        )
        .unwrap();
        // Warm directory / leaders.
        backend
            .put_json("t", "k", &json!(1), PutOptions::default())
            .unwrap();
        let subject = subject_str("t", "k2").unwrap();
        let p = backend.cache().partition_of(subject.as_bytes());
        let before = backend.cache().entry_refresh_count();
        // Poison with a leader that is not the live one.
        backend.cache_mut().poison_leader(p, NodeId::new(99));
        backend
            .put_json("t", "k2", &json!(2), PutOptions::default())
            .unwrap();
        assert!(backend.cache().entry_refresh_count() > before);
        assert_eq!(backend.get_json("t", "k2").unwrap().unwrap(), json!(2));
    }
}
