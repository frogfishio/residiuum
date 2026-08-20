//! Heap-bound SDK surface (`HEAP_SPEC` §30.7–§30.9 / HP-007).
//!
//! Handles carry an unforgeable [`HeapCap`]. Two heaps with the same collection
//! *name* still cannot exchange typed handles, cursors, batch members, or pool
//! checkouts — composition requires pointer-identical capabilities.

use crate::error::Error;
use crate::history::KeyHistory;
use crate::indexes::IndexInfo;
use crate::receipt::{DeleteReceipt, PutOptions, WriteReceipt};
use crate::subject::{validate_collection_name, validate_key};
use blake3::Hasher;
use residiuum_heap::{CollectionId, HeapCap, HeapId, StreamId};
use residiuum_store::{
    rebuild_heap_entry_from_chain, rebuild_object_entry_from_chain, try_load_collections_catalog,
    try_load_streams_catalog, HeapMetaLayout, HeapStore, ObjectKind, StoreError, StoreHost,
};
use serde::Serialize;
use std::collections::VecDeque;
use std::hash::{BuildHasher, Hash, Hasher as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_DECODED_JSON_CACHE_CHARGE: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DecodedJsonKey {
    heap_id: [u8; 16],
    collection_id: [u8; 16],
    key: String,
}

#[derive(Debug)]
struct CachedDecodedJson {
    version: [u8; 16],
    value: Arc<serde_json::Value>,
    logical_bytes: u64,
    charge: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProjectedJsonKey {
    document: DecodedJsonKey,
    fields_hash: [u8; 32],
}

#[derive(Debug)]
struct CachedProjectedJson {
    version: [u8; 16],
    fields: Arc<[String]>,
    values: Arc<[Option<serde_json::Value>]>,
    logical_bytes: u64,
    charge: usize,
}

#[derive(Debug)]
struct DecodedJsonCache {
    entries: hashbrown::HashMap<DecodedJsonKey, CachedDecodedJson>,
    admission_order: VecDeque<(DecodedJsonKey, [u8; 16])>,
    resident_charge: usize,
    max_charge: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    projected_entries: hashbrown::HashMap<ProjectedJsonKey, CachedProjectedJson>,
    projected_admission_order: VecDeque<(ProjectedJsonKey, [u8; 16])>,
    projected_resident_charge: usize,
    projected_max_charge: usize,
}

/// Constant-time deployment-wide decoded JSON cache counters.
///
/// Hits and misses count version-qualified lookup attempts, not queries. The
/// counters start at zero for every physical deployment open, making reopen
/// and first-query cache behavior directly observable without a store scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecodedJsonCacheStats {
    /// Version-qualified cache lookups satisfied from decoded memory.
    pub hits: u64,
    /// Version-qualified cache lookups that required another path.
    pub misses: u64,
    /// Currently resident decoded documents.
    pub entries: usize,
    /// Current conservative memory charge in bytes.
    pub resident_charge: usize,
    /// Hard conservative memory-charge bound in bytes.
    pub max_charge: usize,
}

impl DecodedJsonCache {
    fn new(max_charge: usize) -> Self {
        Self {
            entries: hashbrown::HashMap::new(),
            admission_order: VecDeque::new(),
            resident_charge: 0,
            max_charge,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            projected_entries: hashbrown::HashMap::new(),
            projected_admission_order: VecDeque::new(),
            projected_resident_charge: 0,
            projected_max_charge: max_charge.saturating_div(4).max(1),
        }
    }

    fn observe<T>(&self, value: Option<T>) -> Option<T> {
        if value.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        value
    }

    fn stats(&self) -> DecodedJsonCacheStats {
        DecodedJsonCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: self.entries.len(),
            resident_charge: self.resident_charge,
            max_charge: self.max_charge,
        }
    }

    fn get(&self, key: &DecodedJsonKey, version: &[u8; 16]) -> Option<Arc<serde_json::Value>> {
        self.observe(
            self.entries
                .get(key)
                .filter(|entry| entry.version == *version)
                .map(|entry| Arc::clone(&entry.value)),
        )
    }

    fn get_with_len(
        &self,
        key: &DecodedJsonKey,
        version: &[u8; 16],
    ) -> Option<(Arc<serde_json::Value>, u64)> {
        self.observe(
            self.entries
                .get(key)
                .filter(|entry| entry.version == *version)
                .map(|entry| (Arc::clone(&entry.value), entry.logical_bytes)),
        )
    }

    fn get_raw_ref_with_len(
        &self,
        heap_id: &[u8; 16],
        collection_id: &[u8; 16],
        key: &str,
        version: &[u8; 16],
    ) -> Option<(&serde_json::Value, u64)> {
        let mut hasher = self.entries.hasher().build_hasher();
        heap_id.hash(&mut hasher);
        collection_id.hash(&mut hasher);
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let result = self.entries.raw_entry().from_hash(hash, |candidate| {
            &candidate.heap_id == heap_id
                && &candidate.collection_id == collection_id
                && candidate.key == key
        });
        let value = result
            .map(|(_, entry)| entry)
            .filter(|entry| entry.version == *version)
            .map(|entry| (entry.value.as_ref(), entry.logical_bytes));
        self.observe(value)
    }

    fn get_projected(
        &self,
        key: &DecodedJsonKey,
        version: &[u8; 16],
        fields: &[String],
    ) -> Option<Vec<serde_json::Value>> {
        self.observe(
            self.entries
                .get(key)
                .filter(|entry| entry.version == *version)
                .map(|entry| project_json_fields(&entry.value, fields)),
        )
    }

    fn insert(
        &mut self,
        key: DecodedJsonKey,
        version: [u8; 16],
        value: Arc<serde_json::Value>,
        encoded_len: usize,
    ) {
        if self
            .entries
            .get(&key)
            .is_some_and(|entry| entry.version == version)
        {
            return;
        }
        if let Some(previous) = self.entries.remove(&key) {
            self.resident_charge = self.resident_charge.saturating_sub(previous.charge);
        }
        // serde_json's tree has allocator overhead beyond encoded bytes. A
        // conservative 3x logical charge plus key/entry overhead keeps this
        // cache bounded without coupling the SDK to serde_json internals.
        let charge = encoded_len
            .saturating_mul(3)
            .saturating_add(key.key.len())
            .saturating_add(128);
        if charge > self.max_charge {
            return;
        }
        self.resident_charge = self.resident_charge.saturating_add(charge);
        self.entries.insert(
            key.clone(),
            CachedDecodedJson {
                version,
                value,
                logical_bytes: encoded_len.saturating_sub(1) as u64,
                charge,
            },
        );
        self.admission_order.push_back((key, version));
        if self.admission_order.len() > self.entries.len().saturating_mul(2).saturating_add(1_024) {
            self.admission_order.retain(|(key, version)| {
                self.entries
                    .get(key)
                    .is_some_and(|entry| entry.version == *version)
            });
        }
        while self.resident_charge > self.max_charge {
            let Some((key, version)) = self.admission_order.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&key)
                .is_some_and(|entry| entry.version == version)
            {
                if let Some(removed) = self.entries.remove(&key) {
                    self.resident_charge = self.resident_charge.saturating_sub(removed.charge);
                }
            }
        }
    }

    fn get_projection_raw(
        &self,
        heap_id: &[u8; 16],
        collection_id: &[u8; 16],
        key: &str,
        fields_hash: &[u8; 32],
        version: &[u8; 16],
        fields: &[String],
    ) -> Option<(Arc<[Option<serde_json::Value>]>, u64)> {
        let mut hasher = self.projected_entries.hasher().build_hasher();
        heap_id.hash(&mut hasher);
        collection_id.hash(&mut hasher);
        key.hash(&mut hasher);
        fields_hash.hash(&mut hasher);
        let hash = hasher.finish();
        self.projected_entries
            .raw_entry()
            .from_hash(hash, |candidate| {
                &candidate.document.heap_id == heap_id
                    && &candidate.document.collection_id == collection_id
                    && candidate.document.key == key
                    && &candidate.fields_hash == fields_hash
            })
            .map(|(_, entry)| entry)
            .filter(|entry| entry.version == *version && entry.fields.as_ref() == fields)
            .map(|entry| (Arc::clone(&entry.values), entry.logical_bytes))
    }

    fn insert_projection(
        &mut self,
        key: ProjectedJsonKey,
        version: [u8; 16],
        fields: Arc<[String]>,
        values: Arc<[Option<serde_json::Value>]>,
        logical_bytes: u64,
    ) {
        if self
            .projected_entries
            .get(&key)
            .is_some_and(|entry| entry.version == version && entry.fields == fields)
        {
            return;
        }
        if let Some(previous) = self.projected_entries.remove(&key) {
            self.projected_resident_charge = self
                .projected_resident_charge
                .saturating_sub(previous.charge);
        }
        let value_charge = values
            .iter()
            .filter_map(Option::as_ref)
            .map(crate::resource::estimate_json_bytes)
            .sum::<u64>() as usize;
        let charge = value_charge
            .saturating_add(key.document.key.len())
            .saturating_add(160);
        if charge > self.projected_max_charge {
            return;
        }
        self.projected_resident_charge = self.projected_resident_charge.saturating_add(charge);
        self.projected_entries.insert(
            key.clone(),
            CachedProjectedJson {
                version,
                fields,
                values,
                logical_bytes,
                charge,
            },
        );
        self.projected_admission_order.push_back((key, version));
        if self.projected_admission_order.len()
            > self
                .projected_entries
                .len()
                .saturating_mul(2)
                .saturating_add(1_024)
        {
            self.projected_admission_order.retain(|(key, version)| {
                self.projected_entries
                    .get(key)
                    .is_some_and(|entry| entry.version == *version)
            });
        }
        while self.projected_resident_charge > self.projected_max_charge {
            let Some((key, version)) = self.projected_admission_order.pop_front() else {
                break;
            };
            if self
                .projected_entries
                .get(&key)
                .is_some_and(|entry| entry.version == version)
            {
                if let Some(removed) = self.projected_entries.remove(&key) {
                    self.projected_resident_charge = self
                        .projected_resident_charge
                        .saturating_sub(removed.charge);
                }
            }
        }
    }
}

fn projection_fields_hash(fields: &[String]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for field in fields {
        hasher.update(&(field.len() as u64).to_le_bytes());
        hasher.update(field.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn project_json_fields(value: &serde_json::Value, fields: &[String]) -> Vec<serde_json::Value> {
    fields
        .iter()
        .map(|field| {
            let mut current = value;
            for segment in field.split('.') {
                let Some(next) = current.as_object().and_then(|map| map.get(segment)) else {
                    return serde_json::Value::Null;
                };
                current = next;
            }
            current.clone()
        })
        .collect()
}

/// Deployment-level host wrapping a capability-gated [`StoreHost`].
pub struct ResidiuumDeployment {
    host: StoreHost,
    data_root: PathBuf,
    decoded_json_cache: Arc<Mutex<DecodedJsonCache>>,
}

impl ResidiuumDeployment {
    /// Open an existing store directory as a deployment host (no raw data API).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            host: StoreHost::open(path)?,
            data_root: path.to_path_buf(),
            decoded_json_cache: Arc::new(Mutex::new(DecodedJsonCache::new(
                DEFAULT_DECODED_JSON_CACHE_CHARGE,
            ))),
        })
    }

    /// Create a new store directory as a deployment host.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            host: StoreHost::create(path)?,
            data_root: path.to_path_buf(),
            decoded_json_cache: Arc::new(Mutex::new(DecodedJsonCache::new(
                DEFAULT_DECODED_JSON_CACHE_CHARGE,
            ))),
        })
    }

    /// Bind a validated capability into a heap handle.
    pub fn open_heap(&self, cap: HeapCap) -> Heap {
        let store = self.host.open_heap(cap.clone());
        Heap {
            cap,
            store: Arc::new(store),
            layout: HeapMetaLayout::new(&self.data_root),
            pool: Arc::new(Mutex::new(HeapPoolInner::default())),
            decoded_json_cache: Arc::clone(&self.decoded_json_cache),
        }
    }

    /// Bind a capability only when it names the requested published Heap.
    ///
    /// Name resolution is verified from the authoritative descriptor chain,
    /// not trusted from a rebuildable catalogue. This is an ergonomic guard;
    /// the unforgeable capability remains the authorization boundary.
    pub fn open_named_heap(&self, name: &str, cap: HeapCap) -> Result<Heap, Error> {
        let heap_id = *cap.heap_id().as_bytes();
        let entry = rebuild_heap_entry_from_chain(&HeapMetaLayout::new(&self.data_root), &heap_id)?
            .ok_or_else(|| Error::ValidationMsg("capability heap is not published".into()))?;
        if entry.name != name && !entry.aliases.iter().any(|alias| alias == name) {
            return Err(Error::ValidationMsg(format!(
                "heap name {name:?} does not match the supplied capability"
            )));
        }
        Ok(self.open_heap(cap))
    }

    /// Data root (catalog / meta layout).
    pub fn root(&self) -> &Path {
        &self.data_root
    }

    /// Physical-store open timings and bounded inventory I/O counters.
    pub fn open_metrics(&self) -> Result<residiuum_store::StoreOpenMetrics, Error> {
        Ok(self.host.open_metrics()?)
    }

    /// Structured startup disposition, recovery actions, counts, and timings.
    pub fn open_report(&self) -> Result<residiuum_store::StoreOpenReport, Error> {
        Ok(self.host.open_report()?)
    }

    /// Constant-time decoded JSON cache counters for this deployment open.
    pub fn decoded_json_cache_stats(&self) -> Option<DecodedJsonCacheStats> {
        self.decoded_json_cache
            .lock()
            .ok()
            .map(|cache| cache.stats())
    }

    /// Durable group-commit counters from the shared physical deployment.
    pub fn operation_commit_stats(&self) -> residiuum_store::OperationCommitStats {
        self.host.operation_commit_stats()
    }

    /// Physical Atomic execution and open-recovery counters.
    pub fn atomic_store_stats(&self) -> Result<residiuum_store::AtomicStoreStats, Error> {
        Ok(self.host.atomic_store_stats()?)
    }

    /// Redacted sustained-write lifecycle counters; performs no store scan.
    pub fn write_path_stats(&self) -> Result<residiuum_store::StoreWritePathStats, Error> {
        Ok(self.host.write_path_stats()?)
    }

    /// Enable or disable rebuildable Hydra/Chimera enrichment for this open.
    pub(crate) fn set_enrichment_enabled(&self, enabled: bool) -> Result<(), Error> {
        Ok(self.host.set_enrichment_enabled(enabled)?)
    }

    /// Establish the deployment's durable clean-restart boundary.
    pub(crate) fn prepare_orderly_close(&self) -> Result<(), Error> {
        Ok(self.host.prepare_orderly_close()?)
    }
}

/// Heap handle bound to exactly one [`HeapCap`].
pub struct Heap {
    cap: HeapCap,
    store: Arc<HeapStore>,
    layout: HeapMetaLayout,
    pool: Arc<Mutex<HeapPoolInner>>,
    decoded_json_cache: Arc<Mutex<DecodedJsonCache>>,
}

impl Heap {
    /// Bound heap id.
    pub fn id(&self) -> HeapId {
        self.cap.heap_id()
    }

    /// Capability instance (for composition checks).
    pub fn capability(&self) -> &HeapCap {
        &self.cap
    }

    /// Store segment fingerprint for this heap's physical store (APB-6 pin).
    ///
    /// Identity is over segment paths + sizes; it is a frontier token, not a
    /// multi-query snapshot proof. Requires Read on the capability.
    pub fn segment_fingerprint(&self) -> Result<[u8; 32], Error> {
        Ok(self.store.segment_fingerprint()?)
    }

    /// Shared store handle for read-view pin re-checks (crate-internal).
    pub(crate) fn store_arc(&self) -> Arc<HeapStore> {
        Arc::clone(&self.store)
    }

    /// Qualification-only ATM-3 bridge used to prove the real SDK/RQL reader
    /// path before the public transaction builder is introduced.
    #[doc(hidden)]
    pub fn decide_atomic_plan_evidence(
        &self,
        plan: &residiuum_atomics::AtomicPlan,
    ) -> Result<residiuum_atomics::AtomicDecision, Error> {
        Ok(self.store.decide_atomic_plan_evidence(plan)?)
    }

    /// Qualification-only ATM-3 outcome carrying exact per-member versions.
    #[doc(hidden)]
    pub fn decide_atomic_plan_outcome(
        &self,
        plan: &residiuum_atomics::AtomicPlan,
    ) -> Result<residiuum_atomics::AtomicOutcome, Error> {
        Ok(self.store.decide_atomic_plan_outcome(plan)?)
    }

    /// Resolve one Heap-local Atomic identity from durable evidence.
    pub fn atomic_status(
        &self,
        atomic_id: residiuum_atomics::AtomicId,
    ) -> Result<residiuum_atomics::AtomicStatus, Error> {
        Ok(self.store.atomic_status(atomic_id)?)
    }

    /// Qualification-only ATM-3D independent-outcome durability cohort.
    #[doc(hidden)]
    pub fn decide_atomic_plan_cohort_outcomes(
        &self,
        plans: &[residiuum_atomics::AtomicPlan],
    ) -> Result<Vec<residiuum_atomics::AtomicCohortOutcome>, Error> {
        Ok(self.store.decide_atomic_plan_cohort_outcomes(plans)?)
    }

    /// Whether `other` is the same capability instance (pointer identity).
    pub fn same_capability(&self, other: &Heap) -> bool {
        self.cap.same_instance(&other.cap)
    }

    /// Reject composition with a foreign heap handle.
    pub fn require_same_instance(&self, other: &Heap) -> Result<(), Error> {
        if self.cap.same_instance(&other.cap) {
            Ok(())
        } else {
            Err(Error::PermissionDenied(
                "heap capability instance mismatch".into(),
            ))
        }
    }

    /// Open a collection by canonical name (catalog tip).
    pub fn collection(&self, name: &str) -> Result<HeapCollection, Error> {
        let heap_id = *self.cap.heap_id().as_bytes();
        let (id, tip_name) = lookup_named(&self.layout, &heap_id, ObjectKind::Collection, name)?;
        let id = CollectionId::from_bytes_unchecked_nonzero(id)
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(HeapCollection {
            cap: self.cap.clone(),
            store: Arc::clone(&self.store),
            id,
            name_at_open: tip_name,
            decoded_json_cache: Arc::clone(&self.decoded_json_cache),
        })
    }

    /// Open a collection by immutable id.
    pub fn collection_by_id(&self, id: CollectionId) -> Result<HeapCollection, Error> {
        let heap_id = *self.cap.heap_id().as_bytes();
        let entry = rebuild_object_entry_from_chain(
            &self.layout,
            &heap_id,
            ObjectKind::Collection,
            id.as_bytes(),
        )?
        .ok_or_else(|| Error::ValidationMsg("collection id unknown".into()))?;
        Ok(HeapCollection {
            cap: self.cap.clone(),
            store: Arc::clone(&self.store),
            id,
            name_at_open: entry.name,
            decoded_json_cache: Arc::clone(&self.decoded_json_cache),
        })
    }

    /// Open a stream by canonical name.
    pub fn stream(&self, name: &str) -> Result<HeapStream, Error> {
        let heap_id = *self.cap.heap_id().as_bytes();
        let (id, tip_name) = lookup_named(&self.layout, &heap_id, ObjectKind::Stream, name)?;
        let id = StreamId::from_bytes_unchecked_nonzero(id)
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(HeapStream {
            cap: self.cap.clone(),
            id,
            name_at_open: tip_name,
        })
    }

    /// Open a stream by immutable id.
    pub fn stream_by_id(&self, id: StreamId) -> Result<HeapStream, Error> {
        let heap_id = *self.cap.heap_id().as_bytes();
        let entry = rebuild_object_entry_from_chain(
            &self.layout,
            &heap_id,
            ObjectKind::Stream,
            id.as_bytes(),
        )?
        .ok_or_else(|| Error::ValidationMsg("stream id unknown".into()))?;
        Ok(HeapStream {
            cap: self.cap.clone(),
            id,
            name_at_open: entry.name,
        })
    }

    /// Checkout a pooled connection tagged with this heap's capability instance.
    pub fn checkout_connection(&self) -> Result<HeapConnection, Error> {
        let mut guard = self
            .pool
            .lock()
            .map_err(|_| Error::Internal("heap pool poisoned".into()))?;
        let token = guard
            .idle
            .pop_front()
            .unwrap_or_else(|| ConnectionToken::new(&self.cap));
        Ok(HeapConnection {
            cap: self.cap.clone(),
            token,
            pool: Arc::clone(&self.pool),
        })
    }

    /// Start a batch that can only accept members from this heap instance.
    pub fn begin_batch(&self) -> HeapBatch {
        HeapBatch {
            cap: self.cap.clone(),
            members: Vec::new(),
        }
    }

    /// Create a collection by canonical name (APP-1 / CORE plan §6; wire op 106 path).
    ///
    /// Embedded path uses the same durable `(HeapId, operation_id)` create as
    /// qualified remote op 106. Optional `operation_id` is the client idempotency key.
    pub fn create_collection(&self, name: &str) -> Result<CreatedCollection, Error> {
        self.create_collection_with(name, None)
    }

    /// Create a collection; `operation_id` is the 16-byte client operation id when set.
    ///
    /// Identical `(heap, operation_id, name)` retries return the original receipt and
    /// collection id. Reuse of the same `operation_id` with a different name is
    /// [`ErrorCode::ConsistencyViolation`].
    pub fn create_collection_with(
        &self,
        name: &str,
        operation_id: Option<[u8; 16]>,
    ) -> Result<CreatedCollection, Error> {
        validate_collection_name(name)?;
        let op_id = match operation_id {
            Some(id) => id,
            None => {
                let eid = CollectionId::new_random().map_err(|e| Error::Internal(e.to_string()))?;
                *eid.as_bytes()
            }
        };
        let admin = self
            .store
            .create_collection_idempotent(op_id, name)
            .map_err(map_create_object_err)?;
        let collection_id = CollectionId::from_bytes_unchecked_nonzero(admin.object_id)
            .map_err(|e| Error::Internal(e.to_string()))?;
        let collection = HeapCollection {
            cap: self.cap.clone(),
            store: Arc::clone(&self.store),
            id: collection_id,
            name_at_open: admin.name,
            decoded_json_cache: Arc::clone(&self.decoded_json_cache),
        };
        Ok(CreatedCollection {
            collection,
            receipt_id: admin.receipt_id,
            descriptor_hash: admin.descriptor_hash,
            created_at_unix_s: admin.created_at,
            operation_id: admin.operation_id,
            replayed: admin.replayed,
        })
    }

    /// List active collections in this heap (catalog tip).
    pub fn list_collections(&self) -> Result<Vec<ListedCollection>, Error> {
        let heap_id = *self.cap.heap_id().as_bytes();
        let entries = try_load_collections_catalog(&self.layout, &heap_id)?.unwrap_or_default();
        let mut out = Vec::with_capacity(entries.len());
        for e in entries {
            if e.state == residiuum_format::ObjectDescriptorState::Retired {
                continue;
            }
            let collection_id = CollectionId::from_bytes_unchecked_nonzero(e.object_id)
                .map_err(|err| Error::Internal(err.to_string()))?;
            out.push(ListedCollection {
                heap_id: self.cap.heap_id(),
                collection_id,
                name: e.name,
                descriptor_hash: e.descriptor_hash,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }
}

/// Result of [`Heap::create_collection`] (APP-1 embedded).
pub struct CreatedCollection {
    /// Open handle for the new collection.
    pub collection: HeapCollection,
    /// Admin receipt id (original on replay).
    pub receipt_id: [u8; 16],
    /// Tip descriptor hash at first acceptance (original on replay).
    pub descriptor_hash: [u8; 32],
    /// Create timestamp (unix seconds; original on replay).
    pub created_at_unix_s: u64,
    /// Client operation id used for this attempt (idempotency key).
    pub operation_id: [u8; 16],
    /// True when this result came from `(HeapId, operation_id)` dedup replay.
    pub replayed: bool,
}

/// One row from [`Heap::list_collections`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedCollection {
    /// Owning heap.
    pub heap_id: HeapId,
    /// Immutable collection id.
    pub collection_id: CollectionId,
    /// Canonical name.
    pub name: String,
    /// Tip descriptor hash.
    pub descriptor_hash: [u8; 32],
}

fn map_create_object_err(e: StoreError) -> Error {
    match e {
        StoreError::HeapAdmit(ref msg)
            if msg.contains("name/alias conflict") || msg.contains("object id already exists") =>
        {
            Error::Remote {
                code: "already_exists".into(),
                message: msg.clone(),
            }
        }
        other => Error::from(other),
    }
}

/// Typed collection handle. Identity is immutable after open.
///
/// Data plane methods encode **SubjectV2** keys
/// (`heap_id || collection || key`) so two heaps that share a human collection
/// name still address disjoint store subjects (HP-007 residual).
///
/// [`Clone`] is cheap (`Arc` store + cap) and supports multi-thread CAS
/// matrices (APB-2 T8) where each task holds its own handle over the same
/// [`HeapStore`] mutex.
#[derive(Clone)]
pub struct HeapCollection {
    cap: HeapCap,
    store: Arc<HeapStore>,
    id: CollectionId,
    name_at_open: String,
    decoded_json_cache: Arc<Mutex<DecodedJsonCache>>,
}

impl HeapCollection {
    /// Immutable collection id.
    pub fn id(&self) -> CollectionId {
        self.id
    }

    /// Name observed at open (may later be an alias).
    pub fn name(&self) -> &str {
        &self.name_at_open
    }

    /// Owning heap id.
    pub fn heap_id(&self) -> HeapId {
        self.cap.heap_id()
    }

    /// Capability instance.
    pub fn capability(&self) -> &HeapCap {
        &self.cap
    }

    /// Whether this handle shares the same capability instance as `other`.
    pub fn same_heap_instance(&self, other: &HeapCollection) -> bool {
        self.cap.same_instance(&other.cap)
    }

    /// Put a JSON value under `key` (SubjectV2, durable).
    pub fn put<T: Serialize>(&self, key: &str, value: &T) -> Result<WriteReceipt, Error> {
        self.put_with(key, value, PutOptions::default())
    }

    /// Put JSON with durability options. Durability other than durable is
    /// accepted for API parity; the heap façade currently always writes durable.
    pub fn put_with<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        _options: PutOptions,
    ) -> Result<WriteReceipt, Error> {
        let json = serde_json::to_value(value)?;
        let body = crate::value::encode_json(&json)?;
        self.put_raw_body(key, &body)
    }

    /// Put opaque typed-bytes body under `key` (SubjectV2, durable).
    ///
    /// Prefer [`Self::put_bytes`] which applies the SDK type tag.
    pub fn put_bytes(&self, key: &str, bytes: &[u8]) -> Result<WriteReceipt, Error> {
        let body = crate::value::encode_bytes(bytes);
        self.put_raw_body(key, &body)
    }

    fn put_raw_body(&self, key: &str, body: &[u8]) -> Result<WriteReceipt, Error> {
        self.put_raw_body_if(key, body, residiuum_store::WriteCondition::Unconditional)
    }

    pub(crate) fn submit_raw_body_with_operation<F>(
        &self,
        key: String,
        body: Vec<u8>,
        condition: residiuum_store::WriteCondition,
        operation_id: [u8; 16],
        content_hash: [u8; 32],
        completion: F,
    ) -> Result<(), Error>
    where
        F: FnOnce(Result<(WriteReceipt, bool), Error>) + Send + 'static,
    {
        validate_key(&key)?;
        let receipt_key = key.clone();
        self.store.submit_collection_put_with_operation(
            self.id.as_bytes(),
            key.as_bytes(),
            body,
            condition,
            operation_id,
            content_hash,
            move |result| {
                completion(result.map_err(Error::from).map(|(receipt, deduplicated)| {
                    (WriteReceipt::from_store(receipt_key, receipt), deduplicated)
                }));
            },
        )?;
        Ok(())
    }

    pub(crate) fn submit_delete_with_operation<F>(
        &self,
        key: String,
        condition: residiuum_store::WriteCondition,
        operation_id: [u8; 16],
        content_hash: [u8; 32],
        completion: F,
    ) -> Result<(), Error>
    where
        F: FnOnce(Result<(DeleteReceipt, bool), Error>) + Send + 'static,
    {
        validate_key(&key)?;
        let receipt_key = key.clone();
        self.store.submit_collection_delete_with_operation(
            self.id.as_bytes(),
            key.as_bytes(),
            condition,
            operation_id,
            content_hash,
            move |result| {
                completion(result.map_err(Error::from).map(|(receipt, deduplicated)| {
                    (
                        DeleteReceipt::from_store(receipt_key, true, receipt),
                        deduplicated,
                    )
                }));
            },
        )?;
        Ok(())
    }

    /// Conditional put of a raw body under Key Atomic store CAS (APB-2).
    fn put_raw_body_if(
        &self,
        key: &str,
        body: &[u8],
        condition: residiuum_store::WriteCondition,
    ) -> Result<WriteReceipt, Error> {
        validate_key(key)?;
        let receipt =
            self.store
                .put_collection_if(self.id.as_bytes(), key.as_bytes(), body, condition)?;
        Ok(WriteReceipt::from_store(key.to_string(), receipt))
    }

    /// Create: put only when the key is absent (store Key Atomic).
    pub fn create_if_absent<T: Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<WriteReceipt, Error> {
        let json = serde_json::to_value(value)?;
        let body = crate::value::encode_json(&json)?;
        self.put_raw_body_if(key, &body, residiuum_store::WriteCondition::Absent)
    }

    /// Replace: put only when live establishing event id matches (store Key Atomic).
    pub fn replace_if_version<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        if_version: [u8; 16],
    ) -> Result<WriteReceipt, Error> {
        let json = serde_json::to_value(value)?;
        let body = crate::value::encode_json(&json)?;
        self.put_raw_body_if(
            key,
            &body,
            residiuum_store::WriteCondition::LiveEventId(if_version),
        )
    }

    /// Conditional delete with store Key Atomic (APB-2).
    pub fn delete_if(
        &self,
        key: &str,
        condition: residiuum_store::WriteCondition,
    ) -> Result<DeleteReceipt, Error> {
        validate_key(key)?;
        // LiveEventId / Present success implies a live value was removed.
        // Unconditional: pre-observe (removed flag only; write itself is CAS-free).
        let removed = match condition {
            residiuum_store::WriteCondition::LiveEventId(_)
            | residiuum_store::WriteCondition::Present => true,
            residiuum_store::WriteCondition::Unconditional
            | residiuum_store::WriteCondition::Absent => self
                .store
                .get_collection(self.id.as_bytes(), key.as_bytes())?
                .is_some(),
        };
        let receipt =
            self.store
                .delete_collection_if(self.id.as_bytes(), key.as_bytes(), condition)?;
        Ok(DeleteReceipt::from_store(key.to_string(), removed, receipt))
    }

    /// Get JSON for `key`.
    pub fn get(&self, key: &str) -> Result<Option<serde_json::Value>, Error> {
        match self.get_versioned_raw_body(key)? {
            None => Ok(None),
            Some((raw, version)) => Ok(Some(self.decode_json_versioned(key, &raw, version)?)),
        }
    }

    pub(crate) fn decode_json_versioned(
        &self,
        key: &str,
        body: &[u8],
        version: [u8; 16],
    ) -> Result<serde_json::Value, Error> {
        Ok(self
            .decode_json_versioned_shared(key, body, version)?
            .as_ref()
            .clone())
    }

    pub(crate) fn decode_json_versioned_shared(
        &self,
        key: &str,
        body: &[u8],
        version: [u8; 16],
    ) -> Result<Arc<serde_json::Value>, Error> {
        let cache_key = DecodedJsonKey {
            heap_id: *self.cap.heap_id().as_bytes(),
            collection_id: *self.id.as_bytes(),
            key: key.to_string(),
        };
        if let Some(value) = self
            .decoded_json_cache
            .lock()
            .ok()
            .and_then(|cache| cache.get(&cache_key, &version))
        {
            return Ok(value);
        }
        let value = Arc::new(crate::value::decode_json(body)?);
        if let Ok(mut cache) = self.decoded_json_cache.lock() {
            cache.insert(cache_key, version, Arc::clone(&value), body.len());
        }
        Ok(value)
    }

    /// Decode a body already proven absent from the version-qualified cache.
    /// The caller must have obtained and verified `version` in the same bounded
    /// resolution flow; avoiding a second cache probe keeps miss accounting and
    /// hot-lock work honest.
    fn decode_json_cache_miss_shared(
        &self,
        key: &str,
        body: &[u8],
        version: [u8; 16],
    ) -> Result<Arc<serde_json::Value>, Error> {
        let value = Arc::new(crate::value::decode_json(body)?);
        if let Ok(mut cache) = self.decoded_json_cache.lock() {
            cache.insert(
                DecodedJsonKey {
                    heap_id: *self.cap.heap_id().as_bytes(),
                    collection_id: *self.id.as_bytes(),
                    key: key.to_string(),
                },
                version,
                Arc::clone(&value),
                body.len(),
            );
        }
        Ok(value)
    }

    /// Get opaque application bytes for `key` (strips type tag).
    pub fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        match self.get_raw_body(key)? {
            None => Ok(None),
            Some(raw) => Ok(Some(crate::value::decode_bytes(&raw)?)),
        }
    }

    fn get_raw_body(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        validate_key(key)?;
        Ok(self
            .store
            .get_collection(self.id.as_bytes(), key.as_bytes())?)
    }

    pub(crate) fn get_json_many_covered(
        &self,
        keys: &[String],
    ) -> Result<Vec<(String, crate::query_bytecode_v1::HostDocument)>, Error> {
        use crate::query_bytecode_v1::HostDocument;

        for key in keys {
            validate_key(key)?;
        }
        let raw_keys: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let identities = self
            .store
            .get_collection_versions(self.id.as_bytes(), &raw_keys)?;
        if identities.len() != keys.len() {
            return self.get_json_many_covered_raw(keys);
        }
        let mut documents = vec![None; keys.len()];
        let mut misses = Vec::new();
        if let Ok(cache) = self.decoded_json_cache.lock() {
            for (slot, ((expected_key, supplied_key), (raw_key, version))) in keys
                .iter()
                .zip(raw_keys.iter())
                .zip(identities.iter())
                .enumerate()
            {
                if raw_key != supplied_key {
                    return self.get_json_many_covered_raw(keys);
                }
                let Some(version) = version else {
                    documents[slot] = Some((expected_key.clone(), HostDocument::Absent));
                    continue;
                };
                let cache_key = DecodedJsonKey {
                    heap_id: *self.cap.heap_id().as_bytes(),
                    collection_id: *self.id.as_bytes(),
                    key: expected_key.clone(),
                };
                match cache.get_with_len(&cache_key, version) {
                    Some((value, logical_bytes)) => {
                        documents[slot] = Some((
                            expected_key.clone(),
                            HostDocument::Present {
                                value: value.into(),
                                logical_bytes: Some(logical_bytes),
                            },
                        ));
                    }
                    None => {
                        misses.push((slot, supplied_key.clone(), expected_key.clone(), *version))
                    }
                }
            }
        } else {
            for (slot, ((expected_key, supplied_key), (raw_key, version))) in keys
                .iter()
                .zip(raw_keys.iter())
                .zip(identities.iter())
                .enumerate()
            {
                if raw_key != supplied_key {
                    return self.get_json_many_covered_raw(keys);
                }
                match version {
                    Some(version) => {
                        misses.push((slot, supplied_key.clone(), expected_key.clone(), *version))
                    }
                    None => {
                        documents[slot] = Some((expected_key.clone(), HostDocument::Absent));
                    }
                }
            }
        }

        if !misses.is_empty() {
            let miss_keys = misses
                .iter()
                .map(|(_, key, _, _)| key.clone())
                .collect::<Vec<_>>();
            let resolved = self
                .store
                .get_collection_many_versioned(self.id.as_bytes(), &miss_keys)?;
            if resolved.len() != misses.len() {
                return self.get_json_many_covered_raw(keys);
            }
            for (
                (slot, expected_key, key, expected_version),
                (resolved_key, body, version, hole),
            ) in misses.into_iter().zip(resolved)
            {
                if resolved_key != expected_key {
                    return self.get_json_many_covered_raw(keys);
                }
                if let Some(reason) = hole {
                    documents[slot] = Some((
                        key,
                        HostDocument::Hole {
                            code: reason.as_str().to_string(),
                        },
                    ));
                    continue;
                }
                if version != Some(expected_version) {
                    return self.get_json_many_covered_raw(keys);
                }
                let Some(body) = body else {
                    return self.get_json_many_covered_raw(keys);
                };
                let document =
                    match self.decode_json_cache_miss_shared(&key, &body, expected_version) {
                        Ok(value) => HostDocument::Present {
                            value: value.into(),
                            logical_bytes: crate::value::encoded_json_payload_len(&body),
                        },
                        Err(error) => HostDocument::Hole {
                            code: error.code().as_str().to_string(),
                        },
                    };
                documents[slot] = Some((key, document));
            }
        }

        documents
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| Error::Internal("cached get-many left an unresolved row".into()))
    }

    fn get_json_many_covered_raw(
        &self,
        keys: &[String],
    ) -> Result<Vec<(String, crate::query_bytecode_v1::HostDocument)>, Error> {
        use crate::query_bytecode_v1::HostDocument;

        for key in keys {
            validate_key(key)?;
        }
        let raw_keys: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        self.store
            .get_collection_many_versioned(self.id.as_bytes(), &raw_keys)?
            .into_iter()
            .map(|(key, body, version, hole)| {
                let key = String::from_utf8(key)
                    .map_err(|_| Error::Internal("get-many: non-UTF-8 key".into()))?;
                let document = match (body, version, hole) {
                    (Some(body), Some(version), None) => {
                        match self.decode_json_versioned_shared(&key, &body, version) {
                            Ok(value) => HostDocument::Present {
                                value: value.into(),
                                logical_bytes: crate::value::encoded_json_payload_len(&body),
                            },
                            Err(error) => HostDocument::Hole {
                                code: error.code().as_str().to_string(),
                            },
                        }
                    }
                    (None, _, Some(reason)) => HostDocument::Hole {
                        code: reason.as_str().to_string(),
                    },
                    (None, None, None) => HostDocument::Absent,
                    _ => {
                        return Err(Error::Internal(
                            "get-many returned incoherent body/version/hole".into(),
                        ))
                    }
                };
                Ok((key, document))
            })
            .collect()
    }

    pub(crate) fn get_versioned_raw_body(
        &self,
        key: &str,
    ) -> Result<Option<(Vec<u8>, [u8; 16])>, Error> {
        validate_key(key)?;
        Ok(self
            .store
            .get_collection_versioned(self.id.as_bytes(), key.as_bytes())?
            .map(|value| (value.body, value.version)))
    }

    pub(crate) fn scan_page_raw(
        &self,
        limit: usize,
        after_key: Option<&[u8]>,
    ) -> Result<residiuum_store::VersionedCollectionScanPage, Error> {
        Ok(self
            .store
            .scan_collection_page_versioned(self.id.as_bytes(), limit, after_key)?)
    }

    /// Resolve a bounded document page by immutable key/version identity first.
    /// Exact decoded-cache hits avoid body materialisation; misses are fetched
    /// and verified as one batch. Concurrent identity drift refuses this fast
    /// path so the caller can fall back to the ordinary authoritative scan.
    pub(crate) fn scan_json_covered_page_cached(
        &self,
        limit: usize,
        after_key: Option<&[u8]>,
    ) -> Result<Option<Vec<(String, crate::query_bytecode_v1::HostDocument)>>, Error> {
        use crate::query_bytecode_v1::HostDocument;

        let page =
            self.store
                .scan_collection_versions_page(self.id.as_bytes(), limit, after_key)?;
        let mut documents = vec![None; page.entries.len()];
        let mut misses = Vec::new();

        if let Ok(cache) = self.decoded_json_cache.lock() {
            for (slot, (raw_key, version)) in page.entries.iter().enumerate() {
                let key = String::from_utf8(raw_key.clone())
                    .map_err(|_| Error::Internal("cached scan: non-UTF-8 key".into()))?;
                let cache_key = DecodedJsonKey {
                    heap_id: *self.cap.heap_id().as_bytes(),
                    collection_id: *self.id.as_bytes(),
                    key: key.clone(),
                };
                match cache.get_with_len(&cache_key, version) {
                    Some((value, logical_bytes)) => {
                        documents[slot] = Some((
                            key,
                            HostDocument::Present {
                                value: value.into(),
                                logical_bytes: Some(logical_bytes),
                            },
                        ));
                    }
                    None => misses.push((slot, raw_key.clone(), key, *version, cache_key)),
                }
            }
        } else {
            for (slot, (raw_key, version)) in page.entries.iter().enumerate() {
                let key = String::from_utf8(raw_key.clone())
                    .map_err(|_| Error::Internal("cached scan: non-UTF-8 key".into()))?;
                misses.push((
                    slot,
                    raw_key.clone(),
                    key.clone(),
                    *version,
                    DecodedJsonKey {
                        heap_id: *self.cap.heap_id().as_bytes(),
                        collection_id: *self.id.as_bytes(),
                        key,
                    },
                ));
            }
        }

        if !misses.is_empty() {
            let miss_keys = misses
                .iter()
                .map(|(_, raw_key, _, _, _)| raw_key.clone())
                .collect::<Vec<_>>();
            let resolved = self
                .store
                .get_collection_many_versioned(self.id.as_bytes(), &miss_keys)?;
            if resolved.len() != misses.len() {
                return Ok(None);
            }
            let mut decoded = Vec::with_capacity(misses.len());
            for (
                (slot, expected_key, key, expected_version, cache_key),
                (resolved_key, body, version, hole),
            ) in misses.into_iter().zip(resolved)
            {
                if resolved_key != expected_key {
                    return Ok(None);
                }
                if let Some(reason) = hole {
                    documents[slot] = Some((
                        key,
                        HostDocument::Hole {
                            code: reason.as_str().to_string(),
                        },
                    ));
                    continue;
                }
                if version != Some(expected_version) {
                    return Ok(None);
                }
                let Some(body) = body else {
                    return Ok(None);
                };
                match crate::value::decode_json(&body) {
                    Ok(value) => {
                        let value = Arc::new(value);
                        let logical_bytes = crate::value::encoded_json_payload_len(&body);
                        documents[slot] = Some((
                            key,
                            HostDocument::Present {
                                value: Arc::clone(&value).into(),
                                logical_bytes,
                            },
                        ));
                        decoded.push((cache_key, expected_version, value, body.len()));
                    }
                    Err(error) => {
                        documents[slot] = Some((
                            key,
                            HostDocument::Hole {
                                code: error.code().as_str().to_string(),
                            },
                        ));
                    }
                }
            }
            if let Ok(mut cache) = self.decoded_json_cache.lock() {
                for (key, version, value, encoded_len) in decoded {
                    cache.insert(key, version, value, encoded_len);
                }
            }
        }

        documents
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| Error::Internal("cached scan left an unresolved row".into()))
            .map(Some)
    }

    /// Delete `key` (SubjectV2, durable).
    pub fn delete(&self, key: &str) -> Result<DeleteReceipt, Error> {
        validate_key(key)?;
        let existed = self
            .store
            .get_collection(self.id.as_bytes(), key.as_bytes())?
            .is_some();
        let receipt = self
            .store
            .delete_collection(self.id.as_bytes(), key.as_bytes())?;
        Ok(DeleteReceipt::from_store(key.to_string(), existed, receipt))
    }

    /// Immutable event history for `key` (SubjectV2; DEF-099 surface, oldest first).
    ///
    /// First cut accepts Read; ReadHistory is preferred when present on the cap.
    pub fn history(&self, key: &str) -> Result<KeyHistory, Error> {
        validate_key(key)?;
        let hist = self
            .store
            .history_collection(self.id.as_bytes(), key.as_bytes())?;
        KeyHistory::from_store(key.to_string(), hist)
    }

    /// List application keys (SubjectV2), deterministic order.
    ///
    /// `limit` is clamped 1..=4096. `after_key` resumes after that key.
    pub fn list_keys(&self, limit: usize, after_key: Option<&str>) -> Result<Vec<String>, Error> {
        if let Some(k) = after_key {
            validate_key(k)?;
        }
        let raw = self.store.list_collection_keys(
            self.id.as_bytes(),
            limit,
            after_key.map(|s| s.as_bytes()),
        )?;
        let mut out = Vec::with_capacity(raw.len());
        for k in raw {
            match String::from_utf8(k) {
                Ok(s) => out.push(s),
                Err(_) => {
                    return Err(Error::Internal(
                        "list_keys: non-UTF-8 application key".into(),
                    ))
                }
            }
        }
        Ok(out)
    }

    /// List secondary indexes (metadata only). Requires Read.
    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>, Error> {
        let listed = self.store.list_indexes(self.id.as_bytes())?;
        Ok(listed
            .iter()
            .map(|idx| IndexInfo::from_store_with_collection(idx, self.name_at_open.clone()))
            .collect())
    }

    /// Equality index acceleration (APB-7 T4): candidate application keys.
    ///
    /// `equalities` is a shallow AND of (field path, JSON value). Returns:
    /// - `Ok(None)` — no usable ready/partial index; caller must scan
    /// - `Ok(Some(keys))` — candidates (may be empty when Ready+complete proves absence)
    pub fn lookup_index_keys(
        &self,
        equalities: &[(String, serde_json::Value)],
    ) -> Result<Option<Vec<String>>, Error> {
        let raw = self
            .store
            .lookup_index_keys(self.id.as_bytes(), equalities)?;
        let Some(keys) = raw else {
            return Ok(None);
        };
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            match String::from_utf8(k) {
                Ok(s) => out.push(s),
                Err(_) => {
                    return Err(Error::Internal(
                        "index lookup: non-UTF-8 application key".into(),
                    ))
                }
            }
        }
        Ok(Some(out))
    }

    pub(crate) fn lookup_index_keys_many(
        &self,
        field: &str,
        values: &[serde_json::Value],
    ) -> Result<Option<Vec<String>>, Error> {
        let raw = self
            .store
            .lookup_index_keys_many(self.id.as_bytes(), field, values)?;
        let Some(keys) = raw else {
            return Ok(None);
        };
        keys.into_iter()
            .map(|key| {
                String::from_utf8(key).map_err(|_| {
                    Error::Internal("index lookup-many: non-UTF-8 application key".into())
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    pub(crate) fn source_frontier(&self) -> Result<[u8; 32], Error> {
        Ok(self.store.segment_fingerprint()?)
    }

    pub(crate) fn lookup_index_keys_with_constraints(
        &self,
        equalities: &[(String, serde_json::Value)],
        constrained_fields: &[String],
    ) -> Result<Option<Vec<String>>, Error> {
        let raw = self.store.lookup_index_keys_with_constraints(
            self.id.as_bytes(),
            equalities,
            constrained_fields,
        )?;
        let Some(keys) = raw else {
            return Ok(None);
        };
        keys.into_iter()
            .map(|key| {
                String::from_utf8(key)
                    .map_err(|_| Error::Internal("index lookup: non-UTF-8 application key".into()))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    pub(crate) fn lookup_ordered_index_keys(
        &self,
        field: &str,
        field_descending: bool,
        key_descending: bool,
    ) -> Result<Option<Vec<String>>, Error> {
        let raw = self.store.lookup_ordered_index_keys(
            self.id.as_bytes(),
            field,
            field_descending,
            key_descending,
        )?;
        let Some(keys) = raw else {
            return Ok(None);
        };
        keys.into_iter()
            .map(|key| {
                String::from_utf8(key).map_err(|_| {
                    Error::Internal("ordered index lookup: non-UTF-8 application key".into())
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    pub(crate) fn lookup_covering_index_values(
        &self,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<serde_json::Value>>>, Error> {
        Ok(self
            .store
            .lookup_covering_index_values(self.id.as_bytes(), fields)?)
    }

    /// Scan authoritative live documents while materializing only requested
    /// JSON paths. A page hole refuses the optimization so normal query
    /// coverage handling remains authoritative.
    pub(crate) fn scan_projected_values(
        &self,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<serde_json::Value>>>, Error> {
        if fields.is_empty() {
            return Ok(None);
        }
        let mut rows = Vec::new();
        let mut after_key: Option<Vec<u8>> = None;
        loop {
            let page = self.store.scan_collection_versions_page(
                self.id.as_bytes(),
                4_096,
                after_key.as_deref(),
            )?;
            let mut projected = vec![None; page.entries.len()];
            let mut misses = Vec::new();
            if let Ok(cache) = self.decoded_json_cache.lock() {
                for (slot, (raw_key, version)) in page.entries.iter().enumerate() {
                    let key = String::from_utf8(raw_key.clone())
                        .map_err(|_| Error::Internal("projected scan: non-UTF-8 key".into()))?;
                    let cache_key = DecodedJsonKey {
                        heap_id: *self.cap.heap_id().as_bytes(),
                        collection_id: *self.id.as_bytes(),
                        key,
                    };
                    match cache.get_projected(&cache_key, version, fields) {
                        Some(row) => projected[slot] = Some(row),
                        None => misses.push((slot, raw_key.clone(), *version, cache_key)),
                    }
                }
            } else {
                for (slot, (raw_key, version)) in page.entries.iter().enumerate() {
                    let key = String::from_utf8(raw_key.clone())
                        .map_err(|_| Error::Internal("projected scan: non-UTF-8 key".into()))?;
                    misses.push((
                        slot,
                        raw_key.clone(),
                        *version,
                        DecodedJsonKey {
                            heap_id: *self.cap.heap_id().as_bytes(),
                            collection_id: *self.id.as_bytes(),
                            key,
                        },
                    ));
                }
            }
            if !misses.is_empty() {
                let miss_keys = misses
                    .iter()
                    .map(|(_, key, _, _)| key.clone())
                    .collect::<Vec<_>>();
                let resolved = self
                    .store
                    .get_collection_many_versioned(self.id.as_bytes(), &miss_keys)?;
                let mut decoded = Vec::with_capacity(misses.len());
                for (
                    (slot, expected_key, expected_version, cache_key),
                    (key, body, version, hole),
                ) in misses.into_iter().zip(resolved)
                {
                    if key != expected_key || hole.is_some() || version != Some(expected_version) {
                        return Ok(None);
                    }
                    let Some(body) = body else {
                        return Ok(None);
                    };
                    let value = crate::value::decode_json(&body)?;
                    projected[slot] = Some(project_json_fields(&value, fields));
                    decoded.push((cache_key, expected_version, value, body.len()));
                }
                if let Ok(mut cache) = self.decoded_json_cache.lock() {
                    for (key, version, value, encoded_len) in decoded {
                        cache.insert(key, version, Arc::new(value), encoded_len);
                    }
                }
            }
            rows.extend(
                projected
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| {
                        Error::Internal("projected scan left an unresolved row".into())
                    })?,
            );
            if !page.has_more {
                return Ok(Some(rows));
            }
            let Some(last_key) = page.last_key else {
                return Err(Error::Internal(
                    "projected scan has_more without continuation key".into(),
                ));
            };
            after_key = Some(last_key);
        }
    }

    pub(crate) fn scan_group_aggregate(
        &self,
        spec: &crate::plan_v1::GroupAggSpec,
        where_k: &crate::CompiledKernelWhere,
        candidate_keys: Option<&[String]>,
    ) -> Result<Option<crate::query_bytecode_v1::HostGroupAggregate>, Error> {
        use crate::query_bytecode_v1::HostGroupAggregate;

        let mut accumulator = crate::query_bytecode_v1::GroupAccumulator::new(spec);
        let key_only = accumulator.uses_exact_logical_key_count_only();
        let (projected_fields, projected_predicate) = if where_k.constant_value() == Some(true) {
            (
                accumulator
                    .covering_fields()
                    .or_else(|| key_only.then(Vec::new)),
                false,
            )
        } else if let Some(mut fields) = where_k.direct_fields() {
            if let Some(aggregate_fields) = accumulator.covering_fields() {
                for field in aggregate_fields {
                    if !fields.contains(&field) {
                        fields.push(field);
                    }
                }
                (Some(fields), true)
            } else if key_only {
                (Some(fields), true)
            } else {
                (None, false)
            }
        } else {
            (None, false)
        };
        let mut examined_documents = 0u64;
        let mut examined_bytes = 0u64;
        if let Some(keys) = candidate_keys {
            if keys.len() > 4_096 {
                return Ok(None);
            }
            let raw_keys = keys
                .iter()
                .map(|key| key.as_bytes().to_vec())
                .collect::<Vec<_>>();
            let identities = self
                .store
                .get_collection_versions(self.id.as_bytes(), &raw_keys)?;
            if identities.len() != keys.len() {
                return Ok(None);
            }
            let mut required = Vec::with_capacity(keys.len());
            for ((expected_raw, (actual_raw, version)), expected_key) in
                raw_keys.iter().zip(&identities).zip(keys)
            {
                if actual_raw != expected_raw {
                    return Ok(None);
                }
                let Some(version) = version else {
                    return Ok(None);
                };
                required.push((expected_key.clone(), expected_raw.clone(), *version));
            }
            if let Some(fields) = projected_fields.as_ref() {
                let Some(rows) = self.resolve_projected_aggregate_rows(&required, fields)? else {
                    return Ok(None);
                };
                for (key, values, logical_bytes) in rows {
                    examined_documents = examined_documents.saturating_add(1);
                    examined_bytes = examined_bytes.saturating_add(logical_bytes);
                    if projected_predicate
                        && !where_k
                            .eval_direct_projected(fields, &values)
                            .ok_or_else(|| {
                                Error::Internal("projected predicate field mismatch".into())
                            })?
                    {
                        continue;
                    }
                    if key_only {
                        accumulator.ingest_with_logical_key(&key, &serde_json::Value::Null)?;
                    } else {
                        accumulator.ingest_covering(
                            fields,
                            values.into_iter().map(Option::unwrap_or_default).collect(),
                        )?;
                    }
                }
                return Ok(Some(HostGroupAggregate {
                    rows: accumulator.finish()?,
                    examined_documents,
                    examined_bytes,
                }));
            }
            if !self.ensure_decoded_cache_entries(&required)? {
                return Ok(None);
            }
            let Ok(cache) = self.decoded_json_cache.lock() else {
                return Ok(None);
            };
            for (expected_key, _, version) in &required {
                let Some((value, logical_bytes)) = cache.get_raw_ref_with_len(
                    self.cap.heap_id().as_bytes(),
                    self.id.as_bytes(),
                    expected_key,
                    version,
                ) else {
                    return Ok(None);
                };
                examined_documents = examined_documents.saturating_add(1);
                examined_bytes = examined_bytes.saturating_add(logical_bytes);
                if where_k.eval_doc(value)? {
                    if accumulator.uses_exact_logical_key() {
                        accumulator.ingest_with_logical_key(expected_key, value)?;
                    } else {
                        accumulator.ingest(value)?;
                    }
                }
            }
            return Ok(Some(HostGroupAggregate {
                rows: accumulator.finish()?,
                examined_documents,
                examined_bytes,
            }));
        }
        let mut after_key: Option<Vec<u8>> = None;
        loop {
            let page = self.store.scan_collection_versions_page(
                self.id.as_bytes(),
                4_096,
                after_key.as_deref(),
            )?;
            let required = page
                .entries
                .iter()
                .map(|(raw_key, version)| {
                    let key = String::from_utf8(raw_key.clone())
                        .map_err(|_| Error::Internal("aggregate scan: non-UTF-8 key".into()))?;
                    Ok((key, raw_key.clone(), *version))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            if let Some(fields) = projected_fields.as_ref() {
                let Some(rows) = self.resolve_projected_aggregate_rows(&required, fields)? else {
                    return Ok(None);
                };
                for (key, values, logical_bytes) in rows {
                    examined_documents = examined_documents.saturating_add(1);
                    examined_bytes = examined_bytes.saturating_add(logical_bytes);
                    if projected_predicate
                        && !where_k
                            .eval_direct_projected(fields, &values)
                            .ok_or_else(|| {
                                Error::Internal("projected predicate field mismatch".into())
                            })?
                    {
                        continue;
                    }
                    if key_only {
                        accumulator.ingest_with_logical_key(&key, &serde_json::Value::Null)?;
                    } else {
                        accumulator.ingest_covering(
                            fields,
                            values.into_iter().map(Option::unwrap_or_default).collect(),
                        )?;
                    }
                }
                if !page.has_more {
                    break;
                }
                let Some(next_key) = page.last_key else {
                    return Err(Error::Internal(
                        "aggregate scan has_more without continuation key".into(),
                    ));
                };
                after_key = Some(next_key);
                continue;
            }
            if !self.ensure_decoded_cache_entries(&required)? {
                return Ok(None);
            }
            let Ok(cache) = self.decoded_json_cache.lock() else {
                return Ok(None);
            };
            for (key, _, version) in &required {
                let Some((value, logical_bytes)) = cache.get_raw_ref_with_len(
                    self.cap.heap_id().as_bytes(),
                    self.id.as_bytes(),
                    key,
                    version,
                ) else {
                    return Ok(None);
                };
                examined_documents = examined_documents.saturating_add(1);
                examined_bytes = examined_bytes.saturating_add(logical_bytes);
                if where_k.eval_doc(value)? {
                    if accumulator.uses_exact_logical_key() {
                        accumulator.ingest_with_logical_key(key, value)?;
                    } else {
                        accumulator.ingest(value)?;
                    }
                }
            }
            drop(cache);
            if !page.has_more {
                break;
            }
            let Some(next_key) = page.last_key else {
                return Err(Error::Internal(
                    "aggregate scan has_more without continuation key".into(),
                ));
            };
            after_key = Some(next_key);
        }
        Ok(Some(HostGroupAggregate {
            rows: accumulator.finish()?,
            examined_documents,
            examined_bytes,
        }))
    }

    /// Resolve and partially decode an exact set of aggregate inputs. Every
    /// body is still version/hole checked and its complete JSON syntax is
    /// consumed before any row reaches the accumulator.
    fn resolve_projected_aggregate_rows(
        &self,
        required: &[(String, Vec<u8>, [u8; 16])],
        fields: &[String],
    ) -> Result<Option<Vec<(String, Vec<Option<serde_json::Value>>, u64)>>, Error> {
        let fields_hash = projection_fields_hash(fields);
        let cached_fields: Arc<[String]> = fields.to_vec().into();
        let projected_key = |key: &str| ProjectedJsonKey {
            document: DecodedJsonKey {
                heap_id: *self.cap.heap_id().as_bytes(),
                collection_id: *self.id.as_bytes(),
                key: key.to_string(),
            },
            fields_hash,
        };
        let mut rows = vec![None; required.len()];
        let mut missing = Vec::new();
        {
            let Ok(cache) = self.decoded_json_cache.lock() else {
                return Ok(None);
            };
            for (slot, (key, _, version)) in required.iter().enumerate() {
                if let Some((values, logical_bytes)) = cache.get_projection_raw(
                    self.cap.heap_id().as_bytes(),
                    self.id.as_bytes(),
                    key,
                    &fields_hash,
                    version,
                    fields,
                ) {
                    rows[slot] = Some((key.clone(), values.to_vec(), logical_bytes));
                } else {
                    missing.push(slot);
                }
            }
        }
        if missing.is_empty() {
            return Ok(rows.into_iter().collect());
        }
        let raw_keys = missing
            .iter()
            .map(|slot| required[*slot].1.clone())
            .collect::<Vec<_>>();
        let resolved = self
            .store
            .get_collection_many_versioned(self.id.as_bytes(), &raw_keys)?;
        if resolved.len() != missing.len() {
            return Ok(None);
        }
        let mut admissions = Vec::with_capacity(missing.len());
        for (slot, (actual_key, body, version, hole)) in missing.into_iter().zip(resolved) {
            let (key, expected_key, expected_version) = &required[slot];
            if actual_key != *expected_key || version != Some(*expected_version) || hole.is_some() {
                return Ok(None);
            }
            let Some(body) = body else {
                return Ok(None);
            };
            let logical_bytes = crate::value::encoded_json_payload_len(&body)
                .ok_or_else(|| crate::value::decode_json(&body).unwrap_err())?;
            let Some(values) = crate::value::project_json_fields_present(&body, fields)? else {
                return Ok(None);
            };
            let values: Arc<[Option<serde_json::Value>]> = values.into();
            rows[slot] = Some((key.clone(), values.to_vec(), logical_bytes));
            admissions.push((
                projected_key(key),
                *expected_version,
                Arc::clone(&cached_fields),
                values,
                logical_bytes,
            ));
        }
        if let Ok(mut cache) = self.decoded_json_cache.lock() {
            for (key, version, fields, values, logical_bytes) in admissions {
                cache.insert_projection(key, version, fields, values, logical_bytes);
            }
        }
        Ok(rows.into_iter().collect())
    }

    /// Populate exact key/version decoded identities as one bounded body read.
    /// Returns `false` when concurrent drift, disappearance, a storage hole or
    /// cache admission prevents the caller from proving the shortcut; callers
    /// then use the ordinary coverage-aware path.
    fn ensure_decoded_cache_entries(
        &self,
        required: &[(String, Vec<u8>, [u8; 16])],
    ) -> Result<bool, Error> {
        let mut missing = Vec::new();
        {
            let Ok(cache) = self.decoded_json_cache.lock() else {
                return Ok(false);
            };
            for (slot, (key, _, version)) in required.iter().enumerate() {
                if cache
                    .get_raw_ref_with_len(
                        self.cap.heap_id().as_bytes(),
                        self.id.as_bytes(),
                        key,
                        version,
                    )
                    .is_none()
                {
                    missing.push(slot);
                }
            }
        }
        if missing.is_empty() {
            return Ok(true);
        }

        let raw_keys = missing
            .iter()
            .map(|slot| required[*slot].1.clone())
            .collect::<Vec<_>>();
        let resolved = self
            .store
            .get_collection_many_versioned(self.id.as_bytes(), &raw_keys)?;
        if resolved.len() != missing.len() {
            return Ok(false);
        }
        let mut decoded = Vec::with_capacity(missing.len());
        for (slot, (actual_key, body, version, hole)) in missing.into_iter().zip(resolved) {
            let (key, expected_key, expected_version) = &required[slot];
            if actual_key != *expected_key || version != Some(*expected_version) || hole.is_some() {
                return Ok(false);
            }
            let Some(body) = body else {
                return Ok(false);
            };
            let value = Arc::new(crate::value::decode_json(&body)?);
            decoded.push((
                DecodedJsonKey {
                    heap_id: *self.cap.heap_id().as_bytes(),
                    collection_id: *self.id.as_bytes(),
                    key: key.clone(),
                },
                *expected_version,
                value,
                body.len(),
            ));
        }
        let Ok(mut cache) = self.decoded_json_cache.lock() else {
            return Ok(false);
        };
        for (key, version, value, encoded_len) in decoded {
            cache.insert(key, version, value, encoded_len);
        }
        // The caller immediately performs the authoritative version-qualified
        // lookup while holding this cache. Oversize/evicted admissions therefore
        // refuse there without paying for a second complete cache probe here.
        Ok(true)
    }

    /// Create a field index over JSON documents. Requires IndexAdmin + Read for build.
    pub fn create_index(&self, name: &str, fields: &[&str]) -> Result<IndexInfo, Error> {
        let idx = self.store.create_index(self.id.as_bytes(), name, fields)?;
        Ok(IndexInfo::from_store_with_collection(
            &idx,
            self.name_at_open.clone(),
        ))
    }

    /// Drop a secondary index by name. Requires IndexAdmin.
    pub fn drop_index(&self, name: &str) -> Result<(), Error> {
        self.store.drop_index(self.id.as_bytes(), name)?;
        Ok(())
    }

    /// Rebuild an existing index definition from a full collection scan.
    pub fn rebuild_index(&self, name: &str) -> Result<IndexInfo, Error> {
        let idx = self.store.rebuild_index(self.id.as_bytes(), name)?;
        Ok(IndexInfo::from_store_with_collection(
            &idx,
            self.name_at_open.clone(),
        ))
    }

    /// Issue a signed cursor bound to this collection + capability instance.
    pub fn sign_cursor(&self, position: &[u8]) -> SignedCursor {
        SignedCursor::sign(
            &self.cap,
            ObjectKind::Collection,
            self.id.as_bytes(),
            position,
        )
    }

    /// Verify a cursor was issued for this exact collection handle instance.
    pub fn verify_cursor(&self, cursor: &SignedCursor) -> Result<(), Error> {
        cursor.verify(&self.cap, ObjectKind::Collection, self.id.as_bytes())
    }
}

/// Typed stream handle.
pub struct HeapStream {
    cap: HeapCap,
    id: StreamId,
    name_at_open: String,
}

impl HeapStream {
    /// Immutable stream id.
    pub fn id(&self) -> StreamId {
        self.id
    }

    /// Name at open.
    pub fn name(&self) -> &str {
        &self.name_at_open
    }

    /// Capability instance.
    pub fn capability(&self) -> &HeapCap {
        &self.cap
    }

    /// Sign a cursor for this stream instance.
    pub fn sign_cursor(&self, position: &[u8]) -> SignedCursor {
        SignedCursor::sign(&self.cap, ObjectKind::Stream, self.id.as_bytes(), position)
    }

    /// Verify cursor for this stream instance.
    pub fn verify_cursor(&self, cursor: &SignedCursor) -> Result<(), Error> {
        cursor.verify(&self.cap, ObjectKind::Stream, self.id.as_bytes())
    }
}

/// Signed, heap-bound scan cursor (not transferable across heaps).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedCursor {
    capability_id: [u8; 16],
    heap_id: [u8; 16],
    object_kind: u8,
    object_id: [u8; 16],
    position: Vec<u8>,
    mac: [u8; 32],
}

impl SignedCursor {
    fn sign(cap: &HeapCap, kind: ObjectKind, object_id: &[u8; 16], position: &[u8]) -> Self {
        let capability_id = *cap.capability_id().as_bytes();
        let heap_id = *cap.heap_id().as_bytes();
        let object_kind = kind as u8;
        let mac = cursor_mac(
            &capability_id,
            &heap_id,
            object_kind,
            object_id,
            position,
            cap.security_revision().get(),
        );
        Self {
            capability_id,
            heap_id,
            object_kind,
            object_id: *object_id,
            position: position.to_vec(),
            mac,
        }
    }

    fn verify(&self, cap: &HeapCap, kind: ObjectKind, object_id: &[u8; 16]) -> Result<(), Error> {
        if self.capability_id != *cap.capability_id().as_bytes()
            || self.heap_id != *cap.heap_id().as_bytes()
            || self.object_kind != kind as u8
            || &self.object_id != object_id
        {
            return Err(Error::PermissionDenied(
                "cursor heap/object mismatch".into(),
            ));
        }
        let expect = cursor_mac(
            &self.capability_id,
            &self.heap_id,
            self.object_kind,
            &self.object_id,
            &self.position,
            cap.security_revision().get(),
        );
        if expect != self.mac {
            return Err(Error::PermissionDenied("cursor mac invalid".into()));
        }
        Ok(())
    }

    /// Opaque position bytes.
    pub fn position(&self) -> &[u8] {
        &self.position
    }
}

fn cursor_mac(
    capability_id: &[u8; 16],
    heap_id: &[u8; 16],
    object_kind: u8,
    object_id: &[u8; 16],
    position: &[u8],
    security_revision: u64,
) -> [u8; 32] {
    let mut h = Hasher::new();
    h.update(b"RESIDIUUM-HEAP-CURSOR-V1");
    h.update(&[0u8]);
    h.update(capability_id);
    h.update(heap_id);
    h.update(&[object_kind]);
    h.update(object_id);
    h.update(&security_revision.to_be_bytes());
    h.update(position);
    *h.finalize().as_bytes()
}

/// Pooled connection tagged with a heap capability instance.
pub struct HeapConnection {
    cap: HeapCap,
    token: ConnectionToken,
    pool: Arc<Mutex<HeapPoolInner>>,
}

impl HeapConnection {
    /// Owning capability.
    pub fn capability(&self) -> &HeapCap {
        &self.cap
    }

    /// Use this connection only with a matching heap handle.
    pub fn require_heap(&self, heap: &Heap) -> Result<(), Error> {
        if self.cap.same_instance(&heap.cap)
            && self.token.capability_id == *heap.cap.capability_id().as_bytes()
        {
            Ok(())
        } else {
            Err(Error::PermissionDenied(
                "pooled connection heap mismatch".into(),
            ))
        }
    }
}

impl Drop for HeapConnection {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.pool.lock() {
            guard.idle.push_back(self.token.clone());
        }
    }
}

#[derive(Clone)]
struct ConnectionToken {
    capability_id: [u8; 16],
    #[allow(dead_code)]
    minted_unix_s: u64,
}

impl ConnectionToken {
    fn new(cap: &HeapCap) -> Self {
        Self {
            capability_id: *cap.capability_id().as_bytes(),
            minted_unix_s: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        }
    }
}

#[derive(Default)]
struct HeapPoolInner {
    idle: VecDeque<ConnectionToken>,
}

/// Batch of collection members that must share one capability instance.
pub struct HeapBatch {
    cap: HeapCap,
    members: Vec<CollectionId>,
}

impl HeapBatch {
    /// Add a collection handle; rejects foreign heap instances.
    pub fn add_collection(&mut self, collection: &HeapCollection) -> Result<(), Error> {
        if !self.cap.same_instance(&collection.cap) {
            return Err(Error::PermissionDenied("batch member heap mismatch".into()));
        }
        self.members.push(collection.id);
        Ok(())
    }

    /// Member collection ids.
    pub fn members(&self) -> &[CollectionId] {
        &self.members
    }
}

fn lookup_named(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    kind: ObjectKind,
    name: &str,
) -> Result<([u8; 16], String), Error> {
    let entries = match kind {
        ObjectKind::Collection => try_load_collections_catalog(layout, heap_id)?,
        ObjectKind::Stream => try_load_streams_catalog(layout, heap_id)?,
    };
    if let Some(entries) = entries {
        for e in entries {
            if e.name == name || e.aliases.iter().any(|a| a == name) {
                return Ok((e.object_id, e.name));
            }
        }
    }
    Err(Error::ValidationMsg(format!(
        "heap object name not found: {name}"
    )))
}

#[cfg(test)]
mod decoded_cache_tests {
    use super::*;
    use serde_json::json;

    fn key(name: &str) -> DecodedJsonKey {
        DecodedJsonKey {
            heap_id: [1; 16],
            collection_id: [2; 16],
            key: name.to_string(),
        }
    }

    #[test]
    fn decoded_cache_is_version_keyed_clone_isolated_and_bounded() {
        let mut cache = DecodedJsonCache::new(160);
        let first_key = key("a");
        cache.insert(first_key.clone(), [3; 16], Arc::new(json!({"v": 1})), 4);
        let mut returned = cache.get(&first_key, &[3; 16]).unwrap().as_ref().clone();
        returned["v"] = json!(99);
        assert_eq!(
            cache.get(&first_key, &[3; 16]).unwrap().as_ref(),
            &json!({"v": 1})
        );
        assert!(cache.get(&first_key, &[4; 16]).is_none());

        cache.insert(first_key.clone(), [4; 16], Arc::new(json!({"v": 2})), 4);
        assert!(cache.get(&first_key, &[3; 16]).is_none());
        assert_eq!(
            cache.get(&first_key, &[4; 16]).unwrap().as_ref(),
            &json!({"v": 2})
        );

        let second_key = key("b");
        cache.insert(second_key.clone(), [5; 16], Arc::new(json!({"v": 3})), 4);
        assert!(cache.resident_charge <= cache.max_charge);
        assert!(cache.get(&first_key, &[4; 16]).is_none());
        assert_eq!(
            cache.get(&second_key, &[5; 16]).unwrap().as_ref(),
            &json!({"v": 3})
        );
        let stats = cache.stats();
        assert_eq!(stats.hits, 4);
        assert_eq!(stats.misses, 3);
        assert_eq!(stats.entries, 1);
        assert!(stats.resident_charge <= stats.max_charge);
    }

    #[test]
    fn projected_cache_is_version_and_exact_field_list_fenced_and_bounded() {
        let mut cache = DecodedJsonCache::new(1_024);
        let fields: Arc<[String]> = vec!["region".to_string()].into();
        let first = ProjectedJsonKey {
            document: key("a"),
            fields_hash: projection_fields_hash(&fields),
        };
        cache.insert_projection(
            first.clone(),
            [3; 16],
            Arc::clone(&fields),
            vec![Some(json!("us"))].into(),
            1_000,
        );
        assert_eq!(
            cache
                .get_projection_raw(
                    &[1; 16],
                    &[2; 16],
                    "a",
                    &first.fields_hash,
                    &[3; 16],
                    &fields
                )
                .unwrap()
                .0
                .as_ref(),
            &[Some(json!("us"))]
        );
        assert!(cache
            .get_projection_raw(
                &[1; 16],
                &[2; 16],
                "a",
                &first.fields_hash,
                &[4; 16],
                &fields
            )
            .is_none());
        assert!(cache
            .get_projection_raw(
                &[1; 16],
                &[2; 16],
                "a",
                &first.fields_hash,
                &[3; 16],
                &["status".to_string()],
            )
            .is_none());

        let second = ProjectedJsonKey {
            document: key("b"),
            fields_hash: first.fields_hash,
        };
        cache.insert_projection(
            second.clone(),
            [5; 16],
            fields,
            vec![Some(json!("eu"))].into(),
            1_000,
        );
        assert!(cache.projected_resident_charge <= cache.projected_max_charge);
        assert!(!cache.projected_entries.contains_key(&first));
        assert!(cache.projected_entries.contains_key(&second));
    }
}
