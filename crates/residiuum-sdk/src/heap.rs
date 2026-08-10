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
    create_collection_idempotent, rebuild_heap_entry_from_chain, rebuild_object_entry_from_chain,
    try_load_collections_catalog, try_load_streams_catalog, HeapMetaLayout, HeapStore, ObjectKind,
    StoreError, StoreHost,
};
use serde::Serialize;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Deployment-level host wrapping a capability-gated [`StoreHost`].
pub struct ResidiuumDeployment {
    host: StoreHost,
    data_root: PathBuf,
}

impl ResidiuumDeployment {
    /// Open an existing store directory as a deployment host (no raw data API).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            host: StoreHost::open(path)?,
            data_root: path.to_path_buf(),
        })
    }

    /// Create a new store directory as a deployment host.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            host: StoreHost::create(path)?,
            data_root: path.to_path_buf(),
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

    /// Durable group-commit counters from the shared physical deployment.
    pub fn operation_commit_stats(&self) -> residiuum_store::OperationCommitStats {
        self.host.operation_commit_stats()
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
        let heap_id = *self.cap.heap_id().as_bytes();
        let op_id = match operation_id {
            Some(id) => id,
            None => {
                let eid = CollectionId::new_random().map_err(|e| Error::Internal(e.to_string()))?;
                *eid.as_bytes()
            }
        };
        let admin = create_collection_idempotent(&self.layout, &heap_id, op_id, name)
            .map_err(map_create_object_err)?;
        let collection_id = CollectionId::from_bytes_unchecked_nonzero(admin.object_id)
            .map_err(|e| Error::Internal(e.to_string()))?;
        let collection = HeapCollection {
            cap: self.cap.clone(),
            store: Arc::clone(&self.store),
            id: collection_id,
            name_at_open: admin.name,
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
        match self.get_raw_body(key)? {
            None => Ok(None),
            Some(raw) => Ok(Some(crate::value::decode_json(&raw)?)),
        }
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
        self.store
            .get_collection_many(self.id.as_bytes(), &raw_keys)?
            .into_iter()
            .map(|(key, body, hole)| {
                let key = String::from_utf8(key)
                    .map_err(|_| Error::Internal("get-many: non-UTF-8 key".into()))?;
                let document = match (body, hole) {
                    (Some(body), None) => match crate::value::decode_json(&body) {
                        Ok(value) => HostDocument::Present(value),
                        Err(error) => HostDocument::Hole {
                            code: error.code().as_str().to_string(),
                        },
                    },
                    (None, Some(reason)) => HostDocument::Hole {
                        code: reason.as_str().to_string(),
                    },
                    (None, None) => HostDocument::Absent,
                    (Some(_), Some(_)) => {
                        return Err(Error::Internal(
                            "get-many returned body and hole for one key".into(),
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
