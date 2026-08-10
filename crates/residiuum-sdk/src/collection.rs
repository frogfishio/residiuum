//! Named collection handle (DX_SPEC §5.2, §6, §7, §8, §10).

use crate::error::Error;
use crate::filter::{compare_field, Filter, QueryBuilder, QueryOptions};
use crate::history::KeyHistory;
use crate::indexes::{mark_indexes_stale, try_index_lookup, IndexInfo, Indexes};
use crate::receipt::{DeleteReceipt, PutOptions, WriteReceipt};
use crate::residiuum::Backend;
use crate::resource::{
    check_json_depth, check_payload_len, check_result_bytes, estimate_row_bytes, host_limits,
};
use crate::sda_query::eval_sda_program;
use crate::subject::{collection_prefix, decode_subject, encode_subject};
use crate::value::{decode_bytes, decode_json, encode_bytes, encode_json};
use residiuum_store::{PayloadResult, Store};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::str;

/// A lazy handle to a named collection within a [`crate::Residiuum`] database.
///
/// Merely constructing a collection does not touch disk. The first write
/// creates store-level subject entries; the collection catalog is derived
/// (Stage 6) and rebuildable.
pub struct Collection<'a> {
    backend: &'a mut Backend,
    name: String,
}

impl<'a> Collection<'a> {
    pub(crate) fn new(backend: &'a mut Backend, name: String) -> Self {
        Self { backend, name }
    }

    /// Collection name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Durable 16-byte store identity for dialect HostCapabilities binding (Q0.A6).
    fn durable_store_id_bytes(&mut self) -> Result<[u8; 16], Error> {
        let id = match self.backend {
            Backend::Local(s) => s.store_id(),
            Backend::Remote(c) => c.store_id(),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => {
                return Err(Error::RemoteUnsupported(
                    "portable dialect durable store id (cluster)",
                ));
            }
        };
        if id == [0u8; 16] {
            return Err(Error::QueryInvalid(
                "portable dialect requires non-zero durable store_id (Q0.A6)".into(),
            ));
        }
        Ok(id)
    }

    fn local_store(&mut self) -> Result<&Store, Error> {
        match self.backend {
            Backend::Local(s) => Ok(s),
            Backend::Remote(_) => Err(Error::RemoteUnsupported("local store access")),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Err(Error::RemoteUnsupported("local store access")),
        }
    }

    /// Secondary index management (DX_SPEC §8). Embedded and remote.
    pub fn indexes(&mut self) -> Result<Indexes<'_>, Error> {
        let collection = self.name.clone();
        Ok(Indexes {
            backend: self.backend,
            collection,
        })
    }

    /// Put (create-or-replace) a JSON-serializable value under `key`.
    ///
    /// Default durability is [`residiuum_store::DurabilityMode::Durable`].
    pub fn put<T: Serialize>(&mut self, key: &str, value: &T) -> Result<WriteReceipt, Error> {
        self.put_with(key, value, PutOptions::default())
    }

    /// Put with explicit options.
    pub fn put_with<T: Serialize>(
        &mut self,
        key: &str,
        value: &T,
        options: PutOptions,
    ) -> Result<WriteReceipt, Error> {
        let json = serde_json::to_value(value)?;
        // DEF-029: hard host limits on JSON depth and payload size.
        let limits = host_limits();
        check_json_depth(&json, &limits)?;
        match self.backend {
            Backend::Local(store) => {
                let subject = encode_subject(&self.name, key)?;
                let body = encode_json(&json)?;
                check_payload_len(body.len(), &limits)?;
                let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
                let receipt = store.put(subject_str, &body, options.durability)?;
                // DEF-027: surface stale-marking failures (health / diagnostics).
                mark_indexes_stale(store, &self.name)?;
                Ok(WriteReceipt::from_store(key.to_string(), receipt))
            }
            Backend::Remote(client) => client.put_json(&self.name, key, &json, options),
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => c.put_json(&self.name, key, &json, options),
        }
    }

    /// Get the current JSON value for `key`, if present.
    ///
    /// Returns `Ok(None)` only when absence is established for this key.
    pub fn get(&mut self, key: &str) -> Result<Option<JsonValue>, Error> {
        match self.backend {
            Backend::Local(store) => {
                let subject = encode_subject(&self.name, key)?;
                let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
                match store.get(subject_str)? {
                    None => Ok(None),
                    Some(body) => Ok(Some(decode_json(&body)?)),
                }
            }
            Backend::Remote(client) => client.get_json(&self.name, key),
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => c.get_json(&self.name, key),
        }
    }

    /// Get payload with explicit completeness (chunked values).
    ///
    /// Returns partial / unavailable / conflicting maps without silent fill.
    /// Works over embedded and remote (`residiuum://`) backends.
    pub fn get_payload(&mut self, key: &str) -> Result<Option<PayloadResult>, Error> {
        match self.backend {
            Backend::Local(store) => {
                let subject = encode_subject(&self.name, key)?;
                let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
                Ok(store.get_payload(subject_str)?)
            }
            Backend::Remote(client) => client.get_payload(&self.name, key),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => {
                Err(Error::RemoteUnsupported("get_payload on cluster (use get)"))
            }
        }
    }

    /// Get and deserialize into `T`.
    pub fn get_as<T: serde::de::DeserializeOwned>(
        &mut self,
        key: &str,
    ) -> Result<Option<T>, Error> {
        match self.get(key)? {
            None => Ok(None),
            Some(v) => Ok(Some(serde_json::from_value(v)?)),
        }
    }

    /// Put opaque bytes under `key` (DX_SPEC §6.9).
    pub fn put_bytes(&mut self, key: &str, bytes: &[u8]) -> Result<WriteReceipt, Error> {
        self.put_bytes_with(key, bytes, PutOptions::default())
    }

    /// Put opaque bytes with explicit options.
    pub fn put_bytes_with(
        &mut self,
        key: &str,
        bytes: &[u8],
        options: PutOptions,
    ) -> Result<WriteReceipt, Error> {
        let limits = host_limits();
        // Typed body = tag + payload; bound the full on-disk body size.
        check_payload_len(bytes.len().saturating_add(1), &limits)?;
        match self.backend {
            Backend::Local(store) => {
                let subject = encode_subject(&self.name, key)?;
                let body = encode_bytes(bytes);
                let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
                let receipt = store.put(subject_str, &body, options.durability)?;
                mark_indexes_stale(store, &self.name)?;
                Ok(WriteReceipt::from_store(key.to_string(), receipt))
            }
            Backend::Remote(client) => client.put_bytes(&self.name, key, bytes, options),
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => c.put_bytes(&self.name, key, bytes, options),
        }
    }

    /// Get opaque bytes for `key`, if present.
    pub fn get_bytes(&mut self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        match self.backend {
            Backend::Local(store) => {
                let subject = encode_subject(&self.name, key)?;
                let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
                match store.get(subject_str)? {
                    None => Ok(None),
                    Some(body) => Ok(Some(decode_bytes(&body)?)),
                }
            }
            Backend::Remote(client) => client.get_bytes(&self.name, key),
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => c.get_bytes(&self.name, key),
        }
    }

    /// Delete the current value for `key` (tombstone; history retained).
    ///
    /// Deleting an absent key is idempotent; [`DeleteReceipt::removed`] reports
    /// whether a visible value changed.
    pub fn delete(&mut self, key: &str) -> Result<DeleteReceipt, Error> {
        self.delete_with(key, PutOptions::default())
    }

    /// Delete with explicit durability options.
    pub fn delete_with(&mut self, key: &str, options: PutOptions) -> Result<DeleteReceipt, Error> {
        match self.backend {
            Backend::Local(store) => {
                let subject = encode_subject(&self.name, key)?;
                let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
                let removed = store.get(subject_str)?.is_some();
                let receipt = store.delete(subject_str, options.durability)?;
                mark_indexes_stale(store, &self.name)?;
                Ok(DeleteReceipt::from_store(key.to_string(), removed, receipt))
            }
            Backend::Remote(client) => client.delete(&self.name, key, options),
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => c.delete(&self.name, key, options),
        }
    }

    /// Immutable event history for `key` (DX_SPEC §10.1). Embedded, remote, cluster.
    pub fn history(&mut self, key: &str) -> Result<KeyHistory, Error> {
        match self.backend {
            Backend::Local(store) => {
                let subject = encode_subject(&self.name, key)?;
                let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
                let hist = store.history(subject_str)?;
                KeyHistory::from_store(key.to_string(), hist)
            }
            Backend::Remote(client) => client.history(&self.name, key),
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => c.history(&self.name, key),
        }
    }

    /// Reconstruct the logical value of one exact historical event (DEF-099).
    ///
    /// `event_id` is the 16-byte authoritative event id (not a history index).
    /// Chunked puts reassemble via DEF-098. Ordinary [`Self::get`] never falls
    /// back to this path. Embedded only.
    pub fn get_version(
        &mut self,
        key: &str,
        event_id: &[u8; 16],
    ) -> Result<residiuum_store::VersionedPayloadResult, Error> {
        let subject = encode_subject(&self.name, key)?;
        let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
        let store = self
            .local_store()
            .map_err(|_| Error::RemoteUnsupported("get_version (embedded only)"))?;
        Ok(store.get_payload_version(
            subject_str,
            event_id,
            residiuum_store::ReadBudget::default(),
        )?)
    }

    /// Find the newest complete put strictly before the current generation (DEF-099).
    ///
    /// Default: stops at the first delete tombstone. Pass `cross_tombstone: true`
    /// for forensic search across deletes. Embedded only. Read-only.
    pub fn find_last_complete(
        &mut self,
        key: &str,
        options: residiuum_store::RecoveryReadOptions,
    ) -> Result<residiuum_store::HistoricalSearchResult, Error> {
        let subject = encode_subject(&self.name, key)?;
        let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
        let store = self
            .local_store()
            .map_err(|_| Error::RemoteUnsupported("find_last_complete (embedded only)"))?;
        Ok(store.find_last_complete_version(
            subject_str,
            residiuum_store::BeforeEvent::Current,
            options,
        )?)
    }

    /// Scan live keys in this collection (deterministic key order).
    ///
    /// Returns application keys only. Payload access is via get / get_bytes.
    ///
    /// **DEF-100:** drains [`Self::scan_keys_page`] and returns a complete list
    /// only when key-bearing coverage is complete. Incomplete coverage yields
    /// [`Error::CoverageIncomplete`] (never a silent partial `[]`).
    pub fn scan_keys(&mut self) -> Result<Vec<String>, Error> {
        match self.backend {
            Backend::Local(_) => {
                let mut keys = Vec::new();
                let mut cont: Option<Vec<u8>> = None;
                let page_size = residiuum_store::DEFAULT_PAGE_SIZE;
                loop {
                    let page = self.scan_keys_page(page_size, cont.as_deref())?;
                    keys.extend(page.keys);
                    if !page.has_more {
                        if !page.coverage_complete {
                            return Err(Error::CoverageIncomplete(format!(
                                "key coverage incomplete after scan ({} gap(s)); use scan_keys_page for partial key lists",
                                page.coverage_gaps.len()
                            )));
                        }
                        return Ok(keys);
                    }
                    cont = page.continuation;
                }
            }
            Backend::Remote(client) => client.list_keys(&self.name),
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => c.scan_keys(&self.name),
        }
    }

    /// One page of live keys with explicit coverage (DEF-100). Embedded only.
    ///
    /// Does **not** resolve document bodies. Body damage cannot suppress a key.
    pub fn scan_keys_page(
        &mut self,
        page_size: usize,
        continuation: Option<&[u8]>,
    ) -> Result<KeyScanPage, Error> {
        let prefix = collection_prefix(&self.name)?;
        let name = self.name.clone();
        let store = self
            .local_store()
            .map_err(|_| Error::RemoteUnsupported("scan_keys_page (embedded only)"))?;
        scan_keys_page_on_store(store, &name, &prefix, page_size, continuation)
    }

    /// One page of live JSON documents with per-key incompleteness (DEF-100).
    ///
    /// Healthy rows stream alongside damaged/undecodable keys. Prefer this over
    /// [`Self::scan_json_page`] when partial results are useful. Embedded only.
    pub fn scan_json_partial_page(
        &mut self,
        page_size: usize,
        continuation: Option<&[u8]>,
    ) -> Result<DocumentScanPage, Error> {
        let prefix = collection_prefix(&self.name)?;
        let name = self.name.clone();
        let store = self
            .local_store()
            .map_err(|_| Error::RemoteUnsupported("scan_json_partial_page (embedded only)"))?;
        scan_json_partial_page_on_store(store, &name, &prefix, page_size, continuation)
    }

    /// Scan live JSON entries `(key, value)` in this collection.
    ///
    /// Entries stored as bytes (not JSON) yield [`Error::TypeMismatch`] for that
    /// key and abort the scan. Prefer [`Self::scan_keys`] + typed gets when mixed.
    pub fn scan_json(&mut self) -> Result<Vec<(String, JsonValue)>, Error> {
        self.scan_json_filtered(&Filter::Always, &QueryOptions::default())
    }

    /// Stream live JSON entries one at a time (DX journey 6 / Stage 4b).
    ///
    /// Yields `(key, value)` in stable key order. Incomplete chunked payloads
    /// cause [`Error::CoverageIncomplete`] (DEF-012 fail-closed); use
    /// [`residiuum_store::Store::scan_live_page`] / `scan_live_logical` for partial maps.
    /// Embedded only.
    ///
    /// **DEF-026:** pages through the store with a bounded page size (default
    /// [`residiuum_store::DEFAULT_PAGE_SIZE`]) instead of materializing the full
    /// live set up front. The returned iterator holds a borrow of the local
    /// store for the duration of the scan.
    pub fn scan_json_iter(&mut self) -> Result<JsonScanIter<'_>, Error> {
        let page_size = residiuum_store::DEFAULT_PAGE_SIZE;
        self.scan_json_iter_paged(page_size)
    }

    /// Like [`Self::scan_json_iter`] with an explicit page size (DEF-026).
    pub fn scan_json_iter_paged(&mut self, page_size: usize) -> Result<JsonScanIter<'_>, Error> {
        let prefix = collection_prefix(&self.name)?;
        let name = self.name.clone();
        // Validate embedded-only before constructing the iterator.
        let _ = self
            .local_store()
            .map_err(|_| Error::RemoteUnsupported("scan_json_iter (embedded only)"))?;
        let store = match self.backend {
            Backend::Local(s) => &*s,
            Backend::Remote(_) => unreachable!("checked above"),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => unreachable!("checked above"),
        };
        Ok(JsonScanIter {
            store,
            collection: name,
            prefix,
            page_size: page_size.clamp(1, residiuum_store::MAX_PAGE_SIZE),
            continuation: None,
            buffer: std::collections::VecDeque::new(),
            done: false,
            started: false,
        })
    }

    /// Fetch one page of live JSON rows with an optional continuation (DEF-026).
    ///
    /// Embedded only. Tokens are opaque, store-bound, and generation-fenced.
    /// Tampered or cross-store tokens fail; concurrent mutations that change
    /// the scan generation fail with a consistency error so the client restarts.
    pub fn scan_json_page(
        &mut self,
        page_size: usize,
        continuation: Option<&[u8]>,
    ) -> Result<JsonScanPage, Error> {
        let prefix = collection_prefix(&self.name)?;
        let name = self.name.clone();
        let store = self
            .local_store()
            .map_err(|_| Error::RemoteUnsupported("scan_json_page (embedded only)"))?;
        scan_json_page_on_store(store, &name, &prefix, page_size, continuation)
    }

    /// Find JSON documents matching `filter` (DX_SPEC §7.1).
    ///
    /// Uses a secondary index when one is ready and applicable; otherwise scans.
    /// Unbounded materialization in stable key order. Prefer
    /// [`Self::find_with`] with a limit for large collections.
    pub fn find(&mut self, filter: &Filter) -> Result<Vec<(String, JsonValue)>, Error> {
        self.find_with(filter, QueryOptions::default())
    }

    /// Find with limit / order / budget options.
    pub fn find_with(
        &mut self,
        filter: &Filter,
        options: QueryOptions,
    ) -> Result<Vec<(String, JsonValue)>, Error> {
        self.scan_json_filtered(filter, &options)
    }

    /// Cluster-capable find that returns matches **and** coverage (Stage 8e).
    ///
    /// Requires the `cluster` feature. Embedded and remote backends return
    /// complete coverage (single-node / single-store scope). On cluster
    /// backends, incomplete coverage is always reported even when
    /// `allow_partial_coverage` is set.
    #[cfg(feature = "cluster")]
    pub fn find_with_coverage(
        &mut self,
        filter: &Filter,
        options: QueryOptions,
    ) -> Result<crate::ClusterFindResult, Error> {
        match self.backend {
            Backend::Cluster(c) => c.find_with_coverage(&self.name, filter, &options),
            Backend::Local(_) | Backend::Remote(_) => {
                let rows = self.scan_json_filtered(filter, &options)?;
                Ok(crate::ClusterFindResult {
                    rows,
                    coverage: residiuum_cluster::Coverage::default(),
                    query_id: "local-or-remote".into(),
                    truncated: false,
                    has_more: false,
                    continuation: None,
                })
            }
        }
    }

    /// Parse a DX/Mongo-style object filter and find matching documents.
    pub fn find_json(&mut self, filter_obj: &JsonValue) -> Result<Vec<(String, JsonValue)>, Error> {
        let filter = Filter::from_json(filter_obj)?;
        self.find(&filter)
    }

    /// Find via a **query dialect** (`doc/SDA/DIALECTS.md`).
    ///
    /// Builtin dialect ids: `json` / `mongo` (object filter text) and `sql`
    /// (`SELECT` / `WHERE` mimicry) compile to a portable filter and execute
    /// on the Query VM (**RQL-DQ1**) — not SDA. `sda` remains pure
    /// predicate/program text, executed via [`Self::filter_sda_with`] /
    /// [`Self::sda_with`]. Dialect id `rql` is **retired** on this surface
    /// (RQL-R1) — use [`crate::app_v1::CollectionClient::rql`] / Query VM
    /// instead.
    ///
    /// Program-shaped `sda` dialects materialise the collection as a JSON
    /// array under `input` and return the SDA result as a single synthetic
    /// row key `"$result"`.
    ///
    /// Pure SDA remains the mathematical language; dialects are comfortable
    /// imperfect frontends. Official RQL is **not** a dialect→SDA peer.
    pub fn find_dialect(
        &mut self,
        dialect: &str,
        source: &str,
    ) -> Result<Vec<(String, JsonValue)>, Error> {
        self.find_dialect_with(dialect, source, QueryOptions::default())
    }

    /// Like [`Self::find_dialect`] with scan options.
    pub fn find_dialect_with(
        &mut self,
        dialect: &str,
        source: &str,
        options: QueryOptions,
    ) -> Result<Vec<(String, JsonValue)>, Error> {
        let compiled = crate::dialects::compile_dialect(dialect, source)?;
        match compiled {
            crate::dialects::CompiledDialect::Sda(sda) => match sda.shape {
                crate::dialects::SdaShape::DocumentPredicate => {
                    self.filter_sda_with(&sda.sda, options)
                }
                crate::dialects::SdaShape::Program => {
                    let value = self.sda_with(&sda.sda, options)?;
                    Ok(vec![("$result".into(), value)])
                }
            },
            crate::dialects::CompiledDialect::Portable(portable) => {
                self.find_portable_with(&portable, options)
            }
        }
    }

    /// Execute a portable-filter dialect compile via Query VM (**RQL-DQ1** / **Q0.A6**).
    ///
    /// Host identity for the plan/cursor is **store-scoped durable**:
    /// - `HeapId` bytes = durable [`Store::store_id`] / remote store id (not a name hash)
    /// - `CollectionId` = domain-separated id over `(store_id, collection name)`
    ///
    /// This is **not** a Heap-catalog collection UUID. Product Heap query
    /// ([`crate::app_v1::CollectionClient::rql`]) binds the real
    /// [`CollectionClient::id`] / [`CollectionClient::heap_id`]. Frontend
    /// equivalence claims that require Heap catalog identity must use that path.
    ///
    /// Name-only synthetic ids (v1) are **removed** so two stores no longer
    /// share dialect host ids for the same collection name.
    fn find_portable_with(
        &mut self,
        compiled: &crate::dialects::CompiledPortable,
        options: QueryOptions,
    ) -> Result<Vec<(String, JsonValue)>, Error> {
        use crate::app_v1::{QueryBudget as RunBudget, QueryRunOptions};
        use crate::plan_v1::{CollectionBindings, PlanBuilder, MAX_PAGE_SIZE};
        use crate::query_bytecode_v1::{execute_bytecode, HostCapabilities, QueryBytecodeV1};
        use residiuum_heap::{CollectionId, HeapId};

        let predicate = compiled.filter.to_predicate()?;
        let store_id = self.durable_store_id_bytes()?;
        // Shape durable store_id into HeapId/CollectionId UUID constraints (v4/RFC).
        let heap_id = HeapId::from_bytes(uuid_v4_shape(store_id))
            .map_err(|e| Error::QueryInvalid(format!("dialect heap id from store_id: {e}")))?;
        let collection_id =
            CollectionId::from_bytes(dialect_collection_id_bytes(store_id, &self.name))
                .map_err(|e| Error::QueryInvalid(format!("dialect collection id: {e}")))?;

        let mut bindings = CollectionBindings::default();
        bindings.bind(&self.name, collection_id);
        let mut builder = PlanBuilder::from_source(&self.name).where_(predicate);
        if let Some(cols) = &compiled.project {
            builder = builder.project(cols)?;
        }
        if let Some((ref field, order)) = options.order_by {
            use crate::filter::SortOrder;
            use crate::plan_v1::OrderDir;
            let dir = match order {
                SortOrder::Asc => OrderDir::Asc,
                SortOrder::Desc => OrderDir::Desc,
            };
            builder = builder.order_by(field, dir)?;
        }
        builder = builder.coverage(if options.allow_partial_coverage {
            crate::app_v1::CoveragePolicy::IncompleteAllowed
        } else {
            crate::app_v1::CoveragePolicy::Complete
        });
        let mut page_size: Option<u32> = None;
        if let Some(n) = options.limit {
            builder = builder.limit(n as u64);
            page_size = Some((n as u32).clamp(1, MAX_PAGE_SIZE));
        }
        let plan = builder.compile(&bindings)?;
        let bytecode = QueryBytecodeV1::from_core_plan_force_scan(plan, None, options.force_scan)?;

        let mut run_options = QueryRunOptions::default();
        run_options.page_size = page_size;
        run_options.cancel = options.cancel.clone();
        if let Some(b) = &options.budget {
            run_options.budget = Some(RunBudget {
                max_documents: b.max_docs_scanned.map(|n| n as u64),
                max_bytes: b.max_bytes_scanned,
                max_result_bytes: b.max_result_bytes,
            });
        }

        struct DialectHost<'c, 'a> {
            collection: &'c mut Collection<'a>,
            bound_id: CollectionId,
            keys: Option<Vec<String>>,
        }
        impl<'c, 'a> DialectHost<'c, 'a> {
            fn check_id(&self, collection_id: CollectionId) -> Result<(), Error> {
                if collection_id != self.bound_id {
                    return Err(Error::QueryInvalid(format!(
                        "dialect host: collection_id mismatch (got {collection_id}, bound {})",
                        self.bound_id
                    )));
                }
                Ok(())
            }
            fn sorted_keys(&mut self) -> Result<&[String], Error> {
                if self.keys.is_none() {
                    let mut keys = self.collection.scan_keys()?;
                    keys.sort();
                    self.keys = Some(keys);
                }
                Ok(self.keys.as_deref().unwrap())
            }
        }
        impl HostCapabilities for DialectHost<'_, '_> {
            fn list_keys(
                &mut self,
                collection_id: CollectionId,
                limit: Option<usize>,
                after_key: Option<&str>,
            ) -> Result<Vec<String>, Error> {
                self.check_id(collection_id)?;
                let keys = self.sorted_keys()?;
                let start = match after_key {
                    Some(a) => keys.partition_point(|k| k.as_str() <= a),
                    None => 0,
                };
                let mut out = keys[start..].to_vec();
                if let Some(n) = limit {
                    out.truncate(n);
                }
                Ok(out)
            }

            fn get_json(
                &mut self,
                collection_id: CollectionId,
                key: &str,
            ) -> Result<Option<JsonValue>, Error> {
                self.check_id(collection_id)?;
                self.collection.get(key)
            }

            fn get_json_covered(
                &mut self,
                collection_id: CollectionId,
                key: &str,
            ) -> Result<crate::query_bytecode_v1::HostDocument, Error> {
                use crate::error::ErrorCode;
                use crate::query_bytecode_v1::HostDocument;

                self.check_id(collection_id)?;
                match self.collection.get(key) {
                    Ok(Some(value)) => Ok(HostDocument::Present {
                        value: value.into(),
                        logical_bytes: None,
                    }),
                    Ok(None) => Ok(HostDocument::Absent),
                    Err(error)
                        if matches!(
                            error.code(),
                            ErrorCode::DataDamaged
                                | ErrorCode::PayloadPartial
                                | ErrorCode::CoverageIncomplete
                                | ErrorCode::TypeMismatch
                        ) =>
                    {
                        Ok(HostDocument::Hole {
                            code: error.code().as_str().to_string(),
                        })
                    }
                    Err(error) => Err(error),
                }
            }
        }

        let mut host = DialectHost {
            collection: self,
            bound_id: collection_id,
            keys: None,
        };
        let mut out = Vec::new();
        loop {
            let page = execute_bytecode(
                &mut host,
                &bytecode,
                &std::collections::BTreeMap::new(),
                &run_options,
                heap_id,
                collection_id,
            )?;
            for row in page.rows {
                out.push((row.key, row.value));
            }
            if let Some(n) = options.limit {
                if out.len() >= n {
                    break;
                }
            }
            if page.exhausted || page.next.is_none() {
                break;
            }
            run_options.after = page.next;
        }
        if let Some(n) = options.limit {
            out.truncate(n);
        }
        Ok(out)
    }

    /// Fluent query builder (DX_SPEC §7.2).
    pub fn query(&mut self) -> QueryBuilder<'_, 'a> {
        QueryBuilder::new(self)
    }

    /// Evaluate pure SDA / ENR1 **text** over this collection (DX_SPEC §7.6).
    ///
    /// Scans live JSON documents (stable key order), binds them as a JSON array
    /// under `input`, and runs `program` with the standalone SDA evaluator
    /// (including the ENR1 kernel: `one?` / `one!` / `merge` / `+` / `asBag`).
    ///
    /// Portable [`Filter`] builders remain the everyday path; this is the
    /// advanced surface for people who prefer to write SDA/ENR as text.
    ///
    /// ```ignore
    /// let active = users.sda(r#"{
    ///   yield u | u in input
    ///     | getPath(u, Seq["status"]) = Some("active")
    /// }"#)?;
    /// ```
    ///
    /// The host performs the scan; SDA remains pure (no collection IO inside
    /// the program). Prefer [`Self::sda_with`] when a limit or budget is needed.
    pub fn sda(&mut self, program: &str) -> Result<JsonValue, Error> {
        self.sda_with(program, QueryOptions::default())
    }

    /// Like [`Self::sda`] with scan options (limit / budget / force_scan / …).
    pub fn sda_with(&mut self, program: &str, options: QueryOptions) -> Result<JsonValue, Error> {
        let rows = self.find_with(&Filter::Always, options)?;
        let docs: Vec<JsonValue> = rows.into_iter().map(|(_, v)| v).collect();
        eval_sda_program(program, JsonValue::Array(docs))
    }

    /// Keep live JSON rows whose pure SDA/ENR **text** predicate is `true`.
    ///
    /// `predicate` is evaluated once per document with that document bound as
    /// `input`. Non-boolean results (including SDA `Fail`) are non-matches.
    /// Prefer [`Filter`] + [`Self::find`] for the portable vocabulary; use this
    /// when the predicate needs full SDA/ENR surface.
    ///
    /// ```ignore
    /// let rows = users.filter_sda(
    ///     r#"getPath(input, Seq["status"]) = Some("active")"#,
    /// )?;
    /// ```
    pub fn filter_sda(&mut self, predicate: &str) -> Result<Vec<(String, JsonValue)>, Error> {
        self.filter_sda_with(predicate, QueryOptions::default())
    }

    /// Like [`Self::filter_sda`] with scan options.
    pub fn filter_sda_with(
        &mut self,
        predicate: &str,
        options: QueryOptions,
    ) -> Result<Vec<(String, JsonValue)>, Error> {
        let prog = sda_core::Program::parse(predicate).map_err(|e| {
            Error::QueryInvalid(format!("filter_sda parse failed: {e}; src={predicate}"))
        })?;
        let candidates = self.find_with(&Filter::Always, options)?;
        let mut out = Vec::new();
        for (key, doc) in candidates {
            match prog.run_json("input", doc.clone()) {
                Ok(JsonValue::Bool(true)) => out.push((key, doc)),
                Ok(JsonValue::Bool(false)) | Ok(_) => {}
                Err(e) => {
                    return Err(Error::QueryInvalid(format!(
                        "filter_sda eval failed: {e}; src={predicate}"
                    )));
                }
            }
        }
        Ok(out)
    }

    /// List indexes on this collection (convenience). Embedded and remote.
    pub fn list_indexes(&mut self) -> Result<Vec<IndexInfo>, Error> {
        self.indexes()?.list()
    }

    fn scan_json_filtered(
        &mut self,
        filter: &Filter,
        options: &QueryOptions,
    ) -> Result<Vec<(String, JsonValue)>, Error> {
        match self.backend {
            Backend::Remote(client) => client.find(&self.name, filter, options),
            Backend::Local(store) => find_on_store(store, &self.name, filter, options),
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => c.find(&self.name, filter, options),
        }
    }
}

/// Store-scoped durable collection id for DX portable dialect (Q0.A6).
///
/// Domain-separated over the durable store id + collection name. Stable across
/// reopens of the same store; does not collide across different stores that
/// happen to share a collection name. Not a Heap-catalog UUID.

/// Force RFC4122 version/variant bits so store-derived ids parse as HeapId/CollectionId.
fn uuid_v4_shape(mut bytes: [u8; 16]) -> [u8; 16] {
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes
}

fn dialect_collection_id_bytes(store_id: [u8; 16], name: &str) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"residiuum-dialect-collection-id-v2");
    hasher.update(&[0u8]);
    hasher.update(&store_id);
    hasher.update(&[0u8]);
    hasher.update(name.as_bytes());
    let hash = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC variant
    bytes
}

/// Run a filter query against an open store (index-accelerated when possible).
///
/// Shared by the embedded collection path and the remote server dispatch.
pub fn find_on_store(
    store: &Store,
    collection: &str,
    filter: &Filter,
    options: &QueryOptions,
) -> Result<Vec<(String, JsonValue)>, Error> {
    // DEF-012: offline tiers make ordinary complete find results dishonest.
    let tier_cov = store.tier_coverage();
    if tier_cov.is_incomplete() && !options.allow_partial_coverage {
        return Err(Error::CoverageIncomplete(
            "find refused: tier coverage incomplete (offline tiers / unavailable segments)"
                .to_string(),
        ));
    }

    // Try index acceleration when not force-scanning.
    if !options.force_scan {
        if let Some((info, subjects)) = try_index_lookup(store, collection, filter)? {
            if info.state.usable() {
                // DEF-012: index miss is authoritative only with a complete frontier.
                if subjects.is_empty() && !info.complete_coverage {
                    // Fall through to full scan — partial indexes cannot prove absence.
                } else {
                    return collect_from_subjects(store, collection, subjects, filter, options);
                }
            }
        }
    }

    let prefix = collection_prefix(collection)?;
    // DEF-026: page through live subjects; never materialize the full body set.
    // DEF-029: docs/bytes budgets, result memory, cooperative cancel.
    let limits = host_limits();
    let mut scanned = 0usize;
    let mut bytes_scanned = 0u64;
    let mut result_bytes = 0u64;
    let mut out = Vec::new();
    let mut cont: Option<Vec<u8>> = None;
    let page_size = options
        .limit
        .unwrap_or(residiuum_store::DEFAULT_PAGE_SIZE)
        .clamp(1, residiuum_store::MAX_PAGE_SIZE);
    loop {
        if let Some(token) = &options.cancel {
            token.check()?;
        }
        let page = scan_json_page_on_store(store, collection, &prefix, page_size, cont.as_deref())?;
        scanned = scanned.saturating_add(page.examined);
        bytes_scanned = bytes_scanned.saturating_add(page.bytes_examined);
        if let Some(stop) = budget_stop(
            options,
            scanned,
            bytes_scanned,
            "scan examined more than budget without a usable index; raise budget or create an index",
        )? {
            if stop {
                return finish_query(out, options);
            }
        }
        for (key, value) in page.rows {
            if !filter.matches(&value) {
                continue;
            }
            let row_bytes = estimate_row_bytes(&key, &value);
            result_bytes = result_bytes.saturating_add(row_bytes);
            let budget_cap = options.budget.as_ref().and_then(|b| b.max_result_bytes);
            check_result_bytes(result_bytes, budget_cap, &limits)?;
            out.push((key, value));
        }
        if page.complete {
            break;
        }
        cont = page.continuation;
        if cont.is_none() {
            break;
        }
        // Early exit when the caller only needs `limit` matches (still may over-scan).
        if let Some(lim) = options.limit {
            if out.len() >= lim && options.order_by.is_none() {
                break;
            }
        }
    }
    finish_query(out, options)
}

fn collect_from_subjects(
    store: &Store,
    collection: &str,
    subjects: Vec<Vec<u8>>,
    filter: &Filter,
    options: &QueryOptions,
) -> Result<Vec<(String, JsonValue)>, Error> {
    let limits = host_limits();
    let mut out = Vec::new();
    let mut scanned = 0usize;
    let mut bytes_scanned = 0u64;
    let mut result_bytes = 0u64;
    for subject in subjects {
        if let Some(token) = &options.cancel {
            token.check()?;
        }
        scanned += 1;
        if let Some(stop) = budget_stop(
            options,
            scanned,
            bytes_scanned,
            "index probe exceeded document/byte budget",
        )? {
            if stop {
                return finish_query(out, options);
            }
        }
        let subject_str = match str::from_utf8(&subject) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let Some(body) = store.get(subject_str)? else {
            continue;
        };
        bytes_scanned = bytes_scanned.saturating_add(body.len() as u64);
        if let Some(stop) = budget_stop(
            options,
            scanned,
            bytes_scanned,
            "index probe exceeded document/byte budget",
        )? {
            if stop {
                return finish_query(out, options);
            }
        }
        let value = decode_json(&body)?;
        if !filter.matches(&value) {
            continue;
        }
        let Some((coll, key)) = decode_subject(&subject) else {
            continue;
        };
        if coll != collection {
            continue;
        }
        let row_bytes = estimate_row_bytes(key, &value);
        result_bytes = result_bytes.saturating_add(row_bytes);
        let budget_cap = options.budget.as_ref().and_then(|b| b.max_result_bytes);
        check_result_bytes(result_bytes, budget_cap, &limits)?;
        out.push((key.to_string(), value));
    }
    finish_query(out, options)
}

/// Returns `Ok(Some(true))` when the caller should return partial matches,
/// `Ok(Some(false))` is unused, `Ok(None)` continue, `Err` hard budget stop.
fn budget_stop(
    options: &QueryOptions,
    scanned: usize,
    bytes_scanned: u64,
    context: &str,
) -> Result<Option<bool>, Error> {
    let Some(budget) = &options.budget else {
        return Ok(None);
    };
    let docs_hit = budget.max_docs_scanned.is_some_and(|max| scanned > max);
    let bytes_hit = budget
        .max_bytes_scanned
        .is_some_and(|max| bytes_scanned > max);
    if !docs_hit && !bytes_hit {
        return Ok(None);
    }
    if options.allow_partial_coverage {
        return Ok(Some(true));
    }
    let detail = match (budget.max_docs_scanned, budget.max_bytes_scanned) {
        (Some(d), Some(b)) if docs_hit && bytes_hit => {
            format!("docs>{d} and bytes>{b}")
        }
        (Some(d), _) if docs_hit => format!("docs examined {scanned} > {d}"),
        (_, Some(b)) if bytes_hit => format!("bytes examined {bytes_scanned} > {b}"),
        _ => format!("docs={scanned} bytes={bytes_scanned}"),
    };
    Err(Error::QueryBudgetRequired(format!("{context} ({detail})")))
}

/// One page of JSON scan results (DEF-026).
#[derive(Debug, Clone)]
pub struct JsonScanPage {
    /// Rows in stable key order for this page.
    pub rows: Vec<(String, JsonValue)>,
    /// Opaque continuation for the next page (`None` when complete).
    pub continuation: Option<Vec<u8>>,
    /// True when no further pages remain under the cursor consistency model.
    pub complete: bool,
    /// Subjects examined on this page (for budgets / diagnostics).
    pub examined: usize,
    /// Raw body bytes examined on this page (DEF-029 scan-byte budgets).
    pub bytes_examined: u64,
}

/// One page of collection keys with coverage honesty (DEF-100).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyScanPage {
    /// Application keys in stable order for this page.
    pub keys: Vec<String>,
    /// Opaque continuation for the next page (`None` when `has_more` is false).
    pub continuation: Option<Vec<u8>>,
    /// True when a further page may be fetched.
    pub has_more: bool,
    /// True only on the final page when key-bearing coverage is complete.
    pub coverage_complete: bool,
    /// Typed coverage gaps (empty when complete).
    pub coverage_gaps: Vec<String>,
    /// Subjects examined on this page.
    pub examined: usize,
}

/// A verified key whose body could not be fully read (DEF-100).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncompleteDocument {
    /// Application key.
    pub key: String,
    /// Completeness class: `partial`, `unavailable`, or `conflicting`.
    pub completeness: String,
    /// Stable detail label.
    pub detail: String,
}

/// A verified key whose body could not be decoded as the requested type (DEF-100).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndecodableDocument {
    /// Application key.
    pub key: String,
    /// Error class (e.g. `type_mismatch`, `invalid_json`).
    pub error_class: String,
}

/// Partial-aware document page (DEF-100).
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentScanPage {
    /// Healthy JSON rows in stable key order.
    pub rows: Vec<(String, JsonValue)>,
    /// Verified keys with incomplete payloads.
    pub incomplete: Vec<IncompleteDocument>,
    /// Verified keys with undecodable bodies (still listed, not aborted).
    pub undecodable: Vec<UndecodableDocument>,
    /// Opaque continuation for the next page.
    pub continuation: Option<Vec<u8>>,
    /// True when a further page may be fetched.
    pub has_more: bool,
    /// True only on the final page when key-bearing coverage is complete.
    pub key_coverage_complete: bool,
    /// Typed coverage gaps.
    pub coverage_gaps: Vec<String>,
    /// Subjects examined on this page.
    pub examined: usize,
    /// Raw body bytes examined for complete rows.
    pub bytes_examined: u64,
}

/// Streaming JSON scan over the embedded store (DEF-026).
///
/// Fetches bounded pages on demand; peak buffer is one page of decoded rows.
pub struct JsonScanIter<'a> {
    store: &'a Store,
    collection: String,
    prefix: Vec<u8>,
    page_size: usize,
    continuation: Option<Vec<u8>>,
    buffer: std::collections::VecDeque<(String, JsonValue)>,
    done: bool,
    started: bool,
}

impl Iterator for JsonScanIter<'_> {
    type Item = Result<(String, JsonValue), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.buffer.pop_front() {
                return Some(Ok(row));
            }
            if self.done {
                return None;
            }
            let cont = if self.started {
                self.continuation.as_deref()
            } else {
                None
            };
            self.started = true;
            match scan_json_page_on_store(
                self.store,
                &self.collection,
                &self.prefix,
                self.page_size,
                cont,
            ) {
                Ok(page) => {
                    self.continuation = page.continuation;
                    if page.complete || self.continuation.is_none() {
                        self.done = true;
                    }
                    if page.rows.is_empty() {
                        // Empty final page or only incompletes filtered out.
                        if self.done {
                            return None;
                        }
                        continue;
                    }
                    self.buffer.extend(page.rows);
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

fn scan_json_page_on_store(
    store: &Store,
    collection: &str,
    prefix: &[u8],
    page_size: usize,
    continuation: Option<&[u8]>,
) -> Result<JsonScanPage, Error> {
    use residiuum_store::LiveScanPageOptions;

    let mut opts = LiveScanPageOptions::new(page_size).prefix(prefix.to_vec());
    if let Some(tok) = continuation {
        opts = opts.continuation(tok.to_vec());
    }
    let page = store.scan_live_page(&opts)?;
    if !page.incomplete.is_empty() {
        return Err(Error::CoverageIncomplete(format!(
            "{} live subject(s) incomplete during paged scan; use scan_json_partial_page or store.scan_live_documents_page for partial maps",
            page.incomplete.len()
        )));
    }
    let mut rows = Vec::with_capacity(page.entries.len());
    let mut bytes_examined = 0u64;
    for (subject, body) in page.entries {
        bytes_examined = bytes_examined.saturating_add(body.len() as u64);
        if !subject.starts_with(prefix) {
            continue;
        }
        match decode_subject(&subject) {
            Some((coll, key)) if coll == collection => {
                rows.push((key.to_string(), decode_json(&body)?));
            }
            _ => continue,
        }
    }
    Ok(JsonScanPage {
        rows,
        continuation: page.continuation,
        complete: !page.has_more,
        examined: page.examined,
        bytes_examined,
    })
}

fn scan_keys_page_on_store(
    store: &Store,
    collection: &str,
    prefix: &[u8],
    page_size: usize,
    continuation: Option<&[u8]>,
) -> Result<KeyScanPage, Error> {
    use residiuum_store::LiveScanPageOptions;

    let mut opts = LiveScanPageOptions::new(page_size).prefix(prefix.to_vec());
    if let Some(tok) = continuation {
        opts = opts.continuation(tok.to_vec());
    }
    let page = store.scan_live_keys_page(&opts)?;
    let mut keys = Vec::with_capacity(page.keys.len());
    for subject in &page.keys {
        if !subject.starts_with(prefix) {
            continue;
        }
        match decode_subject(subject) {
            Some((coll, key)) if coll == collection => keys.push(key.to_string()),
            _ => continue,
        }
    }
    let coverage_gaps: Vec<String> = page
        .coverage_gaps
        .iter()
        .map(|g| format!("{:?}: {}", g.kind, g.detail))
        .collect();
    Ok(KeyScanPage {
        keys,
        continuation: page.continuation,
        has_more: page.has_more,
        coverage_complete: page.coverage_complete,
        coverage_gaps,
        examined: page.examined,
    })
}

fn scan_json_partial_page_on_store(
    store: &Store,
    collection: &str,
    prefix: &[u8],
    page_size: usize,
    continuation: Option<&[u8]>,
) -> Result<DocumentScanPage, Error> {
    use residiuum_store::{IncompleteReason, LiveScanPageOptions};

    let mut opts = LiveScanPageOptions::new(page_size).prefix(prefix.to_vec());
    if let Some(tok) = continuation {
        opts = opts.continuation(tok.to_vec());
    }
    let page = store.scan_live_documents_page(&opts)?;
    let mut rows = Vec::new();
    let mut undecodable = Vec::new();
    let mut bytes_examined = 0u64;

    for (subject, body) in page.rows {
        if !subject.starts_with(prefix) {
            continue;
        }
        match decode_subject(&subject) {
            Some((coll, key)) if coll == collection => {
                bytes_examined = bytes_examined.saturating_add(body.len() as u64);
                match decode_json(&body) {
                    Ok(v) => rows.push((key.to_string(), v)),
                    Err(e) => {
                        let error_class = match &e {
                            Error::TypeMismatch { .. } => "type_mismatch",
                            _ => "invalid_json",
                        };
                        undecodable.push(UndecodableDocument {
                            key: key.to_string(),
                            error_class: error_class.into(),
                        });
                    }
                }
            }
            _ => continue,
        }
    }

    let mut incomplete = Vec::new();
    for item in page.incomplete {
        if !item.subject.starts_with(prefix) {
            continue;
        }
        match decode_subject(&item.subject) {
            Some((coll, key)) if coll == collection => {
                let (completeness, detail) = match item.reason {
                    IncompleteReason::PayloadPartial => {
                        ("partial", "payload only partially available")
                    }
                    IncompleteReason::PayloadUnavailable => {
                        ("unavailable", "payload chunks unavailable")
                    }
                    IncompleteReason::PayloadConflict => {
                        ("conflicting", "conflicting chunk content")
                    }
                    IncompleteReason::NonUtf8Subject => ("unavailable", "non-utf8 subject"),
                    IncompleteReason::LocatorOffsetInvalid => {
                        ("locator_offset_invalid", "locator frame offset invalid")
                    }
                    IncompleteReason::LocatorFrameVerifyFailed => {
                        ("locator_frame_verify_failed", "locator frame verify failed")
                    }
                    IncompleteReason::LocatorSegmentIdMismatch => {
                        ("locator_segment_id_mismatch", "locator segment id mismatch")
                    }
                    IncompleteReason::SegmentNotFound => ("segment_not_found", "segment not found"),
                };
                incomplete.push(IncompleteDocument {
                    key: key.to_string(),
                    completeness: completeness.into(),
                    detail: detail.into(),
                });
            }
            _ => continue,
        }
    }

    let coverage_gaps: Vec<String> = page
        .coverage_gaps
        .iter()
        .map(|g| format!("{:?}: {}", g.kind, g.detail))
        .collect();

    Ok(DocumentScanPage {
        rows,
        incomplete,
        undecodable,
        continuation: page.continuation,
        has_more: page.has_more,
        key_coverage_complete: page.key_coverage_complete,
        coverage_gaps,
        examined: page.examined,
        bytes_examined,
    })
}

/// Shared by embedded find and the cluster backend (Stage 8d).
pub(crate) fn finish_query_pub(
    out: Vec<(String, JsonValue)>,
    options: &QueryOptions,
) -> Result<Vec<(String, JsonValue)>, Error> {
    finish_query(out, options)
}

fn finish_query(
    mut out: Vec<(String, JsonValue)>,
    options: &QueryOptions,
) -> Result<Vec<(String, JsonValue)>, Error> {
    if let Some(token) = &options.cancel {
        token.check()?;
    }
    // DEF-029: fail closed on oversized materialised sets before sort.
    // Spill-to-disk is not enabled in this profile.
    let limits = host_limits();
    let result_bytes: u64 = out
        .iter()
        .map(|(k, v)| estimate_row_bytes(k, v))
        .fold(0u64, u64::saturating_add);
    let budget_cap = options.budget.as_ref().and_then(|b| b.max_result_bytes);
    check_result_bytes(result_bytes, budget_cap, &limits)?;

    if let Some((ref field, order)) = options.order_by {
        out.sort_by(|a, b| {
            let cmp = compare_field(&a.1, &b.1, field, order);
            if cmp == std::cmp::Ordering::Equal {
                a.0.cmp(&b.0)
            } else {
                cmp
            }
        });
    } else {
        out.sort_by(|a, b| a.0.cmp(&b.0));
    }

    if let Some(limit) = options.limit {
        if out.len() > limit {
            out.truncate(limit);
        }
    }
    Ok(out)
}
