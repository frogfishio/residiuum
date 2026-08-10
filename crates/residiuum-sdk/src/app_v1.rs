//! APP-0 public Rust application surface (`residiuum-rust-app-v1`).
//!
//! These types freeze the **names and fields** from
//! `doc/todo/application-baseline/CORE_APPLICATION_API_IMPLEMENTATION_PLAN.md` §5 / §10.
//! Method bodies that require storage/wire activation land in APP-1…APP-8;
//! this module must compile so implementers share one contract.
//!
//! Normative companions: `spec/app/v1/`, `spec/heap/rpc-v1/collection_create.*`,
//! `spec/heap/rpc-v1/rql_query.*`.

use crate::error::{Error, ErrorCode};
use crate::filter::Filter;
use crate::heap::{Heap, HeapCollection};
use crate::history::{KeyHistory, Version};
use crate::indexes::IndexInfo;
use crate::plan_v1::{CollectionBindings, NullsOrder, OrderDir, PlanBuilder, RqlPlanV1};
use crate::predicate::Predicate;
use crate::receipt::{DeleteReceipt, PutOptions, WriteReceipt};
use crate::remote_heap::RemoteHeap;
use residiuum_heap::{CollectionId, HeapId};
use residiuum_store::DurabilityMode;
use residiuum_store::IndexState;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Profile label for the public Rust application façade.
pub const RUST_APP_PROFILE: &str = "residiuum-rust-app-v1";

/// RQL Application Core source profile (serialized value is frozen Class C).
pub const RQL_APP_CORE_PROFILE: &str = "rql-app-core-v1";

/// Canonical logical plan profile (serialized value is frozen Class C).
pub const RQL_PLAN_PROFILE: &str = "rql-plan-v1";

/// Authenticated continuation profile.
pub const CURSOR_PROFILE: &str = "residiuum-cursor-v1";

/// Shared predicate profile.
pub const PREDICATE_PROFILE: &str = "residiuum-predicate-v1";

/// Catalog listing entry for one collection in a Heap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionInfo {
    /// Owning Heap.
    pub heap_id: HeapId,
    /// Immutable collection identity.
    pub collection_id: CollectionId,
    /// Canonical display name.
    pub name: String,
    /// Descriptor content hash (32 bytes).
    pub descriptor_hash: [u8; 32],
}

/// Options for collection create (CORE plan §5 / §6).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateCollectionOptions {
    /// Stable client operation id (16 bytes). Generated when `None`.
    pub operation_id: Option<[u8; 16]>,
}

/// Successful create returns a bound handle plus receipt.
#[derive(Debug)]
pub struct CreateCollectionResult {
    /// Open handle for the new collection (identity only until APP-1 wires storage).
    pub collection: CollectionClient,
    /// Typed create receipt.
    pub receipt: CollectionCreateReceipt,
}

/// Result of [`CollectionClient::upsert`] (APB-2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertResult {
    /// True when the key was absent before this write (inserted vs replaced).
    pub inserted: bool,
    /// Write receipt for the mutation.
    pub receipt: WriteReceipt,
}

/// Options for version-conditional replace (APB-2 / PD-002).
///
/// `if_version` is the establishing event id of the live value
/// ([`WriteReceipt::version`] / [`WriteReceipt::event_id`], or history last
/// put `event_id`). Concurrent lost-update remains residual until store-level
/// Key Atomic CAS lands (this façade is read-then-write).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplaceOptions {
    /// Establishing event id that must still be current.
    pub if_version: [u8; 16],
}

/// Options for conditional delete (APB-2 / PD-002).
///
/// Same OCC token rules as [`ReplaceOptions`]. First cut is read-then-write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteWithOptions {
    /// When set, the live establishing event id must match.
    pub if_version: Option<[u8; 16]>,
    /// When `true`, absence yields [`Error::NotFound`]. When `false`, absence
    /// is idempotent (`removed: false`).
    pub if_present: bool,
}

impl Default for DeleteWithOptions {
    fn default() -> Self {
        Self {
            if_version: None,
            if_present: false,
        }
    }
}

/// Generated-key profile for [`CollectionClient::add`] (APB-2 / PD-004).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyProfile {
    /// Opaque random 16-byte key as lowercase hex (32 chars).
    ///
    /// Profile id: [`KEY_PROFILE_RANDOM_V1`]. Collision-safe under create+retry;
    /// **not** claimed sortable.
    #[default]
    RandomV1,
}

/// Frozen profile label for [`KeyProfile::RandomV1`].
pub const KEY_PROFILE_RANDOM_V1: &str = "residiuum-key-random-v1";

impl KeyProfile {
    /// Stable profile label (documented / product claim surface).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RandomV1 => KEY_PROFILE_RANDOM_V1,
        }
    }

    fn mint_key(self) -> Result<String, Error> {
        match self {
            Self::RandomV1 => {
                let id = residiuum_store::random_id().map_err(Error::from)?;
                Ok(residiuum_store::hex16(&id))
            }
        }
    }
}

/// Result of [`CollectionClient::add`] (APB-2 / PD-004).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddResult {
    /// Generated application key (stable opaque string).
    pub key: String,
    /// Profile that produced the key.
    pub key_profile: &'static str,
    /// Write receipt for the insert.
    pub receipt: WriteReceipt,
}

/// Max mint attempts when a generated key collides (astronomically rare).
const ADD_KEY_MINT_ATTEMPTS: usize = 8;

/// Receipt for `create_collection` (wire op 106).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionCreateReceipt {
    /// Stable receipt id (16 bytes).
    pub receipt_id: [u8; 16],
    /// Always create-collection for this receipt kind.
    pub operation: AdminOperation,
    /// Owning Heap.
    pub heap_id: HeapId,
    /// New collection id.
    pub collection_id: CollectionId,
    /// Descriptor hash at create.
    pub descriptor_hash: [u8; 32],
    /// Wall clock at create (source of truth for replay equality).
    pub created_at: SystemTime,
}

/// Admin operation discriminant on receipts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminOperation {
    /// Collection create.
    CreateCollection,
}

/// Opaque query id (16 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId(pub [u8; 16]);

/// Named RQL / builder parameters.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Parameters {
    /// Name → JSON value.
    pub values: BTreeMap<String, serde_json::Value>,
}

/// Options for `rql` / `explain_rql` / page execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRunOptions {
    /// Page size (1..=4096). Default 64 when `None`.
    pub page_size: Option<u32>,
    /// Coverage policy.
    pub coverage: CoveragePolicy,
    /// Consistency mode.
    pub consistency: ConsistencyMode,
    /// Optional budgets.
    pub budget: Option<QueryBudget>,
    /// When true, explain only (op 118 `explain: true`).
    pub explain: bool,
    /// Authenticated continuation from a prior page (APP-6).
    ///
    /// Preferred over source `after` clause (still rejected at compile until
    /// product continuation surface freezes).
    pub after: Option<Continuation>,
    /// Wall-clock budget from the start of this page execution (APB-7 T8).
    ///
    /// When elapsed time reaches this duration, the executor fails closed with
    /// [`Error::DeadlineExceeded`]. Checked cooperatively between scan steps.
    pub deadline: Option<Duration>,
    /// Cooperative cancellation (APB-7 T8). Shared via [`crate::CancelToken`].
    ///
    /// When cancelled, the executor fails closed with
    /// [`Error::ResourceLimit`] (`query cancelled`).
    pub cancel: Option<crate::resource::CancelToken>,
    /// Collect per-phase executor timings on the returned page.
    ///
    /// Disabled by default so production queries do not pay diagnostic clock
    /// reads. Timings are observational and never alter planning or execution.
    pub diagnostics: bool,
}

impl Default for QueryRunOptions {
    fn default() -> Self {
        Self {
            page_size: None,
            coverage: CoveragePolicy::Complete,
            consistency: ConsistencyMode::Available,
            budget: None,
            explain: false,
            after: None,
            deadline: None,
            cancel: None,
            diagnostics: false,
        }
    }
}

/// Opt-in wall-clock breakdown for one query-page execution.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryDiagnostics {
    /// Source parsing, semantic compilation, and parameter binding.
    pub compile_ns: u64,
    /// Plan-to-QVM lowering, encoding, and authority round-trip validation.
    pub lower_ns: u64,
    /// Product QVM decode before execution.
    pub decode_ns: u64,
    /// VM structural and semantic verification.
    pub verify_ns: u64,
    /// Collection binding and Core frame construction.
    pub bind_ns: u64,
    /// Equality-index selection and candidate lookup.
    pub index_ns: u64,
    /// Scan-source establishment or unfiltered materialisation.
    pub scan_ns: u64,
    /// Document loading and predicate evaluation.
    pub filter_ns: u64,
    /// Sort-key construction and ordering.
    pub order_ns: u64,
    /// Limit, page, and continuation processing.
    pub page_ns: u64,
    /// Core projection, grouping, and aggregation.
    pub project_ns: u64,
    /// Full-RQL enrich, within, attached filter, and brace projection.
    pub attach_ns: u64,
    /// Time inside host key, document, and index calls.
    pub host_read_ns: u64,
    /// Time spent serializing decoded JSON solely for logical-byte evidence.
    pub byte_account_ns: u64,
    /// Number of host data-access calls made by the VM.
    pub host_read_calls: u64,
}

/// Coverage policy for Application Core queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoveragePolicy {
    /// Complete coverage required (default).
    Complete,
    /// Incomplete coverage allowed with evidence.
    IncompleteAllowed,
}

/// Consistency mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyMode {
    /// Available reads (default).
    Available,
    /// Current / linearizable when supported.
    Current,
}

/// Scan / materialization budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryBudget {
    /// Max documents examined.
    pub max_documents: Option<u64>,
    /// Max bytes read.
    pub max_bytes: Option<u64>,
    /// Max result payload bytes.
    pub max_result_bytes: Option<u64>,
}

impl QueryBudget {
    /// Document-count budget only.
    pub fn documents(max_documents: u64) -> Self {
        Self {
            max_documents: Some(max_documents),
            max_bytes: None,
            max_result_bytes: None,
        }
    }
}

/// One page of query results (CORE plan §10).
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPage {
    /// Query instance id.
    pub query_id: QueryId,
    /// Canonical plan hash.
    pub plan_hash: [u8; 32],
    /// Heap binding.
    pub heap_id: HeapId,
    /// Collection binding.
    pub collection_id: CollectionId,
    /// Rows for this page.
    pub rows: Vec<QueryRow>,
    /// Authenticated continuation when not exhausted.
    pub next: Option<Continuation>,
    /// No further logical rows.
    pub exhausted: bool,
    /// Coverage evidence.
    pub coverage: CoverageEvidence,
    /// Consistency evidence.
    pub consistency: ConsistencyEvidence,
    /// Remaining total limit across pages.
    pub remaining_limit: Option<u64>,
    /// Explicit hole evidence.
    pub known_holes: Vec<HoleEvidence>,
    /// Serialized logical JSON payload bytes successfully read from every QVM
    /// source while producing this page. Full RQL includes foreign enrich and
    /// nested-within loads; damaged/unavailable bodies contribute zero bytes
    /// and remain represented by coverage holes.
    pub logical_bytes_examined: u64,
    /// Present only when [`QueryRunOptions::diagnostics`] was enabled.
    pub diagnostics: Option<QueryDiagnostics>,
}

/// One projected row.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryRow {
    /// Immutable document key.
    pub key: String,
    /// Projected JSON value.
    pub value: serde_json::Value,
}

/// Opaque authenticated continuation (`residiuum-cursor-v1`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Continuation {
    /// Opaque token bytes (never log).
    pub token: Vec<u8>,
}

/// Coverage evidence on a page (APB-7 T9 / DEF-100 honesty).
///
/// `complete` means every listed key on this page was readable (no known holes).
/// Complete-by-default: when [`CoveragePolicy::Complete`] and holes exist, the
/// executor fails closed with [`crate::Error::CoverageIncomplete`] rather than
/// returning a page that pretends completeness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageEvidence {
    /// Whether coverage is complete for the materialised page sources.
    pub complete: bool,
    /// Policy in force (from the logical plan / run options).
    pub mode: CoveragePolicy,
    /// Documents examined while building this page (get/list attempts counted).
    pub examined_documents: u64,
    /// Number of known holes on this page (`known_holes.len()`).
    pub hole_count: u32,
}

impl CoverageEvidence {
    /// Complete page: no holes.
    pub fn complete(mode: CoveragePolicy, examined_documents: u64) -> Self {
        Self {
            complete: true,
            mode,
            examined_documents,
            hole_count: 0,
        }
    }

    /// Incomplete page with hole evidence (only when incomplete is allowed).
    pub fn incomplete(mode: CoveragePolicy, examined_documents: u64, hole_count: u32) -> Self {
        Self {
            complete: false,
            mode,
            examined_documents,
            hole_count,
        }
    }
}

/// Consistency evidence on a page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsistencyEvidence {
    /// Mode in force.
    pub mode: ConsistencyMode,
}

/// Hole / damage evidence (never silent null).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoleEvidence {
    /// Stable machine code.
    pub code: String,
    /// Optional subject key.
    pub key: Option<String>,
}

/// Structured explain result (op 118 with `explain: true`).
#[derive(Debug, Clone, PartialEq)]
pub struct QueryExplanation {
    /// Query language / plan profile label.
    pub plan_profile: String,
    /// Canonical executable QVM hash (the identity returned by execution).
    pub plan_hash: [u8; 32],
    /// Human/debug tree (shape only at APP-0).
    pub tree: serde_json::Value,
}

/// Options for [`CollectionClient::scan_json`] (APB-7 / op 115 family).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScanJsonOptions {
    /// Max rows to return (1..=4096). Default 256 when `None`.
    pub limit: Option<u32>,
    /// Resume after this key (exclusive).
    pub after_key: Option<String>,
}

/// One page of JSON documents from a coverage-aware scan (APB-7 T3).
///
/// Not a product query page ([`QueryPage`]); no plan hash / continuation token.
/// Use [`CollectionClient::rql`] / [`CollectionClient::query`] for Application
/// Core execution.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanJsonPage {
    /// Owning Heap.
    pub heap_id: HeapId,
    /// Collection binding.
    pub collection_id: CollectionId,
    /// Rows for this page (key + full JSON value).
    pub rows: Vec<QueryRow>,
    /// Next `after_key` when more keys may exist.
    pub next_after_key: Option<String>,
    /// No further keys in deterministic order.
    pub exhausted: bool,
    /// Coverage evidence (holes when list/get races).
    pub coverage: CoverageEvidence,
    /// Explicit hole evidence (never silent null).
    pub known_holes: Vec<HoleEvidence>,
}

/// Options for [`CollectionClient::find_json`] (op 116 family).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FindJsonOptions {
    /// Max matching rows (1..=4096). Default 256 when `None`.
    pub limit: Option<u32>,
}

/// Sealed storage backend for [`HeapClient`] (APB-1 G1 / G1b).
///
/// Unbound remains the contract-only fixture.
enum HeapBackend {
    /// No storage — `from_id_for_contract` / APP-0 fixtures.
    Unbound,
    /// Embedded capability-gated [`Heap`].
    Embedded(Heap),
    /// Qualified remote session ([`RemoteHeap`]); shared with collection handles.
    Remote(Arc<Mutex<RemoteHeap>>),
}

/// Heap-bound application client (façade).
///
/// Bind storage with [`From<Heap>`] or [`From<RemoteHeap>`] (APB-1 G1/G1b).
/// Contract fixtures use [`HeapClient::from_id_for_contract`] (unbound, fail-closed).
pub struct HeapClient {
    heap_id: HeapId,
    backend: HeapBackend,
}

impl fmt::Debug for HeapClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let backend = match &self.backend {
            HeapBackend::Unbound => "unbound",
            HeapBackend::Embedded(_) => "embedded",
            HeapBackend::Remote(_) => "remote",
        };
        f.debug_struct("HeapClient")
            .field("heap_id", &self.heap_id)
            .field("backend", &backend)
            .finish()
    }
}

impl From<Heap> for HeapClient {
    fn from(heap: Heap) -> Self {
        Self {
            heap_id: heap.id(),
            backend: HeapBackend::Embedded(heap),
        }
    }
}

impl From<RemoteHeap> for HeapClient {
    fn from(remote: RemoteHeap) -> Self {
        Self {
            heap_id: remote.id(),
            backend: HeapBackend::Remote(Arc::new(Mutex::new(remote))),
        }
    }
}

impl HeapClient {
    /// Contract fixture constructor (does not open storage).
    pub fn from_id_for_contract(heap_id: HeapId) -> Self {
        Self {
            heap_id,
            backend: HeapBackend::Unbound,
        }
    }

    /// Bound Heap id.
    pub fn id(&self) -> HeapId {
        self.heap_id
    }

    /// Whether this client has a storage backend (not a contract fixture).
    pub fn is_bound(&self) -> bool {
        !matches!(self.backend, HeapBackend::Unbound)
    }

    /// Open a stable bounded read view (APB-6).
    ///
    /// **Embedded:** pins `authoritative_frontier` to the store segment
    /// fingerprint (`observation_pinned = true`). Drift is re-checkable via
    /// [`crate::read_view_v1::ReadView::check_drift`]. This is **not** multi-
    /// query snapshot isolation. View-bound Core query is available when
    /// [`crate::read_view_v1::ReadView::ensure_observation_stable`] holds
    /// (APB-7 T5); see [`crate::read_view_v1::ReadView::open_collection`].
    ///
    /// **Remote:** live-unpinned residual (honest; remote pin / HAR-4 later).
    ///
    /// See `APB6_READ_VIEW_GAP_INVENTORY.md`.
    pub fn read_view(
        &mut self,
        options: crate::read_view_v1::ReadViewOptions,
    ) -> Result<crate::read_view_v1::ReadView, Error> {
        match &self.backend {
            HeapBackend::Unbound => Err(Error::Internal(
                "HeapClient unbound; construct with From<Heap|RemoteHeap> (APB-1)".into(),
            )),
            HeapBackend::Embedded(heap) => {
                let fp = heap.segment_fingerprint()?;
                let store = heap.store_arc();
                crate::read_view_v1::ReadView::open_segment_fingerprint_pinned(
                    self.heap_id,
                    options,
                    store,
                    fp,
                )
            }
            HeapBackend::Remote(_) => {
                // Residual: no remote segment pin / view op yet.
                crate::read_view_v1::ReadView::open_live_unpinned(self.heap_id, options)
            }
        }
    }

    /// Create a collection (APP-1 / APB-1).
    pub fn create_collection(&mut self, name: &str) -> Result<CreateCollectionResult, Error> {
        self.create_collection_with(name, CreateCollectionOptions::default())
    }

    /// Create with options (APP-1 / APB-1).
    pub fn create_collection_with(
        &mut self,
        name: &str,
        options: CreateCollectionOptions,
    ) -> Result<CreateCollectionResult, Error> {
        match &self.backend {
            HeapBackend::Unbound => Err(Error::Internal(
                "HeapClient unbound; construct with From<Heap|RemoteHeap> (APB-1)".into(),
            )),
            HeapBackend::Embedded(heap) => {
                let created = heap.create_collection_with(name, options.operation_id)?;
                Ok(create_result_from_embedded(created))
            }
            HeapBackend::Remote(remote) => {
                let mut guard = lock_remote(remote)?;
                let created = guard.create_collection(name, options.operation_id)?;
                create_result_from_remote(self.heap_id, Arc::clone(remote), created)
            }
        }
    }

    /// Open a collection by immutable id (RQL-P1b).
    ///
    /// Resolves the live name via [`Self::list_collections`], opens it, and
    /// refuses if the opened id ≠ `collection_id`.
    pub fn open_collection_by_id(
        &mut self,
        collection_id: CollectionId,
    ) -> Result<CollectionClient, Error> {
        let listed = self.list_collections()?;
        let name = listed
            .iter()
            .find(|c| c.collection_id == collection_id)
            .map(|c| c.name.clone())
            .ok_or_else(|| {
                Error::QueryInvalid(format!(
                    "open_collection_by_id: unknown collection {collection_id}"
                ))
            })?;
        let col = self.open_collection(&name)?;
        if col.id() != collection_id {
            return Err(Error::QueryInvalid(format!(
                "collection id mismatch for `{name}`: opened {}, want {collection_id}",
                col.id()
            )));
        }
        Ok(col)
    }

    /// Open by canonical name (embedded: [`Heap::collection`]; remote: op 105).
    pub fn open_collection(&mut self, name: &str) -> Result<CollectionClient, Error> {
        match &self.backend {
            HeapBackend::Unbound => Err(Error::Internal(
                "HeapClient unbound; construct with From<Heap|RemoteHeap> (APB-1)".into(),
            )),
            HeapBackend::Embedded(heap) => {
                let hc = heap.collection(name)?;
                Ok(CollectionClient::from_embedded(hc))
            }
            HeapBackend::Remote(remote) => {
                let mut guard = lock_remote(remote)?;
                let cid_s = guard.collection_open(name)?;
                let collection_id = collection_id_from_wire(&cid_s)?;
                Ok(CollectionClient::from_remote(
                    self.heap_id,
                    collection_id,
                    name.to_string(),
                    Arc::clone(remote),
                    cid_s,
                ))
            }
        }
    }

    /// List collections (embedded catalog; remote op 110).
    ///
    /// Remote wire omits descriptor hashes today — listed remote rows use
    /// zeroed `descriptor_hash` until the wire grows the field.
    pub fn list_collections(&mut self) -> Result<Vec<CollectionInfo>, Error> {
        match &self.backend {
            HeapBackend::Unbound => Err(Error::Internal(
                "HeapClient unbound; construct with From<Heap|RemoteHeap> (APB-1)".into(),
            )),
            HeapBackend::Embedded(heap) => {
                let listed = heap.list_collections()?;
                Ok(listed
                    .into_iter()
                    .map(|e| CollectionInfo {
                        heap_id: e.heap_id,
                        collection_id: e.collection_id,
                        name: e.name,
                        descriptor_hash: e.descriptor_hash,
                    })
                    .collect())
            }
            HeapBackend::Remote(remote) => {
                let mut guard = lock_remote(remote)?;
                let listed = guard.list_collections()?;
                let mut out = Vec::with_capacity(listed.len());
                for (id_s, name) in listed {
                    let collection_id = collection_id_from_wire(&id_s)?;
                    out.push(CollectionInfo {
                        heap_id: self.heap_id,
                        collection_id,
                        name,
                        descriptor_hash: [0u8; 32],
                    });
                }
                Ok(out)
            }
        }
    }

    /// Execute the current bounded Full RQL profile against this Heap.
    ///
    /// Unlike [`CollectionClient::rql`], the Full surface is Heap-bound because
    /// `enrich` and nested `within` may read several authorised collections.
    /// Embedded execution and remote op 118 both compile the same source to the
    /// same verified QVM profile; remote execution occurs on the server.
    pub fn rql_full(
        &mut self,
        source: &str,
        parameters: &Parameters,
        options: QueryRunOptions,
    ) -> Result<crate::query_bytecode_v1::RqlFullPage, Error> {
        if options.explain {
            return Err(Error::QueryInvalid(
                "HeapClient::rql_full executes pages; use explain_rql_full_on_heap for compile-only Full explanation"
                    .into(),
            ));
        }
        if matches!(self.backend, HeapBackend::Unbound) {
            return Err(Error::Internal(
                "HeapClient unbound; construct with From<Heap|RemoteHeap> (APB-1)".into(),
            ));
        }
        let remote = match &self.backend {
            HeapBackend::Remote(remote) => Some(Arc::clone(remote)),
            HeapBackend::Embedded(_) | HeapBackend::Unbound => None,
        };
        let Some(remote) = remote else {
            return crate::query_bytecode_v1::execute_rql_full(self, source, parameters, options);
        };

        let infos = self.list_collections()?;
        let mut bindings = CollectionBindings::default();
        for info in &infos {
            bindings.bind(&info.name, info.collection_id);
        }
        let compiled = crate::query_bytecode_v1::compile_rql_full(source, &bindings)?;
        let root_id = compiled.base.plan.from.collection_id;
        if !infos.iter().any(|info| info.collection_id == root_id) {
            return Err(Error::QueryInvalid(format!(
                "Full RQL root collection {root_id} is outside the authorised Heap catalogue"
            )));
        }

        let args = rql_wire_args(source, parameters, &options, Some("full"))?;
        let mut guard = lock_remote(&remote)?;
        let result = guard.rql_query(&root_id.to_string(), serde_json::Value::Object(args))?;
        drop(guard);
        parse_rql_full_result(result, self.heap_id, root_id, compiled)
    }
}

fn lock_remote(
    remote: &Arc<Mutex<RemoteHeap>>,
) -> Result<std::sync::MutexGuard<'_, RemoteHeap>, Error> {
    remote
        .lock()
        .map_err(|_| Error::Internal("RemoteHeap mutex poisoned".into()))
}

fn create_result_from_embedded(created: crate::heap::CreatedCollection) -> CreateCollectionResult {
    let heap_id = created.collection.heap_id();
    let collection_id = created.collection.id();
    let created_at = UNIX_EPOCH + Duration::from_secs(created.created_at_unix_s);
    CreateCollectionResult {
        collection: CollectionClient::from_embedded(created.collection),
        receipt: CollectionCreateReceipt {
            receipt_id: created.receipt_id,
            operation: AdminOperation::CreateCollection,
            heap_id,
            collection_id,
            descriptor_hash: created.descriptor_hash,
            created_at,
        },
    }
}

fn create_result_from_remote(
    heap_id: HeapId,
    remote: Arc<Mutex<RemoteHeap>>,
    created: crate::remote_heap::RemoteCreatedCollection,
) -> Result<CreateCollectionResult, Error> {
    let collection_id = collection_id_from_wire(&created.collection_id)?;
    let descriptor_hash = parse_hex32(&created.descriptor_hash)?;
    let receipt_id = if created.receipt_id.is_empty() {
        [0u8; 16]
    } else {
        parse_hex16(&created.receipt_id)?
    };
    Ok(CreateCollectionResult {
        collection: CollectionClient::from_remote(
            heap_id,
            collection_id,
            created.canonical_name,
            remote,
            created.collection_id,
        ),
        receipt: CollectionCreateReceipt {
            receipt_id,
            operation: AdminOperation::CreateCollection,
            heap_id,
            collection_id,
            descriptor_hash,
            created_at: SystemTime::now(),
        },
    })
}

/// Parse a wire collection UUID string into [`CollectionId`].
///
/// Prefers strict UUIDv4; falls back to nonzero integrity-valid bytes (embedded
/// create path already uses unchecked reconstruction of stored object ids).
fn collection_id_from_wire(s: &str) -> Result<CollectionId, Error> {
    if let Ok(id) = CollectionId::from_str(s) {
        return Ok(id);
    }
    let bytes = parse_hex16(s)?;
    CollectionId::from_bytes_unchecked_nonzero(bytes)
        .map_err(|e| Error::ProtocolViolation(format!("collection_id: {e}")))
}

fn parse_hex16(s: &str) -> Result<[u8; 16], Error> {
    let clean: String = s.chars().filter(|c| *c != '-').collect();
    if clean.len() != 32 {
        return Err(Error::ProtocolViolation(format!(
            "expected 16-byte hex, got len {}",
            clean.len()
        )));
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&clean[i * 2..i * 2 + 2], 16)
            .map_err(|e| Error::ProtocolViolation(format!("hex: {e}")))?;
    }
    Ok(out)
}

fn parse_hex32(s: &str) -> Result<[u8; 32], Error> {
    let clean: String = s.chars().filter(|c| *c != '-').collect();
    if clean.len() != 64 {
        return Err(Error::ProtocolViolation(format!(
            "expected 32-byte hex, got len {}",
            clean.len()
        )));
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&clean[i * 2..i * 2 + 2], 16)
            .map_err(|e| Error::ProtocolViolation(format!("hex: {e}")))?;
    }
    Ok(out)
}

/// Sealed collection backend (embedded or remote handle when bound).
enum CollectionBackend {
    /// Identity-only / contract fixture.
    Unbound,
    /// Embedded [`HeapCollection`] for data-plane methods.
    Embedded(HeapCollection),
    /// Remote collection: wire collection UUID + shared session.
    Remote {
        remote: Arc<Mutex<RemoteHeap>>,
        wire_collection_id: String,
    },
}

/// Collection-bound application client (façade).
pub struct CollectionClient {
    heap_id: HeapId,
    collection_id: CollectionId,
    name: String,
    backend: CollectionBackend,
}

impl fmt::Debug for CollectionClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let backend = match &self.backend {
            CollectionBackend::Unbound => "unbound",
            CollectionBackend::Embedded(_) => "embedded",
            CollectionBackend::Remote { .. } => "remote",
        };
        f.debug_struct("CollectionClient")
            .field("heap_id", &self.heap_id)
            .field("collection_id", &self.collection_id)
            .field("name", &self.name)
            .field("backend", &backend)
            .finish()
    }
}

impl CollectionClient {
    /// Contract fixture constructor (does not open storage).
    pub fn from_parts_for_contract(
        heap_id: HeapId,
        collection_id: CollectionId,
        name: impl Into<String>,
    ) -> Self {
        Self {
            heap_id,
            collection_id,
            name: name.into(),
            backend: CollectionBackend::Unbound,
        }
    }

    fn from_embedded(hc: HeapCollection) -> Self {
        Self {
            heap_id: hc.heap_id(),
            collection_id: hc.id(),
            name: hc.name().to_string(),
            backend: CollectionBackend::Embedded(hc),
        }
    }

    fn from_remote(
        heap_id: HeapId,
        collection_id: CollectionId,
        name: String,
        remote: Arc<Mutex<RemoteHeap>>,
        wire_collection_id: String,
    ) -> Self {
        Self {
            heap_id,
            collection_id,
            name,
            backend: CollectionBackend::Remote {
                remote,
                wire_collection_id,
            },
        }
    }

    /// Whether this client holds an embedded or remote collection handle.
    pub fn is_bound(&self) -> bool {
        !matches!(self.backend, CollectionBackend::Unbound)
    }

    /// Owning Heap.
    pub fn heap_id(&self) -> HeapId {
        self.heap_id
    }

    /// Immutable collection id.
    pub fn id(&self) -> CollectionId {
        self.collection_id
    }

    /// Canonical name (display).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// JSON put (APP-3 / APB-2 path; embedded + remote).
    pub fn put<T: Serialize>(&mut self, key: &str, value: &T) -> Result<WriteReceipt, Error> {
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient::open_collection / create (APB-1)"
                    .into(),
            )),
            CollectionBackend::Embedded(hc) => hc.put(key, value),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let json = serde_json::to_value(value)
                    .map_err(|e| Error::ValidationMsg(format!("serialize: {e}")))?;
                let mut guard = lock_remote(remote)?;
                let (event_id_s, version_s) = guard.put_json(wire_collection_id, key, &json)?;
                Ok(write_receipt_from_remote_ids(key, &event_id_s, &version_s)?)
            }
        }
    }

    /// JSON put with options (APP-3). Remote ignores durability options today
    /// (server default); embedded honors [`PutOptions`].
    pub fn put_with<T: Serialize>(
        &mut self,
        key: &str,
        value: &T,
        options: PutOptions,
    ) -> Result<WriteReceipt, Error> {
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.put_with(key, value, options),
            CollectionBackend::Remote { .. } => {
                // Wire put path has no durability field on this surface yet.
                let _ = options;
                self.put(key, value)
            }
        }
    }

    /// Bytes put (APP-3).
    pub fn put_bytes(&mut self, key: &str, value: &[u8]) -> Result<WriteReceipt, Error> {
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.put_bytes(key, value),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let mut guard = lock_remote(remote)?;
                let (event_id_s, version_s) = guard.put_bytes(wire_collection_id, key, value)?;
                Ok(write_receipt_from_remote_ids(key, &event_id_s, &version_s)?)
            }
        }
    }

    /// JSON get (APP-3).
    pub fn get(&mut self, key: &str) -> Result<Option<serde_json::Value>, Error> {
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.get(key),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let mut guard = lock_remote(remote)?;
                guard.get_json(wire_collection_id, key)
            }
        }
    }

    /// Bytes get (APP-3).
    pub fn get_bytes(&mut self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.get_bytes(key),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let mut guard = lock_remote(remote)?;
                guard.get_bytes(wire_collection_id, key)
            }
        }
    }

    /// Delete (APP-3).
    pub fn delete(&mut self, key: &str) -> Result<DeleteReceipt, Error> {
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.delete(key),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let mut guard = lock_remote(remote)?;
                let removed = guard.delete(wire_collection_id, key)?;
                // Remote delete returns only removed: bool today — receipt ids zeroed.
                Ok(DeleteReceipt {
                    key: key.to_string(),
                    removed,
                    event_id: [0u8; 16],
                    version: [0u8; 16],
                    acknowledgement: DurabilityMode::Durable,
                    committed: true,
                    store_id: [0u8; 16],
                    segment_id: [0u8; 16],
                })
            }
        }
    }

    /// Create JSON only when the key is currently absent (APB-2 / `apb.doc.create`).
    ///
    /// Embedded store CAS and remote wire `if_absent` (APB-2 T7) are Key Atomic.
    pub fn create<T: Serialize>(&mut self, key: &str, value: &T) -> Result<WriteReceipt, Error> {
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc
                .create_if_absent(key, value)
                .map_err(crate::error::lift_store_cas),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let json = serde_json::to_value(value)?;
                let mut guard = lock_remote(remote)?;
                match guard.put_json_if(wire_collection_id, key, &json, None, true) {
                    Ok((event_id_s, version_s)) => {
                        Ok(write_receipt_from_remote_ids(key, &event_id_s, &version_s)?)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Upsert JSON; reports whether the key was absent before the write (APB-2).
    ///
    /// Create path is Key Atomic on both backends (T5/T7).
    pub fn upsert<T: Serialize>(&mut self, key: &str, value: &T) -> Result<UpsertResult, Error> {
        match self.create(key, value) {
            Ok(receipt) => Ok(UpsertResult {
                inserted: true,
                receipt,
            }),
            Err(e) if e.code() == ErrorCode::AlreadyExists => {
                let receipt = self.put(key, value)?;
                Ok(UpsertResult {
                    inserted: false,
                    receipt,
                })
            }
            Err(e) => Err(e),
        }
    }

    /// Replace JSON only when the live establishing event id matches (APB-2).
    ///
    /// Embedded store CAS and remote wire `if_version` (APB-2 T7) are Key Atomic.
    /// Pass [`WriteReceipt::version`] (or history last put `event_id`) as
    /// `if_version`.
    pub fn replace<T: Serialize>(
        &mut self,
        key: &str,
        value: &T,
        if_version: [u8; 16],
    ) -> Result<WriteReceipt, Error> {
        self.replace_with(
            key,
            value,
            ReplaceOptions { if_version },
            PutOptions::default(),
        )
    }

    /// Insert with a generated key (APB-2 / `apb.doc.add` / PD-004).
    ///
    /// Default profile: [`KeyProfile::RandomV1`]. Returns the key explicitly.
    pub fn add<T: Serialize>(&mut self, value: &T) -> Result<AddResult, Error> {
        self.add_with(value, KeyProfile::RandomV1, PutOptions::default())
    }

    /// Insert with a generated key under a named profile (APB-2).
    ///
    /// Uses create-then-write (absent check + put). Retries mint on the rare
    /// collision. Residual: not a single-key atomic create; concurrent create
    /// races remain until store CAS.
    pub fn add_with<T: Serialize>(
        &mut self,
        value: &T,
        profile: KeyProfile,
        options: PutOptions,
    ) -> Result<AddResult, Error> {
        for _ in 0..ADD_KEY_MINT_ATTEMPTS {
            let key = profile.mint_key()?;
            if self.get(&key)?.is_some() {
                continue;
            }
            let receipt = self.put_with(&key, value, options)?;
            return Ok(AddResult {
                key,
                key_profile: profile.as_str(),
                receipt,
            });
        }
        Err(Error::Internal(
            "add: exhausted generated-key collision retries".into(),
        ))
    }

    /// Replace with durability options (APB-2).
    pub fn replace_with<T: Serialize>(
        &mut self,
        key: &str,
        value: &T,
        replace: ReplaceOptions,
        options: PutOptions,
    ) -> Result<WriteReceipt, Error> {
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => {
                let _ = options; // heap façade is durable today
                hc.replace_if_version(key, value, replace.if_version)
                    .map_err(crate::error::lift_store_cas)
            }
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let _ = options;
                let json = serde_json::to_value(value)?;
                let mut guard = lock_remote(remote)?;
                match guard.put_json_if(
                    wire_collection_id,
                    key,
                    &json,
                    Some(replace.if_version),
                    false,
                ) {
                    Ok((event_id_s, version_s)) => {
                        Ok(write_receipt_from_remote_ids(key, &event_id_s, &version_s)?)
                    }
                    Err(Error::VersionConflict {
                        expected: _,
                        observed,
                    }) => {
                        // Prefer caller's token as expected (server echoes same).
                        Err(Error::VersionConflict {
                            expected: replace.if_version,
                            observed,
                        })
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Conditional delete (APB-2 / `apb.doc.delete_with`).
    ///
    /// - `if_version`: when `Some`, must match live establishing event id
    /// - `if_present`: when `true`, absence is [`Error::NotFound`]; when
    ///   `false`, absence returns `removed: false` without error
    ///
    /// Embedded + remote wire Key Atomic (APB-2 T5/T7).
    pub fn delete_with(
        &mut self,
        key: &str,
        if_version: Option<[u8; 16]>,
        if_present: bool,
        options: PutOptions,
    ) -> Result<DeleteReceipt, Error> {
        self.delete_with_options(
            key,
            DeleteWithOptions {
                if_version,
                if_present,
            },
            options,
        )
    }

    /// Conditional delete with structured options (APB-2).
    pub fn delete_with_options(
        &mut self,
        key: &str,
        cond: DeleteWithOptions,
        options: PutOptions,
    ) -> Result<DeleteReceipt, Error> {
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => {
                let _ = options;
                let condition = match cond.if_version {
                    Some(v) => residiuum_store::WriteCondition::LiveEventId(v),
                    None if cond.if_present => residiuum_store::WriteCondition::Present,
                    None => {
                        // Idempotent: if absent, report removed=false without error.
                        if hc.get(key)?.is_none() {
                            return Ok(DeleteReceipt {
                                key: key.to_string(),
                                removed: false,
                                event_id: [0u8; 16],
                                version: [0u8; 16],
                                acknowledgement: options.durability,
                                committed: true,
                                store_id: [0u8; 16],
                                segment_id: [0u8; 16],
                            });
                        }
                        residiuum_store::WriteCondition::Unconditional
                    }
                };
                match hc.delete_if(key, condition) {
                    Ok(r) => Ok(r),
                    Err(e) => {
                        let e = crate::error::lift_store_cas(e);
                        if matches!(condition, residiuum_store::WriteCondition::Present)
                            && e.code() == ErrorCode::VersionConflict
                        {
                            return Err(Error::NotFound(format!("key absent: {key}")));
                        }
                        Err(e)
                    }
                }
            }
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let _ = options;
                let mut guard = lock_remote(remote)?;
                match guard.delete_if(wire_collection_id, key, cond.if_version, cond.if_present) {
                    Ok((removed, event_id_s, version_s)) => {
                        let event_id = parse_hex16(&event_id_s).unwrap_or([0u8; 16]);
                        let version = parse_hex16(&version_s).unwrap_or(event_id);
                        Ok(DeleteReceipt {
                            key: key.to_string(),
                            removed,
                            event_id,
                            version,
                            acknowledgement: options.durability,
                            committed: true,
                            store_id: [0u8; 16],
                            segment_id: [0u8; 16],
                        })
                    }
                    Err(Error::VersionConflict { expected, observed }) => {
                        if cond.if_present && cond.if_version.is_none() {
                            return Err(Error::NotFound(format!("key absent: {key}")));
                        }
                        Err(Error::VersionConflict {
                            expected: cond.if_version.unwrap_or(expected),
                            observed,
                        })
                    }
                    Err(Error::Remote { code, message }) if code == "not_found" => {
                        Err(Error::NotFound(message))
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// List application keys (APB-2 / `apb.doc.list_keys`).
    ///
    /// `limit` defaults to 256 when `None`. `after_key` resumes after that key.
    pub fn list_keys(
        &mut self,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let limit = limit.unwrap_or(256);
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.list_keys(limit, after_key),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let mut guard = lock_remote(remote)?;
                guard.list_keys(wire_collection_id, Some(limit), after_key)
            }
        }
    }

    /// Per-key event history (APB-1 G4 / DEF-099 surface).
    ///
    /// Embedded: SubjectV2 via [`HeapCollection::history`]. Remote: op 117,
    /// projected into [`KeyHistory`].
    pub fn history(&mut self, key: &str) -> Result<KeyHistory, Error> {
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.history(key),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let mut guard = lock_remote(remote)?;
                let (versions, has_known_holes) = guard.history(wire_collection_id, key)?;
                key_history_from_remote_versions(key, versions, has_known_holes)
            }
        }
    }

    /// Secondary-index manager for this collection (APB-1 G3).
    ///
    /// Unbound clients return an unbound manager that fails closed on ops.
    pub fn indexes(&mut self) -> IndexManager<'_> {
        IndexManager { client: self }
    }

    /// Equality index candidate keys (APB-7 T4 probe; embedded only).
    ///
    /// See [`crate::heap::HeapCollection::lookup_index_keys`]. Remote returns
    /// `Ok(None)` until a product index-probe wire exists.
    pub fn lookup_index_keys(
        &mut self,
        equalities: &[(String, serde_json::Value)],
    ) -> Result<Option<Vec<String>>, Error> {
        match &self.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.lookup_index_keys(equalities),
            CollectionBackend::Remote { .. } => Ok(None),
        }
    }

    /// Coverage-aware JSON document scan (APB-7 T3 / `apb.collection.scan_json`).
    ///
    /// Embedded: `list_keys` + `get` with hole evidence when a listed key is
    /// absent. Remote: wire op **115** `scan_json`. Not Application Core query
    /// (no plan hash / authenticated cursor) — use [`Self::rql`] / [`Self::query`].
    pub fn scan_json(&mut self, options: ScanJsonOptions) -> Result<ScanJsonPage, Error> {
        if matches!(self.backend, CollectionBackend::Unbound) {
            return Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            ));
        }
        let limit = options.limit.unwrap_or(256).clamp(1, 4_096) as usize;
        let after = options.after_key.as_deref();

        match &self.backend {
            CollectionBackend::Unbound => unreachable!(),
            CollectionBackend::Embedded(_) | CollectionBackend::Remote { .. } => {
                // Shared embedded-style materialization via list_keys+get for
                // both backends: remote list_keys/get already exist, and this
                // yields honest hole evidence. Prefer wire 115 when available
                // for a single round-trip on remote.
                if let CollectionBackend::Remote {
                    remote,
                    wire_collection_id,
                } = &self.backend
                {
                    return self.scan_json_remote(
                        Arc::clone(remote),
                        wire_collection_id.clone(),
                        limit,
                        after,
                    );
                }
                self.scan_json_via_list_get(limit, after)
            }
        }
    }

    fn scan_json_via_list_get(
        &mut self,
        limit: usize,
        after_key: Option<&str>,
    ) -> Result<ScanJsonPage, Error> {
        // Fetch limit+1 keys to detect exhaustion without a second round-trip.
        let fetch = limit.saturating_add(1).min(4_096);
        let keys = self.list_keys(Some(fetch), after_key)?;
        let has_more = keys.len() > limit;
        let page_keys: Vec<String> = keys.into_iter().take(limit).collect();

        let mut rows = Vec::with_capacity(page_keys.len());
        let mut known_holes = Vec::new();
        for key in &page_keys {
            match self.get(key)? {
                Some(value) => rows.push(QueryRow {
                    key: key.clone(),
                    value,
                }),
                None => known_holes.push(HoleEvidence {
                    code: "key_listed_absent".into(),
                    key: Some(key.clone()),
                }),
            }
        }
        let next_after_key = if has_more {
            page_keys.last().cloned()
        } else {
            None
        };
        let exhausted = !has_more;
        let examined = (rows.len() + known_holes.len()) as u64;
        let coverage = if known_holes.is_empty() {
            CoverageEvidence::complete(CoveragePolicy::Complete, examined)
        } else {
            // scan_json is coverage-aware but not fail-closed Complete; report holes.
            CoverageEvidence::incomplete(
                CoveragePolicy::IncompleteAllowed,
                examined,
                known_holes.len() as u32,
            )
        };
        Ok(ScanJsonPage {
            heap_id: self.heap_id,
            collection_id: self.collection_id,
            rows,
            next_after_key,
            exhausted,
            coverage,
            known_holes,
        })
    }

    fn scan_json_remote(
        &mut self,
        remote: Arc<Mutex<RemoteHeap>>,
        wire_collection_id: String,
        limit: usize,
        after_key: Option<&str>,
    ) -> Result<ScanJsonPage, Error> {
        let mut guard = lock_remote(&remote)?;
        let page = guard.scan_json(&wire_collection_id, Some(limit), after_key)?;
        drop(guard);
        // Wire contract (T8): exhausted / next_after_key already validated against
        // has_more. Never derive continuation from last successful row alone.
        let exhausted = page.exhausted;
        let next_after_key = page.next_after_key;
        let known_holes: Vec<HoleEvidence> = page
            .incomplete
            .iter()
            .map(|h| {
                let key = h.get("key").and_then(|v| v.as_str()).map(|s| s.to_string());
                let code = h
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("incomplete")
                    .to_string();
                HoleEvidence { code, key }
            })
            .collect();
        let rows: Vec<QueryRow> = page
            .rows
            .into_iter()
            .map(|(key, value)| QueryRow { key, value })
            .collect();
        let examined = (rows.len() + known_holes.len()) as u64;
        let coverage = if page.coverage_complete && known_holes.is_empty() {
            CoverageEvidence::complete(CoveragePolicy::Complete, examined)
        } else {
            CoverageEvidence::incomplete(
                CoveragePolicy::IncompleteAllowed,
                examined,
                known_holes.len() as u32,
            )
        };
        Ok(ScanJsonPage {
            heap_id: self.heap_id,
            collection_id: self.collection_id,
            rows,
            next_after_key,
            exhausted,
            coverage,
            known_holes,
        })
    }

    /// Mongo/DX-style find (APB-7 T3 / `apb.collection.find`, wire op **116**).
    ///
    /// Filter vocabulary is [`Filter::from_json`]. Embedded scans with
    /// list_keys+get (no index pushdown). Remote uses op 116. Not Application
    /// Core — prefer [`Self::query`] / [`Self::rql`] for plan-bound pages.
    pub fn find_json(
        &mut self,
        filter: &serde_json::Value,
        options: FindJsonOptions,
    ) -> Result<Vec<QueryRow>, Error> {
        if matches!(self.backend, CollectionBackend::Unbound) {
            return Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            ));
        }
        let limit = options.limit.unwrap_or(256).clamp(1, 4_096) as usize;
        let parsed = Filter::from_json(filter)?;

        match &self.backend {
            CollectionBackend::Unbound => unreachable!(),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let mut guard = lock_remote(remote)?;
                let page = guard.find(wire_collection_id, filter, Some(limit))?;
                // Fail closed on incomplete find: secondary-index and scan holes
                // must not look like a complete match set (DEF-SCAN-001 #5).
                if !page.coverage_complete {
                    return Err(Error::CoverageIncomplete(format!(
                        "find incomplete: {} hole(s); refuse silent partial match set",
                        page.incomplete.len()
                    )));
                }
                Ok(page
                    .rows
                    .into_iter()
                    .map(|(key, value)| QueryRow { key, value })
                    .collect())
            }
            CollectionBackend::Embedded(_) => {
                // Bounded scan: walk keys until `limit` matches or keys end.
                let mut out = Vec::new();
                let mut after: Option<String> = None;
                while out.len() < limit {
                    let batch = self.list_keys(Some(256), after.as_deref())?;
                    if batch.is_empty() {
                        break;
                    }
                    for key in batch {
                        after = Some(key.clone());
                        if let Some(doc) = self.get(&key)? {
                            if parsed.matches(&doc) {
                                out.push(QueryRow { key, value: doc });
                                if out.len() >= limit {
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok(out)
            }
        }
    }

    /// Application Core builder path (APB-7 T1).
    ///
    /// Returns a [`CollectionQuery`] seeded with this collection's canonical
    /// name. Compiles via [`PlanBuilder`] to the same [`RqlPlanV1`] shape as
    /// RQL Application Core; execute reuses the APP-6 page executor.
    ///
    /// Executes through the same Core QVM path used by active op 118.
    pub fn query(&mut self) -> CollectionQuery<'_> {
        CollectionQuery::open(self)
    }

    /// RQL Application Core execution (APP-6 page executor / APP-7 remote wire).
    ///
    /// **Embedded:** compiles Application Core source, scans via `list_keys`+`get`,
    /// evaluates predicates, pages with key / field-order continuation.
    ///
    /// **Remote:** product wire op **118** `rql_query` (APP-7 T6). Server
    /// recompiles source; not a package-accept claim. Full-language
    /// (`enrich` / `within` / brace `project`) is refused on this collection-
    /// bound Core surface — use [`HeapClient::rql_full`] (RQL-F2).
    pub fn rql(
        &mut self,
        source: &str,
        parameters: &Parameters,
        options: QueryRunOptions,
    ) -> Result<QueryPage, Error> {
        if matches!(self.backend, CollectionBackend::Unbound) {
            return Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            ));
        }
        if let CollectionBackend::Remote {
            remote,
            wire_collection_id,
        } = &self.backend
        {
            crate::query_bytecode_v1::refuse_full_language_on_core_wire(source)?;
            return self.rql_via_wire(
                Arc::clone(remote),
                wire_collection_id.clone(),
                source,
                parameters,
                options,
            );
        }
        let heap_id = self.heap_id;
        let collection_id = self.collection_id;
        let name = self.name.clone();
        crate::query_bytecode_v1::execute_core_rql(
            self,
            source,
            parameters,
            &options,
            heap_id,
            collection_id,
            &name,
        )
    }

    /// RQL explain — plan tree + hash (no row scan).
    ///
    /// Remote uses op **118** with `explain: true` when available.
    /// Full-language explain uses [`crate::explain_rql_full`]; the Heap-bound
    /// Full execute surface is [`HeapClient::rql_full`].
    pub fn explain_rql(
        &mut self,
        source: &str,
        parameters: &Parameters,
        options: QueryRunOptions,
    ) -> Result<QueryExplanation, Error> {
        if matches!(self.backend, CollectionBackend::Unbound) {
            return Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            ));
        }
        if let CollectionBackend::Remote {
            remote,
            wire_collection_id,
        } = &self.backend
        {
            crate::query_bytecode_v1::refuse_full_language_on_core_wire(source)?;
            let mut opts = options;
            opts.explain = true;
            let page = self.rql_via_wire(
                Arc::clone(remote),
                wire_collection_id.clone(),
                source,
                parameters,
                opts,
            )?;
            // Wire explain packs tree under known_holes-free page; recover from
            // local compile if server only returned plan_hash (first cut).
            return crate::query_bytecode_v1::explain_core_source(
                source,
                self.collection_id,
                &self.name,
            )
            .map(|mut ex| {
                // Prefer wire plan_hash when present.
                if page.plan_hash != [0u8; 32] {
                    ex.plan_hash = page.plan_hash;
                }
                ex
            });
        }
        crate::query_bytecode_v1::explain_core_source(source, self.collection_id, &self.name)
    }

    /// Remote op 118 wire path (APP-7 T6).
    fn rql_via_wire(
        &mut self,
        remote: Arc<Mutex<RemoteHeap>>,
        wire_collection_id: String,
        source: &str,
        parameters: &Parameters,
        options: QueryRunOptions,
    ) -> Result<QueryPage, Error> {
        let args = rql_wire_args(source, parameters, &options, None)?;

        let mut guard = remote
            .lock()
            .map_err(|_| Error::Internal("remote heap lock poisoned".into()))?;
        let result = guard.rql_query(&wire_collection_id, serde_json::Value::Object(args))?;
        drop(guard);
        parse_rql_query_result(result, self.heap_id, self.collection_id)
    }
}

fn rql_wire_args(
    source: &str,
    parameters: &Parameters,
    options: &QueryRunOptions,
    profile: Option<&str>,
) -> Result<serde_json::Map<String, serde_json::Value>, Error> {
    let mut args = serde_json::Map::new();
    args.insert("source".into(), serde_json::Value::String(source.into()));
    if let Some(profile) = profile {
        args.insert("profile".into(), serde_json::Value::String(profile.into()));
    }
    if !parameters.values.is_empty() {
        let mut m = serde_json::Map::new();
        for (k, v) in &parameters.values {
            m.insert(k.clone(), v.clone());
        }
        args.insert("parameters".into(), serde_json::Value::Object(m));
    }
    if options.explain {
        args.insert("explain".into(), serde_json::Value::Bool(true));
    }
    if let Some(ps) = options.page_size {
        args.insert("page_size".into(), serde_json::json!(ps));
    }
    let coverage = match options.coverage {
        CoveragePolicy::Complete => "complete",
        CoveragePolicy::IncompleteAllowed => "incomplete_allowed",
    };
    args.insert(
        "coverage".into(),
        serde_json::Value::String(coverage.into()),
    );
    let consistency = match options.consistency {
        ConsistencyMode::Available => "available",
        ConsistencyMode::Current => "current",
    };
    args.insert(
        "consistency".into(),
        serde_json::Value::String(consistency.into()),
    );
    if let Some(ref cont) = options.after {
        let s = std::str::from_utf8(&cont.token).map_err(|_| {
            Error::QueryInvalid("continuation token must be UTF-8 for wire op 118".into())
        })?;
        args.insert("continuation".into(), serde_json::Value::String(s.into()));
    }
    if let Some(b) = options.budget {
        let mut bm = serde_json::Map::new();
        if let Some(n) = b.max_documents {
            bm.insert("max_documents".into(), serde_json::json!(n));
        }
        if let Some(n) = b.max_bytes {
            bm.insert("max_bytes".into(), serde_json::json!(n));
        }
        if let Some(n) = b.max_result_bytes {
            bm.insert("max_result_bytes".into(), serde_json::json!(n));
        }
        if !bm.is_empty() {
            args.insert("budget".into(), serde_json::Value::Object(bm));
        }
    }
    Ok(args)
}

fn parse_rql_full_result(
    result: serde_json::Value,
    heap_id: HeapId,
    root_id: CollectionId,
    compiled: crate::query_bytecode_v1::CompiledRqlFull,
) -> Result<crate::query_bytecode_v1::RqlFullPage, Error> {
    let expected_program_hash = compiled.program_hash()?;
    if result.get("profile").and_then(|v| v.as_str()) != Some(crate::RQL_FULL_PROFILE) {
        return Err(Error::ProtocolViolation(
            "rql_query Full response missing rql-full-v1 profile".into(),
        ));
    }
    let rows_v = result
        .get("rows")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::ProtocolViolation("rql_query Full missing rows".into()))?;
    let mut rows = Vec::with_capacity(rows_v.len());
    for row in rows_v {
        let key = row
            .get("key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ProtocolViolation("rql_query Full row key".into()))?;
        let value = row
            .get("value")
            .cloned()
            .ok_or_else(|| Error::ProtocolViolation("rql_query Full row value".into()))?;
        rows.push((key.to_string(), value));
    }

    let base_rows = result
        .get("base_rows")
        .cloned()
        .ok_or_else(|| Error::ProtocolViolation("rql_query Full missing base_rows".into()))?;
    let mut base_result = result.clone();
    base_result
        .as_object_mut()
        .expect("wire result is object when fields are readable")
        .insert("rows".into(), base_rows);
    let base = parse_rql_query_result(base_result, heap_id, root_id)?;
    if base.plan_hash != expected_program_hash {
        return Err(Error::ProtocolViolation(
            "rql_query Full server QVM identity differs from the locally compiled programme".into(),
        ));
    }

    let mut enrich_loads = Vec::new();
    let loads = result
        .get("enrich_loads")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::ProtocolViolation("rql_query Full missing enrich_loads".into()))?;
    for load in loads {
        let using = load
            .get("using")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ProtocolViolation("rql_query Full enrich using".into()))?;
        let output = load
            .get("output")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ProtocolViolation("rql_query Full enrich output".into()))?;
        let mode = match load.get("mode").and_then(|v| v.as_str()) {
            Some("scan_list_keys_get") => crate::query_bytecode_v1::EnrichAttachMode::Scan,
            Some("equality_index") => crate::query_bytecode_v1::EnrichAttachMode::EqualityIndex,
            _ => {
                return Err(Error::ProtocolViolation(
                    "rql_query Full enrich mode".into(),
                ))
            }
        };
        enrich_loads.push(crate::query_bytecode_v1::EnrichLoadEvidence {
            using: using.into(),
            output: output.into(),
            mode,
        });
    }

    Ok(crate::query_bytecode_v1::RqlFullPage {
        profile: crate::RQL_FULL_PROFILE,
        rows,
        base,
        pipeline: compiled.pipeline,
        project: compiled.project,
        enrich_loads,
    })
}

/// Parse op 118 response into [`QueryPage`].
fn parse_rql_query_result(
    result: serde_json::Value,
    fallback_heap: HeapId,
    fallback_coll: CollectionId,
) -> Result<QueryPage, Error> {
    let wire_heap = result
        .get("heap_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::ProtocolViolation("rql_query missing heap_id".into()))?;
    let wire_heap = HeapId::from_str(wire_heap)
        .map_err(|e| Error::ProtocolViolation(format!("rql_query bad heap_id: {e}")))?;
    if wire_heap != fallback_heap {
        return Err(Error::ProtocolViolation(
            "rql_query response crossed the bound Heap identity".into(),
        ));
    }
    let wire_coll = result
        .get("collection_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::ProtocolViolation("rql_query missing collection_id".into()))?;
    let wire_coll = collection_id_from_wire(wire_coll)?;
    if wire_coll != fallback_coll {
        return Err(Error::ProtocolViolation(
            "rql_query response crossed the bound collection identity".into(),
        ));
    }
    let plan_hash_hex = result
        .get("plan_hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::ProtocolViolation("rql_query missing plan_hash".into()))?;
    let plan_hash = hex_to_32(plan_hash_hex)
        .ok_or_else(|| Error::ProtocolViolation("rql_query bad plan_hash".into()))?;
    let query_id_fallback = "0".repeat(32);
    let query_id_hex = result
        .get("query_id")
        .and_then(|v| v.as_str())
        .unwrap_or(query_id_fallback.as_str());
    let query_id = QueryId(hex_to_16(query_id_hex).unwrap_or([0u8; 16]));
    let rows_v = result
        .get("rows")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::ProtocolViolation("rql_query missing rows".into()))?;
    let mut rows = Vec::with_capacity(rows_v.len());
    for row in rows_v {
        let key = row
            .get("key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ProtocolViolation("rql_query row key".into()))?
            .to_string();
        let value = row
            .get("value")
            .cloned()
            .ok_or_else(|| Error::ProtocolViolation("rql_query row value".into()))?;
        rows.push(QueryRow { key, value });
    }
    let next = match result.get("next") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(s)) => Some(Continuation {
            token: s.as_bytes().to_vec(),
        }),
        _ => {
            return Err(Error::ProtocolViolation(
                "rql_query next must be string or null".into(),
            ))
        }
    };
    let exhausted = result
        .get("exhausted")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let complete = result
        .pointer("/coverage/complete")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let cov_mode = match result
        .pointer("/coverage/mode")
        .and_then(|v| v.as_str())
        .unwrap_or("complete")
    {
        "incomplete_allowed" => CoveragePolicy::IncompleteAllowed,
        _ => CoveragePolicy::Complete,
    };
    let cons_mode = match result
        .pointer("/consistency/mode")
        .and_then(|v| v.as_str())
        .unwrap_or("available")
    {
        "current" => ConsistencyMode::Current,
        _ => ConsistencyMode::Available,
    };
    let remaining_limit = result.get("remaining_limit").and_then(|v| v.as_u64());
    let mut known_holes = Vec::new();
    if let Some(arr) = result.get("known_holes").and_then(|v| v.as_array()) {
        for h in arr {
            known_holes.push(HoleEvidence {
                code: h
                    .get("code")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                key: h.get("key").and_then(|v| v.as_str()).map(|s| s.to_string()),
            });
        }
    }
    let examined = (rows.len() + known_holes.len()) as u64;
    let coverage = if complete {
        CoverageEvidence::complete(cov_mode, examined)
    } else {
        CoverageEvidence::incomplete(cov_mode, examined, known_holes.len() as u32)
    };
    Ok(QueryPage {
        query_id,
        plan_hash,
        heap_id: fallback_heap,
        collection_id: fallback_coll,
        rows,
        next,
        exhausted,
        coverage,
        consistency: ConsistencyEvidence { mode: cons_mode },
        remaining_limit,
        known_holes,
        logical_bytes_examined: result
            .get("logical_bytes_examined")
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
        diagnostics: None,
    })
}

fn hex_to_32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn hex_to_16(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Fluent Application Core builder bound to a [`CollectionClient`] (APB-7 T1).
///
/// Wraps [`PlanBuilder`] so builder and RQL compile to the same plan hash when
/// they express the same logical plan. Execution uses
/// [`crate::query_bytecode_v1::execute_bytecode`] (not product wire op 118).
pub struct CollectionQuery<'a> {
    client: &'a mut CollectionClient,
    builder: PlanBuilder,
}

impl<'a> CollectionQuery<'a> {
    fn open(client: &'a mut CollectionClient) -> Self {
        let name = client.name.clone();
        Self {
            client,
            builder: PlanBuilder::from_source(name),
        }
    }

    /// Attach a where predicate.
    pub fn where_(mut self, pred: Predicate) -> Self {
        self.builder = self.builder.where_(pred);
        self
    }

    /// Projection field paths (dotted).
    pub fn project<I, S>(mut self, fields: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.builder = self.builder.project(fields)?;
        Ok(self)
    }

    /// Add an order term (nulls last by default).
    pub fn order_by(mut self, path: &str, dir: OrderDir) -> Result<Self, Error> {
        self.builder = self.builder.order_by(path, dir)?;
        Ok(self)
    }

    /// Add an order term with explicit null/missing placement.
    pub fn order_by_nulls(
        mut self,
        path: &str,
        dir: OrderDir,
        nulls: NullsOrder,
    ) -> Result<Self, Error> {
        self.builder = self.builder.order_by_nulls(path, dir, nulls)?;
        Ok(self)
    }

    /// Total limit across pages.
    pub fn limit(mut self, n: u64) -> Self {
        self.builder = self.builder.limit(n);
        self
    }

    /// Page size.
    pub fn page_size(mut self, n: u32) -> Result<Self, Error> {
        self.builder = self.builder.page_size(n)?;
        Ok(self)
    }

    /// Coverage policy.
    pub fn coverage(mut self, c: CoveragePolicy) -> Self {
        self.builder = self.builder.coverage(c);
        self
    }

    /// Consistency mode.
    pub fn consistency(mut self, c: ConsistencyMode) -> Self {
        self.builder = self.builder.consistency(c);
        self
    }

    fn bindings(&self) -> CollectionBindings {
        let mut bindings = CollectionBindings::default();
        bindings.bind(self.client.name(), self.client.id());
        bindings
    }

    /// Compile to a validated logical plan (no scan).
    pub fn compile(self) -> Result<RqlPlanV1, Error> {
        let bindings = self.bindings();
        let mut plan = self.builder.compile(&bindings)?;
        // Prefer live collection id if binding name resolution ever diverges.
        if plan.from.collection_id != self.client.collection_id {
            plan.from.collection_id = self.client.collection_id;
        }
        Ok(plan)
    }

    /// Explain: plan profile, hash, and canonical tree (no row materialization).
    pub fn explain(self) -> Result<QueryExplanation, Error> {
        let plan = self.compile()?;
        Ok(QueryExplanation {
            plan_profile: plan.profile.clone(),
            plan_hash: plan.plan_hash(),
            tree: plan.to_canonical_json(),
        })
    }

    /// Execute one page against the bound collection (APP-6 executor).
    ///
    /// Unbound clients fail closed. Not a product query claim.
    pub fn run(
        self,
        parameters: &Parameters,
        options: QueryRunOptions,
    ) -> Result<QueryPage, Error> {
        if matches!(self.client.backend, CollectionBackend::Unbound) {
            return Err(Error::Internal(
                "CollectionClient unbound; open via HeapClient (APB-1)".into(),
            ));
        }
        if options.explain {
            return Err(Error::QueryInvalid(
                "use CollectionQuery::explain or explain_rql; run executes rows".into(),
            ));
        }
        let heap_id = self.client.heap_id;
        let collection_id = self.client.collection_id;
        let bindings = self.bindings();
        let mut plan = self.builder.compile(&bindings)?;
        if plan.from.collection_id != collection_id {
            plan.from.collection_id = collection_id;
        }
        let bytecode = crate::query_bytecode_v1::QueryBytecodeV1::from_core_plan(plan, None)?;
        crate::query_bytecode_v1::execute_bytecode(
            self.client,
            &bytecode,
            &parameters.values,
            &options,
            heap_id,
            collection_id,
        )
    }
}

impl crate::query_bytecode_v1::HostCapabilities for HeapClient {
    fn list_keys(
        &mut self,
        collection_id: CollectionId,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        CollectionClient::list_keys(&mut col, limit, after_key)
    }

    fn get_json(
        &mut self,
        collection_id: CollectionId,
        key: &str,
    ) -> Result<Option<serde_json::Value>, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        CollectionClient::get(&mut col, key)
    }

    fn get_json_covered(
        &mut self,
        collection_id: CollectionId,
        key: &str,
    ) -> Result<crate::query_bytecode_v1::HostDocument, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        <CollectionClient as crate::query_bytecode_v1::HostCapabilities>::get_json_covered(
            &mut col,
            collection_id,
            key,
        )
    }

    fn get_json_covered_many(
        &mut self,
        collection_id: CollectionId,
        keys: &[String],
    ) -> Result<Vec<(String, crate::query_bytecode_v1::HostDocument)>, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        <CollectionClient as crate::query_bytecode_v1::HostCapabilities>::get_json_covered_many(
            &mut col,
            collection_id,
            keys,
        )
    }

    fn lookup_index_keys(
        &mut self,
        collection_id: CollectionId,
        equalities: &[(String, serde_json::Value)],
    ) -> Result<Option<Vec<String>>, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        <CollectionClient as crate::query_bytecode_v1::HostCapabilities>::lookup_index_keys(
            &mut col,
            collection_id,
            equalities,
        )
    }

    fn lookup_index_keys_many(
        &mut self,
        collection_id: CollectionId,
        field: &str,
        values: &[serde_json::Value],
    ) -> Result<Option<Vec<String>>, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        <CollectionClient as crate::query_bytecode_v1::HostCapabilities>::lookup_index_keys_many(
            &mut col,
            collection_id,
            field,
            values,
        )
    }

    fn lookup_index_keys_with_constraints(
        &mut self,
        collection_id: CollectionId,
        equalities: &[(String, serde_json::Value)],
        constrained_fields: &[String],
    ) -> Result<Option<Vec<String>>, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        <CollectionClient as crate::query_bytecode_v1::HostCapabilities>::lookup_index_keys_with_constraints(
            &mut col,
            collection_id,
            equalities,
            constrained_fields,
        )
    }

    fn lookup_ordered_index_keys(
        &mut self,
        collection_id: CollectionId,
        order: &[crate::plan_v1::OrderTerm],
    ) -> Result<Option<Vec<String>>, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        <CollectionClient as crate::query_bytecode_v1::HostCapabilities>::lookup_ordered_index_keys(
            &mut col,
            collection_id,
            order,
        )
    }

    fn source_frontier(&mut self, collection_id: CollectionId) -> Result<Option<[u8; 32]>, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        <CollectionClient as crate::query_bytecode_v1::HostCapabilities>::source_frontier(
            &mut col,
            collection_id,
        )
    }

    fn lookup_covering_index_values(
        &mut self,
        collection_id: CollectionId,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<serde_json::Value>>>, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        <CollectionClient as crate::query_bytecode_v1::HostCapabilities>::lookup_covering_index_values(
            &mut col,
            collection_id,
            fields,
        )
    }

    fn scan_projected_values(
        &mut self,
        collection_id: CollectionId,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<serde_json::Value>>>, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        <CollectionClient as crate::query_bytecode_v1::HostCapabilities>::scan_projected_values(
            &mut col,
            collection_id,
            fields,
        )
    }

    fn scan_group_aggregate(
        &mut self,
        collection_id: CollectionId,
        spec: &crate::plan_v1::GroupAggSpec,
        where_k: &crate::CompiledKernelWhere,
        candidate_keys: Option<&[String]>,
    ) -> Result<Option<crate::query_bytecode_v1::HostGroupAggregate>, Error> {
        let mut col = self.open_collection_by_id(collection_id)?;
        <CollectionClient as crate::query_bytecode_v1::HostCapabilities>::scan_group_aggregate(
            &mut col,
            collection_id,
            spec,
            where_k,
            candidate_keys,
        )
    }
}

impl crate::query_bytecode_v1::HostCapabilities for CollectionClient {
    fn list_keys(
        &mut self,
        collection_id: CollectionId,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        CollectionClient::list_keys(self, limit, after_key)
    }

    fn get_json(
        &mut self,
        collection_id: CollectionId,
        key: &str,
    ) -> Result<Option<serde_json::Value>, Error> {
        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        CollectionClient::get(self, key)
    }

    fn get_json_covered(
        &mut self,
        collection_id: CollectionId,
        key: &str,
    ) -> Result<crate::query_bytecode_v1::HostDocument, Error> {
        use crate::query_bytecode_v1::HostDocument;
        use residiuum_store::CollectionScanHoleReason;

        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        match CollectionClient::get(self, key) {
            Ok(Some(value)) => Ok(HostDocument::Present {
                value: value.into(),
                logical_bytes: None,
            }),
            Ok(None) => Ok(HostDocument::Absent),
            Err(Error::Store(store_error)) => {
                if let Some(reason) = CollectionScanHoleReason::from_store_error(&store_error) {
                    Ok(HostDocument::Hole {
                        code: reason.as_str().to_string(),
                    })
                } else {
                    Err(Error::Store(store_error))
                }
            }
            Err(error)
                if matches!(
                    error.code(),
                    crate::error::ErrorCode::DataDamaged
                        | crate::error::ErrorCode::PayloadPartial
                        | crate::error::ErrorCode::CoverageIncomplete
                        | crate::error::ErrorCode::TypeMismatch
                ) =>
            {
                Ok(HostDocument::Hole {
                    code: error.code().as_str().to_string(),
                })
            }
            Err(error) => Err(error),
        }
    }

    fn get_json_covered_many(
        &mut self,
        collection_id: CollectionId,
        keys: &[String],
    ) -> Result<Vec<(String, crate::query_bytecode_v1::HostDocument)>, Error> {
        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        match &self.backend {
            CollectionBackend::Embedded(collection) => collection.get_json_many_covered(keys),
            CollectionBackend::Remote { .. } | CollectionBackend::Unbound => keys
                .iter()
                .map(|key| {
                    let document =
                        <Self as crate::query_bytecode_v1::HostCapabilities>::get_json_covered(
                            self,
                            collection_id,
                            key,
                        )?;
                    Ok((key.clone(), document))
                })
                .collect(),
        }
    }

    fn lookup_index_keys(
        &mut self,
        collection_id: CollectionId,
        equalities: &[(String, serde_json::Value)],
    ) -> Result<Option<Vec<String>>, Error> {
        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        match &self.backend {
            CollectionBackend::Embedded(hc) => hc.lookup_index_keys(equalities),
            // Remote residual: no index-probe wire for Application Core yet.
            CollectionBackend::Remote { .. } | CollectionBackend::Unbound => Ok(None),
        }
    }

    fn lookup_index_keys_many(
        &mut self,
        collection_id: CollectionId,
        field: &str,
        values: &[serde_json::Value],
    ) -> Result<Option<Vec<String>>, Error> {
        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        match &self.backend {
            CollectionBackend::Embedded(hc) => hc.lookup_index_keys_many(field, values),
            CollectionBackend::Remote { .. } | CollectionBackend::Unbound => Ok(None),
        }
    }

    fn lookup_index_keys_with_constraints(
        &mut self,
        collection_id: CollectionId,
        equalities: &[(String, serde_json::Value)],
        constrained_fields: &[String],
    ) -> Result<Option<Vec<String>>, Error> {
        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        match &self.backend {
            CollectionBackend::Embedded(collection) => {
                collection.lookup_index_keys_with_constraints(equalities, constrained_fields)
            }
            CollectionBackend::Remote { .. } | CollectionBackend::Unbound => Ok(None),
        }
    }

    fn lookup_ordered_index_keys(
        &mut self,
        collection_id: CollectionId,
        order: &[crate::plan_v1::OrderTerm],
    ) -> Result<Option<Vec<String>>, Error> {
        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        let Some((field, field_descending, key_descending)) =
            crate::query_bytecode_v1::ordered_index_request(order)
        else {
            return Ok(None);
        };
        match &self.backend {
            CollectionBackend::Embedded(collection) => {
                collection.lookup_ordered_index_keys(&field, field_descending, key_descending)
            }
            CollectionBackend::Remote { .. } | CollectionBackend::Unbound => Ok(None),
        }
    }

    fn source_frontier(&mut self, collection_id: CollectionId) -> Result<Option<[u8; 32]>, Error> {
        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        match &self.backend {
            CollectionBackend::Embedded(collection) => collection.source_frontier().map(Some),
            CollectionBackend::Remote { .. } | CollectionBackend::Unbound => Ok(None),
        }
    }

    fn lookup_covering_index_values(
        &mut self,
        collection_id: CollectionId,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<serde_json::Value>>>, Error> {
        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        match &self.backend {
            CollectionBackend::Embedded(collection) => {
                collection.lookup_covering_index_values(fields)
            }
            CollectionBackend::Remote { .. } | CollectionBackend::Unbound => Ok(None),
        }
    }

    fn scan_projected_values(
        &mut self,
        collection_id: CollectionId,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<serde_json::Value>>>, Error> {
        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        match &self.backend {
            CollectionBackend::Embedded(collection) => collection.scan_projected_values(fields),
            CollectionBackend::Remote { .. } | CollectionBackend::Unbound => Ok(None),
        }
    }

    fn scan_group_aggregate(
        &mut self,
        collection_id: CollectionId,
        spec: &crate::plan_v1::GroupAggSpec,
        where_k: &crate::CompiledKernelWhere,
        candidate_keys: Option<&[String]>,
    ) -> Result<Option<crate::query_bytecode_v1::HostGroupAggregate>, Error> {
        if collection_id != self.collection_id {
            return Err(Error::QueryInvalid(
                "HostCapabilities: collection_id mismatch on CollectionClient".into(),
            ));
        }
        match &self.backend {
            CollectionBackend::Embedded(collection) => {
                collection.scan_group_aggregate(spec, where_k, candidate_keys)
            }
            CollectionBackend::Remote { .. } | CollectionBackend::Unbound => Ok(None),
        }
    }
}

impl crate::query_bytecode_v1::DocScan for CollectionClient {
    fn list_keys(
        &mut self,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        <Self as crate::query_bytecode_v1::HostCapabilities>::list_keys(
            self,
            self.collection_id,
            limit,
            after_key,
        )
    }

    fn get_json(&mut self, key: &str) -> Result<Option<serde_json::Value>, Error> {
        <Self as crate::query_bytecode_v1::HostCapabilities>::get_json(
            self,
            self.collection_id,
            key,
        )
    }

    fn get_json_covered(
        &mut self,
        key: &str,
    ) -> Result<crate::query_bytecode_v1::HostDocument, Error> {
        <Self as crate::query_bytecode_v1::HostCapabilities>::get_json_covered(
            self,
            self.collection_id,
            key,
        )
    }

    fn get_json_covered_many(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<(String, crate::query_bytecode_v1::HostDocument)>, Error> {
        <Self as crate::query_bytecode_v1::HostCapabilities>::get_json_covered_many(
            self,
            self.collection_id,
            keys,
        )
    }

    fn try_equality_index_keys(
        &mut self,
        equalities: &[(String, serde_json::Value)],
    ) -> Result<Option<Vec<String>>, Error> {
        <Self as crate::query_bytecode_v1::HostCapabilities>::lookup_index_keys(
            self,
            self.collection_id,
            equalities,
        )
    }

    fn try_index_keys_with_constraints(
        &mut self,
        equalities: &[(String, serde_json::Value)],
        constrained_fields: &[String],
    ) -> Result<Option<Vec<String>>, Error> {
        <Self as crate::query_bytecode_v1::HostCapabilities>::lookup_index_keys_with_constraints(
            self,
            self.collection_id,
            equalities,
            constrained_fields,
        )
    }

    fn try_ordered_index_keys(
        &mut self,
        order: &[crate::plan_v1::OrderTerm],
    ) -> Result<Option<Vec<String>>, Error> {
        <Self as crate::query_bytecode_v1::HostCapabilities>::lookup_ordered_index_keys(
            self,
            self.collection_id,
            order,
        )
    }

    fn try_source_frontier(&mut self) -> Result<Option<[u8; 32]>, Error> {
        <Self as crate::query_bytecode_v1::HostCapabilities>::source_frontier(
            self,
            self.collection_id,
        )
    }

    fn try_covering_index_values(
        &mut self,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<serde_json::Value>>>, Error> {
        <Self as crate::query_bytecode_v1::HostCapabilities>::lookup_covering_index_values(
            self,
            self.collection_id,
            fields,
        )
    }

    fn try_projected_values(
        &mut self,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<serde_json::Value>>>, Error> {
        <Self as crate::query_bytecode_v1::HostCapabilities>::scan_projected_values(
            self,
            self.collection_id,
            fields,
        )
    }

    fn try_group_aggregate(
        &mut self,
        spec: &crate::plan_v1::GroupAggSpec,
        where_k: &crate::CompiledKernelWhere,
        candidate_keys: Option<&[String]>,
    ) -> Result<Option<crate::query_bytecode_v1::HostGroupAggregate>, Error> {
        <Self as crate::query_bytecode_v1::HostCapabilities>::scan_group_aggregate(
            self,
            self.collection_id,
            spec,
            where_k,
            candidate_keys,
        )
    }
}

/// Collection handle opened under a [`ReadView`] pin gate (APB-7 T5).
///
/// Each `rql` / builder `run` re-checks
/// [`ReadView::ensure_observation_stable`]. This is **not** snapshot isolation:
/// rows are still live `list_keys`+`get`; the gate only fails closed when the
/// segment-fingerprint frontier is Unpinned or Drifted.
pub struct ViewBoundCollection<'a> {
    view: &'a mut crate::read_view_v1::ReadView,
    inner: CollectionClient,
}

impl<'a> ViewBoundCollection<'a> {
    /// Bound collection id.
    pub fn id(&self) -> CollectionId {
        self.inner.id()
    }

    /// Canonical collection name.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Owning heap id.
    pub fn heap_id(&self) -> HeapId {
        self.inner.heap_id()
    }

    /// Underlying live client (advanced; prefer `rql` / `query` so pin is checked).
    pub fn inner_mut(&mut self) -> &mut CollectionClient {
        &mut self.inner
    }

    /// Application Core builder under this view (pin checked at `run`).
    pub fn query(&mut self) -> ViewBoundQuery<'_> {
        let source_name = self.inner.name().to_string();
        ViewBoundQuery {
            view: self.view,
            client: &mut self.inner,
            builder: PlanBuilder::from_source(source_name),
        }
    }

    /// RQL Application Core page under the view pin (APB-7 T5 / APB-6 T3).
    ///
    /// Re-checks pin stability and records examined documents toward retention
    /// `max_pinned_documents` (multipage honesty under the view).
    pub fn rql(
        &mut self,
        source: &str,
        parameters: &Parameters,
        options: QueryRunOptions,
    ) -> Result<QueryPage, Error> {
        self.view.ensure_observation_stable()?;
        let page = self.inner.rql(source, parameters, options)?;
        // Re-check pin after the page (mutation-between-pages residual honesty).
        self.view.ensure_observation_stable()?;
        self.view
            .note_examined_documents(page.coverage.examined_documents)?;
        Ok(page)
    }

    /// Explain under the view pin (no row scan; still requires Stable).
    pub fn explain_rql(
        &mut self,
        source: &str,
        parameters: &Parameters,
        options: QueryRunOptions,
    ) -> Result<QueryExplanation, Error> {
        self.view.ensure_observation_stable()?;
        self.inner.explain_rql(source, parameters, options)
    }
}

/// Builder query bound to a [`ViewBoundCollection`] (pin re-checked at `run`).
pub struct ViewBoundQuery<'a> {
    view: &'a mut crate::read_view_v1::ReadView,
    client: &'a mut CollectionClient,
    builder: PlanBuilder,
}

impl<'a> ViewBoundQuery<'a> {
    /// Attach a where predicate.
    pub fn where_(mut self, pred: Predicate) -> Self {
        self.builder = self.builder.where_(pred);
        self
    }

    /// Projection field paths (dotted).
    pub fn project<I, S>(mut self, fields: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.builder = self.builder.project(fields)?;
        Ok(self)
    }

    /// Add an order term (nulls last by default).
    pub fn order_by(mut self, path: &str, dir: OrderDir) -> Result<Self, Error> {
        self.builder = self.builder.order_by(path, dir)?;
        Ok(self)
    }

    /// Total limit across pages.
    pub fn limit(mut self, n: u64) -> Self {
        self.builder = self.builder.limit(n);
        self
    }

    /// Page size.
    pub fn page_size(mut self, n: u32) -> Result<Self, Error> {
        self.builder = self.builder.page_size(n)?;
        Ok(self)
    }

    /// Compile without executing (still requires Stable pin).
    pub fn compile(self) -> Result<RqlPlanV1, Error> {
        self.view.ensure_observation_stable()?;
        let mut bindings = CollectionBindings::default();
        bindings.bind(self.client.name(), self.client.id());
        let mut plan = self.builder.compile(&bindings)?;
        if plan.from.collection_id != self.client.collection_id {
            plan.from.collection_id = self.client.collection_id;
        }
        Ok(plan)
    }

    /// Execute one page; re-checks pin before/after and records retention docs.
    pub fn run(
        self,
        parameters: &Parameters,
        options: QueryRunOptions,
    ) -> Result<QueryPage, Error> {
        self.view.ensure_observation_stable()?;
        if options.explain {
            return Err(Error::QueryInvalid(
                "use explain_rql; ViewBoundQuery::run executes rows".into(),
            ));
        }
        let heap_id = self.client.heap_id;
        let collection_id = self.client.collection_id;
        let mut bindings = CollectionBindings::default();
        bindings.bind(self.client.name(), self.client.id());
        let mut plan = self.builder.compile(&bindings)?;
        if plan.from.collection_id != collection_id {
            plan.from.collection_id = collection_id;
        }
        let page = crate::query_bytecode_v1::execute_bytecode(
            self.client,
            &crate::query_bytecode_v1::QueryBytecodeV1::from_core_plan(plan, None)?,
            &parameters.values,
            &options,
            heap_id,
            collection_id,
        )?;
        self.view.ensure_observation_stable()?;
        self.view
            .note_examined_documents(page.coverage.examined_documents)?;
        Ok(page)
    }
}

impl crate::read_view_v1::ReadView {
    /// Open a collection for view-bound Application Core query (APB-7 T5).
    ///
    /// Requires [`Self::ensure_observation_stable`] (pinned + Stable drift).
    /// `heap` must be the same Heap as this view. Returns a
    /// [`ViewBoundCollection`] that re-checks the pin on each page.
    ///
    /// **Not** snapshot isolation — live documents are read under a frontier
    /// gate only.
    pub fn open_collection(
        &mut self,
        heap: &mut HeapClient,
        name: &str,
    ) -> Result<ViewBoundCollection<'_>, Error> {
        self.ensure_observation_stable()?;
        if heap.id() != self.heap_id() {
            return Err(Error::ConsistencyViolation(
                "read view heap_id does not match HeapClient; refuse view-bound open".into(),
            ));
        }
        let inner = heap.open_collection(name)?;
        Ok(ViewBoundCollection { view: self, inner })
    }
}

/// Secondary index administration bound to a [`CollectionClient`] (APB-1 G3).
///
/// Obtained via [`CollectionClient::indexes`]. Mirrors baseline ops
/// `apb.index.list|create|drop|rebuild`.
pub struct IndexManager<'a> {
    client: &'a mut CollectionClient,
}

impl IndexManager<'_> {
    /// List secondary indexes on this collection.
    pub fn list(&mut self) -> Result<Vec<IndexInfo>, Error> {
        match &self.client.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "IndexManager unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.list_indexes(),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let mut guard = lock_remote(remote)?;
                let rows = guard.index_list(wire_collection_id)?;
                let display = self.client.name.clone();
                rows.into_iter()
                    .map(|v| index_info_from_remote_json(v, &display))
                    .collect()
            }
        }
    }

    /// Create (or rebuild-by-create) a field index. Requires IndexAdmin on remote/embedded caps.
    pub fn create(&mut self, name: &str, fields: &[&str]) -> Result<IndexInfo, Error> {
        match &self.client.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "IndexManager unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.create_index(name, fields),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let mut guard = lock_remote(remote)?;
                let row = guard.index_create(wire_collection_id, name, fields)?;
                index_info_from_remote_json(row, &self.client.name)
            }
        }
    }

    /// Drop a secondary index by name.
    pub fn drop(&mut self, name: &str) -> Result<(), Error> {
        match &self.client.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "IndexManager unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.drop_index(name),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let mut guard = lock_remote(remote)?;
                let _ = guard.index_drop(wire_collection_id, name)?;
                Ok(())
            }
        }
    }

    /// Rebuild an existing index definition from live data.
    pub fn rebuild(&mut self, name: &str) -> Result<IndexInfo, Error> {
        match &self.client.backend {
            CollectionBackend::Unbound => Err(Error::Internal(
                "IndexManager unbound; open via HeapClient (APB-1)".into(),
            )),
            CollectionBackend::Embedded(hc) => hc.rebuild_index(name),
            CollectionBackend::Remote {
                remote,
                wire_collection_id,
            } => {
                let mut guard = lock_remote(remote)?;
                let row = guard.index_rebuild(wire_collection_id, name)?;
                index_info_from_remote_json(row, &self.client.name)
            }
        }
    }

    /// Get one index by name (list + find).
    pub fn get(&mut self, name: &str) -> Result<Option<IndexInfo>, Error> {
        Ok(self.list()?.into_iter().find(|i| i.name == name))
    }
}

/// Map remote put event/version hex ids into a façade [`WriteReceipt`].
///
/// Wire put does not yet return store/segment ids; those fields are zeroed.
fn write_receipt_from_remote_ids(
    key: &str,
    event_id_s: &str,
    _version_s: &str,
) -> Result<WriteReceipt, Error> {
    // Public OCC version is always the establishing event id (matches embedded
    // WriteReceipt::from_store and post-APB-2 server put receipts).
    let event_id = parse_hex16(event_id_s).unwrap_or([0u8; 16]);
    Ok(WriteReceipt {
        key: key.to_string(),
        event_id,
        version: event_id,
        acknowledgement: DurabilityMode::Durable,
        committed: true,
        store_id: [0u8; 16],
        segment_id: [0u8; 16],
    })
}

/// Project remote index metadata JSON (ops 130–133) into [`IndexInfo`].
fn index_info_from_remote_json(
    row: serde_json::Value,
    display_collection: &str,
) -> Result<IndexInfo, Error> {
    let name = row
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::ProtocolViolation("index metadata missing name".into()))?
        .to_string();
    let fields: Vec<String> = row
        .get("fields")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let state_s = row.get("state").and_then(|v| v.as_str()).unwrap_or("ready");
    let state = IndexState::parse(state_s).ok_or_else(|| {
        Error::ProtocolViolation(format!("unknown index state from remote: {state_s}"))
    })?;
    let entry_count = row.get("entry_count").and_then(|v| v.as_u64()).unwrap_or(0);
    let complete_coverage = row
        .get("complete_coverage")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let failure_reason = row
        .get("failure_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let build_id_hex = row
        .get("build_id_hex")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let collection = row
        .get("collection")
        .and_then(|v| v.as_str())
        .unwrap_or(display_collection)
        .to_string();
    Ok(IndexInfo {
        name,
        collection,
        fields,
        state,
        entry_count,
        complete_coverage,
        failure_reason,
        build_id_hex,
    })
}

/// Project remote op-117 version rows (JSON objects) into [`KeyHistory`].
fn key_history_from_remote_versions(
    key: &str,
    versions: Vec<serde_json::Value>,
    has_known_holes: bool,
) -> Result<KeyHistory, Error> {
    let mut out = Vec::with_capacity(versions.len());
    for row in versions {
        out.push(version_from_remote_json(row)?);
    }
    Ok(KeyHistory {
        key: key.to_string(),
        versions: out,
        has_known_holes,
    })
}

fn version_from_remote_json(row: serde_json::Value) -> Result<Version, Error> {
    let kind_s = row
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::ProtocolViolation("history version missing kind".into()))?;
    let kind = match kind_s {
        "put" => "put",
        "delete" => "delete",
        other => {
            return Err(Error::ProtocolViolation(format!(
                "unknown history kind from remote: {other}"
            )))
        }
    };
    let event_id = row
        .get("event_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let item_id = row
        .get("item_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let segment_id = row
        .get("segment_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let known_gap_before = row
        .get("known_gap_before")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let json = row.get("json").cloned().filter(|v| !v.is_null());
    // RemoteHeap history rows typically carry `json` for puts; raw body is optional
    // and left unset on this façade path (no base64 dep in app_v1).
    let _ = row.get("body_b64");
    Ok(Version {
        kind,
        event_id,
        item_id,
        segment_id,
        json,
        body: None,
        known_gap_before,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_are_stable() {
        assert_eq!(RUST_APP_PROFILE, "residiuum-rust-app-v1");
        assert_eq!(RQL_APP_CORE_PROFILE, "rql-app-core-v1");
        assert_eq!(RQL_PLAN_PROFILE, "rql-plan-v1");
        assert_eq!(CURSOR_PROFILE, "residiuum-cursor-v1");
        assert_eq!(PREDICATE_PROFILE, "residiuum-predicate-v1");
    }

    fn v4(seed: u8) -> [u8; 16] {
        let mut b = [seed; 16];
        b[6] = (b[6] & 0x0f) | 0x40;
        b[8] = (b[8] & 0x3f) | 0x80;
        if b == [0u8; 16] {
            b[0] = 1;
        }
        b
    }

    #[test]
    fn facade_constructors_compile() {
        let hid = HeapId::from_bytes(v4(1)).expect("heap id");
        let cid = CollectionId::from_bytes(v4(2)).expect("collection id");
        let mut heap = HeapClient::from_id_for_contract(hid);
        assert_eq!(heap.id(), hid);
        let col = CollectionClient::from_parts_for_contract(hid, cid, "orders");
        assert_eq!(col.name(), "orders");
        assert_eq!(col.id(), cid);
        let err = heap.create_collection("orders").unwrap_err();
        assert_eq!(err.code().as_str(), "internal");
    }
}
