//! Bounded embedded application driver (DRV-1 / DRV-5 foundation).
//!
//! The synchronous storage kernel remains authoritative. This module owns the
//! async boundary: bounded admission, dedicated workers, cloneable handles,
//! request identity, cancellation-before-dispatch, deadlines, telemetry, and
//! orderly shared shutdown. It intentionally contains no second query engine.

use crate::heap::{Heap, HeapCollection, ResidiuumDeployment, SignedCursor};
use crate::{
    DeleteReceipt as SdkDeleteReceipt, Error as SdkError, WriteReceipt as SdkWriteReceipt,
};
pub use residiuum_client::{OperationId, RequestId, RetryDisposition, TerminalOutcome};
pub use residiuum_heap::HeapCap;
use residiuum_heap::{CollectionId, HeapId};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Embedded driver defaults from `ASYNC_DRIVER_SPINE_SPEC.md` §9.
#[derive(Debug, Clone)]
pub struct EmbeddedOptions {
    /// Existing Residiuum deployment root.
    pub path: PathBuf,
    /// Dedicated synchronous-kernel workers.
    pub workers: usize,
    /// Hard bound on queued operations, in addition to running workers.
    pub queue_capacity: usize,
}

impl EmbeddedOptions {
    /// Options with product defaults: min(4, available parallelism), queue 1024.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let workers = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(4)
            .max(1);
        Self {
            path: path.into(),
            workers,
            queue_capacity: 1024,
        }
    }

    /// Override the worker bound. Zero is rejected by [`Client::open_embedded`].
    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    /// Override the queue bound. Zero is rejected by [`Client::open_embedded`].
    pub fn queue_capacity(mut self, queue_capacity: usize) -> Self {
        self.queue_capacity = queue_capacity;
        self
    }
}

/// Per-operation identity and deadline overrides.
#[derive(Debug, Clone, Default)]
pub struct OperationContext {
    /// Stable request identity. Minted when absent.
    pub request_id: Option<RequestId>,
    /// Stable mutation identity. Minted before mutation admission when absent.
    pub operation_id: Option<OperationId>,
    /// One monotonic end-to-end deadline.
    pub deadline: Option<Instant>,
}

impl OperationContext {
    /// Set a deadline relative to now.
    pub fn timeout(mut self, duration: Duration) -> Self {
        let now = Instant::now();
        self.deadline = Some(now.checked_add(duration).unwrap_or(now));
        self
    }
}

/// Options for an idempotent smart-client put.
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    /// Request identity and deadline.
    pub context: OperationContext,
}

/// Options for create-if-absent.
#[derive(Debug, Clone, Default)]
pub struct CreateOptions {
    /// Request identity, mutation identity, and deadline.
    pub context: OperationContext,
}

/// Options for version-conditional replacement.
#[derive(Debug, Clone)]
pub struct ReplaceOptions {
    /// Establishing event id that must still be live.
    pub if_version: [u8; 16],
    /// Request identity, mutation identity, and deadline.
    pub context: OperationContext,
}

/// Options for a conditional delete.
#[derive(Debug, Clone)]
pub struct DeleteOptions {
    /// Optional establishing event id that must still be live.
    pub if_version: Option<[u8; 16]>,
    /// Require the key to be present. The embedded v1 driver refuses `false`
    /// until absence outcomes are included in the persisted dedup record.
    pub if_present: bool,
    /// Request identity, mutation identity, and deadline.
    pub context: OperationContext,
}

impl Default for DeleteOptions {
    fn default() -> Self {
        Self {
            if_version: None,
            if_present: true,
            context: OperationContext::default(),
        }
    }
}

/// Options for idempotent collection creation.
#[derive(Debug, Clone, Default)]
pub struct CreateCollectionOptions {
    /// Request identity, mutation identity, and deadline.
    pub context: OperationContext,
}

/// Maximum complete rows admitted in one embedded scan page.
pub const MAX_SCAN_PAGE_SIZE: usize = 1_000;

/// Bounded collection scan options.
#[derive(Debug, Clone)]
pub struct ScanOptions {
    /// Complete-row target for this page (`1..=MAX_SCAN_PAGE_SIZE`).
    pub page_size: usize,
    /// Opaque continuation returned by the preceding page.
    pub continuation: Option<ScanContinuation>,
    /// Request identity and deadline. `operation_id` must be absent for reads.
    pub context: OperationContext,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            page_size: 64,
            continuation: None,
            context: OperationContext::default(),
        }
    }
}

/// Capability- and collection-bound scan continuation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanContinuation(SignedCursor);

/// One fully decoded row in a scan page.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanRow<T> {
    /// Application key.
    pub key: String,
    /// Decoded typed value.
    pub value: T,
    /// Event that established this live value; usable for conditional mutation.
    pub version: [u8; 16],
}

/// One typed value paired with its establishing event identifier.
#[derive(Debug, Clone, PartialEq)]
pub struct VersionedValue<T> {
    /// Decoded typed value.
    pub value: T,
    /// Event that established this live value; usable for conditional mutation.
    pub version: [u8; 16],
}

/// A live key whose body could not be resolved completely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanHole {
    /// Application key, lossily decoded only when damaged key bytes are not UTF-8.
    pub key: String,
    /// Stable storage hole classification.
    pub reason: &'static str,
}

/// One bounded, coverage-honest scan page.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanPage<T> {
    /// Fully decoded rows, ordered by application key.
    pub rows: Vec<ScanRow<T>>,
    /// Examined live keys whose payload could not be completed.
    pub incomplete: Vec<ScanHole>,
    /// Number of live keys examined, including incomplete keys.
    pub examined: usize,
    /// True only when this page contains no incomplete key.
    pub complete: bool,
    /// Opaque continuation when more live keys remain.
    pub continuation: Option<ScanContinuation>,
}

/// Capabilities of this driver handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// Operations execute through the bounded embedded scheduler.
    pub embedded: bool,
    /// One physical connection can host multiple authorized Heap bindings.
    pub multi_heap_bindings: bool,
    /// Mutation outcomes are persisted and replayable by operation identity.
    pub mutation_identity: bool,
    /// Query streaming is available.
    pub query_streaming: bool,
    /// Bounded collection scan pages are available.
    pub bounded_scan_pages: bool,
    /// Heap-local multi-record Atomics are implemented.
    pub atomics: bool,
    /// Remote transport pooling is available.
    pub remote_pooling: bool,
}

/// Stable smart-driver error code. Applications never parse messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Driver options or application input were invalid.
    Validation,
    /// Bounded admission queue was full.
    Overloaded,
    /// Shared client was already closed.
    Closed,
    /// Deadline expired before a result was available.
    DeadlineExceeded,
    /// A dispatched mutation crossed its deadline and requires outcome lookup.
    CommitOutcomeUnknown,
    /// Work was cancelled before reaching a kernel worker.
    CancelledBeforeDispatch,
    /// Operation identity was reused with different canonical content.
    OperationIdentityConflict,
    /// Capability or authentication refused the operation.
    PermissionDenied,
    /// Requested entity was absent.
    NotFound,
    /// Optimistic condition failed.
    Conflict,
    /// Stored data or protocol evidence was damaged.
    DataDamaged,
    /// Underlying storage or I/O failed.
    Unavailable,
    /// Unexpected internal failure.
    Internal,
}

/// Stable high-level error class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Caller can correct the request.
    Request,
    /// Driver refused work before dispatch.
    Admission,
    /// Deadline or cancellation ended waiting.
    Cancellation,
    /// Storage/capability service failure.
    Service,
    /// Internal invariant failure.
    Internal,
}

/// Structured smart-driver error with request identity and retry decision.
#[derive(Debug)]
pub struct Error {
    /// Stable code.
    pub code: ErrorCode,
    /// Broad handling class.
    pub class: ErrorClass,
    /// Operator-readable detail; not a decision interface.
    pub message: String,
    /// Logical request identity.
    pub request_id: Option<RequestId>,
    /// Mutation identity when applicable.
    pub operation_id: Option<OperationId>,
    /// Machine-actionable retry decision.
    pub retry: RetryDisposition,
    /// Terminal class recorded by the driver.
    pub terminal: TerminalOutcome,
    source: Option<SdkError>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl Error {
    fn local(code: ErrorCode, class: ErrorClass, message: impl Into<String>) -> Self {
        Self {
            code,
            class,
            message: message.into(),
            request_id: None,
            operation_id: None,
            retry: RetryDisposition::Never,
            terminal: TerminalOutcome::Refused,
            source: None,
        }
    }

    fn for_request(
        code: ErrorCode,
        class: ErrorClass,
        message: impl Into<String>,
        request_id: RequestId,
        operation_id: Option<OperationId>,
        terminal: TerminalOutcome,
        retry: RetryDisposition,
    ) -> Self {
        Self {
            code,
            class,
            message: message.into(),
            request_id: Some(request_id),
            operation_id,
            retry,
            terminal,
            source: None,
        }
    }

    fn from_sdk(
        source: SdkError,
        request_id: RequestId,
        operation_id: Option<OperationId>,
    ) -> Self {
        let (code, class, retry) = match &source {
            SdkError::Store(residiuum_store::StoreError::OperationIdentityConflict) => (
                ErrorCode::OperationIdentityConflict,
                ErrorClass::Request,
                RetryDisposition::Never,
            ),
            _ => match source.code() {
                crate::ErrorCode::ValidationFailed | crate::ErrorCode::QueryInvalid => (
                    ErrorCode::Validation,
                    ErrorClass::Request,
                    RetryDisposition::Never,
                ),
                crate::ErrorCode::PermissionDenied | crate::ErrorCode::AuthenticationFailed => (
                    ErrorCode::PermissionDenied,
                    ErrorClass::Service,
                    RetryDisposition::Never,
                ),
                crate::ErrorCode::NotFound => (
                    ErrorCode::NotFound,
                    ErrorClass::Request,
                    RetryDisposition::Never,
                ),
                crate::ErrorCode::AlreadyExists
                | crate::ErrorCode::VersionConflict
                | crate::ErrorCode::ConsistencyViolation => (
                    ErrorCode::Conflict,
                    ErrorClass::Request,
                    RetryDisposition::Never,
                ),
                crate::ErrorCode::DataDamaged
                | crate::ErrorCode::PayloadPartial
                | crate::ErrorCode::CoverageIncomplete => (
                    ErrorCode::DataDamaged,
                    ErrorClass::Service,
                    RetryDisposition::Never,
                ),
                crate::ErrorCode::DeadlineExceeded => (
                    ErrorCode::DeadlineExceeded,
                    ErrorClass::Cancellation,
                    RetryDisposition::Never,
                ),
                crate::ErrorCode::Io
                | crate::ErrorCode::PartitionUnavailable
                | crate::ErrorCode::WriterLockHeld => (
                    ErrorCode::Unavailable,
                    ErrorClass::Service,
                    if operation_id.is_some() {
                        RetryDisposition::OutcomeLookupRequired
                    } else {
                        RetryDisposition::SafeSameRequest
                    },
                ),
                _ => (
                    ErrorCode::Internal,
                    ErrorClass::Internal,
                    RetryDisposition::Never,
                ),
            },
        };
        let message = source.to_string();
        Self {
            code,
            class,
            message,
            request_id: Some(request_id),
            operation_id,
            retry,
            terminal: TerminalOutcome::Refused,
            source: Some(source),
        }
    }
}

/// Complete smart-client write receipt.
#[derive(Debug, Clone)]
pub struct WriteReceipt {
    /// Request identity.
    pub request_id: RequestId,
    /// Stable mutation identity.
    pub operation_id: OperationId,
    /// Bound Heap.
    pub heap_id: HeapId,
    /// Bound collection.
    pub collection_id: CollectionId,
    /// True when the original stored outcome was returned.
    pub deduplicated: bool,
    /// Existing SDK/store receipt fields.
    pub storage: SdkWriteReceipt,
}

/// Complete smart-client delete receipt.
#[derive(Debug, Clone)]
pub struct DeleteReceipt {
    /// Request identity.
    pub request_id: RequestId,
    /// Stable mutation identity.
    pub operation_id: OperationId,
    /// Bound Heap.
    pub heap_id: HeapId,
    /// Bound collection.
    pub collection_id: CollectionId,
    /// True when the original stored outcome was returned.
    pub deduplicated: bool,
    /// Existing SDK/store receipt fields.
    pub storage: SdkDeleteReceipt,
}

/// Redacted bounded scheduler state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientInspection {
    /// Dedicated worker bound.
    pub workers: usize,
    /// Admission queue bound.
    pub queue_capacity: usize,
    /// Currently queued jobs.
    pub queued: usize,
    /// Currently executing jobs.
    pub running: usize,
    /// Completed jobs.
    pub completed: u64,
    /// Admission refusals.
    pub refused: u64,
    /// Jobs cancelled before dispatch.
    pub cancelled_before_dispatch: u64,
    /// Whether shared shutdown began.
    pub closed: bool,
}

/// Async connection to one physical Residiuum deployment.
///
/// The connection owns exactly one physical writer and one bounded scheduler.
/// Any number of authorized [`HeapClient`] bindings may share it.
#[derive(Clone)]
pub struct Client {
    deployment: Arc<ResidiuumDeployment>,
    scheduler: Arc<Scheduler>,
    open_report: crate::StoreOpenReport,
    capabilities: Capabilities,
}

impl Client {
    /// Open an embedded deployment off the caller's async executor thread.
    pub async fn open_embedded(options: EmbeddedOptions) -> Result<Self, Error> {
        if options.workers == 0 || options.queue_capacity == 0 {
            return Err(Error::local(
                ErrorCode::Validation,
                ErrorClass::Request,
                "embedded workers and queue capacity must be non-zero",
            ));
        }
        let workers = options.workers;
        let queue_capacity = options.queue_capacity;
        let opened = run_open(move || {
            let deployment = ResidiuumDeployment::open(&options.path)?;
            let report = deployment.open_report()?;
            Ok((deployment, report))
        })
        .await?;
        Ok(Self {
            deployment: Arc::new(opened.0),
            scheduler: Arc::new(Scheduler::new(workers, queue_capacity)?),
            open_report: opened.1,
            capabilities: Capabilities {
                embedded: true,
                multi_heap_bindings: true,
                mutation_identity: true,
                query_streaming: false,
                bounded_scan_pages: true,
                atomics: false,
                remote_pooling: false,
            },
        })
    }

    /// Bind one validated capability to a Heap on this connection.
    pub async fn open_heap(&self, capability: HeapCap) -> Result<HeapClient, Error> {
        let deployment = Arc::clone(&self.deployment);
        let request_id = mint_request_id()?;
        let heap = self
            .scheduler
            .dispatch(request_id, None, None, move || {
                Ok(deployment.open_heap(capability))
            })
            .await?;
        Ok(HeapClient {
            connection: self.clone(),
            heap: Arc::new(heap),
        })
    }

    /// Bind a capability only when it resolves to the requested Heap name.
    pub async fn open_named_heap(
        &self,
        name: &str,
        capability: HeapCap,
    ) -> Result<HeapClient, Error> {
        let name = name.to_string();
        let deployment = Arc::clone(&self.deployment);
        let request_id = mint_request_id()?;
        let heap = self
            .scheduler
            .dispatch(request_id, None, None, move || {
                deployment.open_named_heap(&name, capability)
            })
            .await?;
        Ok(HeapClient {
            connection: self.clone(),
            heap: Arc::new(heap),
        })
    }

    /// Negotiated/implemented behavior for this handle.
    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Structured report from the physical store open.
    pub fn open_report(&self) -> crate::StoreOpenReport {
        self.open_report
    }

    /// Redacted bounded scheduler state; performs no store scan.
    pub fn inspect(&self) -> ClientInspection {
        self.scheduler.inspect()
    }

    /// Close the physical connection's shared scheduler.
    ///
    /// Every Heap and collection binding created from this connection observes
    /// the same closed state.
    pub async fn close(&self) -> Result<(), Error> {
        self.scheduler.close().await
    }
}

/// One capability-authorized Heap binding within a shared [`Client`].
#[derive(Clone)]
pub struct HeapClient {
    connection: Client,
    heap: Arc<Heap>,
}

impl HeapClient {
    /// Shared physical connection that owns this binding.
    pub fn connection(&self) -> &Client {
        &self.connection
    }

    /// Bound Heap identity.
    pub fn heap_id(&self) -> HeapId {
        self.heap.id()
    }

    /// Negotiated/implemented behavior of the shared connection.
    pub fn capabilities(&self) -> &Capabilities {
        self.connection.capabilities()
    }

    /// Structured report from the one physical store open.
    pub fn open_report(&self) -> crate::StoreOpenReport {
        self.connection.open_report()
    }

    /// Open a typed collection handle without blocking the caller executor.
    pub async fn open_collection<T>(&self, name: &str) -> Result<Collection<T>, Error>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let name = name.to_string();
        let heap = Arc::clone(&self.heap);
        let request_id = mint_request_id()?;
        let inner = self
            .connection
            .scheduler
            .dispatch(request_id, None, None, move || heap.collection(&name))
            .await?;
        Ok(Collection {
            heap_client: self.clone(),
            inner,
            marker: PhantomData,
        })
    }

    /// Create a collection idempotently and return its typed handle.
    pub async fn create_collection<T>(
        &self,
        name: &str,
        options: CreateCollectionOptions,
    ) -> Result<Collection<T>, Error>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let name = name.to_string();
        let request_id = resolve_request_id(options.context.request_id)?;
        let operation_id = resolve_operation_id(options.context.operation_id)?;
        let deadline = options.context.deadline;
        let heap = Arc::clone(&self.heap);
        let created = self
            .connection
            .scheduler
            .dispatch(request_id, Some(operation_id), deadline, move || {
                heap.create_collection_with(&name, Some(operation_id.0))
                    .map(|created| created.collection)
            })
            .await?;
        Ok(Collection {
            heap_client: self.clone(),
            inner: created,
            marker: PhantomData,
        })
    }

    /// List active collections in canonical name order.
    pub async fn list_collections(&self) -> Result<Vec<crate::CollectionInfo>, Error> {
        let request_id = mint_request_id()?;
        let heap = Arc::clone(&self.heap);
        self.connection
            .scheduler
            .dispatch(request_id, None, None, move || {
                heap.list_collections().map(|items| {
                    items
                        .into_iter()
                        .map(|item| crate::CollectionInfo {
                            heap_id: item.heap_id,
                            collection_id: item.collection_id,
                            name: item.name,
                            descriptor_hash: item.descriptor_hash,
                        })
                        .collect()
                })
            })
            .await
    }

    /// Redacted state of the shared connection scheduler.
    pub fn inspect(&self) -> ClientInspection {
        self.connection.inspect()
    }
}

/// Cloneable typed collection handle. Ordinary operations use `&self`.
pub struct Collection<T = serde_json::Value> {
    heap_client: HeapClient,
    inner: HeapCollection,
    marker: PhantomData<fn() -> T>,
}

impl<T> Clone for Collection<T> {
    fn clone(&self) -> Self {
        Self {
            heap_client: self.heap_client.clone(),
            inner: self.inner.clone(),
            marker: PhantomData,
        }
    }
}

impl<T> Collection<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Owning Heap.
    pub fn heap_id(&self) -> HeapId {
        self.inner.heap_id()
    }

    /// Immutable collection identity.
    pub fn id(&self) -> CollectionId {
        self.inner.id()
    }

    /// Name observed when this handle was opened.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Idempotent durable put with a freshly minted operation identity.
    pub async fn put(&self, key: impl Into<String>, value: &T) -> Result<WriteReceipt, Error> {
        self.put_with(key, value, PutOptions::default()).await
    }

    /// Idempotent durable put with explicit request context.
    pub async fn put_with(
        &self,
        key: impl Into<String>,
        value: &T,
        options: PutOptions,
    ) -> Result<WriteReceipt, Error> {
        let key = key.into();
        let json = serde_json::to_value(value)
            .map_err(|e| Error::local(ErrorCode::Validation, ErrorClass::Request, e.to_string()))?;
        let body = crate::value::encode_json(&json)
            .map_err(|e| Error::local(ErrorCode::Validation, ErrorClass::Request, e.to_string()))?;
        let request_id = resolve_request_id(options.context.request_id)?;
        let operation_id = resolve_operation_id(options.context.operation_id)?;
        let deadline = options.context.deadline;
        let content_hash = canonical_mutation_hash(
            b"put-v1",
            self.heap_id(),
            self.id(),
            key.as_bytes(),
            &[body.as_slice()],
        );
        let collection = self.inner.clone();
        let heap_id = self.heap_id();
        let collection_id = self.id();
        self.heap_client
            .connection
            .scheduler
            .dispatch(request_id, Some(operation_id), deadline, move || {
                let (storage, deduplicated) = collection.put_raw_body_with_operation(
                    &key,
                    &body,
                    residiuum_store::WriteCondition::Unconditional,
                    operation_id.0,
                    content_hash,
                )?;
                Ok(WriteReceipt {
                    request_id,
                    operation_id,
                    heap_id,
                    collection_id,
                    deduplicated,
                    storage,
                })
            })
            .await
    }

    /// Insert only when the key is absent.
    pub async fn create(
        &self,
        key: impl Into<String>,
        value: &T,
        options: CreateOptions,
    ) -> Result<WriteReceipt, Error> {
        self.write_conditionally(
            key.into(),
            value,
            options.context,
            residiuum_store::WriteCondition::Absent,
            b"create-absent-v1",
        )
        .await
    }

    /// Replace only when the supplied establishing version is still live.
    pub async fn replace(
        &self,
        key: impl Into<String>,
        value: &T,
        options: ReplaceOptions,
    ) -> Result<WriteReceipt, Error> {
        self.write_conditionally(
            key.into(),
            value,
            options.context,
            residiuum_store::WriteCondition::LiveEventId(options.if_version),
            b"replace-version-v1",
        )
        .await
    }

    async fn write_conditionally(
        &self,
        key: String,
        value: &T,
        context: OperationContext,
        condition: residiuum_store::WriteCondition,
        operation_domain: &'static [u8],
    ) -> Result<WriteReceipt, Error> {
        let json = serde_json::to_value(value)
            .map_err(|e| Error::local(ErrorCode::Validation, ErrorClass::Request, e.to_string()))?;
        let body = crate::value::encode_json(&json)
            .map_err(|e| Error::local(ErrorCode::Validation, ErrorClass::Request, e.to_string()))?;
        let request_id = resolve_request_id(context.request_id)?;
        let operation_id = resolve_operation_id(context.operation_id)?;
        let deadline = context.deadline;
        let condition_bytes = write_condition_bytes(condition);
        let content_hash = canonical_mutation_hash(
            operation_domain,
            self.heap_id(),
            self.id(),
            key.as_bytes(),
            &[body.as_slice(), condition_bytes.as_slice()],
        );
        let collection = self.inner.clone();
        let heap_id = self.heap_id();
        let collection_id = self.id();
        self.heap_client
            .connection
            .scheduler
            .dispatch(request_id, Some(operation_id), deadline, move || {
                let (storage, deduplicated) = collection.put_raw_body_with_operation(
                    &key,
                    &body,
                    condition,
                    operation_id.0,
                    content_hash,
                )?;
                Ok(WriteReceipt {
                    request_id,
                    operation_id,
                    heap_id,
                    collection_id,
                    deduplicated,
                    storage,
                })
            })
            .await
    }

    /// Read and decode one typed value.
    pub async fn get(&self, key: impl Into<String>) -> Result<Option<T>, Error> {
        Ok(self
            .get_versioned(key)
            .await?
            .map(|versioned| versioned.value))
    }

    /// Read and decode one typed value together with its CAS version.
    ///
    /// The value and version are observed atomically at the store boundary.
    /// The version may be passed to [`ReplaceOptions::if_version`] or
    /// [`DeleteOptions::if_version`].
    pub async fn get_versioned(
        &self,
        key: impl Into<String>,
    ) -> Result<Option<VersionedValue<T>>, Error> {
        let key = key.into();
        let collection = self.inner.clone();
        let request_id = mint_request_id()?;
        self.heap_client
            .connection
            .scheduler
            .dispatch(request_id, None, None, move || {
                match collection.get_versioned_raw_body(&key)? {
                    None => Ok(None),
                    Some((body, version)) => {
                        let json = crate::value::decode_json(&body)?;
                        let value = serde_json::from_value(json)?;
                        Ok(Some(VersionedValue { value, version }))
                    }
                }
            })
            .await
    }

    /// Read one bounded page in canonical key order.
    ///
    /// The continuation is authenticated to this exact capability and
    /// collection. Incomplete payloads are returned as explicit holes and do
    /// not silently disappear from the page.
    pub async fn scan_page(&self, options: ScanOptions) -> Result<ScanPage<T>, Error> {
        if !(1..=MAX_SCAN_PAGE_SIZE).contains(&options.page_size) {
            return Err(Error::local(
                ErrorCode::Validation,
                ErrorClass::Request,
                format!(
                    "scan page_size must be between 1 and {MAX_SCAN_PAGE_SIZE}"
                ),
            ));
        }
        if options.context.operation_id.is_some() {
            return Err(Error::local(
                ErrorCode::Validation,
                ErrorClass::Request,
                "scan reads do not accept operation_id",
            ));
        }
        let request_id = resolve_request_id(options.context.request_id)?;
        let deadline = options.context.deadline;
        let after_key = match options.continuation {
            Some(continuation) => {
                self.inner
                    .verify_cursor(&continuation.0)
                    .map_err(|source| Error::from_sdk(source, request_id, None))?;
                Some(continuation.0.position().to_vec())
            }
            None => None,
        };
        let page_size = options.page_size;
        let collection = self.inner.clone();
        self.heap_client
            .connection
            .scheduler
            .dispatch(request_id, None, deadline, move || {
                let raw = collection.scan_page_raw(page_size, after_key.as_deref())?;
                let mut rows = Vec::with_capacity(raw.entries.len());
                for (key, body, version) in raw.entries {
                    let key = String::from_utf8(key).map_err(|_| {
                        SdkError::Internal("scan returned a non-UTF-8 application key".into())
                    })?;
                    let json = crate::value::decode_json(&body)?;
                    let value = serde_json::from_value(json)?;
                    rows.push(ScanRow {
                        key,
                        value,
                        version,
                    });
                }
                let incomplete = raw
                    .incomplete
                    .into_iter()
                    .map(|hole| ScanHole {
                        key: String::from_utf8_lossy(&hole.key).into_owned(),
                        reason: hole.reason.as_str(),
                    })
                    .collect();
                let continuation = if raw.has_more {
                    raw.last_key
                        .as_deref()
                        .map(|key| ScanContinuation(collection.sign_cursor(key)))
                } else {
                    None
                };
                Ok(ScanPage {
                    rows,
                    incomplete,
                    examined: raw.examined,
                    complete: raw.complete,
                    continuation,
                })
            })
            .await
    }

    /// Delete a present key idempotently. Absence is a typed conflict.
    pub async fn delete(
        &self,
        key: impl Into<String>,
        options: DeleteOptions,
    ) -> Result<DeleteReceipt, Error> {
        if !options.if_present {
            return Err(Error::local(
                ErrorCode::Validation,
                ErrorClass::Request,
                "embedded v1 requires delete if_present=true for replay-stable receipts",
            ));
        }
        let key = key.into();
        let request_id = resolve_request_id(options.context.request_id)?;
        let operation_id = resolve_operation_id(options.context.operation_id)?;
        let deadline = options.context.deadline;
        let condition = options
            .if_version
            .map(residiuum_store::WriteCondition::LiveEventId)
            .unwrap_or(residiuum_store::WriteCondition::Present);
        let condition_bytes = write_condition_bytes(condition);
        let content_hash = canonical_mutation_hash(
            b"delete-present-v1",
            self.heap_id(),
            self.id(),
            key.as_bytes(),
            &[condition_bytes.as_slice()],
        );
        let collection = self.inner.clone();
        let heap_id = self.heap_id();
        let collection_id = self.id();
        self.heap_client
            .connection
            .scheduler
            .dispatch(request_id, Some(operation_id), deadline, move || {
                let (storage, deduplicated) = collection.delete_with_operation(
                    &key,
                    condition,
                    operation_id.0,
                    content_hash,
                )?;
                Ok(DeleteReceipt {
                    request_id,
                    operation_id,
                    heap_id,
                    collection_id,
                    deduplicated,
                    storage,
                })
            })
            .await
    }
}

fn write_condition_bytes(condition: residiuum_store::WriteCondition) -> Vec<u8> {
    match condition {
        residiuum_store::WriteCondition::Unconditional => b"unconditional".to_vec(),
        residiuum_store::WriteCondition::Absent => b"absent".to_vec(),
        residiuum_store::WriteCondition::Present => b"present".to_vec(),
        residiuum_store::WriteCondition::LiveEventId(version) => {
            let mut bytes = b"live-event-id:".to_vec();
            bytes.extend_from_slice(&version);
            bytes
        }
    }
}

fn canonical_mutation_hash(
    operation: &[u8],
    heap_id: HeapId,
    collection_id: CollectionId,
    key: &[u8],
    request_fields: &[&[u8]],
) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    let heap_bytes = heap_id.as_bytes();
    let collection_bytes = collection_id.as_bytes();
    for field in [
        operation,
        heap_bytes.as_slice(),
        collection_bytes.as_slice(),
        key,
    ] {
        hash.update(&(field.len() as u64).to_le_bytes());
        hash.update(field);
    }
    for field in request_fields {
        hash.update(&(field.len() as u64).to_le_bytes());
        hash.update(field);
    }
    let durability = b"durable";
    hash.update(&(durability.len() as u64).to_le_bytes());
    hash.update(durability);
    *hash.finalize().as_bytes()
}

fn mint_id() -> Result<[u8; 16], Error> {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|e| {
        Error::local(
            ErrorCode::Internal,
            ErrorClass::Internal,
            format!("secure identity generation failed: {e}"),
        )
    })?;
    if id == [0u8; 16] {
        id[15] = 1;
    }
    Ok(id)
}

fn mint_request_id() -> Result<RequestId, Error> {
    mint_id().map(RequestId)
}

fn mint_operation_id() -> Result<OperationId, Error> {
    mint_id().map(OperationId)
}

fn resolve_request_id(request_id: Option<RequestId>) -> Result<RequestId, Error> {
    match request_id {
        Some(RequestId(id)) if id == [0; 16] => Err(Error::local(
            ErrorCode::Validation,
            ErrorClass::Request,
            "request_id must be non-zero",
        )),
        Some(request_id) => Ok(request_id),
        None => mint_request_id(),
    }
}

fn resolve_operation_id(operation_id: Option<OperationId>) -> Result<OperationId, Error> {
    match operation_id {
        Some(OperationId(id)) if id == [0; 16] => Err(Error::local(
            ErrorCode::Validation,
            ErrorClass::Request,
            "operation_id must be non-zero",
        )),
        Some(operation_id) => Ok(operation_id),
        None => mint_operation_id(),
    }
}

type Task = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    Run(Task),
    Shutdown,
}

#[derive(Default)]
struct Counters {
    queued: AtomicUsize,
    running: AtomicUsize,
    completed: AtomicU64,
    refused: AtomicU64,
    cancelled_before_dispatch: AtomicU64,
}

type DeadlineCallback = Box<dyn FnOnce() + Send + 'static>;

struct DeadlineEntry {
    id: u64,
    at: Instant,
    callback: Option<DeadlineCallback>,
}

#[derive(Default)]
struct DeadlineState {
    entries: Vec<DeadlineEntry>,
    stopped: bool,
}

struct DeadlineService {
    state: Arc<(Mutex<DeadlineState>, Condvar)>,
    next_id: AtomicU64,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl DeadlineService {
    fn new() -> Result<Self, Error> {
        let state = Arc::new((Mutex::new(DeadlineState::default()), Condvar::new()));
        let worker_state = Arc::clone(&state);
        let worker = thread::Builder::new()
            .name("residiuum-driver-deadlines".into())
            .spawn(move || deadline_loop(worker_state))
            .map_err(|error| {
                Error::local(
                    ErrorCode::Unavailable,
                    ErrorClass::Service,
                    error.to_string(),
                )
            })?;
        Ok(Self {
            state,
            next_id: AtomicU64::new(1),
            worker: Mutex::new(Some(worker)),
        })
    }

    fn register(&self, at: Instant, callback: DeadlineCallback) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).max(1);
        let (lock, wake) = &*self.state;
        if let Ok(mut state) = lock.lock() {
            if !state.stopped {
                state.entries.push(DeadlineEntry {
                    id,
                    at,
                    callback: Some(callback),
                });
                wake.notify_one();
            }
        }
        id
    }

    fn cancel(&self, id: u64) {
        if id == 0 {
            return;
        }
        let (lock, wake) = &*self.state;
        if let Ok(mut state) = lock.lock() {
            if let Some(index) = state.entries.iter().position(|entry| entry.id == id) {
                state.entries.swap_remove(index);
                wake.notify_one();
            }
        }
    }

    fn stop(&self) {
        let (lock, wake) = &*self.state;
        if let Ok(mut state) = lock.lock() {
            state.stopped = true;
            state.entries.clear();
            wake.notify_all();
        }
        if let Ok(mut worker) = self.worker.lock() {
            if let Some(worker) = worker.take() {
                let _ = worker.join();
            }
        }
    }
}

fn deadline_loop(shared: Arc<(Mutex<DeadlineState>, Condvar)>) {
    let (lock, wake) = &*shared;
    loop {
        let mut callbacks = Vec::new();
        let mut state = match lock.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        if state.stopped {
            return;
        }
        let now = Instant::now();
        let mut index = 0;
        while index < state.entries.len() {
            if state.entries[index].at <= now {
                let mut entry = state.entries.swap_remove(index);
                if let Some(callback) = entry.callback.take() {
                    callbacks.push(callback);
                }
            } else {
                index += 1;
            }
        }
        if callbacks.is_empty() {
            let next = state.entries.iter().map(|entry| entry.at).min();
            state = match next {
                Some(at) => {
                    let wait = at.saturating_duration_since(Instant::now());
                    match wake.wait_timeout(state, wait) {
                        Ok((state, _)) => state,
                        Err(_) => return,
                    }
                }
                None => match wake.wait(state) {
                    Ok(state) => state,
                    Err(_) => return,
                },
            };
            drop(state);
            continue;
        }
        drop(state);
        for callback in callbacks {
            callback();
        }
    }
}

struct Scheduler {
    sender: Mutex<Option<mpsc::SyncSender<Message>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    counters: Arc<Counters>,
    deadlines: Arc<DeadlineService>,
    worker_count: usize,
    queue_capacity: usize,
    closed: AtomicBool,
}

impl Scheduler {
    fn new(worker_count: usize, queue_capacity: usize) -> Result<Self, Error> {
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        let counters = Arc::new(Counters::default());
        let deadlines = Arc::new(DeadlineService::new()?);
        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let handle = thread::Builder::new()
                .name(format!("residiuum-driver-{index}"))
                .spawn(move || loop {
                    let message = match receiver.lock() {
                        Ok(guard) => guard.recv(),
                        Err(_) => return,
                    };
                    match message {
                        Ok(Message::Run(task)) => task(),
                        Ok(Message::Shutdown) | Err(_) => return,
                    }
                })
                .map_err(|e| {
                    Error::local(ErrorCode::Unavailable, ErrorClass::Service, e.to_string())
                })?;
            workers.push(handle);
        }
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            workers: Mutex::new(workers),
            counters,
            deadlines,
            worker_count,
            queue_capacity,
            closed: AtomicBool::new(false),
        })
    }

    fn dispatch<T, F>(
        &self,
        request_id: RequestId,
        operation_id: Option<OperationId>,
        deadline: Option<Instant>,
        operation: F,
    ) -> ResponseFuture<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, SdkError> + Send + 'static,
    {
        let (future, responder) = response_pair();
        if deadline.map(|at| Instant::now() >= at).unwrap_or(false) {
            responder.complete(Err(Error::for_request(
                ErrorCode::DeadlineExceeded,
                ErrorClass::Cancellation,
                "deadline exceeded before admission",
                request_id,
                operation_id,
                TerminalOutcome::DeadlineExceeded,
                RetryDisposition::Never,
            )));
            return future;
        }
        let cancelled = Arc::clone(&future.cancelled);
        let counters = Arc::clone(&self.counters);
        let stage = Arc::new(AtomicU8::new(0)); // 0 queued, 1 dispatched, 2 terminal
        let deadline_registration = Arc::new(AtomicU64::new(0));
        let task_stage = Arc::clone(&stage);
        let task_registration = Arc::clone(&deadline_registration);
        let deadlines = Arc::clone(&self.deadlines);
        let task = Box::new(move || {
            counters.queued.fetch_sub(1, Ordering::AcqRel);
            if deadline.map(|at| Instant::now() >= at).unwrap_or(false) {
                task_stage.store(2, Ordering::Release);
                deadlines.cancel(task_registration.load(Ordering::Acquire));
                responder.complete(Err(Error::for_request(
                    ErrorCode::DeadlineExceeded,
                    ErrorClass::Cancellation,
                    "deadline exceeded before dispatch",
                    request_id,
                    operation_id,
                    TerminalOutcome::DeadlineExceeded,
                    RetryDisposition::Never,
                )));
                return;
            }
            if cancelled.load(Ordering::Acquire) {
                counters
                    .cancelled_before_dispatch
                    .fetch_add(1, Ordering::Relaxed);
                task_stage.store(2, Ordering::Release);
                deadlines.cancel(task_registration.load(Ordering::Acquire));
                responder.complete(Err(Error::for_request(
                    ErrorCode::CancelledBeforeDispatch,
                    ErrorClass::Cancellation,
                    "request cancelled before dispatch",
                    request_id,
                    operation_id,
                    TerminalOutcome::CancelledBeforeDispatch,
                    RetryDisposition::Never,
                )));
                return;
            }
            task_stage.store(1, Ordering::Release);
            counters.running.fetch_add(1, Ordering::AcqRel);
            let result = operation().map_err(|e| Error::from_sdk(e, request_id, operation_id));
            counters.running.fetch_sub(1, Ordering::AcqRel);
            task_stage.store(2, Ordering::Release);
            deadlines.cancel(task_registration.load(Ordering::Acquire));
            // Inspection must not observe `completed` while scheduler-owned
            // deadline state for the request is still retained.
            counters.completed.fetch_add(1, Ordering::Release);
            responder.complete(result);
        });

        let guard = match self.sender.lock() {
            Ok(guard) => guard,
            Err(_) => {
                responder_error(
                    &future,
                    Error::for_request(
                        ErrorCode::Internal,
                        ErrorClass::Internal,
                        "driver sender lock poisoned",
                        request_id,
                        operation_id,
                        TerminalOutcome::Refused,
                        RetryDisposition::Never,
                    ),
                );
                return future;
            }
        };
        if self.closed.load(Ordering::Acquire) {
            responder_error(
                &future,
                Error::for_request(
                    ErrorCode::Closed,
                    ErrorClass::Admission,
                    "client is closed",
                    request_id,
                    operation_id,
                    TerminalOutcome::Refused,
                    RetryDisposition::Never,
                ),
            );
            return future;
        }
        if !reserve_bounded(&self.counters.queued, self.queue_capacity) {
            self.counters.refused.fetch_add(1, Ordering::Relaxed);
            responder_error(
                &future,
                Error::for_request(
                    ErrorCode::Overloaded,
                    ErrorClass::Admission,
                    "embedded driver queue is full",
                    request_id,
                    operation_id,
                    TerminalOutcome::Refused,
                    RetryDisposition::After(Duration::from_millis(1)),
                ),
            );
            return future;
        }
        // Register before enqueue. Otherwise a fast worker can complete, load
        // registration id 0, and race with a late callback registration.
        let registration = deadline.map(|at| {
            let callback_shared = Arc::clone(&future.shared);
            let callback_cancelled = Arc::clone(&future.cancelled);
            let callback_stage = Arc::clone(&stage);
            let registration = self.deadlines.register(
                at,
                Box::new(move || {
                    let observed = callback_stage.load(Ordering::Acquire);
                    if observed >= 2 {
                        return;
                    }
                    if observed == 0 {
                        callback_cancelled.store(true, Ordering::Release);
                    }
                    let error = if observed == 0 || operation_id.is_none() {
                        Error::for_request(
                            ErrorCode::DeadlineExceeded,
                            ErrorClass::Cancellation,
                            if observed == 0 {
                                "deadline exceeded while queued"
                            } else {
                                "deadline exceeded after read dispatch"
                            },
                            request_id,
                            operation_id,
                            TerminalOutcome::DeadlineExceeded,
                            RetryDisposition::Never,
                        )
                    } else {
                        Error::for_request(
                            ErrorCode::CommitOutcomeUnknown,
                            ErrorClass::Cancellation,
                            "mutation deadline exceeded after dispatch; resolve by operation id",
                            request_id,
                            operation_id,
                            TerminalOutcome::CommitOutcomeUnknown,
                            RetryDisposition::OutcomeLookupRequired,
                        )
                    };
                    complete_response_shared(&callback_shared, Err(error));
                }),
            );
            deadline_registration.store(registration, Ordering::Release);
            registration
        });
        match guard
            .as_ref()
            .expect("open sender")
            .try_send(Message::Run(task))
        {
            Ok(()) => {
                if let Some(registration) = registration {
                    if stage.load(Ordering::Acquire) >= 2 {
                        self.deadlines.cancel(registration);
                    }
                }
            }
            Err(mpsc::TrySendError::Full(_)) => {
                if let Some(registration) = registration {
                    self.deadlines.cancel(registration);
                }
                self.counters.queued.fetch_sub(1, Ordering::AcqRel);
                self.counters.refused.fetch_add(1, Ordering::Relaxed);
                responder_error(
                    &future,
                    Error::for_request(
                        ErrorCode::Overloaded,
                        ErrorClass::Admission,
                        "embedded driver queue is full",
                        request_id,
                        operation_id,
                        TerminalOutcome::Refused,
                        RetryDisposition::After(Duration::from_millis(1)),
                    ),
                );
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                if let Some(registration) = registration {
                    self.deadlines.cancel(registration);
                }
                self.counters.queued.fetch_sub(1, Ordering::AcqRel);
                responder_error(
                    &future,
                    Error::for_request(
                        ErrorCode::Closed,
                        ErrorClass::Admission,
                        "embedded driver workers are closed",
                        request_id,
                        operation_id,
                        TerminalOutcome::Refused,
                        RetryDisposition::Never,
                    ),
                );
            }
        }
        future
    }

    fn inspect(&self) -> ClientInspection {
        ClientInspection {
            workers: self.worker_count,
            queue_capacity: self.queue_capacity,
            queued: self.counters.queued.load(Ordering::Acquire),
            running: self.counters.running.load(Ordering::Acquire),
            completed: self.counters.completed.load(Ordering::Relaxed),
            refused: self.counters.refused.load(Ordering::Relaxed),
            cancelled_before_dispatch: self
                .counters
                .cancelled_before_dispatch
                .load(Ordering::Relaxed),
            closed: self.closed.load(Ordering::Acquire),
        }
    }

    async fn close(&self) -> Result<(), Error> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let sender = self.sender.lock().ok().and_then(|mut sender| sender.take());
        let handles = self
            .workers
            .lock()
            .map(|mut workers| std::mem::take(&mut *workers))
            .unwrap_or_default();
        let worker_count = self.worker_count;
        let deadlines = Arc::clone(&self.deadlines);
        run_join(move || {
            if let Some(sender) = sender {
                for _ in 0..worker_count {
                    let _ = sender.send(Message::Shutdown);
                }
            }
            for handle in handles {
                let _ = handle.join();
            }
            deadlines.stop();
        })
        .await
    }
}

fn reserve_bounded(counter: &AtomicUsize, bound: usize) -> bool {
    let mut observed = counter.load(Ordering::Acquire);
    loop {
        if observed >= bound {
            return false;
        }
        match counter.compare_exchange_weak(
            observed,
            observed + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(actual) => observed = actual,
        }
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        let sender = self.sender.get_mut().ok().and_then(Option::take);
        drop(sender);
        if let Ok(workers) = self.workers.get_mut() {
            for worker in workers.drain(..) {
                let _ = worker.join();
            }
        }
        self.deadlines.stop();
    }
}

struct ResponseState<T> {
    result: Option<Result<T, Error>>,
    waker: Option<Waker>,
}

struct ResponseShared<T> {
    state: Mutex<ResponseState<T>>,
}

struct Responder<T> {
    shared: Arc<ResponseShared<T>>,
}

impl<T> Responder<T> {
    fn complete(self, result: Result<T, Error>) {
        complete_response_shared(&self.shared, result);
    }
}

fn complete_response_shared<T>(shared: &ResponseShared<T>, result: Result<T, Error>) {
    if let Ok(mut state) = shared.state.lock() {
        if state.result.is_some() {
            return;
        }
        state.result = Some(result);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}

struct ResponseFuture<T> {
    shared: Arc<ResponseShared<T>>,
    cancelled: Arc<AtomicBool>,
    completed: bool,
}

impl<T> Future for ResponseFuture<T> {
    type Output = Result<T, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = {
            let mut state = self.shared.state.lock().expect("response lock");
            if let Some(result) = state.result.take() {
                Some(result)
            } else {
                state.waker = Some(cx.waker().clone());
                None
            }
        };
        if let Some(result) = result {
            self.completed = true;
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}

impl<T> Drop for ResponseFuture<T> {
    fn drop(&mut self) {
        if !self.completed {
            self.cancelled.store(true, Ordering::Release);
        }
    }
}

fn response_pair<T>() -> (ResponseFuture<T>, Responder<T>) {
    let shared = Arc::new(ResponseShared {
        state: Mutex::new(ResponseState {
            result: None,
            waker: None,
        }),
    });
    (
        ResponseFuture {
            shared: Arc::clone(&shared),
            cancelled: Arc::new(AtomicBool::new(false)),
            completed: false,
        },
        Responder { shared },
    )
}

fn responder_error<T>(future: &ResponseFuture<T>, error: Error) {
    complete_response_shared(&future.shared, Err(error));
}

fn run_open<T, F>(operation: F) -> ResponseFuture<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, SdkError> + Send + 'static,
{
    let (future, responder) = response_pair();
    let request_id = mint_request_id().unwrap_or(RequestId([0; 16]));
    let shared = Arc::clone(&responder.shared);
    match thread::Builder::new()
        .name("residiuum-driver-open".into())
        .spawn(move || {
            responder.complete(operation().map_err(|e| Error::from_sdk(e, request_id, None)));
        }) {
        Ok(_) => {}
        Err(e) => Responder { shared }.complete(Err(Error::for_request(
            ErrorCode::Unavailable,
            ErrorClass::Service,
            e.to_string(),
            request_id,
            None,
            TerminalOutcome::Refused,
            RetryDisposition::SafeSameRequest,
        ))),
    }
    future
}

fn run_join<F>(operation: F) -> ResponseFuture<()>
where
    F: FnOnce() + Send + 'static,
{
    let (future, responder) = response_pair();
    match thread::Builder::new()
        .name("residiuum-driver-close".into())
        .spawn(move || {
            operation();
            responder.complete(Ok(()));
        }) {
        Ok(_) => {}
        Err(e) => responder_error(
            &future,
            Error::local(ErrorCode::Unavailable, ErrorClass::Service, e.to_string()),
        ),
    }
    future
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn public_handles_are_clone_send_sync() {
        assert_send_sync::<Client>();
        assert_send_sync::<HeapClient>();
        assert_send_sync::<Collection<serde_json::Value>>();
        fn assert_clone<T: Clone>() {}
        assert_clone::<Client>();
        assert_clone::<HeapClient>();
        assert_clone::<Collection<serde_json::Value>>();
    }

    #[test]
    fn canonical_mutation_hash_preserves_request_field_boundaries() {
        let heap_id = HeapId::new_random().unwrap();
        let collection_id = CollectionId::new_random().unwrap();
        let left = canonical_mutation_hash(
            b"conditional-v1",
            heap_id,
            collection_id,
            b"key",
            &[b"ab", b"c"],
        );
        let right = canonical_mutation_hash(
            b"conditional-v1",
            heap_id,
            collection_id,
            b"key",
            &[b"a", b"bc"],
        );
        assert_ne!(left, right);
    }

    #[test]
    fn scheduler_refuses_beyond_hard_bound() {
        let scheduler = Scheduler::new(1, 1).unwrap();
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let first_gate = Arc::clone(&gate);
        let first = scheduler.dispatch(RequestId([1; 16]), None, None, move || {
            let (lock, cv) = &*first_gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = cv.wait(open).unwrap();
            }
            Ok(())
        });
        while scheduler.inspect().running == 0 {
            thread::yield_now();
        }
        let _queued = scheduler.dispatch(RequestId([2; 16]), None, None, || Ok(()));
        let refused = scheduler.dispatch(RequestId([3; 16]), None, None, || Ok(()));
        let state = refused.shared.state.lock().unwrap();
        assert!(matches!(
            state
                .result
                .as_ref()
                .map(|r| r.as_ref().err().map(|e| e.code)),
            Some(Some(ErrorCode::Overloaded))
        ));
        drop(state);
        let (lock, cv) = &*gate;
        *lock.lock().unwrap() = true;
        cv.notify_all();
        drop(first);
    }

    #[test]
    fn queued_drop_cancels_before_dispatch_and_deadline_refuses_execution() {
        let scheduler = Scheduler::new(1, 2).unwrap();
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let first_gate = Arc::clone(&gate);
        let blocker = scheduler.dispatch(RequestId([1; 16]), None, None, move || {
            let (lock, cv) = &*first_gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = cv.wait(open).unwrap();
            }
            Ok(())
        });
        while scheduler.inspect().running == 0 {
            thread::yield_now();
        }

        let cancelled_ran = Arc::new(AtomicBool::new(false));
        let cancelled_marker = Arc::clone(&cancelled_ran);
        let cancelled = scheduler.dispatch(RequestId([2; 16]), None, None, move || {
            cancelled_marker.store(true, Ordering::Release);
            Ok(())
        });
        drop(cancelled);

        let deadline_ran = Arc::new(AtomicBool::new(false));
        let deadline_marker = Arc::clone(&deadline_ran);
        let deadline =
            scheduler.dispatch(RequestId([3; 16]), None, Some(Instant::now()), move || {
                deadline_marker.store(true, Ordering::Release);
                Ok(())
            });
        assert!(scheduler.inspect().queued <= 2);

        let (lock, cv) = &*gate;
        *lock.lock().unwrap() = true;
        cv.notify_all();
        let wait_until = Instant::now() + Duration::from_secs(1);
        while scheduler.inspect().cancelled_before_dispatch != 1 && Instant::now() < wait_until {
            thread::yield_now();
        }
        assert_eq!(scheduler.inspect().cancelled_before_dispatch, 1);
        assert!(!cancelled_ran.load(Ordering::Acquire));
        assert!(!deadline_ran.load(Ordering::Acquire));
        let state = deadline.shared.state.lock().unwrap();
        assert!(matches!(
            state
                .result
                .as_ref()
                .map(|result| result.as_ref().err().map(|e| e.code)),
            Some(Some(ErrorCode::DeadlineExceeded))
        ));
        drop(state);
        drop(blocker);
    }

    #[test]
    fn queued_deadline_wakes_without_waiting_for_a_worker() {
        let scheduler = Scheduler::new(1, 2).unwrap();
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let blocker_gate = Arc::clone(&gate);
        let blocker = scheduler.dispatch(RequestId([1; 16]), None, None, move || {
            let (lock, wake) = &*blocker_gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = wake.wait(open).unwrap();
            }
            Ok(())
        });
        while scheduler.inspect().running == 0 {
            thread::yield_now();
        }

        let deadline = scheduler.dispatch(
            RequestId([2; 16]),
            None,
            Some(Instant::now() + Duration::from_millis(20)),
            || Ok(()),
        );
        thread::sleep(Duration::from_millis(60));
        let state = deadline.shared.state.lock().unwrap();
        assert!(matches!(
            state
                .result
                .as_ref()
                .map(|result| result.as_ref().err().map(|error| error.code)),
            Some(Some(ErrorCode::DeadlineExceeded))
        ));
        drop(state);

        let (lock, wake) = &*gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        drop(blocker);
    }

    #[test]
    fn dispatched_mutation_deadline_reports_unknown_outcome() {
        let scheduler = Scheduler::new(1, 1).unwrap();
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let mutation_gate = Arc::clone(&gate);
        let mutation = scheduler.dispatch(
            RequestId([1; 16]),
            Some(OperationId([2; 16])),
            Some(Instant::now() + Duration::from_millis(20)),
            move || {
                let (lock, wake) = &*mutation_gate;
                let mut open = lock.lock().unwrap();
                while !*open {
                    open = wake.wait(open).unwrap();
                }
                Ok(())
            },
        );
        while scheduler.inspect().running == 0 {
            thread::yield_now();
        }
        thread::sleep(Duration::from_millis(60));
        let state = mutation.shared.state.lock().unwrap();
        let error = state.result.as_ref().unwrap().as_ref().unwrap_err();
        assert_eq!(error.code, ErrorCode::CommitOutcomeUnknown);
        assert_eq!(error.terminal, TerminalOutcome::CommitOutcomeUnknown);
        assert_eq!(error.retry, RetryDisposition::OutcomeLookupRequired);
        drop(state);

        let (lock, wake) = &*gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        while scheduler.inspect().running != 0 {
            thread::yield_now();
        }
        let state = mutation.shared.state.lock().unwrap();
        assert!(matches!(
            state
                .result
                .as_ref()
                .map(|result| result.as_ref().err().map(|error| error.code)),
            Some(Some(ErrorCode::CommitOutcomeUnknown))
        ));
    }

    #[test]
    fn completed_requests_release_long_deadline_registrations() {
        let scheduler = Scheduler::new(2, 64).unwrap();
        let mut requests = Vec::new();
        for byte in 1..=32u8 {
            requests.push(scheduler.dispatch(
                RequestId([byte; 16]),
                None,
                Some(Instant::now() + Duration::from_secs(60)),
                || Ok(()),
            ));
        }
        let wait_until = Instant::now() + Duration::from_secs(1);
        while scheduler.inspect().completed != 32 && Instant::now() < wait_until {
            thread::yield_now();
        }
        assert_eq!(scheduler.inspect().completed, 32);
        let state = scheduler.deadlines.state.0.lock().unwrap();
        assert!(
            state.entries.is_empty(),
            "completed requests must not retain deadline callbacks"
        );
        drop(state);
        drop(requests);
    }
}
