//! SDK-level errors (DX_SPEC §15 everyday surface).

use residiuum_store::StoreError;
use thiserror::Error;

/// Stable machine-readable error codes (DX_SPEC §15).
///
/// Everyday callers should match on [`Error::code`] rather than English text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// Store or path already exists when exclusive create was requested.
    AlreadyExists,
    /// Requested key/entity is absent (when an API chooses error over `None`).
    NotFound,
    /// Optimistic concurrency / version precondition failed.
    VersionConflict,
    /// Collection name, key, or argument failed validation.
    ValidationFailed,
    /// Filter / query object is malformed or uses unknown operators.
    QueryInvalid,
    /// Query would need an explicit resource budget (Stage 6+).
    QueryBudgetRequired,
    /// Configured resource limit hit.
    ResourceLimit,
    /// Coverage is incomplete; absence cannot be proven.
    CoverageIncomplete,
    /// Required partition/tier is offline (cluster).
    PartitionUnavailable,
    /// Cached placement route was stale (usually retried internally).
    StaleRoute,
    /// Consistency mode does not allow the operation.
    ConsistencyViolation,
    /// Requested durability mode cannot be met.
    DurabilityUnavailable,
    /// Payload or store bytes failed integrity / are damaged.
    DataDamaged,
    /// Payload is only partially available (chunks; Stage 6+).
    PayloadPartial,
    /// Encoding or format is not supported by this build.
    FormatUnsupported,
    /// JSON vs bytes (or other) type mismatch on get.
    TypeMismatch,
    /// Shared token / authentication failed (remote).
    AuthenticationFailed,
    /// Permission denied after authentication (reserved).
    PermissionDenied,
    /// Connect or request deadline exceeded (remote).
    DeadlineExceeded,
    /// Underlying IO failure.
    Io,
    /// Wire/protocol fields missing, malformed, or inconsistently optimistic.
    ProtocolViolation,
    /// Exclusive store writer ownership is held by another process/handle.
    WriterLockHeld,
    /// Internal / unexpected failure.
    Internal,
}

impl ErrorCode {
    /// Stable snake_case code string for logs and interop.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyExists => "already_exists",
            Self::NotFound => "not_found",
            Self::VersionConflict => "version_conflict",
            Self::ValidationFailed => "validation_failed",
            Self::QueryInvalid => "query_invalid",
            Self::QueryBudgetRequired => "query_budget_required",
            Self::ResourceLimit => "resource_limit",
            Self::CoverageIncomplete => "coverage_incomplete",
            Self::PartitionUnavailable => "partition_unavailable",
            Self::StaleRoute => "stale_route",
            Self::ConsistencyViolation => "consistency_violation",
            Self::DurabilityUnavailable => "durability_unavailable",
            Self::DataDamaged => "data_damaged",
            Self::PayloadPartial => "payload_partial",
            Self::FormatUnsupported => "format_unsupported",
            Self::TypeMismatch => "type_mismatch",
            Self::AuthenticationFailed => "authentication_failed",
            Self::PermissionDenied => "permission_denied",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Io => "io",
            Self::ProtocolViolation => "protocol_violation",
            Self::WriterLockHeld => "writer_lock_held",
            Self::Internal => "internal",
        }
    }
}

/// Errors from open, collection access, put/get/delete, and query.
#[derive(Debug, Error)]
pub enum Error {
    /// Underlying store failure.
    #[error(transparent)]
    Store(#[from] StoreError),

    /// Collection name is empty, too long, or contains NUL.
    #[error("invalid collection name: {0}")]
    InvalidCollectionName(&'static str),

    /// Key is empty, too long, or contains NUL.
    #[error("invalid key: {0}")]
    InvalidKey(&'static str),

    /// Stored payload is not valid UTF-8 JSON for a JSON get.
    #[error("invalid JSON payload: {0}")]
    InvalidJson(#[from] serde_json::Error),

    /// Payload type tag does not match the requested API (JSON vs bytes).
    #[error("payload type mismatch: expected {expected}, found {found}")]
    TypeMismatch {
        /// Requested logical type.
        expected: &'static str,
        /// On-disk type tag name.
        found: &'static str,
    },

    /// Payload is missing a type tag, truncated, or fails integrity.
    #[error("corrupt or unsupported payload encoding")]
    BadPayload,

    /// Filter / query specification is invalid.
    #[error("invalid query: {0}")]
    QueryInvalid(String),

    /// Result materialization exceeded a configured or explicit limit.
    #[error("resource limit exceeded: {0}")]
    ResourceLimit(String),

    /// Query needs an explicit budget (expensive scan without usable index).
    #[error("query budget required: {0}")]
    QueryBudgetRequired(String),

    /// Payload only partially available (chunk completeness).
    #[error("payload only partially available")]
    PayloadPartial,

    /// Coverage incomplete; absence cannot be proven.
    #[error("coverage incomplete: {0}")]
    CoverageIncomplete(String),

    /// Required partition is offline or has no electable leader.
    #[error("partition unavailable: partition {partition} ({reason})")]
    PartitionUnavailable {
        /// Virtual partition id.
        partition: u32,
        /// Human-readable reason.
        reason: &'static str,
    },

    /// Client directory cache held a stale placement route (CLUSTER_SPEC §13).
    ///
    /// Ordinary SDK paths refresh and retry; callers see this only when retries
    /// are exhausted or when probing cache behaviour in tests.
    #[error("stale route for partition {partition}: {message}")]
    StaleRoute {
        /// Virtual partition id.
        partition: u32,
        /// Detail.
        message: String,
    },

    /// Consistency mode does not allow this operation.
    #[error("consistency mode violation: {0}")]
    ConsistencyViolation(String),

    /// Requested durability cannot be met.
    #[error("durability unavailable: {0}")]
    DurabilityUnavailable(String),

    /// Validation failure with a dynamic message (URLs, options).
    #[error("validation failed: {0}")]
    ValidationMsg(String),

    /// Optimistic concurrency precondition failed (DX_SPEC §6.4 / §15).
    ///
    /// `expected` is the caller-supplied token; `observed` is the live
    /// establishing event id when the key is present, or `None` when absent.
    #[error("version conflict")]
    VersionConflict {
        /// Token the caller required (establishing event id).
        expected: [u8; 16],
        /// Live establishing event id, or `None` if the key is absent.
        observed: Option<[u8; 16]>,
    },

    /// Key/entity not found when the API requires presence.
    #[error("not found: {0}")]
    NotFound(String),

    /// Remote server returned an error.
    #[error("remote error ({code}): {message}")]
    Remote {
        /// Stable code from the server when available.
        code: String,
        /// Human-readable message.
        message: String,
    },

    /// Feature not available on remote connections (indexes/history/etc.).
    #[error("not available over remote connection: {0}")]
    RemoteUnsupported(&'static str),

    /// Shared token missing or wrong (DX_SPEC §15 `AuthenticationFailed`).
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Authenticated principal lacks the required privilege (DEF-033).
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Connect or request deadline exceeded (DX_SPEC §15 `DeadlineExceeded`).
    #[error("deadline exceeded: {0}")]
    DeadlineExceeded(String),

    /// Wire response omitted or weakened required guarantee fields (DEF-014).
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),

    /// Internal SDK failure.
    #[error("internal: {0}")]
    Internal(String),

    /// Direct IO failure (remote sockets, CLI paths).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    /// Wrap an IO error (remote transport). Timeouts become [`Error::DeadlineExceeded`].
    pub fn from_io(e: std::io::Error) -> Self {
        match e.kind() {
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => {
                Self::DeadlineExceeded(e.to_string())
            }
            _ => Self::Io(e),
        }
    }

    /// Stable machine code (DX_SPEC §15). Prefer this over parsing [`Display`].
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Store(e) => map_store(e),
            Self::InvalidCollectionName(_) | Self::InvalidKey(_) | Self::ValidationMsg(_) => {
                ErrorCode::ValidationFailed
            }
            Self::InvalidJson(_) => ErrorCode::DataDamaged,
            Self::TypeMismatch { .. } => ErrorCode::TypeMismatch,
            Self::BadPayload => ErrorCode::DataDamaged,
            Self::QueryInvalid(_) => ErrorCode::QueryInvalid,
            Self::ResourceLimit(_) => ErrorCode::ResourceLimit,
            Self::QueryBudgetRequired(_) => ErrorCode::QueryBudgetRequired,
            Self::PayloadPartial => ErrorCode::PayloadPartial,
            Self::CoverageIncomplete(_) => ErrorCode::CoverageIncomplete,
            Self::PartitionUnavailable { .. } => ErrorCode::PartitionUnavailable,
            Self::StaleRoute { .. } => ErrorCode::StaleRoute,
            Self::ConsistencyViolation(_) => ErrorCode::ConsistencyViolation,
            Self::DurabilityUnavailable(_) => ErrorCode::DurabilityUnavailable,
            Self::VersionConflict { .. } => ErrorCode::VersionConflict,
            Self::NotFound(_) => ErrorCode::NotFound,
            Self::Remote { code, .. } => remote_code(code),
            Self::RemoteUnsupported(_) => ErrorCode::FormatUnsupported,
            Self::AuthenticationFailed(_) => ErrorCode::AuthenticationFailed,
            Self::PermissionDenied(_) => ErrorCode::PermissionDenied,
            Self::DeadlineExceeded(_) => ErrorCode::DeadlineExceeded,
            Self::ProtocolViolation(_) => ErrorCode::ProtocolViolation,
            Self::Internal(_) => ErrorCode::Internal,
            Self::Io(_) => ErrorCode::Io,
        }
    }

    /// Whether this is an ordinary store IO failure.
    pub fn is_io(&self) -> bool {
        matches!(self.code(), ErrorCode::Io)
    }

    /// Whether the payload appears damaged or unreadable as requested.
    pub fn is_data_damaged(&self) -> bool {
        matches!(self.code(), ErrorCode::DataDamaged)
    }

    /// Whether the error is a validation problem on names/keys.
    pub fn is_validation(&self) -> bool {
        matches!(self.code(), ErrorCode::ValidationFailed)
    }

    /// Whether the filter/query object was rejected.
    pub fn is_query_invalid(&self) -> bool {
        matches!(self.code(), ErrorCode::QueryInvalid)
    }
}

fn map_store(e: &StoreError) -> ErrorCode {
    match e {
        StoreError::Io(_) => ErrorCode::Io,
        StoreError::AlreadyExists(_) => ErrorCode::AlreadyExists,
        StoreError::NotAStore(_) => ErrorCode::ValidationFailed,
        StoreError::SubjectTooLong { .. } => ErrorCode::ValidationFailed,
        StoreError::PayloadTooLarge => ErrorCode::ResourceLimit,
        StoreError::WriteAdmissionFull => ErrorCode::ResourceLimit,
        StoreError::BadEnvelope(_)
        | StoreError::CorruptMeta(_)
        | StoreError::CorruptControl { .. } => ErrorCode::DataDamaged,
        StoreError::Frame(_) | StoreError::Segment(_) => ErrorCode::DataDamaged,
        StoreError::PayloadPartial => ErrorCode::PayloadPartial,
        StoreError::PayloadConflict => ErrorCode::DataDamaged,
        StoreError::HistoryEventNotFound => ErrorCode::NotFound,
        StoreError::SegmentNotFound => ErrorCode::NotFound,
        StoreError::LocatorFault(f) => match f.kind {
            residiuum_store::LocatorFaultKind::SegmentNotFound => ErrorCode::NotFound,
            _ => ErrorCode::DataDamaged,
        },
        StoreError::TierOffline(_) => ErrorCode::PartitionUnavailable,
        StoreError::FormatUnsupported { .. } => ErrorCode::FormatUnsupported,
        StoreError::MediaUnsupported(_) => ErrorCode::FormatUnsupported,
        StoreError::WriterLockHeld(_) => ErrorCode::WriterLockHeld,
        StoreError::CoverageIncomplete(_) => ErrorCode::CoverageIncomplete,
        StoreError::ConsistencyViolation(_) | StoreError::OperationIdentityConflict => {
            ErrorCode::ConsistencyViolation
        }
        // Failpoints are test-only injection; surface as I/O class failures.
        StoreError::Failpoint(_) => ErrorCode::Io,
        // OS CSPRNG unavailable (DEF-025); rare host configuration failure.
        StoreError::RandomUnavailable(_) => ErrorCode::Internal,
        // DEF-026: bad/tampered token → query invalid; generation fence → stale route class.
        StoreError::CursorInvalid(_) => ErrorCode::QueryInvalid,
        StoreError::CursorStale(_) => ErrorCode::ConsistencyViolation,
        StoreError::HeapCapability(_) => ErrorCode::PermissionDenied,
        StoreError::HeapAdmit(_) => ErrorCode::PermissionDenied,
        StoreError::VersionConflict { .. } => ErrorCode::VersionConflict,
        StoreError::KeyExists => ErrorCode::AlreadyExists,
        StoreError::AdaptiveWriterActive | StoreError::AdaptiveWriterPoisoned => ErrorCode::Io,
        StoreError::AtomicStage(_) => ErrorCode::ConsistencyViolation,
        // P0: sealed segment identity collision — refuse without mutating media.
        StoreError::SegmentIdCollision { .. } => ErrorCode::ConsistencyViolation,
    }
}

/// Lift store CAS errors into the structured SDK variants used by APB-2 tests.
pub(crate) fn lift_store_cas(err: Error) -> Error {
    match err {
        Error::Store(StoreError::VersionConflict { expected, observed }) => {
            Error::VersionConflict { expected, observed }
        }
        Error::Store(StoreError::KeyExists) => Error::Remote {
            code: "already_exists".into(),
            message: "key already present".into(),
        },
        other => other,
    }
}

fn remote_code(code: &str) -> ErrorCode {
    match code {
        "authentication_failed" => ErrorCode::AuthenticationFailed,
        "permission_denied" => ErrorCode::PermissionDenied,
        "deadline_exceeded" => ErrorCode::DeadlineExceeded,
        "validation_failed" => ErrorCode::ValidationFailed,
        "not_found" => ErrorCode::NotFound,
        "resource_limit" => ErrorCode::ResourceLimit,
        "data_damaged" => ErrorCode::DataDamaged,
        "query_invalid" => ErrorCode::QueryInvalid,
        "query_budget_required" => ErrorCode::QueryBudgetRequired,
        "partition_unavailable" => ErrorCode::PartitionUnavailable,
        "stale_route" | "stale_epoch" | "not_leader" => ErrorCode::StaleRoute,
        "consistency_violation" => ErrorCode::ConsistencyViolation,
        "durability_unavailable" => ErrorCode::DurabilityUnavailable,
        "protocol_violation" => ErrorCode::ProtocolViolation,
        "writer_lock_held" => ErrorCode::WriterLockHeld,
        "coverage_incomplete" => ErrorCode::CoverageIncomplete,
        "payload_partial" => ErrorCode::PayloadPartial,
        "already_exists" => ErrorCode::AlreadyExists,
        "version_conflict" => ErrorCode::VersionConflict,
        "io" => ErrorCode::Io,
        _ => ErrorCode::Internal,
    }
}

impl From<residiuum_client::Error> for Error {
    fn from(e: residiuum_client::Error) -> Self {
        match e {
            residiuum_client::Error::ProtocolViolation(s) => Self::ProtocolViolation(s),
            residiuum_client::Error::ResourceLimit(s) => Self::ResourceLimit(s),
            residiuum_client::Error::AuthenticationFailed(s) => Self::AuthenticationFailed(s),
            residiuum_client::Error::DeadlineExceeded(s) => Self::DeadlineExceeded(s),
            residiuum_client::Error::Remote { code, message } => Self::Remote { code, message },
            residiuum_client::Error::Internal(s) => Self::Internal(s),
            residiuum_client::Error::Io(e) => Self::from_io(e),
        }
    }
}
