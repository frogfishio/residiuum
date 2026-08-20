//! Async-driver bridge for Heap-local multi-record Atomics.

use super::{
    encode_driver_json, mint_request_id, Collection, Error, ErrorClass, ErrorCode, HeapClient,
};
use residiuum_atomics::{
    BoundCollection, CanonicalKey, CanonicalValue, CollectionId, ConstructionRead, EncodingProfile,
    VersionId,
};
use serde::{de::DeserializeOwned, Serialize};
use std::time::Instant;
use std::{sync::atomic::Ordering, sync::Arc};

pub use residiuum_atomics::{
    AtomicAbortReason, AtomicId, AtomicMemberReceipt, AtomicOptions, AtomicOutcome, AtomicReceipt,
    AtomicRefuseReason, AtomicResolutionHandle, AtomicStatus, LogicalStatus, MaterialStatus,
    ResourceLimits,
};

/// Stable Atomics semantic code from `ATOMICS_SPEC` §22.
///
/// This vocabulary is independent of the driver's transport/admission
/// [`super::ErrorCode`]. Not every reserved code is emitted by the v1 kernel:
/// callers receive only distinctions supported by durable evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AtomicCode {
    /// One identity was reused for different canonical content.
    AtomicIdConflict,
    /// Atomic identity bytes were illegal.
    AtomicIdInvalid,
    /// A plan or member escaped its authorised Heap scope.
    AtomicScopeEscape,
    /// The requested Atomic scope/profile is unavailable.
    AtomicScopeUnavailable,
    /// A configured or hard Atomic bound was exceeded.
    AtomicLimitExceeded,
    /// The deadline expired before Atomic acceptance or dispatch.
    AtomicDeadlineExceeded,
    /// A construction read no longer matches the serialization frontier.
    AtomicReadConflict,
    /// A predicate no longer holds at the serialization frontier.
    AtomicPredicateConflict,
    /// The governing rule changed after construction.
    AtomicRuleChanged,
    /// The closed plan violates an active rule.
    AtomicRuleViolation,
    /// Durable evidence proves a not-committed decision.
    AtomicNotCommitted,
    /// The observer cannot yet establish the logical outcome.
    AtomicOutcomeUnknown,
    /// Valid durable decision or material evidence conflicts.
    AtomicEvidenceConflicting,
    /// Required evidence coverage is incomplete.
    AtomicCoverageIncomplete,
    /// A committed decision exists but named material is incomplete.
    AtomicMaterialPartial,
    /// The capability lacks a required Atomic right.
    AtomicRightDenied,
    /// A relationship requires a parent that is absent.
    RelationshipParentMissing,
    /// A relationship prevents removal while children exist.
    RelationshipChildrenExist,
    /// Relationship enforcement is operating in a degraded state.
    RelationshipDegraded,
    /// A uniqueness rule found an existing live value.
    UniqueValueExists,
}

impl AtomicCode {
    /// Exact stable machine-readable spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AtomicIdConflict => "atomic_id_conflict",
            Self::AtomicIdInvalid => "atomic_id_invalid",
            Self::AtomicScopeEscape => "atomic_scope_escape",
            Self::AtomicScopeUnavailable => "atomic_scope_unavailable",
            Self::AtomicLimitExceeded => "atomic_limit_exceeded",
            Self::AtomicDeadlineExceeded => "atomic_deadline_exceeded",
            Self::AtomicReadConflict => "atomic_read_conflict",
            Self::AtomicPredicateConflict => "atomic_predicate_conflict",
            Self::AtomicRuleChanged => "atomic_rule_changed",
            Self::AtomicRuleViolation => "atomic_rule_violation",
            Self::AtomicNotCommitted => "atomic_not_committed",
            Self::AtomicOutcomeUnknown => "atomic_outcome_unknown",
            Self::AtomicEvidenceConflicting => "atomic_evidence_conflicting",
            Self::AtomicCoverageIncomplete => "atomic_coverage_incomplete",
            Self::AtomicMaterialPartial => "atomic_material_partial",
            Self::AtomicRightDenied => "atomic_right_denied",
            Self::RelationshipParentMissing => "relationship_parent_missing",
            Self::RelationshipChildrenExist => "relationship_children_exist",
            Self::RelationshipDegraded => "relationship_degraded",
            Self::UniqueValueExists => "unique_value_exists",
        }
    }

    /// Classify a structural refusal when the kernel proves a §22 distinction.
    pub const fn from_refusal(reason: AtomicRefuseReason) -> Option<Self> {
        match reason {
            AtomicRefuseReason::ScopeUnavailable => Some(Self::AtomicScopeUnavailable),
            AtomicRefuseReason::CrossHeapCollection => Some(Self::AtomicScopeEscape),
            AtomicRefuseReason::StaleOrForeignCapability
            | AtomicRefuseReason::AuthorizationFailure => Some(Self::AtomicRightDenied),
            AtomicRefuseReason::LimitExceeded => Some(Self::AtomicLimitExceeded),
            AtomicRefuseReason::Deadline => Some(Self::AtomicDeadlineExceeded),
            AtomicRefuseReason::AtomicIdConflict => Some(Self::AtomicIdConflict),
            AtomicRefuseReason::MalformedInput
            | AtomicRefuseReason::UnsupportedProfile
            | AtomicRefuseReason::DuplicateTarget
            | AtomicRefuseReason::InvalidValue
            | AtomicRefuseReason::UnknownMutationKind
            | AtomicRefuseReason::UnsupportedPredicate
            | AtomicRefuseReason::CancelledBeforeAcceptance => None,
        }
    }

    /// Classify an issued Atomic outcome. `None` means committed.
    ///
    /// The v1 durable abort record intentionally has coarse reason codes.
    /// Therefore precondition and rule aborts report `atomic_not_committed`
    /// until durable evidence can honestly distinguish their finer §22 forms.
    pub const fn from_outcome(outcome: &AtomicOutcome) -> Option<Self> {
        match outcome {
            AtomicOutcome::Committed(_) => None,
            AtomicOutcome::Unknown { .. } => Some(Self::AtomicOutcomeUnknown),
            AtomicOutcome::NotCommitted {
                reason: AtomicAbortReason::CoverageIncomplete,
                ..
            } => Some(Self::AtomicCoverageIncomplete),
            AtomicOutcome::NotCommitted { .. } => Some(Self::AtomicNotCommitted),
        }
    }

    /// Classify a status result, prioritising conflicting and incomplete proof.
    pub const fn from_status(status: &AtomicStatus) -> Option<Self> {
        if matches!(status.logical, LogicalStatus::ConflictingDecisionEvidence)
            || matches!(status.material, MaterialStatus::Conflicting)
        {
            return Some(Self::AtomicEvidenceConflicting);
        }
        if matches!(status.material, MaterialStatus::CoverageIncomplete) {
            return Some(Self::AtomicCoverageIncomplete);
        }
        if matches!(status.logical, LogicalStatus::Committed)
            && !matches!(status.material, MaterialStatus::Complete)
        {
            return Some(Self::AtomicMaterialPartial);
        }
        match status.logical {
            LogicalStatus::UnknownCommit => Some(Self::AtomicOutcomeUnknown),
            LogicalStatus::NotCommitted => Some(Self::AtomicNotCommitted),
            LogicalStatus::Committed
            | LogicalStatus::ConflictingDecisionEvidence
            | LogicalStatus::NotFound => None,
        }
    }

    pub(super) const fn from_error(error: residiuum_atomics::AtomicsError) -> Option<Self> {
        match error {
            residiuum_atomics::AtomicsError::InvalidIdentity(_) => Some(Self::AtomicIdInvalid),
            residiuum_atomics::AtomicsError::Refused(reason) => Self::from_refusal(reason),
            residiuum_atomics::AtomicsError::EntropyUnavailable
            | residiuum_atomics::AtomicsError::Cbor(_) => None,
        }
    }
}

impl std::fmt::Display for AtomicCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Immutable canonical Atomic plan plus non-canonical submission metadata.
/// Application code cannot construct or modify the canonical inner plan.
#[derive(Clone, Debug)]
pub struct AtomicPlan {
    inner: residiuum_atomics::AtomicPlan,
    deadline: Option<Instant>,
}

impl AtomicPlan {
    /// Issued identity retained for retry and outcome resolution.
    pub fn atomic_id(&self) -> AtomicId {
        self.inner.atomic_id()
    }

    /// Replace the submission deadline without changing canonical bytes or
    /// content identity. This permits a safe retry on a renewed connection.
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Remove a previous submission deadline for an explicit unbounded retry.
    pub fn without_deadline(mut self) -> Self {
        self.deadline = None;
        self
    }
}

/// Value observed during Atomic construction.
#[derive(Debug, Clone, PartialEq)]
pub enum AtomicRead<T> {
    /// Present value. Planned values have no establishing version until commit.
    Present {
        /// Decoded value.
        value: T,
        /// Store version for an external observation; `None` for plan overlay.
        version: Option<[u8; 16]>,
    },
    /// Absent externally or because of an earlier planned delete.
    Absent,
}

/// Heap-bound construction session holding no store lock between calls.
pub struct AtomicBuilder {
    heap: HeapClient,
    inner: residiuum_atomics::AtomicBuilder,
    deadline: Option<Instant>,
}

struct AtomicInflightCredit {
    counters: Arc<super::Counters>,
}

impl Drop for AtomicInflightCredit {
    fn drop(&mut self) {
        self.counters.atomic_inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl AtomicBuilder {
    pub(super) fn new(heap: HeapClient, options: AtomicOptions) -> Result<Self, Error> {
        let deadline = options.deadline();
        let heap_id = residiuum_atomics::HeapId::from_bytes(*heap.heap_id().as_bytes())
            .map_err(atomics_error)?;
        let inner =
            residiuum_atomics::AtomicBuilder::new(heap_id, options).map_err(atomics_error)?;
        Ok(Self {
            heap,
            inner,
            deadline,
        })
    }

    fn bound<T>(&self, collection: &Collection<T>) -> Result<BoundCollection, Error> {
        if !self
            .heap
            .heap
            .capability()
            .same_instance(collection.inner.capability())
        {
            return Err(Error::local(
                ErrorCode::PermissionDenied,
                ErrorClass::Request,
                "Atomic collection belongs to a foreign capability instance",
            )
            .with_atomic_code(AtomicCode::AtomicScopeEscape));
        }
        let capability = self.heap.heap.capability();
        let slot = std::sync::Arc::clone(capability.slot());
        let guard = slot.lock_authority_frontier().map_err(|_| {
            Error::local(
                ErrorCode::PermissionDenied,
                ErrorClass::Service,
                "Heap authority frontier lock poisoned",
            )
        })?;
        residiuum_heap::refresh_capability_or_terminate(capability).map_err(|error| {
            Error::local(
                ErrorCode::PermissionDenied,
                ErrorClass::Service,
                error.to_string(),
            )
        })?;
        let mut rights = residiuum_atomics::CollectionRights::empty();
        if capability.rights().contains(residiuum_heap::Rights::READ) {
            rights = rights.union(residiuum_atomics::CollectionRights::READ);
        }
        if capability.rights().contains(residiuum_heap::Rights::WRITE) {
            rights = rights.union(residiuum_atomics::CollectionRights::CREATE);
            rights = rights.union(residiuum_atomics::CollectionRights::PUT);
            rights = rights.union(residiuum_atomics::CollectionRights::REPLACE);
            rights = rights.union(residiuum_atomics::CollectionRights::DELETE);
        }
        let heap_id = residiuum_atomics::HeapId::from_bytes(*capability.heap_id().as_bytes())
            .map_err(atomics_error)?;
        let collection_id =
            CollectionId::from_bytes(*collection.inner.id().as_bytes()).map_err(atomics_error)?;
        let revision = guard.load().atomic_authority_revision();
        // SAFETY: every field comes from this live HeapCap while its authority
        // frontier remains guarded; none is accepted from the application.
        unsafe {
            BoundCollection::from_verified_authority_parts(
                heap_id,
                collection_id,
                rights,
                revision,
                EncodingProfile::STRING_BYTES,
            )
        }
        .map_err(atomics_error)
    }

    /// Insert only when the key is absent.
    pub fn create<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
        value: &T,
    ) -> Result<&mut Self, Error>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let bound = self.bound(collection)?;
        self.inner
            .create(
                &bound,
                CanonicalKey::string(key.into()),
                CanonicalValue::from_bytes(&encode_driver_json(value)?),
            )
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Explicit unconditional upsert.
    pub fn put_unconditional<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
        value: &T,
    ) -> Result<&mut Self, Error>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let bound = self.bound(collection)?;
        self.inner
            .put_unconditional(
                &bound,
                CanonicalKey::string(key.into()),
                CanonicalValue::from_bytes(&encode_driver_json(value)?),
            )
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Replace only when the establishing version is still live.
    pub fn replace<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
        if_version: [u8; 16],
        value: &T,
    ) -> Result<&mut Self, Error>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let bound = self.bound(collection)?;
        self.inner
            .replace(
                &bound,
                CanonicalKey::string(key.into()),
                VersionId::from_bytes(if_version).map_err(atomics_error)?,
                CanonicalValue::from_bytes(&encode_driver_json(value)?),
            )
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Delete only when the establishing version is still live.
    pub fn delete<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
        if_version: [u8; 16],
    ) -> Result<&mut Self, Error> {
        let bound = self.bound(collection)?;
        self.inner
            .delete(
                &bound,
                CanonicalKey::string(key.into()),
                VersionId::from_bytes(if_version).map_err(atomics_error)?,
            )
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Require absence at the serialization frontier.
    pub fn assert_absent<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
    ) -> Result<&mut Self, Error> {
        let bound = self.bound(collection)?;
        self.inner
            .assert_absent(&bound, CanonicalKey::string(key.into()))
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Require presence at the serialization frontier.
    pub fn assert_present<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
    ) -> Result<&mut Self, Error> {
        let bound = self.bound(collection)?;
        self.inner
            .assert_present(&bound, CanonicalKey::string(key.into()))
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Require an exact establishing version.
    pub fn assert_version<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
        version: [u8; 16],
    ) -> Result<&mut Self, Error> {
        let bound = self.bound(collection)?;
        self.inner
            .assert_version(
                &bound,
                CanonicalKey::string(key.into()),
                VersionId::from_bytes(version).map_err(atomics_error)?,
            )
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Read through the plan overlay, or perform one bounded external read and
    /// record its exact version/absence witness.
    pub async fn read<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
    ) -> Result<AtomicRead<T>, Error>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let key = key.into();
        let bound = self.bound(collection)?;
        match self
            .inner
            .read_your_plan(&bound, &CanonicalKey::string(key.clone()))
            .map_err(atomics_error)?
        {
            ConstructionRead::Present { encoded_value, .. } => Ok(AtomicRead::Present {
                value: decode_value(&encoded_value)?,
                version: None,
            }),
            ConstructionRead::Absent { .. } => Ok(AtomicRead::Absent),
            ConstructionRead::External => {
                let inner = collection.inner.clone();
                let heap = std::sync::Arc::clone(&self.heap.heap);
                let read_key = key.clone();
                let request_id = mint_request_id()?;
                let (observed, frontier) = self
                    .heap
                    .connection
                    .scheduler
                    .dispatch(request_id, None, None, move || {
                        Ok((
                            inner.get_versioned_raw_body(&read_key)?,
                            heap.segment_fingerprint()?,
                        ))
                    })
                    .await?;
                self.inner
                    .bind_read_frontier(frontier)
                    .map_err(atomics_error)?;
                match observed {
                    None => {
                        self.inner
                            .witness_absent(
                                &bound,
                                CanonicalKey::string(key.clone()),
                                absent_hash(collection.inner.id(), key.as_bytes()),
                            )
                            .map_err(atomics_error)?;
                        Ok(AtomicRead::Absent)
                    }
                    Some((body, version)) => {
                        self.inner
                            .witness_version(
                                &bound,
                                CanonicalKey::string(key),
                                VersionId::from_bytes(version).map_err(atomics_error)?,
                                *blake3::hash(&body).as_bytes(),
                            )
                            .map_err(atomics_error)?;
                        Ok(AtomicRead::Present {
                            value: decode_value(&body)?,
                            version: Some(version),
                        })
                    }
                }
            }
        }
    }

    /// Close into one immutable canonical plan.
    pub fn build(self) -> Result<AtomicPlan, Error> {
        Ok(AtomicPlan {
            inner: self.inner.build().map_err(atomics_error)?,
            deadline: self.deadline,
        })
    }
}

pub(super) async fn commit(heap: &HeapClient, plan: AtomicPlan) -> Result<AtomicOutcome, Error> {
    let expected =
        residiuum_atomics::HeapId::from_bytes(*heap.heap_id().as_bytes()).map_err(atomics_error)?;
    if plan.inner.heap_id() != expected {
        return Err(Error::local(
            ErrorCode::PermissionDenied,
            ErrorClass::Request,
            "Atomic plan belongs to a foreign Heap",
        )
        .with_atomic_code(AtomicCode::AtomicScopeEscape));
    }
    let bytes = residiuum_atomics::encode_canonical_plan(&plan.inner)
        .map_err(atomics_error)?
        .len();
    let members = plan.inner.mutations().len();
    let atomic_id = plan.inner.atomic_id();
    let deadline = plan.deadline;
    let inner = plan.inner;
    let bound = std::sync::Arc::clone(&heap.heap);
    let request_id = mint_request_id()?;
    let counters = Arc::clone(&heap.connection.scheduler.counters);
    counters.atomic_submitted.fetch_add(1, Ordering::Relaxed);
    counters.atomic_inflight.fetch_add(1, Ordering::AcqRel);
    counters
        .atomic_submitted_members
        .fetch_add(members as u64, Ordering::Relaxed);
    counters
        .atomic_max_members
        .fetch_max(members, Ordering::Relaxed);
    counters
        .atomic_submitted_plan_bytes
        .fetch_add(bytes as u64, Ordering::Relaxed);
    counters
        .atomic_max_plan_bytes
        .fetch_max(bytes, Ordering::Relaxed);
    let unknown_counters = Arc::clone(&counters);
    let operation_counters = Arc::clone(&counters);
    let credit = AtomicInflightCredit {
        counters: Arc::clone(&counters),
    };
    let result = heap
        .connection
        .scheduler
        .dispatch_weighted_with_unknown(
            request_id,
            deadline,
            bytes,
            move || {
                unknown_counters
                    .atomic_unknown
                    .fetch_add(1, Ordering::Relaxed);
                AtomicOutcome::Unknown {
                    atomic_id,
                    resolution: AtomicResolutionHandle { atomic_id },
                }
            },
            move || {
                let _credit = credit;
                let started = Instant::now();
                let outcome = match bound.decide_atomic_plan_outcome(&inner) {
                    Ok(outcome) => outcome,
                    Err(error) if definitely_before_atomic_acceptance(&error) => return Err(error),
                    Err(_) => AtomicOutcome::Unknown {
                        atomic_id,
                        resolution: AtomicResolutionHandle { atomic_id },
                    },
                };
                let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                operation_counters
                    .atomic_total_engine_latency_ns
                    .fetch_add(elapsed, Ordering::Relaxed);
                operation_counters
                    .atomic_max_engine_latency_ns
                    .fetch_max(elapsed, Ordering::Relaxed);
                match &outcome {
                    AtomicOutcome::Committed(_) => {
                        operation_counters
                            .atomic_committed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    AtomicOutcome::NotCommitted { .. } => {
                        operation_counters
                            .atomic_not_committed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    AtomicOutcome::Unknown { .. } => {
                        operation_counters
                            .atomic_unknown
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(outcome)
            },
        )
        .await;
    if let Err(error) = &result {
        counters.atomic_refused.fetch_add(1, Ordering::Relaxed);
        if error.code == super::ErrorCode::AtomicIdConflict {
            counters
                .atomic_identity_conflicts
                .fetch_add(1, Ordering::Relaxed);
        }
    }
    result
}

pub(super) async fn status(heap: &HeapClient, atomic_id: AtomicId) -> Result<AtomicStatus, Error> {
    heap.connection
        .scheduler
        .counters
        .atomic_status_lookups
        .fetch_add(1, Ordering::Relaxed);
    let bound = std::sync::Arc::clone(&heap.heap);
    let request_id = mint_request_id()?;
    heap.connection
        .scheduler
        .dispatch(request_id, None, None, move || {
            bound.atomic_status(atomic_id)
        })
        .await
}

fn definitely_before_atomic_acceptance(error: &crate::Error) -> bool {
    matches!(
        error,
        crate::Error::Store(
            residiuum_store::StoreError::Atomic(_)
                | residiuum_store::StoreError::HeapCapability(_)
                | residiuum_store::StoreError::HeapAdmit(_)
        )
    )
}

fn decode_value<T: DeserializeOwned>(body: &[u8]) -> Result<T, Error> {
    let json = crate::value::decode_json(body).map_err(|error| {
        Error::local(
            ErrorCode::DataDamaged,
            ErrorClass::Service,
            error.to_string(),
        )
    })?;
    serde_json::from_value(json).map_err(|error| {
        Error::local(
            ErrorCode::DataDamaged,
            ErrorClass::Service,
            error.to_string(),
        )
    })
}

fn absent_hash(collection: residiuum_heap::CollectionId, key: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"RESIDIUUM-ATOMIC-ABSENT-PROJECTION-V1");
    hash.update(collection.as_bytes());
    hash.update(key);
    *hash.finalize().as_bytes()
}

fn atomics_error(error: residiuum_atomics::AtomicsError) -> Error {
    use residiuum_atomics::AtomicRefuseReason;
    let (code, class) = match &error {
        residiuum_atomics::AtomicsError::Refused(
            AtomicRefuseReason::AuthorizationFailure
            | AtomicRefuseReason::CrossHeapCollection
            | AtomicRefuseReason::StaleOrForeignCapability,
        ) => (ErrorCode::PermissionDenied, ErrorClass::Request),
        residiuum_atomics::AtomicsError::Refused(AtomicRefuseReason::LimitExceeded) => {
            (ErrorCode::ResourceLimit, ErrorClass::Request)
        }
        residiuum_atomics::AtomicsError::Refused(AtomicRefuseReason::Deadline) => {
            (ErrorCode::AtomicDeadlineExceeded, ErrorClass::Cancellation)
        }
        residiuum_atomics::AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict) => {
            (ErrorCode::AtomicIdConflict, ErrorClass::Request)
        }
        _ => (ErrorCode::Validation, ErrorClass::Request),
    };
    let atomic_code = AtomicCode::from_error(error);
    let error = Error::local(code, class, error.to_string());
    match atomic_code {
        Some(code) => error.with_atomic_code(code),
        None => error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn section_22_vocabulary_has_exact_stable_spellings() {
        let cases = [
            (AtomicCode::AtomicIdConflict, "atomic_id_conflict"),
            (AtomicCode::AtomicIdInvalid, "atomic_id_invalid"),
            (AtomicCode::AtomicScopeEscape, "atomic_scope_escape"),
            (
                AtomicCode::AtomicScopeUnavailable,
                "atomic_scope_unavailable",
            ),
            (AtomicCode::AtomicLimitExceeded, "atomic_limit_exceeded"),
            (
                AtomicCode::AtomicDeadlineExceeded,
                "atomic_deadline_exceeded",
            ),
            (AtomicCode::AtomicReadConflict, "atomic_read_conflict"),
            (
                AtomicCode::AtomicPredicateConflict,
                "atomic_predicate_conflict",
            ),
            (AtomicCode::AtomicRuleChanged, "atomic_rule_changed"),
            (AtomicCode::AtomicRuleViolation, "atomic_rule_violation"),
            (AtomicCode::AtomicNotCommitted, "atomic_not_committed"),
            (AtomicCode::AtomicOutcomeUnknown, "atomic_outcome_unknown"),
            (
                AtomicCode::AtomicEvidenceConflicting,
                "atomic_evidence_conflicting",
            ),
            (
                AtomicCode::AtomicCoverageIncomplete,
                "atomic_coverage_incomplete",
            ),
            (AtomicCode::AtomicMaterialPartial, "atomic_material_partial"),
            (AtomicCode::AtomicRightDenied, "atomic_right_denied"),
            (
                AtomicCode::RelationshipParentMissing,
                "relationship_parent_missing",
            ),
            (
                AtomicCode::RelationshipChildrenExist,
                "relationship_children_exist",
            ),
            (AtomicCode::RelationshipDegraded, "relationship_degraded"),
            (AtomicCode::UniqueValueExists, "unique_value_exists"),
        ];
        for (code, expected) in cases {
            assert_eq!(code.as_str(), expected);
            assert_eq!(code.to_string(), expected);
        }
    }

    #[test]
    fn classifications_preserve_proof_strength() {
        assert_eq!(
            AtomicCode::from_refusal(AtomicRefuseReason::CrossHeapCollection),
            Some(AtomicCode::AtomicScopeEscape)
        );
        assert_eq!(
            AtomicCode::from_refusal(AtomicRefuseReason::MalformedInput),
            None
        );

        let atomic_id = AtomicId::from_bytes([7; 32]).unwrap();
        let coarse_abort = AtomicOutcome::NotCommitted {
            atomic_id,
            reason: AtomicAbortReason::PreconditionConflict,
        };
        assert_eq!(
            AtomicCode::from_outcome(&coarse_abort),
            Some(AtomicCode::AtomicNotCommitted)
        );
        let coverage_abort = AtomicOutcome::NotCommitted {
            atomic_id,
            reason: AtomicAbortReason::CoverageIncomplete,
        };
        assert_eq!(
            AtomicCode::from_outcome(&coverage_abort),
            Some(AtomicCode::AtomicCoverageIncomplete)
        );

        let conflicting = AtomicStatus {
            logical: LogicalStatus::UnknownCommit,
            material: MaterialStatus::Conflicting,
            content_root: None,
            receipt: None,
        };
        assert_eq!(
            AtomicCode::from_status(&conflicting),
            Some(AtomicCode::AtomicEvidenceConflicting)
        );
        assert_eq!(
            AtomicCode::from_status(&AtomicStatus::incomplete_coverage()),
            Some(AtomicCode::AtomicCoverageIncomplete)
        );
        assert_eq!(AtomicCode::from_status(&AtomicStatus::not_found()), None);
    }

    #[test]
    fn structural_errors_carry_the_precise_atomic_code() {
        let cases = [
            (
                AtomicRefuseReason::CrossHeapCollection,
                ErrorCode::PermissionDenied,
                AtomicCode::AtomicScopeEscape,
            ),
            (
                AtomicRefuseReason::ScopeUnavailable,
                ErrorCode::Validation,
                AtomicCode::AtomicScopeUnavailable,
            ),
            (
                AtomicRefuseReason::AuthorizationFailure,
                ErrorCode::PermissionDenied,
                AtomicCode::AtomicRightDenied,
            ),
            (
                AtomicRefuseReason::LimitExceeded,
                ErrorCode::ResourceLimit,
                AtomicCode::AtomicLimitExceeded,
            ),
            (
                AtomicRefuseReason::Deadline,
                ErrorCode::AtomicDeadlineExceeded,
                AtomicCode::AtomicDeadlineExceeded,
            ),
            (
                AtomicRefuseReason::AtomicIdConflict,
                ErrorCode::AtomicIdConflict,
                AtomicCode::AtomicIdConflict,
            ),
        ];
        for (reason, driver_code, atomic_code) in cases {
            let error = atomics_error(residiuum_atomics::AtomicsError::Refused(reason));
            assert_eq!(error.code, driver_code);
            assert_eq!(error.atomic_code, Some(atomic_code));
        }

        let invalid = atomics_error(residiuum_atomics::AtomicsError::InvalidIdentity(
            "test identity",
        ));
        assert_eq!(invalid.code, ErrorCode::Validation);
        assert_eq!(invalid.atomic_code, Some(AtomicCode::AtomicIdInvalid));

        let malformed = atomics_error(residiuum_atomics::AtomicsError::Refused(
            AtomicRefuseReason::MalformedInput,
        ));
        assert_eq!(malformed.code, ErrorCode::Validation);
        assert_eq!(malformed.atomic_code, None);
    }
}
