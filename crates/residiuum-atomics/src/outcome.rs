//! Outcomes, status axes, receipts, and abort reasons (`ATOMICS_SPEC` §10).

use crate::id::{AtomicId, CollectionId, ContentRoot, HeapId, VersionId};

/// Durable not-committed reason after the Atomic was issued.
///
/// Structural refusals before acceptance are [`AtomicRefuseReason`] and write
/// no evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AtomicAbortReason {
    /// Data precondition failed at the serialization frontier.
    PreconditionConflict,
    /// Active rule or relationship rejected the closed plan.
    RuleRejected,
    /// Recovery wrote a deterministic not-committed decision.
    RecoveryAbort,
    /// Absence or uniqueness could not be proven under complete coverage.
    CoverageIncomplete,
}

impl AtomicAbortReason {
    /// Stable snake_case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreconditionConflict => "precondition_conflict",
            Self::RuleRejected => "rule_rejected",
            Self::RecoveryAbort => "recovery_abort",
            Self::CoverageIncomplete => "coverage_incomplete",
        }
    }
}

/// Structural refusal before acceptance. No Atomic is issued.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AtomicRefuseReason {
    /// Malformed input or illegal identity.
    MalformedInput,
    /// Profile is unknown or not executable on this backend.
    UnsupportedProfile,
    /// Same `(collection_id, canonical_key)` appeared twice as a mutation target.
    DuplicateTarget,
    /// A collection belongs to another Heap.
    CrossHeapCollection,
    /// Capability is stale or foreign to this Heap.
    StaleOrForeignCapability,
    /// Key or value failed the collection encoding contract.
    InvalidValue,
    /// Mutation kind is not in the closed v1 set.
    UnknownMutationKind,
    /// Predicate kind is unsupported on this backend.
    UnsupportedPredicate,
    /// Configured or hard resource ceiling.
    LimitExceeded,
    /// Deadline expired before acceptance.
    Deadline,
    /// Missing right on the union of proposed operations.
    AuthorizationFailure,
    /// Caller cancelled before admission.
    CancelledBeforeAcceptance,
    /// Same `AtomicId` with a different content root.
    AtomicIdConflict,
}

impl AtomicRefuseReason {
    /// Stable snake_case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MalformedInput => "malformed_input",
            Self::UnsupportedProfile => "unsupported_profile",
            Self::DuplicateTarget => "duplicate_target",
            Self::CrossHeapCollection => "cross_heap_collection",
            Self::StaleOrForeignCapability => "stale_or_foreign_capability",
            Self::InvalidValue => "invalid_value",
            Self::UnknownMutationKind => "unknown_mutation_kind",
            Self::UnsupportedPredicate => "unsupported_predicate",
            Self::LimitExceeded => "limit_exceeded",
            Self::Deadline => "deadline",
            Self::AuthorizationFailure => "authorization_failure",
            Self::CancelledBeforeAcceptance => "cancelled_before_acceptance",
            Self::AtomicIdConflict => "atomic_id_conflict",
        }
    }
}

/// Logical status axis. Independent of material coverage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LogicalStatus {
    /// Durable committed decision.
    Committed,
    /// Durable not-committed decision.
    NotCommitted,
    /// Observer cannot establish whether a decision existed.
    UnknownCommit,
    /// Conflicting valid decisions. Never guessed.
    ConflictingDecisionEvidence,
    /// Lookup result: complete coverage and no prepare or decision for the ID.
    /// Never used when coverage is incomplete.
    NotFound,
}

impl LogicalStatus {
    /// Stable snake_case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::NotCommitted => "not_committed",
            Self::UnknownCommit => "unknown_commit",
            Self::ConflictingDecisionEvidence => "conflicting_decision_evidence",
            Self::NotFound => "not_found",
        }
    }
}

/// Material status axis. Independent of the logical decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MaterialStatus {
    /// Named material is complete.
    Complete,
    /// Some named material is damaged or missing; healthy members remain examinable.
    Partial,
    /// Named material is missing.
    Missing,
    /// Material evidence conflicts.
    Conflicting,
    /// Coverage is incomplete; absence cannot be proven.
    CoverageIncomplete,
}

impl MaterialStatus {
    /// Stable snake_case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Missing => "missing",
            Self::Conflicting => "conflicting",
            Self::CoverageIncomplete => "coverage_incomplete",
        }
    }
}

/// Combined status. Receipt is present only when committed material permits it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicStatus {
    /// Logical axis.
    pub logical: LogicalStatus,
    /// Material axis.
    pub material: MaterialStatus,
    /// Content root when known.
    pub content_root: Option<ContentRoot>,
}

impl AtomicStatus {
    /// Incomplete coverage must not be reported as [`LogicalStatus::NotFound`].
    pub const fn incomplete_coverage() -> Self {
        Self {
            logical: LogicalStatus::UnknownCommit,
            material: MaterialStatus::CoverageIncomplete,
            content_root: None,
        }
    }
}

/// Handle used to resolve an unknown observer outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AtomicResolutionHandle {
    /// Identity that must be looked up after admission.
    pub atomic_id: AtomicId,
}

/// Per-member committed receipt (`ATOMICS_SPEC` §15).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicMemberReceipt {
    /// Target collection.
    pub collection_id: CollectionId,
    /// Canonical key bytes. Encoding is ATM-0.2.
    pub key: Vec<u8>,
    /// Before version, if any.
    pub before_version: Option<VersionId>,
    /// After version. Absent for delete.
    pub after_version: Option<VersionId>,
    /// History event identity.
    pub event_id: VersionId,
}

/// Committed receipt (`ATOMICS_SPEC` §15).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicReceipt {
    /// Issued identity.
    pub atomic_id: AtomicId,
    /// Heap the Atomic committed on.
    pub heap_id: HeapId,
    /// Plan content root.
    pub content_root: ContentRoot,
    /// Heap commit position.
    pub commit_position: u64,
    /// Members in canonical target order.
    pub members: Vec<AtomicMemberReceipt>,
    /// Decision evidence hash. Encoding is ATM-0.2.
    pub decision_hash: [u8; 32],
    /// True when this receipt is a same-ID/same-root replay.
    pub replayed: bool,
}

/// Product outcome of submit or status resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AtomicOutcome {
    /// Durable committed transition.
    Committed(AtomicReceipt),
    /// Durable not-committed decision.
    NotCommitted {
        /// Issued identity.
        atomic_id: AtomicId,
        /// Why the issued Atomic did not commit.
        reason: AtomicAbortReason,
    },
    /// Observer knowledge is incomplete. Not a third engine decision.
    Unknown {
        /// Issued or submitted identity.
        atomic_id: AtomicId,
        /// Later `atomic_status` handle.
        resolution: AtomicResolutionHandle,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abort_and_refuse_codes_are_snake_case() {
        assert_eq!(
            AtomicAbortReason::PreconditionConflict.as_str(),
            "precondition_conflict"
        );
        assert_eq!(
            AtomicRefuseReason::AtomicIdConflict.as_str(),
            "atomic_id_conflict"
        );
        assert_eq!(LogicalStatus::UnknownCommit.as_str(), "unknown_commit");
        assert_eq!(
            MaterialStatus::CoverageIncomplete.as_str(),
            "coverage_incomplete"
        );
    }

    #[test]
    fn incomplete_coverage_is_never_not_found() {
        let s = AtomicStatus::incomplete_coverage();
        assert_eq!(s.logical, LogicalStatus::UnknownCommit);
        assert_ne!(s.logical, LogicalStatus::NotFound);
        assert_eq!(s.material, MaterialStatus::CoverageIncomplete);
    }
}
