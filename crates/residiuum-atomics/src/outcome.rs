//! Outcomes, status axes, receipts, and abort reasons (`ATOMICS_SPEC` §§10, 15).

use crate::evidence::Durability;
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

    /// Wire code recorded on a durable not-committed decision (`spec/atomics/cbor-v1.json`).
    pub const fn wire_code(self) -> u8 {
        match self {
            Self::PreconditionConflict => 1,
            Self::RuleRejected => 2,
            Self::RecoveryAbort => 3,
            Self::CoverageIncomplete => 4,
        }
    }

    /// Decode a known abort-reason code.
    pub const fn from_wire_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::PreconditionConflict),
            2 => Some(Self::RuleRejected),
            3 => Some(Self::RecoveryAbort),
            4 => Some(Self::CoverageIncomplete),
            _ => None,
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
    /// Declared scope is not enabled for this profile (`atomic_scope_unavailable`).
    ScopeUnavailable,
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
            Self::ScopeUnavailable => "atomic_scope_unavailable",
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
    /// Committed receipt when committed evidence is complete enough to reproduce it.
    /// Never present for [`LogicalStatus::NotFound`] or not-committed decisions.
    pub receipt: Option<AtomicReceipt>,
}

impl AtomicStatus {
    /// Incomplete coverage must not be reported as [`LogicalStatus::NotFound`].
    pub const fn incomplete_coverage() -> Self {
        Self {
            logical: LogicalStatus::UnknownCommit,
            material: MaterialStatus::CoverageIncomplete,
            content_root: None,
            receipt: None,
        }
    }

    /// Complete coverage and no prepare or decision. Not a logical engine decision.
    pub const fn not_found() -> Self {
        Self {
            logical: LogicalStatus::NotFound,
            material: MaterialStatus::Complete,
            content_root: None,
            receipt: None,
        }
    }

    /// A committed receipt may be attached only for committed + complete/partial material.
    pub const fn receipt_permitted(logical: LogicalStatus, material: MaterialStatus) -> bool {
        matches!(logical, LogicalStatus::Committed)
            && matches!(material, MaterialStatus::Complete | MaterialStatus::Partial)
    }

    /// Attach a committed receipt when material availability permits it.
    pub fn with_committed_receipt(self, receipt: AtomicReceipt) -> Option<Self> {
        if !Self::receipt_permitted(self.logical, self.material) {
            return None;
        }
        Some(Self {
            receipt: Some(receipt),
            ..self
        })
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
    /// Product acknowledgements are durable (`ATOMICS_SPEC` §15).
    pub durability: Durability,
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

/// One independently resolved entry in a physical Atomic durability cohort.
///
/// Structural refusal remains outside the issued Atomic outcome axis and does
/// not poison neighboring plans that share the physical boundaries.
pub type AtomicCohortOutcome = Result<AtomicOutcome, AtomicRefuseReason>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abort_and_refuse_codes_are_snake_case() {
        assert_eq!(
            AtomicAbortReason::PreconditionConflict.as_str(),
            "precondition_conflict"
        );
        assert_eq!(AtomicAbortReason::PreconditionConflict.wire_code(), 1);
        assert_eq!(AtomicAbortReason::CoverageIncomplete.wire_code(), 4);
        assert_eq!(
            AtomicAbortReason::from_wire_code(3),
            Some(AtomicAbortReason::RecoveryAbort)
        );
        assert_eq!(AtomicAbortReason::from_wire_code(0), None);
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
        assert!(s.receipt.is_none());
    }

    #[test]
    fn not_found_is_outside_the_logical_decision_axis() {
        let s = AtomicStatus::not_found();
        assert_eq!(s.logical, LogicalStatus::NotFound);
        assert_ne!(s.logical, LogicalStatus::Committed);
        assert_ne!(s.logical, LogicalStatus::NotCommitted);
        assert!(s.receipt.is_none());
        assert!(!AtomicStatus::receipt_permitted(s.logical, s.material));
    }

    fn sample_receipt() -> AtomicReceipt {
        let mut hid = [0u8; 16];
        hid[0] = 1;
        let mut aid = [0u8; 32];
        aid[0] = 1;
        AtomicReceipt {
            atomic_id: AtomicId::from_bytes(aid).unwrap(),
            heap_id: HeapId::from_bytes(hid).unwrap(),
            content_root: ContentRoot::from_bytes([7u8; 32]).unwrap(),
            commit_position: 1,
            durability: Durability::Durable,
            members: Vec::new(),
            decision_hash: [8u8; 32],
            replayed: false,
        }
    }

    #[test]
    fn committed_receipt_is_durable_and_attachable() {
        let receipt = sample_receipt();
        assert_eq!(receipt.durability, Durability::Durable);
        let status = AtomicStatus {
            logical: LogicalStatus::Committed,
            material: MaterialStatus::Complete,
            content_root: Some(receipt.content_root),
            receipt: None,
        }
        .with_committed_receipt(receipt.clone())
        .expect("complete committed material permits a receipt");
        assert_eq!(
            status.receipt.as_ref().unwrap().durability,
            Durability::Durable
        );
    }

    #[test]
    fn not_committed_and_incomplete_cannot_carry_a_receipt() {
        let receipt = sample_receipt();
        assert!(AtomicStatus {
            logical: LogicalStatus::NotCommitted,
            material: MaterialStatus::Complete,
            content_root: Some(receipt.content_root),
            receipt: None,
        }
        .with_committed_receipt(receipt.clone())
        .is_none());
        assert!(AtomicStatus::incomplete_coverage()
            .with_committed_receipt(receipt)
            .is_none());
    }
}
