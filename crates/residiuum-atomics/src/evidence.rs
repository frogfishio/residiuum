//! Authoritative evidence and formal lifecycle phases (`ATOMICS_SPEC` §§9–11).
//!
//! These types are the semantic freeze. Persistent CBOR field numbers are ATM-0.2.
//! No store append path lives here.

use crate::id::{AtomicId, CollectionId, ContentRoot, HeapId, VersionId};
use crate::limits::ResourceLimits;
use crate::plan::{CoordinationScope, MutationKind};

/// Decision written by the engine. Examination-only states are not decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum DecisionCode {
    /// Transition committed.
    Committed = 1,
    /// Transition not committed. The accepted ID is never re-executed.
    NotCommitted = 2,
}

impl DecisionCode {
    /// Wire code from `ATOMICS_SPEC` §9.
    pub const fn wire_code(self) -> u8 {
        self as u8
    }

    /// Decode a known decision code.
    pub const fn from_wire_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Committed),
            2 => Some(Self::NotCommitted),
            _ => None,
        }
    }

    /// Stable snake_case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::NotCommitted => "not_committed",
        }
    }
}

/// Durable acknowledgement class. Product receipts only advertise `durable`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Durability {
    /// Durable acknowledgement. Buffering/group commit may exist internally.
    Durable,
}

/// Formal prepare phase. Not a store file state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PreparePhase {
    /// No valid prepare exists.
    None,
    /// Valid prepare recorded for `(heap_id, atomic_id, content_root)`.
    Prepared,
}

/// Formal member material phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MemberPhase {
    /// No staged members.
    Absent,
    /// Members appended but not yet at the member stable boundary.
    Staged,
    /// Named member bytes are durable and still invisible to ordinary reads.
    DurableInvisible,
}

/// Formal decision phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DecisionPhase {
    /// No valid decision.
    None,
    /// Durable committed decision.
    Committed,
    /// Durable not-committed decision.
    NotCommitted,
    /// Conflicting valid decisions. Heap is degraded; never guessed.
    Conflicting,
}

/// Formal publication phase. Publication follows a committed decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PublicationPhase {
    /// No ordinary-visible mutation from this Atomic.
    Unpublished,
    /// One whole-Heap read-view delta has been applied.
    Published,
}

/// Combined formal lifecycle. Used by the later oracle and store kernel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AtomicLifecycle {
    /// Prepare phase.
    pub prepare: PreparePhase,
    /// Member phase.
    pub members: MemberPhase,
    /// Decision phase.
    pub decision: DecisionPhase,
    /// Publication phase.
    pub publication: PublicationPhase,
}

impl AtomicLifecycle {
    /// Initial state before any evidence exists.
    pub const fn empty() -> Self {
        Self {
            prepare: PreparePhase::None,
            members: MemberPhase::Absent,
            decision: DecisionPhase::None,
            publication: PublicationPhase::Unpublished,
        }
    }

    /// Ordinary readers must not see staged members.
    pub const fn ordinary_visible(self) -> bool {
        matches!(self.publication, PublicationPhase::Published)
            && matches!(self.decision, DecisionPhase::Committed)
    }
}

/// Prepare record shape (`ATOMICS_SPEC` §9). Hashing is ATM-0.2.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicPrepare {
    /// Issued Atomic identity.
    pub atomic_id: AtomicId,
    /// Heap this Atomic is bound to.
    pub heap_id: HeapId,
    /// Declared coordination scope.
    pub scope: CoordinationScope,
    /// Plan content root.
    pub content_root: ContentRoot,
    /// Commit frontier bound at prepare. Encoding is ATM-0.2.
    pub frontier: [u8; 32],
    /// Ordered member manifest root.
    pub ordered_member_manifest_root: [u8; 32],
    /// Read-set root.
    pub read_set_root: [u8; 32],
    /// Predicate-set root.
    pub predicate_set_root: [u8; 32],
    /// Active rule-revision root.
    pub active_rule_revision_root: [u8; 32],
    /// Applied limits recorded in the prepare.
    pub limits: ResourceLimits,
}

/// Member record shape (`ATOMICS_SPEC` §9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicMember {
    /// Enclosing Atomic.
    pub atomic_id: AtomicId,
    /// Manifest ordinal.
    pub ordinal: u32,
    /// Target collection.
    pub collection_id: CollectionId,
    /// Mutation kind after assertions have been folded.
    pub member_kind: MutationKind,
    /// Observed before-version, if any.
    pub before_version: Option<VersionId>,
    /// After-content hash. Absent for delete.
    pub after_content_hash: Option<[u8; 32]>,
    /// History event identity.
    pub event_id: VersionId,
}

/// Decision record shape (`ATOMICS_SPEC` §9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicDecision {
    /// Enclosing Atomic.
    pub atomic_id: AtomicId,
    /// Hash of the verified prepare. Encoding is ATM-0.2.
    pub prepare_hash: [u8; 32],
    /// Ordered member root.
    pub member_root: [u8; 32],
    /// Member count named by the decision.
    pub member_count: u32,
    /// Engine decision.
    pub decision: DecisionCode,
    /// Heap commit position. Present only when committed.
    pub commit_position: Option<u64>,
    /// Durability class of the acknowledgement.
    pub durability: Durability,
}

/// Compact lifetime tombstone (`ATOMICS_SPEC` §12).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecisionTombstone {
    /// Issued identity.
    pub atomic_id: AtomicId,
    /// Plan content root.
    pub content_root: ContentRoot,
    /// Terminal decision.
    pub decision: DecisionCode,
    /// Commit position when committed.
    pub commit_position: Option<u64>,
    /// Decision evidence hash. Encoding is ATM-0.2.
    pub decision_hash: [u8; 32],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_codes_match_spec() {
        assert_eq!(DecisionCode::Committed.wire_code(), 1);
        assert_eq!(DecisionCode::NotCommitted.wire_code(), 2);
        assert_eq!(DecisionCode::from_wire_code(3), None);
        assert_eq!(DecisionCode::NotCommitted.as_str(), "not_committed");
    }

    #[test]
    fn staged_members_are_not_ordinary_visible() {
        let mut life = AtomicLifecycle::empty();
        life.prepare = PreparePhase::Prepared;
        life.members = MemberPhase::DurableInvisible;
        assert!(!life.ordinary_visible());
        life.decision = DecisionPhase::Committed;
        assert!(!life.ordinary_visible());
        life.publication = PublicationPhase::Published;
        assert!(life.ordinary_visible());
    }
}
