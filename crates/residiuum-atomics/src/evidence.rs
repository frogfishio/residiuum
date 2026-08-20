//! Authoritative evidence and formal lifecycle phases (`ATOMICS_SPEC` §§9–11).
//!
//! These types are the semantic freeze. Persistent CBOR field numbers and
//! codecs for prepare/member/decision/tombstone are ATM-0.8.
//! No store append path lives here.

use crate::id::{AtomicId, CollectionId, ContentRoot, HeapId, VersionId};
use crate::limits::ResourceLimits;
use crate::outcome::{AtomicAbortReason, AtomicOutcome, AtomicRefuseReason};
use crate::plan::{CanonicalKey, CoordinationScope, MutationKind};
use crate::AtomicsError;

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

impl Durability {
    /// Stable snake_case name. Product receipts only advertise `durable`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
        }
    }

    /// Wire code (`spec/atomics/cbor-v1.json`).
    pub const fn wire_code(self) -> u8 {
        match self {
            Self::Durable => 1,
        }
    }

    /// Decode a known durability code.
    pub const fn from_wire_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Durable),
            _ => None,
        }
    }
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

/// Canonical object identity: collection plus key (`ATOMICS_SPEC` §§7, 9).
///
/// Bound into the member hash and ordered manifest so recovery can verify
/// which object a member represents without consulting the plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectIdentity {
    /// Collection that owns the key.
    pub collection_id: CollectionId,
    /// Canonical key inside that collection.
    pub key: CanonicalKey,
}

impl ObjectIdentity {
    /// Construct an identity from a collection and key.
    pub const fn new(collection_id: CollectionId, key: CanonicalKey) -> Self {
        Self { collection_id, key }
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
    /// Exact intended member count. Required for deterministic recovery when
    /// no member record survived beyond the durable prepare.
    pub member_count: u32,
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
    /// Target object: collection plus canonical key.
    pub object_identity: ObjectIdentity,
    /// Mutation kind after assertions have been folded.
    pub member_kind: MutationKind,
    /// Observed before-version, if any.
    pub before_version: Option<VersionId>,
    /// After-content hash. Absent for delete.
    pub after_content_hash: Option<[u8; 32]>,
    /// History event identity.
    pub event_id: VersionId,
}

impl AtomicMember {
    /// Refuse kind/field combinations that cannot be a real member.
    ///
    /// Create has an after-hash and no before-version. Put is an upsert: after-hash
    /// is required, before-version is present when an existing object is overwritten
    /// and absent on insert. Replace has both; delete has a before-version and no
    /// after-hash (`ATOMICS_SPEC` §9).
    pub fn validate(&self) -> Result<(), AtomicsError> {
        let before = self.before_version.is_some();
        let after = self.after_content_hash.is_some();
        let legal = match self.member_kind {
            MutationKind::Create => !before && after,
            MutationKind::Put => after,
            MutationKind::Replace => before && after,
            MutationKind::Delete => before && !after,
        };
        if legal {
            Ok(())
        } else {
            Err(AtomicsError::Refused(AtomicRefuseReason::InvalidValue))
        }
    }
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
    /// Exact abort reason. Required when `not_committed`; omitted when committed.
    /// Frozen so a durable decision reconstructs [`AtomicOutcome::NotCommitted`].
    pub abort_reason: Option<AtomicAbortReason>,
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
    /// Exact abort reason. Required when `not_committed`; omitted when committed.
    pub abort_reason: Option<AtomicAbortReason>,
}

impl AtomicDecision {
    /// Durable committed decision. `commit_position` must be nonzero.
    pub fn committed(
        atomic_id: AtomicId,
        prepare_hash: [u8; 32],
        member_root: [u8; 32],
        member_count: u32,
        commit_position: u64,
    ) -> Result<Self, AtomicsError> {
        let rec = Self {
            atomic_id,
            prepare_hash,
            member_root,
            member_count,
            decision: DecisionCode::Committed,
            commit_position: Some(commit_position),
            durability: Durability::Durable,
            abort_reason: None,
        };
        rec.validate()?;
        Ok(rec)
    }

    /// Durable not-committed decision. Preserves the exact abort reason.
    pub fn not_committed(
        atomic_id: AtomicId,
        prepare_hash: [u8; 32],
        member_root: [u8; 32],
        member_count: u32,
        reason: AtomicAbortReason,
    ) -> Self {
        Self {
            atomic_id,
            prepare_hash,
            member_root,
            member_count,
            decision: DecisionCode::NotCommitted,
            commit_position: None,
            durability: Durability::Durable,
            abort_reason: Some(reason),
        }
    }

    /// Refuse committed/not-committed field combinations that cannot be decoded.
    pub fn validate(&self) -> Result<(), AtomicsError> {
        validate_decision_axis(self.decision, self.commit_position, self.abort_reason)
    }

    /// Compact lifetime tombstone that copies the abort reason when present.
    pub fn tombstone(
        &self,
        content_root: ContentRoot,
        decision_hash: [u8; 32],
    ) -> DecisionTombstone {
        DecisionTombstone {
            atomic_id: self.atomic_id,
            content_root,
            decision: self.decision,
            commit_position: self.commit_position,
            decision_hash,
            abort_reason: self.abort_reason,
        }
    }

    /// Reconstruct [`AtomicOutcome::NotCommitted`] from this decision.
    pub fn not_committed_outcome(&self) -> Result<AtomicOutcome, AtomicsError> {
        not_committed_outcome(self.atomic_id, self.decision, self.abort_reason)
    }
}

impl DecisionTombstone {
    /// Refuse committed/not-committed field combinations that cannot be decoded.
    pub fn validate(self) -> Result<(), AtomicsError> {
        validate_decision_axis(self.decision, self.commit_position, self.abort_reason)
    }

    /// Reconstruct [`AtomicOutcome::NotCommitted`] after detail removal.
    pub fn not_committed_outcome(&self) -> Result<AtomicOutcome, AtomicsError> {
        not_committed_outcome(self.atomic_id, self.decision, self.abort_reason)
    }
}

pub(crate) fn validate_decision_axis(
    decision: DecisionCode,
    commit_position: Option<u64>,
    abort_reason: Option<AtomicAbortReason>,
) -> Result<(), AtomicsError> {
    match decision {
        DecisionCode::Committed => {
            if matches!(commit_position, Some(0) | None) || abort_reason.is_some() {
                return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
            }
        }
        DecisionCode::NotCommitted => {
            if commit_position.is_some() || abort_reason.is_none() {
                return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
            }
        }
    }
    Ok(())
}

fn not_committed_outcome(
    atomic_id: AtomicId,
    decision: DecisionCode,
    abort_reason: Option<AtomicAbortReason>,
) -> Result<AtomicOutcome, AtomicsError> {
    match (decision, abort_reason) {
        (DecisionCode::NotCommitted, Some(reason)) => {
            Ok(AtomicOutcome::NotCommitted { atomic_id, reason })
        }
        _ => Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput)),
    }
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
    fn not_committed_decision_preserves_abort_reason() {
        let mut id = [0u8; 32];
        id[0] = 1;
        let atomic_id = AtomicId::from_bytes(id).unwrap();
        let d = AtomicDecision::not_committed(
            atomic_id,
            [2u8; 32],
            [3u8; 32],
            0,
            AtomicAbortReason::PreconditionConflict,
        );
        match d.not_committed_outcome().unwrap() {
            AtomicOutcome::NotCommitted { reason, .. } => {
                assert_eq!(reason, AtomicAbortReason::PreconditionConflict);
            }
            other => panic!("{other:?}"),
        }
        let root = ContentRoot::from_bytes([4u8; 32]).unwrap();
        let stone = d.tombstone(root, [5u8; 32]);
        match stone.not_committed_outcome().unwrap() {
            AtomicOutcome::NotCommitted { reason, .. } => {
                assert_eq!(reason, AtomicAbortReason::PreconditionConflict);
            }
            other => panic!("{other:?}"),
        }
        assert!(AtomicDecision::committed(atomic_id, [2u8; 32], [3u8; 32], 0, 0).is_err());
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
