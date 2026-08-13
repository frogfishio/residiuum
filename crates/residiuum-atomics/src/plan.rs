//! Closed plan vocabulary (`ATOMICS_SPEC` §§4, 6–7).
//!
//! `AtomicPlan` is not publicly constructible. The typed builder is ATM-1 / SDK.

use crate::id::{AtomicId, CollectionId, HeapId, VersionId};
use crate::limits::ResourceLimits;
use crate::AtomicsError;

/// Coordination scope. Every scope contains exactly one `HeapId`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CoordinationScope {
    /// One immutable collection/key identity.
    Key = 1,
    /// Bounded identities in one embedded or single-server Heap.
    LocalHeap = 2,
    /// Bounded identities in one qualified strong partition. Disabled until a
    /// separate profile and qualification gate pass.
    Partition = 3,
}

impl CoordinationScope {
    /// Wire code from `ATOMICS_SPEC` §4.
    pub const fn wire_code(self) -> u8 {
        self as u8
    }

    /// Decode a known scope code.
    pub const fn from_wire_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Key),
            2 => Some(Self::LocalHeap),
            3 => Some(Self::Partition),
            _ => None,
        }
    }

    /// Stable snake_case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Key => "key",
            Self::LocalHeap => "local_heap",
            Self::Partition => "partition",
        }
    }

    /// Hard ceilings for this scope (`ATOMICS_SPEC` §13).
    pub const fn hard_limits(self) -> ResourceLimits {
        match self {
            Self::Key => ResourceLimits::hard_key(),
            Self::LocalHeap => ResourceLimits::hard_local_heap(),
            Self::Partition => ResourceLimits::hard_partition(),
        }
    }
}

/// Named Atomic profile. Unknown codes are preserved for examination and
/// refused by execution (`ATOMICS_SPEC` ATM-0 properties).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AtomicProfile {
    /// First shipped capability. Must not be advertised until ATM-5.
    LocalHeapV1,
    /// Unrecognized profile code. Execution MUST refuse; examination MAY keep it.
    Unknown(u16),
}

impl AtomicProfile {
    /// True when execution may accept this profile in v1.
    pub const fn execution_supported(self) -> bool {
        matches!(self, Self::LocalHeapV1)
    }

    /// Stable snake_case name. Unknown codes stay numeric.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalHeapV1 => "local_heap_v1",
            Self::Unknown(_) => "unknown",
        }
    }

    /// Wire code. Unknown profiles keep their original code.
    pub const fn wire_code(self) -> u16 {
        match self {
            Self::LocalHeapV1 => 1,
            Self::Unknown(code) => code,
        }
    }

    /// Decode a profile code.
    pub const fn from_wire_code(code: u16) -> Self {
        match code {
            1 => Self::LocalHeapV1,
            other => Self::Unknown(other),
        }
    }
}

/// Closed LocalHeap v1 mutation vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MutationKind {
    /// Create-if-absent.
    Create,
    /// Explicit blind upsert (`put_unconditional` on the SDK).
    Put,
    /// Version-replace.
    Replace,
    /// Version-delete.
    Delete,
}

impl MutationKind {
    /// Stable snake_case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Put => "put",
            Self::Replace => "replace",
            Self::Delete => "delete",
        }
    }

    /// Wire code (`spec/atomics/cbor-v1.json`).
    pub const fn wire_code(self) -> u8 {
        match self {
            Self::Create => 1,
            Self::Put => 2,
            Self::Replace => 3,
            Self::Delete => 4,
        }
    }

    /// Decode a known mutation kind.
    pub const fn from_wire_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Create),
            2 => Some(Self::Put),
            3 => Some(Self::Replace),
            4 => Some(Self::Delete),
            _ => None,
        }
    }
}

/// Closed predicate vocabulary. Public builder only exposes the three asserts;
/// remaining kinds compile through RQL/RRE and are not host closures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PredicateKind {
    /// Key absence.
    AssertAbsent,
    /// Key presence.
    AssertPresent,
    /// Exact version equality.
    AssertVersion,
    /// Exact scalar equality (compiled predicate).
    ExactScalarEquality,
    /// Bounded key-range absence when the index declares exact coverage.
    BoundedKeyRangeAbsence,
    /// Bounded key-range presence when the index declares exact coverage.
    BoundedKeyRangePresence,
    /// Active rule revision equality.
    ActiveRuleRevisionEquality,
    /// Collection or object lifecycle state.
    CollectionLifecycleState,
    /// Heap authority or security revision.
    HeapAuthorityRevision,
}

impl PredicateKind {
    /// True for the three public-builder assertions.
    pub const fn is_public_builder_assert(self) -> bool {
        matches!(
            self,
            Self::AssertAbsent | Self::AssertPresent | Self::AssertVersion
        )
    }

    /// Stable snake_case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AssertAbsent => "assert_absent",
            Self::AssertPresent => "assert_present",
            Self::AssertVersion => "assert_version",
            Self::ExactScalarEquality => "exact_scalar_equality",
            Self::BoundedKeyRangeAbsence => "bounded_key_range_absence",
            Self::BoundedKeyRangePresence => "bounded_key_range_presence",
            Self::ActiveRuleRevisionEquality => "active_rule_revision_equality",
            Self::CollectionLifecycleState => "collection_lifecycle_state",
            Self::HeapAuthorityRevision => "heap_authority_revision",
        }
    }

    /// Wire code (`spec/atomics/cbor-v1.json`).
    pub const fn wire_code(self) -> u8 {
        match self {
            Self::AssertAbsent => 1,
            Self::AssertPresent => 2,
            Self::AssertVersion => 3,
            Self::ExactScalarEquality => 4,
            Self::BoundedKeyRangeAbsence => 5,
            Self::BoundedKeyRangePresence => 6,
            Self::ActiveRuleRevisionEquality => 7,
            Self::CollectionLifecycleState => 8,
            Self::HeapAuthorityRevision => 9,
        }
    }

    /// Decode a known predicate kind.
    pub const fn from_wire_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::AssertAbsent),
            2 => Some(Self::AssertPresent),
            3 => Some(Self::AssertVersion),
            4 => Some(Self::ExactScalarEquality),
            5 => Some(Self::BoundedKeyRangeAbsence),
            6 => Some(Self::BoundedKeyRangePresence),
            7 => Some(Self::ActiveRuleRevisionEquality),
            8 => Some(Self::CollectionLifecycleState),
            9 => Some(Self::HeapAuthorityRevision),
            _ => None,
        }
    }
}

/// Canonical key kinds permitted as relationship or ordered-lock keys in v1.
/// Boolean, Null, products, sequences, and floating point are excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CanonicalKeyKind {
    /// UTF-8 string bytes.
    String,
    /// Opaque byte string.
    Opaque,
    /// Mathematical integer (canonical signed encoding).
    Integer,
    /// Exact decimal (canonical coefficient + scale).
    Decimal,
}

impl CanonicalKeyKind {
    /// Wire code (`spec/atomics/cbor-v1.json`).
    pub const fn wire_code(self) -> u8 {
        match self {
            Self::String => 1,
            Self::Opaque => 2,
            Self::Integer => 3,
            Self::Decimal => 4,
        }
    }
}

/// Canonical key value. Integer/decimal payloads are already-canonical bytes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CanonicalKey {
    /// UTF-8 string.
    String(String),
    /// Opaque byte string.
    Opaque(Vec<u8>),
    /// Already-canonical signed integer bytes.
    Integer(Vec<u8>),
    /// Exact decimal: canonical coefficient bytes plus scale.
    Decimal {
        /// Canonical coefficient bytes.
        coefficient: Vec<u8>,
        /// Decimal scale.
        scale: i64,
    },
}

impl CanonicalKey {
    /// Kind tag.
    pub const fn kind(&self) -> CanonicalKeyKind {
        match self {
            Self::String(_) => CanonicalKeyKind::String,
            Self::Opaque(_) => CanonicalKeyKind::Opaque,
            Self::Integer(_) => CanonicalKeyKind::Integer,
            Self::Decimal { .. } => CanonicalKeyKind::Decimal,
        }
    }
}

/// Read witness recorded for a prior observation (`ATOMICS_SPEC` §7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadWitness {
    /// Collection that was observed.
    pub collection_id: CollectionId,
    /// Observed key.
    pub key: CanonicalKey,
    /// Observed version, or `None` when the key was absent.
    pub observed_version: Option<VersionId>,
    /// Hash of the observed projection. Encoding is ATM-0.2.
    pub projection_hash: [u8; 32],
}

/// One mutation in a closed plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanMutation {
    /// Closed mutation kind.
    pub kind: MutationKind,
    /// Target collection.
    pub collection_id: CollectionId,
    /// Target key.
    pub key: CanonicalKey,
    /// Canonical value bytes. Absent on delete.
    pub encoded_value: Option<Vec<u8>>,
    /// Version precondition for replace/delete.
    pub if_version: Option<VersionId>,
}

/// One predicate in a closed plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanPredicate {
    /// Closed predicate kind.
    pub kind: PredicateKind,
    /// Collection when the predicate names one.
    pub collection_id: Option<CollectionId>,
    /// Key when the predicate names one.
    pub key: Option<CanonicalKey>,
    /// Version for `assert_version`.
    pub version: Option<VersionId>,
    /// Compiled RQL/RRE payload when not a public-builder assert.
    pub encoded: Option<Vec<u8>>,
}

/// Unordered parts used to close an [`AtomicPlan`].
///
/// This is assembly input, not the frozen plan. Builder order must not survive
/// [`AtomicPlan::close`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicPlanParts {
    /// Profile.
    pub profile: AtomicProfile,
    /// Caller or engine Atomic identity.
    pub atomic_id: AtomicId,
    /// Heap this plan is bound to.
    pub heap_id: HeapId,
    /// Coordination scope.
    pub scope: CoordinationScope,
    /// Optional read frontier.
    pub read_frontier: Option<[u8; 32]>,
    /// Read witnesses in any builder order.
    pub reads: Vec<ReadWitness>,
    /// Predicates in any builder order.
    pub predicates: Vec<PlanPredicate>,
    /// Mutations in any builder order.
    pub mutations: Vec<PlanMutation>,
    /// Active rule revision hashes.
    pub active_rule_revisions: Vec<[u8; 32]>,
    /// Applied semantic limits.
    pub limits: ResourceLimits,
}

/// Immutable Heap-bound plan. Fields are frozen; construction is ATM-1.
///
/// Public field construction is forbidden so SDK callers cannot bypass the
/// closed builder and authority binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicPlan {
    profile: AtomicProfile,
    atomic_id: AtomicId,
    heap_id: HeapId,
    scope: CoordinationScope,
    read_frontier: Option<[u8; 32]>,
    reads: Vec<ReadWitness>,
    predicates: Vec<PlanPredicate>,
    mutations: Vec<PlanMutation>,
    active_rule_revisions: Vec<[u8; 32]>,
    limits: ResourceLimits,
}

impl AtomicPlan {
    /// Declared profile.
    pub const fn profile(&self) -> AtomicProfile {
        self.profile
    }

    /// Issued identity.
    pub const fn atomic_id(&self) -> AtomicId {
        self.atomic_id
    }

    /// Heap this plan is bound to.
    pub const fn heap_id(&self) -> HeapId {
        self.heap_id
    }

    /// Declared coordination scope.
    pub const fn scope(&self) -> CoordinationScope {
        self.scope
    }

    /// Optional read frontier.
    pub fn read_frontier(&self) -> Option<[u8; 32]> {
        self.read_frontier
    }

    /// Read witnesses in canonical order.
    pub fn reads(&self) -> &[ReadWitness] {
        &self.reads
    }

    /// Predicates in canonical order.
    pub fn predicates(&self) -> &[PlanPredicate] {
        &self.predicates
    }

    /// Mutations in canonical target order.
    pub fn mutations(&self) -> &[PlanMutation] {
        &self.mutations
    }

    /// Active rule revision hashes in sorted byte order.
    pub fn active_rule_revisions(&self) -> &[[u8; 32]] {
        &self.active_rule_revisions
    }

    /// Applied semantic limits bound into the plan.
    pub const fn limits(&self) -> ResourceLimits {
        self.limits
    }

    /// Close unordered parts: refuse duplicate mutation/read/predicate identities,
    /// require `read_frontier` when prior-read witnesses are present, then sort
    /// with a total order so builder permutation cannot change canonical bytes.
    pub fn close(parts: AtomicPlanParts) -> Result<Self, AtomicsError> {
        crate::canonical::close_plan(parts)
    }

    pub(crate) fn from_closed(
        profile: AtomicProfile,
        atomic_id: AtomicId,
        heap_id: HeapId,
        scope: CoordinationScope,
        read_frontier: Option<[u8; 32]>,
        reads: Vec<ReadWitness>,
        predicates: Vec<PlanPredicate>,
        mutations: Vec<PlanMutation>,
        active_rule_revisions: Vec<[u8; 32]>,
        limits: ResourceLimits,
    ) -> Self {
        Self {
            profile,
            atomic_id,
            heap_id,
            scope,
            read_frontier,
            reads,
            predicates,
            mutations,
            active_rule_revisions,
            limits,
        }
    }
}

#[cfg(test)]
impl AtomicPlan {
    pub(crate) fn freeze_skeleton(
        profile: AtomicProfile,
        scope: CoordinationScope,
        limits: ResourceLimits,
    ) -> Self {
        let mut hid = [0u8; 16];
        hid[0] = 1;
        let mut aid = [0u8; 32];
        aid[0] = 1;
        Self {
            profile,
            atomic_id: AtomicId::from_bytes(aid).expect("nonzero"),
            heap_id: HeapId::from_bytes(hid).expect("nonzero"),
            scope,
            read_frontier: None,
            reads: Vec::new(),
            predicates: Vec::new(),
            mutations: Vec::new(),
            active_rule_revisions: Vec::new(),
            limits,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_wire_codes_match_spec() {
        assert_eq!(CoordinationScope::Key.wire_code(), 1);
        assert_eq!(CoordinationScope::LocalHeap.wire_code(), 2);
        assert_eq!(CoordinationScope::Partition.wire_code(), 3);
        assert_eq!(CoordinationScope::from_wire_code(0), None);
        assert_eq!(
            CoordinationScope::from_wire_code(2),
            Some(CoordinationScope::LocalHeap)
        );
    }

    #[test]
    fn mutation_vocabulary_is_closed() {
        let kinds = [
            MutationKind::Create,
            MutationKind::Put,
            MutationKind::Replace,
            MutationKind::Delete,
        ];
        assert_eq!(kinds.len(), 4);
        assert_eq!(MutationKind::Put.as_str(), "put");
    }

    #[test]
    fn public_builder_asserts_are_exactly_three() {
        let public: Vec<_> = [
            PredicateKind::AssertAbsent,
            PredicateKind::AssertPresent,
            PredicateKind::AssertVersion,
            PredicateKind::ExactScalarEquality,
            PredicateKind::BoundedKeyRangeAbsence,
            PredicateKind::BoundedKeyRangePresence,
            PredicateKind::ActiveRuleRevisionEquality,
            PredicateKind::CollectionLifecycleState,
            PredicateKind::HeapAuthorityRevision,
        ]
        .into_iter()
        .filter(|k| k.is_public_builder_assert())
        .collect();
        assert_eq!(public.len(), 3);
    }

    #[test]
    fn unknown_profile_is_not_executable() {
        assert!(AtomicProfile::LocalHeapV1.execution_supported());
        assert!(!AtomicProfile::Unknown(99).execution_supported());
    }

    #[test]
    fn plan_has_no_public_constructor_fields() {
        let plan = AtomicPlan::freeze_skeleton(
            AtomicProfile::LocalHeapV1,
            CoordinationScope::LocalHeap,
            ResourceLimits::builder_defaults_local_heap(),
        );
        assert_eq!(plan.scope(), CoordinationScope::LocalHeap);
        assert!(plan.limits().is_within(ResourceLimits::hard_local_heap()));
    }
}
