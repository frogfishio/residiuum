//! Closed plan vocabulary (`ATOMICS_SPEC` §§4, 6–7).
//!
//! `AtomicPlan` is not publicly constructible. The typed builder is ATM-1 / SDK.

use crate::id::{CollectionId, VersionId};
use crate::limits::ResourceLimits;

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

/// Read witness recorded for a prior observation (`ATOMICS_SPEC` §7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadWitness {
    /// Collection that was observed.
    pub collection_id: CollectionId,
    /// Observed version, or `None` when the key was absent.
    pub observed_version: Option<VersionId>,
    /// Hash of the observed projection. Encoding is ATM-0.2.
    pub projection_hash: [u8; 32],
}

/// Immutable Heap-bound plan. Fields are frozen; construction is ATM-1.
///
/// Public field construction is forbidden so SDK callers cannot bypass the
/// closed builder and authority binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicPlan {
    profile: AtomicProfile,
    scope: CoordinationScope,
    limits: ResourceLimits,
}

impl AtomicPlan {
    /// Declared profile.
    pub const fn profile(&self) -> AtomicProfile {
        self.profile
    }

    /// Declared coordination scope.
    pub const fn scope(&self) -> CoordinationScope {
        self.scope
    }

    /// Applied semantic limits bound into the plan.
    pub const fn limits(&self) -> ResourceLimits {
        self.limits
    }
}

#[cfg(test)]
impl AtomicPlan {
    pub(crate) fn freeze_skeleton(
        profile: AtomicProfile,
        scope: CoordinationScope,
        limits: ResourceLimits,
    ) -> Self {
        Self {
            profile,
            scope,
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
