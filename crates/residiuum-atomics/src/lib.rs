//! Residiuum Atomic protocol types (`ATOMICS_SPEC` §§4–13).
//!
//! Pure crate: no file, network, thread, store, or SDK dependency. ATM-0.1
//! freezes identity, scope, limits, vocabulary, outcomes, and formal lifecycle
//! states. Canonical CBOR and the serial oracle are later ATM-0 cards.

#![deny(missing_docs)]

pub mod canonical;
pub mod error;
pub mod evidence;
pub mod id;
pub mod limits;
pub mod oracle;
pub mod outcome;
pub mod plan;

pub use canonical::{
    CANONICAL_TARGET_ORDER, DOMAIN_ATOMIC_CONTENT, DOMAIN_ATOMIC_DECISION, DOMAIN_ATOMIC_ID,
    DOMAIN_ATOMIC_MANIFEST, DOMAIN_ATOMIC_MEMBER, DOMAIN_ATOMIC_PREDICATES, DOMAIN_ATOMIC_PREPARE,
    DOMAIN_ATOMIC_READSET,
};
pub use error::AtomicsError;
pub use evidence::{
    AtomicDecision, AtomicLifecycle, AtomicMember, AtomicPrepare, DecisionCode, DecisionPhase,
    DecisionTombstone, Durability, MemberPhase, PreparePhase, PublicationPhase,
};
pub use id::{AtomicId, CollectionId, ContentRoot, HeapId, VersionId};
pub use limits::ResourceLimits;
pub use oracle::SerialOracle;
pub use outcome::{
    AtomicAbortReason, AtomicMemberReceipt, AtomicOutcome, AtomicReceipt, AtomicRefuseReason,
    AtomicResolutionHandle, AtomicStatus, LogicalStatus, MaterialStatus,
};
pub use plan::{
    AtomicPlan, AtomicProfile, CanonicalKeyKind, CoordinationScope, MutationKind, PredicateKind,
    ReadWitness,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_has_no_store_or_sdk_in_manifest() {
        let manifest = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
        assert!(!manifest.contains("residiuum-store"));
        assert!(!manifest.contains("residiuum-sdk"));
        assert!(!manifest.contains("residiuum-server"));
        assert!(!manifest.contains("residiuum-cluster"));
        assert!(!manifest.contains("residiuum-format"));
        assert!(!manifest.contains("residiuum-heap"));
    }
}
