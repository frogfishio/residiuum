//! Residiuum Atomic protocol types (`ATOMICS_SPEC` §§4–13).
//!
//! Pure crate: no file, network, thread, store, or SDK dependency. ATM-0.1
//! freezes identity, scope, limits, vocabulary, outcomes, and formal lifecycle
//! states. ATM-0.2 adds deterministic CBOR, content-root hashing, and
//! canonical target order. The serial oracle is ATM-0.5.
//! ATM-0.4 is the hostile decoder corpus.

#![deny(missing_docs)]

pub mod canonical;
mod cbor;
pub mod error;
pub mod evidence;
pub mod id;
pub mod limits;
pub mod oracle;
pub mod outcome;
pub mod plan;

pub use canonical::{
    decode_canonical_plan, encode_canonical_plan, plan_content_root, CANONICAL_TARGET_ORDER,
    CBOR_V1_REL, DOMAIN_ATOMIC_CONTENT, DOMAIN_ATOMIC_DECISION, DOMAIN_ATOMIC_ID,
    DOMAIN_ATOMIC_MANIFEST, DOMAIN_ATOMIC_MEMBER, DOMAIN_ATOMIC_PREDICATES, DOMAIN_ATOMIC_PREPARE,
    DOMAIN_ATOMIC_READSET,
};
pub use cbor::MAX_CBOR_DEPTH;
pub use error::{AtomicsError, CborError};
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
    AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CanonicalKeyKind, CoordinationScope,
    MutationKind, PlanMutation, PlanPredicate, PredicateKind, ReadWitness,
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
