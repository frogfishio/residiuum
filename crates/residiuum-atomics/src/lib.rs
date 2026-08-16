//! Residiuum Atomic protocol types (`ATOMICS_SPEC` §§4–13).
//!
//! Pure crate: no file, network, thread, store, or SDK dependency. ATM-0.1
//! freezes identity, scope, limits, vocabulary, outcomes, and formal lifecycle
//! states. ATM-0.2 adds deterministic CBOR, content-root hashing, and
//! canonical target order. ATM-0.7 freezes prepare/member/decision/tombstone
//! codecs, member object identity, and not-committed abort-reason preservation.
//! ATM-1.1 adds typed mutation/predicate encodings and closed-plan accounting.
//! ATM-1.2 is the pure closed-plan validator shared with the serial oracle.
//! ATM-1.3 is the Heap-bound typed builder: rights union, authority-revision
//! binding, deadline/limit checks, and read witnesses. No SDK dependency.
//! CR-ATM1-001 encodes Heap authority as `HeapAuthorityRevision` and admits
//! against a trusted grant view; active rule revisions stay RRE hashes.
//! CR-ATM1-002 binds a frozen key/value encoding profile to the trusted
//! collection handle and refuses noncanonical integer/decimal encodings.
//! ATM-2.2 is the per-Heap coordinator, placement manifest, and staged append
//! lane. Staged members never enter the ordinary primary map.
//! ATM-2.3 adds chunked-value members with a frozen chunk manifest and the
//! first member-stable boundary (`DurableInvisible`).
//! CR-ATM2-004 binds each staged append to the frozen member hash and payload.
//! ATM-2.4 is failpoints (`before_prepare`, `after_prepare`, `after_member_n`)
//! and prepared-visibility negatives. Crash-before-decision does not publish.
//! The serial oracle is ATM-0.5. ATM-0.4 is the hostile decoder corpus.

#![deny(missing_docs)]

pub mod builder;
pub mod canonical;
mod cbor;
pub mod encode;
pub mod encoding;
pub mod error;
pub mod evidence;
mod evidence_cbor;
pub mod failpoints;
pub mod id;
pub mod lifecycle;
pub mod limits;
pub mod oracle;
pub mod outcome;
pub mod plan;
pub mod staging;
pub mod validate;

pub use builder::{
    admit_closed_plan, AtomicBuilder, AtomicOptions, BoundCollection, CollectionRights,
    TrustedAuthorityView,
};
pub use canonical::{
    decode_canonical_plan, encode_canonical_plan, plan_content_root, CANONICAL_TARGET_ORDER,
    CBOR_V1_REL, DOMAIN_ATOMIC_CONTENT, DOMAIN_ATOMIC_DECISION, DOMAIN_ATOMIC_ID,
    DOMAIN_ATOMIC_MANIFEST, DOMAIN_ATOMIC_MEMBER, DOMAIN_ATOMIC_PREDICATES, DOMAIN_ATOMIC_PREPARE,
    DOMAIN_ATOMIC_READSET, DOMAIN_ATOMIC_TOMBSTONE,
};
pub use cbor::MAX_CBOR_DEPTH;
pub use encode::{
    account_closed_plan, encode_assert_absent, encode_assert_present, encode_assert_version,
    encode_create, encode_delete, encode_heap_authority_revision, encode_put, encode_replace,
    serialize_canonical_value, CanonicalValue, PlanAccounting,
};
pub use encoding::{
    encode_decimal_value, encode_signed_integer, validate_canonical_key, verify_decimal_value,
    verify_signed_integer, EncodingProfile, ValueEncoding,
};
pub use error::{AtomicsError, CborError};
pub use evidence::{
    AtomicDecision, AtomicLifecycle, AtomicMember, AtomicPrepare, DecisionCode, DecisionPhase,
    DecisionTombstone, Durability, MemberPhase, ObjectIdentity, PreparePhase, PublicationPhase,
};
pub use evidence_cbor::{
    decision_hash, decode_decision, decode_member, decode_prepare, decode_tombstone,
    encode_decision, encode_member, encode_prepare, encode_tombstone, member_hash,
    ordered_member_manifest_root, prepare_hash, tombstone_hash,
};
pub use failpoints::{FaultError, FaultSession, StagingFailpoint};
pub use id::{AtomicId, CollectionId, ContentRoot, HeapId, VersionId};
pub use lifecycle::{
    check_model, LifecycleError, LifecycleEvent, LifecycleTrace, ModelCheckReport, ModelProofs,
};
pub use limits::ResourceLimits;
pub use oracle::{OracleCell, OracleHistoryKind, OracleHistoryRecord, SerialOracle};
pub use outcome::{
    AtomicAbortReason, AtomicMemberReceipt, AtomicOutcome, AtomicReceipt, AtomicRefuseReason,
    AtomicResolutionHandle, AtomicStatus, LogicalStatus, MaterialStatus,
};
pub use plan::{
    AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CanonicalKeyKind, CoordinationScope,
    MutationKind, PlanMutation, PlanPredicate, PredicateKind, ReadWitness, UnknownProfile,
};
pub use staging::{
    ChunkPlan, CoordinatorRecord, CoordinatorSeq, CoordinatorStream, OrdinaryCell, PlacementEntry,
    PlacementManifest, ShardId, StagedMember, StagingHeap,
};
pub use validate::validate_closed_plan;

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
