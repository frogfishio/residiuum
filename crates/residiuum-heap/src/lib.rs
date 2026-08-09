//! Residiuum heap identity, capability, and authority kernel (`residiuum-heap-v1`).
//!
//! Normative: `doc/wip/heap/HEAP_SPEC.md` §§30–32. Contract artifacts: `spec/heap/`.

#![deny(missing_docs)]

mod authority;
mod authority_model;
mod capability;
mod certificate;
mod constraints;
mod decide;
mod decide_obligations;
mod error;
mod holder_proof;
mod ids;
mod isolation;
mod isolation_model;
mod isolation_profile;
mod operational;
mod pure_proofs;
mod qualification;
mod rights;
mod security_time;
mod snapshot;
mod wire;

pub use authority::{AuthorityMutationKind, BlacklistEntry, BlacklistKind};
pub use authority_model::{
    connected_authority_model_smoke, AuthorityModel, ModelAdminState, ModelCert,
};
pub use capability::{HeapCap, HeapMaintenanceCap, RecoveryCap, ReplicaCap};
pub use certificate::{
    inspect_certificate, sig_structure_for, verify_certificate, VerifiedCertificate,
};
pub use constraints::{Constraint, Constraints, SourceNetwork};
pub use decide::{
    authority_admission_ok, authority_binding_holds, certificate_blacklisted, decide,
    generation_accepted, mint_capability, refresh_capability_or_terminate,
    AuthorizationDecision, OperationDescriptor,
};
pub use decide_obligations::obligations as h6_decide_obligations;
pub use pure_proofs::{
    connected_pure_proof_bundle, lemma_authority_model_inv_walk,
    lemma_binding_rejects_foreign_heap, lemma_blacklist_hits_certificate_hash,
    lemma_generation_grace_window, lemma_isolation_model_inv_walk,
    lemma_non_serving_refuses_admission,
};
pub use error::{HeapError, HeapUnavailableCause};
pub use holder_proof::{build_holder_proof, verify_holder_proof, VerifiedHolderProof};
pub use ids::{
    AuthorityEpoch, AuthorityGeneration, CapabilityId, CertificateId, CollectionId, DeploymentId,
    HeapId, SecurityRevision, StreamId,
};
pub use isolation::{
    confine_query_observation, ConfinedObservation, QueryObservationRequest,
    H6_PUBLISHED_LIMITATIONS,
};
pub use isolation_model::{connected_model_smoke, IsolationModel, ModelUnit};
pub use isolation_profile::{
    load_isolation_profiles, load_isolation_profiles_from, unauthenticated_field_allowed_for,
    IsolationProfile, IsolationProfileId, IsolationProfileRegistry, H6_MINIMUM_PROFILE,
    ISOLATION_PROFILES_JSON, ISOLATION_PROFILES_REL, REFERENCE_ISOLATION_PROFILE,
};
pub use operational::{
    confine_export_heaps, confine_health_detail, confine_operational_observation,
    confine_operational_observation_under, confine_support_bundle, confine_support_bundle_under,
    unauthenticated_field_allowed, unauthenticated_field_allowed_under, ConfinedHealthDetail,
    ConfinedOperationalObservation, ConfinedSupportBundle, HealthDetailInput, OperationalEvent,
    SupportBundleEntry, UNAUTHENTICATED_DECLASSIFIED_FIELDS,
};
pub use qualification::{
    claim_language, may_advertise_qualified, HP010_MATRIX_REL, PRE_QUALIFICATION_LANGUAGE,
    QUALIFIED_CLAIM, QUALIFIED_PROFILE,
};
pub use rights::{Operation, OperationStatus, Rights};
pub use security_time::{SecurityTimeFloor, TimeDecision, TrustedInstant};
pub use snapshot::{HeapAdministrativeState, HeapSecuritySnapshot, HeapSlot};
pub use wire::{
    AUDIENCE_DATA_V1, CERT_MAX_LIFETIME_S, CONTENT_TYPE_CERTIFICATE, CONTENT_TYPE_HOLDER_PROOF,
    EXTERNAL_AAD_CERTIFICATE, EXTERNAL_AAD_HOLDER_PROOF, PROFILE_VERSION,
};

#[allow(missing_docs)]
mod generated_registry {
    include!(concat!(env!("OUT_DIR"), "/generated_registry.rs"));
}

pub use generated_registry::{
    active_operation_ids, admitted_ops_for_state, allowed_states_for_op, generated_op,
    generated_rights, GeneratedOp, GENERATED_OPS,
};

/// Profile label for the frozen heap isolation contract.
pub const HEAP_PROFILE: &str = "residiuum-heap-v1";

/// Spec artifact lengths recorded at build time (drift detection).
pub mod artifacts {
    /// Byte length of `operations-v1.json` at build.
    pub const OPS_LEN: usize = parse_usize(env!("HEAP_OPS_LEN"));
    /// Byte length of `cbor-v1.json` at build.
    pub const CBOR_LEN: usize = parse_usize(env!("HEAP_CBOR_LEN"));
    /// Byte length of `vectors-v1.json` at build.
    pub const VECTORS_LEN: usize = parse_usize(env!("HEAP_VECTORS_LEN"));

    const fn parse_usize(s: &str) -> usize {
        let bytes = s.as_bytes();
        let mut n = 0usize;
        let mut i = 0;
        while i < bytes.len() {
            n = n * 10 + (bytes[i] - b'0') as usize;
            i += 1;
        }
        n
    }
}
