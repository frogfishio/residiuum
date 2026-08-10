//! Residiuum RQL-Q4 cross-engine qualification harness (`residiuum-rql-qual`).
//!
//! **Q4.1:** architecture skeleton — lanes, fixtures, adapters, evidence.
//! **Q4.2:** dataset axes, generators, lifecycle, cell plans.
//! **Q4.3:** metrics collectors, shared-logical adapters, smoke runner +
//! evidence publication (Mongo/CBL execute residual; logical digests Ready).
//!
//! **Non-claims:** not Gate-1; not competitive; not package accept.

#![forbid(unsafe_code)]

pub mod canonicalize;
pub mod cell_plan;
pub mod cells;
pub mod concurrent;
pub mod dataset;
pub mod engine;
pub mod evidence;
pub mod fixture;
pub mod generator;
pub mod lane;
pub mod lifecycle;
pub mod metrics;
pub mod run;
pub mod shared_work;

#[cfg(feature = "residiuum-embedded")]
pub mod residiuum_embedded;

/// Profile stamp for harness artefacts (distinct from product QVM profiles).
pub const HARNESS_PROFILE: &str = "residiuum-rql-qual-harness-v1";

/// Evidence bundle schema id (machine contracts under `spec/rql/qualification/harness-v1/`).
pub const EVIDENCE_BUNDLE_SCHEMA: &str = "residiuum-rql-qual-evidence-bundle-v1";

/// Env fingerprint schema id.
pub const ENV_FINGERPRINT_SCHEMA: &str = "residiuum-rql-qual-env-fingerprint-v1";

#[cfg(test)]
mod smoke {
    use super::*;

    #[test]
    fn profile_stamps_stable() {
        assert!(HARNESS_PROFILE.contains("rql-qual"));
        assert!(EVIDENCE_BUNDLE_SCHEMA.contains("evidence-bundle"));
        assert!(ENV_FINGERPRINT_SCHEMA.contains("env-fingerprint"));
    }
}
