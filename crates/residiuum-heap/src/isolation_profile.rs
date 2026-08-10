//! Named isolation profiles and closed declassification registry (`HEAP_SPEC` §13).
//!
//! Machine-readable source: `spec/heap/isolation-profiles-v1.json`.
//! Gate H3/H6 evidence loads this registry; runtime cannot add fields without a
//! newly qualified artifact. Does **not** flip [`crate::may_advertise_qualified`].

use crate::error::{HeapError, HeapUnavailableCause};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

/// Relative path from the workspace root (monorepo layout).
///
/// Published crates also ship a copy under `crates/residiuum-heap/spec/`.
pub const ISOLATION_PROFILES_REL: &str = "spec/heap/isolation-profiles-v1.json";

/// Embedded registry bytes (crate-local `spec/` so crates.io packages verify).
pub const ISOLATION_PROFILES_JSON: &str = include_str!("../spec/isolation-profiles-v1.json");

/// Named isolation profile id (`HEAP_SPEC` §13.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IsolationProfileId {
    /// Logical data isolation (H6 minimum).
    HeapDataIsolated,
    /// Data isolation + closed metadata declassification.
    HeapMetadataHardened,
    /// Metadata-hardened + resource budgets (not HP-010 qualified).
    HeapResourceIsolated,
    /// Separate physical boundaries (not HP-010 qualified).
    HeapPhysicalIsolated,
}

impl IsolationProfileId {
    /// Wire / JSON id string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HeapDataIsolated => "heap-data-isolated",
            Self::HeapMetadataHardened => "heap-metadata-hardened",
            Self::HeapResourceIsolated => "heap-resource-isolated",
            Self::HeapPhysicalIsolated => "heap-physical-isolated",
        }
    }

    /// Parse a wire id.
    pub fn parse(s: &str) -> Result<Self, HeapError> {
        match s {
            "heap-data-isolated" => Ok(Self::HeapDataIsolated),
            "heap-metadata-hardened" => Ok(Self::HeapMetadataHardened),
            "heap-resource-isolated" => Ok(Self::HeapResourceIsolated),
            "heap-physical-isolated" => Ok(Self::HeapPhysicalIsolated),
            _ => Err(HeapError::InvalidArgument("unknown isolation profile")),
        }
    }

    /// Whether this profile meets the Gate H6 minimum.
    #[must_use]
    pub fn meets_h6_minimum(self) -> bool {
        matches!(
            self,
            Self::HeapDataIsolated
                | Self::HeapMetadataHardened
                | Self::HeapResourceIsolated
                | Self::HeapPhysicalIsolated
        )
    }
}

/// Reference single-node profile for HP-010 evidence (data isolation).
pub const REFERENCE_ISOLATION_PROFILE: IsolationProfileId = IsolationProfileId::HeapDataIsolated;

/// H6 minimum profile id string.
pub const H6_MINIMUM_PROFILE: &str = "heap-data-isolated";

#[derive(Debug, Clone, Deserialize)]
struct ProfilesDoc {
    format: String,
    h6_minimum_profile: String,
    reference_profile: String,
    deployment_extension: DeploymentExtension,
    profiles: Vec<ProfileEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct DeploymentExtension {
    version: u32,
    fields: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProfileEntry {
    id: String,
    #[serde(default)]
    unauthenticated_fields: Vec<String>,
    #[serde(default)]
    authenticated_heap_local_fields: Vec<String>,
    #[serde(default)]
    always_confidential: Vec<String>,
    #[serde(default)]
    coarsen_public_timing: bool,
    #[serde(default)]
    expose_aggregate_load: bool,
    #[serde(default)]
    extends: Option<String>,
}

/// Parsed, closed declassification view for one named profile.
#[derive(Debug, Clone)]
pub struct IsolationProfile {
    /// Profile id.
    pub id: IsolationProfileId,
    /// Unauthenticated allowlist.
    pub unauthenticated_fields: Vec<String>,
    /// Authenticated heap-local allowlist.
    pub authenticated_heap_local_fields: Vec<String>,
    /// Always confidential field names (examples + hardened extras).
    pub always_confidential: Vec<String>,
    /// Whether public timing must be coarsened.
    pub coarsen_public_timing: bool,
    /// Whether aggregate load may be exposed.
    pub expose_aggregate_load: bool,
}

/// Loaded registry with digest for qualification evidence.
#[derive(Debug, Clone)]
pub struct IsolationProfileRegistry {
    /// SHA-256 of the exact JSON bytes (lowercase hex).
    pub sha256_hex: String,
    /// Deployment extension version (0 = empty base).
    pub extension_version: u32,
    /// Extension field names (empty for base).
    pub extension_fields: Vec<String>,
    /// H6 minimum profile id string.
    pub h6_minimum_profile: String,
    /// Reference profile id string.
    pub reference_profile: String,
    profiles: Vec<IsolationProfile>,
}

fn parse_doc(raw: &str) -> Result<(ProfilesDoc, String), HeapError> {
    let sha = {
        let mut h = Sha256::new();
        h.update(raw.as_bytes());
        format!("{:x}", h.finalize())
    };
    let doc: ProfilesDoc = serde_json::from_str(raw)
        .map_err(|_| HeapError::InvalidArgument("isolation-profiles-v1.json parse failed"))?;
    if doc.format != "residiuum-heap-isolation-profiles-v1" {
        return Err(HeapError::InvalidArgument(
            "unexpected isolation profiles format",
        ));
    }
    Ok((doc, sha))
}

fn resolve_entry(doc: &ProfilesDoc, id: &str) -> Result<IsolationProfile, HeapError> {
    let entry = doc
        .profiles
        .iter()
        .find(|p| p.id == id)
        .ok_or_else(|| HeapError::InvalidArgument("isolation profile missing"))?;

    let mut unauthenticated = entry.unauthenticated_fields.clone();
    let mut authenticated = entry.authenticated_heap_local_fields.clone();
    let mut confidential = entry.always_confidential.clone();
    let mut coarsen = entry.coarsen_public_timing;
    let mut expose_load = entry.expose_aggregate_load;

    if let Some(parent_id) = &entry.extends {
        let parent = resolve_entry(doc, parent_id)?;
        if unauthenticated.is_empty() {
            unauthenticated = parent.unauthenticated_fields;
        }
        if authenticated.is_empty() {
            authenticated = parent.authenticated_heap_local_fields;
        }
        for c in parent.always_confidential {
            if !confidential.iter().any(|x| x == &c) {
                confidential.push(c);
            }
        }
        // Child may only tighten (never loosen) timing/load disclosure.
        coarsen = coarsen || parent.coarsen_public_timing;
        expose_load = expose_load && parent.expose_aggregate_load;
    }

    Ok(IsolationProfile {
        id: IsolationProfileId::parse(id)?,
        unauthenticated_fields: unauthenticated,
        authenticated_heap_local_fields: authenticated,
        always_confidential: confidential,
        coarsen_public_timing: coarsen,
        expose_aggregate_load: expose_load,
    })
}

/// Load the embedded isolation profile registry.
pub fn load_isolation_profiles() -> Result<&'static IsolationProfileRegistry, HeapError> {
    static REG: OnceLock<Result<IsolationProfileRegistry, String>> = OnceLock::new();
    let stored = REG.get_or_init(|| {
        load_isolation_profiles_from(ISOLATION_PROFILES_JSON).map_err(|e| e.to_string())
    });
    match stored {
        Ok(r) => Ok(r),
        Err(_) => Err(HeapError::InvalidArgument(
            "isolation profile registry failed to load",
        )),
    }
}

/// Parse registry JSON (tests / tools).
pub fn load_isolation_profiles_from(raw: &str) -> Result<IsolationProfileRegistry, HeapError> {
    let (doc, sha) = parse_doc(raw)?;
    if doc.h6_minimum_profile != H6_MINIMUM_PROFILE {
        return Err(HeapError::InvalidArgument("h6_minimum_profile drift"));
    }
    let mut profiles = Vec::new();
    for p in &doc.profiles {
        profiles.push(resolve_entry(&doc, &p.id)?);
    }
    Ok(IsolationProfileRegistry {
        sha256_hex: sha,
        extension_version: doc.deployment_extension.version,
        extension_fields: doc.deployment_extension.fields.clone(),
        h6_minimum_profile: doc.h6_minimum_profile,
        reference_profile: doc.reference_profile,
        profiles,
    })
}

impl IsolationProfileRegistry {
    /// Look up a profile by id.
    pub fn get(&self, id: IsolationProfileId) -> Result<&IsolationProfile, HeapError> {
        self.profiles
            .iter()
            .find(|p| p.id == id)
            .ok_or_else(|| HeapError::InvalidArgument("isolation profile not in registry"))
    }

    /// Reference profile used by HP-010 single-node evidence.
    pub fn reference(&self) -> Result<&IsolationProfile, HeapError> {
        let id = IsolationProfileId::parse(&self.reference_profile)?;
        self.get(id)
    }
}

impl IsolationProfile {
    /// Whether an unauthenticated caller may observe `field`.
    #[must_use]
    pub fn unauthenticated_allows(&self, field: &str) -> bool {
        if self.always_confidential.iter().any(|c| c == field) {
            return false;
        }
        if field == "aggregate_load" && !self.expose_aggregate_load {
            return false;
        }
        if field == "fine_timing_ms" && self.coarsen_public_timing {
            return false;
        }
        self.unauthenticated_fields.iter().any(|f| f == field)
    }

    /// Whether a field is always confidential (must never appear cross-heap / public).
    #[must_use]
    pub fn is_always_confidential(&self, field: &str) -> bool {
        self.always_confidential.iter().any(|c| c == field)
            || (field == "aggregate_load" && !self.expose_aggregate_load)
            || (field == "fine_timing_ms" && self.coarsen_public_timing)
    }

    /// Authenticated heap-local observation allow check (bound heap only).
    pub fn authenticated_allows_heap_local(&self, field: &str) -> Result<(), HeapError> {
        if self.is_always_confidential(field) {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::ConstraintDenied,
            ));
        }
        if self
            .authenticated_heap_local_fields
            .iter()
            .any(|f| f == field)
            || self.unauthenticated_fields.iter().any(|f| f == field)
        {
            return Ok(());
        }
        Err(HeapError::unavailable(
            HeapUnavailableCause::ConstraintDenied,
        ))
    }
}

/// Profile-aware unauthenticated field check (defaults to reference profile).
#[must_use]
pub fn unauthenticated_field_allowed_for(profile: IsolationProfileId, field: &str) -> bool {
    load_isolation_profiles()
        .ok()
        .and_then(|r| r.get(profile).ok())
        .is_some_and(|p| p.unauthenticated_allows(field))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_loads_and_h6_minimum_is_data_isolated() {
        let reg = load_isolation_profiles().unwrap();
        assert_eq!(reg.h6_minimum_profile, "heap-data-isolated");
        assert_eq!(reg.reference_profile, "heap-data-isolated");
        assert_eq!(reg.extension_version, 0);
        assert!(reg.extension_fields.is_empty());
        assert_eq!(reg.sha256_hex.len(), 64);

        let data = reg.get(IsolationProfileId::HeapDataIsolated).unwrap();
        assert!(data.unauthenticated_allows("live"));
        assert!(!data.unauthenticated_allows("heap_count"));
        assert!(data.expose_aggregate_load);

        let hard = reg.get(IsolationProfileId::HeapMetadataHardened).unwrap();
        assert!(hard.coarsen_public_timing);
        assert!(!hard.expose_aggregate_load);
        assert!(hard.is_always_confidential("aggregate_load"));
        assert!(hard.is_always_confidential("fine_timing_ms"));
        assert!(hard.authenticated_allows_heap_local("usage").is_ok());
        assert!(hard.authenticated_allows_heap_local("heap_count").is_err());
    }

    #[test]
    fn all_named_profiles_present() {
        let reg = load_isolation_profiles().unwrap();
        for id in [
            IsolationProfileId::HeapDataIsolated,
            IsolationProfileId::HeapMetadataHardened,
            IsolationProfileId::HeapResourceIsolated,
            IsolationProfileId::HeapPhysicalIsolated,
        ] {
            assert!(reg.get(id).is_ok(), "{}", id.as_str());
            assert!(id.meets_h6_minimum());
        }
    }
}
