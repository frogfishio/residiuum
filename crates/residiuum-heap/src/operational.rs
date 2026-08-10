//! Operational-surface confinement for Gate H3 (§9.5 / §13.2 / §26.4).
//!
//! Metrics, logs, audit, and export observations are part of `Obs` and cannot
//! escape non-interference by being called diagnostics. This module encodes the
//! closed base declassification registry and heap-scoped filtering helpers.

use crate::capability::HeapCap;
use crate::decide::refresh_capability_or_terminate;
use crate::error::{HeapError, HeapUnavailableCause};
use crate::ids::HeapId;
use crate::isolation_profile::{
    load_isolation_profiles, IsolationProfileId, REFERENCE_ISOLATION_PROFILE,
};

/// Closed unauthenticated declassification registry (`HEAP_SPEC` §13.2).
///
/// Kept as a const alias of the reference profile allowlist; HP-010 verifies it
/// matches `spec/heap/isolation-profiles-v1.json`.
pub const UNAUTHENTICATED_DECLASSIFIED_FIELDS: &[&str] =
    &["protocol_versions", "live", "ready", "build_id"];

/// Whether an unauthenticated caller may observe `field` under the reference profile.
#[must_use]
pub fn unauthenticated_field_allowed(field: &str) -> bool {
    unauthenticated_field_allowed_under(REFERENCE_ISOLATION_PROFILE, field)
}

/// Whether an unauthenticated caller may observe `field` under a named profile.
///
/// `heap-metadata-hardened` additionally denies `aggregate_load` and
/// `fine_timing_ms` even when a looser profile would allow them.
#[must_use]
pub fn unauthenticated_field_allowed_under(profile: IsolationProfileId, field: &str) -> bool {
    load_isolation_profiles()
        .ok()
        .and_then(|r| r.get(profile).ok())
        .is_some_and(|p| p.unauthenticated_allows(field))
}

/// One operational log/metric/audit event offered to a capability-bound observer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationalEvent {
    /// Heap the event pertains to, when heap-local. `None` = deployment-wide.
    pub heap_id: Option<HeapId>,
    /// Stable field / metric name (not a free-form label).
    pub field: String,
    /// Redacted value (never credentials or payloads).
    pub value: String,
}

/// Confined operational observation after filtering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfinedOperationalObservation {
    /// Bound heap of the observing capability.
    pub heap_id: HeapId,
    /// Events that survived confinement.
    pub events: Vec<OperationalEvent>,
}

/// Confine metrics/logs/audit events to the live capability (§9.5).
///
/// Uses the reference isolation profile (`heap-data-isolated`).
///
/// Rules:
/// - capability must refresh;
/// - unauthenticated-class fields may pass without a heap tag;
/// - heap-tagged events must equal the capability heap;
/// - foreign-heap events are dropped (not disclosed);
/// - fields outside the closed registry that claim to be deployment-wide
///   (`heap_id == None` and not in the base table) are denied;
/// - always-confidential fields on the bound heap are denied (not dropped).
pub fn confine_operational_observation(
    cap: &HeapCap,
    events: &[OperationalEvent],
) -> Result<ConfinedOperationalObservation, HeapError> {
    confine_operational_observation_under(cap, events, REFERENCE_ISOLATION_PROFILE)
}

/// Profile-aware operational confinement (`HEAP_SPEC` §13 / Gate H3).
///
/// Under `heap-metadata-hardened`, deployment-wide `aggregate_load` and
/// `fine_timing_ms` are refused, and heap-local confidential fields fail closed.
pub fn confine_operational_observation_under(
    cap: &HeapCap,
    events: &[OperationalEvent],
    profile: IsolationProfileId,
) -> Result<ConfinedOperationalObservation, HeapError> {
    refresh_capability_or_terminate(cap)?;
    let bound = cap.heap_id();
    let profile_view = load_isolation_profiles()?.get(profile)?;
    let mut out = Vec::with_capacity(events.len());
    for ev in events {
        match ev.heap_id {
            Some(h) if h == bound => {
                // Bound-heap events may carry local metrics, but never always-confidential names.
                if profile_view.is_always_confidential(&ev.field) {
                    return Err(HeapError::unavailable(
                        HeapUnavailableCause::ConstraintDenied,
                    ));
                }
                // Metadata-hardened (and tighter) profiles enforce the closed authenticated
                // heap-local allowlist; data-isolated remains looser for residual metrics.
                if profile != IsolationProfileId::HeapDataIsolated {
                    profile_view.authenticated_allows_heap_local(&ev.field)?;
                }
                out.push(ev.clone());
            }
            Some(_) => {
                // Foreign heap — drop silently (no existence leak via error shape).
            }
            None => {
                if unauthenticated_field_allowed_under(profile, &ev.field) {
                    out.push(ev.clone());
                } else {
                    return Err(HeapError::unavailable(
                        HeapUnavailableCause::ConstraintDenied,
                    ));
                }
            }
        }
    }
    Ok(ConfinedOperationalObservation {
        heap_id: bound,
        events: out,
    })
}

/// Confine an ordinary export/backup request to the capability heap (§9.6 / H3).
///
/// An ordinary `HeapCap` may only export its own heap. Requesting any other heap
/// (or an empty multi-heap bag that would imply deployment inventory) fails closed.
pub fn confine_export_heaps(cap: &HeapCap, requested: &[HeapId]) -> Result<Vec<HeapId>, HeapError> {
    refresh_capability_or_terminate(cap)?;
    let bound = cap.heap_id();
    if requested.is_empty() {
        return Ok(vec![bound]);
    }
    for h in requested {
        if *h != bound {
            return Err(HeapError::unavailable(HeapUnavailableCause::StaleAuthority));
        }
    }
    Ok(vec![bound])
}

/// Health / probe fields offered for confinement (§9.5 / §13.2).
///
/// Mirrors the server `HealthReport` surface without depending on `residiuum-server`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthDetailInput {
    /// Process liveness.
    pub live: bool,
    /// Process readiness.
    pub ready: bool,
    /// Human reasons when not ready (must already be secret-free).
    pub reasons: Vec<String>,
    /// Physical store path (confidential under §13.2).
    pub store_path: Option<String>,
    /// Global live object count (confidential — not heap-local).
    pub live_count: Option<usize>,
    /// Placement / node index (confidential topology).
    pub node_index: Option<u32>,
    /// Whether draining.
    pub draining: bool,
    /// Heap-local usage for the bound heap (permitted for authenticated observers).
    pub bound_heap_usage_bytes: Option<u64>,
    /// Foreign-heap usage sneak (must never survive confinement).
    pub foreign_heap_usage_bytes: Option<u64>,
}

/// Confined health observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfinedHealthDetail {
    /// Always present for public probes and authenticated observers.
    pub live: bool,
    /// Always present for public probes and authenticated observers.
    pub ready: bool,
    /// Present only for authenticated heap-bound detail.
    pub reasons: Vec<String>,
    /// Present only when authenticated; never a physical path.
    pub draining: Option<bool>,
    /// Heap-local usage for the bound heap only.
    pub bound_heap_usage_bytes: Option<u64>,
}

/// Confine a health report.
///
/// - `cap == None` → public probe: only `live` / `ready` (base registry).
/// - `cap == Some` → authenticated detail: adds draining + bound-heap usage;
///   strips physical paths, global live counts, node topology, and foreign usage.
pub fn confine_health_detail(
    cap: Option<&HeapCap>,
    input: &HealthDetailInput,
) -> Result<ConfinedHealthDetail, HeapError> {
    match cap {
        None => Ok(ConfinedHealthDetail {
            live: input.live,
            ready: input.ready,
            reasons: Vec::new(),
            draining: None,
            bound_heap_usage_bytes: None,
        }),
        Some(cap) => {
            refresh_capability_or_terminate(cap)?;
            // Foreign usage is dropped; physical/global fields never appear.
            let _ = (&input.store_path, &input.live_count, &input.node_index);
            let _ = &input.foreign_heap_usage_bytes;
            Ok(ConfinedHealthDetail {
                live: input.live,
                ready: input.ready,
                reasons: input.reasons.clone(),
                draining: Some(input.draining),
                bound_heap_usage_bytes: input.bound_heap_usage_bytes,
            })
        }
    }
}

/// One support-bundle artifact candidate (§9.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportBundleEntry {
    /// Owning heap when heap-local; `None` = deployment-wide artifact.
    pub heap_id: Option<HeapId>,
    /// Stable kind (`log`, `metrics`, `receipt`, `config`, …).
    pub kind: String,
    /// Path label (never absolute secret-bearing paths in confined output).
    pub label: String,
    /// Whether the entry contains credential / key material (must be refused).
    pub contains_secrets: bool,
}

/// Confined support-bundle membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfinedSupportBundle {
    /// Bound heap.
    pub heap_id: HeapId,
    /// Entries that survived confinement.
    pub entries: Vec<SupportBundleEntry>,
}

/// Confine a support bundle to the live capability (reference profile).
///
/// Secret-bearing entries are always denied. Foreign-heap entries are dropped.
/// Undeclared deployment-wide kinds (not in the base registry) are denied.
pub fn confine_support_bundle(
    cap: &HeapCap,
    entries: &[SupportBundleEntry],
) -> Result<ConfinedSupportBundle, HeapError> {
    confine_support_bundle_under(cap, entries, REFERENCE_ISOLATION_PROFILE)
}

/// Profile-aware support-bundle confinement (`HEAP_SPEC` §13 / Gate H3).
///
/// Under metadata-hardened (and tighter) profiles, heap-local entry kinds must
/// appear on the authenticated heap-local allowlist (or be unauthenticated base).
pub fn confine_support_bundle_under(
    cap: &HeapCap,
    entries: &[SupportBundleEntry],
    profile: IsolationProfileId,
) -> Result<ConfinedSupportBundle, HeapError> {
    refresh_capability_or_terminate(cap)?;
    let bound = cap.heap_id();
    let profile_view = load_isolation_profiles()?.get(profile)?;
    let mut out = Vec::with_capacity(entries.len());
    for e in entries {
        if e.contains_secrets {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::ConstraintDenied,
            ));
        }
        match e.heap_id {
            Some(h) if h == bound => {
                if profile_view.is_always_confidential(&e.kind) {
                    return Err(HeapError::unavailable(
                        HeapUnavailableCause::ConstraintDenied,
                    ));
                }
                if profile != IsolationProfileId::HeapDataIsolated {
                    // Treat kind as a field name against the closed authenticated allowlist.
                    profile_view.authenticated_allows_heap_local(&e.kind)?;
                }
                out.push(e.clone());
            }
            Some(_) => {}
            None => {
                // Deployment-wide bundle artifacts must be explicitly allowlisted.
                if unauthenticated_field_allowed_under(profile, &e.kind) {
                    out.push(e.clone());
                } else {
                    return Err(HeapError::unavailable(
                        HeapUnavailableCause::ConstraintDenied,
                    ));
                }
            }
        }
    }
    Ok(ConfinedSupportBundle {
        heap_id: bound,
        entries: out,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::certificate::VerifiedCertificate;
    use crate::constraints::Constraints;
    use crate::decide::mint_capability;
    use crate::ids::{
        AuthorityEpoch, AuthorityGeneration, CertificateId, DeploymentId, SecurityRevision,
    };
    use crate::isolation_profile::{load_isolation_profiles, IsolationProfileId};
    use crate::rights::Rights;
    use crate::security_time::TrustedInstant;
    use crate::snapshot::{HeapAdministrativeState, HeapSecuritySnapshot, HeapSlot};
    use std::sync::Arc;

    fn uuidish(seed: u8) -> [u8; 16] {
        let mut id = [seed; 16];
        id[6] = (id[6] & 0x0f) | 0x40;
        id[8] = (id[8] & 0x3f) | 0x80;
        id
    }

    fn mint() -> HeapCap {
        let deployment = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
        let heap = HeapId::from_bytes(uuidish(0xc0)).unwrap();
        let snap = HeapSecuritySnapshot {
            deployment_id: deployment,
            heap_id: heap,
            authority_epoch: AuthorityEpoch::new(1).unwrap(),
            authority_generation: AuthorityGeneration::new(1).unwrap(),
            previous_generation: None,
            grace_deadline_unix_s: None,
            master_public_key: [0xab; 32],
            previous_master_public_key: None,
            security_revision: SecurityRevision::new(1).unwrap(),
            authority_chain_head_hash: [0x11; 32],
            administrative_state: HeapAdministrativeState::Active,
            blacklist: vec![],
            policy_rights_ceiling: None,
        };
        let slot = Arc::new(HeapSlot::new(snap));
        let cert = VerifiedCertificate {
            cose_bytes: vec![0x01],
            fingerprint: [3u8; 32],
            deployment_id: deployment,
            heap_id: heap,
            authority_epoch: AuthorityEpoch::new(1).unwrap(),
            authority_generation: AuthorityGeneration::new(1).unwrap(),
            certificate_id: CertificateId::new_random().unwrap(),
            holder_public_key: [4u8; 32],
            rights: Rights::from_bits_certificate(0x5).unwrap(),
            constraints: Constraints::empty(),
            not_before: 1,
            expires_at: 4_000_000_000,
            issuer_master_key_id: [5u8; 32],
        };
        mint_capability(
            slot,
            &cert,
            TrustedInstant {
                unix_s: 1_700_000_000,
            },
        )
        .unwrap()
    }

    #[test]
    fn drops_foreign_heap_metrics_and_denies_undeclared_global() {
        let cap = mint();
        let foreign = HeapId::from_bytes(uuidish(0xc1)).unwrap();
        let events = vec![
            OperationalEvent {
                heap_id: None,
                field: "live".into(),
                value: "1".into(),
            },
            OperationalEvent {
                heap_id: Some(cap.heap_id()),
                field: "usage_bytes".into(),
                value: "42".into(),
            },
            OperationalEvent {
                heap_id: Some(foreign),
                field: "usage_bytes".into(),
                value: "99".into(),
            },
        ];
        let obs = confine_operational_observation(&cap, &events).unwrap();
        assert_eq!(obs.events.len(), 2);
        assert!(obs.events.iter().all(|e| e.heap_id != Some(foreign)));

        assert!(confine_operational_observation(
            &cap,
            &[OperationalEvent {
                heap_id: None,
                field: "heap_count".into(),
                value: "2".into(),
            }]
        )
        .is_err());
    }

    #[test]
    fn metadata_hardened_denies_aggregate_load_and_fine_timing() {
        let cap = mint();
        assert!(!unauthenticated_field_allowed_under(
            IsolationProfileId::HeapMetadataHardened,
            "aggregate_load"
        ));
        assert!(!unauthenticated_field_allowed_under(
            IsolationProfileId::HeapMetadataHardened,
            "fine_timing_ms"
        ));
        // Reference data-isolated still permits aggregate_load as leakage class.
        assert!(
            load_isolation_profiles()
                .unwrap()
                .get(IsolationProfileId::HeapDataIsolated)
                .unwrap()
                .expose_aggregate_load
        );

        assert!(confine_operational_observation_under(
            &cap,
            &[OperationalEvent {
                heap_id: None,
                field: "aggregate_load".into(),
                value: "0.9".into(),
            }],
            IsolationProfileId::HeapMetadataHardened,
        )
        .is_err());
        assert!(confine_operational_observation_under(
            &cap,
            &[OperationalEvent {
                heap_id: None,
                field: "fine_timing_ms".into(),
                value: "12".into(),
            }],
            IsolationProfileId::HeapMetadataHardened,
        )
        .is_err());
        // Base unauthenticated fields still pass under hardened.
        let ok = confine_operational_observation_under(
            &cap,
            &[OperationalEvent {
                heap_id: None,
                field: "ready".into(),
                value: "1".into(),
            }],
            IsolationProfileId::HeapMetadataHardened,
        )
        .unwrap();
        assert_eq!(ok.events.len(), 1);

        // Closed authenticated allowlist: undeclared bound-heap field fails closed.
        assert!(confine_operational_observation_under(
            &cap,
            &[OperationalEvent {
                heap_id: Some(cap.heap_id()),
                field: "usage_bytes".into(),
                value: "9".into(),
            }],
            IsolationProfileId::HeapMetadataHardened,
        )
        .is_err());
        // Declared heap-local field still passes.
        let local = confine_operational_observation_under(
            &cap,
            &[OperationalEvent {
                heap_id: Some(cap.heap_id()),
                field: "usage".into(),
                value: "9".into(),
            }],
            IsolationProfileId::HeapMetadataHardened,
        )
        .unwrap();
        assert_eq!(local.events.len(), 1);
    }

    #[test]
    fn export_confined_to_bound_heap() {
        let cap = mint();
        let foreign = HeapId::from_bytes(uuidish(0xc2)).unwrap();
        assert_eq!(
            confine_export_heaps(&cap, &[]).unwrap(),
            vec![cap.heap_id()]
        );
        assert_eq!(
            confine_export_heaps(&cap, &[cap.heap_id()]).unwrap(),
            vec![cap.heap_id()]
        );
        assert!(confine_export_heaps(&cap, &[foreign]).is_err());
        assert!(confine_export_heaps(&cap, &[cap.heap_id(), foreign]).is_err());
    }

    #[test]
    fn health_detail_and_support_bundle_confined() {
        let cap = mint();
        let foreign = HeapId::from_bytes(uuidish(0xc3)).unwrap();
        let input = HealthDetailInput {
            live: true,
            ready: true,
            reasons: vec!["ok".into()],
            store_path: Some("/var/lib/residiuum/secret".into()),
            live_count: Some(999),
            node_index: Some(3),
            draining: false,
            bound_heap_usage_bytes: Some(42),
            foreign_heap_usage_bytes: Some(777),
        };
        let public = confine_health_detail(None, &input).unwrap();
        assert!(public.live && public.ready);
        assert!(public.reasons.is_empty());
        assert!(public.draining.is_none());
        assert!(public.bound_heap_usage_bytes.is_none());

        let auth = confine_health_detail(Some(&cap), &input).unwrap();
        assert_eq!(auth.bound_heap_usage_bytes, Some(42));
        assert_eq!(auth.draining, Some(false));
        assert!(!auth.reasons.is_empty());

        let bundle = confine_support_bundle(
            &cap,
            &[
                SupportBundleEntry {
                    heap_id: Some(cap.heap_id()),
                    kind: "receipt".into(),
                    label: "purge".into(),
                    contains_secrets: false,
                },
                SupportBundleEntry {
                    heap_id: Some(foreign),
                    kind: "log".into(),
                    label: "other".into(),
                    contains_secrets: false,
                },
                SupportBundleEntry {
                    heap_id: None,
                    kind: "live".into(),
                    label: "probe".into(),
                    contains_secrets: false,
                },
            ],
        )
        .unwrap();
        assert_eq!(bundle.entries.len(), 2);
        assert!(bundle
            .entries
            .iter()
            .all(|e| e.heap_id.is_none() || e.heap_id == Some(cap.heap_id())));
        assert!(confine_support_bundle(
            &cap,
            &[SupportBundleEntry {
                heap_id: Some(cap.heap_id()),
                kind: "key".into(),
                label: "master".into(),
                contains_secrets: true,
            }]
        )
        .is_err());
        assert!(confine_support_bundle(
            &cap,
            &[SupportBundleEntry {
                heap_id: None,
                kind: "heap_inventory".into(),
                label: "all".into(),
                contains_secrets: false,
            }]
        )
        .is_err());

        // Metadata-hardened: undeclared bound-heap kind fails; allowlisted kind passes.
        assert!(confine_support_bundle_under(
            &cap,
            &[SupportBundleEntry {
                heap_id: Some(cap.heap_id()),
                kind: "not_in_registry".into(),
                label: "x".into(),
                contains_secrets: false,
            }],
            IsolationProfileId::HeapMetadataHardened,
        )
        .is_err());
        let hard = confine_support_bundle_under(
            &cap,
            &[SupportBundleEntry {
                heap_id: Some(cap.heap_id()),
                kind: "receipts".into(),
                label: "purge".into(),
                contains_secrets: false,
            }],
            IsolationProfileId::HeapMetadataHardened,
        )
        .unwrap();
        assert_eq!(hard.entries.len(), 1);
    }
}
