//! Isolation-kernel observation helpers for Gate H3/H6 evidence (`HEAP_SPEC` §26.3–§26.4 / §27).
//!
//! These pure checks prove a deliberately faulty planner or derived-path request cannot
//! escape the capability's bound heap and critical constraints. They do **not** flip
//! [`crate::may_advertise_qualified`].

use crate::capability::HeapCap;
use crate::decide::refresh_capability_or_terminate;
use crate::error::{HeapError, HeapUnavailableCause};
use crate::ids::{CollectionId, HeapId, StreamId};

/// A planner-emitted observation request (may be malicious / unconstrained).
#[derive(Debug, Clone)]
pub struct QueryObservationRequest {
    /// Heap the planner claims to scan.
    pub requested_heap: HeapId,
    /// Collections the planner wants to touch (empty = unconstrained scan request).
    pub collections: Vec<CollectionId>,
    /// Streams the planner wants to combine.
    pub streams: Vec<StreamId>,
    /// Estimated query work units.
    pub estimated_work: u64,
    /// Estimated result bytes.
    pub estimated_result_bytes: u64,
}

/// Functional observation allowed after confinement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfinedObservation {
    /// Bound heap (always equals the capability heap).
    pub heap_id: HeapId,
    /// Collections that survived allowlist filtering (empty if none requested and none allowed).
    pub collections: Vec<CollectionId>,
    /// Streams that survived allowlist filtering.
    pub streams: Vec<StreamId>,
    /// Work budget remaining after the estimate (or the estimate itself when unbounded).
    pub work_budget: u64,
}

/// Confine a (possibly faulty) query plan to the live capability.
///
/// Fail-closed rules:
/// - capability must still refresh against its slot;
/// - requested heap must equal the capability heap (no cross-heap escape);
/// - every requested collection/stream must pass critical allowlists;
/// - unconstrained collection/stream scans under a non-empty allowlist are denied;
/// - work / result maxima from the certificate are enforced.
pub fn confine_query_observation(
    cap: &HeapCap,
    request: &QueryObservationRequest,
) -> Result<ConfinedObservation, HeapError> {
    refresh_capability_or_terminate(cap)?;

    if request.requested_heap != cap.heap_id() {
        return Err(HeapError::unavailable(HeapUnavailableCause::StaleAuthority));
    }

    let constraints = cap.constraints();

    // Unconstrained scan (empty lists) under a narrowing allowlist is an escape attempt.
    let has_collection_allowlist = constraints
        .as_slice()
        .iter()
        .any(|c| matches!(c, crate::constraints::Constraint::CollectionAllowlist(_)));
    let has_stream_allowlist = constraints
        .as_slice()
        .iter()
        .any(|c| matches!(c, crate::constraints::Constraint::StreamAllowlist(_)));

    if has_collection_allowlist && request.collections.is_empty() {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::ConstraintDenied,
        ));
    }
    if has_stream_allowlist && request.streams.is_empty() && request.collections.is_empty() {
        // Fully unconstrained under stream allowlist — deny.
        return Err(HeapError::unavailable(
            HeapUnavailableCause::ConstraintDenied,
        ));
    }

    let mut collections = Vec::with_capacity(request.collections.len());
    for c in &request.collections {
        if !constraints.allows_collection(*c) {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::ConstraintDenied,
            ));
        }
        collections.push(*c);
    }

    let mut streams = Vec::with_capacity(request.streams.len());
    for s in &request.streams {
        if !constraints.allows_stream(*s) {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::ConstraintDenied,
            ));
        }
        streams.push(*s);
    }

    if let Some(max) = constraints.max_query_work() {
        if request.estimated_work > max {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::ConstraintDenied,
            ));
        }
    }
    if let Some(max) = constraints.max_result_bytes() {
        if request.estimated_result_bytes > max {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::ConstraintDenied,
            ));
        }
    }

    let work_budget = constraints
        .max_query_work()
        .unwrap_or(request.estimated_work);

    Ok(ConfinedObservation {
        heap_id: cap.heap_id(),
        collections,
        streams,
        work_budget,
    })
}

/// Published H6 limitation language (administrators, physical access, side channels).
pub const H6_PUBLISHED_LIMITATIONS: &str = concat!(
    "Logical heap isolation does not protect against: privileged administrators with ",
    "local recovery authority; physical access to media or process memory; shared-resource ",
    "side channels (timing, cache, scheduler); or compromise of the serving process TCB. ",
    "See doc/wip/heap/RUNBOOK_HEAP_QUALIFICATION.md."
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::certificate::VerifiedCertificate;
    use crate::constraints::{Constraint, Constraints};
    use crate::decide::mint_capability;
    use crate::ids::{
        AuthorityEpoch, AuthorityGeneration, CertificateId, DeploymentId, SecurityRevision,
    };
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

    fn mint_with(constraints: Constraints) -> HeapCap {
        let deployment = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
        let heap = HeapId::from_bytes(uuidish(0xa0)).unwrap();
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
            constraints,
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
    fn faulty_unconstrained_planner_denied_under_allowlist() {
        let allowed = CollectionId::from_bytes(uuidish(0xb1)).unwrap();
        let constraints =
            Constraints::from_sorted(vec![Constraint::CollectionAllowlist(vec![allowed])]).unwrap();
        let cap = mint_with(constraints);
        let other_heap = HeapId::from_bytes(uuidish(0xa1)).unwrap();

        // Cross-heap request.
        assert!(confine_query_observation(
            &cap,
            &QueryObservationRequest {
                requested_heap: other_heap,
                collections: vec![allowed],
                streams: vec![],
                estimated_work: 1,
                estimated_result_bytes: 1,
            }
        )
        .is_err());

        // Unconstrained scan under allowlist.
        assert!(confine_query_observation(
            &cap,
            &QueryObservationRequest {
                requested_heap: cap.heap_id(),
                collections: vec![],
                streams: vec![],
                estimated_work: 1,
                estimated_result_bytes: 1,
            }
        )
        .is_err());

        // Wrong collection.
        let wrong = CollectionId::from_bytes(uuidish(0xb2)).unwrap();
        assert!(confine_query_observation(
            &cap,
            &QueryObservationRequest {
                requested_heap: cap.heap_id(),
                collections: vec![wrong],
                streams: vec![],
                estimated_work: 1,
                estimated_result_bytes: 1,
            }
        )
        .is_err());

        // Allowed collection succeeds.
        let ok = confine_query_observation(
            &cap,
            &QueryObservationRequest {
                requested_heap: cap.heap_id(),
                collections: vec![allowed],
                streams: vec![],
                estimated_work: 1,
                estimated_result_bytes: 1,
            },
        )
        .unwrap();
        assert_eq!(ok.heap_id, cap.heap_id());
        assert_eq!(ok.collections, vec![allowed]);
    }
}
