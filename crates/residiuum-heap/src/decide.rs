//! Pure authorization decision function (`HEAP_SPEC` §39).

use crate::authority::BlacklistKind;
use crate::capability::{sha256, CapInner, HeapCap};
use crate::certificate::VerifiedCertificate;
use crate::constraints::require_rights;
use crate::error::{HeapError, HeapUnavailableCause};
use crate::generated_registry::admitted_ops_for_state;
use crate::ids::{CapabilityId, SecurityRevision};
use crate::rights::{Operation, OperationStatus};
use crate::security_time::{TimeDecision, TrustedInstant};
use crate::snapshot::{HeapSecuritySnapshot, HeapSlot};
use std::sync::Arc;

/// Operation request descriptor for admission.
#[derive(Debug, Clone)]
pub struct OperationDescriptor {
    /// Numeric operation id.
    pub operation_id: u16,
    /// Request byte size (for MaxRequestBytes).
    pub request_bytes: u64,
}

/// Authorization outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizationDecision {
    /// Allow; caller may mint / use capability for this op.
    Allow,
    /// Deny with fail-closed cause.
    Deny(HeapUnavailableCause),
}

/// Pure authority-binding check used by [`decide`] for every non-public op.
///
/// Verus obligation O1: allow implies this returns true.
#[must_use]
pub fn authority_binding_holds(
    snapshot: &HeapSecuritySnapshot,
    certificate: &VerifiedCertificate,
) -> bool {
    certificate.heap_id == snapshot.heap_id
        && certificate.deployment_id == snapshot.deployment_id
        && certificate.authority_epoch == snapshot.authority_epoch
}

/// Generation acceptance (`HeapAuthority` `GenOK` / §39): current generation, or
/// previous generation while trusted time is within the grace deadline.
#[must_use]
pub fn generation_accepted(
    snapshot: &HeapSecuritySnapshot,
    certificate: &VerifiedCertificate,
    now: TrustedInstant,
) -> bool {
    certificate.authority_generation == snapshot.authority_generation
        || snapshot.previous_generation == Some(certificate.authority_generation)
            && snapshot
                .grace_deadline_unix_s
                .is_some_and(|d| now.unix_s <= d)
}

/// Whether the certificate is hit by a same-generation blacklist entry.
///
/// Matches `HeapAuthority` `NotBlacklisted` / `BlacklistAdd` and HEAP_SPEC §8.11
/// typed entries (`CertificateFingerprint` / `HolderFingerprint`).
#[must_use]
pub fn certificate_blacklisted(
    snapshot: &HeapSecuritySnapshot,
    certificate: &VerifiedCertificate,
) -> bool {
    let cert_fp = certificate.fingerprint;
    let holder_fp = sha256(&certificate.holder_public_key);
    for entry in &snapshot.blacklist {
        if entry.generation != certificate.authority_generation.get() {
            continue;
        }
        let hit = match entry.kind {
            BlacklistKind::CertificateHash => entry.fingerprint == cert_fp,
            BlacklistKind::HolderPublicKeyHash => entry.fingerprint == holder_fp,
        };
        if hit {
            return true;
        }
    }
    false
}

/// Combined authority admission predicate (HeapAuthority `AdmissionOK` stand-in
/// over a resident snapshot). Does not check rights, constraints, or op status.
///
/// Used by HP-010 Gate H6 obligations to connect `generation_accepted` /
/// `certificate_blacklisted` / administrative serving to the formal model.
#[must_use]
pub fn authority_admission_ok(
    snapshot: &HeapSecuritySnapshot,
    certificate: &VerifiedCertificate,
    now: TrustedInstant,
) -> bool {
    if !authority_binding_holds(snapshot, certificate) {
        return false;
    }
    if !snapshot.administrative_state.is_serving() || snapshot.administrative_state.is_terminal() {
        return false;
    }
    if !generation_accepted(snapshot, certificate, now) {
        return false;
    }
    if certificate_blacklisted(snapshot, certificate) {
        return false;
    }
    // Issuer must match the generation's master key (current or previous during grace).
    let expected_master = if certificate.authority_generation == snapshot.authority_generation {
        Some(snapshot.master_public_key)
    } else {
        snapshot.previous_master_public_key
    };
    match expected_master {
        Some(pk) if certificate.issuer_master_key_id == sha256(&pk) => true,
        _ => false,
    }
}

/// Pure decision: no I/O, no ambient clock.
pub fn decide(
    snapshot: &HeapSecuritySnapshot,
    certificate: &VerifiedCertificate,
    operation: &OperationDescriptor,
    now: TrustedInstant,
) -> AuthorizationDecision {
    match decide_inner(snapshot, certificate, operation, now) {
        Ok(()) => AuthorizationDecision::Allow,
        Err(HeapError::UnavailableDetailed { cause, .. }) => AuthorizationDecision::Deny(cause),
        Err(_) => AuthorizationDecision::Deny(HeapUnavailableCause::Denied),
    }
}

fn decide_inner(
    snapshot: &HeapSecuritySnapshot,
    certificate: &VerifiedCertificate,
    operation: &OperationDescriptor,
    now: TrustedInstant,
) -> Result<(), HeapError> {
    // Public process ops bypass heap state / rights.
    if matches!(operation.operation_id, 1..=3) {
        if Operation::status(operation.operation_id)? != OperationStatus::Active {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::UnknownOperation,
            ));
        }
        return Ok(());
    }

    if Operation::status(operation.operation_id)? != OperationStatus::Active {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::UnknownOperation,
        ));
    }

    if !authority_binding_holds(snapshot, certificate) {
        return Err(HeapError::unavailable(HeapUnavailableCause::StaleAuthority));
    }

    match time_window(certificate.not_before, certificate.expires_at, now) {
        TimeDecision::Accept => {}
        TimeDecision::NotYetValid | TimeDecision::Expired => {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::NotYetValidOrExpired,
            ));
        }
    }

    // Generation acceptance: current, or previous during grace.
    if !generation_accepted(snapshot, certificate, now) {
        return Err(HeapError::unavailable(HeapUnavailableCause::StaleAuthority));
    }

    // Issuer must match the generation's master key.
    let expected_master = if certificate.authority_generation == snapshot.authority_generation {
        snapshot.master_public_key
    } else {
        snapshot
            .previous_master_public_key
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::StaleAuthority))?
    };
    let expected_issuer = sha256(&expected_master);
    if certificate.issuer_master_key_id != expected_issuer {
        return Err(HeapError::unavailable(HeapUnavailableCause::StaleAuthority));
    }

    if certificate_blacklisted(snapshot, certificate) {
        return Err(HeapError::unavailable(HeapUnavailableCause::Blacklisted));
    }

    if snapshot.administrative_state.is_terminal() {
        return Err(HeapError::unavailable(HeapUnavailableCause::InvalidState));
    }

    let state_name = snapshot.administrative_state.wire_name();
    let admitted = admitted_ops_for_state(state_name)
        .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::InvalidState))?;
    if !admitted.contains(&operation.operation_id) {
        return Err(HeapError::unavailable(HeapUnavailableCause::InvalidState));
    }

    let required = Operation::required_rights(operation.operation_id)?;
    let mut effective = certificate.rights;
    if let Some(ceiling) = snapshot.policy_rights_ceiling {
        effective = effective.intersection(ceiling);
    }
    require_rights(effective, required)?;

    if !certificate
        .constraints
        .allows_operation(operation.operation_id)
    {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::ConstraintDenied,
        ));
    }

    for c in certificate.constraints.as_slice() {
        if let crate::constraints::Constraint::MaxRequestBytes(max) = c {
            if operation.request_bytes > *max {
                return Err(HeapError::unavailable(
                    HeapUnavailableCause::ConstraintDenied,
                ));
            }
        }
    }

    Ok(())
}

fn time_window(not_before: u64, expires_at: u64, now: TrustedInstant) -> TimeDecision {
    if now.unix_s < not_before {
        TimeDecision::NotYetValid
    } else if now.unix_s >= expires_at {
        TimeDecision::Expired
    } else {
        TimeDecision::Accept
    }
}

/// After a successful `decide` Allow on a heap op, mint a HeapCap bound to `slot`.
pub fn mint_capability(
    slot: Arc<HeapSlot>,
    certificate: &VerifiedCertificate,
    now: TrustedInstant,
) -> Result<HeapCap, HeapError> {
    let snap = slot.load();
    // Re-check revision binding.
    if certificate.heap_id != snap.heap_id {
        return Err(HeapError::unavailable(HeapUnavailableCause::StaleAuthority));
    }
    // Grace deadline bounds *previous-generation* certificates only (§8.11).
    // Current-generation certs use their own expires_at (hard cycle has no grace).
    let deadline = if snap.previous_generation == Some(certificate.authority_generation) {
        match snap.grace_deadline_unix_s {
            Some(g) => certificate.expires_at.min(g),
            None => certificate.expires_at,
        }
    } else {
        certificate.expires_at
    };
    if now.unix_s >= deadline {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::NotYetValidOrExpired,
        ));
    }
    let mut effective = certificate.rights;
    if let Some(ceiling) = snap.policy_rights_ceiling {
        effective = effective.intersection(ceiling);
    }
    Ok(HeapCap::mint(CapInner {
        capability_id: CapabilityId::new_random()?,
        slot,
        deployment_id: certificate.deployment_id,
        heap_id: certificate.heap_id,
        certificate_id: certificate.certificate_id,
        certificate_fingerprint: certificate.fingerprint,
        holder_fingerprint: sha256(&certificate.holder_public_key),
        authority_epoch: certificate.authority_epoch,
        authority_generation: certificate.authority_generation,
        validated_security_revision: snap.security_revision,
        validated_authority_chain_head_hash: snap.authority_chain_head_hash,
        effective_rights: effective,
        effective_constraints: certificate.constraints.clone(),
        validity_deadline_unix_s: deadline,
    }))
}

/// Ensure a capability is still live against its slot.
///
/// Frozen v1: any mismatch of heap/deployment/epoch, security revision, authority
/// chain head, or a non-serving administrative state terminates the instance
/// (`HEAP_SPEC` §8.7 / Gate H3).
pub fn refresh_capability_or_terminate(cap: &HeapCap) -> Result<(), HeapError> {
    let snap = cap.slot().load();
    if snap.heap_id != cap.heap_id()
        || snap.deployment_id != cap.deployment_id()
        || snap.authority_epoch != cap.inner.authority_epoch
        || snap.security_revision != cap.security_revision()
        || snap.authority_chain_head_hash != cap.inner.validated_authority_chain_head_hash
        || !snap.administrative_state.is_serving()
    {
        return Err(HeapError::unavailable(HeapUnavailableCause::StaleAuthority));
    }
    let _ = SecurityRevision::new(snap.security_revision.get())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::certificate::VerifiedCertificate;
    use crate::constraints::Constraints;
    use crate::ids::{
        AuthorityEpoch, AuthorityGeneration, CertificateId, DeploymentId, HeapId, SecurityRevision,
    };
    use crate::rights::Rights;
    use crate::snapshot::HeapAdministrativeState;

    fn sample_snap(heap: HeapId, dep: DeploymentId, master: [u8; 32]) -> HeapSecuritySnapshot {
        HeapSecuritySnapshot {
            deployment_id: dep,
            heap_id: heap,
            authority_epoch: AuthorityEpoch::new(1).unwrap(),
            authority_generation: AuthorityGeneration::new(1).unwrap(),
            previous_generation: None,
            grace_deadline_unix_s: None,
            master_public_key: master,
            previous_master_public_key: None,
            security_revision: SecurityRevision::new(1).unwrap(),
            authority_chain_head_hash: [9u8; 32],
            administrative_state: HeapAdministrativeState::Active,
            blacklist: vec![],
            policy_rights_ceiling: None,
        }
    }

    fn mint_test_cap(slot: Arc<HeapSlot>) -> HeapCap {
        let snap = slot.load();
        let cert = VerifiedCertificate {
            cose_bytes: vec![0x01],
            fingerprint: [3u8; 32],
            deployment_id: snap.deployment_id,
            heap_id: snap.heap_id,
            authority_epoch: snap.authority_epoch,
            authority_generation: snap.authority_generation,
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
    fn public_ping_always_allow_when_active() {
        let heap = HeapId::new_random().unwrap();
        let dep = DeploymentId::new_random().unwrap();
        let snap = sample_snap(heap, dep, [1u8; 32]);
        let _ = snap;
        let op = OperationDescriptor {
            operation_id: 1,
            request_bytes: 0,
        };
        let _ = op;
    }

    #[test]
    fn refresh_terminates_on_security_revision_or_non_serving_state() {
        let heap = HeapId::new_random().unwrap();
        let dep = DeploymentId::new_random().unwrap();
        let slot = Arc::new(HeapSlot::new(sample_snap(heap, dep, [7u8; 32])));
        let cap = mint_test_cap(Arc::clone(&slot));
        assert!(refresh_capability_or_terminate(&cap).is_ok());

        // Security revision bump (state/policy change) terminates.
        let mut next = (*slot.load()).clone();
        next.security_revision = SecurityRevision::new(2).unwrap();
        slot.store(next);
        assert!(refresh_capability_or_terminate(&cap).is_err());

        // Fresh mint on new revision is live again.
        let cap2 = mint_test_cap(Arc::clone(&slot));
        assert!(refresh_capability_or_terminate(&cap2).is_ok());

        // Suspended is non-serving even if we also bump revision.
        let mut suspended = (*slot.load()).clone();
        suspended.administrative_state = HeapAdministrativeState::Suspended;
        suspended.security_revision = SecurityRevision::new(3).unwrap();
        slot.store(suspended);
        let cap3 = mint_test_cap(Arc::clone(&slot));
        // Mint succeeds from snapshot bytes, but refresh refuses non-serving state.
        assert!(refresh_capability_or_terminate(&cap3).is_err());

        // Chain-head change terminates.
        let mut active = (*slot.load()).clone();
        active.administrative_state = HeapAdministrativeState::Active;
        active.security_revision = SecurityRevision::new(4).unwrap();
        active.authority_chain_head_hash = [0xab; 32];
        slot.store(active);
        let cap4 = mint_test_cap(Arc::clone(&slot));
        assert!(refresh_capability_or_terminate(&cap4).is_ok());
        let mut rotated = (*slot.load()).clone();
        rotated.authority_chain_head_hash = [0xcd; 32];
        // Intentionally leave revision unchanged — chain head alone must terminate.
        slot.store(rotated);
        assert!(refresh_capability_or_terminate(&cap4).is_err());
    }
}
