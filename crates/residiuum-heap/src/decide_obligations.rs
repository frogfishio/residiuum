//! Executable §39 Verus proof obligations (HP-010 / Gate H6 connected evidence).
//!
//! These property tests are the CI-connected stand-in until Verus proves the same
//! predicates over [`crate::decide`]. They do **not** flip `qualified=true`.

use crate::authority::{BlacklistEntry, BlacklistKind};
use crate::capability::sha256;
use crate::certificate::VerifiedCertificate;
use crate::constraints::{Constraint, Constraints};
use crate::decide::{
    authority_admission_ok, authority_binding_holds, certificate_blacklisted, generation_accepted,
    mint_capability, refresh_capability_or_terminate,
};
use crate::ids::{
    AuthorityEpoch, AuthorityGeneration, CertificateId, CollectionId, DeploymentId, HeapId,
    SecurityRevision,
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

fn snap(
    heap: HeapId,
    dep: DeploymentId,
    master: [u8; 32],
    state: HeapAdministrativeState,
) -> HeapSecuritySnapshot {
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
        administrative_state: state,
        blacklist: vec![],
        policy_rights_ceiling: None,
    }
}

fn cert_for(snap: &HeapSecuritySnapshot, heap: HeapId) -> VerifiedCertificate {
    VerifiedCertificate {
        cose_bytes: vec![0x01],
        fingerprint: [3u8; 32],
        deployment_id: snap.deployment_id,
        heap_id: heap,
        authority_epoch: snap.authority_epoch,
        authority_generation: snap.authority_generation,
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4u8; 32],
        rights: Rights::from_bits_certificate(0x5).unwrap(),
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: sha256(&snap.master_public_key),
    }
}

/// Obligation runners used by HP-010 Accept.
pub mod obligations {
    use super::*;

    /// O1: allow for non-public ops requires authority_binding_holds.
    pub fn allow_implies_authority_binding() -> bool {
        let dep = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
        let heap_a = HeapId::from_bytes(uuidish(0xf0)).unwrap();
        let heap_b = HeapId::from_bytes(uuidish(0xf1)).unwrap();
        let master = [0x11u8; 32];
        let snapshot = snap(heap_a, dep, master, HeapAdministrativeState::Active);
        let ok = cert_for(&snapshot, heap_a);
        let bad = cert_for(&snapshot, heap_b);
        authority_binding_holds(&snapshot, &ok) && !authority_binding_holds(&snapshot, &bad)
    }

    /// O2/O3: critical collection allowlists cannot admit foreign collections.
    pub fn unknown_constraints_cannot_allow_foreign() -> bool {
        let allowed = CollectionId::from_bytes(uuidish(0xf3)).unwrap();
        let foreign = CollectionId::from_bytes(uuidish(0xf4)).unwrap();
        let constraints =
            Constraints::from_sorted(vec![Constraint::CollectionAllowlist(vec![allowed])]).unwrap();
        constraints.allows_collection(allowed) && !constraints.allows_collection(foreign)
    }

    /// O4: terminal / non-serving states refuse capability refresh.
    pub fn terminal_or_non_serving_refuses_refresh() -> bool {
        let dep = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
        let heap = HeapId::from_bytes(uuidish(0xf5)).unwrap();
        let master = [0x22u8; 32];
        for state in [
            HeapAdministrativeState::Purged,
            HeapAdministrativeState::Retired,
            HeapAdministrativeState::Suspended,
            HeapAdministrativeState::Purging,
        ] {
            let slot = Arc::new(HeapSlot::new(snap(heap, dep, master, state)));
            let cert = cert_for(&slot.load(), heap);
            let cap = mint_capability(
                Arc::clone(&slot),
                &cert,
                TrustedInstant {
                    unix_s: 1_700_000_000,
                },
            )
            .unwrap();
            if refresh_capability_or_terminate(&cap).is_ok() {
                return false;
            }
        }
        // Active still refreshes.
        let slot = Arc::new(HeapSlot::new(snap(
            heap,
            dep,
            master,
            HeapAdministrativeState::Active,
        )));
        let cert = cert_for(&slot.load(), heap);
        let cap = mint_capability(
            slot,
            &cert,
            TrustedInstant {
                unix_s: 1_700_000_000,
            },
        )
        .unwrap();
        refresh_capability_or_terminate(&cap).is_ok()
    }

    /// O5: epoch mismatch fails authority_binding_holds.
    pub fn epoch_mismatch_fails_binding() -> bool {
        let dep = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
        let heap = HeapId::from_bytes(uuidish(0xf6)).unwrap();
        let master = [0x33u8; 32];
        let mut snapshot = snap(heap, dep, master, HeapAdministrativeState::Active);
        let mut cert = cert_for(&snapshot, heap);
        assert!(authority_binding_holds(&snapshot, &cert));
        cert.authority_epoch = AuthorityEpoch::new(2).unwrap();
        !authority_binding_holds(&snapshot, &cert) && {
            snapshot.authority_epoch = AuthorityEpoch::new(2).unwrap();
            authority_binding_holds(&snapshot, &cert)
        }
    }

    /// O6: previous generation accepted during grace; refused after deadline
    /// (HeapAuthority `GenOK` / `ExpireGrace` connection via [`generation_accepted`]).
    pub fn generation_grace_acceptance() -> bool {
        let dep = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
        let heap = HeapId::from_bytes(uuidish(0xf7)).unwrap();
        let master = [0x44u8; 32];
        let mut snapshot = snap(heap, dep, master, HeapAdministrativeState::Active);
        snapshot.authority_generation = AuthorityGeneration::new(2).unwrap();
        snapshot.previous_generation = Some(AuthorityGeneration::new(1).unwrap());
        snapshot.grace_deadline_unix_s = Some(1_700_000_100);

        let mut cert = cert_for(&snapshot, heap);
        cert.authority_generation = AuthorityGeneration::new(1).unwrap();

        let during = generation_accepted(
            &snapshot,
            &cert,
            TrustedInstant {
                unix_s: 1_700_000_050,
            },
        );
        let after = generation_accepted(
            &snapshot,
            &cert,
            TrustedInstant {
                unix_s: 1_700_000_200,
            },
        );
        let current_ok = {
            cert.authority_generation = AuthorityGeneration::new(2).unwrap();
            generation_accepted(
                &snapshot,
                &cert,
                TrustedInstant {
                    unix_s: 1_700_000_200,
                },
            )
        };
        during && !after && current_ok
    }

    /// O7: blacklisted certificate is detected (HeapAuthority `NotBlacklisted`
    /// via [`certificate_blacklisted`]).
    pub fn blacklist_refuses_admit() -> bool {
        let dep = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
        let heap = HeapId::from_bytes(uuidish(0xf8)).unwrap();
        let master = [0x55u8; 32];
        let mut snapshot = snap(heap, dep, master, HeapAdministrativeState::Active);
        let cert = cert_for(&snapshot, heap);
        assert!(!certificate_blacklisted(&snapshot, &cert));
        snapshot.blacklist.push(BlacklistEntry {
            kind: BlacklistKind::CertificateHash,
            generation: cert.authority_generation.get(),
            fingerprint: cert.fingerprint,
        });
        certificate_blacklisted(&snapshot, &cert)
    }

    /// O8: `authority_admission_ok` bundles GenOK / NotBlacklisted / Serving / issuer
    /// (HeapAuthority `AdmissionOK` over a resident snapshot).
    pub fn authority_admission_bundle() -> bool {
        let dep = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
        let heap = HeapId::from_bytes(uuidish(0xf9)).unwrap();
        let master = [0x66u8; 32];
        let now = TrustedInstant {
            unix_s: 1_700_000_050,
        };

        let mut snapshot = snap(heap, dep, master, HeapAdministrativeState::Active);
        let cert = cert_for(&snapshot, heap);
        if !authority_admission_ok(&snapshot, &cert, now) {
            return false;
        }

        // Previous generation during grace with previous master key.
        snapshot.authority_generation = AuthorityGeneration::new(2).unwrap();
        snapshot.previous_generation = Some(AuthorityGeneration::new(1).unwrap());
        snapshot.grace_deadline_unix_s = Some(1_700_000_100);
        snapshot.previous_master_public_key = Some(master);
        snapshot.master_public_key = [0x77u8; 32];
        let mut prev = cert_for(&snapshot, heap);
        prev.authority_generation = AuthorityGeneration::new(1).unwrap();
        prev.issuer_master_key_id = sha256(&master);
        if !authority_admission_ok(&snapshot, &prev, now) {
            return false;
        }
        // After grace deadline.
        if authority_admission_ok(
            &snapshot,
            &prev,
            TrustedInstant {
                unix_s: 1_700_000_200,
            },
        ) {
            return false;
        }

        // Blacklist refuses admission.
        let mut blacklisted = snap(heap, dep, master, HeapAdministrativeState::Active);
        let cert2 = cert_for(&blacklisted, heap);
        blacklisted.blacklist.push(BlacklistEntry {
            kind: BlacklistKind::CertificateHash,
            generation: cert2.authority_generation.get(),
            fingerprint: cert2.fingerprint,
        });
        if authority_admission_ok(&blacklisted, &cert2, now) {
            return false;
        }

        // Non-serving refuses admission.
        let suspended = snap(heap, dep, master, HeapAdministrativeState::Suspended);
        let cert3 = cert_for(&suspended, heap);
        if authority_admission_ok(&suspended, &cert3, now) {
            return false;
        }

        true
    }

    /// O9: mint grace deadline binds only previous-generation certificates (§8.11).
    pub fn mint_grace_only_previous_generation() -> bool {
        let dep = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
        let heap = HeapId::from_bytes(uuidish(0xfa)).unwrap();
        let master_prev = [0x88u8; 32];
        let master_cur = [0x99u8; 32];

        // Current generation past an unrelated grace deadline must still mint.
        let mut snapshot = snap(heap, dep, master_cur, HeapAdministrativeState::Active);
        snapshot.authority_generation = AuthorityGeneration::new(2).unwrap();
        snapshot.previous_generation = Some(AuthorityGeneration::new(1).unwrap());
        snapshot.grace_deadline_unix_s = Some(1_700_000_000); // already expired
        snapshot.previous_master_public_key = Some(master_prev);
        let slot = Arc::new(HeapSlot::new(snapshot.clone()));
        let mut current = cert_for(&snapshot, heap);
        current.authority_generation = AuthorityGeneration::new(2).unwrap();
        current.issuer_master_key_id = sha256(&master_cur);
        let now_past_grace = TrustedInstant {
            unix_s: 1_700_000_050,
        };
        if mint_capability(Arc::clone(&slot), &current, now_past_grace).is_err() {
            return false;
        }

        // Previous generation after grace deadline must refuse mint.
        let mut prev = cert_for(&snapshot, heap);
        prev.authority_generation = AuthorityGeneration::new(1).unwrap();
        prev.issuer_master_key_id = sha256(&master_prev);
        if mint_capability(slot, &prev, now_past_grace).is_ok() {
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::obligations;

    #[test]
    fn verus_connected_obligations_hold() {
        assert!(obligations::allow_implies_authority_binding());
        assert!(obligations::unknown_constraints_cannot_allow_foreign());
        assert!(obligations::terminal_or_non_serving_refuses_refresh());
        assert!(obligations::epoch_mismatch_fails_binding());
        assert!(obligations::generation_grace_acceptance());
        assert!(obligations::blacklist_refuses_admit());
        assert!(obligations::authority_admission_bundle());
        assert!(obligations::mint_grace_only_previous_generation());
    }
}
