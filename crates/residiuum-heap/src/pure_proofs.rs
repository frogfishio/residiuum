//! Pure-kernel proof obligations for Gate H6 (§39) — Verus/Kani targets.
//!
//! These predicates are intentionally free of I/O and ambient clocks. They are the
//! executable stand-ins that a future Verus (or Kani) harness should prove over the
//! same pure functions in [`crate::decide`] and the model machines.
//!
//! **Does not** flip [`crate::may_advertise_qualified`]. Machine-checked paths:
//! Kani harnesses here (`#[cfg(kani)]`) and Verus `verification/heap-verus/verus/pure_kernel.rs`
//! (`VERUS_PROOFS_CONNECTED`). CPR-005 external review remains open.

use crate::authority::{BlacklistEntry, BlacklistKind};
use crate::capability::sha256;
use crate::certificate::VerifiedCertificate;
use crate::constraints::Constraints;
use crate::decide::{
    authority_admission_ok, authority_binding_holds, certificate_blacklisted, generation_accepted,
};
use crate::ids::{
    AuthorityEpoch, AuthorityGeneration, CertificateId, DeploymentId, HeapId, SecurityRevision,
};
use crate::rights::Rights;
use crate::security_time::TrustedInstant;
use crate::snapshot::{HeapAdministrativeState, HeapSecuritySnapshot};
use crate::authority_model::{AuthorityModel, ModelAdminState, ModelCert};
use crate::isolation_model::{IsolationModel, ModelUnit};

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

fn cert_for(s: &HeapSecuritySnapshot, heap: HeapId) -> VerifiedCertificate {
    // Deterministic IDs (no getrandom) so Kani harnesses stay pure.
    VerifiedCertificate {
        cose_bytes: vec![0x01],
        fingerprint: [3u8; 32],
        deployment_id: s.deployment_id,
        heap_id: heap,
        authority_epoch: s.authority_epoch,
        authority_generation: s.authority_generation,
        certificate_id: CertificateId::from_bytes(uuidish(0x30)).unwrap(),
        holder_public_key: [4u8; 32],
        rights: Rights::from_bits_certificate(0x5).unwrap(),
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: sha256(&s.master_public_key),
    }
}

/// O-binding: non-matching heap identity fails authority binding (Verus O1 core).
#[must_use]
pub fn lemma_binding_rejects_foreign_heap() -> bool {
    let dep = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
    let a = HeapId::from_bytes(uuidish(0x10)).unwrap();
    let b = HeapId::from_bytes(uuidish(0x11)).unwrap();
    let s = snap(a, dep, [1u8; 32], HeapAdministrativeState::Active);
    let ok = cert_for(&s, a);
    let bad = cert_for(&s, b);
    authority_binding_holds(&s, &ok) && !authority_binding_holds(&s, &bad)
}

/// O-gen: previous generation accepted only inside grace window.
#[must_use]
pub fn lemma_generation_grace_window() -> bool {
    let dep = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
    let heap = HeapId::from_bytes(uuidish(0x12)).unwrap();
    let mut s = snap(heap, dep, [2u8; 32], HeapAdministrativeState::Active);
    s.authority_generation = AuthorityGeneration::new(2).unwrap();
    s.previous_generation = Some(AuthorityGeneration::new(1).unwrap());
    s.grace_deadline_unix_s = Some(100);
    let mut c = cert_for(&s, heap);
    c.authority_generation = AuthorityGeneration::new(1).unwrap();
    let during = generation_accepted(&s, &c, TrustedInstant { unix_s: 50 });
    let after = generation_accepted(&s, &c, TrustedInstant { unix_s: 101 });
    during && !after
}

/// O-black: same-generation certificate blacklist hits.
#[must_use]
pub fn lemma_blacklist_hits_certificate_hash() -> bool {
    let dep = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
    let heap = HeapId::from_bytes(uuidish(0x13)).unwrap();
    let mut s = snap(heap, dep, [3u8; 32], HeapAdministrativeState::Active);
    let c = cert_for(&s, heap);
    assert!(!certificate_blacklisted(&s, &c));
    s.blacklist.push(BlacklistEntry {
        kind: BlacklistKind::CertificateHash,
        generation: c.authority_generation.get(),
        fingerprint: c.fingerprint,
    });
    certificate_blacklisted(&s, &c)
}

/// O-admit: non-serving administrative states refuse admission.
#[must_use]
pub fn lemma_non_serving_refuses_admission() -> bool {
    let dep = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
    let heap = HeapId::from_bytes(uuidish(0x14)).unwrap();
    let now = TrustedInstant { unix_s: 10 };
    for state in [
        HeapAdministrativeState::Suspended,
        HeapAdministrativeState::Retired,
        HeapAdministrativeState::Purging,
        HeapAdministrativeState::Purged,
    ] {
        let s = snap(heap, dep, [4u8; 32], state);
        let c = cert_for(&s, heap);
        if authority_admission_ok(&s, &c, now) {
            return false;
        }
    }
    let active = snap(heap, dep, [4u8; 32], HeapAdministrativeState::Active);
    let c = cert_for(&active, heap);
    authority_admission_ok(&active, &c, now)
}

/// O-isolation: IsolationModel Inv preserved under a fixed walk (HeapIsolation.tla).
#[must_use]
pub fn lemma_isolation_model_inv_walk() -> bool {
    let a = HeapId::from_bytes(uuidish(0x20)).unwrap();
    let b = HeapId::from_bytes(uuidish(0x21)).unwrap();
    let mut m = IsolationModel::new(a);
    if !m.invariants_hold() {
        return false;
    }
    assert!(m.assign_owner(ModelUnit(1), a));
    assert!(m.assign_owner(ModelUnit(2), b));
    assert!(m.admit(ModelUnit(1)));
    assert!(!m.admit(ModelUnit(2))); // foreign owner
    m.damage(ModelUnit(1));
    if m.invariants_hold() == false {
        return false;
    }
    m.rebind(b);
    assert!(m.admit(ModelUnit(2)));
    m.invariants_hold()
}

/// O-authority: AuthorityModel Inv preserved under grace/blacklist/terminal walk.
#[must_use]
pub fn lemma_authority_model_inv_walk() -> bool {
    let mut m = AuthorityModel::new(1);
    if !m.invariants_hold() {
        return false;
    }
    // Grace cycle then blacklist a previous-gen identity.
    assert!(m.grace_cycle(2, 10));
    assert!(m.blacklist_add(ModelCert(7)));
    assert!(m.admit(ModelCert(1), 2)); // current gen
    assert!(!m.admit(ModelCert(7), 1)); // blacklisted previous
    while m.now() <= 10 {
        m.advance_time();
    }
    assert!(m.expire_grace());
    assert!(m.suspend());
    if !m.invariants_hold() {
        return false;
    }
    assert!(m.retire());
    assert!(m.purge_done());
    m.admin_state() == ModelAdminState::Purged && m.invariants_hold()
}

/// Bundle used by HP-010 Accept: all pure lemmas hold.
#[must_use]
pub fn connected_pure_proof_bundle() -> bool {
    lemma_binding_rejects_foreign_heap()
        && lemma_generation_grace_window()
        && lemma_blacklist_hits_certificate_hash()
        && lemma_non_serving_refuses_admission()
        && lemma_isolation_model_inv_walk()
        && lemma_authority_model_inv_walk()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pure_proof_lemmas_hold() {
        assert!(connected_pure_proof_bundle());
    }
}

/// Kani harnesses for pure §39 lemmas (Gate H6 / CPR-004).
///
/// Run: `cargo kani -p residiuum-heap --harness kani_connected_pure_proof_bundle`
/// (requires `kani-verifier` + `cargo kani setup`). CI job `kani-heap` runs these.
#[cfg(kani)]
#[allow(dead_code)]
mod kani_harnesses {
    use super::*;

    #[kani::proof]
    fn kani_binding_rejects_foreign_heap() {
        kani::assert(
            lemma_binding_rejects_foreign_heap(),
            "foreign heap must fail authority binding",
        );
    }

    #[kani::proof]
    fn kani_generation_grace_window() {
        kani::assert(
            lemma_generation_grace_window(),
            "previous generation only inside grace",
        );
    }

    #[kani::proof]
    fn kani_blacklist_hits_certificate_hash() {
        kani::assert(
            lemma_blacklist_hits_certificate_hash(),
            "blacklist must hit certificate fingerprint",
        );
    }

    #[kani::proof]
    fn kani_non_serving_refuses_admission() {
        kani::assert(
            lemma_non_serving_refuses_admission(),
            "non-serving states refuse admission",
        );
    }

    #[kani::proof]
    fn kani_isolation_model_inv_walk() {
        kani::assert(
            lemma_isolation_model_inv_walk(),
            "IsolationModel Inv preserved under walk",
        );
    }

    #[kani::proof]
    fn kani_authority_model_inv_walk() {
        kani::assert(
            lemma_authority_model_inv_walk(),
            "AuthorityModel Inv preserved under walk",
        );
    }

    #[kani::proof]
    fn kani_connected_pure_proof_bundle() {
        kani::assert(
            connected_pure_proof_bundle(),
            "full pure proof bundle must hold",
        );
    }
}
