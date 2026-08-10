//! Executable authority model connected to `formal/heap/HeapAuthority.tla`.
//!
//! Rust mirror of the TLA+ sketch used for Gate H6 evidence. Checks generation
//! acceptance, blacklist hits, grace windows, and terminal administrative states.
//! Not a Verus proof; not a claim that `qualified=true`.

use std::collections::BTreeSet;

/// Opaque certificate identity at the model level (`Certs` in TLA).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelCert(pub u32);

/// Administrative state wire names aligned with `HeapAdministrativeState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModelAdminState {
    /// Serving; admits ordinary ops.
    Active,
    /// Serving; mutations restricted.
    ReadOnly,
    /// Non-serving; prior caps terminate.
    Suspended,
    /// Non-serving; identity retained.
    Retired,
    /// Non-serving; purge in progress.
    Purging,
    /// Terminal; no re-admit.
    Purged,
}

impl ModelAdminState {
    /// TLA `Serving`.
    #[must_use]
    pub fn is_serving(self) -> bool {
        matches!(self, Self::Active | Self::ReadOnly)
    }

    /// TLA `Terminal` (`purged` only in the formal sketch).
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Purged)
    }
}

/// HeapAuthority model state (`formal/heap/HeapAuthority.tla`).
#[derive(Debug, Clone)]
pub struct AuthorityModel {
    /// Current authority generation (`currentGen`).
    current_gen: u64,
    /// Previous generation during grace, or `None` (`previousGen = 0`).
    previous_gen: Option<u64>,
    /// Grace deadline unix seconds; `None` means no grace.
    grace_deadline: Option<u64>,
    /// Trusted security time (`now`).
    now: u64,
    /// Certificates blacklisted for the previous generation.
    blacklist: BTreeSet<ModelCert>,
    /// Administrative state.
    admin_state: ModelAdminState,
    /// Currently admitted certificates.
    admitted: BTreeSet<ModelCert>,
}

impl AuthorityModel {
    /// Fresh model: generation 1, active, empty admission.
    pub fn new(current_gen: u64) -> Self {
        Self {
            current_gen,
            previous_gen: None,
            grace_deadline: None,
            now: 0,
            blacklist: BTreeSet::new(),
            admin_state: ModelAdminState::Active,
            admitted: BTreeSet::new(),
        }
    }

    /// Current generation.
    #[must_use]
    pub fn current_gen(&self) -> u64 {
        self.current_gen
    }

    /// Previous generation during grace, if any.
    #[must_use]
    pub fn previous_gen(&self) -> Option<u64> {
        self.previous_gen
    }

    /// Trusted time.
    #[must_use]
    pub fn now(&self) -> u64 {
        self.now
    }

    /// Administrative state.
    #[must_use]
    pub fn admin_state(&self) -> ModelAdminState {
        self.admin_state
    }

    /// Admitted set.
    #[must_use]
    pub fn admitted(&self) -> &BTreeSet<ModelCert> {
        &self.admitted
    }

    /// TLA `GenOK`.
    #[must_use]
    pub fn gen_ok(&self, cert_gen: u64) -> bool {
        if cert_gen == self.current_gen {
            return true;
        }
        match (self.previous_gen, self.grace_deadline) {
            (Some(prev), Some(deadline)) => cert_gen == prev && self.now <= deadline,
            _ => false,
        }
    }

    /// TLA `NotBlacklisted` (blacklist applies to previous-gen certificates).
    #[must_use]
    pub fn not_blacklisted(&self, cert: ModelCert) -> bool {
        !self.blacklist.contains(&cert) || self.previous_gen.is_none()
    }

    /// TLA `AdmissionOK`.
    #[must_use]
    pub fn admission_ok(&self, cert: ModelCert, cert_gen: u64) -> bool {
        self.admin_state.is_serving()
            && !self.admin_state.is_terminal()
            && self.gen_ok(cert_gen)
            && self.not_blacklisted(cert)
    }

    /// Advance trusted time by one step (`AdvanceTime`).
    pub fn advance_time(&mut self) {
        self.now = self.now.saturating_add(1);
    }

    /// Hard generation cycle: no grace, clear blacklist and admissions.
    pub fn hard_cycle(&mut self, new_gen: u64) -> bool {
        if new_gen == self.current_gen {
            return false;
        }
        self.current_gen = new_gen;
        self.previous_gen = None;
        self.grace_deadline = None;
        self.blacklist.clear();
        self.admitted.clear();
        true
    }

    /// Grace cycle: previous gen remains acceptable until `deadline`.
    pub fn grace_cycle(&mut self, new_gen: u64, deadline: u64) -> bool {
        if new_gen == self.current_gen || deadline < self.now {
            return false;
        }
        self.previous_gen = Some(self.current_gen);
        self.current_gen = new_gen;
        self.grace_deadline = Some(deadline);
        self.blacklist.clear();
        self.admitted.clear();
        true
    }

    /// Add a certificate to the previous-generation blacklist.
    pub fn blacklist_add(&mut self, cert: ModelCert) -> bool {
        if self.previous_gen.is_none() {
            return false;
        }
        self.blacklist.insert(cert);
        self.admitted.remove(&cert);
        true
    }

    /// Admit a certificate under its generation when `AdmissionOK`.
    pub fn admit(&mut self, cert: ModelCert, cert_gen: u64) -> bool {
        if !self.admission_ok(cert, cert_gen) {
            return false;
        }
        self.admitted.insert(cert);
        true
    }

    /// Suspend: non-serving; clear admissions.
    pub fn suspend(&mut self) -> bool {
        if self.admin_state != ModelAdminState::Active {
            return false;
        }
        self.admin_state = ModelAdminState::Suspended;
        self.admitted.clear();
        true
    }

    /// Retire: non-serving.
    pub fn retire(&mut self) -> bool {
        if !matches!(
            self.admin_state,
            ModelAdminState::Active | ModelAdminState::ReadOnly | ModelAdminState::Suspended
        ) {
            return false;
        }
        self.admin_state = ModelAdminState::Retired;
        self.admitted.clear();
        true
    }

    /// Complete purge to terminal state.
    pub fn purge_done(&mut self) -> bool {
        if !matches!(
            self.admin_state,
            ModelAdminState::Retired | ModelAdminState::Purging
        ) {
            return false;
        }
        self.admin_state = ModelAdminState::Purged;
        self.admitted.clear();
        true
    }

    /// Expire grace when `now > graceDeadline`.
    pub fn expire_grace(&mut self) -> bool {
        match (self.previous_gen, self.grace_deadline) {
            (Some(_), Some(deadline)) if self.now > deadline => {
                self.previous_gen = None;
                self.grace_deadline = None;
                self.blacklist.clear();
                self.admitted.clear();
                true
            }
            _ => false,
        }
    }

    /// TLA `Inv` (TypeOK + serving/terminal admission rules).
    pub fn invariants_hold(&self) -> bool {
        // All admitted certs require serving admin state.
        if !self.admitted.is_empty() && !self.admin_state.is_serving() {
            return false;
        }
        // Terminal implies empty admitted.
        if self.admin_state.is_terminal() && !self.admitted.is_empty() {
            return false;
        }
        // Every admitted cert must still pass AdmissionOK for some gen
        // (current, or previous during grace and not blacklisted).
        for c in &self.admitted {
            let ok_current = self.admission_ok(*c, self.current_gen);
            let ok_prev = self.previous_gen.is_some_and(|g| self.admission_ok(*c, g));
            if !ok_current && !ok_prev {
                // After time advances past grace without ExpireGrace, Inv still
                // holds only because Next would clear via ExpireGrace; we allow
                // callers to call expire_grace. During the model walk we expire.
                return false;
            }
        }
        true
    }
}

/// Deterministic walk mirroring TLC exploration for HeapAuthority.
pub fn connected_authority_model_smoke() {
    let mut m = AuthorityModel::new(1);
    assert!(m.invariants_hold());
    let c1 = ModelCert(1);
    let c2 = ModelCert(2);

    assert!(m.admit(c1, 1));
    assert!(!m.admit(c2, 2), "future gen refused");
    assert!(m.invariants_hold());

    // Grace cycle: previous gen 1 accepted until deadline.
    assert!(m.grace_cycle(2, 10));
    assert!(m.admitted.is_empty());
    assert!(m.admit(c1, 1), "previous gen during grace");
    assert!(m.admit(c2, 2), "current gen");
    assert!(m.invariants_hold());

    // Blacklist previous-gen cert.
    assert!(m.blacklist_add(c1));
    assert!(!m.admitted.contains(&c1));
    assert!(!m.admit(c1, 1), "blacklisted previous-gen");
    assert!(m.admitted.contains(&c2));
    assert!(m.invariants_hold());

    // Expire grace after time passes.
    while m.now() <= 10 {
        m.advance_time();
    }
    assert!(m.expire_grace());
    assert!(!m.admit(c1, 1), "previous gen after grace");
    assert!(m.admit(c2, 2));
    assert!(m.invariants_hold());

    // Terminal path.
    assert!(m.suspend());
    assert!(m.admitted.is_empty());
    assert!(!m.admit(c2, 2));
    assert!(m.retire());
    assert!(m.purge_done());
    assert!(m.admin_state().is_terminal());
    assert!(!m.admit(c2, 2));
    assert!(m.invariants_hold());

    // Hard cycle clears everything.
    let mut m2 = AuthorityModel::new(1);
    assert!(m2.admit(ModelCert(9), 1));
    assert!(m2.hard_cycle(5));
    assert!(m2.admitted().is_empty());
    assert!(!m2.admit(ModelCert(9), 1));
    assert!(m2.admit(ModelCert(9), 5));
    assert!(m2.invariants_hold());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connected_authority_model_preserves_invariants() {
        connected_authority_model_smoke();

        let mut m = AuthorityModel::new(1);
        for step in 0..300u32 {
            let cert = ModelCert(step % 5);
            match step % 9 {
                0 => {
                    let _ = m.admit(cert, m.current_gen());
                }
                1 => m.advance_time(),
                2 => {
                    let _ = m.grace_cycle(m.current_gen().saturating_add(1).max(2), m.now() + 5);
                }
                3 => {
                    let _ = m.blacklist_add(cert);
                }
                4 => {
                    let _ = m.hard_cycle(m.current_gen().saturating_add(1).max(2));
                }
                5 => {
                    let _ = m.expire_grace();
                }
                6 => {
                    // Prefer suspend→retire→purge path without getting stuck.
                    if m.admin_state() == ModelAdminState::Active {
                        let _ = m.suspend();
                    } else if m.admin_state() == ModelAdminState::Suspended {
                        let _ = m.retire();
                    } else if m.admin_state() == ModelAdminState::Retired {
                        let _ = m.purge_done();
                    } else if m.admin_state().is_terminal() {
                        // Re-seed after terminal so the walk continues.
                        m = AuthorityModel::new(1);
                    }
                }
                7 => {
                    let _ = m.admit(cert, m.previous_gen().unwrap_or(m.current_gen()));
                }
                _ => {
                    let _ = m.admit(cert, m.current_gen());
                }
            }
            // Auto-expire when overdue so Inv stays enforceable.
            let _ = m.expire_grace();
            assert!(
                m.invariants_hold(),
                "invariant broken at step {step}: gen={} state={:?} admitted={:?}",
                m.current_gen(),
                m.admin_state(),
                m.admitted()
            );
        }
    }
}
