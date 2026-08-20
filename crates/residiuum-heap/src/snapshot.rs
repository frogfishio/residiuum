//! Heap security snapshot and atomic slot (`HEAP_SPEC` §8.12 / §30).

use crate::authority::BlacklistEntry;
use crate::ids::{AuthorityEpoch, AuthorityGeneration, DeploymentId, HeapId, SecurityRevision};
use crate::rights::Rights;
use arc_swap::ArcSwap;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard};

/// Administrative state of a heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum HeapAdministrativeState {
    /// Ordinary service.
    Active = 1,
    /// Reads only.
    ReadOnly = 2,
    /// Suspended.
    Suspended = 3,
    /// Retired.
    Retired = 4,
    /// Purge in progress.
    Purging = 5,
    /// Terminal purged.
    Purged = 6,
}

impl HeapAdministrativeState {
    /// Wire / descriptor value.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse descriptor state byte.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Active),
            2 => Some(Self::ReadOnly),
            3 => Some(Self::Suspended),
            4 => Some(Self::Retired),
            5 => Some(Self::Purging),
            6 => Some(Self::Purged),
            _ => None,
        }
    }

    /// State name used by the admission matrix.
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::ReadOnly => "read_only",
            Self::Suspended => "suspended",
            Self::Retired => "retired",
            Self::Purging => "purging",
            Self::Purged => "purged",
        }
    }

    /// Whether the state is terminal for ordinary data service.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Purged)
    }

    /// Whether ordinary data service is available (`HeapAuthority` `Serving`).
    ///
    /// Active and ReadOnly are serving; Suspended/Retired/Purging/Purged are not.
    #[must_use]
    pub fn is_serving(self) -> bool {
        matches!(self, Self::Active | Self::ReadOnly)
    }
}

/// Resident heap security snapshot used by the hot path.
#[derive(Debug, Clone)]
pub struct HeapSecuritySnapshot {
    /// Deployment.
    pub deployment_id: DeploymentId,
    /// Heap.
    pub heap_id: HeapId,
    /// Authority epoch.
    pub authority_epoch: AuthorityEpoch,
    /// Current generation.
    pub authority_generation: AuthorityGeneration,
    /// Previous generation during grace (if any).
    pub previous_generation: Option<AuthorityGeneration>,
    /// Grace deadline unix seconds.
    pub grace_deadline_unix_s: Option<u64>,
    /// Current master public key.
    pub master_public_key: [u8; 32],
    /// Previous master public key during grace.
    pub previous_master_public_key: Option<[u8; 32]>,
    /// Security revision.
    pub security_revision: SecurityRevision,
    /// Authority chain head hash.
    pub authority_chain_head_hash: [u8; 32],
    /// Administrative state.
    pub administrative_state: HeapAdministrativeState,
    /// Blacklist (bounded).
    pub blacklist: Vec<BlacklistEntry>,
    /// Optional rights ceiling from access policy (None = no extra ceiling).
    pub policy_rights_ceiling: Option<Rights>,
}

/// Atomic replaceable snapshot slot.
pub struct HeapSlot {
    inner: ArcSwap<HeapSecuritySnapshot>,
    authority_frontier: Mutex<()>,
}

/// Opaque proof that one caller owns a Heap's authority frontier.
#[doc(hidden)]
pub struct HeapAuthorityGuard<'a> {
    slot: &'a HeapSlot,
    _guard: MutexGuard<'a, ()>,
}

impl HeapAuthorityGuard<'_> {
    /// Load the snapshot protected by this guard.
    pub fn load(&self) -> Arc<HeapSecuritySnapshot> {
        self.slot.load()
    }

    /// Replace the protected snapshot without recursively taking the lock.
    pub fn store(&self, snapshot: HeapSecuritySnapshot) {
        self.slot.inner.store(Arc::new(snapshot));
    }
}

/// The authority frontier was poisoned by a panic in its previous owner.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityFrontierPoisoned;

impl HeapSlot {
    /// Create a slot with an initial snapshot.
    pub fn new(snapshot: HeapSecuritySnapshot) -> Self {
        Self {
            inner: ArcSwap::from_pointee(snapshot),
            authority_frontier: Mutex::new(()),
        }
    }

    /// Load current snapshot.
    pub fn load(&self) -> Arc<HeapSecuritySnapshot> {
        self.inner.load_full()
    }

    /// Replace snapshot (security revision must advance).
    pub fn store(&self, snapshot: HeapSecuritySnapshot) {
        let _frontier = self
            .authority_frontier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.inner.store(Arc::new(snapshot));
    }

    /// Lock the Heap authority/lifecycle serialization frontier.
    ///
    /// A caller that makes a durable decision from Heap authority must retain
    /// this guard until that decision is stable. Snapshot replacements take
    /// the same lock, so admission cannot race suspension, retirement, key
    /// rotation, blacklist changes, or another security-revision advance.
    #[doc(hidden)]
    pub fn lock_authority_frontier(
        &self,
    ) -> Result<HeapAuthorityGuard<'_>, AuthorityFrontierPoisoned> {
        self.authority_frontier
            .lock()
            .map(|guard| HeapAuthorityGuard {
                slot: self,
                _guard: guard,
            })
            .map_err(|_| AuthorityFrontierPoisoned)
    }
}

impl HeapSecuritySnapshot {
    /// Canonical opaque revision bound into an Atomic plan.
    ///
    /// `security_revision` is the monotonic authority/lifecycle generation;
    /// the remaining identity fields prevent a numerically equal revision
    /// from another Heap, deployment, epoch, or authority chain from matching.
    #[must_use]
    pub fn atomic_authority_revision(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"RESIDIUUM-ATOMIC-HEAP-AUTHORITY-V1");
        hash.update([0]);
        hash.update(self.deployment_id.as_bytes());
        hash.update(self.heap_id.as_bytes());
        hash.update(self.authority_epoch.get().to_be_bytes());
        hash.update(self.authority_generation.get().to_be_bytes());
        hash.update(self.security_revision.get().to_be_bytes());
        hash.update(self.authority_chain_head_hash);
        hash.update([self.administrative_state.as_u8()]);
        hash.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthorityEpoch, AuthorityGeneration, DeploymentId, HeapId, SecurityRevision};
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    fn snapshot() -> HeapSecuritySnapshot {
        HeapSecuritySnapshot {
            deployment_id: DeploymentId::new_random().unwrap(),
            heap_id: HeapId::new_random().unwrap(),
            authority_epoch: AuthorityEpoch::new(1).unwrap(),
            authority_generation: AuthorityGeneration::new(1).unwrap(),
            previous_generation: None,
            grace_deadline_unix_s: None,
            master_public_key: [1; 32],
            previous_master_public_key: None,
            security_revision: SecurityRevision::new(1).unwrap(),
            authority_chain_head_hash: [2; 32],
            administrative_state: HeapAdministrativeState::Active,
            blacklist: vec![],
            policy_rights_ceiling: None,
        }
    }

    #[test]
    fn atomic_authority_revision_changes_with_security_and_lifecycle() {
        let base = snapshot();
        let revision = base.atomic_authority_revision();
        let mut advanced = base.clone();
        advanced.security_revision = SecurityRevision::new(2).unwrap();
        assert_ne!(revision, advanced.atomic_authority_revision());
        let mut suspended = base;
        suspended.administrative_state = HeapAdministrativeState::Suspended;
        assert_ne!(revision, suspended.atomic_authority_revision());
    }

    #[test]
    fn snapshot_replacement_waits_for_authority_frontier() {
        let slot = Arc::new(HeapSlot::new(snapshot()));
        let guard = slot.lock_authority_frontier().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let writer = Arc::clone(&slot);
        let join = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let mut next = (*writer.load()).clone();
            next.security_revision = SecurityRevision::new(2).unwrap();
            writer.store(next);
            finished_tx.send(()).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(finished_rx.recv_timeout(Duration::from_millis(25)).is_err());
        drop(guard);
        finished_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        join.join().unwrap();
        assert_eq!(slot.load().security_revision.get(), 2);
    }
}
