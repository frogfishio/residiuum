//! Executable isolation model connected to `formal/heap/HeapIsolation.tla`.
//!
//! This is the Rust mirror of the TLA+ sketch used for Gate H6 evidence. It is
//! not a Verus proof; it is a connected, property-tested state machine that
//! checks the same invariants (`ReadConfinement`, `WriteConfinement`,
//! `OwnershipDisjoint`, `RevocationClosed`).

use crate::ids::HeapId;
use std::collections::{BTreeMap, BTreeSet};

/// Opaque damageable unit identity (model-level).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelUnit(pub u32);

/// HeapIsolation model state (`formal/heap/HeapIsolation.tla`).
#[derive(Debug, Clone)]
pub struct IsolationModel {
    /// Owner map: unit → heap, or absent when damaged/unassigned.
    owners: BTreeMap<ModelUnit, HeapId>,
    /// Currently admitted units under `bound_heap`.
    admitted: BTreeSet<ModelUnit>,
    /// Capability-bound heap for the observation under test.
    bound_heap: HeapId,
}

impl IsolationModel {
    /// Start with no owners and an empty admission set.
    pub fn new(bound_heap: HeapId) -> Self {
        Self {
            owners: BTreeMap::new(),
            admitted: BTreeSet::new(),
            bound_heap,
        }
    }

    /// Bound heap for the current observation.
    pub fn bound_heap(&self) -> HeapId {
        self.bound_heap
    }

    /// Assign an owner to an unowned unit.
    pub fn assign_owner(&mut self, unit: ModelUnit, heap: HeapId) -> bool {
        if self.owners.contains_key(&unit) {
            return false;
        }
        self.owners.insert(unit, heap);
        true
    }

    /// Admit a unit owned by the bound heap.
    pub fn admit(&mut self, unit: ModelUnit) -> bool {
        match self.owners.get(&unit) {
            Some(h) if *h == self.bound_heap => {
                self.admitted.insert(unit);
                true
            }
            _ => false,
        }
    }

    /// Damage a unit: clear ownership and admission.
    pub fn damage(&mut self, unit: ModelUnit) {
        self.owners.remove(&unit);
        self.admitted.remove(&unit);
    }

    /// Rebind the observation to another heap; prior admission dies.
    pub fn rebind(&mut self, heap: HeapId) {
        self.bound_heap = heap;
        self.admitted.clear();
    }

    /// TLA `Inv`: all confinement invariants hold.
    pub fn invariants_hold(&self) -> bool {
        self.read_confinement()
            && self.write_confinement()
            && self.ownership_disjoint()
            && self.revocation_closed()
    }

    fn read_confinement(&self) -> bool {
        self.admitted
            .iter()
            .all(|u| self.owners.get(u).is_some_and(|h| *h == self.bound_heap))
    }

    fn write_confinement(&self) -> bool {
        self.owners.iter().all(|(u, h)| {
            if *h != self.bound_heap {
                !self.admitted.contains(u)
            } else {
                true
            }
        })
    }

    fn ownership_disjoint(&self) -> bool {
        // BTreeMap already enforces ≤1 owner per unit.
        true
    }

    fn revocation_closed(&self) -> bool {
        self.admitted.iter().all(|u| self.owners.contains_key(u))
    }
}

/// Run a deterministic walk that mirrors TLC exploration and assert invariants.
pub fn connected_model_smoke(heap_a: HeapId, heap_b: HeapId) {
    let mut m = IsolationModel::new(heap_a);
    assert!(m.invariants_hold());
    assert!(m.assign_owner(ModelUnit(1), heap_a));
    assert!(m.assign_owner(ModelUnit(2), heap_b));
    assert!(m.admit(ModelUnit(1)));
    assert!(!m.admit(ModelUnit(2)), "write confinement: foreign unit");
    assert!(m.invariants_hold());
    m.damage(ModelUnit(1));
    assert!(!m.admitted.contains(&ModelUnit(1)));
    assert!(m.invariants_hold());
    m.rebind(heap_b);
    assert!(m.admitted.is_empty());
    assert!(m.admit(ModelUnit(2)));
    assert!(m.invariants_hold());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuidish(seed: u8) -> [u8; 16] {
        let mut id = [seed; 16];
        id[6] = (id[6] & 0x0f) | 0x40;
        id[8] = (id[8] & 0x3f) | 0x80;
        id
    }

    #[test]
    fn connected_model_preserves_invariants_under_walk() {
        let a = HeapId::from_bytes(uuidish(0xd0)).unwrap();
        let b = HeapId::from_bytes(uuidish(0xd1)).unwrap();
        connected_model_smoke(a, b);

        // Longer deterministic fuzz of the model.
        let mut m = IsolationModel::new(a);
        for step in 0..200u32 {
            let unit = ModelUnit(step % 7);
            match step % 5 {
                0 => {
                    let _ = m.assign_owner(unit, if step % 2 == 0 { a } else { b });
                }
                1 => {
                    let _ = m.admit(unit);
                }
                2 => m.damage(unit),
                3 => m.rebind(if step % 2 == 0 { a } else { b }),
                _ => {
                    let _ = m.admit(unit);
                }
            }
            assert!(
                m.invariants_hold(),
                "invariant broken at step {step}: bound={:?} admitted={:?} owners={:?}",
                m.bound_heap(),
                m.admitted,
                m.owners
            );
        }
    }
}
