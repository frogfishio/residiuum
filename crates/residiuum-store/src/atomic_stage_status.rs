//! Store-authoritative Atomic examination (CR-ATMR6-005).
//!
//! Derived from the catalogue, not from whether the kernel could install a
//! complete member set. A durable prepare with missing members is `Prepared`,
//! never absence.

use crate::atomic_stage_media::{stage_key, StageAtomicKey, StageCatalog};
use residiuum_atomics::{members_match_prepare, AtomicId, DecisionCode, HeapId};
use std::collections::BTreeSet;

/// Public classification of one Atomic identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AtomicStageClass {
    /// No valid prepare for this identity.
    Absent,
    /// Valid durable prepare; members and/or payloads are incomplete.
    Prepared,
    /// Prepare plus complete matching members and payloads; not sealed.
    Staged,
    /// First stable boundary is recorded.
    Sealed,
    /// A durable committed decision is present. Publication is an ATM-3
    /// derived phase and is not implied by this staging status alone.
    Committed,
    /// A durable terminal not-committed decision is present.
    NotCommitted,
    /// Identity blocked by conflict or damage.
    Blocked,
}

/// Typed examination of surviving prepare/material evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicStageStatus {
    /// Identity under examination.
    pub atomic_id: AtomicId,
    /// Catalogue class. `Absent` only when no valid prepare exists.
    pub class: AtomicStageClass,
    /// Intended member count when known (from the issuing prepare).
    pub intended_members: u32,
    /// Members present in the catalogue.
    pub present_members: u32,
    /// Payload bodies or locators present.
    pub present_payloads: u32,
    /// Frozen chunk maps present.
    pub present_chunk_plans: u32,
    /// Chunk bodies or locators present.
    pub present_chunk_bodies: u32,
    /// A matching seal is catalogued.
    pub sealed: bool,
    /// Durable decision, when present.
    pub decision: Option<DecisionCode>,
    /// Non-zero Heap commit position for a committed decision.
    pub commit_position: Option<u64>,
    /// Identity is blocked.
    pub blocked: bool,
    /// Store coverage is degraded.
    pub coverage_degraded: bool,
}

/// Project one identity from the authoritative catalogue.
pub fn project_atomic(
    catalog: &StageCatalog,
    heap_id: HeapId,
    atomic_id: AtomicId,
) -> AtomicStageStatus {
    let key = stage_key(heap_id, atomic_id);
    let blocked = catalog.blocked.contains(&key);
    let prepare = catalog.prepares.get(&key);
    let members = catalog.members.get(&key);
    let present_members = members.map(|ms| ms.len() as u32).unwrap_or(0);
    let intended_members = prepare.map(|p| p.member_count).unwrap_or(present_members);
    let present_payloads = catalog
        .payloads
        .keys()
        .chain(catalog.payload_refs.keys())
        .filter_map(|(heap, id, ordinal)| ((*heap, *id) == key).then_some(*ordinal))
        .collect::<BTreeSet<_>>()
        .len() as u32;
    let present_chunk_plans = catalog
        .chunk_plans
        .keys()
        .filter(|(heap, id, _)| (*heap, *id) == key)
        .count() as u32;
    let present_chunk_bodies = catalog
        .chunks
        .keys()
        .chain(catalog.chunk_refs.keys())
        .filter_map(|(heap, id, ordinal, index)| {
            ((*heap, *id) == key).then_some((*ordinal, *index))
        })
        .collect::<BTreeSet<_>>()
        .len() as u32;
    let sealed = catalog.seals.contains_key(&key);
    let decision = catalog.decisions.get(&key);
    let members_complete = prepare.is_some_and(|p| {
        let ms = members.cloned().unwrap_or_default();
        intended_members > 0 && present_members >= intended_members && members_match_prepare(p, &ms)
    });
    let material_complete = material_complete(catalog, key);
    let class = if blocked {
        AtomicStageClass::Blocked
    } else if decision.is_some_and(|d| d.decision == DecisionCode::Committed) {
        AtomicStageClass::Committed
    } else if decision.is_some_and(|d| d.decision == DecisionCode::NotCommitted) {
        AtomicStageClass::NotCommitted
    } else if prepare.is_none() {
        AtomicStageClass::Absent
    } else if sealed {
        AtomicStageClass::Sealed
    } else if members_complete && material_complete {
        AtomicStageClass::Staged
    } else {
        AtomicStageClass::Prepared
    };
    AtomicStageStatus {
        atomic_id,
        class,
        intended_members,
        present_members,
        present_payloads,
        present_chunk_plans,
        present_chunk_bodies,
        sealed,
        decision: decision.map(|d| d.decision),
        commit_position: decision.and_then(|d| d.commit_position),
        blocked,
        coverage_degraded: catalog.coverage_degraded,
    }
}

pub(crate) fn material_complete(catalog: &StageCatalog, key: StageAtomicKey) -> bool {
    catalog.members.get(&key).is_some_and(|members| {
        members.iter().all(|member| {
            catalog.has_payload(key, member.ordinal)
                || catalog
                    .payload_refs
                    .contains_key(&(key.0, key.1, member.ordinal))
                || chunk_complete(catalog, key, member.ordinal)
        })
    })
}

fn chunk_complete(catalog: &StageCatalog, key: StageAtomicKey, ordinal: u32) -> bool {
    let Some(plan) = catalog.chunk_plans.get(&(key.0, key.1, ordinal)) else {
        return false;
    };
    (0..plan.total).all(|i| {
        catalog.has_chunk(key, ordinal, i)
            || catalog.chunk_refs.contains_key(&(key.0, key.1, ordinal, i))
    })
}
