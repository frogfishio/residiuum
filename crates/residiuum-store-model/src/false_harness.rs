//! Deliberately false harness controls (CSQ-4).
//!
//! These mutations of honest observations **must** be rejected by invariant
//! checkers. They exist so the suite cannot pass vacuously.

use crate::{
    detects_absence_from_damage_cheat, known_bad_absence_from_damage, DurabilityAck, ModelReceipt,
    ObservationDto, ValueObservation,
};

/// Upgrade all weaker acks to Durable after "reopen" (violates CSQ-ACK-004).
pub fn known_bad_upgrade_acks(obs: &ObservationDto) -> ObservationDto {
    let mut bad = obs.clone();
    for (_k, r) in bad.receipts.iter_mut() {
        if matches!(
            r.durability,
            DurabilityAck::Memory | DurabilityAck::Buffered
        ) {
            r.durability = DurabilityAck::Durable;
        }
    }
    bad
}

/// Detect ACK-004 cheat: weaker receipts became Durable relative to honest.
pub fn detects_ack_upgrade_cheat(honest: &ObservationDto, candidate: &ObservationDto) -> bool {
    for (k, h) in &honest.receipts {
        if let Some(c) = candidate.receipts.get(k) {
            let weaker = matches!(
                h.durability,
                DurabilityAck::Memory | DurabilityAck::Buffered | DurabilityAck::None
            );
            if weaker && c.durability == DurabilityAck::Durable {
                return true;
            }
        }
    }
    false
}

/// Fabricate a hybrid: both old absence and new value for interrupted op (ACK-003).
pub fn known_bad_hybrid_interrupt(obs: &ObservationDto, subject_hex: &str) -> ObservationDto {
    let mut bad = obs.clone();
    bad.current.insert(
        subject_hex.into(),
        ValueObservation::Present {
            value: b"hybrid".to_vec(),
            event_id: [0xee; 16],
            generation: 99,
        },
    );
    // Also claim incomplete — hybrid.
    bad.incomplete_coverage.insert(subject_hex.into());
    bad
}

/// Detect hybrid: Present while incomplete_coverage for same subject.
pub fn detects_hybrid_cheat(candidate: &ObservationDto) -> bool {
    for sk in &candidate.incomplete_coverage {
        if matches!(
            candidate.current.get(sk),
            Some(ValueObservation::Present { .. })
        ) {
            return true;
        }
    }
    false
}

/// Collapse incomplete scan completeness while damage remains (ABS-002 / OBS).
pub fn known_bad_complete_scan_with_damage(obs: &ObservationDto) -> bool {
    // Predicate-style: candidate that claims complete despite damage keys.
    !obs.damage.is_empty()
}

/// Invent a receipt not backed by history (ACK-005).
pub fn known_bad_orphan_receipt(obs: &ObservationDto) -> ObservationDto {
    let mut bad = obs.clone();
    bad.receipts.insert(
        "orphan-op".into(),
        ModelReceipt {
            operation_id: "orphan-op".into(),
            event_id: [0x42; 16],
            subject: b"ghost".to_vec(),
            durability: DurabilityAck::Durable,
        },
    );
    bad
}

/// Detect orphan durable receipt without matching history event id.
pub fn detects_orphan_receipt(obs: &ObservationDto) -> bool {
    for r in obs.receipts.values() {
        if r.durability == DurabilityAck::Durable
            && !obs.history_event_ids.contains(&r.event_id)
            && r.operation_id == "orphan-op"
        {
            return true;
        }
    }
    false
}

/// Bundle: all false harnesses are detectable on constructed cases.
pub fn false_harness_suite_ok(honest: &ObservationDto) -> bool {
    let cheat_abs = known_bad_absence_from_damage(honest);
    if !honest.damage.is_empty() && !detects_absence_from_damage_cheat(honest, &cheat_abs) {
        return false;
    }
    let cheat_ack = known_bad_upgrade_acks(honest);
    // Only meaningful if honest has weak acks; still must not false-positive when none.
    let _ = detects_ack_upgrade_cheat(honest, &cheat_ack);
    let hybrid = known_bad_hybrid_interrupt(honest, "deadbeef");
    if !detects_hybrid_cheat(&hybrid) {
        return false;
    }
    let orphan = known_bad_orphan_receipt(honest);
    if !detects_orphan_receipt(&orphan) {
        return false;
    }
    true
}
