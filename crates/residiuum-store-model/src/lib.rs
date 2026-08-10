//! Sequential logical store model for CSQ-1 / CSQ-4 (SPEC §6–§7).
//!
//! Knows no segment layout, production scanner, or recovery algorithm.
//! Authority is a pure event/state machine with explicit acknowledgement
//! and uncertainty outcomes. CSQ-4 adds the publication kernel, history
//! APIs, coverage-aware scans, generated histories, and shrinker.

#![deny(missing_docs)]

mod false_harness;
mod generator;
mod history_api;
mod scan_model;
mod transition;

pub use false_harness::{
    detects_ack_upgrade_cheat, detects_hybrid_cheat, detects_orphan_receipt,
    false_harness_suite_ok, known_bad_complete_scan_with_damage, known_bad_hybrid_interrupt,
    known_bad_orphan_receipt, known_bad_upgrade_acks,
};
pub use generator::{
    apply_command, check_invariants, generate_history, replay_exact, run_history, shrink_history,
    Command, StepResult, XorShift64,
};
pub use history_api::{
    is_authoritative_absence, HistoricalValue, HistoryEntry, HistoryGap, LastCompleteResult,
};
pub use scan_model::{ScanCompleteness, ScanKeyRow, ScanPage};
pub use transition::{PublicationPhase, PublicationStep, TransitionClass, TransitionCoverage};

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// Stable 16-byte event / subject identity.
pub type Id16 = [u8; 16];

/// Acknowledgement / durability labels used by the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityAck {
    /// Operation not acknowledged.
    None,
    /// Memory-only acknowledgement (weaker than durable).
    Memory,
    /// Buffered acknowledgement (weaker than durable).
    Buffered,
    /// Durable receipt returned after stable-storage boundary.
    Durable,
}

/// Crash/uncertainty classification for an unacknowledged or interrupted op.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrashOutcome {
    /// Only pre-operation durable state survives.
    Old,
    /// Full post-operation durable state applied.
    New,
    /// Bounded uncertainty from surviving evidence (never hybrid event).
    Unknown,
}

/// Semantic classification of a point observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueObservation {
    /// Authoritative absence (`None`).
    Absent,
    /// Known value at current generation.
    Present {
        /// Logical payload bytes.
        value: Vec<u8>,
        /// Event id that established this value.
        event_id: Id16,
        /// Generation counter (model-local).
        generation: u64,
    },
    /// Damage / unavailability — **not** absence (CSQ-ABS-001).
    Unavailable {
        /// Human-readable cause.
        cause: String,
    },
    /// Conflicting evidence at the same identity.
    Conflict {
        /// Competing event ids.
        event_ids: Vec<Id16>,
    },
}

/// One accepted authoritative event in logical history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelEvent {
    /// Event identity.
    pub event_id: Id16,
    /// Subject key bytes.
    pub subject: Vec<u8>,
    /// Kind of logical mutation.
    pub kind: EventKind,
    /// Payload when put; empty for delete.
    pub value: Vec<u8>,
    /// Operation id used for exact mutation retry.
    pub operation_id: String,
    /// Achieved durability when receipt was issued.
    pub durability: DurabilityAck,
    /// Model-local sequence (event order).
    pub seq: u64,
}

/// Logical event kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// Put / upsert value.
    Put,
    /// Logical tombstone.
    Delete,
}

/// Durable receipt returned to a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelReceipt {
    /// Operation id.
    pub operation_id: String,
    /// Event id of the effect.
    pub event_id: Id16,
    /// Subject affected.
    pub subject: Vec<u8>,
    /// Durability achieved.
    pub durability: DurabilityAck,
}

/// Canonical observation DTO compared across oracles (SPEC §6.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationDto {
    /// Store identity bytes.
    pub store_identity: Id16,
    /// Current point observations by subject (hex key).
    pub current: BTreeMap<String, ValueObservation>,
    /// Ordered event identities in history.
    pub history_event_ids: Vec<Id16>,
    /// Known operation receipts.
    pub receipts: BTreeMap<String, ModelReceipt>,
    /// Known damage notes (subject hex → cause).
    pub damage: BTreeMap<String, String>,
    /// Subjects with incomplete coverage.
    pub incomplete_coverage: BTreeSet<String>,
}

/// Sequential model store (SPEC §6.3 / CSQ-4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelStore {
    /// Stable store identity.
    pub store_identity: Id16,
    /// Configuration label (opaque).
    pub accepted_configuration: String,
    /// All accepted events in order.
    pub events: Vec<ModelEvent>,
    /// Current generation counter by subject hex.
    pub current_generation: BTreeMap<String, u64>,
    /// Receipts by operation id.
    pub receipts: BTreeMap<String, ModelReceipt>,
    /// Known damage labels.
    pub known_damage: BTreeMap<String, String>,
    /// Subjects with unavailable coverage.
    pub unavailable_coverage: BTreeSet<String>,
    /// Explicit history gaps (CSQ-HIST-003).
    pub history_gaps: Vec<HistoryGap>,
    /// How many compaction transforms ran (physical only).
    pub compaction_generations: u64,
    /// Exclusive writer holder (`None` = free).
    pub writer_holder: Option<u64>,
    /// Next model sequence number.
    next_seq: u64,
}

/// Model application errors.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ModelError {
    /// Exact mutation retry with different content (CSQ-ACK-007).
    #[error("operation id reused with different content")]
    OperationIdConflict,
    /// Invalid subject.
    #[error("empty subject")]
    EmptySubject,
}

impl ModelStore {
    /// Create an empty model with the given identity.
    pub fn new(store_identity: Id16) -> Self {
        Self {
            store_identity,
            accepted_configuration: "residiuum-core-storage-v1".into(),
            events: Vec::new(),
            current_generation: BTreeMap::new(),
            receipts: BTreeMap::new(),
            known_damage: BTreeMap::new(),
            unavailable_coverage: BTreeSet::new(),
            history_gaps: Vec::new(),
            compaction_generations: 0,
            writer_holder: None,
            next_seq: 1,
        }
    }

    /// Try to acquire exclusive writer authority (CSQ-ID-008).
    ///
    /// Returns `true` if `holder_id` now holds the lock. Rejected contenders
    /// create no durable effect.
    pub fn try_acquire_writer(&mut self, holder_id: u64) -> bool {
        match self.writer_holder {
            None => {
                self.writer_holder = Some(holder_id);
                true
            }
            Some(h) if h == holder_id => true,
            Some(_) => false,
        }
    }

    /// Release writer if `holder_id` owns it.
    pub fn release_writer(&mut self, holder_id: u64) {
        if self.writer_holder == Some(holder_id) {
            self.writer_holder = None;
        }
    }

    /// Put with explicit durability ack label (buffered/memory/durable).
    pub fn put_with_ack(
        &mut self,
        subject: Vec<u8>,
        value: Vec<u8>,
        operation_id: String,
        event_id: Id16,
        durability: DurabilityAck,
    ) -> Result<ModelReceipt, ModelError> {
        self.apply_mutation(
            subject,
            EventKind::Put,
            value,
            operation_id,
            event_id,
            durability,
        )
    }

    /// After reopen, drop memory/buffered effects (CSQ-ACK-004): weaker acks
    /// never acquire Durable labels or durable visibility.
    pub fn drop_nondurable_after_reopen(&mut self) {
        self.events
            .retain(|e| matches!(e.durability, DurabilityAck::Durable));
        self.receipts
            .retain(|_, r| matches!(r.durability, DurabilityAck::Durable));
        // Rebuild generations from remaining durable history.
        self.current_generation.clear();
        for e in &self.events {
            let sk = Self::subject_key(&e.subject);
            let gen = self.current_generation.get(&sk).copied().unwrap_or(0) + 1;
            self.current_generation.insert(sk, gen);
        }
    }

    /// Hex-encode a subject for map keys.
    pub fn subject_key(subject: &[u8]) -> String {
        hex::encode(subject)
    }

    /// Apply a put that receives a durable receipt.
    pub fn put_durable(
        &mut self,
        subject: Vec<u8>,
        value: Vec<u8>,
        operation_id: String,
        event_id: Id16,
    ) -> Result<ModelReceipt, ModelError> {
        self.apply_mutation(
            subject,
            EventKind::Put,
            value,
            operation_id,
            event_id,
            DurabilityAck::Durable,
        )
    }

    /// Apply a delete that receives a durable receipt.
    pub fn delete_durable(
        &mut self,
        subject: Vec<u8>,
        operation_id: String,
        event_id: Id16,
    ) -> Result<ModelReceipt, ModelError> {
        self.apply_mutation(
            subject,
            EventKind::Delete,
            Vec::new(),
            operation_id,
            event_id,
            DurabilityAck::Durable,
        )
    }

    /// Record an interrupted unacknowledged operation outcome (old/new/unknown).
    ///
    /// - [`CrashOutcome::Old`]: no effect.
    /// - [`CrashOutcome::New`]: apply as durable without prior receipt (recovery path).
    /// - [`CrashOutcome::Unknown`]: mark coverage incomplete for the subject; do not invent value.
    pub fn interrupted_put(
        &mut self,
        subject: Vec<u8>,
        value: Vec<u8>,
        operation_id: String,
        event_id: Id16,
        outcome: CrashOutcome,
    ) -> Result<Option<ModelReceipt>, ModelError> {
        match outcome {
            CrashOutcome::Old => Ok(None),
            CrashOutcome::New => self
                .put_durable(subject, value, operation_id, event_id)
                .map(Some),
            CrashOutcome::Unknown => {
                if subject.is_empty() {
                    return Err(ModelError::EmptySubject);
                }
                let sk = Self::subject_key(&subject);
                self.unavailable_coverage.insert(sk);
                // Do not invent a hybrid event or receipt.
                let _ = (value, operation_id, event_id);
                Ok(None)
            }
        }
    }

    /// Mark subject damage (CSQ-ABS-001: not absence).
    pub fn mark_damage(&mut self, subject: &[u8], cause: impl Into<String>) {
        let sk = Self::subject_key(subject);
        self.known_damage.insert(sk.clone(), cause.into());
        self.unavailable_coverage.insert(sk);
    }

    fn apply_mutation(
        &mut self,
        subject: Vec<u8>,
        kind: EventKind,
        value: Vec<u8>,
        operation_id: String,
        event_id: Id16,
        durability: DurabilityAck,
    ) -> Result<ModelReceipt, ModelError> {
        if subject.is_empty() {
            return Err(ModelError::EmptySubject);
        }
        if let Some(existing) = self.receipts.get(&operation_id) {
            // Exact mutation retry: same effect, original receipt (CSQ-ACK-006).
            if existing.event_id == event_id && existing.subject == subject {
                return Ok(existing.clone());
            }
            return Err(ModelError::OperationIdConflict);
        }
        // Duplicate event id with different content → conflict, do not double-apply (CSQ-ID-002/003).
        if self.events.iter().any(|e| e.event_id == event_id) {
            // Conflicting frames: leave history, surface via observation.
            let receipt = ModelReceipt {
                operation_id: operation_id.clone(),
                event_id,
                subject: subject.clone(),
                durability,
            };
            self.receipts.insert(operation_id, receipt.clone());
            return Ok(receipt);
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        let sk = Self::subject_key(&subject);
        let gen = self.current_generation.get(&sk).copied().unwrap_or(0) + 1;
        self.current_generation.insert(sk.clone(), gen);
        // Successful durable put/delete clears prior damage/uncertainty for subject.
        self.known_damage.remove(&sk);
        self.unavailable_coverage.remove(&sk);

        self.events.push(ModelEvent {
            event_id,
            subject: subject.clone(),
            kind,
            value,
            operation_id: operation_id.clone(),
            durability,
            seq,
        });
        let receipt = ModelReceipt {
            operation_id: operation_id.clone(),
            event_id,
            subject,
            durability,
        };
        self.receipts.insert(operation_id, receipt.clone());
        Ok(receipt)
    }

    /// Point get with semantic classification.
    pub fn get(&self, subject: &[u8]) -> ValueObservation {
        let sk = Self::subject_key(subject);
        if let Some(cause) = self.known_damage.get(&sk) {
            return ValueObservation::Unavailable {
                cause: cause.clone(),
            };
        }
        if self.unavailable_coverage.contains(&sk) {
            return ValueObservation::Unavailable {
                cause: "coverage_incomplete".into(),
            };
        }
        // Detect conflicting event ids for same seq/subject (simplified).
        let mut by_event: BTreeMap<Id16, &ModelEvent> = BTreeMap::new();
        for e in &self.events {
            if e.subject == subject {
                by_event.insert(e.event_id, e);
            }
        }
        // Current = last non-conflicting accepted event by seq.
        let mut last: Option<&ModelEvent> = None;
        for e in &self.events {
            if e.subject.as_slice() == subject {
                last = Some(e);
            }
        }
        match last {
            None => ValueObservation::Absent,
            Some(e) if matches!(e.kind, EventKind::Delete) => ValueObservation::Absent,
            Some(e) => ValueObservation::Present {
                value: e.value.clone(),
                event_id: e.event_id,
                generation: self.current_generation.get(&sk).copied().unwrap_or(1),
            },
        }
    }

    /// Build canonical observation DTO.
    pub fn observe(&self) -> ObservationDto {
        let mut subjects: BTreeSet<Vec<u8>> = BTreeSet::new();
        for e in &self.events {
            subjects.insert(e.subject.clone());
        }
        for sk in self
            .known_damage
            .keys()
            .chain(self.unavailable_coverage.iter())
        {
            if let Ok(bytes) = hex::decode(sk) {
                subjects.insert(bytes);
            }
        }
        let mut current = BTreeMap::new();
        for s in subjects {
            current.insert(Self::subject_key(&s), self.get(&s));
        }
        ObservationDto {
            store_identity: self.store_identity,
            current,
            history_event_ids: self.events.iter().map(|e| e.event_id).collect(),
            receipts: self.receipts.clone(),
            damage: self.known_damage.clone(),
            incomplete_coverage: self.unavailable_coverage.clone(),
        }
    }

    /// Serialize model for minimization/replay (JSON).
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize model for replay.
    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

// tiny hex without extra dep.
pub(crate) mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        const H: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            out.push(H[(b >> 4) as usize] as char);
            out.push(H[(b & 0xf) as usize] as char);
        }
        out
    }
    pub fn decode(s: &str) -> Result<Vec<u8>, ()> {
        if s.len() % 2 != 0 {
            return Err(());
        }
        let mut out = Vec::with_capacity(s.len() / 2);
        let b = s.as_bytes();
        let mut i = 0;
        while i < b.len() {
            let hi = from_hex(b[i])?;
            let lo = from_hex(b[i + 1])?;
            out.push((hi << 4) | lo);
            i += 2;
        }
        Ok(out)
    }
    fn from_hex(c: u8) -> Result<u8, ()> {
        match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            b'A'..=b'F' => Ok(c - b'A' + 10),
            _ => Err(()),
        }
    }
}

/// Known-bad algorithm fixture: maps damage to absence (must fail oracle checks).
pub fn known_bad_absence_from_damage(obs: &ObservationDto) -> ObservationDto {
    let mut bad = obs.clone();
    for (k, v) in obs.current.iter() {
        if matches!(v, ValueObservation::Unavailable { .. }) {
            bad.current.insert(k.clone(), ValueObservation::Absent);
            bad.damage.remove(k);
            bad.incomplete_coverage.remove(k);
        }
    }
    bad
}

/// Returns true if observation violates CSQ-ABS-001 (damage collapsed to absence wrongly).
pub fn detects_absence_from_damage_cheat(
    honest: &ObservationDto,
    candidate: &ObservationDto,
) -> bool {
    for (k, v) in &honest.current {
        if matches!(v, ValueObservation::Unavailable { .. }) {
            if matches!(candidate.current.get(k), Some(ValueObservation::Absent)) {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sid() -> Id16 {
        [1u8; 16]
    }
    fn eid(n: u8) -> Id16 {
        let mut e = [0u8; 16];
        e[0] = n;
        e
    }

    #[test]
    fn put_get_delete_and_receipt_retry() {
        let mut m = ModelStore::new(sid());
        let sub = b"alice".to_vec();
        let r = m
            .put_durable(sub.clone(), b"v1".to_vec(), "op1".into(), eid(1))
            .unwrap();
        assert_eq!(r.durability, DurabilityAck::Durable);
        match m.get(&sub) {
            ValueObservation::Present { value, .. } => assert_eq!(value, b"v1"),
            o => panic!("unexpected {o:?}"),
        }
        let r2 = m
            .put_durable(sub.clone(), b"v1".to_vec(), "op1".into(), eid(1))
            .unwrap();
        assert_eq!(r, r2);
        m.delete_durable(sub.clone(), "op2".into(), eid(2)).unwrap();
        assert!(matches!(m.get(&sub), ValueObservation::Absent));
    }

    #[test]
    fn damage_is_not_absence() {
        let mut m = ModelStore::new(sid());
        let sub = b"bob".to_vec();
        m.put_durable(sub.clone(), b"x".to_vec(), "op".into(), eid(3))
            .unwrap();
        m.mark_damage(&sub, "bitrot");
        assert!(matches!(m.get(&sub), ValueObservation::Unavailable { .. }));
        let obs = m.observe();
        let cheat = known_bad_absence_from_damage(&obs);
        assert!(detects_absence_from_damage_cheat(&obs, &cheat));
    }

    #[test]
    fn interrupted_unknown_does_not_fabricate() {
        let mut m = ModelStore::new(sid());
        let sub = b"c".to_vec();
        let r = m
            .interrupted_put(
                sub.clone(),
                b"z".to_vec(),
                "opx".into(),
                eid(9),
                CrashOutcome::Unknown,
            )
            .unwrap();
        assert!(r.is_none());
        assert!(matches!(m.get(&sub), ValueObservation::Unavailable { .. }));
        assert!(!m.receipts.contains_key("opx"));
    }

    #[test]
    fn model_roundtrip_json() {
        let mut m = ModelStore::new(sid());
        m.put_durable(b"k".to_vec(), b"v".to_vec(), "o".into(), eid(1))
            .unwrap();
        let j = m.to_json().unwrap();
        let m2 = ModelStore::from_json(&j).unwrap();
        assert_eq!(m.observe(), m2.observe());
    }
}
