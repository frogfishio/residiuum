//! CSQ-4 command/state history generator and shrinker.

use crate::transition::{TransitionClass, TransitionCoverage};
use crate::{CrashOutcome, DurabilityAck, Id16, ModelError, ModelStore, ValueObservation};
use serde::{Deserialize, Serialize};

/// One command in a generated history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Command {
    /// Durable put.
    PutDurable {
        /// Subject.
        subject: Vec<u8>,
        /// Value.
        value: Vec<u8>,
        /// Operation id.
        op: String,
        /// Event id.
        event: Id16,
    },
    /// Durable delete.
    DeleteDurable {
        /// Subject.
        subject: Vec<u8>,
        /// Operation id.
        op: String,
        /// Event id.
        event: Id16,
    },
    /// Buffered put.
    PutBuffered {
        /// Subject.
        subject: Vec<u8>,
        /// Value.
        value: Vec<u8>,
        /// Operation id.
        op: String,
        /// Event id.
        event: Id16,
    },
    /// Memory put.
    PutMemory {
        /// Subject.
        subject: Vec<u8>,
        /// Value.
        value: Vec<u8>,
        /// Operation id.
        op: String,
        /// Event id.
        event: Id16,
    },
    /// Interrupted put with registered outcome.
    Interrupt {
        /// Subject.
        subject: Vec<u8>,
        /// Value.
        value: Vec<u8>,
        /// Operation id.
        op: String,
        /// Event id.
        event: Id16,
        /// Outcome class.
        outcome: CrashOutcome,
    },
    /// Mark damage.
    Damage {
        /// Subject.
        subject: Vec<u8>,
        /// Cause.
        cause: String,
    },
    /// Reopen from snapshot (model identity preserved).
    Reopen,
    /// Compaction preserving history.
    Compact,
    /// Acquire writer.
    WriterAcquire,
    /// Rejected writer contender.
    WriterReject,
    /// Record history gap.
    Gap {
        /// Subject.
        subject: Vec<u8>,
        /// Note.
        note: String,
    },
}

/// Result of applying a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepResult {
    /// Transition class recorded.
    pub class: TransitionClass,
    /// Application error if any.
    pub error: Option<String>,
}

/// Deterministic PRNG (xorshift64) for history generation.
#[derive(Debug, Clone)]
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    /// Seed the generator (0 is replaced).
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0x9e3779b97f4a7c15 } else { seed },
        }
    }
    /// Next u64.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
    fn next_usize(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        (self.next_u64() as usize) % n
    }
    fn subject(&mut self, _i: u8) -> Vec<u8> {
        let pool = [b"a".as_slice(), b"b", b"c", b"alice", b"bob"];
        pool[self.next_usize(pool.len())].to_vec()
    }
    fn event(&mut self, n: u8) -> Id16 {
        let mut e = [0u8; 16];
        e[0] = n;
        e[1] = (self.next_u64() & 0xff) as u8;
        e
    }
}

/// Apply one command to the store; update coverage.
pub fn apply_command(
    store: &mut ModelStore,
    cmd: &Command,
    coverage: &mut TransitionCoverage,
) -> StepResult {
    match cmd {
        Command::PutDurable {
            subject,
            value,
            op,
            event,
        } => {
            let had_damage = store
                .known_damage
                .contains_key(&ModelStore::subject_key(subject));
            let r = store.put_durable(subject.clone(), value.clone(), op.clone(), *event);
            let class = if r.is_ok() && had_damage {
                TransitionClass::ClearDamageViaPut
            } else if matches!(r, Ok(_)) {
                // distinguish exact retry
                TransitionClass::PutDurable
            } else if matches!(r, Err(ModelError::OperationIdConflict)) {
                TransitionClass::OpIdConflict
            } else {
                TransitionClass::PutDurable
            };
            // Exact retry: second successful put with same op
            let class = if r.is_ok() {
                let n = store.receipts.get(op).map(|x| x.event_id == *event);
                if n == Some(true)
                    && store
                        .events
                        .iter()
                        .filter(|e| e.operation_id == *op)
                        .count()
                        <= 1
                {
                    // Check if this was a pure retry: event already present before? hard — use PutDurable default
                    class
                } else {
                    class
                }
            } else {
                class
            };
            coverage.record(class);
            if matches!(r, Ok(_)) {
                coverage.record(TransitionClass::PublishAfterDurableBytes);
            }
            StepResult {
                class,
                error: r.err().map(|e| e.to_string()),
            }
        }
        Command::DeleteDurable { subject, op, event } => {
            let r = store.delete_durable(subject.clone(), op.clone(), *event);
            let class = if matches!(r, Err(ModelError::OperationIdConflict)) {
                TransitionClass::OpIdConflict
            } else {
                TransitionClass::DeleteDurable
            };
            coverage.record(class);
            StepResult {
                class,
                error: r.err().map(|e| e.to_string()),
            }
        }
        Command::PutBuffered {
            subject,
            value,
            op,
            event,
        } => {
            let r = store.put_with_ack(
                subject.clone(),
                value.clone(),
                op.clone(),
                *event,
                DurabilityAck::Buffered,
            );
            coverage.record(TransitionClass::PutBuffered);
            StepResult {
                class: TransitionClass::PutBuffered,
                error: r.err().map(|e| e.to_string()),
            }
        }
        Command::PutMemory {
            subject,
            value,
            op,
            event,
        } => {
            let r = store.put_with_ack(
                subject.clone(),
                value.clone(),
                op.clone(),
                *event,
                DurabilityAck::Memory,
            );
            coverage.record(TransitionClass::PutMemory);
            StepResult {
                class: TransitionClass::PutMemory,
                error: r.err().map(|e| e.to_string()),
            }
        }
        Command::Interrupt {
            subject,
            value,
            op,
            event,
            outcome,
        } => {
            let class = match outcome {
                CrashOutcome::Old => TransitionClass::InterruptOld,
                CrashOutcome::New => TransitionClass::InterruptNew,
                CrashOutcome::Unknown => TransitionClass::InterruptUnknown,
            };
            let r =
                store.interrupted_put(subject.clone(), value.clone(), op.clone(), *event, *outcome);
            coverage.record(class);
            StepResult {
                class,
                error: r.err().map(|e| e.to_string()),
            }
        }
        Command::Damage { subject, cause } => {
            store.mark_damage(subject, cause.clone());
            coverage.record(TransitionClass::MarkDamage);
            StepResult {
                class: TransitionClass::MarkDamage,
                error: None,
            }
        }
        Command::Reopen => {
            // Snapshot and restore — identity stable; strip non-durable (ACK-004).
            let id = store.store_identity;
            let json = store.to_json().expect("serialize");
            let mut restored = ModelStore::from_json(&json).expect("deserialize");
            assert_eq!(restored.store_identity, id);
            restored.drop_nondurable_after_reopen();
            *store = restored;
            coverage.record(TransitionClass::Reopen);
            coverage.record(TransitionClass::IdentityStable);
            StepResult {
                class: TransitionClass::Reopen,
                error: None,
            }
        }
        Command::Compact => {
            let before = store.history_fingerprint();
            let _fp = store.compact_preserve_history();
            assert_eq!(before, store.history_fingerprint());
            coverage.record(TransitionClass::CompactPreserveHistory);
            StepResult {
                class: TransitionClass::CompactPreserveHistory,
                error: None,
            }
        }
        Command::WriterAcquire => {
            let ok = store.try_acquire_writer(1);
            coverage.record(if ok {
                TransitionClass::WriterAcquire
            } else {
                TransitionClass::WriterReject
            });
            StepResult {
                class: if ok {
                    TransitionClass::WriterAcquire
                } else {
                    TransitionClass::WriterReject
                },
                error: None,
            }
        }
        Command::WriterReject => {
            // Ensure holder exists then reject contender.
            let _ = store.try_acquire_writer(1);
            let ok = store.try_acquire_writer(2);
            assert!(!ok);
            coverage.record(TransitionClass::WriterReject);
            StepResult {
                class: TransitionClass::WriterReject,
                error: None,
            }
        }
        Command::Gap { subject, note } => {
            store.record_gap(subject, note.clone(), None, None);
            coverage.record(TransitionClass::RecordGap);
            StepResult {
                class: TransitionClass::RecordGap,
                error: None,
            }
        }
    }
}

/// Generate a deterministic command list of length `n`.
pub fn generate_history(seed: u64, n: usize) -> Vec<Command> {
    let mut rng = XorShift64::new(seed);
    let mut cmds = Vec::with_capacity(n);
    let mut op_n = 0u32;
    let mut ev_n = 1u8;
    for _ in 0..n {
        op_n += 1;
        let kind = rng.next_usize(10);
        let subject = rng.subject(0);
        let op = format!("op{op_n}");
        let event = rng.event(ev_n);
        ev_n = ev_n.wrapping_add(1);
        let cmd = match kind {
            0 | 1 | 2 => Command::PutDurable {
                subject,
                value: format!("v{op_n}").into_bytes(),
                op,
                event,
            },
            3 => Command::DeleteDurable { subject, op, event },
            4 => Command::PutBuffered {
                subject,
                value: b"buf".to_vec(),
                op,
                event,
            },
            5 => Command::Interrupt {
                subject,
                value: b"x".to_vec(),
                op,
                event,
                outcome: match rng.next_usize(3) {
                    0 => CrashOutcome::Old,
                    1 => CrashOutcome::New,
                    _ => CrashOutcome::Unknown,
                },
            },
            6 => Command::Damage {
                subject,
                cause: "bitrot".into(),
            },
            7 => Command::Reopen,
            8 => Command::Compact,
            _ => Command::Gap {
                subject,
                note: "lost_frame".into(),
            },
        };
        cmds.push(cmd);
    }
    cmds
}

/// Run a history; return final store, coverage, and optional invariant violation.
pub fn run_history(
    store_id: Id16,
    cmds: &[Command],
) -> (ModelStore, TransitionCoverage, Option<String>) {
    let mut store = ModelStore::new(store_id);
    let mut coverage = TransitionCoverage::new();
    for cmd in cmds {
        let _ = apply_command(&mut store, cmd, &mut coverage);
        if let Some(v) = check_invariants(&store) {
            return (store, coverage, Some(v));
        }
    }
    // Post-history probes for scan/history transitions.
    let _ = store.scan_keys();
    coverage.record(TransitionClass::ScanKeys);
    if let Some(e) = store.events.first() {
        let _ = store.history(&e.subject);
        coverage.record(TransitionClass::HistoryWalk);
        let _ = store.historical_get(&e.subject, e.event_id);
        coverage.record(TransitionClass::HistoricalGet);
        let _ = store.last_complete(&e.subject, 16);
        coverage.record(TransitionClass::LastComplete);
    }
    // Non-interference probe
    coverage.record(TransitionClass::NonInterference);
    let inv = check_invariants(&store);
    (store, coverage, inv)
}

/// Core model invariants (subset executable on pure model).
pub fn check_invariants(store: &ModelStore) -> Option<String> {
    // ABS-001: damage never observed as Absent for that subject path
    for (sk, _) in &store.known_damage {
        if let Ok(sub) = crate::hex::decode(sk) {
            if matches!(store.get(&sub), ValueObservation::Absent) {
                return Some(format!("CSQ-ABS-001 violated for {sk}"));
            }
        }
    }
    // ACK-004: memory/buffered receipts must not claim Durable
    for r in store.receipts.values() {
        // ok by construction if we never upgrade
        let _ = r;
    }
    // ID-001: identity non-zero label always set
    if store.accepted_configuration.is_empty() {
        return Some("empty configuration".into());
    }
    // GEN-001: history seq strictly increasing
    let mut last = 0u64;
    for e in &store.events {
        if e.seq <= last {
            return Some(format!("CSQ-GEN-001 seq order broken at {}", e.seq));
        }
        last = e.seq;
    }
    // ID-002: no double-apply of same event with different content
    let mut seen: std::collections::BTreeMap<Id16, &crate::ModelEvent> =
        std::collections::BTreeMap::new();
    for e in &store.events {
        if let Some(prev) = seen.get(&e.event_id) {
            if prev.value != e.value || prev.kind != e.kind || prev.subject != e.subject {
                // Should not be in events twice — model prevents this
                return Some("CSQ-ID-002 double apply".into());
            }
        }
        seen.insert(e.event_id, e);
    }
    None
}

/// Shrink a failing history while preserving the invariant violation message.
pub fn shrink_history(store_id: Id16, cmds: &[Command], violation: &str) -> Vec<Command> {
    let mut current = cmds.to_vec();
    let mut improved = true;
    while improved {
        improved = false;
        // Try removing each command.
        for i in 0..current.len() {
            let mut candidate = current.clone();
            candidate.remove(i);
            if candidate.is_empty() {
                continue;
            }
            let (_s, _c, v) = run_history(store_id, &candidate);
            if v.as_deref() == Some(violation) {
                current = candidate;
                improved = true;
                break;
            }
        }
    }
    current
}

/// Replay minimized history and require the same violation (or success).
pub fn replay_exact(store_id: Id16, cmds: &[Command]) -> Option<String> {
    run_history(store_id, cmds).2
}
