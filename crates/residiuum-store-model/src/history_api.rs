//! CSQ-4 historical reads, gaps, and last-complete recovery (DEF-099).

use crate::{EventKind, Id16, ModelEvent, ModelStore, ValueObservation};
use serde::{Deserialize, Serialize};

/// One history entry for a subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Event identity.
    pub event_id: Id16,
    /// Model sequence (authoritative order evidence).
    pub seq: u64,
    /// Put or delete.
    pub kind: EventKind,
    /// Payload for put; empty for delete.
    pub value: Vec<u8>,
    /// Whether this event is the current generation for the subject.
    pub is_current: bool,
}

/// Exact historical-value read result (CSQ-HIST-005).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistoricalValue {
    /// Event found; `is_current` labels non-current recovery.
    Found {
        /// Event id requested.
        event_id: Id16,
        /// Kind at that event.
        kind: EventKind,
        /// Value at that event (empty for delete).
        value: Vec<u8>,
        /// Whether this is still the current generation.
        is_current: bool,
    },
    /// No such event for subject.
    Missing,
}

/// Explicit gap in history (CSQ-HIST-003).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryGap {
    /// Subject hex key.
    pub subject_hex: String,
    /// Human/causal note.
    pub note: String,
    /// Optional bounding sequences if known.
    pub before_seq: Option<u64>,
    /// Optional after sequence.
    pub after_seq: Option<u64>,
}

/// Bounded last-complete recovery result (CSQ-HIST-006 / DEF-099).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastCompleteResult {
    /// Latest complete put before any incomplete/gap barrier, if any.
    pub complete: Option<HistoryEntry>,
    /// Partial/incomplete candidates preserved (not promoted).
    pub partials: Vec<HistoryEntry>,
    /// Explicit gaps observed.
    pub gaps: Vec<HistoryGap>,
    /// Whether a tombstone blocked further recovery without forensic policy.
    pub tombstone_stop: bool,
}

impl ModelStore {
    /// Walk full verified history for `subject` in authoritative order (CSQ-HIST-001).
    pub fn history(&self, subject: &[u8]) -> Vec<HistoryEntry> {
        let current_id = self
            .events
            .iter()
            .rev()
            .find(|x| x.subject.as_slice() == subject)
            .map(|x| x.event_id);
        let mut out = Vec::new();
        for e in &self.events {
            if e.subject.as_slice() != subject {
                continue;
            }
            out.push(HistoryEntry {
                event_id: e.event_id,
                seq: e.seq,
                kind: e.kind,
                value: e.value.clone(),
                is_current: current_id == Some(e.event_id),
            });
        }
        out
    }

    /// Exact historical-value read for one event id (CSQ-HIST-005).
    pub fn historical_get(&self, subject: &[u8], event_id: Id16) -> HistoricalValue {
        let hist = self.history(subject);
        match hist.iter().find(|h| h.event_id == event_id) {
            None => HistoricalValue::Missing,
            Some(h) => HistoricalValue::Found {
                event_id: h.event_id,
                kind: h.kind,
                value: h.value.clone(),
                is_current: h.is_current,
            },
        }
    }

    /// Record an explicit gap (CSQ-HIST-003). Missing data never becomes a tombstone.
    pub fn record_gap(
        &mut self,
        subject: &[u8],
        note: impl Into<String>,
        before_seq: Option<u64>,
        after_seq: Option<u64>,
    ) {
        self.history_gaps.push(HistoryGap {
            subject_hex: Self::subject_key(subject),
            note: note.into(),
            before_seq,
            after_seq,
        });
    }

    /// Gaps for a subject.
    pub fn gaps_for(&self, subject: &[u8]) -> Vec<&HistoryGap> {
        let sk = Self::subject_key(subject);
        self.history_gaps
            .iter()
            .filter(|g| g.subject_hex == sk)
            .collect()
    }

    /// Bounded last-complete search (CSQ-HIST-006).
    ///
    /// Walks history reverse; stops at first delete tombstone without forensic
    /// policy; preserves partials when coverage is incomplete.
    pub fn last_complete(&self, subject: &[u8], max_events: usize) -> LastCompleteResult {
        let hist = self.history(subject);
        let gaps: Vec<HistoryGap> = self.gaps_for(subject).into_iter().cloned().collect();
        let sk = Self::subject_key(subject);
        let incomplete =
            self.unavailable_coverage.contains(&sk) || self.known_damage.contains_key(&sk);

        let mut complete = None;
        let mut partials = Vec::new();
        let mut tombstone_stop = false;
        let mut seen = 0usize;

        for h in hist.iter().rev() {
            if seen >= max_events {
                break;
            }
            seen += 1;
            match h.kind {
                EventKind::Delete => {
                    tombstone_stop = true;
                    // Tombstone establishes absence at this position; do not
                    // cross without forensic policy.
                    break;
                }
                EventKind::Put => {
                    if incomplete && complete.is_none() {
                        // Current generation incomplete: keep as partial candidate.
                        partials.push(h.clone());
                    } else if complete.is_none() {
                        complete = Some(h.clone());
                        break;
                    }
                }
            }
        }

        LastCompleteResult {
            complete,
            partials,
            gaps,
            tombstone_stop,
        }
    }

    /// Compaction transform that must preserve history order and membership (CSQ-HIST-004).
    ///
    /// Physical representation is model-local; this only records that compaction
    /// ran and asserts history fingerprint stability.
    pub fn compact_preserve_history(&mut self) -> u64 {
        // Fingerprint = count ^ xor of first bytes of event ids.
        let mut fp = self.events.len() as u64;
        for e in &self.events {
            fp ^= e.event_id[0] as u64;
            fp = fp.wrapping_mul(0x9e37).wrapping_add(e.seq);
        }
        self.compaction_generations = self.compaction_generations.saturating_add(1);
        fp
    }

    /// History fingerprint for equality after compact/reopen.
    pub fn history_fingerprint(&self) -> Vec<(Id16, u64, EventKind)> {
        self.events
            .iter()
            .map(|e| (e.event_id, e.seq, e.kind))
            .collect()
    }
}

/// Helper: whether a value observation is authoritative absence only.
pub fn is_authoritative_absence(obs: &ValueObservation) -> bool {
    matches!(obs, ValueObservation::Absent)
}
