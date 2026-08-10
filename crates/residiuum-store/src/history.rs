//! Per-subject event history (OVERVIEW §5.4, DX_SPEC §10.1).
//!
//! History is rebuilt from authoritative segment scans. Derived indexes never
//! invent events that do not exist as verified frames.
//!
//! DEF-099 adds exact historical payload reconstruction and last-complete
//! recovery search types (used by [`crate::Store`] recovery methods).

use crate::chunk_payload::PayloadResult;
use crate::envelope::EventKind;
use crate::error::StoreError;
use crate::layout::StorePaths;
use crate::store::{cmp_disk_events_pub, collect_item_events_for_history, DiskEventPub};
use crate::tier::TierPlacement;
use residiuum_format::SafetyLimits;
use std::collections::HashSet;

/// Bounds on historical reconstruction work (DEF-099).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBudget {
    /// Maximum history events examined during a search (0 = unlimited).
    pub max_events_examined: usize,
    /// Soft byte budget for reconstructed body bytes counted (0 = unlimited).
    pub max_bytes_examined: u64,
}

impl Default for ReadBudget {
    fn default() -> Self {
        Self {
            max_events_examined: 4096,
            max_bytes_examined: 64 * 1024 * 1024,
        }
    }
}

impl ReadBudget {
    /// Unbounded search budget (still deterministic and read-only).
    pub fn unlimited() -> Self {
        Self {
            max_events_examined: 0,
            max_bytes_examined: 0,
        }
    }
}

/// Upper bound for last-complete search (exclusive of the bound event).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeforeEvent {
    /// Search predecessors of the current primary-index event (if any).
    Current,
    /// Search events strictly older than this `event_id` in recovery order.
    EventId([u8; 16]),
}

/// Options for [`crate::Store::find_last_complete_version`] (DEF-099).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReadOptions {
    /// When true, continue past a delete tombstone (forensic; result labelled).
    pub cross_tombstone: bool,
    /// Work budget for the search.
    pub budget: ReadBudget,
}

impl Default for RecoveryReadOptions {
    fn default() -> Self {
        Self {
            cross_tombstone: false,
            budget: ReadBudget::default(),
        }
    }
}

/// Exact historical (or current) payload projection (DEF-099).
///
/// Never mutates store authority. `selected` is `None` for tombstone deletes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedPayloadResult {
    /// Subject key bytes.
    pub subject: Vec<u8>,
    /// Selected historical event id.
    pub selected_event_id: [u8; 16],
    /// Item lineage of the selected event.
    pub selected_item_id: [u8; 16],
    /// Segment of the selected event frame.
    pub selected_segment_id: [u8; 16],
    /// Put or delete.
    pub selected_kind: EventKind,
    /// Current primary-index event id when the subject is live or deleted.
    pub current_event_id: Option<[u8; 16]>,
    /// Current payload completeness label when live (`complete` / `partial` / …).
    pub current_completeness: Option<&'static str>,
    /// Reconstructed payload for puts; `None` for deletes (tombstone).
    pub selected: Option<PayloadResult>,
    /// True when the selected event is a delete.
    pub is_tombstone: bool,
    /// History stream reported a gap before this event.
    pub known_gap_before: bool,
    /// True when subject history reported no salvage holes.
    pub history_coverage_complete: bool,
    /// True when the search path crossed a delete boundary (forensic only).
    pub tombstone_crossed: bool,
    /// Events examined to produce this result.
    pub events_examined: usize,
    /// Body bytes counted toward the read budget.
    pub bytes_examined: u64,
}

/// Result of a last-complete historical search (DEF-099).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalSearchResult {
    /// Newest permitted complete put, if any.
    pub found: Option<VersionedPayloadResult>,
    /// Incomplete/conflicting puts examined before finding a complete one.
    pub incomplete_candidates: usize,
    /// Search stopped at a delete because `cross_tombstone` was false.
    pub tombstone_stopped: bool,
    /// Search stopped because the read budget was exhausted.
    pub budget_exhausted: bool,
    /// History stream has known holes.
    pub history_coverage_complete: bool,
    /// Total events examined.
    pub events_examined: usize,
    /// Total body bytes counted.
    pub bytes_examined: u64,
}

/// One immutable storage event for a subject key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryEvent {
    /// Event kind (`put` or `delete`).
    pub kind: EventKind,
    /// Item lineage identifier.
    pub item_id: [u8; 16],
    /// Unique event identifier.
    pub event_id: [u8; 16],
    /// Segment that holds the frame.
    pub segment_id: [u8; 16],
    /// Writer-local sequence within the segment.
    pub writer_sequence: u64,
    /// Byte offset of the frame within the segment file.
    pub offset: u64,
    /// Payload body for puts (empty for deletes). May be a chunk manifest.
    pub body: Vec<u8>,
    /// Whether a hole was observed in the scan path that produced this stream
    /// (advisory; individual events remain verified).
    pub known_gap_before: bool,
}

/// Full history stream for one subject (recovery order).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectHistory {
    /// Subject bytes as stored.
    pub subject: Vec<u8>,
    /// Events in recovery order (oldest first).
    pub events: Vec<HistoryEvent>,
    /// True when the salvage scan reported any hole in any scanned segment.
    /// Callers MUST NOT treat the stream as a gap-free complete history when set.
    pub has_known_holes: bool,
}

/// Collect history for `subject` by scanning all available segment files.
#[allow(dead_code)] // hot-only helper retained for salvage tools
pub fn subject_history(
    paths: &StorePaths,
    limits: SafetyLimits,
    subject: &[u8],
) -> Result<SubjectHistory, StoreError> {
    subject_history_tiered(paths, limits, subject, None)
}

/// Collect history scanning only available tiers when `placement` is set.
pub fn subject_history_tiered(
    paths: &StorePaths,
    limits: SafetyLimits,
    subject: &[u8],
    placement: Option<&TierPlacement>,
) -> Result<SubjectHistory, StoreError> {
    let (mut events, has_known_holes) =
        collect_subject_disk_events(paths, limits, subject, placement)?;
    events.sort_by(cmp_disk_events_pub);

    let mut seen: HashSet<[u8; 16]> = HashSet::new();
    let mut out = Vec::new();
    let mut gap_before = false;
    for ev in events {
        if !seen.insert(ev.event_id) {
            // Duplicate physical copy of the same event_id: keep first.
            continue;
        }
        out.push(HistoryEvent {
            kind: ev.kind,
            item_id: ev.item_id,
            event_id: ev.event_id,
            segment_id: ev.segment_id,
            writer_sequence: ev.writer_sequence,
            offset: ev.offset,
            body: ev.body,
            known_gap_before: gap_before,
        });
        // After first event, subsequent events may sit after holes elsewhere;
        // we only set known_gap_before on the stream-level flag, not per event
        // except the stream-level has_known_holes disclosure.
        let _ = gap_before;
        gap_before = has_known_holes;
    }

    Ok(SubjectHistory {
        subject: subject.to_vec(),
        events: out,
        has_known_holes,
    })
}

fn collect_subject_disk_events(
    paths: &StorePaths,
    limits: SafetyLimits,
    subject: &[u8],
    placement: Option<&TierPlacement>,
) -> Result<(Vec<DiskEventPub>, bool), StoreError> {
    let (all, has_holes) = collect_item_events_for_history(paths, limits, placement)?;
    let filtered: Vec<_> = all
        .into_iter()
        .filter(|e| e.subject.as_slice() == subject)
        .collect();
    Ok((filtered, has_holes))
}
