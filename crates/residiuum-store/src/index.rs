//! Rebuildable current-state projection (OVERVIEW §5.5, §4.5).
//!
//! Derived only — never the sole map of surviving data.
//!
//! ## Memory model (DEF-095)
//!
//! Durable puts keep **locators** (`segment_id` + `frame_offset`) in the primary
//! index. Full payload bodies are **not** retained for ordinary durable values;
//! [`crate::Store::get`] reloads the item frame on demand. Memory-mode publishes
//! and small chunk manifests remain resident (no durable frame, or body is the
//! tiny manifest). This keeps process RSS O(keys × metadata) instead of
//! O(dataset size).

use crate::envelope::EventKind;
use std::ops::Bound;

/// Live value for a subject after applying surviving put/delete events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveValue {
    /// Resident payload bytes, if any.
    ///
    /// Empty for ordinary durable puts (body lives in the segment frame at
    /// [`Self::frame_offset`]). Non-empty for memory-mode visibility, chunk
    /// manifests, and legacy fat index caches.
    pub body: Vec<u8>,
    /// Item id lineage for this subject.
    pub item_id: [u8; 16],
    /// Event id of the latest event that established this state.
    pub event_id: [u8; 16],
    /// Segment that holds the establishing event.
    pub segment_id: [u8; 16],
    /// Writer-local sequence of the establishing event (diagnostic).
    pub writer_sequence: u64,
    /// Byte offset of the establishing item frame in `segment_id` (0 when none).
    pub frame_offset: u64,
}

/// Index entry: either a live put or a delete tombstone (get returns None).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexEntry {
    /// Subject has a current payload.
    Live(LiveValue),
    /// Subject was deleted; get returns absence.
    Deleted {
        /// Item id lineage.
        item_id: [u8; 16],
        /// Delete event id.
        event_id: [u8; 16],
        /// Segment of the delete event.
        segment_id: [u8; 16],
        /// Writer sequence of the delete.
        writer_sequence: u64,
        /// Byte offset of the delete frame (0 when none / memory-mode).
        frame_offset: u64,
    },
}

impl IndexEntry {
    /// Current live body, if any **and** resident in the index.
    ///
    /// Does not load from segments. Prefer `Store::get` for logical payloads.
    pub fn live_body(&self) -> Option<&[u8]> {
        match self {
            Self::Live(v) => {
                if v.body.is_empty() {
                    None
                } else {
                    Some(v.body.as_slice())
                }
            }
            Self::Deleted { .. } => None,
        }
    }

    /// Item id for this subject lineage.
    pub fn item_id(&self) -> [u8; 16] {
        match self {
            Self::Live(v) => v.item_id,
            Self::Deleted { item_id, .. } => *item_id,
        }
    }

    /// Segment of the establishing event.
    pub fn segment_id(&self) -> [u8; 16] {
        match self {
            Self::Live(v) => v.segment_id,
            Self::Deleted { segment_id, .. } => *segment_id,
        }
    }

    /// Frame offset of the establishing event.
    pub fn frame_offset(&self) -> u64 {
        match self {
            Self::Live(v) => v.frame_offset,
            Self::Deleted { frame_offset, .. } => *frame_offset,
        }
    }
}

/// Subject → current projection. Ordered map for deterministic rebuild tests.
#[derive(Debug, Clone, Default)]
pub struct PrimaryIndex {
    map: std::collections::BTreeMap<Vec<u8>, IndexEntry>,
}

impl PrimaryIndex {
    /// Empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of subjects with any recorded state (including deleted).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Number of live (non-deleted) subjects.
    pub fn live_len(&self) -> usize {
        self.live_entries().count()
    }

    /// Sum of resident body bytes (excludes locator-only entries).
    pub fn resident_body_bytes(&self) -> u64 {
        self.map
            .values()
            .map(|e| match e {
                IndexEntry::Live(lv) => lv.body.len() as u64,
                IndexEntry::Deleted { .. } => 0,
            })
            .sum()
    }

    /// Lookup by subject bytes.
    pub fn get(&self, subject: &[u8]) -> Option<&IndexEntry> {
        self.map.get(subject)
    }

    /// Resident live body only (not a disk fetch).
    pub fn get_live(&self, subject: &[u8]) -> Option<&[u8]> {
        self.map.get(subject).and_then(|e| e.live_body())
    }

    /// Install resident bytes in a temporary derived projection.
    ///
    /// Compaction uses this for committed Atomic values whose authoritative
    /// payload lives behind a Heap-qualified staging locator rather than an
    /// ordinary item-frame locator. It does not alter identity or ordering.
    pub(crate) fn set_resident_body(&mut self, subject: &[u8], body: Vec<u8>) -> bool {
        let Some(IndexEntry::Live(value)) = self.map.get_mut(subject) else {
            return false;
        };
        value.body = body;
        true
    }

    /// Apply one verified item event in recovery order.
    #[allow(clippy::too_many_arguments)] // event identity fields stay explicit
    pub fn apply_event(
        &mut self,
        subject: Vec<u8>,
        kind: EventKind,
        body: Vec<u8>,
        item_id: [u8; 16],
        event_id: [u8; 16],
        segment_id: [u8; 16],
        writer_sequence: u64,
        frame_offset: u64,
    ) {
        match kind {
            EventKind::Put => {
                self.map.insert(
                    subject,
                    IndexEntry::Live(LiveValue {
                        body,
                        item_id,
                        event_id,
                        segment_id,
                        writer_sequence,
                        frame_offset,
                    }),
                );
            }
            EventKind::Delete => {
                self.map.insert(
                    subject,
                    IndexEntry::Deleted {
                        item_id,
                        event_id,
                        segment_id,
                        writer_sequence,
                        frame_offset,
                    },
                );
            }
        }
    }

    /// Iterate live subjects only.
    pub fn live_entries(&self) -> impl Iterator<Item = (&Vec<u8>, &LiveValue)> {
        self.map.iter().filter_map(|(k, v)| match v {
            IndexEntry::Live(lv) => Some((k, lv)),
            IndexEntry::Deleted { .. } => None,
        })
    }

    /// Live subjects strictly after `after` (exclusive), optional prefix (DEF-026).
    ///
    /// Order is subject-byte ascending. When `prefix` is set and `after` is
    /// `None`, iteration starts at the first key ≥ prefix (not the map start),
    /// then stops once the prefix range is left.
    pub fn live_entries_after<'a>(
        &'a self,
        after: Option<&[u8]>,
        prefix: Option<&[u8]>,
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a LiveValue)> + 'a {
        let lower = match after {
            Some(a) => Bound::Excluded(a.to_vec()),
            None => match prefix {
                // Seek to the first key in the prefix range so a foreign
                // collection that sorts earlier cannot starve the scan.
                Some(p) => Bound::Included(p.to_vec()),
                None => Bound::Unbounded,
            },
        };
        let prefix = prefix.map(|p| p.to_vec());
        self.map
            .range((lower, Bound::Unbounded))
            .filter_map(|(k, v)| match v {
                IndexEntry::Live(lv) => Some((k, lv)),
                IndexEntry::Deleted { .. } => None,
            })
            .take_while(move |(k, _)| match &prefix {
                None => true,
                Some(p) => k.starts_with(p),
            })
    }

    /// Iterate every subject entry (live and deleted), in subject order.
    pub fn iter_all(&self) -> impl Iterator<Item = (&Vec<u8>, &IndexEntry)> {
        self.map.iter()
    }

    /// Insert a fully formed entry (used by the optional index cache loader).
    pub fn insert_entry(&mut self, subject: Vec<u8>, entry: IndexEntry) {
        self.map.insert(subject, entry);
    }

    /// Clear all entries (before rebuild).
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.map.clear()
    }
}

/// Decide which put body bytes stay resident in the primary index (DEF-095).
///
/// Chunk manifests are small and required for reassembly routing; ordinary
/// payload bytes are dropped when a durable `frame_offset` will be recorded.
pub fn slim_put_body_for_index(body: Vec<u8>, keep_resident: bool) -> Vec<u8> {
    if keep_resident {
        return body;
    }
    if crate::chunk_payload::is_chunk_manifest(&body) {
        return body;
    }
    Vec::new()
}
