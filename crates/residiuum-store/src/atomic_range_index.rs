//! Bounded process-local acceleration for exact Atomic key ranges.
//!
//! The projection is derived from the authoritative primary key/version domain.
//! It is never persisted and is cleared at every primary-index publication.

use residiuum_atomics::{
    CanonicalKeyKind, CollectionId, HeapId, RangeEntry, CANONICAL_KEY_ORDER_V1,
};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Default maximum represented bytes in exact Atomic range projections.
pub(crate) const DEFAULT_ATOMIC_RANGE_INDEX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AtomicRangeProjectionKey {
    pub(crate) heap_id: HeapId,
    pub(crate) collection_id: CollectionId,
    pub(crate) key_kind: CanonicalKeyKind,
}

#[derive(Clone, Debug)]
pub(crate) struct AtomicRangeProjection {
    pub(crate) coverage_domain: [u8; 32],
    pub(crate) order_profile: u64,
    pub(crate) examined_count: u32,
    pub(crate) entries: Arc<[RangeEntry]>,
    charge: usize,
}

impl AtomicRangeProjection {
    pub(crate) const fn base_charge() -> usize {
        std::mem::size_of::<AtomicRangeProjection>()
            .saturating_add(std::mem::size_of::<AtomicRangeProjectionKey>())
    }

    pub(crate) fn entry_charge(entry: &RangeEntry) -> usize {
        std::mem::size_of::<RangeEntry>().saturating_add(entry.key.subject_bytes().len())
    }

    pub(crate) fn new(
        coverage_domain: [u8; 32],
        examined_count: u32,
        entries: Vec<RangeEntry>,
    ) -> Self {
        let charge = entries.iter().fold(Self::base_charge(), |total, entry| {
            total.saturating_add(Self::entry_charge(entry))
        });
        Self {
            coverage_domain,
            order_profile: CANONICAL_KEY_ORDER_V1,
            examined_count,
            entries: entries.into(),
            charge,
        }
    }
}

/// Observable, non-authoritative cache counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AtomicRangeIndexMetrics {
    /// Validated projection lookups served.
    pub hits: u64,
    /// Lookups without an installed projection.
    pub misses: u64,
    /// Complete authoritative projections installed.
    pub builds: u64,
    /// Primary publications that discarded at least one projection.
    pub invalidations: u64,
    /// Complete projections not cached because they exceeded the memory bound.
    pub oversize_bypasses: u64,
    /// Current represented bytes.
    pub resident_bytes: usize,
    /// Current number of heap/collection/key-profile projections.
    pub resident_projections: usize,
}

#[derive(Debug)]
pub(crate) struct AtomicRangeProjectionCache {
    entries: HashMap<AtomicRangeProjectionKey, AtomicRangeProjection>,
    admission_order: VecDeque<AtomicRangeProjectionKey>,
    resident_bytes: usize,
    max_bytes: usize,
    metrics: AtomicRangeIndexMetrics,
}

impl AtomicRangeProjectionCache {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            admission_order: VecDeque::new(),
            resident_bytes: 0,
            max_bytes,
            metrics: AtomicRangeIndexMetrics::default(),
        }
    }

    pub(crate) fn get(&mut self, key: AtomicRangeProjectionKey) -> Option<AtomicRangeProjection> {
        let value = self.entries.get(&key).cloned();
        if value.is_some() {
            self.metrics.hits = self.metrics.hits.saturating_add(1);
        } else {
            self.metrics.misses = self.metrics.misses.saturating_add(1);
        }
        value
    }

    pub(crate) fn insert(
        &mut self,
        key: AtomicRangeProjectionKey,
        projection: AtomicRangeProjection,
    ) {
        if projection.charge > self.max_bytes {
            self.metrics.oversize_bypasses = self.metrics.oversize_bypasses.saturating_add(1);
            return;
        }
        if let Some(previous) = self.entries.remove(&key) {
            self.resident_bytes = self.resident_bytes.saturating_sub(previous.charge);
            self.admission_order.retain(|candidate| *candidate != key);
        }
        while self.resident_bytes.saturating_add(projection.charge) > self.max_bytes {
            let Some(oldest) = self.admission_order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.resident_bytes = self.resident_bytes.saturating_sub(evicted.charge);
            }
        }
        self.resident_bytes = self.resident_bytes.saturating_add(projection.charge);
        self.entries.insert(key, projection);
        self.admission_order.push_back(key);
        self.metrics.builds = self.metrics.builds.saturating_add(1);
    }

    pub(crate) fn clear(&mut self) {
        if !self.entries.is_empty() {
            self.metrics.invalidations = self.metrics.invalidations.saturating_add(1);
        }
        self.entries.clear();
        self.admission_order.clear();
        self.resident_bytes = 0;
    }

    pub(crate) fn note_oversize_bypass(&mut self) {
        self.metrics.oversize_bypasses = self.metrics.oversize_bypasses.saturating_add(1);
    }

    pub(crate) fn metrics(&self) -> AtomicRangeIndexMetrics {
        AtomicRangeIndexMetrics {
            resident_bytes: self.resident_bytes,
            resident_projections: self.entries.len(),
            ..self.metrics
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn projection_key(collection_byte: u8) -> AtomicRangeProjectionKey {
        let mut heap = [0u8; 16];
        heap[0] = 1;
        let mut collection = [0u8; 16];
        collection[0] = collection_byte;
        AtomicRangeProjectionKey {
            heap_id: HeapId::from_bytes(heap).unwrap(),
            collection_id: CollectionId::from_bytes(collection).unwrap(),
            key_kind: CanonicalKeyKind::String,
        }
    }

    fn empty_projection() -> AtomicRangeProjection {
        AtomicRangeProjection::new([7; 32], 0, Vec::new())
    }

    #[test]
    fn empty_projections_are_charged_evicted_and_bounded() {
        let base = AtomicRangeProjection::base_charge();
        let mut cache = AtomicRangeProjectionCache::new(base * 2);
        cache.insert(projection_key(1), empty_projection());
        cache.insert(projection_key(2), empty_projection());
        cache.insert(projection_key(3), empty_projection());
        assert!(cache.get(projection_key(1)).is_none());
        assert!(cache.get(projection_key(2)).is_some());
        assert!(cache.get(projection_key(3)).is_some());
        assert_eq!(cache.metrics().resident_projections, 2);
        assert!(cache.metrics().resident_bytes <= base * 2);
    }

    #[test]
    fn oversize_projection_is_never_installed() {
        let mut cache = AtomicRangeProjectionCache::new(1);
        cache.insert(projection_key(1), empty_projection());
        let metrics = cache.metrics();
        assert_eq!(metrics.oversize_bypasses, 1);
        assert_eq!(metrics.resident_projections, 0);
    }
}
