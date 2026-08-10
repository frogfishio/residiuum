//! Bounded process-local continuation state for completed group bags.
//!
//! Correctness never depends on this cache: miss, expiry, eviction, replay or
//! source-frontier drift makes the executor recompute the aggregate.

use crate::resource::estimate_row_bytes;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_CHARGE: u64 = 64 * 1024 * 1024;
const TTL: Duration = Duration::from_secs(crate::cursor_v1::TTL_SECONDS);

struct Entry {
    rows: Vec<(String, JsonValue)>,
    frontier: [u8; 32],
    expires_at: Instant,
    charge: u64,
}

#[derive(Default)]
struct Cache {
    entries: HashMap<String, Entry>,
    admission_order: VecDeque<String>,
    resident_charge: u64,
}

impl Cache {
    fn prune(&mut self, now: Instant) {
        let expired = self
            .entries
            .iter()
            .filter_map(|(id, entry)| (entry.expires_at <= now).then(|| id.clone()))
            .collect::<Vec<_>>();
        for id in expired {
            self.remove(&id);
        }
    }

    fn remove(&mut self, id: &str) -> Option<Entry> {
        let entry = self.entries.remove(id)?;
        self.resident_charge = self.resident_charge.saturating_sub(entry.charge);
        Some(entry)
    }

    fn evict_to_fit(&mut self, incoming: u64) {
        while self.resident_charge.saturating_add(incoming) > MAX_CHARGE {
            let Some(id) = self.admission_order.pop_front() else {
                break;
            };
            self.remove(&id);
        }
    }
}

static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn cache() -> &'static Mutex<Cache> {
    CACHE.get_or_init(|| Mutex::new(Cache::default()))
}

fn new_id() -> String {
    let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let mut hash = blake3::Hasher::new();
    hash.update(b"residiuum:group-spool-v1");
    hash.update(&now.to_le_bytes());
    hash.update(&sequence.to_le_bytes());
    hash.finalize().to_hex()[..32].to_string()
}

pub(crate) fn store(rows: Vec<(String, JsonValue)>, frontier: [u8; 32]) -> Option<String> {
    let charge = rows.iter().fold(0u64, |used, (key, value)| {
        used.saturating_add(estimate_row_bytes(key, value))
    });
    if charge > MAX_CHARGE {
        return None;
    }
    let id = new_id();
    let now = Instant::now();
    let mut cache = cache().lock().ok()?;
    cache.prune(now);
    cache.evict_to_fit(charge);
    if cache.resident_charge.saturating_add(charge) > MAX_CHARGE {
        return None;
    }
    cache.resident_charge = cache.resident_charge.saturating_add(charge);
    cache.admission_order.push_back(id.clone());
    cache.entries.insert(
        id.clone(),
        Entry {
            rows,
            frontier,
            expires_at: now + TTL,
            charge,
        },
    );
    Some(id)
}

pub(crate) fn take(
    id: &str,
    current_frontier: Option<[u8; 32]>,
) -> Option<Vec<(String, JsonValue)>> {
    let now = Instant::now();
    let mut cache = cache().lock().ok()?;
    cache.prune(now);
    let entry = cache.remove(id)?;
    (Some(entry.frontier) == current_frontier && entry.expires_at > now).then_some(entry.rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spool_is_single_use_and_frontier_fenced() {
        let frontier = [7; 32];
        let id = store(vec![("k".into(), serde_json::json!({"n": 1}))], frontier).unwrap();
        assert!(take(&id, Some([8; 32])).is_none());

        let id = store(vec![("k".into(), serde_json::json!({"n": 1}))], frontier).unwrap();
        assert_eq!(take(&id, Some(frontier)).unwrap().len(), 1);
        assert!(take(&id, Some(frontier)).is_none());
    }
}
