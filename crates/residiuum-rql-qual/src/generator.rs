//! Deterministic logical dataset generator (no product I/O).
//!
//! Produces the same JSON document multiset for a given [`DatasetSpec`] so
//! Residiuum / Mongo / CBL adapters can load identical logical work.

use crate::dataset::{
    CardinalityClass, DatasetSpec, DistributionKind, DocShape, PayloadClass, SelectivityClass,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// SplitMix64 — same family as Q1 Python materialiser (portable seeds).
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    pub fn next_u32(&mut self) -> u32 {
        self.next_u64() as u32
    }

    pub fn gen_range(&mut self, lo: u64, hi_exclusive: u64) -> u64 {
        if hi_exclusive <= lo {
            return lo;
        }
        lo + (self.next_u64() % (hi_exclusive - lo))
    }
}

/// One generated collection: name → key → document (includes `_key`).
pub type LogicalCollection = BTreeMap<String, Value>;

/// Full logical database for a measured cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalDataset {
    pub spec: DatasetSpec,
    pub collections: BTreeMap<String, LogicalCollection>,
    /// Content hash over sorted (collection, key, canonical json) triples.
    pub content_hash: String,
}

/// Constant-memory deterministic document stream for campaign-sized fixtures.
/// It is byte-for-byte equivalent to the `docs` collection materialised by
/// [`generate_dataset`] for the same specification.
pub struct GeneratedDocs {
    spec: DatasetSpec,
    rng: SplitMix64,
    next: u64,
    count: u64,
    distinct_statuses: u64,
    customer_count: u64,
}

impl Iterator for GeneratedDocs {
    type Item = (String, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.count {
            return None;
        }
        let i = self.next;
        self.next += 1;
        let key = match self.spec.distribution {
            DistributionKind::TimeOrdered => format!("t-{i:08}"),
            _ => format!("d-{i:08}"),
        };
        let status_index = match self.spec.distribution {
            DistributionKind::Uniform | DistributionKind::TimeOrdered => i % self.distinct_statuses,
            DistributionKind::ZipfHotKey if self.rng.next_u32() % 100 < 80 => 0,
            DistributionKind::ZipfHotKey => self.rng.gen_range(0, self.distinct_statuses),
        };
        let status = format!("st-{status_index:04}");
        let score = match self.spec.distribution {
            DistributionKind::TimeOrdered => i as i64,
            _ => self.rng.gen_range(0, 10_000) as i64,
        };
        let region = format!("r{}", i % 5);
        let amount = match self.spec.selectivity {
            SelectivityClass::Point if i == 0 => 42,
            _ => 10 + (self.rng.next_u32() % 990) as i64,
        };
        let mut document = base_doc(
            &key,
            &status,
            score,
            &region,
            amount,
            i,
            &mut self.rng,
            &self.spec,
        );
        pad_payload(
            &mut document,
            self.spec.payload,
            &mut self.rng,
            i == 0 && self.spec.include_heavy_tail,
        );
        apply_shape(&mut document, self.spec.shape, i, &mut self.rng);
        if i % 4 != 3 {
            if let Value::Object(map) = &mut document {
                map.insert(
                    "customer_id".into(),
                    json!(format!("c-{:08}", i % self.customer_count)),
                );
            }
        }
        Some((key, document))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count.saturating_sub(self.next).min(usize::MAX as u64) as usize;
        (remaining, Some(remaining))
    }
}

/// Create a constant-memory stream of primary documents.
pub fn generated_docs(spec: &DatasetSpec) -> GeneratedDocs {
    let count = spec.doc_count.max(1);
    GeneratedDocs {
        spec: spec.clone(),
        rng: SplitMix64::new(spec.seed),
        next: 0,
        count,
        distinct_statuses: distinct_count(count, spec.cardinality),
        customer_count: (count / 4).max(8),
    }
}

/// Generate deterministic logical docs for the primary `docs` collection
/// (plus optional `customers` for enrich cells).
pub fn generate_dataset(spec: &DatasetSpec) -> LogicalDataset {
    let n = spec.doc_count.max(1);
    let docs: BTreeMap<_, _> = generated_docs(spec).collect();

    // Companion customers for enrich cells (1:1 default + some optional gaps).
    let mut customers = BTreeMap::new();
    let n_cust = (n / 4).max(8);
    for i in 0..n_cust {
        let key = format!("c-{i:08}");
        customers.insert(
            key.clone(),
            json!({
                "_key": key,
                "id": key,
                "name": format!("Customer {i}"),
                "tier": if i % 3 == 0 { "gold" } else { "std" },
            }),
        );
    }

    // F12: preserve genuine 1:N cardinality in the shared logical fixture.
    // These documents have distinct immutable keys but deliberately share the
    // join field used by `customer_id = id`.
    for fanout in 1..=2 {
        let key = format!("c-00000000-fanout-{fanout}");
        customers.insert(
            key.clone(),
            json!({
                "_key": key,
                "id": "c-00000000",
                "name": format!("Customer 0 fanout {fanout}"),
                "tier": "fanout",
            }),
        );
    }

    let mut collections = BTreeMap::new();
    collections.insert("docs".into(), docs);
    collections.insert("customers".into(), customers);

    let content_hash = hash_collections(&collections);
    LogicalDataset {
        spec: spec.clone(),
        collections,
        content_hash,
    }
}

fn distinct_count(n: u64, card: CardinalityClass) -> u64 {
    let r = card.distinct_ratio();
    let d = ((n as f64) * r).round() as u64;
    d.clamp(2, n.max(2))
}

fn base_doc(
    key: &str,
    status: &str,
    score: i64,
    region: &str,
    amount: i64,
    i: u64,
    rng: &mut SplitMix64,
    spec: &DatasetSpec,
) -> Value {
    let mut m = Map::new();
    m.insert("_key".into(), json!(key));
    m.insert("status".into(), json!(status));
    m.insert("score".into(), json!(score));
    m.insert("region".into(), json!(region));
    m.insert("amount".into(), json!(amount));
    m.insert("i".into(), json!(i));
    m.insert(
        "created_at".into(),
        json!(format!("2024-01-01T00:00:00Z+{}m", i)),
    );
    // Selectivity bucket field for multi-selectivity eq cells.
    let bucket = selectivity_bucket(i, spec.doc_count, spec.selectivity, rng);
    m.insert("sel_bucket".into(), json!(bucket));
    Value::Object(m)
}

/// Target hit count for 0.01% selectivity: round(n * 0.0001) clamped to [1, n].
///
/// F4: earlier modulo+`i < period` logic yielded only ~1 hit at large n (~0.0001%).
pub fn s0_01_target_hits(n: u64) -> u64 {
    let n = n.max(1);
    let t = ((n as f64) * 0.0001).round() as u64;
    t.clamp(1, n)
}

fn selectivity_bucket(i: u64, n: u64, sel: SelectivityClass, rng: &mut SplitMix64) -> String {
    match sel {
        SelectivityClass::Point => {
            // Exactly one document; literal "POINT" (not HIT) — plans must match (F3).
            if i == 0 {
                "POINT".into()
            } else {
                format!("other-{}", i)
            }
        }
        SelectivityClass::S0_01 => {
            // Exactly s0_01_target_hits(n) documents labeled HIT (first i in 0..target).
            let target = s0_01_target_hits(n);
            if i < target {
                "HIT".into()
            } else {
                format!("m-{}", rng.next_u32() % 10_000)
            }
        }
        SelectivityClass::S1 => {
            if i % 100 == 0 {
                "HIT".into()
            } else {
                format!("m-{}", i % 99)
            }
        }
        SelectivityClass::S10 => {
            if i % 10 == 0 {
                "HIT".into()
            } else {
                format!("m-{}", i % 9)
            }
        }
        SelectivityClass::Broad => {
            if i % 2 == 0 {
                "HIT".into()
            } else {
                "MISS".into()
            }
        }
    }
}

fn pad_payload(doc: &mut Value, class: PayloadClass, rng: &mut SplitMix64, force_heavy: bool) {
    let target = if force_heavy {
        PayloadClass::HeavyTail.target_bytes()
    } else {
        class.target_bytes()
    };
    // Rough JSON overhead; pad with a string field.
    let pad_len = target.saturating_sub(128);
    let mut s = String::with_capacity(pad_len);
    while s.len() < pad_len {
        s.push(char::from(b'a' + (rng.next_u32() % 26) as u8));
    }
    if let Value::Object(m) = doc {
        m.insert("payload".into(), json!(s));
    }
}

fn apply_shape(doc: &mut Value, shape: DocShape, i: u64, rng: &mut SplitMix64) {
    let Value::Object(m) = doc else { return };
    match shape {
        DocShape::Flat => {}
        DocShape::DeeplyNested => {
            m.insert(
                "nested".into(),
                json!({
                    "l1": { "l2": { "l3": { "flag": i % 2 == 0, "v": rng.next_u32() % 100 } } }
                }),
            );
        }
        DocShape::SparseHeterogeneous => {
            if i % 3 == 0 {
                m.remove("amount");
            }
            if i % 5 == 0 {
                m.insert("extra_only".into(), json!(true));
            }
            if i % 7 == 0 {
                m.insert("amount".into(), json!("not-a-number"));
            }
            if i % 11 == 0 {
                m.insert("deleted_at".into(), Value::Null);
            }
        }
        DocShape::ArrayHeavy => {
            let len = 4 + (rng.next_u32() % 12) as usize;
            let tags: Vec<Value> = (0..len)
                .map(|j| json!(format!("t{}-{}", j, i % 5)))
                .collect();
            m.insert("tags".into(), Value::Array(tags));
            if i % 9 == 0 {
                m.insert("tags".into(), json!([]));
            }
        }
    }
}

fn hash_collections(cols: &BTreeMap<String, LogicalCollection>) -> String {
    let mut h = Sha256::new();
    for (cname, docs) in cols {
        h.update(cname.as_bytes());
        h.update(b"\n");
        for (k, v) in docs {
            h.update(k.as_bytes());
            h.update(b"\t");
            let s = serde_json::to_string(v).unwrap_or_default();
            h.update(s.as_bytes());
            h.update(b"\n");
        }
    }
    hex::encode(h.finalize())
}

/// Keys that should match `sel_bucket = "HIT"` (or POINT) under the generator rules.
pub fn expected_hit_keys(ds: &LogicalDataset) -> Vec<String> {
    let mut keys = Vec::new();
    let Some(docs) = ds.collections.get("docs") else {
        return keys;
    };
    for (k, doc) in docs {
        match doc.get("sel_bucket").and_then(|v| v.as_str()) {
            Some("HIT") | Some("POINT") => keys.push(k.clone()),
            _ => {}
        }
    }
    keys.sort();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{DatasetSpec, SelectivityClass};

    #[test]
    fn deterministic_same_seed() {
        let spec = DatasetSpec::smoke_default(42);
        let a = generate_dataset(&spec);
        let b = generate_dataset(&spec);
        assert_eq!(a.content_hash, b.content_hash);
        assert_eq!(a.collections["docs"].len(), 64);
        assert!(a.collections.contains_key("customers"));
    }

    #[test]
    fn different_seed_diverges() {
        let a = generate_dataset(&DatasetSpec::smoke_default(1));
        let b = generate_dataset(&DatasetSpec::smoke_default(2));
        assert_ne!(a.content_hash, b.content_hash);
    }

    #[test]
    fn shapes_emit_expected_fields() {
        for shape in DocShape::ALL {
            let mut spec = DatasetSpec::smoke_default(7);
            spec.shape = *shape;
            spec.doc_count = 16;
            let ds = generate_dataset(&spec);
            let docs = &ds.collections["docs"];
            assert_eq!(docs.len(), 16);
            match shape {
                DocShape::DeeplyNested => {
                    assert!(docs.values().any(|d| d.get("nested").is_some()));
                }
                DocShape::ArrayHeavy => {
                    assert!(docs.values().any(|d| d.get("tags").is_some()));
                }
                _ => {}
            }
        }
    }

    #[test]
    fn enrich_fixture_contains_genuine_one_to_many_join_value() {
        let ds = generate_dataset(&DatasetSpec::smoke_default(7));
        let matches = ds.collections["customers"]
            .values()
            .filter(|v| v.get("id").and_then(Value::as_str) == Some("c-00000000"))
            .count();
        assert_eq!(matches, 3, "base customer plus two fanout rows");
    }

    fn count_hits_materialised(n: u64, sel: SelectivityClass) -> u64 {
        let mut spec = DatasetSpec::smoke_default(1);
        spec.doc_count = n;
        spec.selectivity = sel;
        // Tiny payload so 10k materialisation stays cheap.
        spec.payload = crate::dataset::PayloadClass::Approx1KiB;
        let ds = generate_dataset(&spec);
        ds.collections["docs"]
            .values()
            .filter(|d| d.get("sel_bucket").and_then(|x| x.as_str()) == Some("HIT"))
            .count() as u64
    }

    /// Count HIT labels without building full JSON/payloads (campaign-scale n).
    fn count_s0_01_hits_bucket_loop(n: u64) -> u64 {
        let mut rng = SplitMix64::new(1);
        let mut hits = 0u64;
        for i in 0..n {
            if selectivity_bucket(i, n, SelectivityClass::S0_01, &mut rng) == "HIT" {
                hits += 1;
            }
        }
        hits
    }

    #[test]
    fn s0_01_hit_count_smoke_10k_1m() {
        assert_eq!(s0_01_target_hits(64), 1);
        assert_eq!(s0_01_target_hits(10_000), 1);
        assert_eq!(s0_01_target_hits(1_000_000), 100);
        // Materialise smoke + 10k only.
        for n in [64u64, 10_000] {
            let hits = count_hits_materialised(n, SelectivityClass::S0_01);
            let target = s0_01_target_hits(n);
            assert_eq!(hits, target, "n={n}: expected {target} HIT, got {hits}");
        }
        // Campaign scale without materialising 1e6×1KiB docs.
        let hits_1m = count_s0_01_hits_bucket_loop(1_000_000);
        assert_eq!(hits_1m, 100, "1e6 docs must yield 100 HIT for 0.01%");
    }

    #[test]
    fn point_emits_exactly_one_point_literal() {
        let mut spec = DatasetSpec::smoke_default(3);
        spec.selectivity = SelectivityClass::Point;
        spec.doc_count = 64;
        let ds = generate_dataset(&spec);
        let points: Vec<_> = ds.collections["docs"]
            .values()
            .filter(|d| d.get("sel_bucket").and_then(|x| x.as_str()) == Some("POINT"))
            .collect();
        assert_eq!(points.len(), 1);
        let hits = ds.collections["docs"]
            .values()
            .filter(|d| d.get("sel_bucket").and_then(|x| x.as_str()) == Some("HIT"))
            .count();
        assert_eq!(hits, 0, "Point class must not emit HIT");
    }
}
