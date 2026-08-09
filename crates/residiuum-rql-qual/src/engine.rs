//! Engine adapter trait — product and comparator sides (Q4.3).
//!
//! - **Logical harness** (default): pure eval over shared logical datasets for
//!   smoke digests + metrics (not a product claim).
//! - **Residiuum embedded**: feature `residiuum-embedded`.
//! - **Mongo / CBL / Residiuum server**: load shared logical work; execute remains
//!   `NotConfigured` until external drivers are present (honest refuse codes).

use crate::canonicalize::{canonicalize_rows, CanonicalResult, ResultRow};
use crate::cell_plan::MeasuredCellPlan;
use crate::cells::MandatoryCell;
use crate::fixture::CorpusCaseHandle;
use crate::generator::LogicalDataset;
use crate::lane::EngineId;
use crate::metrics::{
    assemble_metrics, CellMetrics, LatencyCollector, QueryPathMetrics, QueryTimer,
};
use crate::shared_work::SharedLogicalWork;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("not configured: {0}")]
    NotConfigured(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("execute: {0}")]
    Execute(String),
    #[error("fixture: {0}")]
    Fixture(String),
}

/// Adapter readiness — never silent-empty for unsupported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterStatus {
    Ready,
    NotConfigured,
    FeatureDisabled,
    /// Adapter executed the request and the product refused it with a stable code.
    Refused,
    /// The measured construct is outside the adapter's current supported surface.
    Unsupported,
}

/// How the outcome was produced (F5 — automated evidence must not confuse simulators).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionKind {
    /// Product Residiuum / real comparator driver path.
    Product,
    /// Pure logical harness simulator (not product).
    LogicalSimulator,
    /// Adapter loaded shared work but did not execute (Mongo/CBL stubs).
    NotConfigured,
}

impl ExecutionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Product => "product",
            Self::LogicalSimulator => "logical_simulator",
            Self::NotConfigured => "not_configured",
        }
    }
}

/// One engine execution outcome for a corpus case / measured cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineRunOutcome {
    pub engine: EngineId,
    /// F5: machine-readable execution kind (required for evidence consumers).
    pub execution_kind: ExecutionKind,
    pub status: AdapterStatus,
    pub result: Option<CanonicalResult>,
    pub metrics: Option<CellMetrics>,
    pub refuse_code: Option<String>,
    pub detail: Option<String>,
    /// Shared work content hash when loaded (cross-engine fixture identity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_work_hash: Option<String>,
}

impl EngineRunOutcome {
    /// Competitive product Ready requires Product kind + competitive engine id.
    pub fn is_product_ready(&self) -> bool {
        self.execution_kind == ExecutionKind::Product
            && self.engine.is_competitive_product()
            && self.status == AdapterStatus::Ready
            && self.result.is_some()
    }
}

/// Shared adapter contract for all engines.
pub trait EngineAdapter: Send {
    fn engine_id(&self) -> EngineId;
    fn status(&self) -> AdapterStatus;

    /// Load shared logical work (same bytes/hash across engines).
    fn load_shared_work(&mut self, work: &SharedLogicalWork) -> Result<(), AdapterError> {
        let _ = work;
        match self.status() {
            AdapterStatus::Ready | AdapterStatus::NotConfigured => Ok(()),
            AdapterStatus::FeatureDisabled => Err(AdapterError::NotConfigured(format!(
                "{} feature disabled",
                self.engine_id().as_str()
            ))),
            AdapterStatus::Refused | AdapterStatus::Unsupported => Err(AdapterError::Unsupported(
                format!("{} is not ready for preparation", self.engine_id().as_str()),
            )),
        }
    }

    fn prepare_case(&mut self, _case: &CorpusCaseHandle) -> Result<(), AdapterError> {
        match self.status() {
            AdapterStatus::Ready => Ok(()),
            AdapterStatus::NotConfigured => Err(AdapterError::NotConfigured(
                self.engine_id().as_str().into(),
            )),
            AdapterStatus::FeatureDisabled => Err(AdapterError::NotConfigured(format!(
                "{} feature disabled",
                self.engine_id().as_str()
            ))),
            AdapterStatus::Refused | AdapterStatus::Unsupported => Err(AdapterError::Unsupported(
                format!("{} is not ready for execution", self.engine_id().as_str()),
            )),
        }
    }

    fn execute_case(&mut self, case: &CorpusCaseHandle) -> Result<EngineRunOutcome, AdapterError>;

    /// Execute a measured cell plan (Q4.2/Q4.3 primary entry).
    fn execute_plan(&mut self, plan: &MeasuredCellPlan) -> Result<EngineRunOutcome, AdapterError> {
        let _ = plan;
        Err(AdapterError::Unsupported(format!(
            "{} execute_plan residual",
            self.engine_id().as_str()
        )))
    }
}

// ---------------------------------------------------------------------------
// Logical harness engine (default Ready — pure, not product)
// ---------------------------------------------------------------------------

/// Pure logical evaluator for smoke digests over [`LogicalDataset`].
///
/// Not a product query path. Used so the harness can publish evidence bundles
/// and metric envelopes without Mongo/CBL drivers.
#[derive(Debug, Default)]
pub struct LogicalHarnessEngine {
    work: Option<SharedLogicalWork>,
}

impl LogicalHarnessEngine {
    pub fn new() -> Self {
        Self::default()
    }

    fn dataset(&self) -> Result<&LogicalDataset, AdapterError> {
        self.work
            .as_ref()
            .map(|w| &w.dataset)
            .ok_or_else(|| AdapterError::Fixture("shared work not loaded".into()))
    }

    /// Returns (rows, examined, detail).
    fn eval_plan(
        &self,
        plan: &MeasuredCellPlan,
    ) -> Result<(Vec<ResultRow>, u64, String), AdapterError> {
        use crate::cell_plan::{EnrichExpect, ReadWriteMix};
        use crate::generator::SplitMix64;

        let ds = self.dataset()?;
        let docs = ds
            .collections
            .get("docs")
            .ok_or_else(|| AdapterError::Fixture("missing docs".into()))?;
        let mut examined = 0u64;
        let mut rows = Vec::new();
        let mut detail = String::new();

        match plan.cell {
            MandatoryCell::KeyGet => {
                for (k, v) in docs {
                    examined += 1;
                    if k == "d-00000000" || k == "t-00000000" {
                        rows.push(row_from_doc(k, v));
                    }
                }
            }
            MandatoryCell::IndexedEqMultiSelectivity => {
                // Predicate must match generator emission for plan.dataset.selectivity (F3).
                let want = crate::cell_plan::sel_bucket_literal(plan.dataset.selectivity);
                for (k, v) in docs {
                    examined += 1;
                    if v.get("sel_bucket").and_then(|x| x.as_str()) == Some(want) {
                        rows.push(row_from_doc(k, v));
                    }
                }
                detail = format!("sel_bucket={want}");
            }
            MandatoryCell::MixedReadWrite => {
                // §7.2 cell 12: interleave reads of HIT docs with logical writes.
                let mix = plan.rw_mix.unwrap_or(ReadWriteMix::R90W10);
                let read_pct = mix.read_pct() as u32;
                let n_ops = 100u32;
                let mut writes = 0u32;
                let mut reads = 0u32;
                let mut working = docs.clone();
                let mut rng = SplitMix64::new(plan.dataset.seed ^ 0xA11CE);
                for op in 0..n_ops {
                    let roll = rng.next_u32() % 100;
                    if roll < read_pct {
                        reads += 1;
                        for (k, v) in working.iter() {
                            examined += 1;
                            if v.get("sel_bucket").and_then(|x| x.as_str()) == Some("HIT") {
                                let _ = (k, v); // read materialise
                            }
                        }
                    } else {
                        writes += 1;
                        let wk = format!("w-{:08}", op);
                        working.insert(
                            wk.clone(),
                            serde_json::json!({
                                "_key": wk,
                                "status": "open",
                                "sel_bucket": "MISS",
                                "amount": 1,
                                "region": "r0",
                                "score": 0,
                                "i": op,
                            }),
                        );
                    }
                }
                // Result digest: HIT reads under post-write snapshot.
                for (k, v) in &working {
                    if v.get("sel_bucket").and_then(|x| x.as_str()) == Some("HIT") {
                        rows.push(row_from_doc(k, v));
                    }
                }
                if writes == 0 || reads == 0 {
                    return Err(AdapterError::Execute(format!(
                        "mixed R/W must perform reads and writes; reads={reads} writes={writes}"
                    )));
                }
                detail = format!(
                    "mixed_rw mix={} reads={} writes={} final_docs={}",
                    mix.as_str(),
                    reads,
                    writes,
                    working.len()
                );
            }
            MandatoryCell::RangeAndCompound => {
                for (k, v) in docs {
                    examined += 1;
                    let amount = v.get("amount").and_then(|x| x.as_i64()).unwrap_or(-1);
                    let region = v.get("region").and_then(|x| x.as_str()).unwrap_or("");
                    if amount >= 100 && amount < 500 && region == "r0" {
                        rows.push(row_from_doc(k, v));
                    }
                }
            }
            MandatoryCell::DeterministicTopK => {
                let mut all: Vec<_> = docs.iter().collect();
                examined = all.len() as u64;
                all.sort_by(|(ka, va), (kb, vb)| {
                    let sa = va.get("score").and_then(|x| x.as_i64()).unwrap_or(0);
                    let sb = vb.get("score").and_then(|x| x.as_i64()).unwrap_or(0);
                    sb.cmp(&sa).then_with(|| ka.cmp(kb))
                });
                for (k, v) in all.into_iter().take(10) {
                    rows.push(row_from_doc(k, v));
                }
            }
            MandatoryCell::FirstAndDeepCursor => {
                // Multipage: materialise first page + deep (last) page, then full concat
                // as result rows (page_1 ++ … ++ page_n = unpaged).
                let mut keys: Vec<_> = docs.keys().cloned().collect();
                keys.sort();
                examined = keys.len() as u64;
                let page_sz = plan.page_size.unwrap_or(8) as usize;
                let n_pages = keys.len().div_ceil(page_sz.max(1));
                if plan.require_deep_cursor && keys.len() > page_sz && n_pages < 2 {
                    return Err(AdapterError::Execute(format!(
                        "deep cursor requires ≥2 pages; n={} page_sz={}",
                        keys.len(),
                        page_sz
                    )));
                }
                // Full concat = unpaged (ordered).
                for k in &keys {
                    if let Some(v) = docs.get(k) {
                        rows.push(row_from_doc(k, v));
                    }
                }
                let first_end = page_sz.min(keys.len());
                let deep_start = if n_pages > 1 {
                    (n_pages - 1) * page_sz
                } else {
                    0
                };
                detail = format!(
                    "cursor pages={} page_sz={} first_keys={} deep_start={} deep_keys={}",
                    n_pages,
                    page_sz,
                    first_end,
                    deep_start,
                    keys.len().saturating_sub(deep_start)
                );
            }
            MandatoryCell::CoveredNonCoveredProject => {
                // F11: confirm covered vs non-covered against declared indexes.
                let project_fields = ["status", "region"];
                let index_fields: std::collections::BTreeSet<&str> = plan
                    .indexes
                    .iter()
                    .flat_map(|i| i.fields.iter().map(|f| f.as_str()))
                    .collect();
                let cover_label = match plan.project_cover {
                    Some(crate::cell_plan::ProjectCoverKind::Covered) => {
                        for f in project_fields {
                            if !index_fields.contains(f) {
                                return Err(AdapterError::Execute(format!(
                                    "project_cover=covered but index lacks field {f}; indexes={index_fields:?}"
                                )));
                            }
                        }
                        "covered"
                    }
                    Some(crate::cell_plan::ProjectCoverKind::NonCovered) => {
                        let missing = project_fields.iter().any(|f| !index_fields.contains(f));
                        if !missing {
                            return Err(AdapterError::Execute(format!(
                                "project_cover=non_covered but index covers all projected fields; indexes={index_fields:?}"
                            )));
                        }
                        "non_covered"
                    }
                    None => "unspecified",
                };
                for (k, v) in docs {
                    examined += 1;
                    // Smoke/variants filter status = "st-0000".
                    if v.get("status").and_then(|x| x.as_str()) != Some("st-0000") {
                        continue;
                    }
                    if let Value::Object(m) = v {
                        let mut out = serde_json::Map::new();
                        if let Some(s) = m.get("status") {
                            out.insert("status".into(), s.clone());
                        }
                        if let Some(r) = m.get("region") {
                            out.insert("region".into(), r.clone());
                        }
                        rows.push(ResultRow {
                            key: k.clone(),
                            value: Value::Object(out),
                        });
                    }
                }
                detail = format!(
                    "projection_cover={cover_label} index_fields={:?} project_fields={:?}",
                    index_fields.iter().copied().collect::<Vec<_>>(),
                    project_fields
                );
            }
            MandatoryCell::ConditionalComputed => {
                // high_band := amount if amount >= 100 else null (computed conditional).
                for (k, v) in docs {
                    examined += 1;
                    let amount = v.get("amount").and_then(|x| x.as_i64());
                    let high_band = match amount {
                        Some(a) if a >= 100 => Value::from(a),
                        Some(_) => Value::Null,
                        None => Value::Null,
                    };
                    let mut out = serde_json::Map::new();
                    if let Some(a) = v.get("amount") {
                        out.insert("amount".into(), a.clone());
                    }
                    if let Some(r) = v.get("region") {
                        out.insert("region".into(), r.clone());
                    }
                    out.insert("high_band".into(), high_band);
                    rows.push(ResultRow {
                        key: k.clone(),
                        value: Value::Object(out),
                    });
                }
                detail = "conditional high_band computed".into();
            }
            MandatoryCell::NestedAndArrayPreds => {
                use crate::cell_plan::NestedArrayFocus;
                use crate::dataset::DocShape;
                let focus = plan
                    .nested_array_focus
                    .or_else(|| match plan.dataset.shape {
                        DocShape::DeeplyNested => Some(NestedArrayFocus::NestedOnly),
                        DocShape::ArrayHeavy => Some(NestedArrayFocus::ArrayOnly),
                        _ => None,
                    });
                for (k, v) in docs {
                    examined += 1;
                    let nested_ok = v
                        .pointer("/nested/l1/l2/l3/flag")
                        .and_then(|x| x.as_bool())
                        .unwrap_or(false);
                    let tags_hit = v
                        .get("tags")
                        .and_then(|t| t.as_array())
                        .map(|a| a.iter().any(|x| x.as_str() == Some("t0-0")))
                        .unwrap_or(false);
                    let hit = match focus {
                        Some(NestedArrayFocus::NestedOnly) => nested_ok,
                        Some(NestedArrayFocus::ArrayOnly) => tags_hit,
                        None => nested_ok || tags_hit,
                    };
                    if hit {
                        rows.push(row_from_doc(k, v));
                    }
                }
                detail = format!(
                    "nested_array_focus={} shape={}",
                    focus.map(|f| f.as_str()).unwrap_or("combined"),
                    plan.dataset.shape.as_str()
                );
            }
            MandatoryCell::GroupLowHighCard => {
                use std::collections::BTreeMap;
                let mut groups: BTreeMap<String, u64> = BTreeMap::new();
                for (_k, v) in docs {
                    examined += 1;
                    let g = v
                        .get("status")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    *groups.entry(g).or_default() += 1;
                }
                let n_groups = groups.len();
                for (g, cnt) in groups {
                    rows.push(ResultRow {
                        key: g.clone(),
                        value: serde_json::json!({"status": g, "count": cnt}),
                    });
                }
                detail = format!(
                    "group_card={} distinct_groups={}",
                    plan.dataset.cardinality.as_str(),
                    n_groups
                );
            }
            MandatoryCell::AggCountSumMinMaxAvg => {
                use std::collections::BTreeMap;
                let mut groups: BTreeMap<String, (u64, i64, i64, i64)> = BTreeMap::new();
                for (_k, v) in docs {
                    examined += 1;
                    let g = v
                        .get("region")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    let amount = v.get("amount").and_then(|x| x.as_i64()).unwrap_or(0);
                    let e = groups.entry(g).or_insert((0, 0, i64::MAX, i64::MIN));
                    e.0 += 1;
                    e.1 += amount;
                    e.2 = e.2.min(amount);
                    e.3 = e.3.max(amount);
                }
                for (g, (cnt, sum, min, max)) in groups {
                    let avg = if cnt > 0 {
                        (sum as f64) / (cnt as f64)
                    } else {
                        0.0
                    };
                    rows.push(ResultRow {
                        key: g.clone(),
                        value: serde_json::json!({
                            "region": g,
                            "count": cnt,
                            "sum_amount": sum,
                            "min_amount": min,
                            "max_amount": max,
                            "avg_amount": avg,
                        }),
                    });
                }
                detail = "agg includes avg_amount".into();
            }
            MandatoryCell::EnrichCardinalities => {
                let expect = plan.enrich_expect.unwrap_or(EnrichExpect::Optional);
                let customers = ds.collections.get("customers");
                for (k, v) in docs {
                    examined += 1;
                    let cid = v.get("customer_id").and_then(|x| x.as_str());
                    let matches: Vec<&Value> = match (cid, customers) {
                        (Some(id), Some(cs)) if expect == EnrichExpect::Many => cs
                            .values()
                            .filter(|candidate| {
                                candidate.get("id").and_then(Value::as_str) == Some(id)
                            })
                            .collect(),
                        (Some(id), Some(cs)) => cs.get(id).into_iter().collect(),
                        _ => Vec::new(),
                    };
                    match expect {
                        EnrichExpect::Optional => {
                            let mut body = v.clone();
                            if let Value::Object(ref mut m) = body {
                                m.insert(
                                    "customer".into(),
                                    matches.first().copied().cloned().unwrap_or(Value::Null),
                                );
                            }
                            rows.push(row_from_doc(k, &body));
                        }
                        EnrichExpect::ExactlyOne => {
                            if cid.is_some() && matches.len() == 1 {
                                let mut body = v.clone();
                                if let Value::Object(ref mut m) = body {
                                    m.insert("customer".into(), matches[0].clone());
                                }
                                rows.push(row_from_doc(k, &body));
                            }
                            // Source prefilters missing customer_id; invalid fixture
                            // cardinality remains absent from result and is caught by
                            // product/differential validation.
                        }
                        EnrichExpect::Many => {
                            let mut body = v.clone();
                            if let Value::Object(ref mut m) = body {
                                let arr: Vec<Value> =
                                    matches.iter().map(|c| (*c).clone()).collect();
                                m.insert("customer".into(), Value::Array(arr));
                            }
                            rows.push(row_from_doc(k, &body));
                        }
                    }
                }
                detail = format!("enrich_expect={}", expect.as_str());
            }
        }

        if !plan.order_sensitive {
            rows.sort_by(|a, b| a.key.cmp(&b.key));
        }
        Ok((rows, examined, detail))
    }
}

fn row_from_doc(key: &str, doc: &Value) -> ResultRow {
    let mut value = doc.clone();
    if let Value::Object(ref mut m) = value {
        m.remove("_key");
        m.remove("payload"); // strip pad for digest stability across payload sizes
    }
    ResultRow {
        key: key.to_string(),
        value,
    }
}

fn metrics_from_run(
    lat: &LatencyCollector,
    examined: u64,
    digest: &str,
    coverage_complete: bool,
    plan: &MeasuredCellPlan,
) -> CellMetrics {
    let plan_bytes =
        serde_json::to_vec(plan).unwrap_or_else(|_| plan.rql_source.as_bytes().to_vec());
    let plan_digest = hex::encode(Sha256::digest(plan_bytes));
    assemble_metrics(
        lat,
        QueryPathMetrics {
            documents_examined: Some(examined),
            logical_bytes_examined: None,
            index_entries_examined: None,
            index_size_bytes: None,
            index_build_ns: None,
            indexed_write_penalty_ns: None,
            // F15: plan identity must be independent of result identity.
            explain_plan_digest: Some(format!("logical_plan:{plan_digest}")),
        },
        Some(plan.lifecycle.class),
        Some(plan.lifecycle.cold_method.as_str().into()),
        Some(digest.into()),
        Some(coverage_complete),
        Some(true),
    )
}

impl EngineAdapter for LogicalHarnessEngine {
    fn engine_id(&self) -> EngineId {
        // F5: never ResidiuumEmbedded — automated evidence must not confuse simulators.
        EngineId::LogicalHarness
    }

    fn status(&self) -> AdapterStatus {
        AdapterStatus::Ready
    }

    fn load_shared_work(&mut self, work: &SharedLogicalWork) -> Result<(), AdapterError> {
        self.work = Some(work.clone());
        Ok(())
    }

    fn execute_case(&mut self, case: &CorpusCaseHandle) -> Result<EngineRunOutcome, AdapterError> {
        Ok(EngineRunOutcome {
            engine: self.engine_id(),
            execution_kind: ExecutionKind::LogicalSimulator,
            status: AdapterStatus::Ready,
            result: None,
            metrics: None,
            refuse_code: Some("use_execute_plan:logical_harness".into()),
            detail: Some(format!("case={} — use MeasuredCellPlan path", case.case_id)),
            shared_work_hash: self.work.as_ref().map(|w| w.content_hash.clone()),
        })
    }

    fn execute_plan(&mut self, plan: &MeasuredCellPlan) -> Result<EngineRunOutcome, AdapterError> {
        let mut lat = LatencyCollector::new();
        let timer = QueryTimer::start();
        let (rows, examined, eval_detail) = self.eval_plan(plan)?;
        lat.record_duration(timer.elapsed());

        let canon = canonicalize_rows(&rows, plan.order_sensitive, true);
        let digest = canon.values_digest.clone();
        let metrics = metrics_from_run(&lat, examined, &digest, true, plan);

        Ok(EngineRunOutcome {
            engine: self.engine_id(),
            execution_kind: ExecutionKind::LogicalSimulator,
            status: AdapterStatus::Ready,
            result: Some(canon),
            metrics: Some(metrics),
            refuse_code: None,
            detail: Some(format!(
                "logical_harness plan={} examined={} {}",
                plan.plan_id, examined, eval_detail
            )),
            shared_work_hash: self.work.as_ref().map(|w| w.content_hash.clone()),
        })
    }
}

// ---------------------------------------------------------------------------
// Comparator stubs — shared work loaded; execute not configured
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct MongoLocalAdapter {
    work_hash: Option<String>,
}

impl EngineAdapter for MongoLocalAdapter {
    fn engine_id(&self) -> EngineId {
        EngineId::MongoLocal
    }
    fn status(&self) -> AdapterStatus {
        AdapterStatus::NotConfigured
    }
    fn load_shared_work(&mut self, work: &SharedLogicalWork) -> Result<(), AdapterError> {
        self.work_hash = Some(work.content_hash.clone());
        Ok(())
    }
    fn execute_case(&mut self, case: &CorpusCaseHandle) -> Result<EngineRunOutcome, AdapterError> {
        Ok(EngineRunOutcome {
            engine: self.engine_id(),
            execution_kind: ExecutionKind::NotConfigured,
            status: AdapterStatus::NotConfigured,
            result: None,
            metrics: None,
            refuse_code: Some("adapter_not_configured:mongo_local".into()),
            detail: Some(format!(
                "Mongo 8.2.12 pin; shared_work={:?}; case={} — driver residual",
                self.work_hash, case.case_id
            )),
            shared_work_hash: self.work_hash.clone(),
        })
    }
    fn execute_plan(&mut self, plan: &MeasuredCellPlan) -> Result<EngineRunOutcome, AdapterError> {
        Ok(EngineRunOutcome {
            engine: self.engine_id(),
            execution_kind: ExecutionKind::NotConfigured,
            status: AdapterStatus::NotConfigured,
            result: None,
            metrics: None,
            refuse_code: Some("adapter_not_configured:mongo_local".into()),
            detail: Some(format!(
                "shared logical work loaded={}; plan={} (mongodb crate 3.8.0 residual)",
                self.work_hash.is_some(),
                plan.plan_id
            )),
            shared_work_hash: self.work_hash.clone(),
        })
    }
}

#[derive(Debug, Default)]
pub struct CblEmbeddedAdapter {
    work_hash: Option<String>,
}

impl EngineAdapter for CblEmbeddedAdapter {
    fn engine_id(&self) -> EngineId {
        EngineId::CouchbaseLiteEmbedded
    }
    fn status(&self) -> AdapterStatus {
        AdapterStatus::NotConfigured
    }
    fn load_shared_work(&mut self, work: &SharedLogicalWork) -> Result<(), AdapterError> {
        self.work_hash = Some(work.content_hash.clone());
        Ok(())
    }
    fn execute_case(&mut self, case: &CorpusCaseHandle) -> Result<EngineRunOutcome, AdapterError> {
        Ok(EngineRunOutcome {
            engine: self.engine_id(),
            execution_kind: ExecutionKind::NotConfigured,
            status: AdapterStatus::NotConfigured,
            result: None,
            metrics: None,
            refuse_code: Some("adapter_not_configured:cbl_embedded".into()),
            detail: Some(format!(
                "CBL 4.1.0 Full Sync pin; shared_work={:?}; case={}",
                self.work_hash, case.case_id
            )),
            shared_work_hash: self.work_hash.clone(),
        })
    }
    fn execute_plan(&mut self, plan: &MeasuredCellPlan) -> Result<EngineRunOutcome, AdapterError> {
        Ok(EngineRunOutcome {
            engine: self.engine_id(),
            execution_kind: ExecutionKind::NotConfigured,
            status: AdapterStatus::NotConfigured,
            result: None,
            metrics: None,
            refuse_code: Some("adapter_not_configured:cbl_embedded".into()),
            detail: Some(format!(
                "shared logical work loaded={}; plan={} (CBL native residual)",
                self.work_hash.is_some(),
                plan.plan_id
            )),
            shared_work_hash: self.work_hash.clone(),
        })
    }
}

#[derive(Debug, Default)]
pub struct ResidiuumServerAdapter {
    work_hash: Option<String>,
}

impl EngineAdapter for ResidiuumServerAdapter {
    fn engine_id(&self) -> EngineId {
        EngineId::ResidiuumServer
    }
    fn status(&self) -> AdapterStatus {
        AdapterStatus::NotConfigured
    }
    fn load_shared_work(&mut self, work: &SharedLogicalWork) -> Result<(), AdapterError> {
        self.work_hash = Some(work.content_hash.clone());
        Ok(())
    }
    fn execute_case(&mut self, case: &CorpusCaseHandle) -> Result<EngineRunOutcome, AdapterError> {
        Ok(EngineRunOutcome {
            engine: self.engine_id(),
            execution_kind: ExecutionKind::NotConfigured,
            status: AdapterStatus::NotConfigured,
            result: None,
            metrics: None,
            refuse_code: Some("adapter_not_configured:residiuum_server".into()),
            detail: Some(format!(
                "loopback serve + op 118 residual; case={}",
                case.case_id
            )),
            shared_work_hash: self.work_hash.clone(),
        })
    }
    fn execute_plan(&mut self, plan: &MeasuredCellPlan) -> Result<EngineRunOutcome, AdapterError> {
        Ok(EngineRunOutcome {
            engine: self.engine_id(),
            execution_kind: ExecutionKind::NotConfigured,
            status: AdapterStatus::NotConfigured,
            result: None,
            metrics: None,
            refuse_code: Some("adapter_not_configured:residiuum_server".into()),
            detail: Some(format!(
                "shared_work={}; plan={}",
                self.work_hash.is_some(),
                plan.plan_id
            )),
            shared_work_hash: self.work_hash.clone(),
        })
    }
}

/// Residiuum embedded product adapter (feature-gated execute).
#[derive(Debug, Default)]
pub struct ResidiuumEmbeddedAdapter {
    work: Option<SharedLogicalWork>,
}

impl EngineAdapter for ResidiuumEmbeddedAdapter {
    fn engine_id(&self) -> EngineId {
        EngineId::ResidiuumEmbedded
    }

    fn status(&self) -> AdapterStatus {
        #[cfg(feature = "residiuum-embedded")]
        {
            AdapterStatus::Ready
        }
        #[cfg(not(feature = "residiuum-embedded"))]
        {
            AdapterStatus::FeatureDisabled
        }
    }

    fn load_shared_work(&mut self, work: &SharedLogicalWork) -> Result<(), AdapterError> {
        self.work = Some(work.clone());
        Ok(())
    }

    fn execute_case(&mut self, case: &CorpusCaseHandle) -> Result<EngineRunOutcome, AdapterError> {
        Ok(EngineRunOutcome {
            engine: self.engine_id(),
            execution_kind: ExecutionKind::Product,
            status: self.status(),
            result: None,
            metrics: None,
            refuse_code: Some("use_execute_plan:residiuum_embedded".into()),
            detail: Some(case.case_id.clone()),
            shared_work_hash: self.work.as_ref().map(|w| w.content_hash.clone()),
        })
    }

    fn execute_plan(&mut self, plan: &MeasuredCellPlan) -> Result<EngineRunOutcome, AdapterError> {
        #[cfg(not(feature = "residiuum-embedded"))]
        {
            let _ = plan;
            return Ok(EngineRunOutcome {
                engine: self.engine_id(),
                execution_kind: ExecutionKind::NotConfigured,
                status: AdapterStatus::FeatureDisabled,
                result: None,
                metrics: None,
                refuse_code: Some("adapter_feature_disabled:residiuum_embedded".into()),
                detail: Some("enable --features residiuum-embedded".into()),
                shared_work_hash: self.work.as_ref().map(|w| w.content_hash.clone()),
            });
        }
        #[cfg(feature = "residiuum-embedded")]
        {
            crate::residiuum_embedded::execute_plan_embedded(self.work.as_ref(), plan)
        }
    }
}

// Legacy aliases
pub type MongoLocalStub = MongoLocalAdapter;
pub type CblEmbeddedStub = CblEmbeddedAdapter;
pub type ResidiuumServerStub = ResidiuumServerAdapter;

pub fn adapter_for(engine: EngineId) -> Box<dyn EngineAdapter> {
    match engine {
        EngineId::ResidiuumEmbedded => Box::new(ResidiuumEmbeddedAdapter::default()),
        EngineId::ResidiuumServer => Box::new(ResidiuumServerAdapter::default()),
        EngineId::MongoLocal => Box::new(MongoLocalAdapter::default()),
        EngineId::CouchbaseLiteEmbedded => Box::new(CblEmbeddedAdapter::default()),
        EngineId::LogicalHarness => Box::new(LogicalHarnessEngine::default()),
    }
}

pub fn synthetic_ready_outcome(engine: EngineId, digest_hex: &str) -> EngineRunOutcome {
    EngineRunOutcome {
        engine,
        execution_kind: if engine.is_logical_simulator() {
            ExecutionKind::LogicalSimulator
        } else if engine.is_competitive_product() {
            ExecutionKind::Product
        } else {
            ExecutionKind::NotConfigured
        },
        status: AdapterStatus::Ready,
        result: Some(CanonicalResult {
            keys_digest: digest_hex.to_string(),
            values_digest: digest_hex.to_string(),
            multiplicity: "multiset".into(),
            order_sensitive: false,
            coverage_complete: true,
            row_count: 0,
            raw_preview: Value::Null,
        }),
        metrics: None,
        refuse_code: None,
        detail: Some("synthetic_test_only".into()),
        shared_work_hash: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell_plan::MeasuredCellPlan;
    use crate::cells::MandatoryCell;
    use crate::dataset::DatasetSpec;
    use crate::generator::generate_dataset;
    use crate::shared_work::SharedLogicalWork;

    #[test]
    fn stubs_load_shared_work_without_inventing_digests() {
        let ds = generate_dataset(&DatasetSpec::smoke_default(1));
        let work = SharedLogicalWork::from_dataset(ds);
        let mut mongo = MongoLocalAdapter::default();
        mongo.load_shared_work(&work).unwrap();
        let out = mongo
            .execute_plan(&MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 1))
            .unwrap();
        assert_eq!(out.status, AdapterStatus::NotConfigured);
        assert!(out.result.is_none());
        assert_eq!(
            out.shared_work_hash.as_deref(),
            Some(work.content_hash.as_str())
        );
    }

    #[test]
    fn logical_harness_key_get_digest() {
        let ds = generate_dataset(&DatasetSpec::smoke_default(2));
        let work = SharedLogicalWork::from_dataset(ds);
        let mut eng = LogicalHarnessEngine::new();
        eng.load_shared_work(&work).unwrap();
        let plan = MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 2);
        let out = eng.execute_plan(&plan).unwrap();
        assert_eq!(out.status, AdapterStatus::Ready);
        let r = out.result.unwrap();
        assert_eq!(r.row_count, 1);
        assert!(out.metrics.as_ref().unwrap().latency.samples >= 1);
        assert_eq!(out.metrics.as_ref().unwrap().validity_ok, Some(true));
    }

    #[test]
    fn factory_covers_all_engines() {
        for e in [
            EngineId::ResidiuumEmbedded,
            EngineId::ResidiuumServer,
            EngineId::MongoLocal,
            EngineId::CouchbaseLiteEmbedded,
            EngineId::LogicalHarness,
        ] {
            assert_eq!(adapter_for(e).engine_id(), e);
        }
    }

    #[test]
    fn logical_outcome_is_not_product_ready() {
        let ds = generate_dataset(&DatasetSpec::smoke_default(2));
        let work = SharedLogicalWork::from_dataset(ds);
        let mut eng = LogicalHarnessEngine::new();
        eng.load_shared_work(&work).unwrap();
        let plan = MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 2);
        let out = eng.execute_plan(&plan).unwrap();
        assert_eq!(out.engine, EngineId::LogicalHarness);
        assert_eq!(out.execution_kind, ExecutionKind::LogicalSimulator);
        assert!(!out.is_product_ready());
        assert!(out.result.is_some());
    }

    #[test]
    fn enrich_many_produces_genuine_multiple_matches() {
        let mut plan = MeasuredCellPlan::smoke_for(MandatoryCell::EnrichCardinalities, 7);
        plan.enrich_expect = Some(crate::cell_plan::EnrichExpect::Many);
        let ds = generate_dataset(&plan.dataset);
        let work = SharedLogicalWork::from_dataset(ds);
        let mut eng = LogicalHarnessEngine::new();
        eng.load_shared_work(&work).unwrap();
        let (rows, _, _) = eng.eval_plan(&plan).unwrap();
        assert!(rows.iter().any(|row| {
            row.value
                .get("customer")
                .and_then(Value::as_array)
                .is_some_and(|matches| matches.len() >= 2)
        }));
    }

    #[test]
    fn logical_plan_digest_is_not_result_digest() {
        let plan = MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 2);
        let ds = generate_dataset(&plan.dataset);
        let work = SharedLogicalWork::from_dataset(ds);
        let mut eng = LogicalHarnessEngine::new();
        eng.load_shared_work(&work).unwrap();
        let out = eng.execute_plan(&plan).unwrap();
        let metrics = out.metrics.unwrap();
        assert_ne!(
            metrics.path.explain_plan_digest.as_deref(),
            metrics.result_digest_echo.as_deref()
        );
        assert!(metrics
            .path
            .explain_plan_digest
            .as_deref()
            .unwrap()
            .starts_with("logical_plan:"));
    }
}
