//! Core VM phase helpers (RQL-VM2 / RQL-VM3 / RQL-VM3b).
//!
//! Product Core execute goes through Query VM opcodes → [`CoreFrame`] phases.
//! Each Core opcode owns a real phase body:
//! - **IndexEq** — host equality-index probe
//! - **Scan** — host key source / unfiltered doc load (no `where`)
//! - **Filter** — kernel `where` (+ key-stream get / early-stop)
//! - **Order** — sort-tuple order
//! - **Page** — limit / page-size / field-order resume
//! - **ProjectPaths** — path-project + page artefact
//!
//! **RQL-DEL1:** the demoted `execute_plan` / `run_core_page` orchestrator
//! wrappers (superseded by direct VM opcode dispatch) are deleted; only
//! [`CoreFrame`] phases remain as the shared implementation.
//! Decision 0 remains OPEN; RQL-C1 must not be accepted.

use super::vm_exec::{CoreOperands, VmPool};
use crate::app_v1::CoveragePolicy;
use crate::app_v1::{
    ConsistencyEvidence, HoleEvidence, QueryBudget, QueryId, QueryPage, QueryRow, QueryRunOptions,
};
use crate::cursor_v1::parameter_hash as cursor_parameter_hash;
use crate::error::Error;
use crate::plan_v1::OrderTerm;
use crate::predicate::Predicate;
use crate::rql_app_core::merge_budgets;
use residiuum_heap::{CollectionId, HeapId};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::time::Instant;

use super::core_page::{constrained_paths, equality_constraints, DocScan};
use super::kernel::CompiledKernelWhere;
use super::HostDocument;

/// Bind the immutable document key for RQL `_key` path predicates and projects.
///
/// Store bodies deliberately omit `_key` (key lives in the key stream). Corpus
/// and logical oracles use `_key` in `where` / `project`. Inject only when
/// absent so fixtures that already carry `_key` are unchanged.
fn with_logical_key(key: &str, doc: JsonValue) -> JsonValue {
    match doc {
        JsonValue::Object(mut m) => {
            m.entry("_key".to_string())
                .or_insert_with(|| JsonValue::String(key.to_string()));
            JsonValue::Object(m)
        }
        other => other,
    }
}

fn json_byte_len(v: &JsonValue) -> u64 {
    serde_json::to_vec(v).map(|b| b.len() as u64).unwrap_or(0)
}

fn check_governance(options: &QueryRunOptions, started: Instant) -> Result<(), Error> {
    if let Some(ref cancel) = options.cancel {
        cancel.check()?;
    }
    if let Some(deadline) = options.deadline {
        if started.elapsed() >= deadline {
            return Err(Error::DeadlineExceeded("query deadline exceeded".into()));
        }
    }
    Ok(())
}

fn has_later_match<S: DocScan>(
    scan: &mut S,
    after_key: &str,
    where_k: &CompiledKernelWhere,
    budget: Option<QueryBudget>,
    mut examined_docs: u64,
    mut examined_bytes: u64,
    known_holes: &mut Vec<HoleEvidence>,
) -> Result<bool, Error> {
    let mut after = Some(after_key.to_string());
    let mut probes = 0usize;
    while probes < 64 {
        let batch = scan.list_keys(Some(32), after.as_deref())?;
        if batch.is_empty() {
            return Ok(false);
        }
        for key in batch {
            after = Some(key.clone());
            probes += 1;
            match scan.get_json_covered(&key)? {
                HostDocument::Present(doc) => {
                    examined_docs += 1;
                    examined_bytes = examined_bytes.saturating_add(json_byte_len(&doc));
                    if budget
                        .and_then(|b| b.max_documents)
                        .is_some_and(|m| examined_docs > m)
                    {
                        return Ok(false);
                    }
                    if budget
                        .and_then(|b| b.max_bytes)
                        .is_some_and(|m| examined_bytes > m)
                    {
                        return Ok(false);
                    }
                    let doc = with_logical_key(&key, doc);
                    if where_k.eval_doc(&doc)? {
                        return Ok(true);
                    }
                }
                HostDocument::Absent => {
                    examined_docs += 1;
                    known_holes.push(HoleEvidence {
                        code: "key_listed_absent".into(),
                        key: Some(key),
                    });
                }
                HostDocument::Hole { code } => {
                    examined_docs += 1;
                    known_holes.push(HoleEvidence {
                        code,
                        key: Some(key),
                    });
                }
            }
            if budget
                .and_then(|b| b.max_documents)
                .is_some_and(|m| examined_docs > m)
            {
                return Ok(false);
            }
            if probes >= 64 {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Pending key source after Scan (RQL-VM3b). Filter consumes this.
enum PendingKeys {
    /// Field-order: docs already in `working`; Filter only applies where.
    Materialized,
    /// Key-stream index path: sorted candidate keys (after resume).
    Index(Vec<String>),
    /// Key-stream full scan: resume cursor for `list_keys`.
    Stream { after: Option<String> },
    /// Field-order index path: every candidate must be examined, but Filter
    /// may retain only the bounded best rows.
    FieldIndex(Vec<String>),
    /// Exclusive candidates already in requested field order. Filter may stop
    /// after the bounded page frontier is full.
    OrderedIndex(Vec<String>),
    /// Field-order full scan: every document must be examined, but Filter may
    /// retain only the bounded best rows.
    FieldStream,
}

/// Mutable Core pipeline state driven by Query VM opcodes (RQL-VM2/VM3/VM3b).
///
/// Operands come from typed QVM immediates / pool identity — not a full plan body.
pub(crate) struct CoreFrame<'a> {
    plan_hash: [u8; 32],
    coverage: crate::app_v1::CoveragePolicy,
    consistency: crate::app_v1::ConsistencyMode,
    order: Vec<OrderTerm>,
    project: super::vm_exec::ProjectImm,
    params: &'a BTreeMap<String, JsonValue>,
    options: &'a QueryRunOptions,
    heap_id: HeapId,
    collection_id: CollectionId,
    budget: Option<QueryBudget>,
    started: Instant,
    where_k: CompiledKernelWhere,
    page_size: usize,
    param_hash: String,
    last_sort_tuple_resume: Option<JsonValue>,
    remaining_limit: Option<u64>,
    key_only_order: bool,
    /// `None` until [`Self::index_eq`].
    index_keys: Option<Option<Vec<String>>>,
    /// The selected candidate set is already in requested field order.
    index_keys_ordered: bool,
    /// Set by Scan; consumed by Filter (RQL-VM3b).
    pending_keys: Option<PendingKeys>,
    /// Full documents in the working bag (pre-project).
    working: Vec<(String, JsonValue)>,
    /// Group rows were accumulated while filtering; Project must not aggregate
    /// them a second time.
    pre_aggregated: bool,
    known_holes: Vec<HoleEvidence>,
    examined_docs: u64,
    examined_bytes: u64,
    used_index: bool,
    /// Key-stream / index early-stop: more candidates remain.
    index_more: bool,
    /// Field-order: truncated for page size.
    field_order_more: bool,
    saw_scan: bool,
    saw_filter: bool,
    saw_order: bool,
    saw_page: bool,
    /// Soft-stopped because a query budget was exhausted under incomplete coverage.
    budget_truncated: bool,
}

impl<'a> CoreFrame<'a> {
    /// Soft budget stop allowed when plan or run options permit incomplete coverage.
    ///
    /// RQL_SPEC: budget exhaust under `coverage complete` fails closed; under
    /// `allow incomplete` (or run options IncompleteAllowed) returns partial page
    /// + incomplete coverage evidence. Hard fail-closed paths (APB dual-pack) keep
    /// default Complete and still get [`Error::ResourceLimit`].
    fn allows_budget_partial(&self) -> bool {
        matches!(self.coverage, CoveragePolicy::IncompleteAllowed)
            || matches!(self.options.coverage, CoveragePolicy::IncompleteAllowed)
    }

    /// Record soft budget exhaust; return Ok(true) = stop. Hard mode → ResourceLimit.
    fn hit_budget(&mut self, kind: &str, detail: String) -> Result<bool, Error> {
        if self.allows_budget_partial() {
            if !self.budget_truncated {
                self.budget_truncated = true;
                self.known_holes.push(HoleEvidence {
                    code: format!("budget_exhausted_{kind}"),
                    key: None,
                });
            }
            Ok(true)
        } else {
            Err(Error::ResourceLimit(detail))
        }
    }

    /// Before examining another document: stop if document budget is already full.
    fn stop_if_doc_budget_full(&mut self) -> Result<bool, Error> {
        if let Some(max) = self.budget.and_then(|b| b.max_documents) {
            if self.examined_docs >= max {
                return self.hit_budget(
                    "documents",
                    format!("query budget max_documents={max} exceeded"),
                );
            }
        }
        Ok(false)
    }

    /// Before accepting payload bytes for a candidate: stop if bytes budget would exceed.
    fn stop_if_bytes_would_exceed(&mut self, add_bytes: u64) -> Result<bool, Error> {
        if let Some(max) = self.budget.and_then(|b| b.max_bytes) {
            let next = self.examined_bytes.saturating_add(add_bytes);
            if next > max {
                return self.hit_budget(
                    "bytes",
                    format!("query budget max_bytes={max} exceeded (examined_bytes={next})"),
                );
            }
        }
        Ok(false)
    }

    /// Before materialising a result row: stop if result_bytes budget would exceed.
    fn stop_if_result_would_exceed(&mut self, next_total: u64) -> Result<bool, Error> {
        if let Some(max) = self.budget.and_then(|b| b.max_result_bytes) {
            if next_total > max {
                return self.hit_budget(
                    "result_bytes",
                    format!(
                        "query budget max_result_bytes={max} exceeded (result_bytes={next_total})"
                    ),
                );
            }
        }
        Ok(false)
    }

    /// Prepare frame after BindCollection from typed pool + Core opcode operands.
    ///
    /// `program_hash` is the canonical QVM hash of the **complete** program
    /// (including Full attach ops) — used for cursor identity, never an
    /// independent trusted wire field.
    pub fn begin(
        program_hash: [u8; 32],
        pool: &VmPool,
        core: &CoreOperands,
        params: &'a BTreeMap<String, JsonValue>,
        options: &'a QueryRunOptions,
        heap_id: HeapId,
        collection_id: CollectionId,
        source_budget: Option<QueryBudget>,
    ) -> Result<Self, Error> {
        let started = Instant::now();
        check_governance(options, started)?;
        let where_k = super::kernel::compile_where(&core.where_pred, params)?;
        let budget = merge_budgets(source_budget, options.budget);
        let page_size = super::ir_page::resolve_page_size(core.page_size, options.page_size);
        let param_hash = cursor_parameter_hash(params);
        let (last_sort_tuple_resume, remaining_limit) = if let Some(ref cont) = options.after {
            super::ir_page::decode_after(cont, heap_id, collection_id, &program_hash, &param_hash)?
        } else {
            (None, core.limit)
        };
        let key_only_order = core
            .order
            .iter()
            .all(|t| t.tie_break || t.path.0 == ["$key"]);
        Ok(Self {
            plan_hash: program_hash,
            coverage: pool.coverage,
            consistency: pool.consistency,
            order: core.order.clone(),
            project: core.project.clone(),
            params,
            options,
            heap_id,
            collection_id,
            budget,
            started,
            where_k,
            page_size,
            param_hash,
            last_sort_tuple_resume,
            remaining_limit,
            key_only_order,
            index_keys: None,
            index_keys_ordered: false,
            pending_keys: None,
            working: Vec::new(),
            pre_aggregated: false,
            known_holes: Vec::new(),
            examined_docs: 0,
            examined_bytes: 0,
            used_index: false,
            index_more: false,
            field_order_more: false,
            saw_scan: false,
            saw_filter: false,
            saw_order: false,
            saw_page: false,
            budget_truncated: false,
        })
    }

    /// OpCode::IndexEq — real host equality-index probe (or force-scan skip).
    pub fn index_eq<S: DocScan>(
        &mut self,
        scan: &mut S,
        where_pred: &Predicate,
        force_scan: bool,
    ) -> Result<(), Error> {
        let mut ordered = false;
        let mut keys = if force_scan {
            None
        } else {
            let eqs = equality_constraints(where_pred, self.params);
            if eqs.is_empty() {
                None
            } else {
                let constrained = constrained_paths(where_pred, self.params);
                scan.try_index_keys_with_constraints(&eqs, &constrained)?
            }
        };
        if !force_scan
            && keys.is_none()
            && !self.key_only_order
            && !self.project.group_agg.is_active()
        {
            keys = scan.try_ordered_index_keys(&self.order)?;
            ordered = keys.is_some();
        }
        self.index_keys_ordered = ordered;
        self.index_keys = Some(keys);
        Ok(())
    }

    /// OpCode::Scan — establish key source or load unfiltered docs.
    ///
    /// Does **not** apply `where` (RQL-VM3b). Key-stream paths leave a
    /// [`PendingKeys`] cursor for Filter; field-order materializes docs now.
    pub fn scan<S: DocScan>(&mut self, scan: &mut S) -> Result<(), Error> {
        let index_keys = self
            .index_keys
            .as_ref()
            .ok_or_else(|| Error::QueryInvalid("core frame: Scan before IndexEq".into()))?;
        self.used_index = index_keys.is_some();
        let after_key = self
            .last_sort_tuple_resume
            .as_ref()
            .and_then(super::ir_order::key_from_sort_tuple);

        if let Some(mut candidates) = index_keys.clone() {
            if !self.index_keys_ordered {
                candidates.sort();
            }
            if self.key_only_order {
                if let Some(ak) = after_key.as_deref() {
                    candidates.retain(|k| k.as_str() > ak);
                }
                self.pending_keys = Some(PendingKeys::Index(candidates));
            } else if self.index_keys_ordered {
                self.pending_keys = Some(PendingKeys::OrderedIndex(candidates));
            } else if !self.project.group_agg.is_active() {
                self.pending_keys = Some(PendingKeys::FieldIndex(candidates));
            } else {
                self.scan_index_materialize(scan, candidates)?;
                self.pending_keys = Some(PendingKeys::Materialized);
            }
        } else if self.key_only_order {
            self.pending_keys = Some(PendingKeys::Stream { after: after_key });
        } else if !self.project.group_agg.is_active() {
            self.pending_keys = Some(PendingKeys::FieldStream);
        } else {
            self.scan_full_unordered(scan)?;
            self.pending_keys = Some(PendingKeys::Materialized);
        }
        self.saw_scan = true;
        Ok(())
    }

    fn scan_index_materialize<S: DocScan>(
        &mut self,
        scan: &mut S,
        candidates: Vec<String>,
    ) -> Result<(), Error> {
        if self
            .budget
            .is_none_or(|budget| budget.max_documents.is_none() && budget.max_bytes.is_none())
        {
            for keys in candidates.chunks(256) {
                check_governance(self.options, self.started)?;
                for (key, document) in scan.get_json_covered_many(keys)? {
                    match document {
                        HostDocument::Present(doc) => {
                            self.examined_docs += 1;
                            self.examined_bytes =
                                self.examined_bytes.saturating_add(json_byte_len(&doc));
                            self.working.push((key, doc));
                        }
                        HostDocument::Absent => {
                            self.examined_docs += 1;
                            self.known_holes.push(HoleEvidence {
                                code: "index_key_absent".into(),
                                key: Some(key),
                            });
                        }
                        HostDocument::Hole { code } => {
                            self.examined_docs += 1;
                            self.known_holes.push(HoleEvidence {
                                code,
                                key: Some(key),
                            });
                        }
                    }
                }
            }
            return Ok(());
        }
        for key in candidates {
            check_governance(self.options, self.started)?;
            if self.stop_if_doc_budget_full()? {
                return Ok(());
            }
            match scan.get_json_covered(&key)? {
                HostDocument::Present(doc) => {
                    let blen = json_byte_len(&doc);
                    if self.stop_if_bytes_would_exceed(blen)? {
                        return Ok(());
                    }
                    self.examined_docs += 1;
                    self.examined_bytes = self.examined_bytes.saturating_add(blen);
                    self.working.push((key, doc));
                }
                HostDocument::Absent => {
                    self.examined_docs += 1;
                    self.known_holes.push(HoleEvidence {
                        code: "index_key_absent".into(),
                        key: Some(key),
                    });
                }
                HostDocument::Hole { code } => {
                    self.examined_docs += 1;
                    self.known_holes.push(HoleEvidence {
                        code,
                        key: Some(key),
                    });
                }
            }
        }
        Ok(())
    }

    fn scan_full_unordered<S: DocScan>(&mut self, scan: &mut S) -> Result<(), Error> {
        let mut after: Option<String> = None;
        loop {
            check_governance(self.options, self.started)?;
            if self.budget_truncated {
                break;
            }
            let batch = scan.scan_json_covered_page(Some(256), after.as_deref())?;
            if batch.is_empty() {
                break;
            }
            for (key, document) in batch {
                check_governance(self.options, self.started)?;
                if self.stop_if_doc_budget_full()? {
                    return Ok(());
                }
                after = Some(key.clone());
                match document {
                    HostDocument::Present(doc) => {
                        let blen = json_byte_len(&doc);
                        if self.stop_if_bytes_would_exceed(blen)? {
                            return Ok(());
                        }
                        self.examined_docs += 1;
                        self.examined_bytes = self.examined_bytes.saturating_add(blen);
                        self.working.push((key, doc));
                    }
                    HostDocument::Absent => {
                        self.examined_docs += 1;
                        self.known_holes.push(HoleEvidence {
                            code: "key_listed_absent".into(),
                            key: Some(key),
                        });
                    }
                    HostDocument::Hole { code } => {
                        self.examined_docs += 1;
                        self.known_holes.push(HoleEvidence {
                            code,
                            key: Some(key),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// OpCode::Filter — kernel where; key-stream also gets docs + early-stop.
    pub fn filter<S: DocScan>(&mut self, scan: &mut S) -> Result<(), Error> {
        if !self.saw_scan {
            return Err(Error::QueryInvalid("core frame: Filter before Scan".into()));
        }
        let pending = self.pending_keys.take().ok_or_else(|| {
            Error::QueryInvalid("core frame: Filter without Scan pending keys".into())
        })?;
        if self.project.group_agg.is_active() {
            self.filter_group_aggregate(scan, pending)?;
            self.saw_filter = true;
            return Ok(());
        }
        // Group/aggregate must see the full filtered bag (budget still applies).
        // Early-stop at page size would under-count groups.
        let need = if self.project.group_agg.is_active() {
            usize::MAX / 4
        } else {
            super::ir_page::rows_needed(self.remaining_limit, self.page_size)
        };
        match pending {
            PendingKeys::Materialized => {
                let mut kept = Vec::with_capacity(self.working.len());
                for (key, doc) in self.working.drain(..) {
                    check_governance(self.options, self.started)?;
                    let doc = with_logical_key(&key, doc);
                    if self.where_k.eval_doc(&doc)? {
                        kept.push((key, doc));
                    }
                }
                self.working = kept;
            }
            PendingKeys::Index(candidates) => {
                self.filter_index_stream(scan, candidates, need)?;
            }
            PendingKeys::Stream { after } => {
                self.filter_key_stream(scan, after, need)?;
            }
            PendingKeys::FieldIndex(candidates) => {
                self.filter_field_index_stream(scan, candidates)?;
            }
            PendingKeys::OrderedIndex(candidates) => {
                self.filter_ordered_index_stream(scan, candidates)?;
            }
            PendingKeys::FieldStream => {
                self.filter_field_stream(scan)?;
            }
        }
        self.saw_filter = true;
        Ok(())
    }

    fn filter_group_aggregate<S: DocScan>(
        &mut self,
        scan: &mut S,
        pending: PendingKeys,
    ) -> Result<(), Error> {
        let spec = self.project.group_agg.clone();
        let mut accumulator = super::group_agg::GroupAccumulator::new(&spec);
        let needs_logical_key = !self.where_k.is_constant() || accumulator.requires_logical_key();
        match pending {
            PendingKeys::Materialized => {
                for (key, doc) in self.working.drain(..) {
                    check_governance(self.options, self.started)?;
                    let doc = if needs_logical_key {
                        with_logical_key(&key, doc)
                    } else {
                        doc
                    };
                    if self.where_k.eval_doc(&doc)? {
                        accumulator.ingest(&doc)?;
                    }
                }
            }
            PendingKeys::Index(candidates) => {
                let can_batch = self.budget.is_none_or(|budget| {
                    budget.max_documents.is_none() && budget.max_bytes.is_none()
                });
                if can_batch {
                    for keys in candidates.chunks(256) {
                        check_governance(self.options, self.started)?;
                        for (key, document) in scan.get_json_covered_many(keys)? {
                            match document {
                                HostDocument::Present(doc) => {
                                    let doc = if needs_logical_key {
                                        with_logical_key(&key, doc)
                                    } else {
                                        doc
                                    };
                                    self.examined_docs += 1;
                                    self.examined_bytes =
                                        self.examined_bytes.saturating_add(json_byte_len(&doc));
                                    if self.where_k.eval_doc(&doc)? {
                                        accumulator.ingest(&doc)?;
                                    }
                                }
                                HostDocument::Absent => {
                                    self.examined_docs += 1;
                                    self.known_holes.push(HoleEvidence {
                                        code: "index_key_absent".into(),
                                        key: Some(key),
                                    });
                                }
                                HostDocument::Hole { code } => {
                                    self.examined_docs += 1;
                                    self.known_holes.push(HoleEvidence {
                                        code,
                                        key: Some(key),
                                    });
                                }
                            }
                        }
                    }
                } else {
                    for key in candidates {
                        check_governance(self.options, self.started)?;
                        if self.stop_if_doc_budget_full()? {
                            break;
                        }
                        match scan.get_json_covered(&key)? {
                            HostDocument::Present(doc) => {
                                let doc = if needs_logical_key {
                                    with_logical_key(&key, doc)
                                } else {
                                    doc
                                };
                                let length = json_byte_len(&doc);
                                if self.stop_if_bytes_would_exceed(length)? {
                                    break;
                                }
                                self.examined_docs += 1;
                                self.examined_bytes = self.examined_bytes.saturating_add(length);
                                if self.where_k.eval_doc(&doc)? {
                                    accumulator.ingest(&doc)?;
                                }
                            }
                            HostDocument::Absent => {
                                self.examined_docs += 1;
                                self.known_holes.push(HoleEvidence {
                                    code: "index_key_absent".into(),
                                    key: Some(key),
                                });
                            }
                            HostDocument::Hole { code } => {
                                self.examined_docs += 1;
                                self.known_holes.push(HoleEvidence {
                                    code,
                                    key: Some(key),
                                });
                            }
                        }
                    }
                }
            }
            PendingKeys::Stream { after } => {
                let mut after = after;
                'pages: loop {
                    check_governance(self.options, self.started)?;
                    if self.budget_truncated {
                        break;
                    }
                    let batch = scan.scan_json_covered_page(Some(256), after.as_deref())?;
                    if batch.is_empty() {
                        break;
                    }
                    for (key, document) in batch {
                        check_governance(self.options, self.started)?;
                        if self.stop_if_doc_budget_full()? {
                            break 'pages;
                        }
                        after = Some(key.clone());
                        match document {
                            HostDocument::Present(doc) => {
                                let doc = if needs_logical_key {
                                    with_logical_key(&key, doc)
                                } else {
                                    doc
                                };
                                let length = json_byte_len(&doc);
                                if self.stop_if_bytes_would_exceed(length)? {
                                    break 'pages;
                                }
                                self.examined_docs += 1;
                                self.examined_bytes = self.examined_bytes.saturating_add(length);
                                if self.where_k.eval_doc(&doc)? {
                                    accumulator.ingest(&doc)?;
                                }
                            }
                            HostDocument::Absent => {
                                self.examined_docs += 1;
                                self.known_holes.push(HoleEvidence {
                                    code: "key_listed_absent".into(),
                                    key: Some(key),
                                });
                            }
                            HostDocument::Hole { code } => {
                                self.examined_docs += 1;
                                self.known_holes.push(HoleEvidence {
                                    code,
                                    key: Some(key),
                                });
                            }
                        }
                    }
                }
            }
            PendingKeys::FieldIndex(_)
            | PendingKeys::OrderedIndex(_)
            | PendingKeys::FieldStream => {
                return Err(Error::QueryInvalid(
                    "core frame: invalid field-order source for group aggregation".into(),
                ));
            }
        }
        self.working = accumulator.finish()?;
        self.pre_aggregated = true;
        Ok(())
    }

    fn field_order_keep(&self) -> usize {
        match self.remaining_limit {
            Some(limit) if limit <= self.page_size as u64 => limit as usize,
            _ => self.page_size.saturating_add(1),
        }
    }

    fn retain_field_order_bound(&mut self, keep: usize, force: bool) {
        // Pruning on every insertion would repeatedly select the same small
        // bag. Let it grow to at most 2K, then reduce it to K.
        let prune_at = keep.saturating_mul(2).max(keep.saturating_add(1));
        if force || self.working.len() >= prune_at {
            super::ir_order::retain_best_rows(&mut self.working, &self.order, keep);
        }
    }

    fn push_field_order_match(&mut self, key: String, doc: JsonValue, keep: usize) {
        if keep == 0 {
            return;
        }
        if let Some(ref last) = self.last_sort_tuple_resume {
            let tuple = super::ir_order::build_sort_tuple(&key, &doc, &self.order);
            if super::ir_order::cmp_sort_tuples(&tuple, last, &self.order)
                != std::cmp::Ordering::Greater
            {
                return;
            }
        }
        // A small top-K frontier is cheapest as a permanently sorted vector:
        // O(log K) comparisons, a tiny pointer move, and no periodic general
        // selection pass. Larger pages use the batched partition path below.
        if keep <= 64 {
            let position = self
                .working
                .binary_search_by(|(existing_key, existing_doc)| {
                    super::ir_order::compare_rows(
                        existing_key,
                        existing_doc,
                        &key,
                        &doc,
                        &self.order,
                    )
                })
                .unwrap_or_else(|position| position);
            if position < keep {
                self.working.insert(position, (key, doc));
                self.working.truncate(keep);
            }
            return;
        }
        self.working.push((key, doc));
        self.retain_field_order_bound(keep, false);
    }

    fn filter_field_index_stream<S: DocScan>(
        &mut self,
        scan: &mut S,
        candidates: Vec<String>,
    ) -> Result<(), Error> {
        let keep = self.field_order_keep();
        let can_batch = self
            .budget
            .is_none_or(|budget| budget.max_documents.is_none() && budget.max_bytes.is_none());
        if can_batch {
            for keys in candidates.chunks(256) {
                check_governance(self.options, self.started)?;
                for (key, document) in scan.get_json_covered_many(keys)? {
                    match document {
                        HostDocument::Present(doc) => {
                            let blen = json_byte_len(&doc);
                            let doc = with_logical_key(&key, doc);
                            self.examined_docs += 1;
                            self.examined_bytes = self.examined_bytes.saturating_add(blen);
                            if self.where_k.eval_doc(&doc)? {
                                self.push_field_order_match(key, doc, keep);
                            }
                        }
                        HostDocument::Absent => {
                            self.examined_docs += 1;
                            self.known_holes.push(HoleEvidence {
                                code: "index_key_absent".into(),
                                key: Some(key),
                            });
                        }
                        HostDocument::Hole { code } => {
                            self.examined_docs += 1;
                            self.known_holes.push(HoleEvidence {
                                code,
                                key: Some(key),
                            });
                        }
                    }
                }
            }
        } else {
            for key in candidates {
                check_governance(self.options, self.started)?;
                if self.stop_if_doc_budget_full()? {
                    break;
                }
                match scan.get_json_covered(&key)? {
                    HostDocument::Present(doc) => {
                        let blen = json_byte_len(&doc);
                        if self.stop_if_bytes_would_exceed(blen)? {
                            break;
                        }
                        let doc = with_logical_key(&key, doc);
                        self.examined_docs += 1;
                        self.examined_bytes = self.examined_bytes.saturating_add(blen);
                        if self.where_k.eval_doc(&doc)? {
                            self.push_field_order_match(key, doc, keep);
                        }
                    }
                    HostDocument::Absent => {
                        self.examined_docs += 1;
                        self.known_holes.push(HoleEvidence {
                            code: "index_key_absent".into(),
                            key: Some(key),
                        });
                    }
                    HostDocument::Hole { code } => {
                        self.examined_docs += 1;
                        self.known_holes.push(HoleEvidence {
                            code,
                            key: Some(key),
                        });
                    }
                }
            }
        }
        self.retain_field_order_bound(keep, true);
        Ok(())
    }

    fn filter_ordered_index_stream<S: DocScan>(
        &mut self,
        scan: &mut S,
        candidates: Vec<String>,
    ) -> Result<(), Error> {
        let keep = self.field_order_keep();
        if keep == 0 {
            return Ok(());
        }
        let batch_size = keep.clamp(16, 64);
        for keys in candidates.chunks(batch_size) {
            check_governance(self.options, self.started)?;
            let documents = if self
                .budget
                .is_none_or(|budget| budget.max_documents.is_none() && budget.max_bytes.is_none())
            {
                scan.get_json_covered_many(keys)?
            } else {
                let mut documents = Vec::with_capacity(keys.len());
                for key in keys {
                    documents.push((key.clone(), scan.get_json_covered(key)?));
                }
                documents
            };
            for (key, document) in documents {
                check_governance(self.options, self.started)?;
                if self.stop_if_doc_budget_full()? {
                    return Ok(());
                }
                match document {
                    HostDocument::Present(doc) => {
                        let blen = json_byte_len(&doc);
                        if self.stop_if_bytes_would_exceed(blen)? {
                            return Ok(());
                        }
                        let doc = with_logical_key(&key, doc);
                        self.examined_docs += 1;
                        self.examined_bytes = self.examined_bytes.saturating_add(blen);
                        if self.where_k.eval_doc(&doc)? {
                            self.push_field_order_match(key, doc, keep);
                            if self.working.len() >= keep {
                                return Ok(());
                            }
                        }
                    }
                    HostDocument::Absent => {
                        self.examined_docs += 1;
                        self.known_holes.push(HoleEvidence {
                            code: "index_key_absent".into(),
                            key: Some(key),
                        });
                    }
                    HostDocument::Hole { code } => {
                        self.examined_docs += 1;
                        self.known_holes.push(HoleEvidence {
                            code,
                            key: Some(key),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn filter_field_stream<S: DocScan>(&mut self, scan: &mut S) -> Result<(), Error> {
        let keep = self.field_order_keep();
        let mut after: Option<String> = None;
        loop {
            check_governance(self.options, self.started)?;
            if self.budget_truncated {
                break;
            }
            let batch = scan.scan_json_covered_page(Some(256), after.as_deref())?;
            if batch.is_empty() {
                break;
            }
            for (key, document) in batch {
                check_governance(self.options, self.started)?;
                if self.stop_if_doc_budget_full()? {
                    self.retain_field_order_bound(keep, true);
                    return Ok(());
                }
                after = Some(key.clone());
                match document {
                    HostDocument::Present(doc) => {
                        let blen = json_byte_len(&doc);
                        if self.stop_if_bytes_would_exceed(blen)? {
                            self.retain_field_order_bound(keep, true);
                            return Ok(());
                        }
                        let doc = with_logical_key(&key, doc);
                        self.examined_docs += 1;
                        self.examined_bytes = self.examined_bytes.saturating_add(blen);
                        if self.where_k.eval_doc(&doc)? {
                            self.push_field_order_match(key, doc, keep);
                        }
                    }
                    HostDocument::Absent => {
                        self.examined_docs += 1;
                        self.known_holes.push(HoleEvidence {
                            code: "key_listed_absent".into(),
                            key: Some(key),
                        });
                    }
                    HostDocument::Hole { code } => {
                        self.examined_docs += 1;
                        self.known_holes.push(HoleEvidence {
                            code,
                            key: Some(key),
                        });
                    }
                }
            }
        }
        self.retain_field_order_bound(keep, true);
        Ok(())
    }

    fn filter_index_stream<S: DocScan>(
        &mut self,
        scan: &mut S,
        candidates: Vec<String>,
        need: usize,
    ) -> Result<(), Error> {
        let can_batch_all = need >= candidates.len()
            && self
                .budget
                .is_none_or(|budget| budget.max_documents.is_none() && budget.max_bytes.is_none());
        if can_batch_all {
            for keys in candidates.chunks(256) {
                check_governance(self.options, self.started)?;
                for (key, document) in scan.get_json_covered_many(keys)? {
                    match document {
                        HostDocument::Absent => {
                            self.examined_docs += 1;
                            self.known_holes.push(HoleEvidence {
                                code: "index_key_absent".into(),
                                key: Some(key),
                            });
                        }
                        HostDocument::Hole { code } => {
                            self.examined_docs += 1;
                            self.known_holes.push(HoleEvidence {
                                code,
                                key: Some(key),
                            });
                        }
                        HostDocument::Present(doc) => {
                            let doc = with_logical_key(&key, doc);
                            self.examined_docs += 1;
                            self.examined_bytes =
                                self.examined_bytes.saturating_add(json_byte_len(&doc));
                            if self.where_k.eval_doc(&doc)? {
                                self.working.push((key, doc));
                            }
                        }
                    }
                }
            }
            return Ok(());
        }
        let mut iter = candidates.into_iter();
        while let Some(key) = iter.next() {
            check_governance(self.options, self.started)?;
            if self.stop_if_doc_budget_full()? {
                return Ok(());
            }
            match scan.get_json_covered(&key)? {
                HostDocument::Absent => {
                    self.examined_docs += 1;
                    self.known_holes.push(HoleEvidence {
                        code: "index_key_absent".into(),
                        key: Some(key),
                    });
                }
                HostDocument::Hole { code } => {
                    self.examined_docs += 1;
                    self.known_holes.push(HoleEvidence {
                        code,
                        key: Some(key),
                    });
                }
                HostDocument::Present(doc) => {
                    let doc = with_logical_key(&key, doc);
                    let blen = json_byte_len(&doc);
                    if self.stop_if_bytes_would_exceed(blen)? {
                        return Ok(());
                    }
                    self.examined_docs += 1;
                    self.examined_bytes = self.examined_bytes.saturating_add(blen);
                    if self.where_k.eval_doc(&doc)? {
                        self.working.push((key, doc));
                        if self.working.len() >= need {
                            self.index_more = iter.next().is_some();
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn filter_key_stream<S: DocScan>(
        &mut self,
        scan: &mut S,
        after_key: Option<String>,
        need: usize,
    ) -> Result<(), Error> {
        let mut after = after_key;
        'outer: loop {
            check_governance(self.options, self.started)?;
            if self.budget_truncated {
                break;
            }
            let batch = scan.scan_json_covered_page(Some(256), after.as_deref())?;
            if batch.is_empty() {
                break;
            }
            for (key, document) in batch {
                check_governance(self.options, self.started)?;
                if self.stop_if_doc_budget_full()? {
                    return Ok(());
                }
                after = Some(key.clone());
                match document {
                    HostDocument::Absent => {
                        self.known_holes.push(HoleEvidence {
                            code: "key_listed_absent".into(),
                            key: Some(key),
                        });
                        self.examined_docs += 1;
                    }
                    HostDocument::Hole { code } => {
                        self.known_holes.push(HoleEvidence {
                            code,
                            key: Some(key),
                        });
                        self.examined_docs += 1;
                    }
                    HostDocument::Present(doc) => {
                        let doc = with_logical_key(&key, doc);
                        let blen = json_byte_len(&doc);
                        if self.stop_if_bytes_would_exceed(blen)? {
                            return Ok(());
                        }
                        self.examined_docs += 1;
                        self.examined_bytes = self.examined_bytes.saturating_add(blen);
                        if self.where_k.eval_doc(&doc)? {
                            self.working.push((key, doc));
                            if self.working.len() >= need {
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// OpCode::Order — sort-tuple order (no-op for key-only stream).
    ///
    /// When group/aggregate is active, pre-group order is skipped; group rows
    /// are ordered after aggregation inside [`Self::project_paths`].
    pub fn order(&mut self) -> Result<(), Error> {
        if !self.saw_filter {
            return Err(Error::QueryInvalid(
                "core frame: Order before Filter".into(),
            ));
        }
        if !self.project.group_agg.is_active() && !self.key_only_order {
            if let Some(ref lst) = self.last_sort_tuple_resume {
                super::ir_order::retain_after_sort_tuple(&mut self.working, &self.order, lst);
            }
            let page_size = self.page_size;
            let keep = match self.remaining_limit {
                Some(limit) if limit <= page_size as u64 => limit as usize,
                // Preserve one look-ahead row so Page can distinguish an exact
                // page from a page with a valid continuation.
                _ => page_size.saturating_add(1),
            };
            super::ir_order::retain_best_rows(&mut self.working, &self.order, keep);
        }
        self.saw_order = true;
        Ok(())
    }

    /// OpCode::Page — field-order resume + limit + page-size truncate.
    ///
    /// When group/aggregate is active, truncation is deferred until after
    /// aggregation in [`Self::project_paths`].
    pub fn page(&mut self) -> Result<(), Error> {
        if !self.saw_order {
            return Err(Error::QueryInvalid("core frame: Page before Order".into()));
        }
        if !self.project.group_agg.is_active() && !self.key_only_order {
            if let Some(n) = self.remaining_limit {
                self.working.truncate(n as usize);
            }
            if self.working.len() > self.page_size {
                self.field_order_more = true;
                self.working.truncate(self.page_size);
            }
        }
        self.saw_page = true;
        Ok(())
    }

    /// OpCode::ProjectPaths — path-project + coverage / cursor page artefact.
    pub fn project_paths<S: DocScan>(&mut self, scan: &mut S) -> Result<QueryPage, Error> {
        if !self.saw_page {
            return Err(Error::QueryInvalid(
                "core frame: ProjectPaths before Page".into(),
            ));
        }
        let _ = self.index_keys.take();

        // Q2 group/aggregate: reshape, then order + page the group bag.
        if self.project.group_agg.is_active() {
            if !self.pre_aggregated {
                self.working = super::group_agg::apply_group_agg(
                    std::mem::take(&mut self.working),
                    &self.project.group_agg,
                )?;
            }
            // Deterministic order on group rows (key tie-break still applies).
            self.working.sort_by(|(ka, va), (kb, vb)| {
                super::ir_order::compare_rows(ka, va, kb, vb, &self.order)
            });
            if let Some(ref lst) = self.last_sort_tuple_resume {
                super::ir_order::retain_after_sort_tuple(&mut self.working, &self.order, lst);
            }
            if let Some(n) = self.remaining_limit {
                self.working.truncate(n as usize);
            }
            if self.working.len() > self.page_size {
                self.field_order_more = true;
                self.working.truncate(self.page_size);
            }
        }

        let mut matched: Vec<(String, JsonValue)> = Vec::with_capacity(self.working.len());
        let mut result_bytes: u64 = 0;
        let mut last_full_for_cursor: Option<(String, JsonValue)> = None;

        let bag = std::mem::take(&mut self.working);
        for (key, doc) in bag {
            check_governance(self.options, self.started)?;
            last_full_for_cursor = Some((key.clone(), doc.clone()));
            let value = super::ir_project::apply_project_paths(&doc, self.project.paths.as_ref())?;
            let row_len = json_byte_len(&value);
            let next_result = result_bytes.saturating_add(row_len);
            if self.stop_if_result_would_exceed(next_result)? {
                // Soft: omit this and remaining rows; hard already returned Err.
                break;
            }
            result_bytes = next_result;
            matched.push((key, value));
        }

        let took = matched.len();
        let need = super::ir_page::rows_needed(self.remaining_limit, self.page_size);
        let exhausted = if self.used_index {
            if self.key_only_order {
                !self.index_more && took <= need
            } else {
                !self.field_order_more
            }
        } else if self.key_only_order {
            took < need
        } else {
            !self.field_order_more
        };

        let exhausted = if !self.used_index && self.key_only_order && took == need {
            if let Some((last_k, _)) = matched.last() {
                let more = scan.list_keys(Some(1), Some(last_k.as_str()))?;
                if more.is_empty() {
                    true
                } else {
                    !has_later_match(
                        scan,
                        last_k,
                        &self.where_k,
                        self.budget,
                        self.examined_docs,
                        self.examined_bytes,
                        &mut self.known_holes,
                    )?
                }
            } else {
                true
            }
        } else if self.used_index && self.key_only_order {
            !self.index_more
        } else {
            exhausted
        };

        // Budget soft-stop: no continuation (budget is query-wide, not page-resume).
        let exhausted = exhausted || self.budget_truncated;

        let rows: Vec<QueryRow> = matched
            .into_iter()
            .map(|(key, value)| QueryRow { key, value })
            .collect();

        let remaining_after = self
            .remaining_limit
            .map(|n| n.saturating_sub(rows.len() as u64));
        let next = if exhausted || rows.is_empty() {
            None
        } else {
            let (last_key, last_doc) = last_full_for_cursor.unwrap_or_else(|| {
                (
                    rows.last().map(|r| r.key.clone()).unwrap_or_default(),
                    JsonValue::Null,
                )
            });
            let last_tuple = super::ir_order::build_sort_tuple(&last_key, &last_doc, &self.order);
            Some(super::ir_page::mint_page_cursor(
                self.heap_id,
                self.collection_id,
                &self.plan_hash,
                &self.param_hash,
                &self.order,
                &last_tuple,
                remaining_after,
                self.page_size as u32,
                self.coverage,
                self.consistency,
            )?)
        };

        let coverage_mode =
            super::ir_page::resolve_coverage_mode(self.coverage, self.options.coverage);
        // Soft budget exhaust forces incomplete coverage evidence even when the
        // plan defaulted to Complete but run options allowed incomplete.
        let coverage_mode = if self.budget_truncated {
            CoveragePolicy::IncompleteAllowed
        } else {
            coverage_mode
        };
        let coverage =
            super::ir_page::finish_coverage(coverage_mode, &self.known_holes, self.examined_docs)?;

        let _ = result_bytes;
        let _ = self.examined_bytes;

        Ok(QueryPage {
            query_id: QueryId(residiuum_store::random_id().map_err(Error::from)?),
            plan_hash: self.plan_hash,
            heap_id: self.heap_id,
            collection_id: self.collection_id,
            rows,
            next: if exhausted { None } else { next },
            exhausted,
            coverage,
            consistency: ConsistencyEvidence {
                mode: self.options.consistency,
            },
            remaining_limit: remaining_after,
            known_holes: std::mem::take(&mut self.known_holes),
            // Filled by the VM-wide counting HostCapabilities wrapper after
            // Core and any Full attach opcodes have completed.
            logical_bytes_examined: 0,
            diagnostics: None,
        })
    }
}
