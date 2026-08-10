//! Query VM dispatch (**RQL-VM1R**) — one `run_vm` for Core + Full.
//!
//! Profile: **`residiuum-query-vm-v1`** (see [`super::vm`]).
//! Normative: [QUERY_VM_V1.md](../../../../../doc/todo/rql/QUERY_VM_V1.md)
//!
//! Product execute enters here after ISA decode + lower (+ QVM materialize).
//! Core pipeline opcodes call [`super::core_phases::CoreFrame`] (**RQL-VM2/VM3/VM3b**).
//! Scan establishes `PendingKeys`; Filter owns where (+ key-stream get/early-stop).
//! Full attach continues in the **same** loop after `ProjectPaths` (**RQL-VM4**);
//! `Within` imm is a shell (carrier + alias only).
//!
//! Decision 0 remains OPEN; **RQL-C1 must not be accepted.**

use crate::app_v1::{
    ConsistencyMode, CoveragePolicy, QueryBudget, QueryDiagnostics, QueryPage, QueryRunOptions,
};
use crate::error::Error;
use crate::plan_v1::{OrderTerm, RqlPlanV1};
use crate::predicate::{Path, Predicate};
use crate::resource::{check_result_bytes, estimate_row_bytes, host_limits};
use crate::rql_app_core::merge_budgets;
use residiuum_heap::{CollectionId, HeapId};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::time::Instant;

use super::core_page::DocScan;
use super::core_phases::CoreFrame;
use super::full_attach::{
    apply_project_rows, attach_enrich_rows_owned, ensure_foreign_docs, filter_rows,
    load_foreign_docs_for_root_enrich, within_enter, within_leave, EnrichAttachMode,
    EnrichLoadEvidence, EnrichStepV1, FullPipelineStepV1, ProjectItemV1, WithinStepV1,
};
use super::vm::{OpCode, VM_PROFILE};
use super::{HostCapabilities, HostDocument};

/// Accounts logical JSON payload bytes at the one host boundary shared by
/// Core and Full execution. This deliberately counts every successful load,
/// including repeated reads and foreign/nested attach sources.
struct CountingHost<'a, H: HostCapabilities> {
    inner: &'a mut H,
    logical_bytes_examined: u64,
    collect_diagnostics: bool,
    host_read_ns: u64,
    byte_account_ns: u64,
    host_read_calls: u64,
}

impl<'a, H: HostCapabilities> CountingHost<'a, H> {
    fn new(inner: &'a mut H, collect_diagnostics: bool) -> Self {
        Self {
            inner,
            logical_bytes_examined: 0,
            collect_diagnostics,
            host_read_ns: 0,
            byte_account_ns: 0,
            host_read_calls: 0,
        }
    }

    fn account(&mut self, value: &JsonValue) -> Result<(), Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let bytes = serde_json::to_vec(value).map_err(|error| {
            Error::Internal(format!("logical JSON byte accounting failed: {error}"))
        })?;
        self.logical_bytes_examined = self
            .logical_bytes_examined
            .saturating_add(bytes.len() as u64);
        if let Some(started) = started {
            self.byte_account_ns = self.byte_account_ns.saturating_add(elapsed_ns(started));
        }
        Ok(())
    }

    fn account_host_call(&mut self, started: Option<Instant>) {
        if let Some(started) = started {
            self.host_read_ns = self.host_read_ns.saturating_add(elapsed_ns(started));
            self.host_read_calls = self.host_read_calls.saturating_add(1);
        }
    }

    fn account_document(&mut self, document: &HostDocument) -> Result<(), Error> {
        if let HostDocument::Present {
            value,
            logical_bytes,
        } = document
        {
            if let Some(logical_bytes) = logical_bytes {
                self.logical_bytes_examined =
                    self.logical_bytes_examined.saturating_add(*logical_bytes);
                Ok(())
            } else {
                self.account(value.as_value())
            }
        } else {
            Ok(())
        }
    }
}

impl<H: HostCapabilities> HostCapabilities for CountingHost<'_, H> {
    fn list_keys(
        &mut self,
        collection_id: CollectionId,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let result = self.inner.list_keys(collection_id, limit, after_key);
        self.account_host_call(started);
        result
    }

    fn get_json(
        &mut self,
        collection_id: CollectionId,
        key: &str,
    ) -> Result<Option<JsonValue>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let value = self.inner.get_json(collection_id, key)?;
        self.account_host_call(started);
        if let Some(value) = value.as_ref() {
            self.account(value)?;
        }
        Ok(value)
    }

    fn get_json_covered(
        &mut self,
        collection_id: CollectionId,
        key: &str,
    ) -> Result<HostDocument, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let document = self.inner.get_json_covered(collection_id, key)?;
        self.account_host_call(started);
        self.account_document(&document)?;
        Ok(document)
    }

    fn scan_json_covered_page(
        &mut self,
        collection_id: CollectionId,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<(String, HostDocument)>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let documents = self
            .inner
            .scan_json_covered_page(collection_id, limit, after_key)?;
        self.account_host_call(started);
        for (_, document) in &documents {
            self.account_document(document)?;
        }
        Ok(documents)
    }

    fn get_json_covered_many(
        &mut self,
        collection_id: CollectionId,
        keys: &[String],
    ) -> Result<Vec<(String, HostDocument)>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let documents = self.inner.get_json_covered_many(collection_id, keys)?;
        self.account_host_call(started);
        for (_, document) in &documents {
            self.account_document(document)?;
        }
        Ok(documents)
    }

    fn lookup_index_keys(
        &mut self,
        collection_id: CollectionId,
        equalities: &[(String, JsonValue)],
    ) -> Result<Option<Vec<String>>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let result = self.inner.lookup_index_keys(collection_id, equalities);
        self.account_host_call(started);
        result
    }

    fn lookup_index_keys_many(
        &mut self,
        collection_id: CollectionId,
        field: &str,
        values: &[JsonValue],
    ) -> Result<Option<Vec<String>>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let result = self
            .inner
            .lookup_index_keys_many(collection_id, field, values);
        self.account_host_call(started);
        result
    }

    fn lookup_index_keys_with_constraints(
        &mut self,
        collection_id: CollectionId,
        equalities: &[(String, JsonValue)],
        constrained_fields: &[String],
    ) -> Result<Option<Vec<String>>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let result = self.inner.lookup_index_keys_with_constraints(
            collection_id,
            equalities,
            constrained_fields,
        );
        self.account_host_call(started);
        result
    }

    fn lookup_ordered_index_keys(
        &mut self,
        collection_id: CollectionId,
        order: &[OrderTerm],
    ) -> Result<Option<Vec<String>>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let result = self.inner.lookup_ordered_index_keys(collection_id, order);
        self.account_host_call(started);
        result
    }

    fn source_frontier(&mut self, collection_id: CollectionId) -> Result<Option<[u8; 32]>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let result = self.inner.source_frontier(collection_id);
        self.account_host_call(started);
        result
    }

    fn lookup_covering_index_values(
        &mut self,
        collection_id: CollectionId,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<JsonValue>>>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let result = self
            .inner
            .lookup_covering_index_values(collection_id, fields);
        self.account_host_call(started);
        result
    }

    fn scan_projected_values(
        &mut self,
        collection_id: CollectionId,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<JsonValue>>>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let result = self.inner.scan_projected_values(collection_id, fields);
        self.account_host_call(started);
        result
    }

    fn scan_group_aggregate(
        &mut self,
        collection_id: CollectionId,
        spec: &crate::plan_v1::GroupAggSpec,
        where_k: &super::CompiledKernelWhere,
        candidate_keys: Option<&[String]>,
    ) -> Result<Option<super::HostGroupAggregate>, Error> {
        let started = self.collect_diagnostics.then(Instant::now);
        let result = self
            .inner
            .scan_group_aggregate(collection_id, spec, where_k, candidate_keys);
        self.account_host_call(started);
        result
    }
}

/// Bound-collection [`DocScan`] over [`HostCapabilities`] for Core opcodes.
struct HostScan<'a, H: HostCapabilities> {
    host: &'a mut H,
    collection_id: CollectionId,
}

impl<H: HostCapabilities> DocScan for HostScan<'_, H> {
    fn list_keys(
        &mut self,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        self.host.list_keys(self.collection_id, limit, after_key)
    }

    fn get_json(&mut self, key: &str) -> Result<Option<JsonValue>, Error> {
        self.host.get_json(self.collection_id, key)
    }

    fn get_json_covered(&mut self, key: &str) -> Result<HostDocument, Error> {
        self.host.get_json_covered(self.collection_id, key)
    }

    fn scan_json_covered_page(
        &mut self,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<(String, HostDocument)>, Error> {
        self.host
            .scan_json_covered_page(self.collection_id, limit, after_key)
    }

    fn get_json_covered_many(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<(String, HostDocument)>, Error> {
        self.host.get_json_covered_many(self.collection_id, keys)
    }

    fn try_equality_index_keys(
        &mut self,
        equalities: &[(String, JsonValue)],
    ) -> Result<Option<Vec<String>>, Error> {
        self.host.lookup_index_keys(self.collection_id, equalities)
    }

    fn try_index_keys_with_constraints(
        &mut self,
        equalities: &[(String, JsonValue)],
        constrained_fields: &[String],
    ) -> Result<Option<Vec<String>>, Error> {
        self.host.lookup_index_keys_with_constraints(
            self.collection_id,
            equalities,
            constrained_fields,
        )
    }

    fn try_ordered_index_keys(
        &mut self,
        order: &[OrderTerm],
    ) -> Result<Option<Vec<String>>, Error> {
        self.host
            .lookup_ordered_index_keys(self.collection_id, order)
    }

    fn try_source_frontier(&mut self) -> Result<Option<[u8; 32]>, Error> {
        self.host.source_frontier(self.collection_id)
    }

    fn try_covering_index_values(
        &mut self,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<JsonValue>>>, Error> {
        self.host
            .lookup_covering_index_values(self.collection_id, fields)
    }

    fn try_projected_values(
        &mut self,
        fields: &[String],
    ) -> Result<Option<Vec<Vec<JsonValue>>>, Error> {
        self.host.scan_projected_values(self.collection_id, fields)
    }

    fn try_group_aggregate(
        &mut self,
        spec: &crate::plan_v1::GroupAggSpec,
        where_k: &super::CompiledKernelWhere,
        candidate_keys: Option<&[String]>,
    ) -> Result<Option<super::HostGroupAggregate>, Error> {
        self.host
            .scan_group_aggregate(self.collection_id, spec, where_k, candidate_keys)
    }
}

/// Result of one [`run_vm`] pass (Core page + optional attach rows).
#[derive(Debug, Clone)]
pub(crate) struct VmOutcome {
    /// Core page (Bind…ProjectPaths).
    pub page: QueryPage,
    /// Working rows after attach (equals `page.rows` when no Full opcodes ran).
    pub rows: Vec<(String, JsonValue)>,
    /// Root enrich load evidence (empty for Core-only programs).
    pub enrich_loads: Vec<EnrichLoadEvidence>,
}

/// Open Within scope: parents saved; working set is carrier elements.
struct WithinScope {
    carrier: Path,
    parents: Vec<(String, JsonValue)>,
}

/// Core `ProjectPaths` immediate: path list + optional group/aggregate.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct ProjectImm {
    /// Path projection; `None` = full document (after optional group).
    pub paths: Option<Vec<Path>>,
    /// Group-by + aggregates; inactive when empty.
    pub group_agg: crate::plan_v1::GroupAggSpec,
}

impl ProjectImm {
    /// Path-only project (no group).
    pub fn paths_only(paths: Option<Vec<Path>>) -> Self {
        Self {
            paths,
            group_agg: crate::plan_v1::GroupAggSpec::default(),
        }
    }
}

/// Typed immediate for one VM instruction (operands live on the opcode).
///
/// Core opcodes no longer recover semantics from a full [`RqlPlanV1`] sidecar —
/// each instruction carries its own typed operands (**RQL-QVM typed pool**).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum VmImm {
    /// Bind / verify collection id.
    Collection(CollectionId),
    /// IndexEq: force-scan only; where is sole authority on Filter (`Where`).
    IndexEq { force_scan: bool },
    /// Scan has no payload (key-source mode is derived from order at run start).
    None,
    /// Core Filter where predicate.
    Where(Predicate),
    /// Core order terms (includes key tie-break).
    Order(Vec<OrderTerm>),
    /// Core page size + optional total limit.
    Page { page_size: u32, limit: Option<u64> },
    /// Core path projection + optional group/aggregate payload.
    Project(ProjectImm),
    /// Enrich step (root or nested inside Within…WithinEnd).
    Enrich(EnrichStepV1),
    /// Within shell: carrier + alias only; body is stream ops until WithinEnd.
    Within(WithinStepV1),
    /// Post-attach filter (root or nested).
    FilterAttach(Predicate),
    /// Brace project.
    ProjectBrace(Vec<ProjectItemV1>),
}

/// Constant pool for page policy (**RQL-QVM typed**).
///
/// Does **not** hold a full logical plan or trusted cursor identity.
/// Cursor / page identity is the **canonical QVM program hash** computed over
/// the durable bytes after encode (see [`super::qvm::qvm_hash`]), not an
/// embedded wire field.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VmPool {
    /// Coverage policy for the Core page artefact.
    pub coverage: CoveragePolicy,
    /// Consistency mode stamped on the plan at compile time.
    pub consistency: ConsistencyMode,
}

/// One typed VM instruction.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VmInstr {
    /// Opcode.
    pub op: OpCode,
    /// Immediate.
    pub imm: VmImm,
}

/// Lowered Query VM program (Core or Full) — **no plan/pipeline/project sidecars**.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VmProgram {
    /// Profile stamp.
    pub profile: &'static str,
    /// Instruction stream (executable with [`VmPool`]).
    pub ops: Vec<VmInstr>,
    /// Constant pool (coverage / consistency only).
    pub pool: VmPool,
    /// Optional execute budget from ISA/QVM.
    pub budget: Option<QueryBudget>,
    /// Canonical QVM program hash (cursor / page identity).
    ///
    /// Set by [`super::qvm::decode_qvm`] / encode path from the **complete**
    /// durable bytes — never trusted as an independent wire field.
    pub program_hash: [u8; 32],
}

/// Core operands extracted from a verified opcode stream (not a plan sidecar).
#[derive(Debug, Clone)]
pub(crate) struct CoreOperands {
    pub where_pred: Predicate,
    pub order: Vec<OrderTerm>,
    pub project: ProjectImm,
    pub page_size: u32,
    pub limit: Option<u64>,
    #[allow(dead_code)]
    pub force_scan: bool,
}

/// Extract typed Core operands from the instruction stream.
///
/// Call after [`verify_vm_program`]. Operands come only from opcode immediates.
pub(crate) fn extract_core_operands(ops: &[VmInstr]) -> Result<CoreOperands, Error> {
    let mut where_pred = None;
    let mut order = None;
    let mut project = None;
    let mut page_size = None;
    let mut limit = None;
    let mut force_scan = false;
    for i in ops {
        match (&i.op, &i.imm) {
            (OpCode::IndexEq, VmImm::IndexEq { force_scan: fs }) => {
                force_scan = *fs;
            }
            (OpCode::Filter, VmImm::Where(w)) => {
                // Filter is the sole where authority.
                where_pred = Some(w.clone());
            }
            (OpCode::Order, VmImm::Order(o)) => {
                order = Some(o.clone());
            }
            (
                OpCode::Page,
                VmImm::Page {
                    page_size: ps,
                    limit: lim,
                },
            ) => {
                page_size = Some(*ps);
                limit = *lim;
            }
            (OpCode::ProjectPaths, VmImm::Project(p)) => {
                project = Some(p.clone());
            }
            _ => {}
        }
    }
    Ok(CoreOperands {
        where_pred: where_pred
            .ok_or_else(|| Error::QueryInvalid("qvm: missing Core Filter Where".into()))?,
        order: order.ok_or_else(|| Error::QueryInvalid("qvm: missing Core Order".into()))?,
        project: project
            .ok_or_else(|| Error::QueryInvalid("qvm: missing Core ProjectPaths".into()))?,
        page_size: page_size.ok_or_else(|| Error::QueryInvalid("qvm: missing Core Page".into()))?,
        limit,
        force_scan,
    })
}

/// Verify a lowered program: grammar, operands, Within balance, single terminal Halt.
pub(crate) fn verify_vm_program(prog: &VmProgram) -> Result<(), Error> {
    if prog.profile != VM_PROFILE {
        return Err(Error::QueryInvalid(format!(
            "verify: profile mismatch {:?}",
            prog.profile
        )));
    }
    if prog.ops.is_empty() {
        return Err(Error::QueryInvalid("verify: empty program".into()));
    }
    // Exactly one Halt, must be final.
    let halt_positions: Vec<usize> = prog
        .ops
        .iter()
        .enumerate()
        .filter(|(_, i)| i.op == OpCode::Halt)
        .map(|(i, _)| i)
        .collect();
    if halt_positions.len() != 1 {
        return Err(Error::QueryInvalid(format!(
            "verify: require exactly one Halt, found {}",
            halt_positions.len()
        )));
    }
    if *halt_positions.last().unwrap() != prog.ops.len() - 1 {
        return Err(Error::QueryInvalid(
            "verify: Halt must be the final instruction (no unreachable ops)".into(),
        ));
    }
    // Core prefix grammar.
    let need = [
        OpCode::BindCollection,
        OpCode::IndexEq,
        OpCode::Scan,
        OpCode::Filter,
        OpCode::Order,
        OpCode::Page,
        OpCode::ProjectPaths,
    ];
    if prog.ops.len() < need.len() + 1 {
        return Err(Error::QueryInvalid(
            "verify: program shorter than Core prefix + Halt".into(),
        ));
    }
    for (i, op) in need.iter().enumerate() {
        if prog.ops[i].op != *op {
            return Err(Error::QueryInvalid(format!(
                "verify: Core prefix[{i}] want {}, got {}",
                op.name(),
                prog.ops[i].op.name()
            )));
        }
    }
    // Operand compatibility for Core prefix.
    match &prog.ops[0].imm {
        VmImm::Collection(_) => {}
        _ => {
            return Err(Error::QueryInvalid(
                "verify: BindCollection requires Collection imm".into(),
            ))
        }
    }
    match &prog.ops[1].imm {
        VmImm::IndexEq { .. } => {}
        _ => {
            return Err(Error::QueryInvalid(
                "verify: IndexEq requires IndexEq{force_scan} imm".into(),
            ))
        }
    }
    // Filter is sole where authority; IndexEq must not carry a predicate.
    if !matches!(prog.ops[2].imm, VmImm::None) {
        return Err(Error::QueryInvalid("verify: Scan requires None imm".into()));
    }
    if !matches!(prog.ops[3].imm, VmImm::Where(_)) {
        return Err(Error::QueryInvalid(
            "verify: Filter requires Where imm".into(),
        ));
    }
    if !matches!(prog.ops[4].imm, VmImm::Order(_)) {
        return Err(Error::QueryInvalid(
            "verify: Order requires Order imm".into(),
        ));
    }
    if !matches!(prog.ops[5].imm, VmImm::Page { .. }) {
        return Err(Error::QueryInvalid("verify: Page requires Page imm".into()));
    }
    if !matches!(prog.ops[6].imm, VmImm::Project(_)) {
        return Err(Error::QueryInvalid(
            "verify: ProjectPaths requires Project imm".into(),
        ));
    }
    // Attach section + balanced Within; Halt already checked as final.
    let mut depth = 0i32;
    for (i, instr) in prog.ops.iter().enumerate().skip(need.len()) {
        match instr.op {
            OpCode::Halt => {
                if !matches!(instr.imm, VmImm::None) {
                    return Err(Error::QueryInvalid("verify: Halt requires None imm".into()));
                }
                if depth != 0 {
                    return Err(Error::QueryInvalid(
                        "verify: Halt with unbalanced Within".into(),
                    ));
                }
            }
            OpCode::Enrich => {
                if !matches!(instr.imm, VmImm::Enrich(_)) {
                    return Err(Error::QueryInvalid(format!(
                        "verify: Enrich imm mismatch at {i}"
                    )));
                }
            }
            OpCode::Within => {
                match &instr.imm {
                    VmImm::Within(w) if w.steps.is_empty() => {}
                    VmImm::Within(_) => {
                        return Err(Error::QueryInvalid(
                            "verify: Within imm must be shell (empty steps)".into(),
                        ))
                    }
                    _ => {
                        return Err(Error::QueryInvalid(format!(
                            "verify: Within imm mismatch at {i}"
                        )))
                    }
                }
                depth += 1;
            }
            OpCode::WithinEnd => {
                if !matches!(instr.imm, VmImm::None) {
                    return Err(Error::QueryInvalid(
                        "verify: WithinEnd requires None imm".into(),
                    ));
                }
                depth -= 1;
                if depth < 0 {
                    return Err(Error::QueryInvalid(
                        "verify: WithinEnd without matching Within".into(),
                    ));
                }
            }
            OpCode::FilterAttach => {
                if !matches!(instr.imm, VmImm::FilterAttach(_)) {
                    return Err(Error::QueryInvalid(format!(
                        "verify: FilterAttach imm mismatch at {i}"
                    )));
                }
            }
            OpCode::ProjectBrace => {
                if !matches!(instr.imm, VmImm::ProjectBrace(_)) {
                    return Err(Error::QueryInvalid(format!(
                        "verify: ProjectBrace imm mismatch at {i}"
                    )));
                }
                if depth != 0 {
                    return Err(Error::QueryInvalid(
                        "verify: ProjectBrace inside Within is illegal".into(),
                    ));
                }
            }
            OpCode::BindCollection
            | OpCode::IndexEq
            | OpCode::Scan
            | OpCode::Filter
            | OpCode::Order
            | OpCode::Page
            | OpCode::ProjectPaths => {
                return Err(Error::QueryInvalid(format!(
                    "verify: Core opcode {} only legal in Core prefix (at {i})",
                    instr.op.name()
                )));
            }
        }
    }
    Ok(())
}

/// Lower a Core plan into the canonical opcode sequence with typed immediates.
pub(crate) fn lower_core(core: RqlPlanV1, budget: Option<QueryBudget>) -> VmProgram {
    lower_core_with_force_scan(core, budget, false)
}

/// Lower Core with optional force_scan (dialect QueryOptions).
pub(crate) fn lower_core_with_force_scan(
    core: RqlPlanV1,
    budget: Option<QueryBudget>,
    force_scan: bool,
) -> VmProgram {
    let id = core.from.collection_id;
    let where_pred = core.where_pred.clone();
    let order = core.order.clone();
    let project = ProjectImm {
        paths: core.project.clone(),
        group_agg: core.group_agg.clone(),
    };
    let page_size = core.page_size;
    let limit = core.limit;
    let pool = VmPool {
        coverage: core.coverage,
        consistency: core.consistency,
    };
    let ops = vec![
        VmInstr {
            op: OpCode::BindCollection,
            imm: VmImm::Collection(id),
        },
        VmInstr {
            op: OpCode::IndexEq,
            imm: VmImm::IndexEq { force_scan },
        },
        VmInstr {
            op: OpCode::Scan,
            imm: VmImm::None,
        },
        VmInstr {
            op: OpCode::Filter,
            imm: VmImm::Where(where_pred),
        },
        VmInstr {
            op: OpCode::Order,
            imm: VmImm::Order(order),
        },
        VmInstr {
            op: OpCode::Page,
            imm: VmImm::Page { page_size, limit },
        },
        VmInstr {
            op: OpCode::ProjectPaths,
            imm: VmImm::Project(project),
        },
        VmInstr {
            op: OpCode::Halt,
            imm: VmImm::None,
        },
    ];
    VmProgram {
        profile: VM_PROFILE,
        ops,
        pool,
        budget,
        // Filled by encode/decode (canonical QVM hash of complete program).
        program_hash: [0u8; 32],
    }
}

/// Emit Full pipeline steps onto the opcode stream (**RQL-VM4**).
///
/// Nested Within bodies expand between `Within` (shell imm) and `WithinEnd`.
fn emit_attach_pipeline(ops: &mut Vec<VmInstr>, pipeline: &[FullPipelineStepV1]) {
    for step in pipeline {
        match step {
            FullPipelineStepV1::Enrich(e) => ops.push(VmInstr {
                op: OpCode::Enrich,
                imm: VmImm::Enrich(e.clone()),
            }),
            FullPipelineStepV1::Within(w) => {
                ops.push(VmInstr {
                    op: OpCode::Within,
                    imm: VmImm::Within(WithinStepV1 {
                        carrier: w.carrier.clone(),
                        element_alias: w.element_alias.clone(),
                        steps: Vec::new(),
                    }),
                });
                emit_attach_pipeline(ops, &w.steps);
                ops.push(VmInstr {
                    op: OpCode::WithinEnd,
                    imm: VmImm::None,
                });
            }
            FullPipelineStepV1::Filter(p) => ops.push(VmInstr {
                op: OpCode::FilterAttach,
                imm: VmImm::FilterAttach(p.clone()),
            }),
        }
    }
}

/// Lower Core + Full attach section into one program.
pub(crate) fn lower_full(
    core: RqlPlanV1,
    budget: Option<QueryBudget>,
    pipeline: Vec<FullPipelineStepV1>,
    project: Option<Vec<ProjectItemV1>>,
) -> VmProgram {
    let mut prog = lower_core(core, budget);
    // Insert attach ops before Halt.
    let halt = prog.ops.pop().expect("Halt");
    emit_attach_pipeline(&mut prog.ops, &pipeline);
    if let Some(fields) = project {
        prog.ops.push(VmInstr {
            op: OpCode::ProjectBrace,
            imm: VmImm::ProjectBrace(fields),
        });
    }
    prog.ops.push(halt);
    prog
}

/// One Query VM dispatch for Core and Full programs (**RQL-VM1R**).
///
/// Core opcodes call [`CoreFrame`] phase helpers; after `ProjectPaths`, Full
/// opcodes continue in the same loop (no second dispatcher / Core-prefix skip).
pub(crate) fn run_vm<H: HostCapabilities>(
    host: &mut H,
    prog: &VmProgram,
    params: &BTreeMap<String, JsonValue>,
    options: &QueryRunOptions,
    heap_id: HeapId,
    collection_id: CollectionId,
    force_enrich_scan: bool,
) -> Result<VmOutcome, Error> {
    if prog.profile != VM_PROFILE {
        return Err(Error::QueryInvalid(format!(
            "run_vm: profile mismatch: got {:?}, want {VM_PROFILE}",
            prog.profile
        )));
    }
    let mut diagnostics = options.diagnostics.then(QueryDiagnostics::default);
    let verify_started = diagnostics.as_ref().map(|_| Instant::now());
    verify_vm_program(prog)?;
    if let (Some(diagnostics), Some(started)) = (&mut diagnostics, verify_started) {
        diagnostics.verify_ns = elapsed_ns(started);
    }
    let mut host = CountingHost::new(host, options.diagnostics);
    let mut pc = 0usize;
    let mut frame: Option<CoreFrame<'_>> = None;
    let mut page: Option<QueryPage> = None;
    let mut rows: Vec<(String, JsonValue)> = Vec::new();
    let mut enrich_loads: Vec<EnrichLoadEvidence> = Vec::new();
    let mut foreign_cache: BTreeMap<CollectionId, Vec<(String, JsonValue)>> = BTreeMap::new();
    let mut within_stack: Vec<WithinScope> = Vec::new();
    let budget = merge_budgets(prog.budget, options.budget);

    while pc < prog.ops.len() {
        let instr = &prog.ops[pc];
        let phase_started = diagnostics.as_ref().map(|_| Instant::now());
        match instr.op {
            OpCode::BindCollection => {
                let VmImm::Collection(id) = &instr.imm else {
                    return Err(Error::QueryInvalid(
                        "run_vm: BindCollection immediate mismatch".into(),
                    ));
                };
                if *id != collection_id {
                    return Err(Error::QueryInvalid(
                        "run_vm: BindCollection id mismatch".into(),
                    ));
                }
                if page.is_some() {
                    return Err(Error::QueryInvalid(
                        "run_vm: BindCollection after Core page".into(),
                    ));
                }
                let core_ops = extract_core_operands(&prog.ops)?;
                frame = Some(CoreFrame::begin(
                    prog.program_hash,
                    &prog.pool,
                    &core_ops,
                    params,
                    options,
                    heap_id,
                    collection_id,
                    prog.budget,
                )?);
                pc += 1;
            }
            OpCode::IndexEq => {
                let f = frame.as_mut().ok_or_else(|| {
                    Error::QueryInvalid("run_vm: IndexEq before BindCollection".into())
                })?;
                let VmImm::IndexEq { force_scan } = &instr.imm else {
                    return Err(Error::QueryInvalid(
                        "run_vm: IndexEq immediate mismatch".into(),
                    ));
                };
                // Where is sole authority on Filter; IndexEq only probes equalities.
                let where_pred = match prog
                    .ops
                    .iter()
                    .find(|i| i.op == OpCode::Filter)
                    .map(|i| &i.imm)
                {
                    Some(VmImm::Where(w)) => w,
                    _ => {
                        return Err(Error::QueryInvalid(
                            "run_vm: IndexEq requires Filter Where in program".into(),
                        ))
                    }
                };
                let mut scan = HostScan {
                    host: &mut host,
                    collection_id,
                };
                f.index_eq(&mut scan, where_pred, *force_scan)?;
                pc += 1;
            }
            OpCode::Scan => {
                let f = frame.as_mut().ok_or_else(|| {
                    Error::QueryInvalid("run_vm: Scan before BindCollection".into())
                })?;
                let mut scan = HostScan {
                    host: &mut host,
                    collection_id,
                };
                f.scan(&mut scan)?;
                pc += 1;
            }
            OpCode::Filter => {
                let f = frame.as_mut().ok_or_else(|| {
                    Error::QueryInvalid("run_vm: Filter before BindCollection".into())
                })?;
                if !matches!(instr.imm, VmImm::Where(_)) {
                    return Err(Error::QueryInvalid(
                        "run_vm: Filter immediate mismatch".into(),
                    ));
                }
                let mut scan = HostScan {
                    host: &mut host,
                    collection_id,
                };
                f.filter(&mut scan)?;
                pc += 1;
            }
            OpCode::Order => {
                let f = frame.as_mut().ok_or_else(|| {
                    Error::QueryInvalid("run_vm: Order before BindCollection".into())
                })?;
                if !matches!(instr.imm, VmImm::Order(_)) {
                    return Err(Error::QueryInvalid(
                        "run_vm: Order immediate mismatch".into(),
                    ));
                }
                f.order()?;
                pc += 1;
            }
            OpCode::Page => {
                let f = frame.as_mut().ok_or_else(|| {
                    Error::QueryInvalid("run_vm: Page before BindCollection".into())
                })?;
                if !matches!(instr.imm, VmImm::Page { .. }) {
                    return Err(Error::QueryInvalid(
                        "run_vm: Page immediate mismatch".into(),
                    ));
                }
                f.page()?;
                pc += 1;
            }
            OpCode::ProjectPaths => {
                let f = frame.as_mut().ok_or_else(|| {
                    Error::QueryInvalid("run_vm: ProjectPaths before BindCollection".into())
                })?;
                if !matches!(instr.imm, VmImm::Project(_)) {
                    return Err(Error::QueryInvalid(
                        "run_vm: ProjectPaths immediate mismatch".into(),
                    ));
                }
                let mut scan = HostScan {
                    host: &mut host,
                    collection_id,
                };
                let p = f.project_paths(&mut scan)?;
                rows = p
                    .rows
                    .iter()
                    .map(|r| (r.key.clone(), r.value.clone()))
                    .collect();
                page = Some(p);
                // Core ProjectPaths has already enforced the page's query and
                // host result-memory limits. Full checks resume after the
                // first opcode that can expand or retain additional values.
                // CoreFrame no longer needed; free borrow of pool/params.
                frame = None;
                pc += 1;
            }
            OpCode::Enrich => {
                if page.is_none() {
                    return Err(Error::QueryInvalid(
                        "run_vm: Enrich before ProjectPaths".into(),
                    ));
                }
                let VmImm::Enrich(e) = &instr.imm else {
                    return Err(Error::QueryInvalid(
                        "run_vm: Enrich immediate mismatch".into(),
                    ));
                };
                if within_stack.is_empty() {
                    let (foreign, mode) =
                        load_foreign_docs_for_root_enrich(&mut host, e, &rows, force_enrich_scan)?;
                    enrich_loads.push(EnrichLoadEvidence {
                        using: e.using_name.clone(),
                        output: e.output.clone(),
                        mode,
                    });
                    if mode == EnrichAttachMode::Scan {
                        foreign_cache
                            .entry(e.using_id)
                            .or_insert_with(|| foreign.clone());
                    }
                    rows = attach_enrich_rows_owned(rows, &foreign, e, params)?;
                } else {
                    ensure_foreign_docs(&mut host, e.using_id, &mut foreign_cache)?;
                    let foreign = foreign_cache.get(&e.using_id).ok_or_else(|| {
                        Error::QueryInvalid(format!(
                            "within attach missing foreign docs for id {} (`{}`)",
                            e.using_id, e.using_name
                        ))
                    })?;
                    rows = attach_enrich_rows_owned(rows, foreign, e, params)?;
                }
                check_full_materialization(&rows, &foreign_cache, budget)?;
                pc += 1;
            }
            OpCode::Within => {
                if page.is_none() {
                    return Err(Error::QueryInvalid(
                        "run_vm: Within before ProjectPaths".into(),
                    ));
                }
                let VmImm::Within(w) = &instr.imm else {
                    return Err(Error::QueryInvalid(
                        "run_vm: Within immediate mismatch".into(),
                    ));
                };
                if !w.steps.is_empty() {
                    return Err(Error::QueryInvalid(
                        "run_vm: Within immediate must be a shell (empty steps); body is stream ops"
                            .into(),
                    ));
                }
                let parents = std::mem::take(&mut rows);
                rows = within_enter(&parents, &w.carrier)?;
                within_stack.push(WithinScope {
                    carrier: w.carrier.clone(),
                    parents,
                });
                check_full_materialization(&rows, &foreign_cache, budget)?;
                pc += 1;
            }
            OpCode::WithinEnd => {
                let scope = within_stack.pop().ok_or_else(|| {
                    Error::QueryInvalid("run_vm: WithinEnd without matching Within".into())
                })?;
                rows = within_leave(&scope.parents, &scope.carrier, &rows)?;
                check_full_materialization(&rows, &foreign_cache, budget)?;
                pc += 1;
            }
            OpCode::FilterAttach => {
                if page.is_none() {
                    return Err(Error::QueryInvalid(
                        "run_vm: FilterAttach before ProjectPaths".into(),
                    ));
                }
                let VmImm::FilterAttach(pred) = &instr.imm else {
                    return Err(Error::QueryInvalid(
                        "run_vm: FilterAttach immediate mismatch".into(),
                    ));
                };
                rows = filter_rows(&rows, pred, params)?;
                check_full_materialization(&rows, &foreign_cache, budget)?;
                pc += 1;
            }
            OpCode::ProjectBrace => {
                if page.is_none() {
                    return Err(Error::QueryInvalid(
                        "run_vm: ProjectBrace before ProjectPaths".into(),
                    ));
                }
                let VmImm::ProjectBrace(fields) = &instr.imm else {
                    return Err(Error::QueryInvalid(
                        "run_vm: ProjectBrace immediate mismatch".into(),
                    ));
                };
                if !within_stack.is_empty() {
                    return Err(Error::QueryInvalid(
                        "run_vm: ProjectBrace inside Within is not supported".into(),
                    ));
                }
                rows = apply_project_rows(&rows, fields, params)?;
                check_full_materialization(&rows, &foreign_cache, budget)?;
                pc += 1;
            }
            OpCode::Halt => {
                if !within_stack.is_empty() {
                    return Err(Error::QueryInvalid(
                        "run_vm: Halt with open Within scope".into(),
                    ));
                }
                if pc + 1 != prog.ops.len() {
                    return Err(Error::QueryInvalid(
                        "run_vm: Halt must be terminal (unreachable ops after Halt)".into(),
                    ));
                }
                if page.is_none() {
                    return Err(Error::QueryInvalid("run_vm: Halt without Core page".into()));
                }
                pc += 1;
            }
        }
        if let (Some(diagnostics), Some(started)) = (&mut diagnostics, phase_started) {
            let elapsed = elapsed_ns(started);
            match instr.op {
                OpCode::BindCollection => {
                    diagnostics.bind_ns = diagnostics.bind_ns.saturating_add(elapsed)
                }
                OpCode::IndexEq => {
                    diagnostics.index_ns = diagnostics.index_ns.saturating_add(elapsed)
                }
                OpCode::Scan => diagnostics.scan_ns = diagnostics.scan_ns.saturating_add(elapsed),
                OpCode::Filter => {
                    diagnostics.filter_ns = diagnostics.filter_ns.saturating_add(elapsed)
                }
                OpCode::Order => {
                    diagnostics.order_ns = diagnostics.order_ns.saturating_add(elapsed)
                }
                OpCode::Page => diagnostics.page_ns = diagnostics.page_ns.saturating_add(elapsed),
                OpCode::ProjectPaths => {
                    diagnostics.project_ns = diagnostics.project_ns.saturating_add(elapsed)
                }
                OpCode::Enrich
                | OpCode::Within
                | OpCode::WithinEnd
                | OpCode::FilterAttach
                | OpCode::ProjectBrace => {
                    diagnostics.attach_ns = diagnostics.attach_ns.saturating_add(elapsed)
                }
                OpCode::Halt => {}
            }
        }
    }

    let mut page =
        page.ok_or_else(|| Error::QueryInvalid("run_vm: finished without Core page".into()))?;
    page.logical_bytes_examined = host.logical_bytes_examined;
    if let Some(diagnostics) = &mut diagnostics {
        diagnostics.host_read_ns = host.host_read_ns;
        diagnostics.byte_account_ns = host.byte_account_ns;
        diagnostics.host_read_calls = host.host_read_calls;
    }
    page.diagnostics = diagnostics;
    Ok(VmOutcome {
        page,
        rows,
        enrich_loads,
    })
}

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

/// Bound both the public attached result and the VM's total retained
/// materialisation (result rows + foreign caches). Core checks its own page;
/// Full opcodes can expand that page and therefore must re-check here.
fn check_full_materialization(
    rows: &[(String, JsonValue)],
    foreign_cache: &BTreeMap<CollectionId, Vec<(String, JsonValue)>>,
    budget: Option<QueryBudget>,
) -> Result<(), Error> {
    let row_bytes = rows.iter().fold(0u64, |used, (key, value)| {
        used.saturating_add(estimate_row_bytes(key, value))
    });
    let limits = host_limits();
    check_result_bytes(row_bytes, budget.and_then(|b| b.max_result_bytes), &limits)?;
    let retained_bytes = foreign_cache
        .values()
        .flatten()
        .fold(row_bytes, |used, (key, value)| {
            used.saturating_add(estimate_row_bytes(key, value))
        });
    check_result_bytes(retained_bytes, None, &limits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_v1::{CollectionBindings, PlanBuilder};
    use crate::predicate::Path;
    use residiuum_heap::CollectionId;

    fn uuidish(seed: u8) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0] = seed;
        b[6] = (b[6] & 0x0f) | 0x40;
        b[8] = (b[8] & 0x3f) | 0x80;
        b
    }

    fn empty_core(id: CollectionId) -> RqlPlanV1 {
        let mut bindings = CollectionBindings::default();
        bindings.bind("items", id);
        PlanBuilder::from_source("items")
            .compile(&bindings)
            .expect("plan")
    }

    struct ByteHost {
        value: JsonValue,
    }

    impl HostCapabilities for ByteHost {
        fn list_keys(
            &mut self,
            _collection_id: CollectionId,
            _limit: Option<usize>,
            _after_key: Option<&str>,
        ) -> Result<Vec<String>, Error> {
            Ok(vec!["present".into(), "absent".into()])
        }

        fn get_json(
            &mut self,
            _collection_id: CollectionId,
            key: &str,
        ) -> Result<Option<JsonValue>, Error> {
            Ok((key == "present").then(|| self.value.clone()))
        }
    }

    #[test]
    fn counting_host_counts_each_successful_json_load_only() {
        let id = CollectionId::from_bytes(uuidish(9)).expect("id");
        let value = serde_json::json!({"a": [1, 2], "s": "x"});
        let expected = serde_json::to_vec(&value).unwrap().len() as u64;
        let mut inner = ByteHost { value };
        let mut host = CountingHost::new(&mut inner, false);

        assert!(host.get_json(id, "present").unwrap().is_some());
        assert!(matches!(
            host.get_json_covered(id, "present").unwrap(),
            HostDocument::Present { .. }
        ));
        assert!(host.get_json(id, "absent").unwrap().is_none());
        assert_eq!(host.logical_bytes_examined, expected * 2);
    }

    #[test]
    fn counting_host_prefers_preserved_logical_byte_length() {
        let value = serde_json::json!({"would": "otherwise be serialized"});
        let mut inner = ByteHost {
            value: value.clone(),
        };
        let mut host = CountingHost::new(&mut inner, false);

        host.account_document(&HostDocument::Present {
            value: value.into(),
            logical_bytes: Some(17),
        })
        .expect("account preserved length");

        assert_eq!(host.logical_bytes_examined, 17);
    }

    #[test]
    fn lower_core_shape() {
        let id = CollectionId::from_bytes(uuidish(1)).expect("id");
        let prog = lower_core(empty_core(id), None);
        assert_eq!(prog.profile, VM_PROFILE);
        let names: Vec<_> = prog.ops.iter().map(|o| o.op.name()).collect();
        assert_eq!(
            names,
            [
                "BindCollection",
                "IndexEq",
                "Scan",
                "Filter",
                "Order",
                "Page",
                "ProjectPaths",
                "Halt",
            ]
        );
        assert_eq!(prog.ops.len(), 8);
    }

    #[test]
    fn lower_full_emits_enrich_and_within_end() {
        let id = CollectionId::from_bytes(uuidish(2)).expect("id");
        let fid = CollectionId::from_bytes(uuidish(3)).expect("id");
        let pipeline = vec![
            FullPipelineStepV1::Enrich(EnrichStepV1 {
                output: "f".into(),
                using_name: "foreign".into(),
                using_id: fid,
                left: Path::parse_dotted("a").unwrap(),
                right: Path::parse_dotted("b").unwrap(),
                candidate_where: None,
                expect: super::super::full_attach::EnrichCardinality::Optional,
            }),
            FullPipelineStepV1::Within(WithinStepV1 {
                carrier: Path::parse_dotted("items").unwrap(),
                element_alias: None,
                steps: Vec::new(),
            }),
        ];
        let prog = lower_full(empty_core(id), None, pipeline, None);
        let names: Vec<_> = prog.ops.iter().map(|o| o.op.name()).collect();
        assert!(names.contains(&"Enrich"));
        assert!(names.contains(&"Within"));
        assert!(names.contains(&"WithinEnd"));
        assert_eq!(*names.last().unwrap(), "Halt");
    }

    #[test]
    fn lower_full_flattens_nested_within_body() {
        let id = CollectionId::from_bytes(uuidish(4)).expect("id");
        let fid = CollectionId::from_bytes(uuidish(5)).expect("id");
        let nested_enrich = EnrichStepV1 {
            output: "sku".into(),
            using_name: "products".into(),
            using_id: fid,
            left: Path::parse_dotted("product_id").unwrap(),
            right: Path::parse_dotted("id").unwrap(),
            candidate_where: None,
            expect: super::super::full_attach::EnrichCardinality::Optional,
        };
        let pipeline = vec![FullPipelineStepV1::Within(WithinStepV1 {
            carrier: Path::parse_dotted("items").unwrap(),
            element_alias: Some("item".into()),
            steps: vec![FullPipelineStepV1::Enrich(nested_enrich)],
        })];
        let prog = lower_full(empty_core(id), None, pipeline, None);
        let names: Vec<_> = prog.ops.iter().map(|o| o.op.name()).collect();
        let attach: Vec<_> = names
            .iter()
            .skip_while(|n| **n != "Within")
            .copied()
            .collect();
        assert_eq!(
            attach,
            ["Within", "Enrich", "WithinEnd", "Halt"],
            "nested enrich must be stream ops, not Within imm body"
        );
        let within_imm = prog
            .ops
            .iter()
            .find(|i| i.op == OpCode::Within)
            .expect("Within");
        match &within_imm.imm {
            VmImm::Within(w) => assert!(
                w.steps.is_empty(),
                "Within imm must be shell after VM4 flatten"
            ),
            other => panic!("expected Within imm, got {other:?}"),
        }
    }
}
