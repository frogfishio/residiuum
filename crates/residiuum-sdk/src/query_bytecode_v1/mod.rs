//! Query bytecode v1 — product runtime (Decision 0 — **still open**).
//!
//! Profile: **`residiuum-query-bytecode-v1`**
//! ISA: **`residiuum-query-isa-v1`** ([QUERY_ISA_V1.md](../../../../doc/todo/rql/QUERY_ISA_V1.md))
//!
//! **RQL-WIRE1 / QVM sole public authority:** [`QueryBytecodeV1`] holds **QVM1**
//! durable bytes. Execution is `decode_qvm` → `verify_vm_program` → `run_vm`.
//! **Q0.A10:** RQB1 is fully removed. Public product path is QVM1 only.
//!
//! **RQL-VM1R:** Core and Full run through one [`vm_exec::run_vm`] loop.
//! See [QUERY_IR_RESIDUAL.md](../../../../doc/todo/rql/QUERY_IR_RESIDUAL.md) and
//! [QUERY_VM_V1.md](../../../../doc/todo/rql/QUERY_VM_V1.md).
//!
//! Host adapters supply scan/index/get only.
//!
//! Residual (Decision 0 still open): VM dispatches, but Core/attach helpers are
//! still Rust interpreters of typed QVM operands. **RQL-C1 must not be accepted.**

mod core_page;
mod core_phases;
mod full_attach;
mod full_imm_json;
mod group_agg;
mod ir_attach;
mod ir_order;
mod ir_page;
mod ir_project;
mod kernel;
mod qvm;
mod vm;
mod vm_exec;

pub use core_page::{explain_rql_source, EXEC_PROFILE};
// Crate-private host-scan adapter trait (RQL-P0b) — implemented by
// `CollectionClient` (residual) and the VM's internal `HostScan` (product).
pub(crate) use core_page::DocScan;
pub use full_attach::{
    compile_rql_full, execute_full_qvm_on_host_with, execute_full_qvm_with, execute_rql_full,
    execute_rql_full_on_host_with, execute_rql_full_with, explain_rql_full,
    explain_rql_full_on_heap, refuse_full_language_on_core_wire, source_uses_rql_full_constructs,
    CompiledRqlFull, EnrichAttachMode, EnrichCardinality, EnrichLoadEvidence, EnrichStepV1,
    FullPipelineStepV1, ProjectExprV1, ProjectItemV1, RqlFullExecuteOptions, RqlFullPage,
    WithinStepV1, DIAG_RQL_ENRICH_CARDINALITY, DIAG_RQL_FULL_RESIDUAL,
    DIAG_RQL_PROJECTION_CONFLICT, DIAG_RQL_PROJECT_TYPE, DIAG_RQL_WITHIN_TYPE,
    FULL_EXPLAIN_HASH_DOMAIN, MAX_PROJECT_DEPTH, MAX_WITHIN_DEPTH, RQL_FULL_PROFILE,
};
// IR / attach orchestration: crate-private (RQL-P0b) — profiles remain public stamps.
pub use group_agg::GROUP_AGG_IR_PROFILE;
pub use ir_attach::ATTACH_IR_PROFILE;
pub use ir_order::ORDER_IR_PROFILE;
pub use ir_page::PAGE_IR_PROFILE;
pub use ir_project::PROJECT_IR_PROFILE;
pub use kernel::{compile_where, lower_predicate, CompiledKernelWhere, KERNEL_PROFILE};
pub use qvm::{
    qvm_hash, validate_qvm, QVM_MAGIC, QVM_MAX_BLOB_BYTES, QVM_MAX_OPS, QVM_MAX_TOTAL_BYTES,
};
pub use vm::{Instruction, OpCode, VM_PROFILE, VM_VERSION};

use crate::app_v1::{Parameters, QueryBudget, QueryExplanation, QueryPage, QueryRunOptions};
use crate::error::Error;
use crate::plan_v1::{CollectionBindings, RqlPlanV1};
use crate::rql_app_core::{compile_app_core, CompiledAppCore};
use residiuum_heap::{CollectionId, HeapId};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

/// Architecture freeze profile id.
pub const BYTECODE_PROFILE: &str = "residiuum-query-bytecode-v1";

/// Coverage-aware result of a host document read.
///
/// `Hole` means the key is known to the host but its value cannot be supplied
/// honestly (for example because an on-disk frame failed verification).  Query
/// execution may retain healthy rows only when its coverage policy explicitly
/// allows incomplete results; complete coverage still fails closed.
#[derive(Debug, Clone, PartialEq)]
pub enum HostDocument {
    /// A verified, decodable JSON document.
    Present(JsonValue),
    /// The key is absent at the requested consistency point.
    Absent,
    /// The key exists, but its value is an explicit coverage hole.
    Hole {
        /// Stable machine-readable reason.
        code: String,
    },
}

/// Host data-access capabilities only (Decision 0 / RQL-X1 / **RQL-P1b**).
///
/// Every data op is **collection-qualified** by immutable [`CollectionId`].
/// Core and Full share this surface; Full attach must not bypass via
/// name-only `HeapClient::open_collection` for foreign loads.
pub trait HostCapabilities {
    /// Deterministic key stream for `collection_id`.
    fn list_keys(
        &mut self,
        collection_id: CollectionId,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, Error>;

    /// Document get on `collection_id` (`None` = absent).
    fn get_json(
        &mut self,
        collection_id: CollectionId,
        key: &str,
    ) -> Result<Option<JsonValue>, Error>;

    /// Coverage-aware document get.
    ///
    /// The default preserves compatibility for hosts that cannot distinguish a
    /// damaged value from an ordinary read error. Coverage-aware storage hosts
    /// should override this method and return [`HostDocument::Hole`].
    fn get_json_covered(
        &mut self,
        collection_id: CollectionId,
        key: &str,
    ) -> Result<HostDocument, Error> {
        Ok(match self.get_json(collection_id, key)? {
            Some(value) => HostDocument::Present(value),
            None => HostDocument::Absent,
        })
    }

    /// Optional equality-index candidate keys on `collection_id` (not a semantic filter).
    fn lookup_index_keys(
        &mut self,
        _collection_id: CollectionId,
        _equalities: &[(String, JsonValue)],
    ) -> Result<Option<Vec<String>>, Error> {
        Ok(None)
    }
}

/// Compiled query bytecode envelope — **QVM1 bytes only** (public authority).
///
/// Fields are private so callers cannot pair a Rust plan with unrelated bytes.
#[derive(Debug, Clone)]
pub struct QueryBytecodeV1 {
    profile: String,
    /// Durable QVM1 bytes (sole public executable identity).
    qvm: Vec<u8>,
}

impl QueryBytecodeV1 {
    /// Profile label.
    pub fn profile(&self) -> &str {
        &self.profile
    }

    /// Durable QVM1 bytes (sole public executable identity).
    pub fn qvm_bytes(&self) -> &[u8] {
        &self.qvm
    }

    /// Alias of [`Self::qvm_bytes`] for call sites that still say “isa”.
    ///
    /// The public stored bytes are **QVM1**, not RQB1.
    pub fn isa_bytes(&self) -> &[u8] {
        &self.qvm
    }

    /// Domain-separated hash of the QVM1 bytes (public program identity).
    pub fn isa_hash(&self) -> [u8; 32] {
        qvm::qvm_hash(&self.qvm)
    }

    /// Domain-separated hash of the QVM1 bytes.
    pub fn qvm_hash(&self) -> [u8; 32] {
        qvm::qvm_hash(&self.qvm)
    }

    /// Validate stored QVM bytes (canonical decode + verify).
    pub fn validate(&self) -> Result<(), Error> {
        qvm::validate_qvm(&self.qvm)
    }

    /// Lower a validated Application Core plan → QVM1 envelope.
    pub fn from_core_plan(plan: RqlPlanV1, budget: Option<QueryBudget>) -> Result<Self, Error> {
        Self::from_core_plan_force_scan(plan, budget, false)
    }

    /// Lower Core with optional force_scan into QVM1.
    pub fn from_core_plan_force_scan(
        plan: RqlPlanV1,
        budget: Option<QueryBudget>,
        force_scan: bool,
    ) -> Result<Self, Error> {
        let prog = vm_exec::lower_core_with_force_scan(plan, budget, force_scan);
        let qvm = qvm::encode_qvm(&prog)?;
        // Round-trip authority check.
        let _ = qvm::decode_qvm(&qvm)?;
        Ok(Self {
            profile: BYTECODE_PROFILE.to_string(),
            qvm,
        })
    }

    /// Lower compiled Application Core artefact.
    pub fn from_compiled_core(compiled: CompiledAppCore) -> Result<Self, Error> {
        if compiled.after.is_some() {
            return Err(Error::QueryInvalid(
                "compiled textual `after` must be bound by execute_core_rql before QVM lowering"
                    .into(),
            ));
        }
        Self::from_core_plan(compiled.plan, compiled.budget)
    }

    /// Construct from durable QVM1 bytes (validates decode + verify).
    pub fn from_qvm_bytes(qvm: Vec<u8>) -> Result<Self, Error> {
        let _ = qvm::decode_qvm(&qvm)?;
        Ok(Self {
            profile: BYTECODE_PROFILE.to_string(),
            qvm,
        })
    }
}

/// Compile Application Core RQL source → bytecode envelope.
pub fn lower_core_source(
    source: &str,
    collection_id: CollectionId,
    collection_name: &str,
) -> Result<QueryBytecodeV1, Error> {
    let mut bindings = CollectionBindings {
        by_name: BTreeMap::new(),
    };
    bindings.bind(collection_name, collection_id);
    let mut compiled = compile_app_core(source, &bindings)?;
    if compiled.after.is_some() {
        return Err(Error::QueryInvalid(
            "textual `after` requires execute_core_rql so the cursor binding cannot be ignored"
                .into(),
        ));
    }
    if compiled.plan.from.collection_id != collection_id {
        compiled.plan.from.collection_id = collection_id;
    }
    QueryBytecodeV1::from_compiled_core(compiled)
}

/// Explain via Core compile (plan tree + hash; no row scan).
pub fn explain_core_source(
    source: &str,
    collection_id: CollectionId,
    collection_name: &str,
) -> Result<QueryExplanation, Error> {
    explain_rql_source(source, collection_id, collection_name)
}

/// Execute bytecode against a host — **decodes QVM1; never trusts a side plan**.
pub fn execute_bytecode<H: HostCapabilities>(
    host: &mut H,
    bytecode: &QueryBytecodeV1,
    params: &BTreeMap<String, JsonValue>,
    options: &QueryRunOptions,
    heap_id: HeapId,
    collection_id: CollectionId,
) -> Result<QueryPage, Error> {
    if bytecode.profile() != BYTECODE_PROFILE {
        return Err(Error::QueryInvalid(format!(
            "query bytecode profile mismatch: got {:?}, want {BYTECODE_PROFILE}",
            bytecode.profile()
        )));
    }
    execute_qvm_bytes(
        host,
        bytecode.qvm_bytes(),
        params,
        options,
        heap_id,
        collection_id,
    )
}

/// Product QVM entry: decode + verify + `run_vm`.
pub fn execute_qvm_bytes<H: HostCapabilities>(
    host: &mut H,
    qvm_bytes: &[u8],
    params: &BTreeMap<String, JsonValue>,
    options: &QueryRunOptions,
    heap_id: HeapId,
    collection_id: CollectionId,
) -> Result<QueryPage, Error> {
    let prog = qvm::decode_qvm(qvm_bytes)?;
    let out = vm_exec::run_vm(host, &prog, params, options, heap_id, collection_id, false)?;
    Ok(out.page)
}

/// Product entry: Core RQL source → lower → execute on host.
pub fn execute_core_rql<H: HostCapabilities>(
    host: &mut H,
    source: &str,
    parameters: &Parameters,
    options: &QueryRunOptions,
    heap_id: HeapId,
    collection_id: CollectionId,
    collection_name: &str,
) -> Result<QueryPage, Error> {
    if options.explain {
        return Err(Error::QueryInvalid(
            "use explain_rql for explain; rql executes rows".into(),
        ));
    }
    let mut bindings = CollectionBindings {
        by_name: BTreeMap::new(),
    };
    bindings.bind(collection_name, collection_id);
    let mut compiled = compile_app_core(source, &bindings)?;
    if compiled.plan.from.collection_id != collection_id {
        compiled.plan.from.collection_id = collection_id;
    }
    let mut effective_options = options.clone();
    let semantic_parameters = crate::rql_app_core::bind_textual_after(
        compiled.after.as_ref(),
        parameters,
        &mut effective_options,
    )?;
    compiled.after = None;
    let bytecode = QueryBytecodeV1::from_compiled_core(compiled)?;
    execute_bytecode(
        host,
        &bytecode,
        &semantic_parameters.values,
        &effective_options,
        heap_id,
        collection_id,
    )
}

/// Shared Core page entry used by Full lower paths (RQL-VM1R → one Query VM).
///
/// Crate-private (RQL-P0b): not a public bypass of validated QVM.
/// Lowers a Core plan to a VM program and runs
/// [`vm_exec::run_vm`] (`CoreFrame` phases; RQL-VM2). Full path also calls
/// `run_vm` directly on `lower_full` (no second dispatcher).
/// Not Decision 0 closed.
pub(crate) fn execute_decoded_core<H: HostCapabilities>(
    host: &mut H,
    core: &RqlPlanV1,
    budget: Option<QueryBudget>,
    params: &BTreeMap<String, JsonValue>,
    options: &QueryRunOptions,
    heap_id: HeapId,
    collection_id: CollectionId,
) -> Result<QueryPage, Error> {
    if core.from.collection_id != collection_id {
        return Err(Error::QueryInvalid(
            "execute_decoded_core: collection_id mismatch".into(),
        ));
    }
    let prog = vm_exec::lower_core(core.clone(), budget);
    let prog = qvm::materialize_qvm(&prog)?;
    let out = vm_exec::run_vm(host, &prog, params, options, heap_id, collection_id, false)?;
    Ok(out.page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::{CompareOp, Operand, Path};
    use serde_json::json;

    fn uuidish(seed: u8) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0] = seed;
        b[6] = (b[6] & 0x0f) | 0x40;
        b[8] = (b[8] & 0x3f) | 0x80;
        b
    }

    struct MapHost {
        docs: BTreeMap<String, JsonValue>,
    }

    impl HostCapabilities for MapHost {
        fn list_keys(
            &mut self,
            _collection_id: CollectionId,
            limit: Option<usize>,
            after_key: Option<&str>,
        ) -> Result<Vec<String>, Error> {
            let mut keys: Vec<String> = self.docs.keys().cloned().collect();
            keys.sort();
            if let Some(a) = after_key {
                keys.retain(|k| k.as_str() > a);
            }
            if let Some(n) = limit {
                keys.truncate(n);
            }
            Ok(keys)
        }

        fn get_json(
            &mut self,
            _collection_id: CollectionId,
            key: &str,
        ) -> Result<Option<JsonValue>, Error> {
            Ok(self.docs.get(key).cloned())
        }
    }

    #[test]
    fn bytecode_profile_constant() {
        assert_eq!(BYTECODE_PROFILE, "residiuum-query-bytecode-v1");
    }

    #[test]
    fn lower_core_stamps_profile() {
        let id = CollectionId::from_bytes(uuidish(7)).expect("id");
        let bc = lower_core_source("from items", id, "items").expect("lower");
        assert_eq!(bc.profile(), BYTECODE_PROFILE);
        assert_eq!(&bc.qvm_bytes()[0..4], qvm::QVM_MAGIC);
        bc.validate().expect("validate");
        assert_eq!(bc.qvm_hash(), qvm::qvm_hash(bc.qvm_bytes()));
    }

    #[test]
    fn execute_empty_host_one_page() {
        let id = CollectionId::from_bytes(uuidish(9)).expect("id");
        let heap = HeapId::from_bytes(uuidish(1)).expect("heap");
        let mut host = MapHost {
            docs: BTreeMap::new(),
        };
        let page = execute_core_rql(
            &mut host,
            "from items",
            &Parameters::default(),
            &QueryRunOptions::default(),
            heap,
            id,
            "items",
        )
        .expect("exec");
        assert!(page.rows.is_empty());
    }

    #[test]
    fn core_page_owned_by_bytecode_module() {
        assert_eq!(EXEC_PROFILE, "residiuum-app-core-exec-v1");
    }

    #[test]
    fn execute_bytecode_uses_isa_not_sidecar_plan() {
        let id = CollectionId::from_bytes(uuidish(3)).expect("id");
        let heap = HeapId::from_bytes(uuidish(1)).expect("heap");
        let mut docs = BTreeMap::new();
        docs.insert("a".into(), json!({"status": "active"}));
        docs.insert("b".into(), json!({"status": "paused"}));
        let mut host = MapHost { docs };

        let bc =
            lower_core_source("from items where status = \"active\"", id, "items").expect("lower");

        // Tamper: rebuild QVM with a different Filter where via plan re-lower.
        let mut bindings = CollectionBindings {
            by_name: BTreeMap::new(),
        };
        bindings.bind("items", id);
        let mut plan = crate::plan_v1::PlanBuilder::from_source("items")
            .where_(crate::predicate::Predicate::Cmp {
                cmp: CompareOp::Eq,
                left: Operand::path(Path::parse_dotted("status").unwrap()),
                right: Operand::literal(json!("paused")),
            })
            .compile(&bindings)
            .expect("plan");
        let _ = &mut plan;
        let bc_tampered = QueryBytecodeV1::from_core_plan(plan, None).expect("wrap");

        let page = execute_bytecode(
            &mut host,
            &bc_tampered,
            &BTreeMap::new(),
            &QueryRunOptions::default(),
            heap,
            id,
        )
        .expect("exec");
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].key, "b");
        // Original QVM still selects "active".
        let page0 = execute_bytecode(
            &mut host,
            &bc,
            &BTreeMap::new(),
            &QueryRunOptions::default(),
            heap,
            id,
        )
        .expect("exec0");
        assert_eq!(page0.rows.len(), 1);
        assert_eq!(page0.rows[0].key, "a");
    }

    #[test]
    fn execute_qvm_bytes_matches_execute_bytecode() {
        let id = CollectionId::from_bytes(uuidish(12)).expect("id");
        let heap = HeapId::from_bytes(uuidish(1)).expect("heap");
        let mut host = MapHost {
            docs: BTreeMap::from([("k1".into(), json!({"n": 1}))]),
        };
        let bc = lower_core_source("from items page size 8", id, "items").expect("lower");
        let via_bc = execute_bytecode(
            &mut host,
            &bc,
            &BTreeMap::new(),
            &QueryRunOptions::default(),
            heap,
            id,
        )
        .expect("bytecode");
        let via_qvm = execute_qvm_bytes(
            &mut host,
            bc.qvm_bytes(),
            &BTreeMap::new(),
            &QueryRunOptions::default(),
            heap,
            id,
        )
        .expect("qvm");
        assert_eq!(via_bc.rows, via_qvm.rows);
        assert_eq!(via_bc.rows.len(), 1);
    }

    #[test]
    fn corrupted_qvm_refuses_execute() {
        let id = CollectionId::from_bytes(uuidish(4)).expect("id");
        let heap = HeapId::from_bytes(uuidish(1)).expect("heap");
        let mut host = MapHost {
            docs: BTreeMap::new(),
        };
        let bc = lower_core_source("from items", id, "items").expect("lower");
        let mut bad = bc.qvm_bytes().to_vec();
        // Truncate to drop terminal Halt — verifier requires final Halt.
        assert!(bad.len() > 8);
        bad.truncate(bad.len() - 2);
        let err = execute_qvm_bytes(
            &mut host,
            &bad,
            &BTreeMap::new(),
            &QueryRunOptions::default(),
            heap,
            id,
        )
        .expect_err("corrupt");
        let _ = err;
    }
}
