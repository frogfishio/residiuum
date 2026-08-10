//! Full RQL attach / enrich / within / project — owned by [`crate::query_bytecode_v1`].
//!
//! Ported from the former standalone `rql_full_v1` façade (Decision 0 / RQL-X2c).
//! Product entry remains via [`crate::query_bytecode_v1`] re-exports.
//!
//! Normative: [RQL_SPEC.md](../../../../doc/wip/query/RQL_SPEC.md) enrich / within.
//! Application Core (`rql-app-core-v1`) still **rejects** `enrich` / `within` /
//! `at rank` at the Core compile surface; full-language runs through this module.
//!
//! Current surface:
//! - ordered root pipeline: `enrich` / `within` / post-attach `where` interleaved
//! - nested `within` depth (bounded by [`MAX_WITHIN_DEPTH`])
//! - nested `where` inside `within` (ordered filter on carrier elements)
//! - root `where` after enrich/within filters the Core page rows (page-then-attach honesty)
//! - nested post-pipeline `project { … }` (leaf / rename / nested product + bag map)
//! - execute `exactly_one` / `optional` / `many` attach via foreign scan oracle
//! - **RQL-I1:** root `enrich` may use equality-index pushdown on the foreign
//!   match field (`lookup_index_keys`); fall back to full scan when no usable
//!   index. Nested `within` enrich still scan-loads (residual).
//! - **RQL-X4b:** attach filters / candidate `where` evaluate via
//!   [`super::kernel`] (same SDA substrate as Core `where`)
//! - **RQL-X5b:** execute only after ISA encode→decode; pipeline/project from
//!   decoded full section (not a raw [`CompiledRqlFull`] authority bypass)
//! - refuse `at rank` / access policies (DDA residual)
//!
//! Not package accept. Not a claim that full RQL-v1 is product-ready.
//! Decision 0 remains open (page/order/project/coverage still Rust interpreters).

use crate::app_v1::{HeapClient, Parameters, QueryExplanation, QueryPage, QueryRunOptions};
use crate::error::Error;
use crate::plan_v1::{CollectionBindings, PLAN_HASH_DOMAIN};
use crate::predicate::{resolve_path, Path, Predicate, Resolve};
use crate::resource::{check_result_bytes, estimate_row_bytes, host_limits};
use crate::rql_app_core::{compile_app_core, CompiledAppCore, DIAG_RQL_FEATURE_UNAVAILABLE};
use residiuum_heap::{CollectionId, HeapId};
use serde_json::{Map, Value as JsonValue};
use std::collections::{BTreeMap, BTreeSet};

use super::vm_exec::{lower_full, run_vm};
use super::HostCapabilities;

/// Full-language compile profile (Phase 3 kickoff).
pub const RQL_FULL_PROFILE: &str = "rql-full-v1";

/// Domain separator for [`CompiledRqlFull::explain_hash`].
pub const FULL_EXPLAIN_HASH_DOMAIN: &str = "residiuum:rql-full-v1:explain-v1";

/// Diagnostic when an enrich cardinality cannot be satisfied.
pub const DIAG_RQL_ENRICH_CARDINALITY: &str = "rql_enrich_cardinality";

/// Diagnostic when `within` path is absent, Null, or not a sequence/bag.
pub const DIAG_RQL_WITHIN_TYPE: &str = "rql_within_type";

/// Diagnostic when a full-language construct is still residual.
pub const DIAG_RQL_FULL_RESIDUAL: &str = "rql_full_residual";

/// Diagnostic when nested project hits a non-projectable value.
pub const DIAG_RQL_PROJECT_TYPE: &str = "rql_project_type";

/// Diagnostic when project output names collide.
pub const DIAG_RQL_PROJECTION_CONFLICT: &str = "rql_projection_conflict";

/// Host bound on nested `within` depth (root `within` is depth 1).
pub const MAX_WITHIN_DEPTH: usize = 8;

/// Host bound on nested `project { }` depth.
pub const MAX_PROJECT_DEPTH: usize = 8;

/// How foreign docs were loaded for one enrich step (RQL-I1 honesty).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichAttachMode {
    /// Complete `list_keys`+`get` of the foreign collection.
    Scan,
    /// Equality index on the foreign match field + `get` of hit keys.
    EqualityIndex,
}

impl EnrichAttachMode {
    /// Stable label for evidence / explain.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scan => "scan_list_keys_get",
            Self::EqualityIndex => "equality_index",
        }
    }
}

/// Evidence for one enrich foreign-load decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichLoadEvidence {
    /// Foreign collection name (`using`).
    pub using: String,
    /// Attach output field.
    pub output: String,
    /// Load mode actually used.
    pub mode: EnrichAttachMode,
}

/// Options for [`execute_rql_full_with`].
#[derive(Debug, Clone, Default)]
pub struct RqlFullExecuteOptions {
    /// Application Core page options (continuation, budgets, …).
    pub query: QueryRunOptions,
    /// When true, never use enrich equality-index pushdown (differential oracle).
    pub force_enrich_scan: bool,
}

/// Enrichment cardinality (RQL_SPEC).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichCardinality {
    /// Exactly one matching foreign document.
    ExactlyOne,
    /// Zero or one match.
    Optional,
    /// Zero or more matches (JSON array / bag).
    Many,
}

impl EnrichCardinality {
    /// Spec spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExactlyOne => "exactly_one",
            Self::Optional => "optional",
            Self::Many => "many",
        }
    }
}

/// One compiled enrich step (single-level).
#[derive(Debug, Clone, PartialEq)]
pub struct EnrichStepV1 {
    /// Field name attached onto the current row (root or within element).
    pub output: String,
    /// Bound foreign collection name (diagnostics).
    pub using_name: String,
    /// Bound foreign collection id.
    pub using_id: CollectionId,
    /// Path on the **current** document (left side of matching).
    pub left: Path,
    /// Path on the **foreign** document (right side of matching).
    pub right: Path,
    /// Optional filter evaluated against each **foreign** candidate.
    pub candidate_where: Option<Predicate>,
    /// Cardinality expectation.
    pub expect: EnrichCardinality,
}

/// One step in an ordered enrich/within pipeline (root or nested).
#[derive(Debug, Clone, PartialEq)]
pub enum FullPipelineStepV1 {
    /// Attach foreign docs onto the current row.
    Enrich(EnrichStepV1),
    /// Expand a carrier bag and run nested steps per element.
    Within(WithinStepV1),
    /// Keep current rows where the predicate is true (nested `where` inside `within`).
    Filter(Predicate),
}

/// One compiled `within` step (nested enrich and/or nested `within`).
#[derive(Debug, Clone, PartialEq)]
pub struct WithinStepV1 {
    /// Carrier path on the current row (must resolve to a JSON array).
    pub carrier: Path,
    /// Optional element alias (`as item`); stripped from nested left/carrier paths.
    pub element_alias: Option<String>,
    /// Nested pipeline steps applied in order to each carrier element.
    pub steps: Vec<FullPipelineStepV1>,
}

/// One compiled nested-project item (`project { … }`).
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectItemV1 {
    /// Leaf copy: output name ← source path (`id` or `region: address.region`).
    Leaf {
        /// Output field name.
        output: String,
        /// Source path on the current artefact.
        source: Path,
    },
    /// Nested block: `customer { name }` or bag map `items { sku }`.
    Nested {
        /// Output field name and source field on the current artefact.
        output: String,
        /// Nested projection items.
        fields: Vec<ProjectItemV1>,
    },
    /// Computed field evaluated against the current materialised row.
    Computed {
        /// Output field name.
        output: String,
        /// Canonical expression carried in the QVM immediate.
        expression: ProjectExprV1,
    },
}

/// Bounded computed-projection expression admitted by the Tier-A profile.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectExprV1 {
    /// JSON scalar/compound literal.
    Literal(JsonValue),
    /// Copy a path value.
    Path(Path),
    /// `if predicate then expression else expression`.
    Conditional {
        /// Condition evaluated through the canonical SDA predicate kernel.
        when: Predicate,
        /// Selected when `when` is true.
        then_expr: Box<ProjectExprV1>,
        /// Selected when `when` is false.
        else_expr: Box<ProjectExprV1>,
    },
}

/// Compiled full-language query (Core base + ordered enrich/within pipeline).
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledRqlFull {
    /// Profile label.
    pub profile: &'static str,
    /// Application Core plan for the base `from`/`where`/`project`/`order`/…
    /// (enrich/within/brace-project stripped before Core compile).
    pub base: CompiledAppCore,
    /// Ordered root pipeline (`enrich` / `within` interleaved).
    pub pipeline: Vec<FullPipelineStepV1>,
    /// Optional nested `project { … }` applied after the pipeline.
    pub project: Option<Vec<ProjectItemV1>>,
    /// Source text with enrich/within/brace-project clauses removed (Core surface).
    pub base_source: String,
}

impl CompiledRqlFull {
    /// Canonical identity of the QVM programme produced by this compile.
    pub fn program_hash(&self) -> Result<[u8; 32], Error> {
        let vm = lower_full(
            self.base.plan.clone(),
            self.base.budget,
            self.pipeline.clone(),
            self.project.clone(),
        );
        let qvm = super::qvm::encode_qvm(&vm)?;
        Ok(super::qvm::qvm_hash(&qvm))
    }

    /// Root enrich steps in pipeline order (skips `within` / filter).
    pub fn root_enrich(&self) -> Vec<&EnrichStepV1> {
        self.pipeline
            .iter()
            .filter_map(|s| match s {
                FullPipelineStepV1::Enrich(e) => Some(e),
                FullPipelineStepV1::Within(_) | FullPipelineStepV1::Filter(_) => None,
            })
            .collect()
    }

    /// First root `within` step, if any.
    pub fn first_within(&self) -> Option<&WithinStepV1> {
        self.pipeline.iter().find_map(|s| match s {
            FullPipelineStepV1::Within(w) => Some(w),
            FullPipelineStepV1::Enrich(_) | FullPipelineStepV1::Filter(_) => None,
        })
    }

    /// Structured explain tree (base Core plan + pipeline + project). No rows.
    pub fn to_explain_tree(&self) -> JsonValue {
        let mut root = BTreeMap::new();
        root.insert("profile".into(), JsonValue::String(RQL_FULL_PROFILE.into()));
        root.insert("base".into(), self.base.plan.to_canonical_json());
        root.insert(
            "base_plan_hash".into(),
            JsonValue::String(bytes_to_hex(&self.base.plan.plan_hash())),
        );
        root.insert(
            "base_source".into(),
            JsonValue::String(self.base_source.clone()),
        );
        root.insert(
            "pipeline".into(),
            JsonValue::Array(self.pipeline.iter().map(pipeline_step_to_json).collect()),
        );
        match &self.project {
            Some(items) => root.insert(
                "project".into(),
                JsonValue::Array(items.iter().map(project_item_to_json).collect()),
            ),
            None => root.insert("project".into(), JsonValue::Null),
        };
        root.insert(
            "attach_oracle".into(),
            JsonValue::String("scan_or_equality_index".into()),
        );
        root.insert("wire".into(), JsonValue::String("local_facade_only".into()));
        btree_to_json_obj(root)
    }

    /// Domain-separated BLAKE3 hash over [`Self::to_explain_tree`].
    pub fn explain_hash(&self) -> [u8; 32] {
        let body = serde_json::to_vec(&self.to_explain_tree()).expect("explain tree json");
        let mut h = blake3::Hasher::new();
        h.update(FULL_EXPLAIN_HASH_DOMAIN.as_bytes());
        h.update(&[0u8]);
        // Tie explain hash to the Core plan hash domain so Core drift invalidates.
        h.update(PLAN_HASH_DOMAIN.as_bytes());
        h.update(&[0u8]);
        h.update(&body);
        *h.finalize().as_bytes()
    }
}

impl WithinStepV1 {
    /// Nested enrich steps in order (skips nested `within` / filter).
    pub fn enrich_steps(&self) -> Vec<&EnrichStepV1> {
        self.steps
            .iter()
            .filter_map(|s| match s {
                FullPipelineStepV1::Enrich(e) => Some(e),
                FullPipelineStepV1::Within(_) | FullPipelineStepV1::Filter(_) => None,
            })
            .collect()
    }
}

enum RawPipelineStep {
    Enrich(String),
    Within(String),
    Where(String),
}

enum RawNestedStep {
    Enrich(String),
    Within(String),
    Where(String),
}

/// Compile full RQL-v1 source (Core + ordered enrich/within pipeline).
pub fn compile_rql_full(
    source: &str,
    bindings: &CollectionBindings,
) -> Result<CompiledRqlFull, Error> {
    if source.len() > crate::rql_app_core::MAX_RQL_SOURCE_BYTES {
        return Err(Error::QueryInvalid(format!(
            "RQL source exceeds {} bytes",
            crate::rql_app_core::MAX_RQL_SOURCE_BYTES
        )));
    }
    refuse_residual_constructs(source)?;

    let (without_computed, computed_project) = extract_computed_project(source)?;
    let (without_project, brace_project) = extract_brace_project(&without_computed)?;
    let project = match (computed_project, brace_project) {
        (Some(_), Some(_)) => {
            return Err(Error::QueryInvalid(
                "computed and brace project clauses cannot be combined".into(),
            ))
        }
        (Some(p), None) | (None, Some(p)) => Some(p),
        (None, None) => None,
    };
    let (base_source, raw_steps) = split_full_clauses(&without_project)?;
    let base = compile_app_core(&base_source, bindings)?;
    let mut pipeline = Vec::with_capacity(raw_steps.len());
    for raw in raw_steps {
        match raw {
            RawPipelineStep::Enrich(body) => {
                pipeline.push(FullPipelineStepV1::Enrich(parse_enrich_step(
                    &body, bindings, None,
                )?));
            }
            RawPipelineStep::Within(body) => {
                pipeline.push(FullPipelineStepV1::Within(parse_within_step(
                    &body, bindings, None, 1,
                )?));
            }
            RawPipelineStep::Where(body) => {
                pipeline.push(FullPipelineStepV1::Filter(parse_filter_step(
                    &body, bindings, None,
                )?));
            }
        }
    }
    Ok(CompiledRqlFull {
        profile: RQL_FULL_PROFILE,
        base,
        pipeline,
        project,
        base_source,
    })
}

fn refuse_residual_constructs(source: &str) -> Result<(), Error> {
    let lower = source.to_ascii_lowercase();
    let padded = format!(" {} ", lower.replace('\t', " "));
    for (needle, label) in [
        (" at rank", "at rank"),
        (" sequential", "sequential access"),
        (" direct ", "direct access"),
        (" build ", "build access"),
    ] {
        if padded.contains(needle) {
            return Err(Error::QueryInvalid(format!(
                "{DIAG_RQL_FULL_RESIDUAL}: `{label}` outside rql-full-v1 current slice"
            )));
        }
    }
    Ok(())
}

/// True when source uses constructs that require [`compile_rql_full`] /
/// [`execute_rql_full`] (not the collection-bound Application Core profile).
pub fn source_uses_rql_full_constructs(source: &str) -> bool {
    let lower = source.to_ascii_lowercase();
    let padded = format!(" {} ", lower.replace('\t', " "));
    if padded.contains(" enrich ")
        || padded.contains("\nenrich ")
        || lower
            .split_whitespace()
            .any(|t| t == "enrich" || t == "within")
        || padded.contains(" within ")
    {
        return true;
    }
    if padded.contains(" project ") && padded.contains(" = if ") && padded.contains(" then ") {
        return true;
    }
    // Brace `project { … }` (flat Core `project a, b` is fine on Core wire).
    let bytes = lower.as_bytes();
    let mut i = 0usize;
    let mut depth = 0usize;
    while i < lower.len() {
        match bytes[i] {
            b'{' => {
                depth += 1;
                i += 1;
                continue;
            }
            b'}' => {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0 && matches_kw(&lower, i, "project") {
            let after = i + "project".len();
            let rest = source.get(after..).unwrap_or("").trim_start();
            if rest.starts_with('{') {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Refuse full-language source when op **118** is using its Core profile.
///
/// RQL-F2 honesty: a caller must explicitly choose the Heap-bound Full profile;
/// the collection-bound Core surface never silently widens its semantics.
pub fn refuse_full_language_on_core_wire(source: &str) -> Result<(), Error> {
    if source_uses_rql_full_constructs(source) {
        return Err(Error::QueryInvalid(format!(
            "{DIAG_RQL_FEATURE_UNAVAILABLE}: enrich/within/brace-project require \
             HeapClient::rql_full; not the Application Core op-118 profile"
        )));
    }
    Ok(())
}

/// Structured explain for full RQL (compile only; no row materialization).
pub fn explain_rql_full(
    source: &str,
    bindings: &CollectionBindings,
) -> Result<QueryExplanation, Error> {
    let compiled = compile_rql_full(source, bindings)?;
    Ok(QueryExplanation {
        plan_profile: RQL_FULL_PROFILE.into(),
        // Explain identity is the canonical QVM programme that execute will
        // decode and run, not a second hash over a diagnostic tree.
        plan_hash: compiled.program_hash()?,
        tree: compiled.to_explain_tree(),
    })
}

/// Explain using collection names discovered on [`HeapClient`].
pub fn explain_rql_full_on_heap(
    client: &mut HeapClient,
    source: &str,
) -> Result<QueryExplanation, Error> {
    let infos = client.list_collections()?;
    let mut bindings = CollectionBindings::default();
    for info in &infos {
        bindings.bind(&info.name, info.collection_id);
    }
    explain_rql_full(source, &bindings)
}

fn pipeline_step_to_json(step: &FullPipelineStepV1) -> JsonValue {
    match step {
        FullPipelineStepV1::Enrich(e) => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("enrich".into()));
            m.insert("output".into(), JsonValue::String(e.output.clone()));
            m.insert("using".into(), JsonValue::String(e.using_name.clone()));
            m.insert("using_id".into(), JsonValue::String(e.using_id.to_string()));
            m.insert("left".into(), JsonValue::String(e.left.dotted()));
            m.insert("right".into(), JsonValue::String(e.right.dotted()));
            m.insert("expect".into(), JsonValue::String(e.expect.as_str().into()));
            match &e.candidate_where {
                Some(p) => m.insert("candidate_where".into(), p.to_canonical_json()),
                None => m.insert("candidate_where".into(), JsonValue::Null),
            };
            btree_to_json_obj(m)
        }
        FullPipelineStepV1::Within(w) => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("within".into()));
            m.insert("carrier".into(), JsonValue::String(w.carrier.dotted()));
            match &w.element_alias {
                Some(a) => m.insert("alias".into(), JsonValue::String(a.clone())),
                None => m.insert("alias".into(), JsonValue::Null),
            };
            m.insert(
                "steps".into(),
                JsonValue::Array(w.steps.iter().map(pipeline_step_to_json).collect()),
            );
            btree_to_json_obj(m)
        }
        FullPipelineStepV1::Filter(pred) => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("filter".into()));
            m.insert("predicate".into(), pred.to_canonical_json());
            btree_to_json_obj(m)
        }
    }
}

fn project_item_to_json(item: &ProjectItemV1) -> JsonValue {
    match item {
        ProjectItemV1::Leaf { output, source } => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("leaf".into()));
            m.insert("output".into(), JsonValue::String(output.clone()));
            m.insert("source".into(), JsonValue::String(source.dotted()));
            btree_to_json_obj(m)
        }
        ProjectItemV1::Nested { output, fields } => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("nested".into()));
            m.insert("output".into(), JsonValue::String(output.clone()));
            m.insert(
                "fields".into(),
                JsonValue::Array(fields.iter().map(project_item_to_json).collect()),
            );
            btree_to_json_obj(m)
        }
        ProjectItemV1::Computed { output, expression } => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("computed".into()));
            m.insert("output".into(), JsonValue::String(output.clone()));
            m.insert("expression".into(), computed_expr_explain_json(expression));
            btree_to_json_obj(m)
        }
    }
}

fn computed_expr_explain_json(expr: &ProjectExprV1) -> JsonValue {
    match expr {
        ProjectExprV1::Literal(v) => serde_json::json!({"kind": "literal", "value": v}),
        ProjectExprV1::Path(p) => serde_json::json!({"kind": "path", "path": p.dotted()}),
        ProjectExprV1::Conditional {
            when,
            then_expr,
            else_expr,
        } => serde_json::json!({
            "kind": "conditional",
            "when": when.to_canonical_json(),
            "then": computed_expr_explain_json(then_expr),
            "else": computed_expr_explain_json(else_expr),
        }),
    }
}

fn btree_to_json_obj(m: BTreeMap<String, JsonValue>) -> JsonValue {
    let mut map = Map::new();
    for (k, v) in m {
        map.insert(k, v);
    }
    JsonValue::Object(map)
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

/// Extract the Gate-1 flat computed projection and lower it to the same
/// ProjectBrace QVM opcode used by structured projection. Ordinary flat path
/// projection remains in Application Core.
fn extract_computed_project(source: &str) -> Result<(String, Option<Vec<ProjectItemV1>>), Error> {
    let Some(start) = find_top_level_keyword(source, "project", 0) else {
        return Ok((source.to_string(), None));
    };
    let body_start = start + "project".len();
    let trimmed = source[body_start..].trim_start();
    if trimmed.starts_with('{') {
        return Ok((source.to_string(), None));
    }
    let ws = source[body_start..].len() - trimmed.len();
    let body_start = body_start + ws;
    let mut end = source.len();
    for kw in [
        "order",
        "limit",
        "page",
        "coverage",
        "consistency",
        "budget",
        "after",
        "access",
    ] {
        if let Some(at) = find_top_level_keyword(source, kw, body_start) {
            end = end.min(at);
        }
    }
    let body = source[body_start..end].trim();
    let raw_items = split_top_level(body, ',')?;
    let is_computed = raw_items.iter().any(|item| {
        item.split_once('=')
            .map(|(_, rhs)| rhs.trim_start().to_ascii_lowercase().starts_with("if "))
            .unwrap_or(false)
    });
    if !is_computed {
        return Ok((source.to_string(), None));
    }

    let mut fields = Vec::with_capacity(raw_items.len());
    let mut seen = std::collections::BTreeSet::new();
    for raw in raw_items {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err(Error::QueryInvalid("empty computed project item".into()));
        }
        let item = if let Some((output, rhs)) = raw.split_once('=') {
            let output = output.trim();
            if !is_plain_identifier(output) {
                return Err(Error::QueryInvalid(format!(
                    "computed project output `{output}` is not an identifier"
                )));
            }
            ProjectItemV1::Computed {
                output: output.to_string(),
                expression: parse_project_expr(rhs.trim())?,
            }
        } else {
            let source_path = Path::parse_dotted(raw)?;
            let output = source_path
                .0
                .last()
                .cloned()
                .ok_or_else(|| Error::QueryInvalid("empty project path".into()))?;
            ProjectItemV1::Leaf {
                output,
                source: source_path,
            }
        };
        let output = match &item {
            ProjectItemV1::Leaf { output, .. }
            | ProjectItemV1::Nested { output, .. }
            | ProjectItemV1::Computed { output, .. } => output,
        };
        if !seen.insert(output.clone()) {
            return Err(Error::QueryInvalid(format!(
                "{DIAG_RQL_PROJECTION_CONFLICT}: duplicate output `{output}`"
            )));
        }
        fields.push(item);
    }
    let mut base = String::new();
    base.push_str(&source[..start]);
    base.push_str(&source[end..]);
    Ok((base, Some(fields)))
}

fn parse_project_expr(source: &str) -> Result<ProjectExprV1, Error> {
    let source = source.trim();
    if source.len() >= 2
        && source[..2].eq_ignore_ascii_case("if")
        && source[2..].chars().next().is_some_and(char::is_whitespace)
    {
        let then_at = find_top_level_keyword(source, "then", 2)
            .ok_or_else(|| Error::QueryInvalid("computed project `if` requires `then`".into()))?;
        let condition = source[2..then_at].trim();
        let branches = &source[then_at + "then".len()..];
        let else_at = find_matching_else(branches)?;
        let then_source = branches[..else_at].trim();
        let else_source = branches[else_at + "else".len()..].trim();
        if then_source.is_empty() || else_source.is_empty() {
            return Err(Error::QueryInvalid(
                "computed project conditional requires both branches".into(),
            ));
        }
        return Ok(ProjectExprV1::Conditional {
            when: crate::rql_app_core::parse_predicate_expression(condition)?,
            then_expr: Box::new(parse_project_expr(then_source)?),
            else_expr: Box::new(parse_project_expr(else_source)?),
        });
    }
    if let Ok(value) = serde_json::from_str::<JsonValue>(source) {
        return Ok(ProjectExprV1::Literal(value));
    }
    Ok(ProjectExprV1::Path(Path::parse_dotted(source)?))
}

fn find_matching_else(source: &str) -> Result<usize, Error> {
    let mut search = 0usize;
    let mut nested = 0usize;
    loop {
        let next_if = find_top_level_keyword(source, "if", search);
        let next_else = find_top_level_keyword(source, "else", search);
        match (next_if, next_else) {
            (_, None) => {
                return Err(Error::QueryInvalid(
                    "computed project `if` requires `else`".into(),
                ))
            }
            (Some(i), Some(e)) if i < e => {
                nested += 1;
                search = i + 2;
            }
            (_, Some(e)) if nested == 0 => return Ok(e),
            (_, Some(e)) => {
                nested -= 1;
                search = e + 4;
            }
        }
    }
}

fn split_top_level(source: &str, separator: char) -> Result<Vec<&str>, Error> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut square = 0usize;
    let mut round = 0usize;
    let mut brace = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for (i, ch) in source.char_indices() {
        if quoted {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                quoted = false;
            }
            continue;
        }
        match ch {
            '"' => quoted = true,
            '[' => square += 1,
            ']' => {
                square = square.checked_sub(1).ok_or_else(|| {
                    Error::QueryInvalid("unbalanced `]` in computed project".into())
                })?
            }
            '(' => round += 1,
            ')' => {
                round = round.checked_sub(1).ok_or_else(|| {
                    Error::QueryInvalid("unbalanced `)` in computed project".into())
                })?
            }
            '{' => brace += 1,
            '}' => {
                brace = brace.checked_sub(1).ok_or_else(|| {
                    Error::QueryInvalid("unbalanced `}` in computed project".into())
                })?
            }
            c if c == separator && square == 0 && round == 0 && brace == 0 => {
                out.push(&source[start..i]);
                start = i + ch.len_utf8();
            }
            _ => {}
        }
    }
    if quoted || square != 0 || round != 0 || brace != 0 {
        return Err(Error::QueryInvalid(
            "unbalanced computed project expression".into(),
        ));
    }
    out.push(&source[start..]);
    Ok(out)
}

fn find_top_level_keyword(source: &str, keyword: &str, from: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut square = 0usize;
    let mut round = 0usize;
    let mut brace = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    let mut i = 0usize;
    while i < source.len() {
        let b = bytes[i];
        if quoted {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                quoted = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'"' => quoted = true,
            b'[' => square += 1,
            b']' => square = square.saturating_sub(1),
            b'(' => round += 1,
            b')' => round = round.saturating_sub(1),
            b'{' => brace += 1,
            b'}' => brace = brace.saturating_sub(1),
            _ => {}
        }
        if i >= from && square == 0 && round == 0 && brace == 0 && matches_kw(source, i, keyword) {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn is_plain_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Extract top-level nested `project { … }` (brace form). Flat Core `project a, b` stays.
fn extract_brace_project(source: &str) -> Result<(String, Option<Vec<ProjectItemV1>>), Error> {
    let lower = source.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut i = 0usize;
    let mut depth = 0usize;
    while i < lower.len() {
        match bytes[i] {
            b'{' => {
                depth += 1;
                i += 1;
                continue;
            }
            b'}' => {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0 && matches_kw(&lower, i, "project") {
            let after = i + "project".len();
            let rest = source[after..].trim_start();
            if rest.starts_with('{') {
                let open_abs = source.len() - rest.len();
                let end = find_matching_brace_end(source, open_abs)?;
                let inner = source[open_abs + 1..end - 1].trim();
                let items = parse_project_items(inner, 1)?;
                let mut out = String::new();
                out.push_str(&source[..i]);
                out.push_str(&source[end..]);
                return Ok((out, Some(items)));
            }
        }
        i += 1;
    }
    Ok((source.to_string(), None))
}

fn find_matching_brace_end(source: &str, open_abs: usize) -> Result<usize, Error> {
    let bytes = source.as_bytes();
    if bytes.get(open_abs) != Some(&b'{') {
        return Err(Error::QueryInvalid("project: expected `{`".into()));
    }
    let mut depth = 0usize;
    let mut i = open_abs;
    while i < source.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(Error::QueryInvalid("project: unclosed `{`".into()))
}

fn parse_project_items(inner: &str, depth: usize) -> Result<Vec<ProjectItemV1>, Error> {
    if depth > MAX_PROJECT_DEPTH {
        return Err(Error::QueryInvalid(format!(
            "{DIAG_RQL_FULL_RESIDUAL}: project depth exceeds host bound {MAX_PROJECT_DEPTH}"
        )));
    }
    let mut p = Words::new(inner);
    let mut items = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    while !p.is_eof() {
        let first = p.next_ident()?;
        let item = if p.eat_char(b'{') {
            let nested_inner = p.take_brace_inner()?;
            let fields = parse_project_items(&nested_inner, depth + 1)?;
            ProjectItemV1::Nested {
                output: first,
                fields,
            }
        } else if p.eat_char(b':') {
            let source = Path::parse_dotted(&p.next_path()?)?;
            ProjectItemV1::Leaf {
                output: first,
                source,
            }
        } else {
            let mut segs = vec![first];
            loop {
                p.skip();
                if p.s.as_bytes().get(p.i) == Some(&b'.') {
                    p.i += 1;
                    segs.push(p.next_ident()?);
                } else {
                    break;
                }
            }
            let output = segs.last().cloned().expect("non-empty");
            let source = Path::from_segments(segs)?;
            ProjectItemV1::Leaf { output, source }
        };
        let out_name = match &item {
            ProjectItemV1::Leaf { output, .. }
            | ProjectItemV1::Nested { output, .. }
            | ProjectItemV1::Computed { output, .. } => output.clone(),
        };
        if !seen.insert(out_name.clone()) {
            return Err(Error::QueryInvalid(format!(
                "{DIAG_RQL_PROJECTION_CONFLICT}: duplicate output `{out_name}`"
            )));
        }
        items.push(item);
        let _ = p.eat_char(b',');
    }
    Ok(items)
}

/// Split top-level `enrich` / `within` / post-attach `where`; return Core text + steps.
///
/// Pre-enrich/within `where` clauses stay in Core. `where` after the first
/// enrich/within becomes a pipeline [`FullPipelineStepV1::Filter`].
fn split_full_clauses(source: &str) -> Result<(String, Vec<RawPipelineStep>), Error> {
    let lower = source.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    // kind: 0=enrich 1=within 2=where
    let mut spans: Vec<(usize, usize, u8)> = Vec::new();
    let mut i = 0usize;
    let mut depth = 0usize;
    let mut seen_attach = false;
    while i < lower.len() {
        match bytes[i] {
            b'{' => {
                depth += 1;
                i += 1;
                continue;
            }
            b'}' => {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0 {
            if matches_kw(&lower, i, "enrich") {
                let after = i + "enrich".len();
                let end = find_enrich_end(&lower, after);
                spans.push((i, end, 0));
                seen_attach = true;
                i = end;
                continue;
            }
            if matches_kw(&lower, i, "within") {
                let after = i + "within".len();
                let end = find_within_end(&lower, after)?;
                spans.push((i, end, 1));
                seen_attach = true;
                i = end;
                continue;
            }
            if seen_attach && matches_kw(&lower, i, "where") {
                let after = i + "where".len();
                let end = find_root_where_end(&lower, after);
                spans.push((i, end, 2));
                i = end;
                continue;
            }
        }
        i += 1;
    }

    if spans.is_empty() {
        return Ok((
            source.split_whitespace().collect::<Vec<_>>().join(" "),
            Vec::new(),
        ));
    }

    let mut core = String::new();
    let mut steps = Vec::new();
    let mut cursor = 0;
    for (start, end, kind) in spans {
        core.push_str(&source[cursor..start]);
        match kind {
            0 => {
                let body = source[start + "enrich".len()..end].trim();
                steps.push(RawPipelineStep::Enrich(body.to_string()));
            }
            1 => {
                let body = source[start + "within".len()..end].trim();
                steps.push(RawPipelineStep::Within(body.to_string()));
            }
            2 => {
                let body = source[start + "where".len()..end].trim();
                if body.is_empty() {
                    return Err(Error::QueryInvalid("pipeline where clause is empty".into()));
                }
                steps.push(RawPipelineStep::Where(body.to_string()));
            }
            _ => unreachable!(),
        }
        cursor = end;
    }
    core.push_str(&source[cursor..]);
    let core = core.split_whitespace().collect::<Vec<_>>().join(" ");
    Ok((core, steps))
}

fn find_root_where_end(lower: &str, after_where: usize) -> usize {
    let terminals = [
        " enrich ",
        " within ",
        " where ",
        " project ",
        " order ",
        " limit ",
        " page ",
        " coverage ",
        " consistency ",
        " budget ",
        " at rank",
        " after ",
        " access ",
    ];
    let padded = format!(" {} ", &lower[after_where..]);
    let mut best = None;
    for t in terminals {
        if let Some(rel) = padded.find(t) {
            let abs = after_where + rel;
            best = Some(best.map_or(abs, |b: usize| b.min(abs)));
        }
    }
    best.unwrap_or(lower.len())
}

fn matches_kw(lower: &str, start: usize, kw: &str) -> bool {
    let bytes = lower.as_bytes();
    if start + kw.len() > lower.len() {
        return false;
    }
    if !lower[start..start + kw.len()].eq_ignore_ascii_case(kw) {
        return false;
    }
    let before_ok = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
    let after = start + kw.len();
    let after_ok = after >= lower.len() || !bytes[after].is_ascii_alphanumeric();
    before_ok && after_ok
}

fn find_enrich_end(lower: &str, after_enrich: usize) -> usize {
    // Grammar: enrich … [where …] expect <card> — stop after cardinality.
    let padded = format!(" {} ", &lower[after_enrich..]);
    if let Some(rel) = padded.find(" expect ") {
        let mut i = after_enrich + rel + " expect ".len();
        while i < lower.len() && lower.as_bytes()[i].is_ascii_whitespace() {
            i += 1;
        }
        while i < lower.len() {
            let b = lower.as_bytes()[i];
            if b.is_ascii_alphanumeric() || b == b'_' {
                i += 1;
            } else {
                break;
            }
        }
        return i;
    }

    let terminals = [
        " enrich ",
        " within ",
        " where ",
        " project ",
        " order ",
        " limit ",
        " page ",
        " coverage ",
        " consistency ",
        " budget ",
        " at rank",
        " after ",
        " access ",
    ];
    let mut best = None;
    for t in terminals {
        if let Some(rel) = padded.find(t) {
            let abs = after_enrich + rel;
            best = Some(best.map_or(abs, |b: usize| b.min(abs)));
        }
    }
    best.unwrap_or(lower.len())
}

fn find_within_end(lower: &str, after_within: usize) -> Result<usize, Error> {
    let Some(brace_rel) = lower[after_within..].find('{') else {
        return Err(Error::QueryInvalid(
            "within: expected `{` after path".into(),
        ));
    };
    let open = after_within + brace_rel;
    let bytes = lower.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < lower.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(Error::QueryInvalid("within: unclosed `{`".into()))
}

fn parse_within_step(
    body: &str,
    bindings: &CollectionBindings,
    outer_alias: Option<&str>,
    depth: usize,
) -> Result<WithinStepV1, Error> {
    if depth > MAX_WITHIN_DEPTH {
        return Err(Error::QueryInvalid(format!(
            "{DIAG_RQL_FULL_RESIDUAL}: within depth exceeds host bound {MAX_WITHIN_DEPTH}"
        )));
    }
    // body: <path> [as <alias>] { <nested steps> }
    let mut p = Words::new(body);
    let carrier = strip_alias_prefix(Path::parse_dotted(&p.next_path()?)?, outer_alias)?;
    let element_alias = if p.eat("as") {
        Some(p.next_ident()?)
    } else {
        None
    };
    if !p.eat_char(b'{') {
        return Err(Error::QueryInvalid(
            "within: expected `{` after path".into(),
        ));
    }
    let inner = p.take_brace_inner()?;
    if !p.is_eof() {
        return Err(Error::QueryInvalid(format!(
            "unexpected trailing within tokens near `{}`",
            p.rest()
        )));
    }

    let nested = split_nested_pipeline(&inner)?;
    let mut steps = Vec::with_capacity(nested.len());
    for step in nested {
        match step {
            RawNestedStep::Enrich(b) => {
                steps.push(FullPipelineStepV1::Enrich(parse_enrich_step(
                    &b,
                    bindings,
                    element_alias.as_deref(),
                )?));
            }
            RawNestedStep::Within(b) => {
                steps.push(FullPipelineStepV1::Within(parse_within_step(
                    &b,
                    bindings,
                    element_alias.as_deref(),
                    depth + 1,
                )?));
            }
            RawNestedStep::Where(b) => {
                steps.push(FullPipelineStepV1::Filter(parse_filter_step(
                    &b,
                    bindings,
                    element_alias.as_deref(),
                )?));
            }
        }
    }
    if steps.is_empty() {
        return Err(Error::QueryInvalid(
            "within block requires at least one enrich, within, or where".into(),
        ));
    }
    Ok(WithinStepV1 {
        carrier,
        element_alias,
        steps,
    })
}

/// Split nested `where` / `enrich` / `within` steps inside a `within` block.
fn split_nested_pipeline(source: &str) -> Result<Vec<RawNestedStep>, Error> {
    let lower = source.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut spans: Vec<(usize, usize, u8)> = Vec::new(); // 0=enrich 1=within 2=where
    let mut i = 0usize;
    let mut depth = 0usize;
    while i < lower.len() {
        match bytes[i] {
            b'{' => {
                depth += 1;
                i += 1;
                continue;
            }
            b'}' => {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0 {
            if matches_kw(&lower, i, "enrich") {
                let after = i + "enrich".len();
                let end = find_enrich_end(&lower, after);
                spans.push((i, end, 0));
                i = end;
                continue;
            }
            if matches_kw(&lower, i, "within") {
                let after = i + "within".len();
                let end = find_within_end(&lower, after)?;
                spans.push((i, end, 1));
                i = end;
                continue;
            }
            if matches_kw(&lower, i, "where") {
                let after = i + "where".len();
                let end = find_where_end(&lower, after);
                spans.push((i, end, 2));
                i = end;
                continue;
            }
        }
        i += 1;
    }

    if spans.is_empty() {
        if source.split_whitespace().next().is_some() {
            return Err(Error::QueryInvalid(format!(
                "{DIAG_RQL_FULL_RESIDUAL}: within nested tokens not in this slice near `{source}`"
            )));
        }
        return Ok(Vec::new());
    }

    // Refuse leftover text between / around steps.
    let mut cursor = 0usize;
    let mut steps = Vec::new();
    for (start, end, kind) in spans {
        let gap = source[cursor..start].trim();
        if !gap.is_empty() {
            return Err(Error::QueryInvalid(format!(
                "{DIAG_RQL_FULL_RESIDUAL}: within nested tokens not in this slice near `{gap}`"
            )));
        }
        match kind {
            0 => {
                let body = source[start + "enrich".len()..end].trim();
                steps.push(RawNestedStep::Enrich(body.to_string()));
            }
            1 => {
                let body = source[start + "within".len()..end].trim();
                steps.push(RawNestedStep::Within(body.to_string()));
            }
            2 => {
                let body = source[start + "where".len()..end].trim();
                if body.is_empty() {
                    return Err(Error::QueryInvalid("within where clause is empty".into()));
                }
                steps.push(RawNestedStep::Where(body.to_string()));
            }
            _ => unreachable!(),
        }
        cursor = end;
    }
    let trailing = source[cursor..].trim();
    if !trailing.is_empty() {
        return Err(Error::QueryInvalid(format!(
            "{DIAG_RQL_FULL_RESIDUAL}: within nested tokens not in this slice near `{trailing}`"
        )));
    }
    Ok(steps)
}

fn find_where_end(lower: &str, after_where: usize) -> usize {
    let terminals = [" enrich ", " within ", " where "];
    let padded = format!(" {} ", &lower[after_where..]);
    let mut best = None;
    for t in terminals {
        if let Some(rel) = padded.find(t) {
            let abs = after_where + rel;
            best = Some(best.map_or(abs, |b: usize| b.min(abs)));
        }
    }
    best.unwrap_or(lower.len())
}

fn parse_filter_step(
    body: &str,
    bindings: &CollectionBindings,
    element_alias: Option<&str>,
) -> Result<Predicate, Error> {
    let using = bindings.by_name.keys().next().ok_or_else(|| {
        Error::QueryInvalid("within where requires at least one collection binding".into())
    })?;
    let fake = format!("from {using} where {body}");
    let compiled = compile_app_core(&fake, bindings)?;
    strip_alias_from_predicate(compiled.plan.where_pred, element_alias)
}

fn strip_alias_from_predicate(pred: Predicate, alias: Option<&str>) -> Result<Predicate, Error> {
    let Some(a) = alias else {
        return Ok(pred);
    };
    Ok(match pred {
        Predicate::True | Predicate::False => pred,
        Predicate::Cmp { cmp, left, right } => Predicate::Cmp {
            cmp,
            left: strip_alias_from_operand(left, a)?,
            right: strip_alias_from_operand(right, a)?,
        },
        Predicate::In {
            left,
            list,
            negated,
        } => Predicate::In {
            left: strip_alias_from_operand(left, a)?,
            list,
            negated,
        },
        Predicate::Present { path } => Predicate::Present {
            path: strip_alias_prefix(path, Some(a))?,
        },
        Predicate::Missing { path } => Predicate::Missing {
            path: strip_alias_prefix(path, Some(a))?,
        },
        Predicate::IsNull { path, negated } => Predicate::IsNull {
            path: strip_alias_prefix(path, Some(a))?,
            negated,
        },
        Predicate::StartsWith { path, prefix } => Predicate::StartsWith {
            path: strip_alias_prefix(path, Some(a))?,
            prefix,
        },
        Predicate::Contains { path, needle } => Predicate::Contains {
            path: strip_alias_prefix(path, Some(a))?,
            needle,
        },
        Predicate::And { args } => Predicate::And {
            args: args
                .into_iter()
                .map(|p| strip_alias_from_predicate(p, Some(a)))
                .collect::<Result<Vec<_>, _>>()?,
        },
        Predicate::Or { args } => Predicate::Or {
            args: args
                .into_iter()
                .map(|p| strip_alias_from_predicate(p, Some(a)))
                .collect::<Result<Vec<_>, _>>()?,
        },
        Predicate::Not { arg } => Predicate::Not {
            arg: Box::new(strip_alias_from_predicate(*arg, Some(a))?),
        },
    })
}

fn strip_alias_from_operand(
    op: crate::predicate::Operand,
    alias: &str,
) -> Result<crate::predicate::Operand, Error> {
    use crate::predicate::Operand;
    match op {
        Operand::Path { path } => Ok(Operand::Path {
            path: strip_alias_prefix(path, Some(alias))?,
        }),
        other => Ok(other),
    }
}

fn parse_enrich_step(
    body: &str,
    bindings: &CollectionBindings,
    element_alias: Option<&str>,
) -> Result<EnrichStepV1, Error> {
    // Product:  <output> using <source> [as <alias>] matching <left> = <right> [where …] expect <card>
    // Corpus:   <output> from <source> on <left> = <right> <card>
    //           (optional collection prefixes on paths are stripped to field paths)
    let mut p = Words::new(body);
    let output = p.next_ident()?;
    let corpus_dialect = if p.eat("using") {
        false
    } else if p.eat("from") {
        true
    } else {
        return Err(Error::QueryInvalid(format!(
            "enrich expected `using` or `from` near `{}`",
            p.rest().chars().take(24).collect::<String>()
        )));
    };
    let using_name = p.next_ident()?;
    let foreign_alias = if p.eat("as") {
        Some(p.next_ident()?)
    } else {
        None
    };
    if corpus_dialect {
        p.expect("on")?;
    } else {
        p.expect("matching")?;
    }
    let left_raw = p.next_path()?;
    p.expect("=")?;
    let right_raw = p.next_path()?;
    // Corpus paths often qualify both sides (`orders._key`, `customers._key`).
    // Strip any bound collection name prefix from the left (root) side.
    let left_coll_hint = if corpus_dialect {
        left_raw
            .split('.')
            .next()
            .filter(|seg| bindings.by_name.contains_key(*seg))
    } else {
        None
    };
    let left = normalize_enrich_match_path(
        Path::parse_dotted(&left_raw)?,
        element_alias,
        left_coll_hint,
    )?;
    let right = normalize_enrich_match_path(
        Path::parse_dotted(&right_raw)?,
        foreign_alias.as_deref(),
        Some(using_name.as_str()),
    )?;
    let candidate_where = if !corpus_dialect && p.eat("where") {
        let where_src = p.take_until_keyword("expect")?;
        if where_src.trim().is_empty() {
            return Err(Error::QueryInvalid("enrich where clause is empty".into()));
        }
        let fake = format!("from {using_name} where {where_src}");
        let compiled = compile_app_core(&fake, bindings)?;
        Some(compiled.plan.where_pred)
    } else {
        None
    };
    if !corpus_dialect {
        p.expect("expect")?;
    }
    let card_s = p.next_ident()?;
    let expect = match card_s.as_str() {
        "exactly_one" => EnrichCardinality::ExactlyOne,
        "optional" => EnrichCardinality::Optional,
        "many" => EnrichCardinality::Many,
        other => {
            return Err(Error::QueryInvalid(format!(
                "enrich expect must be exactly_one|optional|many, got `{other}`"
            )));
        }
    };
    if !p.is_eof() {
        return Err(Error::QueryInvalid(format!(
            "unexpected trailing enrich tokens near `{}`",
            p.rest()
        )));
    }
    let using_id = bindings.by_name.get(&using_name).copied().ok_or_else(|| {
        Error::QueryInvalid(format!(
            "unknown collection binding `{using_name}` for enrich using"
        ))
    })?;
    Ok(EnrichStepV1 {
        output,
        using_name,
        using_id,
        left,
        right,
        candidate_where,
        expect,
    })
}

fn strip_alias_prefix(path: Path, alias: Option<&str>) -> Result<Path, Error> {
    let Some(a) = alias else {
        return Ok(path);
    };
    if path.0.first().map(String::as_str) != Some(a) {
        return Ok(path);
    }
    if path.0.len() == 1 {
        return Err(Error::QueryInvalid(format!(
            "path `{a}` is only an alias; need a field under it"
        )));
    }
    Path::from_segments(path.0.into_iter().skip(1))
}

/// Normalize enrich match paths: strip optional element/foreign aliases and
/// optional collection-name prefixes from corpus dialect (`customers._key` → `_key`).
fn normalize_enrich_match_path(
    path: Path,
    alias: Option<&str>,
    collection_name: Option<&str>,
) -> Result<Path, Error> {
    let path = strip_alias_prefix(path, alias)?;
    if let Some(col) = collection_name {
        if path.0.first().map(String::as_str) == Some(col) && path.0.len() > 1 {
            return Path::from_segments(path.0.into_iter().skip(1));
        }
    }
    Ok(path)
}

/// True when the path names the document store key (`_key` or `$key`).
fn is_store_key_path(path: &Path) -> bool {
    path.0.len() == 1 && (path.0[0] == "_key" || path.0[0] == "$key")
}

/// Tiny whitespace tokenizer for enrich / within clause bodies.
struct Words<'a> {
    s: &'a str,
    i: usize,
}

impl<'a> Words<'a> {
    fn new(s: &'a str) -> Self {
        Self { s, i: 0 }
    }
    fn skip(&mut self) {
        while self
            .s
            .as_bytes()
            .get(self.i)
            .copied()
            .is_some_and(|b| b.is_ascii_whitespace())
        {
            self.i += 1;
        }
    }
    fn is_eof(&mut self) -> bool {
        self.skip();
        self.i >= self.s.len()
    }
    fn rest(&mut self) -> &'a str {
        self.skip();
        &self.s[self.i..]
    }
    fn eat_char(&mut self, c: u8) -> bool {
        self.skip();
        if self.s.as_bytes().get(self.i) == Some(&c) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn eat(&mut self, kw: &str) -> bool {
        self.skip();
        let r = self.rest();
        let lower = r.to_ascii_lowercase();
        if lower.starts_with(kw)
            && (r.len() == kw.len()
                || !r.as_bytes()[kw.len()].is_ascii_alphanumeric()
                    && r.as_bytes()[kw.len()] != b'_')
        {
            self.i += kw.len();
            true
        } else {
            false
        }
    }
    fn expect(&mut self, kw: &str) -> Result<(), Error> {
        if self.eat(kw) {
            Ok(())
        } else {
            Err(Error::QueryInvalid(format!(
                "expected `{kw}` near `{}`",
                self.rest().chars().take(24).collect::<String>()
            )))
        }
    }
    fn next_ident(&mut self) -> Result<String, Error> {
        self.skip();
        let r = self.rest();
        let mut n = 0;
        for c in r.chars() {
            if c.is_ascii_alphanumeric() || c == '_' {
                n += c.len_utf8();
            } else {
                break;
            }
        }
        if n == 0 {
            return Err(Error::QueryInvalid(format!(
                "expected identifier near `{}`",
                r.chars().take(24).collect::<String>()
            )));
        }
        let id = r[..n].to_string();
        self.i += n;
        Ok(id)
    }
    fn next_path(&mut self) -> Result<String, Error> {
        let mut parts = vec![self.next_ident()?];
        loop {
            self.skip();
            if self.s.as_bytes().get(self.i) == Some(&b'.') {
                self.i += 1;
                parts.push(self.next_ident()?);
            } else {
                break;
            }
        }
        Ok(parts.join("."))
    }

    /// After consuming `{`, take inner text until the matching `}` (consumed).
    fn take_brace_inner(&mut self) -> Result<String, Error> {
        let start = self.i;
        let bytes = self.s.as_bytes();
        let mut depth = 1usize;
        while self.i < self.s.len() {
            match bytes[self.i] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        let inner = self.s[start..self.i].trim().to_string();
                        self.i += 1;
                        return Ok(inner);
                    }
                }
                _ => {}
            }
            self.i += 1;
        }
        Err(Error::QueryInvalid("within: unclosed `{`".into()))
    }

    /// Consume text until a keyword at a word boundary (keyword not consumed).
    fn take_until_keyword(&mut self, kw: &str) -> Result<String, Error> {
        self.skip();
        let start = self.i;
        let rest = &self.s[start..];
        let lower = rest.to_ascii_lowercase();
        let mut search = 0usize;
        let at = loop {
            let Some(rel) = lower[search..].find(kw) else {
                return Err(Error::QueryInvalid(format!(
                    "enrich where: expected `{kw}` after predicate"
                )));
            };
            let at = search + rel;
            let before_ok = at == 0 || lower.as_bytes()[at - 1].is_ascii_whitespace();
            let after = at + kw.len();
            let after_ok = after >= lower.len()
                || (!lower.as_bytes()[after].is_ascii_alphanumeric()
                    && lower.as_bytes()[after] != b'_');
            if before_ok && after_ok {
                break at;
            }
            search = at + 1;
        };
        let end = start + at;
        let taken = self.s[start..end].trim().to_string();
        self.i = end;
        Ok(taken)
    }
}

/// Keep rows where `pred` evaluates to true (SDA kernel — RQL-X4b).
pub(crate) fn filter_rows(
    rows: &[(String, JsonValue)],
    pred: &Predicate,
    params: &BTreeMap<String, JsonValue>,
) -> Result<Vec<(String, JsonValue)>, Error> {
    let where_k = super::kernel::compile_where(pred, params)?;
    let mut kept = Vec::with_capacity(rows.len());
    for (k, v) in rows {
        if where_k.eval_doc(v)? {
            kept.push((k.clone(), v.clone()));
        }
    }
    Ok(kept)
}

/// Apply nested `project { … }` to materialised rows.
pub(crate) fn apply_project_rows(
    rows: &[(String, JsonValue)],
    fields: &[ProjectItemV1],
    params: &BTreeMap<String, JsonValue>,
) -> Result<Vec<(String, JsonValue)>, Error> {
    let mut out = Vec::with_capacity(rows.len());
    for (k, v) in rows {
        let projected = project_value(v, Some(k), fields, params)?;
        out.push((k.clone(), projected));
    }
    Ok(out)
}

fn project_value(
    doc: &JsonValue,
    row_key: Option<&str>,
    fields: &[ProjectItemV1],
    params: &BTreeMap<String, JsonValue>,
) -> Result<JsonValue, Error> {
    let mut map = serde_json::Map::new();
    for item in fields {
        match item {
            ProjectItemV1::Leaf { output, source }
                if source.dotted() == "_key" && row_key.is_some() =>
            {
                map.insert(
                    output.clone(),
                    JsonValue::String(row_key.unwrap_or_default().to_string()),
                );
            }
            ProjectItemV1::Leaf { output, source } => match resolve_path(doc, source) {
                Resolve::Present(v) => {
                    map.insert(output.clone(), v);
                }
                Resolve::Absent => {}
            },
            ProjectItemV1::Nested { output, fields } => {
                let carrier_path = Path::from_segments(vec![output.clone()])?;
                match resolve_path(doc, &carrier_path) {
                    Resolve::Absent => {}
                    Resolve::Present(JsonValue::Null) => {
                        map.insert(output.clone(), JsonValue::Null);
                    }
                    Resolve::Present(JsonValue::Object(obj)) => {
                        let nested = project_value(&JsonValue::Object(obj), None, fields, params)?;
                        map.insert(output.clone(), nested);
                    }
                    Resolve::Present(JsonValue::Array(arr)) => {
                        let mut mapped = Vec::with_capacity(arr.len());
                        for el in arr {
                            mapped.push(project_value(&el, None, fields, params)?);
                        }
                        map.insert(output.clone(), JsonValue::Array(mapped));
                    }
                    Resolve::Present(other) => {
                        return Err(Error::QueryInvalid(format!(
                            "{DIAG_RQL_PROJECT_TYPE}: `{output}` is {} (need product/optional/bag)",
                            json_type_name(&other)
                        )));
                    }
                }
            }
            ProjectItemV1::Computed { output, expression } => {
                map.insert(output.clone(), eval_project_expr(doc, expression, params)?);
            }
        }
    }
    Ok(JsonValue::Object(map))
}

fn eval_project_expr(
    doc: &JsonValue,
    expression: &ProjectExprV1,
    params: &BTreeMap<String, JsonValue>,
) -> Result<JsonValue, Error> {
    match expression {
        ProjectExprV1::Literal(v) => Ok(v.clone()),
        ProjectExprV1::Path(path) => match resolve_path(doc, path) {
            Resolve::Present(v) => Ok(v),
            Resolve::Absent => Ok(JsonValue::Null),
        },
        ProjectExprV1::Conditional {
            when,
            then_expr,
            else_expr,
        } => {
            let kernel = super::kernel::compile_where(when, params)?;
            if kernel.eval_doc(doc)? {
                eval_project_expr(doc, then_expr, params)
            } else {
                eval_project_expr(doc, else_expr, params)
            }
        }
    }
}

/// Attach enrich fields onto already-materialised root JSON documents.
///
/// `foreign_docs` is the foreign candidate set (complete scan **or**
/// equality-index hits for observed left values). Callers must not label a
/// partial candidate set as a complete collection inventory.
///
/// `expect many` attaches a JSON array of matches, ordered by foreign key.
/// Optional [`EnrichStepV1::candidate_where`] filters foreign docs first.
#[cfg(test)]
pub(crate) fn attach_enrich_rows(
    roots: &[(String, JsonValue)],
    foreign_docs: &[(String, JsonValue)],
    step: &EnrichStepV1,
    params: &BTreeMap<String, JsonValue>,
) -> Result<Vec<(String, JsonValue)>, Error> {
    attach_enrich_rows_owned(roots.to_vec(), foreign_docs, step, params)
}

/// Ownership-preserving product attach path. Root documents are consumed and
/// amended in place; the query-local foreign table borrows its inventory and
/// clones only documents that are actually attached.
pub(crate) fn attach_enrich_rows_owned(
    roots: Vec<(String, JsonValue)>,
    foreign_docs: &[(String, JsonValue)],
    step: &EnrichStepV1,
    params: &BTreeMap<String, JsonValue>,
) -> Result<Vec<(String, JsonValue)>, Error> {
    let mut by_right: hashbrown::HashMap<String, Vec<(&str, &JsonValue)>> =
        hashbrown::HashMap::new();
    let candidate_k = match &step.candidate_where {
        Some(pred) => Some(super::kernel::compile_where(pred, params)?),
        None => None,
    };
    for (fk, doc) in foreign_docs {
        if let Some(ref where_k) = candidate_k {
            if !where_k.eval_doc(doc)? {
                continue;
            }
        }
        if let Some(v) = resolve_enrich_match_value(fk, doc, &step.right) {
            by_right
                .entry(canonical_match_key(&v))
                .or_default()
                .push((fk.as_str(), doc));
        }
    }
    for entries in by_right.values_mut() {
        entries.sort_by(|a, b| a.0.cmp(&b.0));
    }

    let mut out = Vec::with_capacity(roots.len());
    for (key, mut root) in roots {
        let left_key = match resolve_enrich_match_value(&key, &root, &step.left) {
            Some(v) => canonical_match_key(&v),
            None => match step.expect {
                EnrichCardinality::Optional => {
                    if let JsonValue::Object(map) = &mut root {
                        map.insert(step.output.clone(), JsonValue::Null);
                    }
                    out.push((key, root));
                    continue;
                }
                EnrichCardinality::Many => {
                    if let JsonValue::Object(map) = &mut root {
                        map.insert(step.output.clone(), JsonValue::Array(vec![]));
                    }
                    out.push((key, root));
                    continue;
                }
                EnrichCardinality::ExactlyOne => {
                    return Err(Error::QueryInvalid(format!(
                            "{DIAG_RQL_ENRICH_CARDINALITY}: missing left match path `{}` on key `{key}`",
                            step.left.0.join(".")
                        )));
                }
            },
        };
        let candidates = by_right.get(&left_key).map(|v| v.as_slice()).unwrap_or(&[]);
        let attached = match (step.expect, candidates.len()) {
            (EnrichCardinality::ExactlyOne, 1) => candidates[0].1.clone(),
            (EnrichCardinality::ExactlyOne, n) => {
                return Err(Error::QueryInvalid(format!(
                    "{DIAG_RQL_ENRICH_CARDINALITY}: exactly_one expected 1 match, got {n} (key `{key}`)"
                )));
            }
            (EnrichCardinality::Optional, 0) => JsonValue::Null,
            (EnrichCardinality::Optional, 1) => candidates[0].1.clone(),
            (EnrichCardinality::Optional, n) => {
                return Err(Error::QueryInvalid(format!(
                    "{DIAG_RQL_ENRICH_CARDINALITY}: optional expected ≤1 match, got {n} (key `{key}`)"
                )));
            }
            (EnrichCardinality::Many, _) => {
                JsonValue::Array(candidates.iter().map(|(_, d)| (*d).clone()).collect())
            }
        };
        match &mut root {
            JsonValue::Object(map) => {
                map.insert(step.output.clone(), attached);
            }
            _ => {
                return Err(Error::QueryInvalid(
                    "enrich attach requires object root documents".into(),
                ));
            }
        }
        out.push((key, root));
    }
    Ok(out)
}

/// Apply nested enrich / within steps to each element of a carrier array.
///
/// Expand carrier bags into a flat element working set (**RQL-VM4**).
///
/// Absent / Null / non-array carriers fail with [`DIAG_RQL_WITHIN_TYPE`].
pub(crate) fn within_enter(
    parents: &[(String, JsonValue)],
    carrier: &Path,
) -> Result<Vec<(String, JsonValue)>, Error> {
    let mut elements = Vec::new();
    for (key, root) in parents {
        let arr = match resolve_path(root, carrier) {
            Resolve::Present(JsonValue::Array(arr)) => arr,
            Resolve::Present(other) => {
                return Err(Error::QueryInvalid(format!(
                    "{DIAG_RQL_WITHIN_TYPE}: path `{}` is {} on key `{key}` (need sequence/bag)",
                    carrier.dotted(),
                    json_type_name(&other)
                )));
            }
            Resolve::Absent => {
                return Err(Error::QueryInvalid(format!(
                    "{DIAG_RQL_WITHIN_TYPE}: path `{}` absent on key `{key}`",
                    carrier.dotted()
                )));
            }
        };
        for (i, el) in arr.iter().enumerate() {
            elements.push((format!("{key}#{i}"), el.clone()));
        }
    }
    Ok(elements)
}

/// Write element working set back onto parent rows (**RQL-VM4**).
///
/// Elements are grouped by stripping the last `#index` suffix from synthetic keys.
pub(crate) fn within_leave(
    parents: &[(String, JsonValue)],
    carrier: &Path,
    elements: &[(String, JsonValue)],
) -> Result<Vec<(String, JsonValue)>, Error> {
    let mut out = Vec::with_capacity(parents.len());
    for (pkey, parent) in parents {
        let new_arr: Vec<JsonValue> = elements
            .iter()
            .filter(|(k, _)| k.rsplit_once('#').map(|(p, _)| p) == Some(pkey.as_str()))
            .map(|(_, v)| v.clone())
            .collect();
        let mut row = parent.clone();
        set_at_path(&mut row, carrier, JsonValue::Array(new_arr))?;
        out.push((pkey.clone(), row));
    }
    Ok(out)
}

/// `foreign_by_using` maps collection name → complete foreign docs for that
/// using-collection. Absent / Null / non-array carriers fail with
/// [`DIAG_RQL_WITHIN_TYPE`] (never treated as empty bags).
///
/// `foreign_by_id` maps immutable collection id → complete foreign docs.
/// Product execute uses the flat VM path ([`within_enter`] / body opcodes /
/// [`within_leave`]); this recursive helper is **test-only** (RQL-DEL1) —
/// kept for IR unit tests, not reachable from any product entry.
#[cfg(test)]
pub(crate) fn attach_within_rows(
    roots: &[(String, JsonValue)],
    foreign_by_id: &BTreeMap<CollectionId, Vec<(String, JsonValue)>>,
    step: &WithinStepV1,
    params: &BTreeMap<String, JsonValue>,
) -> Result<Vec<(String, JsonValue)>, Error> {
    let mut elements = within_enter(roots, &step.carrier)?;
    for nested in &step.steps {
        match nested {
            FullPipelineStepV1::Enrich(enrich) => {
                let foreign = foreign_by_id.get(&enrich.using_id).ok_or_else(|| {
                    Error::QueryInvalid(format!(
                        "within attach missing foreign docs for id {} (`{}`)",
                        enrich.using_id, enrich.using_name
                    ))
                })?;
                elements = attach_enrich_rows(&elements, foreign, enrich, params)?;
            }
            FullPipelineStepV1::Within(inner) => {
                elements = attach_within_rows(&elements, foreign_by_id, inner, params)?;
            }
            FullPipelineStepV1::Filter(pred) => {
                elements = filter_rows(&elements, pred, params)?;
            }
        }
    }
    within_leave(roots, &step.carrier, &elements)
}

fn json_type_name(v: &JsonValue) -> &'static str {
    match v {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn set_at_path(doc: &mut JsonValue, path: &Path, value: JsonValue) -> Result<(), Error> {
    if path.0.is_empty() {
        return Err(Error::QueryInvalid("empty path for within replace".into()));
    }
    let mut cur = doc;
    for (i, seg) in path.0.iter().enumerate() {
        if i + 1 == path.0.len() {
            match cur {
                JsonValue::Object(map) => {
                    map.insert(seg.clone(), value);
                    return Ok(());
                }
                _ => {
                    return Err(Error::QueryInvalid(format!(
                        "within replace requires object at parent of `{}`",
                        path.dotted()
                    )));
                }
            }
        }
        match cur {
            JsonValue::Object(map) => {
                cur = map.get_mut(seg).ok_or_else(|| {
                    Error::QueryInvalid(format!(
                        "within replace missing segment `{seg}` on `{}`",
                        path.dotted()
                    ))
                })?;
            }
            _ => {
                return Err(Error::QueryInvalid(format!(
                    "within replace requires object along `{}`",
                    path.dotted()
                )));
            }
        }
    }
    Err(Error::QueryInvalid("within replace unreachable".into()))
}

/// Result of [`execute_rql_full`] (one base page + enrich/within attach).
#[derive(Debug, Clone, PartialEq)]
pub struct RqlFullPage {
    /// Profile label.
    pub profile: &'static str,
    /// Rows after pipeline (+ optional project).
    pub rows: Vec<(String, JsonValue)>,
    /// Underlying Application Core page (pre-enrich values).
    pub base: QueryPage,
    /// Compiled ordered pipeline applied after the base page.
    pub pipeline: Vec<FullPipelineStepV1>,
    /// Nested project applied after the pipeline, if any.
    pub project: Option<Vec<ProjectItemV1>>,
    /// Per root-enrich load mode (RQL-I1). Within nested enrich omitted here
    /// (still scan-loaded; see residual inventory).
    pub enrich_loads: Vec<EnrichLoadEvidence>,
}

impl RqlFullPage {
    /// Root enrich steps in pipeline order.
    pub fn enrich(&self) -> Vec<&EnrichStepV1> {
        self.pipeline
            .iter()
            .filter_map(|s| match s {
                FullPipelineStepV1::Enrich(e) => Some(e),
                FullPipelineStepV1::Within(_) | FullPipelineStepV1::Filter(_) => None,
            })
            .collect()
    }

    /// First root `within` step, if any.
    pub fn within(&self) -> Option<&WithinStepV1> {
        self.pipeline.iter().find_map(|s| match s {
            FullPipelineStepV1::Within(w) => Some(w),
            FullPipelineStepV1::Enrich(_) | FullPipelineStepV1::Filter(_) => None,
        })
    }
}

/// Façade: compile full RQL → ISA → decode → execute (RQL-X5b).
///
/// Discovers collection bindings from [`HeapClient::list_collections`].
/// Root `enrich` tries equality-index pushdown on the foreign match field
/// (RQL-I1); falls back to complete `list_keys`+`get` when no usable index.
/// Nested `within` enrich still scan-loads. Multipage: call again with
/// `options.after` from `base.next`.
pub fn execute_rql_full(
    client: &mut HeapClient,
    source: &str,
    parameters: &Parameters,
    options: QueryRunOptions,
) -> Result<RqlFullPage, Error> {
    execute_rql_full_with(
        client,
        source,
        parameters,
        RqlFullExecuteOptions {
            query: options,
            force_enrich_scan: false,
        },
    )
}

/// [`execute_rql_full`] with differential-oracle controls.
///
/// Authority is **QVM1**: compile lowers Core+Full to QVM bytes, then
/// [`execute_full_qvm_with`] decodes/verifies and runs. RQB1 is removed (Q0.A10);
/// product path. [`CompiledRqlFull`] is not an executable sidecar.
pub fn execute_rql_full_with(
    client: &mut HeapClient,
    source: &str,
    parameters: &Parameters,
    options: RqlFullExecuteOptions,
) -> Result<RqlFullPage, Error> {
    let infos = client.list_collections()?;
    let mut bindings = CollectionBindings::default();
    for info in &infos {
        bindings.bind(&info.name, info.collection_id);
    }
    let heap_id = client.id();
    execute_rql_full_on_host_with(client, heap_id, source, &bindings, parameters, options)
}

/// Compile and execute Full RQL against collection-qualified host capabilities.
///
/// This is the shared embedded/server entry. The caller supplies bindings
/// derived from its authorised Heap catalogue; the compiled root collection
/// and every foreign collection id remain encoded in verified QVM operands.
pub fn execute_rql_full_on_host_with<H: HostCapabilities>(
    host: &mut H,
    heap_id: HeapId,
    source: &str,
    bindings: &CollectionBindings,
    parameters: &Parameters,
    mut options: RqlFullExecuteOptions,
) -> Result<RqlFullPage, Error> {
    let compiled = compile_rql_full(source, bindings)?;
    let semantic_parameters = crate::rql_app_core::bind_textual_after(
        compiled.base.after.as_ref(),
        parameters,
        &mut options.query,
    )?;
    let vm = lower_full(
        compiled.base.plan.clone(),
        compiled.base.budget,
        compiled.pipeline.clone(),
        compiled.project.clone(),
    );
    let qvm = super::qvm::encode_qvm(&vm)?;
    execute_full_qvm_on_host_with(host, heap_id, &qvm, &semantic_parameters, options)
}

/// Full-language product entry: decode **QVM1**, then [`run_vm`] once.
///
/// Foreign loads use collection-qualified HostCapabilities by immutable
/// collection id (RQL-P1b).
pub fn execute_full_qvm_with(
    client: &mut HeapClient,
    qvm_bytes: &[u8],
    parameters: &Parameters,
    options: RqlFullExecuteOptions,
) -> Result<RqlFullPage, Error> {
    let heap_id = client.id();
    execute_full_qvm_on_host_with(client, heap_id, qvm_bytes, parameters, options)
}

/// Decode, verify and execute Full QVM against an authorised host.
pub fn execute_full_qvm_on_host_with<H: HostCapabilities>(
    host: &mut H,
    heap_id: HeapId,
    qvm_bytes: &[u8],
    parameters: &Parameters,
    options: RqlFullExecuteOptions,
) -> Result<RqlFullPage, Error> {
    let vm = super::qvm::decode_qvm(qvm_bytes)?;
    // BindCollection imm is the collection id.
    let collection_id = match vm.ops.first().map(|i| &i.imm) {
        Some(super::vm_exec::VmImm::Collection(id)) => *id,
        _ => {
            return Err(Error::QueryInvalid(
                "execute_full_qvm: missing BindCollection".into(),
            ))
        }
    };
    // Recover attach pipeline/project for response diagnostics only (not authority).
    let (pipeline, project) = reconstruct_attach_from_ops(&vm.ops)?;
    let out = run_vm(
        host,
        &vm,
        &parameters.values,
        &options.query,
        heap_id,
        collection_id,
        options.force_enrich_scan,
    )?;

    Ok(RqlFullPage {
        profile: RQL_FULL_PROFILE,
        rows: out.rows,
        base: out.page,
        pipeline,
        project,
        enrich_loads: out.enrich_loads,
    })
}

/// Rebuild attach pipeline/project from verified flat ops (response diagnostics).
fn reconstruct_attach_from_ops(
    ops: &[super::vm_exec::VmInstr],
) -> Result<(Vec<FullPipelineStepV1>, Option<Vec<ProjectItemV1>>), Error> {
    use super::vm::OpCode;
    use super::vm_exec::VmImm;
    // Skip Core prefix (7 ops) until after ProjectPaths.
    let mut i = 7usize;
    let mut pipeline = Vec::new();
    let mut project = None;
    while i < ops.len() {
        match ops[i].op {
            OpCode::Halt => break,
            OpCode::Enrich => {
                let VmImm::Enrich(e) = &ops[i].imm else {
                    return Err(Error::QueryInvalid("reconstruct: Enrich imm".into()));
                };
                pipeline.push(FullPipelineStepV1::Enrich(e.clone()));
                i += 1;
            }
            OpCode::Within => {
                let VmImm::Within(w) = &ops[i].imm else {
                    return Err(Error::QueryInvalid("reconstruct: Within imm".into()));
                };
                let (steps, next) = reconstruct_within_body(ops, i + 1)?;
                pipeline.push(FullPipelineStepV1::Within(WithinStepV1 {
                    carrier: w.carrier.clone(),
                    element_alias: w.element_alias.clone(),
                    steps,
                }));
                i = next;
            }
            OpCode::FilterAttach => {
                let VmImm::FilterAttach(p) = &ops[i].imm else {
                    return Err(Error::QueryInvalid("reconstruct: FilterAttach imm".into()));
                };
                pipeline.push(FullPipelineStepV1::Filter(p.clone()));
                i += 1;
            }
            OpCode::ProjectBrace => {
                let VmImm::ProjectBrace(fields) = &ops[i].imm else {
                    return Err(Error::QueryInvalid("reconstruct: ProjectBrace imm".into()));
                };
                project = Some(fields.clone());
                i += 1;
            }
            other => {
                return Err(Error::QueryInvalid(format!(
                    "reconstruct: unexpected op {} at {i}",
                    other.name()
                )));
            }
        }
    }
    Ok((pipeline, project))
}

fn reconstruct_within_body(
    ops: &[super::vm_exec::VmInstr],
    mut i: usize,
) -> Result<(Vec<FullPipelineStepV1>, usize), Error> {
    use super::vm::OpCode;
    use super::vm_exec::VmImm;
    let mut steps = Vec::new();
    while i < ops.len() {
        match ops[i].op {
            OpCode::WithinEnd => return Ok((steps, i + 1)),
            OpCode::Enrich => {
                let VmImm::Enrich(e) = &ops[i].imm else {
                    return Err(Error::QueryInvalid("reconstruct within: Enrich".into()));
                };
                steps.push(FullPipelineStepV1::Enrich(e.clone()));
                i += 1;
            }
            OpCode::Within => {
                let VmImm::Within(w) = &ops[i].imm else {
                    return Err(Error::QueryInvalid("reconstruct within: Within".into()));
                };
                let (inner, next) = reconstruct_within_body(ops, i + 1)?;
                steps.push(FullPipelineStepV1::Within(WithinStepV1 {
                    carrier: w.carrier.clone(),
                    element_alias: w.element_alias.clone(),
                    steps: inner,
                }));
                i = next;
            }
            OpCode::FilterAttach => {
                let VmImm::FilterAttach(p) = &ops[i].imm else {
                    return Err(Error::QueryInvalid("reconstruct within: Filter".into()));
                };
                steps.push(FullPipelineStepV1::Filter(p.clone()));
                i += 1;
            }
            other => {
                return Err(Error::QueryInvalid(format!(
                    "reconstruct within: unexpected {} at {i}",
                    other.name()
                )));
            }
        }
    }
    Err(Error::QueryInvalid(
        "reconstruct within: missing WithinEnd".into(),
    ))
}

/// Load foreign docs for a **root** enrich: equality index when usable, else scan.
///
/// All I/O goes through collection-qualified [`HostCapabilities`] on
/// `step.using_id` (RQL-P1b). `using_name` is diagnostic only.
pub(crate) fn load_foreign_docs_for_root_enrich<H: HostCapabilities>(
    host: &mut H,
    step: &EnrichStepV1,
    roots: &[(String, JsonValue)],
    force_scan: bool,
) -> Result<(Vec<(String, JsonValue)>, EnrichAttachMode), Error> {
    let cid = step.using_id;
    if force_scan {
        return Ok((load_collection_docs(host, cid)?, EnrichAttachMode::Scan));
    }
    let right_field = step.right.dotted();
    // Equality indexes today are single-field path labels (APB-7 T4).
    if right_field.is_empty() || right_field.contains('.') {
        return Ok((load_collection_docs(host, cid)?, EnrichAttachMode::Scan));
    }

    let left_values = collect_present_left_values(roots, &step.left);

    if left_values.is_empty() {
        // Probe whether a usable equality index exists without loading all docs.
        match host.lookup_index_keys(cid, &[(right_field.clone(), JsonValue::Null)])? {
            None => {
                return Ok((load_collection_docs(host, cid)?, EnrichAttachMode::Scan));
            }
            Some(_) => return Ok((Vec::new(), EnrichAttachMode::EqualityIndex)),
        }
    }

    let values = left_values.into_values().collect::<Vec<_>>();
    let limits = host_limits();
    let mut candidate_keys = BTreeSet::new();
    let mut candidate_key_bytes = 0u64;
    for batch in values.chunks(4_096) {
        match host.lookup_index_keys_many(cid, &right_field, batch)? {
            None => return Ok((load_collection_docs(host, cid)?, EnrichAttachMode::Scan)),
            Some(keys) => {
                for key in keys {
                    let charge = 32 + u64::try_from(key.len()).unwrap_or(u64::MAX);
                    if candidate_keys.insert(key) {
                        candidate_key_bytes = candidate_key_bytes.saturating_add(charge);
                        check_result_bytes(candidate_key_bytes, None, &limits)?;
                    }
                }
            }
        }
    }

    let mut by_key: BTreeMap<String, JsonValue> = BTreeMap::new();
    let mut materialized_bytes = 0u64;
    let candidate_keys = candidate_keys.into_iter().collect::<Vec<_>>();
    for keys in candidate_keys.chunks(4_096) {
        for (key, document) in host.get_json_covered_many(cid, keys)? {
            match document {
                super::HostDocument::Present { value, .. } => {
                    let document = value.into_owned();
                    materialized_bytes =
                        materialized_bytes.saturating_add(estimate_row_bytes(&key, &document));
                    check_result_bytes(materialized_bytes, None, &limits)?;
                    by_key.insert(key, document);
                }
                // A concurrent delete cannot create a false match. Join
                // cardinality validation below remains authoritative.
                super::HostDocument::Absent => {}
                super::HostDocument::Hole { code } => {
                    return Err(Error::CoverageIncomplete(format!(
                        "enrich candidate `{key}` incomplete: {code}"
                    )));
                }
            }
        }
    }
    Ok((
        by_key.into_iter().collect(),
        EnrichAttachMode::EqualityIndex,
    ))
}

fn collect_present_left_values(
    roots: &[(String, JsonValue)],
    left: &Path,
) -> BTreeMap<String, JsonValue> {
    let mut out = BTreeMap::new();
    for (key, root) in roots {
        if let Some(v) = resolve_enrich_match_value(key, root, left) {
            out.insert(canonical_match_key(&v), v);
        }
    }
    out
}

/// Resolve an enrich match path on a document.
///
/// `_key` / `$key` means the store document key (not a body field). Audit
/// fixtures strip `_key` from the JSON body after using it as `put` key.
fn resolve_enrich_match_value(doc_key: &str, doc: &JsonValue, path: &Path) -> Option<JsonValue> {
    if is_store_key_path(path) {
        return Some(JsonValue::String(doc_key.to_string()));
    }
    match resolve_path(doc, path) {
        Resolve::Present(v) => Some(v),
        Resolve::Absent => None,
    }
}

/// Load foreign docs for `using_id` into a CollectionId-keyed cache (RQL-R1).
///
/// `using_name` is diagnostic-only and must never be a cache key.
pub(crate) fn ensure_foreign_docs<H: HostCapabilities>(
    host: &mut H,
    expected_id: CollectionId,
    cache: &mut BTreeMap<CollectionId, Vec<(String, JsonValue)>>,
) -> Result<(), Error> {
    if !cache.contains_key(&expected_id) {
        let docs = load_collection_docs(host, expected_id)?;
        cache.insert(expected_id, docs);
    }
    Ok(())
}

/// **Test-only** (RQL-DEL1): product `Within` foreign-doc collection goes
/// through [`ensure_foreign_docs`] inline in `run_vm`'s attach opcodes, not
/// this recursive pre-pass (which only served the deleted `run_attach_pipeline`
/// orchestrator).
#[cfg(test)]
pub(crate) fn collect_within_using_names<H: HostCapabilities>(
    step: &WithinStepV1,
    cache: &mut BTreeMap<CollectionId, Vec<(String, JsonValue)>>,
    host: &mut H,
) -> Result<(), Error> {
    for nested in &step.steps {
        match nested {
            FullPipelineStepV1::Enrich(e) => {
                ensure_foreign_docs(host, e.using_id, cache)?;
            }
            FullPipelineStepV1::Within(w) => {
                collect_within_using_names(w, cache, host)?;
            }
            FullPipelineStepV1::Filter(_) => {}
        }
    }
    Ok(())
}

/// Open by name and refuse if the live immutable id ≠ ISA-encoded id.
pub(crate) fn open_collection_bound(
    client: &mut HeapClient,
    name: &str,
    expected_id: CollectionId,
) -> Result<crate::app_v1::CollectionClient, Error> {
    let col = client.open_collection(name)?;
    if col.id() != expected_id {
        return Err(Error::QueryInvalid(format!(
            "collection id mismatch for `{name}`: opened {}, isa encoded {expected_id}",
            col.id()
        )));
    }
    Ok(col)
}

/// Full-scan a collection via collection-qualified host (RQL-P1b).
fn load_collection_docs<H: HostCapabilities>(
    host: &mut H,
    collection_id: CollectionId,
) -> Result<Vec<(String, JsonValue)>, Error> {
    let mut foreign = Vec::new();
    let limits = host_limits();
    let mut materialized_bytes = 0u64;
    let mut after: Option<String> = None;
    loop {
        let batch = host.list_keys(collection_id, Some(256), after.as_deref())?;
        if batch.is_empty() {
            break;
        }
        for k in &batch {
            if let Some(v) = host.get_json(collection_id, k)? {
                materialized_bytes = materialized_bytes.saturating_add(estimate_row_bytes(k, &v));
                check_result_bytes(materialized_bytes, None, &limits)?;
                foreign.push((k.clone(), v));
            }
        }
        after = batch.last().cloned();
        if batch.len() < 256 {
            break;
        }
    }
    Ok(foreign)
}

fn canonical_match_key(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "n".into(),
        JsonValue::Bool(value) => format!("b:{value}"),
        JsonValue::Number(value) => format!("d:{value}"),
        JsonValue::String(value) => {
            let mut key = String::with_capacity(value.len() + 2);
            key.push_str("s:");
            key.push_str(value);
            key
        }
        JsonValue::Array(_) | JsonValue::Object(_) => serde_json::to_string(v)
            .map(|value| format!("j:{value}"))
            .unwrap_or_else(|_| format!("j:{v:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rql_app_core::DIAG_RQL_FEATURE_UNAVAILABLE;
    use residiuum_heap::CollectionId;
    use std::str::FromStr;

    fn bindings() -> CollectionBindings {
        let mut b = CollectionBindings::default();
        b.bind(
            "orders",
            CollectionId::from_str("00000000-0000-4000-8000-0000000000a1").unwrap(),
        );
        b.bind(
            "customers",
            CollectionId::from_str("00000000-0000-4000-8000-0000000000a2").unwrap(),
        );
        b.bind(
            "line_items",
            CollectionId::from_str("00000000-0000-4000-8000-0000000000a3").unwrap(),
        );
        b.bind(
            "products",
            CollectionId::from_str("00000000-0000-4000-8000-0000000000a4").unwrap(),
        );
        b.bind(
            "warehouses",
            CollectionId::from_str("00000000-0000-4000-8000-0000000000a5").unwrap(),
        );
        b.bind(
            "components",
            CollectionId::from_str("00000000-0000-4000-8000-0000000000a6").unwrap(),
        );
        b
    }

    fn cid(name: &str) -> CollectionId {
        bindings().resolve(name).expect(name)
    }

    #[test]
    fn core_still_rejects_enrich() {
        let err = compile_app_core(
            "from orders enrich customer using customers matching customer_id = id expect exactly_one",
            &bindings(),
        )
        .unwrap_err();
        assert!(err.to_string().contains(DIAG_RQL_FEATURE_UNAVAILABLE));
    }

    #[test]
    fn compile_single_enrich() {
        let c = compile_rql_full(
            r#"from orders
               enrich customer using customers matching customer_id = id expect exactly_one
               page size 10"#,
            &bindings(),
        )
        .unwrap();
        assert_eq!(c.profile, RQL_FULL_PROFILE);
        assert_eq!(c.root_enrich().len(), 1);
        assert_eq!(c.root_enrich()[0].output, "customer");
        assert_eq!(c.root_enrich()[0].expect, EnrichCardinality::ExactlyOne);
        assert!(c.first_within().is_none());
        assert!(c.base_source.contains("from orders"));
        assert!(!c.base_source.contains("enrich"));
        assert_eq!(c.base.plan.page_size, 10);
    }

    #[test]
    fn computed_conditional_project_compiles_and_evaluates() {
        let compiled = compile_rql_full(
            r#"from orders project _key, amount_band = if amount < 100 then "low" else if amount < 500 then "mid" else "high""#,
            &bindings(),
        )
        .unwrap();
        assert_eq!(compiled.base_source, "from orders");
        let project = compiled.project.as_ref().unwrap();
        let rows = vec![
            ("o1".into(), serde_json::json!({"amount": 10})),
            ("o2".into(), serde_json::json!({"amount": 250})),
            ("o3".into(), serde_json::json!({"amount": 900})),
        ];
        let out = apply_project_rows(&rows, project, &BTreeMap::new()).unwrap();
        assert_eq!(
            out[0].1,
            serde_json::json!({"_key": "o1", "amount_band": "low"})
        );
        assert_eq!(
            out[1].1,
            serde_json::json!({"_key": "o2", "amount_band": "mid"})
        );
        assert_eq!(
            out[2].1,
            serde_json::json!({"_key": "o3", "amount_band": "high"})
        );
    }

    #[test]
    fn compile_corpus_enrich_dialect_from_on_card() {
        let c = compile_rql_full(
            "from orders enrich customer from customers on customer.id = customers._key exactly_one",
            &bindings(),
        )
        .expect("corpus enrich dialect");
        assert_eq!(c.root_enrich().len(), 1);
        let e = c.root_enrich()[0];
        assert_eq!(e.output, "customer");
        assert_eq!(e.using_name, "customers");
        assert_eq!(e.left.dotted(), "customer.id");
        assert_eq!(e.right.dotted(), "_key");
        assert_eq!(e.expect, EnrichCardinality::ExactlyOne);
    }

    #[test]
    fn attach_enrich_matches_store_key_path() {
        let roots = vec![("o1".into(), serde_json::json!({"customer": {"id": "c1"}}))];
        let foreign = vec![
            ("c1".into(), serde_json::json!({"name": "Ada"})),
            ("c2".into(), serde_json::json!({"name": "Bob"})),
        ];
        let step = EnrichStepV1 {
            output: "customer".into(),
            using_name: "customers".into(),
            using_id: cid("customers"),
            left: Path::parse_dotted("customer.id").unwrap(),
            right: Path::parse_dotted("_key").unwrap(),
            candidate_where: None,
            expect: EnrichCardinality::ExactlyOne,
        };
        let out = attach_enrich_rows(&roots, &foreign, &step, &Default::default()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1["customer"]["name"], "Ada");
    }

    #[test]
    fn compile_and_attach_within() {
        let c = compile_rql_full(
            r#"from orders
               enrich items using line_items matching order_id = order_id expect many
               within items as item {
                 enrich product using products as candidate
                   matching item.product_id = candidate.id
                   expect exactly_one
               }"#,
            &bindings(),
        )
        .unwrap();
        assert_eq!(c.root_enrich().len(), 1);
        let w = c.first_within().unwrap();
        assert_eq!(w.carrier.dotted(), "items");
        assert_eq!(w.element_alias.as_deref(), Some("item"));
        assert_eq!(w.enrich_steps().len(), 1);
        assert_eq!(w.enrich_steps()[0].output, "product");
        assert_eq!(w.enrich_steps()[0].left.dotted(), "product_id");
        assert_eq!(w.enrich_steps()[0].right.dotted(), "id");

        let roots = vec![("o1".into(), serde_json::json!({"order_id": "o1"}))];
        let lines = vec![
            (
                "l1".into(),
                serde_json::json!({"order_id": "o1", "product_id": "p1", "sku": "A"}),
            ),
            (
                "l2".into(),
                serde_json::json!({"order_id": "o1", "product_id": "p2", "sku": "B"}),
            ),
        ];
        let products = vec![
            (
                "p1".into(),
                serde_json::json!({"id": "p1", "name": "Widget"}),
            ),
            (
                "p2".into(),
                serde_json::json!({"id": "p2", "name": "Gadget"}),
            ),
        ];
        let after_many =
            attach_enrich_rows(&roots, &lines, c.root_enrich()[0], &BTreeMap::new()).unwrap();
        let mut foreign = BTreeMap::new();
        foreign.insert(cid("products"), products);
        let out = attach_within_rows(&after_many, &foreign, w, &BTreeMap::new()).unwrap();
        let bag = out[0].1["items"].as_array().unwrap();
        assert_eq!(bag.len(), 2);
        assert_eq!(bag[0]["product"]["name"], "Widget");
        assert_eq!(bag[1]["product"]["name"], "Gadget");
    }

    #[test]
    fn compile_and_attach_multi_root_enrich() {
        let c = compile_rql_full(
            r#"from orders
               enrich customer using customers matching customer_id = id expect exactly_one
               enrich items using line_items matching order_id = order_id expect many"#,
            &bindings(),
        )
        .unwrap();
        assert_eq!(c.root_enrich().len(), 2);
        assert_eq!(c.root_enrich()[0].output, "customer");
        assert_eq!(c.root_enrich()[1].output, "items");

        let roots = vec![(
            "o1".into(),
            serde_json::json!({"order_id": "o1", "customer_id": "c1"}),
        )];
        let customers = vec![("c1".into(), serde_json::json!({"id": "c1", "name": "Ada"}))];
        let lines = vec![
            (
                "l1".into(),
                serde_json::json!({"order_id": "o1", "sku": "A"}),
            ),
            (
                "l2".into(),
                serde_json::json!({"order_id": "o1", "sku": "B"}),
            ),
        ];
        let mid =
            attach_enrich_rows(&roots, &customers, c.root_enrich()[0], &BTreeMap::new()).unwrap();
        let out = attach_enrich_rows(&mid, &lines, c.root_enrich()[1], &BTreeMap::new()).unwrap();
        assert_eq!(out[0].1["customer"]["name"], "Ada");
        assert_eq!(out[0].1["items"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn compile_and_attach_multi_within_enrich() {
        let mut b = bindings();
        b.bind(
            "warehouses",
            CollectionId::from_str("00000000-0000-4000-8000-0000000000a5").unwrap(),
        );
        let c = compile_rql_full(
            r#"from orders
               enrich items using line_items matching order_id = order_id expect many
               within items as item {
                 enrich product using products as candidate
                   matching item.product_id = candidate.id
                   expect exactly_one
                 enrich warehouse using warehouses as wh
                   matching item.warehouse_id = wh.id
                   expect optional
               }"#,
            &b,
        )
        .unwrap();
        let w = c.first_within().unwrap();
        assert_eq!(w.enrich_steps().len(), 2);
        assert_eq!(w.enrich_steps()[0].output, "product");
        assert_eq!(w.enrich_steps()[1].output, "warehouse");

        let roots = vec![("o1".into(), serde_json::json!({"order_id": "o1"}))];
        let lines = vec![(
            "l1".into(),
            serde_json::json!({
                "order_id": "o1",
                "product_id": "p1",
                "warehouse_id": "w1"
            }),
        )];
        let products = vec![(
            "p1".into(),
            serde_json::json!({"id": "p1", "name": "Widget"}),
        )];
        let warehouses = vec![("w1".into(), serde_json::json!({"id": "w1", "city": "Oslo"}))];
        let after_many =
            attach_enrich_rows(&roots, &lines, c.root_enrich()[0], &BTreeMap::new()).unwrap();
        let mut foreign = BTreeMap::new();
        foreign.insert(cid("products"), products);
        foreign.insert(cid("warehouses"), warehouses);
        let out = attach_within_rows(&after_many, &foreign, w, &BTreeMap::new()).unwrap();
        let item = &out[0].1["items"].as_array().unwrap()[0];
        assert_eq!(item["product"]["name"], "Widget");
        assert_eq!(item["warehouse"]["city"], "Oslo");
    }

    #[test]
    fn within_type_error_on_absent() {
        let step = WithinStepV1 {
            carrier: Path::parse_dotted("items").unwrap(),
            element_alias: None,
            steps: vec![FullPipelineStepV1::Enrich(EnrichStepV1 {
                output: "product".into(),
                using_name: "products".into(),
                using_id: CollectionId::from_str("00000000-0000-4000-8000-0000000000a4").unwrap(),
                left: Path::parse_dotted("product_id").unwrap(),
                right: Path::parse_dotted("id").unwrap(),
                candidate_where: None,
                expect: EnrichCardinality::ExactlyOne,
            })],
        };
        let roots = vec![("o1".into(), serde_json::json!({"order_id": "o1"}))];
        let foreign = BTreeMap::new();
        let err = attach_within_rows(&roots, &foreign, &step, &BTreeMap::new()).unwrap_err();
        assert!(err.to_string().contains(DIAG_RQL_WITHIN_TYPE));
    }

    #[test]
    fn compile_and_attach_nested_within() {
        let mut b = bindings();
        b.bind(
            "components",
            CollectionId::from_str("00000000-0000-4000-8000-0000000000a6").unwrap(),
        );
        let c = compile_rql_full(
            r#"from orders
               enrich items using line_items matching order_id = order_id expect many
               within items as item {
                 enrich parts using components matching item.sku = parent_sku expect many
                 within item.parts as part {
                   enrich product using products as candidate
                     matching part.product_id = candidate.id
                     expect exactly_one
                 }
               }"#,
            &b,
        )
        .unwrap();
        let w = c.first_within().unwrap();
        assert_eq!(w.steps.len(), 2);
        assert!(matches!(&w.steps[0], FullPipelineStepV1::Enrich(_)));
        assert!(matches!(&w.steps[1], FullPipelineStepV1::Within(_)));
        let FullPipelineStepV1::Within(inner) = &w.steps[1] else {
            panic!("expected nested within");
        };
        assert_eq!(inner.carrier.dotted(), "parts");
        assert_eq!(inner.enrich_steps()[0].left.dotted(), "product_id");

        let roots = vec![("o1".into(), serde_json::json!({"order_id": "o1"}))];
        let lines = vec![(
            "l1".into(),
            serde_json::json!({"order_id": "o1", "sku": "A"}),
        )];
        let components = vec![
            (
                "c1".into(),
                serde_json::json!({"parent_sku": "A", "product_id": "p1"}),
            ),
            (
                "c2".into(),
                serde_json::json!({"parent_sku": "A", "product_id": "p2"}),
            ),
        ];
        let products = vec![
            (
                "p1".into(),
                serde_json::json!({"id": "p1", "name": "Widget"}),
            ),
            (
                "p2".into(),
                serde_json::json!({"id": "p2", "name": "Gadget"}),
            ),
        ];
        let after_many =
            attach_enrich_rows(&roots, &lines, c.root_enrich()[0], &BTreeMap::new()).unwrap();
        let mut foreign = BTreeMap::new();
        foreign.insert(cid("components"), components);
        foreign.insert(cid("products"), products);
        let out = attach_within_rows(&after_many, &foreign, w, &BTreeMap::new()).unwrap();
        let parts = out[0].1["items"].as_array().unwrap()[0]["parts"]
            .as_array()
            .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0]["product"]["name"], "Widget");
        assert_eq!(parts[1]["product"]["name"], "Gadget");
    }

    #[test]
    fn compile_and_attach_enrich_after_within() {
        let c = compile_rql_full(
            r#"from orders
               enrich items using line_items matching order_id = order_id expect many
               within items as item {
                 enrich product using products as candidate
                   matching item.product_id = candidate.id
                   expect exactly_one
               }
               enrich customer using customers matching customer_id = id expect exactly_one"#,
            &bindings(),
        )
        .unwrap();
        assert_eq!(c.pipeline.len(), 3);
        assert!(matches!(&c.pipeline[0], FullPipelineStepV1::Enrich(_)));
        assert!(matches!(&c.pipeline[1], FullPipelineStepV1::Within(_)));
        assert!(matches!(&c.pipeline[2], FullPipelineStepV1::Enrich(_)));
        assert_eq!(c.root_enrich().len(), 2);
        assert_eq!(c.root_enrich()[1].output, "customer");

        let roots = vec![(
            "o1".into(),
            serde_json::json!({"order_id": "o1", "customer_id": "c1"}),
        )];
        let lines = vec![(
            "l1".into(),
            serde_json::json!({"order_id": "o1", "product_id": "p1"}),
        )];
        let products = vec![(
            "p1".into(),
            serde_json::json!({"id": "p1", "name": "Widget"}),
        )];
        let customers = vec![("c1".into(), serde_json::json!({"id": "c1", "name": "Ada"}))];
        let mid = attach_enrich_rows(&roots, &lines, c.root_enrich()[0], &BTreeMap::new()).unwrap();
        let mut foreign = BTreeMap::new();
        foreign.insert(cid("products"), products);
        let after_within =
            attach_within_rows(&mid, &foreign, c.first_within().unwrap(), &BTreeMap::new())
                .unwrap();
        let out = attach_enrich_rows(
            &after_within,
            &customers,
            c.root_enrich()[1],
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(out[0].1["customer"]["name"], "Ada");
        assert_eq!(
            out[0].1["items"].as_array().unwrap()[0]["product"]["name"],
            "Widget"
        );
    }

    #[test]
    fn refuse_within_depth_overflow() {
        let mut nested = String::from("enrich x using products matching a = b expect optional");
        for _ in 0..MAX_WITHIN_DEPTH {
            nested = format!("within nest {{ {nested} }}");
        }
        // depth 1..MAX are ok; one more exceeds
        nested = format!("within nest {{ {nested} }}");
        let src = format!("from orders {nested}");
        let err = compile_rql_full(&src, &bindings()).unwrap_err();
        assert!(
            err.to_string().contains(DIAG_RQL_FULL_RESIDUAL),
            "unexpected: {err}"
        );
        assert!(err.to_string().contains("depth exceeds"));
    }

    #[test]
    fn compile_and_attach_nested_where() {
        let c = compile_rql_full(
            r#"from orders
               enrich items using line_items matching order_id = order_id expect many
               within items as item {
                 where item.qty > 1
                 enrich product using products as candidate
                   matching item.product_id = candidate.id
                   expect exactly_one
               }"#,
            &bindings(),
        )
        .unwrap();
        let w = c.first_within().unwrap();
        assert_eq!(w.steps.len(), 2);
        assert!(matches!(&w.steps[0], FullPipelineStepV1::Filter(_)));
        assert!(matches!(&w.steps[1], FullPipelineStepV1::Enrich(_)));

        let roots = vec![("o1".into(), serde_json::json!({"order_id": "o1"}))];
        let lines = vec![
            (
                "l1".into(),
                serde_json::json!({"order_id": "o1", "product_id": "p1", "qty": 1}),
            ),
            (
                "l2".into(),
                serde_json::json!({"order_id": "o1", "product_id": "p2", "qty": 3}),
            ),
        ];
        let products = vec![
            (
                "p1".into(),
                serde_json::json!({"id": "p1", "name": "Widget"}),
            ),
            (
                "p2".into(),
                serde_json::json!({"id": "p2", "name": "Gadget"}),
            ),
        ];
        let after_many =
            attach_enrich_rows(&roots, &lines, c.root_enrich()[0], &BTreeMap::new()).unwrap();
        let mut foreign = BTreeMap::new();
        foreign.insert(cid("products"), products);
        let out = attach_within_rows(&after_many, &foreign, w, &BTreeMap::new()).unwrap();
        let bag = out[0].1["items"].as_array().unwrap();
        assert_eq!(bag.len(), 1);
        assert_eq!(bag[0]["qty"], 3);
        assert_eq!(bag[0]["product"]["name"], "Gadget");
    }

    #[test]
    fn compile_and_attach_where_after_enrich_in_within() {
        let c = compile_rql_full(
            r#"from orders
               enrich items using line_items matching order_id = order_id expect many
               within items as item {
                 enrich product using products as candidate
                   matching item.product_id = candidate.id
                   expect exactly_one
                 where item.product.name = "Widget"
               }"#,
            &bindings(),
        )
        .unwrap();
        let w = c.first_within().unwrap();
        assert!(matches!(&w.steps[0], FullPipelineStepV1::Enrich(_)));
        assert!(matches!(&w.steps[1], FullPipelineStepV1::Filter(_)));

        let roots = vec![("o1".into(), serde_json::json!({"order_id": "o1"}))];
        let lines = vec![
            (
                "l1".into(),
                serde_json::json!({"order_id": "o1", "product_id": "p1"}),
            ),
            (
                "l2".into(),
                serde_json::json!({"order_id": "o1", "product_id": "p2"}),
            ),
        ];
        let products = vec![
            (
                "p1".into(),
                serde_json::json!({"id": "p1", "name": "Widget"}),
            ),
            (
                "p2".into(),
                serde_json::json!({"id": "p2", "name": "Gadget"}),
            ),
        ];
        let after_many =
            attach_enrich_rows(&roots, &lines, c.root_enrich()[0], &BTreeMap::new()).unwrap();
        let mut foreign = BTreeMap::new();
        foreign.insert(cid("products"), products);
        let out = attach_within_rows(&after_many, &foreign, w, &BTreeMap::new()).unwrap();
        let bag = out[0].1["items"].as_array().unwrap();
        assert_eq!(bag.len(), 1);
        assert_eq!(bag[0]["product"]["name"], "Widget");
    }

    #[test]
    fn compile_and_attach_root_where_after_enrich() {
        let c = compile_rql_full(
            r#"from orders
               where status = "paid"
               enrich customer using customers matching customer_id = id expect exactly_one
               where customer.country = "TH"
               page size 10"#,
            &bindings(),
        )
        .unwrap();
        assert_eq!(c.pipeline.len(), 2);
        assert!(matches!(&c.pipeline[0], FullPipelineStepV1::Enrich(_)));
        assert!(matches!(&c.pipeline[1], FullPipelineStepV1::Filter(_)));
        assert!(c.base_source.contains("where status"));
        assert!(!c.base_source.contains("customer.country"));

        let roots = vec![
            (
                "o1".into(),
                serde_json::json!({"customer_id": "c1", "status": "paid"}),
            ),
            (
                "o2".into(),
                serde_json::json!({"customer_id": "c2", "status": "paid"}),
            ),
        ];
        let customers = vec![
            (
                "c1".into(),
                serde_json::json!({"id": "c1", "country": "TH", "name": "Ada"}),
            ),
            (
                "c2".into(),
                serde_json::json!({"id": "c2", "country": "US", "name": "Bob"}),
            ),
        ];
        let mid =
            attach_enrich_rows(&roots, &customers, c.root_enrich()[0], &BTreeMap::new()).unwrap();
        let FullPipelineStepV1::Filter(pred) = &c.pipeline[1] else {
            panic!("expected filter");
        };
        let out = filter_rows(&mid, pred, &BTreeMap::new()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1["customer"]["name"], "Ada");
    }

    #[test]
    fn compile_and_apply_nested_project() {
        let c = compile_rql_full(
            r#"from orders
               enrich customer using customers matching customer_id = id expect exactly_one
               enrich items using line_items matching order_id = order_id expect many
               within items as item {
                 enrich product using products as candidate
                   matching item.product_id = candidate.id
                   expect exactly_one
               }
               project {
                 order_id,
                 customer { name },
                 items { sku, product { name } }
               }"#,
            &bindings(),
        )
        .unwrap();
        let proj = c.project.as_ref().unwrap();
        assert_eq!(proj.len(), 3);
        assert!(c.base_source.contains("from orders"));
        assert!(!c.base_source.contains("project"));

        let roots = vec![(
            "o1".into(),
            serde_json::json!({"order_id": "o1", "customer_id": "c1"}),
        )];
        let customers = vec![("c1".into(), serde_json::json!({"id": "c1", "name": "Ada"}))];
        let lines = vec![(
            "l1".into(),
            serde_json::json!({"order_id": "o1", "product_id": "p1", "sku": "A"}),
        )];
        let products = vec![(
            "p1".into(),
            serde_json::json!({"id": "p1", "name": "Widget"}),
        )];
        let mid =
            attach_enrich_rows(&roots, &customers, c.root_enrich()[0], &BTreeMap::new()).unwrap();
        let mid2 = attach_enrich_rows(&mid, &lines, c.root_enrich()[1], &BTreeMap::new()).unwrap();
        let mut foreign = BTreeMap::new();
        foreign.insert(cid("products"), products);
        let enriched =
            attach_within_rows(&mid2, &foreign, c.first_within().unwrap(), &BTreeMap::new())
                .unwrap();
        let out = apply_project_rows(&enriched, proj, &BTreeMap::new()).unwrap();
        assert_eq!(out[0].1["order_id"], "o1");
        assert_eq!(out[0].1["customer"]["name"], "Ada");
        assert!(out[0].1["customer"].get("id").is_none());
        let item = &out[0].1["items"].as_array().unwrap()[0];
        assert_eq!(item["sku"], "A");
        assert_eq!(item["product"]["name"], "Widget");
        assert!(item.get("product_id").is_none());
    }

    #[test]
    fn project_conflict_and_type_error() {
        let err = compile_rql_full(r#"from orders project { id, id }"#, &bindings()).unwrap_err();
        assert!(err.to_string().contains(DIAG_RQL_PROJECTION_CONFLICT));

        let fields = vec![ProjectItemV1::Nested {
            output: "status".into(),
            fields: vec![ProjectItemV1::Leaf {
                output: "x".into(),
                source: Path::parse_dotted("x").unwrap(),
            }],
        }];
        let rows = vec![("o1".into(), serde_json::json!({"status": "paid"}))];
        let err = apply_project_rows(&rows, &fields, &BTreeMap::new()).unwrap_err();
        assert!(err.to_string().contains(DIAG_RQL_PROJECT_TYPE));
    }

    #[test]
    fn compile_and_attach_many() {
        let c = compile_rql_full(
            "from orders enrich items using line_items matching order_id = order_id expect many",
            &bindings(),
        )
        .unwrap();
        assert_eq!(c.root_enrich()[0].expect, EnrichCardinality::Many);

        let step = c.root_enrich()[0];
        let roots = vec![
            ("o1".into(), serde_json::json!({"order_id": "o1"})),
            ("o2".into(), serde_json::json!({"order_id": "o2"})),
        ];
        let foreign = vec![
            (
                "l2".into(),
                serde_json::json!({"order_id": "o1", "sku": "B"}),
            ),
            (
                "l1".into(),
                serde_json::json!({"order_id": "o1", "sku": "A"}),
            ),
            (
                "l3".into(),
                serde_json::json!({"order_id": "o2", "sku": "C"}),
            ),
        ];
        let out = attach_enrich_rows(&roots, &foreign, step, &BTreeMap::new()).unwrap();
        let bag = out[0].1["items"].as_array().unwrap();
        assert_eq!(bag.len(), 2);
        assert_eq!(bag[0]["sku"], "A");
        assert_eq!(bag[1]["sku"], "B");
        assert_eq!(out[1].1["items"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn attach_exactly_one_and_optional() {
        let step = EnrichStepV1 {
            output: "customer".into(),
            using_name: "customers".into(),
            using_id: CollectionId::from_str("00000000-0000-4000-8000-0000000000a2").unwrap(),
            left: Path::parse_dotted("customer_id").unwrap(),
            right: Path::parse_dotted("id").unwrap(),
            candidate_where: None,
            expect: EnrichCardinality::ExactlyOne,
        };
        let roots = vec![(
            "o1".into(),
            serde_json::json!({"customer_id": "c1", "n": 1}),
        )];
        let foreign = vec![
            ("c1".into(), serde_json::json!({"id": "c1", "name": "Ada"})),
            ("c2".into(), serde_json::json!({"id": "c2", "name": "Bob"})),
        ];
        let out = attach_enrich_rows(&roots, &foreign, &step, &BTreeMap::new()).unwrap();
        assert_eq!(out[0].1["customer"]["name"], "Ada");

        let mut opt = step.clone();
        opt.expect = EnrichCardinality::Optional;
        let roots2 = vec![("o2".into(), serde_json::json!({"customer_id": "missing"}))];
        let out2 = attach_enrich_rows(&roots2, &foreign, &opt, &BTreeMap::new()).unwrap();
        assert!(out2[0].1["customer"].is_null());
    }

    #[test]
    fn compile_and_attach_candidate_where() {
        let c = compile_rql_full(
            r#"from orders
               enrich customer using customers matching customer_id = id
               where active = true
               expect exactly_one"#,
            &bindings(),
        )
        .unwrap();
        assert!(c.root_enrich()[0].candidate_where.is_some());

        let roots = vec![("o1".into(), serde_json::json!({"customer_id": "c1"}))];
        let foreign = vec![
            (
                "c1a".into(),
                serde_json::json!({"id": "c1", "active": false, "name": "old"}),
            ),
            (
                "c1b".into(),
                serde_json::json!({"id": "c1", "active": true, "name": "Ada"}),
            ),
        ];
        let out =
            attach_enrich_rows(&roots, &foreign, c.root_enrich()[0], &BTreeMap::new()).unwrap();
        assert_eq!(out[0].1["customer"]["name"], "Ada");
    }
}
