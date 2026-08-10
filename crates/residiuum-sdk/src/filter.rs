//! SDK-native JSON filters (DX_SPEC §7.1–§7.2).
//!
//! Common field predicates without requiring callers to write SDA. Filters are
//! evaluated by scanning live collection entries (correct without secondary
//! indexes; DX_SPEC §8.1).
//!
//! This is the **JSON / Mongo filter dialect**: the portable object vocabulary
//! is also available as dialect ids `json` and `mongo` via
//! [`crate::compile_dialect`] (see `doc/SDA/DIALECTS.md`). Pure SDA remains the
//! mathematical language; this module is a comfortable frontend that compiles
//! into it ([`Filter::to_sda`]).
//!
//! ## SDA alignment (DEF-028 / DX_SPEC §7.1)
//!
//! Portable filters compile to boolean SDA programs over document `input`
//! ([`Filter::to_sda`]) and evaluate equivalently via [`Filter::matches`] and
//! [`Filter::matches_sda`]. Path absence uses `getPath` → `None`; stored JSON
//! `null` is `Some(null)`. Comparison and containment failures do not match.
//!
//! Serializable plans use profile tag [`QUERY_PLAN_PROFILE`].

use crate::error::Error;
use serde_json::Value as JsonValue;

/// Version tag for serialized SDK query plans (DEF-028).
pub const QUERY_PLAN_PROFILE: &str = "residiuum-query-plan-v1";

/// Sort direction for ordered queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortOrder {
    /// Ascending (default).
    #[default]
    Asc,
    /// Descending.
    Desc,
}

/// Explicit resource budget for scans (DX_SPEC §8.1 / DEF-029).
///
/// When set and a scan would exceed a field without a usable index, the query
/// fails with [`crate::ErrorCode::QueryBudgetRequired`] (unless
/// [`QueryOptions::allow_partial_coverage`] returns the matches collected so
/// far). Hard host ceilings still apply via [`crate::ResourceLimits`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryBudget {
    /// Maximum live documents the engine may examine (index probe + scan).
    ///
    /// When unset, document count is not budget-capped (host result-byte
    /// ceilings still apply).
    pub max_docs_scanned: Option<usize>,
    /// Maximum payload bytes examined while scanning or probing (raw bodies).
    pub max_bytes_scanned: Option<u64>,
    /// Maximum approximate memory for materialised matches before sort/limit.
    ///
    /// Checked against the tighter of this value and the host
    /// [`crate::ResourceLimits::max_result_bytes`]. Spill-to-disk sort is not
    /// enabled in this profile — oversize sorts fail closed.
    pub max_result_bytes: Option<u64>,
}

impl QueryBudget {
    /// Budget allowing at most `n` documents to be examined.
    pub fn max_docs(n: usize) -> Self {
        Self {
            max_docs_scanned: Some(n),
            max_bytes_scanned: None,
            max_result_bytes: None,
        }
    }

    /// Cap payload bytes examined during scan/probe.
    pub fn max_bytes(mut self, n: u64) -> Self {
        self.max_bytes_scanned = Some(n);
        self
    }

    /// Cap approximate materialised result / sort memory.
    pub fn max_result_memory(mut self, n: u64) -> Self {
        self.max_result_bytes = Some(n);
        self
    }
}

/// Options for [`crate::Collection::find_with`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryOptions {
    /// Maximum number of matching rows to return. `None` means unbounded
    /// (caller is responsible for result size; host result-byte ceilings apply).
    pub limit: Option<usize>,
    /// Optional order-by field path (JSON path, dotted) and direction.
    ///
    /// When unset, results are in stable subject/key order (deterministic).
    pub order_by: Option<(String, SortOrder)>,
    /// Optional scan budget. When set and a scan would exceed it without a
    /// usable index, the query fails with [`crate::ErrorCode::QueryBudgetRequired`]
    /// unless [`Self::allow_partial_coverage`] is set.
    pub budget: Option<QueryBudget>,
    /// When true, force a full collection scan even if an index exists.
    pub force_scan: bool,
    /// When true, allow returning matches under incomplete cluster coverage
    /// (Stage 8e) **or** when an explicit budget stops the scan early (DEF-029).
    /// Default `false`: incomplete coverage / budget stop yields an error so
    /// partial results are never mistaken for a complete empty set.
    pub allow_partial_coverage: bool,
    /// Cooperative cancellation (not serialized in [`QueryPlan`]).
    pub cancel: Option<crate::resource::CancelToken>,
}

impl QueryOptions {
    /// Unbounded, key-ordered result.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cap the number of returned rows.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Order by a JSON field path.
    pub fn order_by(mut self, field: impl Into<String>, order: SortOrder) -> Self {
        self.order_by = Some((field.into(), order));
        self
    }

    /// Attach a scan budget (DX_SPEC §8.1).
    pub fn budget(mut self, budget: QueryBudget) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Force scan path (skip secondary indexes).
    pub fn force_scan(mut self) -> Self {
        self.force_scan = true;
        self
    }

    /// Allow partial cluster coverage or budget-truncated match lists.
    pub fn allow_partial_coverage(mut self) -> Self {
        self.allow_partial_coverage = true;
        self
    }

    /// Attach a cooperative cancellation token (DEF-029).
    pub fn cancel(mut self, token: crate::resource::CancelToken) -> Self {
        self.cancel = Some(token);
        self
    }
}

/// Field-level predicate (DX_SPEC §7.1 portable vocabulary).
#[derive(Debug, Clone, PartialEq)]
pub enum Pred {
    /// Exact equality (JSON value equality).
    Eq(JsonValue),
    /// Inequality.
    Ne(JsonValue),
    /// Strict less-than (numbers and strings).
    Lt(JsonValue),
    /// Less-or-equal.
    Lte(JsonValue),
    /// Strict greater-than.
    Gt(JsonValue),
    /// Greater-or-equal.
    Gte(JsonValue),
    /// Membership in a list of values.
    In(Vec<JsonValue>),
    /// Field presence (`true`) or absence (`false`). `Null` counts as present.
    Exists(bool),
    /// String value starts with this prefix.
    Prefix(String),
    /// String contains substring, or array contains element.
    Contains(JsonValue),
}

/// A filter expression over a JSON document.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// Matches every document.
    Always,
    /// Field path + predicate. Paths use `.` separators (`"address.city"`).
    Field {
        /// Dotted JSON path.
        path: String,
        /// Predicate applied to the value at `path`.
        pred: Pred,
    },
    /// Conjunction.
    And(Vec<Filter>),
    /// Disjunction.
    Or(Vec<Filter>),
    /// Negation.
    Not(Box<Filter>),
}

impl Filter {
    /// Match everything.
    pub fn always() -> Self {
        Self::Always
    }

    /// Start a field predicate builder: `Filter::field("status").eq("active")`.
    pub fn field(path: impl Into<String>) -> FieldBuilder {
        FieldBuilder { path: path.into() }
    }

    /// Conjunction of filters (empty ⇒ always true).
    pub fn and(parts: impl IntoIterator<Item = Filter>) -> Self {
        let v: Vec<_> = parts.into_iter().collect();
        if v.is_empty() {
            Self::Always
        } else if v.len() == 1 {
            v.into_iter().next().unwrap()
        } else {
            Self::And(v)
        }
    }

    /// Disjunction of filters (empty ⇒ never matches).
    pub fn or(parts: impl IntoIterator<Item = Filter>) -> Self {
        let v: Vec<_> = parts.into_iter().collect();
        if v.is_empty() {
            // Empty OR matches nothing.
            Self::Not(Box::new(Self::Always))
        } else if v.len() == 1 {
            v.into_iter().next().unwrap()
        } else {
            Self::Or(v)
        }
    }

    /// Negation.
    #[allow(clippy::should_implement_trait)] // constructor, not operator overload
    pub fn not(inner: Filter) -> Self {
        Self::Not(Box::new(inner))
    }

    /// Parse a Mongo-style / DX_SPEC object filter from JSON.
    ///
    /// Supported shapes:
    /// - `{ "status": "active" }` → equality
    /// - `{ "age": { "$gte": 18 } }` → comparison / `$in` / `$ne` / `$exists` /
    ///   `$prefix` / `$contains`
    /// - `{ "$and": [ ... ] }`, `{ "$or": [ ... ] }`, `{ "$not": { ... } }`
    ///
    /// Multiple top-level keys are combined with AND.
    pub fn from_json(value: &JsonValue) -> Result<Self, Error> {
        parse_filter(value)
    }

    /// Whether `doc` satisfies this filter (native evaluator).
    pub fn matches(&self, doc: &JsonValue) -> bool {
        match self {
            Self::Always => true,
            Self::Field { path, pred } => match resolve_path(doc, path) {
                PathHit::Missing => match pred {
                    Pred::Exists(want) => !*want,
                    Pred::Ne(_) => true, // missing ≠ any concrete value
                    _ => false,
                },
                PathHit::Present(v) => pred_matches(v, pred),
            },
            Self::And(parts) => parts.iter().all(|p| p.matches(doc)),
            Self::Or(parts) => parts.iter().any(|p| p.matches(doc)),
            Self::Not(inner) => !inner.matches(doc),
        }
    }

    /// Lower this portable filter into a total [`crate::predicate::Predicate`]
    /// for Application Core / Query VM (**RQL-DQ1**).
    ///
    /// Preserves DX `Filter` comfort semantics where they diverge from the
    /// Residiuum predicate profile (notably: missing `!=` value is true;
    /// `Ne` becomes `missing OR cmp ne`). Not a lossless SDA peer — use dialect
    /// `sda` when Null≠absence must be exact.
    pub fn to_predicate(&self) -> Result<crate::predicate::Predicate, Error> {
        use crate::predicate::{CompareOp, Operand, Path, Predicate};
        match self {
            Self::Always => Ok(Predicate::True),
            Self::Field { path, pred } => {
                let p = Path::parse_dotted(path)?;
                Ok(match pred {
                    Pred::Eq(v) => {
                        if v.is_null() {
                            Predicate::IsNull {
                                path: p,
                                negated: false,
                            }
                        } else {
                            Predicate::Cmp {
                                cmp: CompareOp::Eq,
                                left: Operand::path(p),
                                right: Operand::literal(v.clone()),
                            }
                        }
                    }
                    Pred::Ne(v) => {
                        // Filter: missing ≠ concrete value is true.
                        Predicate::Or {
                            args: vec![
                                Predicate::Missing { path: p.clone() },
                                Predicate::Cmp {
                                    cmp: CompareOp::Ne,
                                    left: Operand::path(p),
                                    right: Operand::literal(v.clone()),
                                },
                            ],
                        }
                    }
                    Pred::Lt(v) => Predicate::Cmp {
                        cmp: CompareOp::Lt,
                        left: Operand::path(p),
                        right: Operand::literal(v.clone()),
                    },
                    Pred::Lte(v) => Predicate::Cmp {
                        cmp: CompareOp::Lte,
                        left: Operand::path(p),
                        right: Operand::literal(v.clone()),
                    },
                    Pred::Gt(v) => Predicate::Cmp {
                        cmp: CompareOp::Gt,
                        left: Operand::path(p),
                        right: Operand::literal(v.clone()),
                    },
                    Pred::Gte(v) => Predicate::Cmp {
                        cmp: CompareOp::Gte,
                        left: Operand::path(p),
                        right: Operand::literal(v.clone()),
                    },
                    Pred::In(list) => Predicate::In {
                        left: Operand::path(p),
                        list: list.clone(),
                        negated: false,
                    },
                    Pred::Exists(true) => Predicate::Present { path: p },
                    Pred::Exists(false) => Predicate::Missing { path: p },
                    Pred::Prefix(s) => Predicate::StartsWith {
                        path: p,
                        prefix: s.clone(),
                    },
                    Pred::Contains(v) => Predicate::Contains {
                        path: p,
                        needle: v.clone(),
                    },
                })
            }
            Self::And(parts) => {
                let mut args = Vec::with_capacity(parts.len());
                for part in parts {
                    args.push(part.to_predicate()?);
                }
                Ok(if args.is_empty() {
                    Predicate::True
                } else if args.len() == 1 {
                    args.pop().unwrap()
                } else {
                    Predicate::And { args }
                })
            }
            Self::Or(parts) => {
                let mut args = Vec::with_capacity(parts.len());
                for part in parts {
                    args.push(part.to_predicate()?);
                }
                Ok(if args.is_empty() {
                    Predicate::False
                } else if args.len() == 1 {
                    args.pop().unwrap()
                } else {
                    Predicate::Or { args }
                })
            }
            Self::Not(inner) => Ok(Predicate::Not {
                arg: Box::new(inner.to_predicate()?),
            }),
        }
    }

    /// Compile this filter to a boolean SDA program over binding `input`.
    ///
    /// The program is pure standalone SDA plus filter host helpers `getPath`,
    /// `startsWith`, and `strContains` (DEF-028). Evaluation via
    /// [`Self::matches_sda`] MUST agree with [`Self::matches`] on the portable
    /// vocabulary. Product dialect execute prefers [`Self::to_predicate`] → QVM
    /// (**RQL-DQ1**); keep `to_sda` for raw SDA comfort / oracles.
    pub fn to_sda(&self) -> String {
        compile_filter_sda(self)
    }

    /// Compile this filter to a parse-once [`sda_core::Program`].
    ///
    /// Prefer this when evaluating the same filter against many documents
    /// ([`Self::matches_compiled_sda`]). [`Self::matches_sda`] recompiles
    /// every call for convenience.
    pub fn compile_sda(&self) -> Result<sda_core::Program, Error> {
        sda_core::Program::parse(&self.to_sda())
            .map_err(|e| Error::QueryInvalid(format!("filter SDA compile failed: {e}")))
    }

    /// Evaluate a pre-compiled filter program against `doc`.
    ///
    /// Non-boolean results (including SDA `Fail`) are treated as non-matches so
    /// comparison/type failures align with the native evaluator.
    pub fn matches_compiled_sda(
        program: &sda_core::Program,
        doc: &JsonValue,
    ) -> Result<bool, Error> {
        match program.run_json("input", doc.clone()) {
            Ok(JsonValue::Bool(b)) => Ok(b),
            Ok(_) => Ok(false),
            Err(e) => Err(Error::QueryInvalid(format!(
                "filter SDA evaluation failed: {e}"
            ))),
        }
    }

    /// Evaluate this filter by compiling to SDA and running `sda_core`.
    ///
    /// Compiles on every call. For multi-doc paths use [`Self::compile_sda`]
    /// once then [`Self::matches_compiled_sda`].
    ///
    /// Non-boolean results (including SDA `Fail`) are treated as non-matches so
    /// comparison/type failures align with the native evaluator.
    pub fn matches_sda(&self, doc: &JsonValue) -> Result<bool, Error> {
        let program = self.compile_sda()?;
        Self::matches_compiled_sda(&program, doc)
    }

    /// Build a versioned [`QueryPlan`] with default options.
    pub fn into_plan(self) -> QueryPlan {
        QueryPlan::new(self, QueryOptions::default())
    }

    /// Encode this filter as a DX/Mongo-style JSON object (round-trips via [`Self::from_json`]).
    ///
    /// Used for remote `find` RPC: the server parses with [`Self::from_json`].
    pub fn to_json(&self) -> JsonValue {
        match self {
            Self::Always => JsonValue::Object(serde_json::Map::new()),
            Self::Field { path, pred } => {
                let mut map = serde_json::Map::new();
                map.insert(path.clone(), pred_to_json(pred));
                JsonValue::Object(map)
            }
            Self::And(parts) => {
                if parts.is_empty() {
                    return JsonValue::Object(serde_json::Map::new());
                }
                if parts.len() == 1 {
                    return parts[0].to_json();
                }
                // Prefer a flat multi-key object when every part is a distinct field predicate.
                let mut flat = serde_json::Map::new();
                let mut can_flat = true;
                for p in parts {
                    match p {
                        Self::Field { path, pred } if !flat.contains_key(path) => {
                            flat.insert(path.clone(), pred_to_json(pred));
                        }
                        _ => {
                            can_flat = false;
                            break;
                        }
                    }
                }
                if can_flat {
                    JsonValue::Object(flat)
                } else {
                    JsonValue::Object(serde_json::Map::from_iter([(
                        "$and".into(),
                        JsonValue::Array(parts.iter().map(|p| p.to_json()).collect()),
                    )]))
                }
            }
            Self::Or(parts) => {
                if parts.is_empty() {
                    // Empty OR matches nothing ≡ Not(Always).
                    return Self::not(Self::Always).to_json();
                }
                if parts.len() == 1 {
                    return parts[0].to_json();
                }
                JsonValue::Object(serde_json::Map::from_iter([(
                    "$or".into(),
                    JsonValue::Array(parts.iter().map(|p| p.to_json()).collect()),
                )]))
            }
            Self::Not(inner) => JsonValue::Object(serde_json::Map::from_iter([(
                "$not".into(),
                inner.to_json(),
            )])),
        }
    }
}

fn pred_to_json(pred: &Pred) -> JsonValue {
    match pred {
        // Bare value → equality (matches `from_json` bare-value rule).
        Pred::Eq(v) => v.clone(),
        Pred::Ne(v) => json_op("$ne", v.clone()),
        Pred::Lt(v) => json_op("$lt", v.clone()),
        Pred::Lte(v) => json_op("$lte", v.clone()),
        Pred::Gt(v) => json_op("$gt", v.clone()),
        Pred::Gte(v) => json_op("$gte", v.clone()),
        Pred::In(list) => json_op("$in", JsonValue::Array(list.clone())),
        Pred::Exists(b) => json_op("$exists", JsonValue::Bool(*b)),
        Pred::Prefix(s) => json_op("$prefix", JsonValue::String(s.clone())),
        Pred::Contains(v) => json_op("$contains", v.clone()),
    }
}

fn json_op(op: &str, rhs: JsonValue) -> JsonValue {
    JsonValue::Object(serde_json::Map::from_iter([(op.into(), rhs)]))
}

/// Builder for a single field predicate.
#[derive(Debug, Clone)]
pub struct FieldBuilder {
    path: String,
}

impl FieldBuilder {
    /// Equality.
    pub fn eq(self, value: impl Into<JsonValue>) -> Filter {
        Filter::Field {
            path: self.path,
            pred: Pred::Eq(value.into()),
        }
    }

    /// Inequality.
    pub fn ne(self, value: impl Into<JsonValue>) -> Filter {
        Filter::Field {
            path: self.path,
            pred: Pred::Ne(value.into()),
        }
    }

    /// Less-than.
    pub fn lt(self, value: impl Into<JsonValue>) -> Filter {
        Filter::Field {
            path: self.path,
            pred: Pred::Lt(value.into()),
        }
    }

    /// Less-or-equal.
    pub fn lte(self, value: impl Into<JsonValue>) -> Filter {
        Filter::Field {
            path: self.path,
            pred: Pred::Lte(value.into()),
        }
    }

    /// Greater-than.
    pub fn gt(self, value: impl Into<JsonValue>) -> Filter {
        Filter::Field {
            path: self.path,
            pred: Pred::Gt(value.into()),
        }
    }

    /// Greater-or-equal.
    pub fn gte(self, value: impl Into<JsonValue>) -> Filter {
        Filter::Field {
            path: self.path,
            pred: Pred::Gte(value.into()),
        }
    }

    /// Membership.
    pub fn is_in<I, V>(self, values: I) -> Filter
    where
        I: IntoIterator<Item = V>,
        V: Into<JsonValue>,
    {
        Filter::Field {
            path: self.path,
            pred: Pred::In(values.into_iter().map(Into::into).collect()),
        }
    }

    /// Field must exist (`true`) or must be absent (`false`).
    pub fn exists(self, present: bool) -> Filter {
        Filter::Field {
            path: self.path,
            pred: Pred::Exists(present),
        }
    }

    /// String prefix match.
    pub fn prefix(self, prefix: impl Into<String>) -> Filter {
        Filter::Field {
            path: self.path,
            pred: Pred::Prefix(prefix.into()),
        }
    }

    /// String/array containment.
    pub fn contains(self, value: impl Into<JsonValue>) -> Filter {
        Filter::Field {
            path: self.path,
            pred: Pred::Contains(value.into()),
        }
    }
}

/// Versioned, serializable query plan (DX_SPEC §7.2 / DEF-028).
///
/// Embeds the portable filter object plus materialization options. Suitable for
/// embedded, remote, and cluster execution once the wire path carries the plan
/// profile tag [`QUERY_PLAN_PROFILE`].
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    /// Profile / schema version (`residiuum-query-plan-v1`).
    pub profile: String,
    /// Filter expression.
    pub filter: Filter,
    /// Result options (limit, order, budget, …).
    pub options: QueryOptions,
}

impl QueryPlan {
    /// Construct a plan under the current profile tag.
    pub fn new(filter: Filter, options: QueryOptions) -> Self {
        Self {
            profile: QUERY_PLAN_PROFILE.to_string(),
            filter,
            options,
        }
    }

    /// Encode as JSON (round-trips via [`Self::from_json`]).
    pub fn to_json(&self) -> JsonValue {
        let mut map = serde_json::Map::new();
        map.insert("profile".into(), JsonValue::String(self.profile.clone()));
        map.insert("filter".into(), self.filter.to_json());
        map.insert("options".into(), options_to_json(&self.options));
        JsonValue::Object(map)
    }

    /// Parse a plan object; rejects unknown or missing profile tags.
    pub fn from_json(value: &JsonValue) -> Result<Self, Error> {
        let obj = value
            .as_object()
            .ok_or_else(|| Error::QueryInvalid("query plan must be a JSON object".into()))?;
        let profile = obj
            .get("profile")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::QueryInvalid("query plan missing profile".into()))?;
        if profile != QUERY_PLAN_PROFILE {
            return Err(Error::QueryInvalid(format!(
                "unsupported query plan profile {profile:?}; expected {QUERY_PLAN_PROFILE:?}"
            )));
        }
        let filter = match obj.get("filter") {
            Some(f) => Filter::from_json(f)?,
            None => Filter::Always,
        };
        let options = match obj.get("options") {
            Some(o) => options_from_json(o)?,
            None => QueryOptions::default(),
        };
        Ok(Self {
            profile: profile.to_string(),
            filter,
            options,
        })
    }

    /// SDA program for the embedded filter.
    pub fn to_sda(&self) -> String {
        self.filter.to_sda()
    }
}

fn options_to_json(opts: &QueryOptions) -> JsonValue {
    let mut map = serde_json::Map::new();
    if let Some(n) = opts.limit {
        map.insert("limit".into(), JsonValue::from(n as u64));
    }
    if let Some((field, order)) = &opts.order_by {
        map.insert(
            "order_by".into(),
            JsonValue::Object(serde_json::Map::from_iter([
                ("field".into(), JsonValue::String(field.clone())),
                (
                    "order".into(),
                    JsonValue::String(
                        match order {
                            SortOrder::Asc => "asc",
                            SortOrder::Desc => "desc",
                        }
                        .into(),
                    ),
                ),
            ])),
        );
    }
    if let Some(budget) = &opts.budget {
        let mut b = serde_json::Map::new();
        if let Some(n) = budget.max_docs_scanned {
            b.insert("max_docs_scanned".into(), JsonValue::from(n as u64));
        }
        if let Some(n) = budget.max_bytes_scanned {
            b.insert("max_bytes_scanned".into(), JsonValue::from(n));
        }
        if let Some(n) = budget.max_result_bytes {
            b.insert("max_result_bytes".into(), JsonValue::from(n));
        }
        if !b.is_empty() {
            map.insert("budget".into(), JsonValue::Object(b));
        }
    }
    if opts.force_scan {
        map.insert("force_scan".into(), JsonValue::Bool(true));
    }
    if opts.allow_partial_coverage {
        map.insert("allow_partial_coverage".into(), JsonValue::Bool(true));
    }
    JsonValue::Object(map)
}

fn options_from_json(value: &JsonValue) -> Result<QueryOptions, Error> {
    let obj = value
        .as_object()
        .ok_or_else(|| Error::QueryInvalid("query plan options must be a JSON object".into()))?;
    let mut opts = QueryOptions::default();
    if let Some(n) = obj.get("limit") {
        let n = n
            .as_u64()
            .ok_or_else(|| Error::QueryInvalid("options.limit must be a number".into()))?
            as usize;
        opts.limit = Some(n);
    }
    if let Some(ob) = obj.get("order_by") {
        let ob = ob
            .as_object()
            .ok_or_else(|| Error::QueryInvalid("options.order_by must be an object".into()))?;
        let field = ob
            .get("field")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::QueryInvalid("options.order_by.field required".into()))?
            .to_string();
        let order = match ob.get("order").and_then(|v| v.as_str()).unwrap_or("asc") {
            "asc" => SortOrder::Asc,
            "desc" => SortOrder::Desc,
            other => {
                return Err(Error::QueryInvalid(format!(
                    "options.order_by.order unknown: {other:?}"
                )));
            }
        };
        opts.order_by = Some((field, order));
    }
    if let Some(b) = obj.get("budget") {
        let b = b
            .as_object()
            .ok_or_else(|| Error::QueryInvalid("options.budget must be an object".into()))?;
        let mut budget = QueryBudget::default();
        if let Some(n) = b.get("max_docs_scanned") {
            let n = n.as_u64().ok_or_else(|| {
                Error::QueryInvalid("budget.max_docs_scanned must be a number".into())
            })? as usize;
            budget.max_docs_scanned = Some(n);
        }
        if let Some(n) = b.get("max_bytes_scanned") {
            let n = n.as_u64().ok_or_else(|| {
                Error::QueryInvalid("budget.max_bytes_scanned must be a number".into())
            })?;
            budget.max_bytes_scanned = Some(n);
        }
        if let Some(n) = b.get("max_result_bytes") {
            let n = n.as_u64().ok_or_else(|| {
                Error::QueryInvalid("budget.max_result_bytes must be a number".into())
            })?;
            budget.max_result_bytes = Some(n);
        }
        if budget.max_docs_scanned.is_some()
            || budget.max_bytes_scanned.is_some()
            || budget.max_result_bytes.is_some()
        {
            opts.budget = Some(budget);
        }
    }
    if obj.get("force_scan") == Some(&JsonValue::Bool(true)) {
        opts.force_scan = true;
    }
    if obj.get("allow_partial_coverage") == Some(&JsonValue::Bool(true)) {
        opts.allow_partial_coverage = true;
    }
    Ok(opts)
}

/// Fluent query builder (DX_SPEC §7.2).
///
/// ```ignore
/// let rows = users
///     .query()
///     .where_eq("status", "active")
///     .where_gte("age", 18)
///     .order_by("age", SortOrder::Desc)
///     .limit(100)
///     .collect()?;
/// ```
#[cfg(feature = "legacy-flat-sdk")]
pub struct QueryBuilder<'c, 'a> {
    pub(crate) collection: &'c mut crate::collection::Collection<'a>,
    filters: Vec<Filter>,
    options: QueryOptions,
}

#[cfg(feature = "legacy-flat-sdk")]
impl<'c, 'a> QueryBuilder<'c, 'a> {
    pub(crate) fn new(collection: &'c mut crate::collection::Collection<'a>) -> Self {
        Self {
            collection,
            filters: Vec::new(),
            options: QueryOptions::default(),
        }
    }

    /// Add an arbitrary filter (AND-combined with previous clauses).
    pub fn filter(mut self, f: Filter) -> Self {
        self.filters.push(f);
        self
    }

    /// `field == value`.
    pub fn where_eq(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.filter(Filter::field(field).eq(value))
    }

    /// `field != value`.
    pub fn where_ne(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.filter(Filter::field(field).ne(value))
    }

    /// `field < value`.
    pub fn where_lt(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.filter(Filter::field(field).lt(value))
    }

    /// `field <= value`.
    pub fn where_lte(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.filter(Filter::field(field).lte(value))
    }

    /// `field > value`.
    pub fn where_gt(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.filter(Filter::field(field).gt(value))
    }

    /// `field >= value`.
    pub fn where_gte(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.filter(Filter::field(field).gte(value))
    }

    /// `field` in `values`.
    pub fn where_in<I, V>(self, field: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<JsonValue>,
    {
        self.filter(Filter::field(field).is_in(values))
    }

    /// Field existence.
    pub fn where_exists(self, field: impl Into<String>, present: bool) -> Self {
        self.filter(Filter::field(field).exists(present))
    }

    /// String prefix.
    pub fn where_prefix(self, field: impl Into<String>, prefix: impl Into<String>) -> Self {
        self.filter(Filter::field(field).prefix(prefix))
    }

    /// Cap result size.
    pub fn limit(mut self, n: usize) -> Self {
        self.options.limit = Some(n);
        self
    }

    /// Order by field path.
    pub fn order_by(mut self, field: impl Into<String>, order: SortOrder) -> Self {
        self.options.order_by = Some((field.into(), order));
        self
    }

    /// Attach a scan / materialisation budget (DEF-029).
    pub fn budget(mut self, budget: QueryBudget) -> Self {
        self.options.budget = Some(budget);
        self
    }

    /// Cooperative cancellation token.
    pub fn cancel(mut self, token: crate::resource::CancelToken) -> Self {
        self.options.cancel = Some(token);
        self
    }

    /// Execute and materialize matching rows.
    pub fn collect(self) -> Result<Vec<(String, JsonValue)>, Error> {
        let filter = Filter::and(self.filters);
        self.collection.find_with(&filter, self.options)
    }

    /// Build a serializable [`QueryPlan`] without executing.
    pub fn plan(self) -> QueryPlan {
        QueryPlan::new(Filter::and(self.filters), self.options)
    }
}

// --- SDA compilation (DEF-028) ------------------------------------------------

fn compile_filter_sda(filter: &Filter) -> String {
    compile_filter_expr(filter)
}

fn compile_filter_expr(filter: &Filter) -> String {
    match filter {
        Filter::Always => "true".to_string(),
        Filter::Field { path, pred } => compile_field_pred(path, pred),
        Filter::And(parts) => {
            if parts.is_empty() {
                return "true".to_string();
            }
            let inner = parts
                .iter()
                .map(|p| format!("({})", compile_filter_expr(p)))
                .collect::<Vec<_>>()
                .join(" and ");
            inner
        }
        Filter::Or(parts) => {
            if parts.is_empty() {
                return "false".to_string();
            }
            let inner = parts
                .iter()
                .map(|p| format!("({})", compile_filter_expr(p)))
                .collect::<Vec<_>>()
                .join(" or ");
            inner
        }
        Filter::Not(inner) => format!("not ({})", compile_filter_expr(inner)),
    }
}

fn path_expr(path: &str) -> String {
    let segs: Vec<String> = if path.is_empty() {
        Vec::new()
    } else {
        path.split('.').map(sda_string_literal).collect()
    };
    format!("getPath(input, Seq[{}])", segs.join(", "))
}

fn compile_field_pred(path: &str, pred: &Pred) -> String {
    let gp = path_expr(path);
    match pred {
        Pred::Eq(v) => format!("{gp} = Some({})", json_to_sda_literal(v)),
        Pred::Ne(v) => format!("{gp} != Some({})", json_to_sda_literal(v)),
        Pred::Lt(v) => format!(
            "mapOpt({gp}, x => x < {}) = Some(true)",
            json_to_sda_literal(v)
        ),
        Pred::Lte(v) => format!(
            "mapOpt({gp}, x => x <= {}) = Some(true)",
            json_to_sda_literal(v)
        ),
        Pred::Gt(v) => format!(
            "mapOpt({gp}, x => x > {}) = Some(true)",
            json_to_sda_literal(v)
        ),
        Pred::Gte(v) => format!(
            "mapOpt({gp}, x => x >= {}) = Some(true)",
            json_to_sda_literal(v)
        ),
        Pred::In(list) => {
            let items = list
                .iter()
                .map(json_to_sda_literal)
                .collect::<Vec<_>>()
                .join(", ");
            format!("mapOpt({gp}, x => x in Seq[{items}]) = Some(true)")
        }
        Pred::Exists(true) => format!("{gp} != None"),
        Pred::Exists(false) => format!("{gp} = None"),
        Pred::Prefix(p) => format!(
            "mapOpt({gp}, s => startsWith(s, {})) = Some(true)",
            sda_string_literal(p)
        ),
        Pred::Contains(needle) => {
            // Native: string⊃substring OR array∋element.
            // SDA and/or evaluate both sides (no short-circuit), so each branch
            // must be a separate mapOpt that cannot Fail on the other type.
            let lit = json_to_sda_literal(needle);
            if needle.is_string() {
                format!(
                    "((mapOpt({gp}, v => typeOf(v) = \"str\") = Some(true) and mapOpt({gp}, v => strContains(v, {lit})) = Some(true)) or (mapOpt({gp}, v => typeOf(v) = \"seq\") = Some(true) and mapOpt({gp}, v => {lit} in v) = Some(true)))"
                )
            } else {
                format!("mapOpt({gp}, v => {lit} in v) = Some(true)")
            }
        }
    }
}

fn sda_string_literal(s: &str) -> String {
    // SDA strings use JSON-style escapes.
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn json_to_sda_literal(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => sda_string_literal(s),
        JsonValue::Array(items) => {
            let inner = items
                .iter()
                .map(json_to_sda_literal)
                .collect::<Vec<_>>()
                .join(", ");
            format!("Seq[{inner}]")
        }
        JsonValue::Object(map) => {
            let inner = map
                .iter()
                .map(|(k, val)| {
                    format!("{} -> {}", sda_string_literal(k), json_to_sda_literal(val))
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("Map{{{inner}}}")
        }
    }
}

// --- matching internals -------------------------------------------------------

enum PathHit<'a> {
    Missing,
    Present(&'a JsonValue),
}

/// Resolve a dotted JSON path to a present value (for index key extraction).
pub(crate) fn resolve_path_value<'a>(doc: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    match resolve_path(doc, path) {
        PathHit::Present(v) => Some(v),
        PathHit::Missing => None,
    }
}

fn resolve_path<'a>(doc: &'a JsonValue, path: &str) -> PathHit<'a> {
    if path.is_empty() {
        return PathHit::Present(doc);
    }
    let mut cur = doc;
    for part in path.split('.') {
        match cur {
            JsonValue::Object(map) => match map.get(part) {
                Some(next) => cur = next,
                None => return PathHit::Missing,
            },
            _ => return PathHit::Missing,
        }
    }
    PathHit::Present(cur)
}

fn pred_matches(value: &JsonValue, pred: &Pred) -> bool {
    match pred {
        Pred::Eq(rhs) => value == rhs,
        Pred::Ne(rhs) => value != rhs,
        Pred::Lt(rhs) => cmp_ord(value, rhs) == Some(std::cmp::Ordering::Less),
        Pred::Lte(rhs) => matches!(
            cmp_ord(value, rhs),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
        Pred::Gt(rhs) => cmp_ord(value, rhs) == Some(std::cmp::Ordering::Greater),
        Pred::Gte(rhs) => matches!(
            cmp_ord(value, rhs),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
        Pred::In(list) => list.iter().any(|v| v == value),
        Pred::Exists(want) => *want, // present ⇒ exists true; false would need Missing branch
        Pred::Prefix(p) => match value {
            JsonValue::String(s) => s.starts_with(p.as_str()),
            _ => false,
        },
        Pred::Contains(needle) => match (value, needle) {
            (JsonValue::String(hay), JsonValue::String(n)) => hay.contains(n.as_str()),
            (JsonValue::Array(arr), n) => arr.iter().any(|e| e == n),
            _ => false,
        },
    }
}

/// Compare numbers and strings only; mixed/other types yield `None` (predicate fails).
fn cmp_ord(a: &JsonValue, b: &JsonValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (JsonValue::Number(x), JsonValue::Number(y)) => {
            let xf = x.as_f64()?;
            let yf = y.as_f64()?;
            xf.partial_cmp(&yf)
        }
        (JsonValue::String(x), JsonValue::String(y)) => Some(x.cmp(y)),
        (JsonValue::Bool(x), JsonValue::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn parse_filter(value: &JsonValue) -> Result<Filter, Error> {
    match value {
        JsonValue::Object(map) if map.is_empty() => Ok(Filter::Always),
        JsonValue::Object(map) => {
            // Single logical combinator at top level.
            if map.len() == 1 {
                if let Some(arr) = map.get("$and") {
                    return parse_filter_list(arr, true);
                }
                if let Some(arr) = map.get("$or") {
                    return parse_filter_list(arr, false);
                }
                if let Some(inner) = map.get("$not") {
                    return Ok(Filter::not(parse_filter(inner)?));
                }
            }
            let mut parts = Vec::with_capacity(map.len());
            for (key, val) in map {
                if key.starts_with('$') {
                    // Combinators mixed with fields: only allow as sole key (handled above).
                    return Err(Error::QueryInvalid(format!(
                        "unexpected operator key {key:?} among field predicates"
                    )));
                }
                parts.push(parse_field_pred(key, val)?);
            }
            Ok(Filter::and(parts))
        }
        _ => Err(Error::QueryInvalid("filter must be a JSON object".into())),
    }
}

fn parse_filter_list(value: &JsonValue, is_and: bool) -> Result<Filter, Error> {
    let arr = value.as_array().ok_or_else(|| {
        Error::QueryInvalid(if is_and {
            "$and expects an array".into()
        } else {
            "$or expects an array".into()
        })
    })?;
    let mut parts = Vec::with_capacity(arr.len());
    for item in arr {
        parts.push(parse_filter(item)?);
    }
    Ok(if is_and {
        Filter::and(parts)
    } else {
        Filter::or(parts)
    })
}

fn parse_field_pred(path: &str, value: &JsonValue) -> Result<Filter, Error> {
    // Bare value → equality.
    if !value.is_object() {
        return Ok(Filter::field(path).eq(value.clone()));
    }
    let obj = value.as_object().unwrap();
    if obj.is_empty() {
        return Err(Error::QueryInvalid(format!(
            "empty operator object for field {path:?}"
        )));
    }
    // Multiple operators on one field → AND.
    let mut parts = Vec::with_capacity(obj.len());
    for (op, rhs) in obj {
        let pred = match op.as_str() {
            "$eq" => Pred::Eq(rhs.clone()),
            "$ne" => Pred::Ne(rhs.clone()),
            "$lt" => Pred::Lt(rhs.clone()),
            "$lte" => Pred::Lte(rhs.clone()),
            "$gt" => Pred::Gt(rhs.clone()),
            "$gte" => Pred::Gte(rhs.clone()),
            "$in" => {
                let arr = rhs
                    .as_array()
                    .ok_or_else(|| Error::QueryInvalid("$in expects an array".into()))?;
                Pred::In(arr.clone())
            }
            "$exists" => {
                let b = rhs
                    .as_bool()
                    .ok_or_else(|| Error::QueryInvalid("$exists expects a boolean".into()))?;
                Pred::Exists(b)
            }
            "$prefix" => {
                let s = rhs
                    .as_str()
                    .ok_or_else(|| Error::QueryInvalid("$prefix expects a string".into()))?;
                Pred::Prefix(s.to_string())
            }
            "$contains" => Pred::Contains(rhs.clone()),
            other => {
                return Err(Error::QueryInvalid(format!(
                    "unknown filter operator {other:?}"
                )));
            }
        };
        parts.push(Filter::Field {
            path: path.to_string(),
            pred,
        });
    }
    Ok(Filter::and(parts))
}

/// Compare two documents for order-by: missing fields sort first in Asc.
pub(crate) fn compare_field(
    a: &JsonValue,
    b: &JsonValue,
    path: &str,
    order: SortOrder,
) -> std::cmp::Ordering {
    let av = resolve_path(a, path);
    let bv = resolve_path(b, path);
    let base = match (av, bv) {
        (PathHit::Missing, PathHit::Missing) => std::cmp::Ordering::Equal,
        (PathHit::Missing, PathHit::Present(_)) => std::cmp::Ordering::Less,
        (PathHit::Present(_), PathHit::Missing) => std::cmp::Ordering::Greater,
        (PathHit::Present(x), PathHit::Present(y)) => cmp_ord(x, y).unwrap_or_else(|| {
            // Incomparable types: fall back to stringified JSON for stability.
            let xs = x.to_string();
            let ys = y.to_string();
            xs.cmp(&ys)
        }),
    };
    match order {
        SortOrder::Asc => base,
        SortOrder::Desc => base.reverse(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn eq_and_gte() {
        let f = Filter::and([
            Filter::field("status").eq("active"),
            Filter::field("age").gte(18),
        ]);
        assert!(f.matches(&json!({"status": "active", "age": 21})));
        assert!(!f.matches(&json!({"status": "active", "age": 17})));
        assert!(!f.matches(&json!({"status": "paused", "age": 30})));
    }

    #[test]
    fn from_json_object_filter() {
        let f = Filter::from_json(&json!({
            "status": "active",
            "age": { "$gte": 18 },
            "country": { "$in": ["TH", "SG"] }
        }))
        .unwrap();
        assert!(f.matches(&json!({"status":"active","age":20,"country":"TH"})));
        assert!(!f.matches(&json!({"status":"active","age":20,"country":"US"})));
    }

    #[test]
    fn missing_field_and_exists() {
        let doc = json!({"a": 1});
        assert!(!Filter::field("b").eq(1).matches(&doc));
        assert!(Filter::field("b").exists(false).matches(&doc));
        assert!(Filter::field("a").exists(true).matches(&doc));
        assert!(Filter::field("b").ne(1).matches(&doc));
    }

    #[test]
    fn prefix_and_contains() {
        let doc = json!({"name": "Alice", "tags": ["x", "y"]});
        assert!(Filter::field("name").prefix("Al").matches(&doc));
        assert!(Filter::field("name").contains("ice").matches(&doc));
        assert!(Filter::field("tags").contains("y").matches(&doc));
    }

    #[test]
    fn dotted_path() {
        let doc = json!({"address": {"city": "Bangkok"}});
        assert!(Filter::field("address.city").eq("Bangkok").matches(&doc));
        assert!(!Filter::field("address.city").eq("Singapore").matches(&doc));
    }

    #[test]
    fn to_json_roundtrip() {
        let f = Filter::and([
            Filter::field("status").eq("active"),
            Filter::field("age").gte(18),
        ]);
        let j = f.to_json();
        let back = Filter::from_json(&j).unwrap();
        let doc = json!({"status": "active", "age": 21});
        assert!(back.matches(&doc));
        assert!(!back.matches(&json!({"status": "active", "age": 10})));
    }

    #[test]
    fn matches_sda_agrees_on_core_vocab() {
        let cases: Vec<(Filter, JsonValue, bool)> = vec![
            (
                Filter::field("status").eq("active"),
                json!({"status": "active"}),
                true,
            ),
            (
                Filter::field("status").eq("active"),
                json!({"status": "paused"}),
                false,
            ),
            (
                Filter::field("n").eq(JsonValue::Null),
                json!({"n": null}),
                true,
            ),
            (Filter::field("n").eq(JsonValue::Null), json!({}), false),
            (Filter::field("missing").ne(1), json!({}), true),
            (Filter::field("age").gte(18), json!({"age": 21}), true),
            (Filter::field("age").gte(18), json!({"age": 10}), false),
            (Filter::field("age").gte(18), json!({}), false),
            (Filter::field("age").gte(18), json!({"age": null}), false),
            (Filter::field("a").exists(true), json!({"a": null}), true),
            (Filter::field("a").exists(false), json!({}), true),
            (Filter::field("a").exists(false), json!({"a": null}), false),
            (
                Filter::field("name").prefix("Al"),
                json!({"name": "Alice"}),
                true,
            ),
            (
                Filter::field("tags").contains("y"),
                json!({"tags": ["x", "y"]}),
                true,
            ),
            (
                Filter::field("name").contains("ice"),
                json!({"name": "Alice"}),
                true,
            ),
            (
                Filter::field("address.city").eq("Bangkok"),
                json!({"address": {"city": "Bangkok"}}),
                true,
            ),
            (
                // Intermediate non-object → absence
                Filter::field("a.city").exists(false),
                json!({"a": "x"}),
                true,
            ),
            (
                Filter::and([
                    Filter::field("status").eq("active"),
                    Filter::field("age").gte(18),
                ]),
                json!({"status": "active", "age": 21}),
                true,
            ),
            (
                Filter::or([
                    Filter::field("status").eq("active"),
                    Filter::field("status").eq("trial"),
                ]),
                json!({"status": "trial"}),
                true,
            ),
            (
                Filter::not(Filter::field("status").eq("active")),
                json!({"status": "paused"}),
                true,
            ),
            (
                Filter::field("c").is_in(["TH", "SG"]),
                json!({"c": "TH"}),
                true,
            ),
        ];
        for (i, (filter, doc, want)) in cases.iter().enumerate() {
            let native = filter.matches(doc);
            let sda = filter
                .matches_sda(doc)
                .unwrap_or_else(|e| panic!("case {i} SDA error: {e}; program={}", filter.to_sda()));
            assert_eq!(
                native,
                *want,
                "case {i} native mismatch; program={}",
                filter.to_sda()
            );
            assert_eq!(
                sda,
                *want,
                "case {i} sda mismatch; program={}",
                filter.to_sda()
            );
            assert_eq!(
                native,
                sda,
                "case {i} native≠sda; program={}",
                filter.to_sda()
            );
        }
    }

    #[test]
    fn compile_sda_once_agrees_with_matches_sda() {
        let filter = Filter::and([
            Filter::field("status").eq("active"),
            Filter::field("age").gte(18),
        ]);
        let prog = filter.compile_sda().expect("compile_sda");
        let docs = [
            json!({"status": "active", "age": 21}),
            json!({"status": "active", "age": 10}),
            json!({"status": "paused", "age": 30}),
        ];
        for doc in &docs {
            let once = Filter::matches_compiled_sda(&prog, doc).unwrap();
            let convenience = filter.matches_sda(doc).unwrap();
            let native = filter.matches(doc);
            assert_eq!(once, convenience);
            assert_eq!(once, native);
        }
    }

    #[test]
    fn query_plan_roundtrip() {
        let plan = QueryPlan::new(
            Filter::field("status").eq("active"),
            QueryOptions::new()
                .limit(10)
                .order_by("age", SortOrder::Desc),
        );
        let j = plan.to_json();
        assert_eq!(j["profile"], QUERY_PLAN_PROFILE);
        let back = QueryPlan::from_json(&j).unwrap();
        assert_eq!(back.profile, QUERY_PLAN_PROFILE);
        assert_eq!(back.options.limit, Some(10));
        assert!(back.filter.matches(&json!({"status": "active"})));
        assert_eq!(back.to_sda(), plan.filter.to_sda());
    }
}
