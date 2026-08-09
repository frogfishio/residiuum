//! RQL Application Core compiler (`rql-app-core-v1`) — APP-5 first cut.
//!
//! Normative:
//! - CORE plan §9 / §14 APP-5
//! - [RQL_SPEC.md](../../../doc/wip/query/RQL_SPEC.md) (subset; no alternative syntax)
//!
//! Compiles Application Core source text into the same [`RqlPlanV1`] as
//! [`PlanBuilder`]. Full RQL v1 is **not** claimed; unsupported constructs
//! fail with [`Error::QueryInvalid`] and diagnostic `rql_feature_unavailable`
//! when appropriate.
//!
//! Package APP-5 exit (scoreboard accept): §9 Application Core compile + corpus +
//! bounded fuzz. Residual for later packages: `after` continuation (APP-6),
//! ranked access (excluded Core). Budget source clause is parsed into
//! [`CompiledAppCore::budget`] for merge with [`crate::app_v1::QueryRunOptions`]
//! at execution (not part of plan hash). No product query claim until APB-7.

use crate::app_v1::{
    ConsistencyMode, Continuation, CoveragePolicy, Parameters, QueryBudget, QueryRunOptions,
};
use crate::error::Error;
use crate::plan_v1::{
    CollectionBindings, NullsOrder, OrderDir, PlanBuilder, RqlPlanV1, DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
};
use crate::predicate::{param, CompareOp, Operand, Path, Predicate};
use serde_json::{Number, Value as JsonValue};

/// Conformance profile id (same as [`crate::app_v1::RQL_APP_CORE_PROFILE`]).
pub const APP_CORE_PROFILE: &str = "rql-app-core-v1";

/// Hard ceiling on UTF-8 RQL source (CORE plan §9.2).
pub const MAX_RQL_SOURCE_BYTES: usize = 1_048_576;

/// Diagnostic tag for unsupported Application Core features (error map).
pub const DIAG_RQL_FEATURE_UNAVAILABLE: &str = "rql_feature_unavailable";

/// Stable refusal for offset/skip discard pagination.
///
/// Application queries must use authenticated cursor continuation (`after`);
/// silently accepting offset would make work and consistency proportional to
/// discarded rows.
pub const DIAG_RQL_OFFSET_DISCARD_UNSUPPORTED: &str = "rql_offset_discard_unsupported";

/// Result of compiling Application Core source.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledAppCore {
    /// Validated logical plan (hash identity; **no** budget fields).
    pub plan: RqlPlanV1,
    /// True when source began with `explain`.
    pub explain: bool,
    /// Optional budgets from `budget { … }` (run options, not plan hash).
    pub budget: Option<QueryBudget>,
    /// Textual continuation selector. This is execution metadata and is
    /// deliberately excluded from the canonical logical plan/hash.
    pub after: Option<TextualAfter>,
    /// Profile string for advertising.
    pub profile: &'static str,
}

/// Source-level `after` value. The opaque token is resolved only at execute
/// time so query identity never depends on a particular page cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextualAfter {
    /// `after $name`.
    Parameter(String),
    /// `after "opaque-token"`.
    Literal(Vec<u8>),
}

/// Compile Application Core RQL into [`RqlPlanV1`] (+ explain/budget run metadata).
///
/// `bindings` must resolve the `from` source name to an immutable collection id.
///
/// Budgets are **not** part of the canonical plan hash: they travel with
/// [`crate::app_v1::QueryRunOptions`] at execution. Source `budget { … }` and
/// run-option budgets should be merged by the host (stricter wins).
pub fn compile_app_core(
    source: &str,
    bindings: &CollectionBindings,
) -> Result<CompiledAppCore, Error> {
    // UTF-8 validity is guaranteed for `&str`; reject oversized source first.
    if source.len() > MAX_RQL_SOURCE_BYTES {
        return Err(Error::QueryInvalid(format!(
            "RQL source exceeds {MAX_RQL_SOURCE_BYTES} bytes"
        )));
    }
    reject_excluded_features(source)?;

    let mut p = Parser::new(source);
    p.skip_ws();
    let explain = if p.eat_keyword("explain") {
        p.skip_ws();
        true
    } else {
        false
    };

    p.expect_keyword("from")?;
    p.skip_ws();
    let source_name = p.parse_ident_or_string()?;
    p.skip_ws();
    if p.eat_keyword("as") {
        p.skip_ws();
        let _alias = p.parse_ident_or_string()?;
        p.skip_ws();
        // Alias is readability-only for CollectionClient::rql; plan binds the
        // handle's collection via `source_name` resolution below.
    }

    let mut builder = PlanBuilder::from_source(source_name);
    // CORE §9 EBNF allows repeated `where` clauses — they AND together.
    let mut where_parts: Vec<Predicate> = Vec::new();
    let mut budget: Option<QueryBudget> = None;
    // Terminal clauses appear at most once (RQL_SPEC §5).
    let mut saw_project = false;
    let mut saw_group = false;
    let mut saw_order = false;
    let mut saw_limit = false;
    let mut saw_page = false;
    let mut saw_coverage = false;
    let mut saw_consistency = false;
    let mut saw_budget = false;
    let mut after: Option<TextualAfter> = None;
    let mut group_keys: Vec<crate::predicate::Path> = Vec::new();

    loop {
        p.skip_ws();
        if p.is_eof() {
            break;
        }
        if p.eat_keyword("where") {
            p.skip_ws();
            where_parts.push(p.parse_or()?);
            continue;
        }
        // `group by path, path` (Q2 pkg_group_aggregate) — before project.
        if p.eat_keyword("group") {
            if saw_group {
                return Err(Error::QueryInvalid("duplicate group clause".into()));
            }
            if saw_project {
                return Err(Error::QueryInvalid(
                    "group by must appear before project".into(),
                ));
            }
            saw_group = true;
            p.skip_ws();
            p.expect_keyword("by")?;
            p.skip_ws();
            loop {
                let path = p.parse_path_dotted()?;
                group_keys.push(crate::predicate::Path::parse_dotted(&path)?);
                p.skip_ws();
                if p.eat_char(',') {
                    p.skip_ws();
                    continue;
                }
                break;
            }
            continue;
        }
        if p.eat_keyword("project") {
            if saw_project {
                return Err(Error::QueryInvalid("duplicate project clause".into()));
            }
            saw_project = true;
            p.skip_ws();
            let clause = p.parse_project_clause()?;
            if clause.aggregates.is_empty() && !saw_group {
                // Flat path project (existing Application Core).
                builder = builder.project(clause.paths_dotted)?;
            } else {
                // Group / aggregate projection — paths omitted (group row is authority).
                use crate::plan_v1::{AggregateSpec, GroupAggSpec};
                let mut aggregates = Vec::new();
                for a in clause.aggregates {
                    aggregates.push(AggregateSpec {
                        fun: a.fun,
                        source: a
                            .source
                            .map(|s| crate::predicate::Path::parse_dotted(&s))
                            .transpose()?,
                        output: a.output,
                    });
                }
                builder = builder.group_agg(GroupAggSpec {
                    group_by: group_keys.clone(),
                    aggregates,
                })?;
                // Optional identity path list for explain; leave project None so
                // group rows pass through ProjectPaths as full documents.
            }
            continue;
        }
        if p.eat_keyword("order") {
            if saw_order {
                return Err(Error::QueryInvalid("duplicate order clause".into()));
            }
            saw_order = true;
            p.skip_ws();
            p.expect_keyword("by")?;
            p.skip_ws();
            loop {
                let path = p.parse_path_dotted()?;
                p.skip_ws();
                let dir = if p.eat_keyword("desc") {
                    OrderDir::Desc
                } else {
                    let _ = p.eat_keyword("asc");
                    OrderDir::Asc
                };
                p.skip_ws();
                let nulls = if p.eat_keyword("nulls") {
                    p.skip_ws();
                    if p.eat_keyword("first") {
                        NullsOrder::First
                    } else if p.eat_keyword("last") {
                        NullsOrder::Last
                    } else {
                        return Err(Error::QueryInvalid(
                            "order by … nulls requires first|last".into(),
                        ));
                    }
                } else {
                    NullsOrder::Last
                };
                p.skip_ws();
                builder = builder.order_by_nulls(&path, dir, nulls)?;
                p.skip_ws();
                if p.eat_char(',') {
                    p.skip_ws();
                    continue;
                }
                break;
            }
            continue;
        }
        if p.eat_keyword("limit") {
            if saw_limit {
                return Err(Error::QueryInvalid("duplicate limit clause".into()));
            }
            saw_limit = true;
            p.skip_ws();
            let n = p.parse_u64()?;
            builder = builder.limit(n);
            continue;
        }
        if p.eat_keyword("page") {
            if saw_page {
                return Err(Error::QueryInvalid("duplicate page clause".into()));
            }
            saw_page = true;
            p.skip_ws();
            p.expect_keyword("size")?;
            p.skip_ws();
            let n = p.parse_u64()?;
            if n == 0 || n > u64::from(MAX_PAGE_SIZE) {
                return Err(Error::QueryInvalid(format!(
                    "page size must be 1..={MAX_PAGE_SIZE}"
                )));
            }
            builder = builder.page_size(n as u32)?;
            continue;
        }
        if p.eat_keyword("coverage") {
            if saw_coverage {
                return Err(Error::QueryInvalid("duplicate coverage clause".into()));
            }
            saw_coverage = true;
            p.skip_ws();
            if p.eat_keyword("complete") {
                builder = builder.coverage(CoveragePolicy::Complete);
            } else if p.eat_keyword("incomplete") || p.eat_keyword("allow_incomplete") {
                // "incomplete allowed" style — accept incomplete / incomplete_allowed tokens
                let _ = p.eat_keyword("allowed");
                builder = builder.coverage(CoveragePolicy::IncompleteAllowed);
            } else {
                return Err(Error::QueryInvalid(
                    "coverage expects complete|incomplete".into(),
                ));
            }
            continue;
        }
        if p.eat_keyword("consistency") {
            if saw_consistency {
                return Err(Error::QueryInvalid("duplicate consistency clause".into()));
            }
            saw_consistency = true;
            p.skip_ws();
            if p.eat_keyword("available") {
                builder = builder.consistency(ConsistencyMode::Available);
            } else if p.eat_keyword("current") {
                builder = builder.consistency(ConsistencyMode::Current);
            } else {
                return Err(Error::QueryInvalid(
                    "consistency expects available|current".into(),
                ));
            }
            continue;
        }
        if p.eat_keyword("budget") {
            if saw_budget {
                return Err(Error::QueryInvalid("duplicate budget clause".into()));
            }
            saw_budget = true;
            p.skip_ws();
            budget = Some(p.parse_budget_clause()?);
            continue;
        }
        if p.eat_keyword("after") {
            if after.is_some() {
                return Err(Error::QueryInvalid("duplicate after clause".into()));
            }
            p.skip_ws();
            if p.eat_char('$') {
                let name = p.parse_ident_or_string()?;
                if name.starts_with('$') {
                    return Err(Error::QueryInvalid(
                        "after parameter has duplicate `$`".into(),
                    ));
                }
                after = Some(TextualAfter::Parameter(name));
            } else if p.peek() == Some('"') {
                after = Some(TextualAfter::Literal(p.parse_string()?.into_bytes()));
            } else {
                return Err(Error::QueryInvalid(
                    "after expects an opaque string or `$parameter`".into(),
                ));
            }
            continue;
        }
        if p.eat_keyword("offset") || p.eat_keyword("skip") {
            return Err(Error::QueryInvalid(format!(
                "{DIAG_RQL_OFFSET_DISCARD_UNSUPPORTED}: offset/skip discard is unsupported; use `after`"
            )));
        }
        return Err(Error::QueryInvalid(format!(
            "unexpected token near `{}`",
            p.snippet()
        )));
    }

    // Default page size when omitted (CORE §9.2) — PlanBuilder fills DEFAULT_PAGE_SIZE.
    let _ = DEFAULT_PAGE_SIZE;

    if !where_parts.is_empty() {
        let combined = if where_parts.len() == 1 {
            where_parts.pop().expect("len==1")
        } else {
            Predicate::And { args: where_parts }
        };
        builder = builder.where_(combined);
    }

    // `group by` without aggregate project still activates group mode.
    if saw_group && !group_keys.is_empty() {
        // If project already installed group_agg via aggregates, keys were set
        // there. If only keys (or no project), install keys-only group_agg.
        // Re-installing is safe: project branch already cloned group_keys.
        if !saw_project {
            use crate::plan_v1::GroupAggSpec;
            builder = builder.group_agg(GroupAggSpec {
                group_by: group_keys,
                aggregates: Vec::new(),
            })?;
        }
    }

    let plan = builder.compile(bindings)?;
    Ok(CompiledAppCore {
        plan,
        explain,
        budget,
        after,
        profile: APP_CORE_PROFILE,
    })
}

/// Parse one standalone RQL predicate using the same parser as root `where`.
pub(crate) fn parse_predicate_expression(source: &str) -> Result<Predicate, Error> {
    let mut p = Parser::new(source);
    p.skip_ws();
    let pred = p.parse_or()?;
    p.skip_ws();
    if !p.is_eof() {
        return Err(Error::QueryInvalid(format!(
            "unexpected predicate token near `{}`",
            p.snippet()
        )));
    }
    Ok(pred)
}

/// Resolve source-level continuation metadata into run options and return the
/// semantic parameter set used for predicate hashing/evaluation. The cursor
/// binding itself is removed because it identifies a page, not query meaning.
pub(crate) fn bind_textual_after(
    after: Option<&TextualAfter>,
    parameters: &Parameters,
    options: &mut QueryRunOptions,
) -> Result<Parameters, Error> {
    let Some(after) = after else {
        return Ok(parameters.clone());
    };
    if options.after.is_some() {
        return Err(Error::QueryInvalid(
            "continuation supplied both in source and QueryRunOptions".into(),
        ));
    }
    let mut semantic = parameters.clone();
    let token = match after {
        TextualAfter::Literal(bytes) => bytes.clone(),
        TextualAfter::Parameter(name) => {
            let value = semantic.values.remove(name).ok_or_else(|| {
                Error::QueryInvalid(format!("unbound continuation parameter `${name}`"))
            })?;
            match value {
                JsonValue::String(s) => s.into_bytes(),
                JsonValue::Array(values) => values
                    .into_iter()
                    .map(|v| {
                        v.as_u64()
                            .filter(|n| *n <= 255)
                            .map(|n| n as u8)
                            .ok_or_else(|| {
                                Error::QueryInvalid(format!(
                                    "continuation `${name}` byte array contains a non-byte"
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                _ => {
                    return Err(Error::QueryInvalid(format!(
                        "continuation `${name}` must be a string or byte array"
                    )))
                }
            }
        }
    };
    if token.is_empty() {
        return Err(Error::QueryInvalid("continuation token is empty".into()));
    }
    options.after = Some(Continuation { token });
    Ok(semantic)
}

/// Merge source-clause budget with run-option budget (stricter wins per field).
///
/// `None` means unlimited for that field; the other side fills it. When both
/// set a field, the minimum is kept.
pub fn merge_budgets(source: Option<QueryBudget>, run: Option<QueryBudget>) -> Option<QueryBudget> {
    match (source, run) {
        (None, None) => None,
        (Some(a), None) | (None, Some(a)) => Some(a),
        (Some(a), Some(b)) => Some(QueryBudget {
            max_documents: min_opt(a.max_documents, b.max_documents),
            max_bytes: min_opt(a.max_bytes, b.max_bytes),
            max_result_bytes: min_opt(a.max_result_bytes, b.max_result_bytes),
        }),
    }
}

fn min_opt(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (None, None) => None,
        (Some(x), None) | (None, Some(x)) => Some(x),
        (Some(x), Some(y)) => Some(x.min(y)),
    }
}

fn reject_excluded_features(source: &str) -> Result<(), Error> {
    let lower = source.to_ascii_lowercase();
    // Word-ish checks to avoid rejecting identifiers that merely contain letters.
    let banned = [
        (" enrich ", "enrich"),
        ("\nenrich ", "enrich"),
        (" within ", "within"),
        (" at rank", "at rank"),
        (" sequential", "sequential access policy"),
        (" direct ", "direct access policy"),
        (" build ", "build access policy"),
    ];
    let padded = format!(" {} ", lower.replace('\t', " "));
    for (needle, label) in banned {
        if padded.contains(needle) || lower.starts_with(needle.trim()) {
            return Err(Error::QueryInvalid(format!(
                "{DIAG_RQL_FEATURE_UNAVAILABLE}: `{label}` is outside rql-app-core-v1"
            )));
        }
    }
    // bare "enrich" as first token after explain
    if lower
        .split_whitespace()
        .any(|t| t == "enrich" || t == "within")
    {
        return Err(Error::QueryInvalid(format!(
            "{DIAG_RQL_FEATURE_UNAVAILABLE}: enrich/within outside rql-app-core-v1"
        )));
    }
    Ok(())
}

// --- lexer/parser ------------------------------------------------------------

/// Flat project list parse result (paths + optional aggregates).
struct ParsedProjectClause {
    paths_dotted: Vec<String>,
    aggregates: Vec<ParsedAggregate>,
}

struct ParsedAggregate {
    fun: crate::plan_v1::AggFn,
    source: Option<String>,
    output: String,
}

struct Parser<'a> {
    s: &'a str,
    i: usize,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Self {
        Self { s, i: 0 }
    }

    fn is_eof(&self) -> bool {
        self.i >= self.s.len()
    }

    fn rest(&self) -> &'a str {
        // Fuzz-hardening: never panic if `i` lands mid-codepoint.
        let mut i = self.i.min(self.s.len());
        while i > 0 && i < self.s.len() && !self.s.is_char_boundary(i) {
            i -= 1;
        }
        &self.s[i..]
    }

    fn snippet(&self) -> String {
        let r = self.rest();
        r.chars().take(24).collect()
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let r = self.rest();
        let mut it = r.char_indices();
        let (_, c) = it.next()?;
        let next = it.next().map(|(o, _)| o).unwrap_or(r.len());
        // Align self.i to current rest base then advance by char width.
        let base = self.s.len() - r.len();
        self.i = base + next;
        Some(c)
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.bump();
            } else if c == '-' && self.rest().starts_with("--") {
                // line comment
                while let Some(c) = self.bump() {
                    if c == '\n' {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    fn eat_char(&mut self, ch: char) -> bool {
        if self.peek() == Some(ch) {
            self.bump();
            true
        } else {
            false
        }
    }

    /// Consume an ASCII multi-char operator (`!=`, `<=`, …) without mid-char slices.
    fn eat_ascii_op(&mut self, op: &str) -> bool {
        debug_assert!(op.is_ascii());
        let r = self.rest();
        if r.len() >= op.len() && r.is_char_boundary(op.len()) && r.starts_with(op) {
            let base = self.s.len() - r.len();
            self.i = base + op.len();
            true
        } else {
            false
        }
    }

    fn eat_keyword(&mut self, kw: &str) -> bool {
        // Keywords are ASCII; refuse to slice through multi-byte codepoints.
        let r = self.rest();
        if r.len() < kw.len() || !r.is_char_boundary(kw.len()) {
            return false;
        }
        if r[..kw.len()].eq_ignore_ascii_case(kw)
            && r[kw.len()..]
                .chars()
                .next()
                .map(|c| !c.is_ascii_alphanumeric() && c != '_')
                .unwrap_or(true)
        {
            let base = self.s.len() - r.len();
            self.i = base + kw.len();
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<(), Error> {
        if self.eat_keyword(kw) {
            Ok(())
        } else {
            Err(Error::QueryInvalid(format!(
                "expected `{kw}` near `{}`",
                self.snippet()
            )))
        }
    }

    fn parse_ident_or_string(&mut self) -> Result<String, Error> {
        self.skip_ws();
        if self.peek() == Some('"') {
            return self.parse_string();
        }
        let start = self.i;
        let mut first = true;
        while let Some(c) = self.peek() {
            if first {
                if !(c.is_ascii_alphabetic() || c == '_' || c == '$') {
                    break;
                }
                first = false;
                self.bump();
            } else if c.is_ascii_alphanumeric() || c == '_' || c == '$' {
                self.bump();
            } else {
                break;
            }
        }
        if self.i == start {
            return Err(Error::QueryInvalid(format!(
                "expected identifier near `{}`",
                self.snippet()
            )));
        }
        Ok(self.s[start..self.i].to_string())
    }

    fn parse_string(&mut self) -> Result<String, Error> {
        if !self.eat_char('"') {
            return Err(Error::QueryInvalid("expected string".into()));
        }
        let mut out = String::new();
        while let Some(c) = self.bump() {
            match c {
                '"' => return Ok(out),
                '\\' => match self.bump() {
                    Some('"') => out.push('"'),
                    Some('\\') => out.push('\\'),
                    Some('n') => out.push('\n'),
                    Some('t') => out.push('\t'),
                    Some(other) => out.push(other),
                    None => return Err(Error::QueryInvalid("unterminated string escape".into())),
                },
                other => out.push(other),
            }
        }
        Err(Error::QueryInvalid("unterminated string".into()))
    }

    fn parse_path_dotted(&mut self) -> Result<String, Error> {
        // paths like status, customer.country, $key
        let mut parts = vec![self.parse_ident_or_string()?];
        while self.eat_char('.') {
            parts.push(self.parse_ident_or_string()?);
        }
        Ok(parts.join("."))
    }

    fn parse_u64(&mut self) -> Result<u64, Error> {
        self.skip_ws();
        let start = self.i;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.bump();
        }
        if self.i == start {
            return Err(Error::QueryInvalid(format!(
                "expected unsigned integer near `{}`",
                self.snippet()
            )));
        }
        self.s[start..self.i]
            .parse()
            .map_err(|_| Error::QueryInvalid("integer overflow".into()))
    }

    /// `budget { documents: N, bytes: N, result_bytes: N }` (RQL_SPEC §5).
    fn parse_budget_clause(&mut self) -> Result<QueryBudget, Error> {
        self.skip_ws();
        self.expect_char('{')?;
        let mut max_documents = None;
        let mut max_bytes = None;
        let mut max_result_bytes = None;
        self.skip_ws();
        if self.eat_char('}') {
            // Empty budget {} is legal (literal zeros/unlimited).
            return Ok(QueryBudget {
                max_documents: None,
                max_bytes: None,
                max_result_bytes: None,
            });
        }
        loop {
            self.skip_ws();
            let key = if self.eat_keyword("documents") {
                "documents"
            } else if self.eat_keyword("bytes") {
                "bytes"
            } else if self.eat_keyword("result_bytes") {
                "result_bytes"
            } else {
                return Err(Error::QueryInvalid(format!(
                    "budget entry expects documents|bytes|result_bytes near `{}`",
                    self.snippet()
                )));
            };
            self.skip_ws();
            self.expect_char(':')?;
            self.skip_ws();
            let n = self.parse_u64()?;
            match key {
                "documents" => {
                    if max_documents.is_some() {
                        return Err(Error::QueryInvalid(
                            "duplicate budget key `documents`".into(),
                        ));
                    }
                    max_documents = Some(n);
                }
                "bytes" => {
                    if max_bytes.is_some() {
                        return Err(Error::QueryInvalid("duplicate budget key `bytes`".into()));
                    }
                    max_bytes = Some(n);
                }
                "result_bytes" => {
                    if max_result_bytes.is_some() {
                        return Err(Error::QueryInvalid(
                            "duplicate budget key `result_bytes`".into(),
                        ));
                    }
                    max_result_bytes = Some(n);
                }
                _ => unreachable!(),
            }
            self.skip_ws();
            let _ = self.eat_char(','); // optional trailing/separator comma
            self.skip_ws();
            if self.eat_char('}') {
                break;
            }
            if self.is_eof() {
                return Err(Error::QueryInvalid("unterminated budget clause".into()));
            }
        }
        Ok(QueryBudget {
            max_documents,
            max_bytes,
            max_result_bytes,
        })
    }

    fn parse_project_list(&mut self) -> Result<Vec<String>, Error> {
        Ok(self.parse_project_clause()?.paths_dotted)
    }

    /// Flat `project` list: paths and/or aggregates (`count() as n`, `sum(x) as t`).
    fn parse_project_clause(&mut self) -> Result<ParsedProjectClause, Error> {
        // project a, b, c   or project [a, b]   or project region, count() as n
        let mut paths_dotted = Vec::new();
        let mut aggregates = Vec::new();
        let bracket = self.eat_char('[');
        self.skip_ws();
        if self.is_eof() || (bracket && self.peek() == Some(']')) {
            if bracket {
                let _ = self.eat_char(']');
            }
            return Ok(ParsedProjectClause {
                paths_dotted,
                aggregates,
            });
        }
        loop {
            self.skip_ws();
            // Aggregate call: count() | sum(path) | min(path) | max(path) | avg(path)
            if let Some(agg) = self.try_parse_aggregate_item()? {
                aggregates.push(agg);
            } else {
                paths_dotted.push(self.parse_path_dotted()?);
            }
            self.skip_ws();
            if self.eat_char(',') {
                self.skip_ws();
                continue;
            }
            break;
        }
        if bracket {
            self.skip_ws();
            if !self.eat_char(']') {
                return Err(Error::QueryInvalid(
                    "expected `]` after project list".into(),
                ));
            }
        }
        Ok(ParsedProjectClause {
            paths_dotted,
            aggregates,
        })
    }

    /// Try to parse `count() as out` or `sum(path) as out` (and min/max/avg).
    fn try_parse_aggregate_item(&mut self) -> Result<Option<ParsedAggregate>, Error> {
        let save = self.i;
        let fun_name = if self.eat_keyword("count") {
            "count"
        } else if self.eat_keyword("sum") {
            "sum"
        } else if self.eat_keyword("min") {
            "min"
        } else if self.eat_keyword("max") {
            "max"
        } else if self.eat_keyword("avg") {
            "avg"
        } else {
            return Ok(None);
        };
        self.skip_ws();
        if !self.eat_char('(') {
            // Not a call — rewind (e.g. field named `count`).
            self.i = save;
            return Ok(None);
        }
        self.skip_ws();
        let source = if fun_name == "count" {
            if !self.eat_char(')') {
                return Err(Error::QueryInvalid(
                    "count() takes no arguments in this slice".into(),
                ));
            }
            None
        } else {
            let path = self.parse_path_dotted()?;
            self.skip_ws();
            if !self.eat_char(')') {
                return Err(Error::QueryInvalid(format!(
                    "expected `)` after {fun_name}(path)"
                )));
            }
            Some(path)
        };
        self.skip_ws();
        if !self.eat_keyword("as") {
            return Err(Error::QueryInvalid(format!(
                "{fun_name}() requires `as <alias>`"
            )));
        }
        self.skip_ws();
        let output = self.parse_ident_or_string()?;
        let fun = crate::plan_v1::AggFn::parse(fun_name)?;
        Ok(Some(ParsedAggregate {
            fun,
            source,
            output,
        }))
    }

    // predicate: or / and / not / cmp
    fn parse_or(&mut self) -> Result<Predicate, Error> {
        let mut left = self.parse_and()?;
        loop {
            self.skip_ws();
            if self.eat_keyword("or") {
                self.skip_ws();
                let right = self.parse_and()?;
                left = Predicate::Or {
                    args: vec![left, right],
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Predicate, Error> {
        let mut left = self.parse_not()?;
        loop {
            self.skip_ws();
            if self.eat_keyword("and") {
                self.skip_ws();
                let right = self.parse_not()?;
                left = Predicate::And {
                    args: vec![left, right],
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Predicate, Error> {
        self.skip_ws();
        if self.eat_keyword("not") {
            self.skip_ws();
            return Ok(Predicate::Not {
                arg: Box::new(self.parse_not()?),
            });
        }
        self.parse_primary_pred()
    }

    fn parse_primary_pred(&mut self) -> Result<Predicate, Error> {
        self.skip_ws();
        if self.eat_char('(') {
            let inner = self.parse_or()?;
            self.skip_ws();
            if !self.eat_char(')') {
                return Err(Error::QueryInvalid("expected `)`".into()));
            }
            return Ok(inner);
        }
        if self.eat_keyword("true") {
            return Ok(Predicate::True);
        }
        if self.eat_keyword("false") {
            return Ok(Predicate::False);
        }
        if self.eat_keyword("present") {
            self.skip_ws();
            self.expect_char('(')?;
            self.skip_ws();
            let path = Path::parse_dotted(&self.parse_path_dotted()?)?;
            self.skip_ws();
            self.expect_char(')')?;
            return Ok(Predicate::Present { path });
        }
        if self.eat_keyword("missing") {
            self.skip_ws();
            self.expect_char('(')?;
            self.skip_ws();
            let path = Path::parse_dotted(&self.parse_path_dotted()?)?;
            self.skip_ws();
            self.expect_char(')')?;
            return Ok(Predicate::Missing { path });
        }
        if self.eat_keyword("starts_with") {
            self.skip_ws();
            self.expect_char('(')?;
            self.skip_ws();
            let path = Path::parse_dotted(&self.parse_path_dotted()?)?;
            self.skip_ws();
            self.expect_char(',')?;
            self.skip_ws();
            let prefix = self.parse_string()?;
            self.skip_ws();
            self.expect_char(')')?;
            return Ok(Predicate::StartsWith { path, prefix });
        }
        if self.eat_keyword("contains") {
            self.skip_ws();
            self.expect_char('(')?;
            self.skip_ws();
            let path = Path::parse_dotted(&self.parse_path_dotted()?)?;
            self.skip_ws();
            self.expect_char(',')?;
            self.skip_ws();
            let needle = self.parse_literal_value()?;
            self.skip_ws();
            self.expect_char(')')?;
            return Ok(Predicate::Contains { path, needle });
        }

        // path op operand | path [not] in [...] | path is [not] null
        let path_s = self.parse_path_dotted()?;
        let path = Path::parse_dotted(&path_s)?;
        self.skip_ws();

        if self.eat_keyword("is") {
            self.skip_ws();
            let neg = self.eat_keyword("not");
            self.skip_ws();
            self.expect_keyword("null")?;
            return Ok(Predicate::IsNull { path, negated: neg });
        }

        // Infix bag/string contains: `tags contains "vip"` ≡ `contains(tags, "vip")`.
        if self.eat_keyword("contains") {
            self.skip_ws();
            let needle = self.parse_literal_value()?;
            return Ok(Predicate::Contains { path, needle });
        }

        // `not in` / `in`
        let not_in = self.eat_keyword("not");
        self.skip_ws();
        if self.eat_keyword("in") {
            self.skip_ws();
            let list = self.parse_literal_list()?;
            return Ok(Predicate::In {
                left: Operand::path(path),
                list,
                negated: not_in,
            });
        }
        if not_in {
            return Err(Error::QueryInvalid(format!(
                "expected `in` after `not` near path `{path_s}`"
            )));
        }

        let cmp = if self.eat_ascii_op("!=") || self.eat_ascii_op("<>") {
            CompareOp::Ne
        } else if self.eat_char('=') {
            CompareOp::Eq
        } else if self.eat_ascii_op("<=") {
            CompareOp::Lte
        } else if self.eat_ascii_op(">=") {
            CompareOp::Gte
        } else if self.eat_char('<') {
            CompareOp::Lt
        } else if self.eat_char('>') {
            CompareOp::Gt
        } else {
            return Err(Error::QueryInvalid(format!(
                "expected comparison after path `{path_s}`"
            )));
        };
        self.skip_ws();
        let right = self.parse_operand()?;
        Ok(Predicate::Cmp {
            cmp,
            left: Operand::path(path),
            right,
        })
    }

    fn parse_literal_list(&mut self) -> Result<Vec<JsonValue>, Error> {
        self.skip_ws();
        self.expect_char('[')?;
        let mut list = Vec::new();
        self.skip_ws();
        if self.eat_char(']') {
            return Ok(list);
        }
        loop {
            list.push(self.parse_literal_value()?);
            self.skip_ws();
            if self.eat_char(',') {
                self.skip_ws();
                continue;
            }
            break;
        }
        self.skip_ws();
        self.expect_char(']')?;
        Ok(list)
    }

    /// Parse a JSON-ish literal (string, number, bool, null, array) — not a path or param.
    fn parse_literal_value(&mut self) -> Result<JsonValue, Error> {
        self.skip_ws();
        if self.peek() == Some('"') {
            return Ok(JsonValue::String(self.parse_string()?));
        }
        if self.peek() == Some('[') {
            // Array literal (including empty `[]`) — same bracket form as `in` lists.
            return Ok(JsonValue::Array(self.parse_literal_list()?));
        }
        if self.eat_keyword("true") {
            return Ok(JsonValue::Bool(true));
        }
        if self.eat_keyword("false") {
            return Ok(JsonValue::Bool(false));
        }
        if self.eat_keyword("null") {
            return Ok(JsonValue::Null);
        }
        let start = self.i;
        if self.eat_char('-') {
            // keep
        }
        let dig_start = self.i;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.bump();
        }
        if self.i == dig_start {
            self.i = start;
            return Err(Error::QueryInvalid(format!(
                "expected literal near `{}`",
                self.snippet()
            )));
        }
        // optional fractional
        if self.peek() == Some('.') {
            self.bump();
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.bump();
            }
        }
        let num = &self.s[start..self.i];
        if let Ok(i) = num.parse::<i64>() {
            return Ok(JsonValue::Number(i.into()));
        }
        if let Ok(f) = num.parse::<f64>() {
            if let Some(n) = Number::from_f64(f) {
                return Ok(JsonValue::Number(n));
            }
        }
        Err(Error::QueryInvalid(format!("bad number `{num}`")))
    }

    fn expect_char(&mut self, ch: char) -> Result<(), Error> {
        if self.eat_char(ch) {
            Ok(())
        } else {
            Err(Error::QueryInvalid(format!(
                "expected `{ch}` near `{}`",
                self.snippet()
            )))
        }
    }

    fn parse_operand(&mut self) -> Result<Operand, Error> {
        self.skip_ws();
        if self.peek() == Some('$') {
            self.bump();
            let name = self.parse_ident_or_string()?;
            return Ok(param(name));
        }
        if self.peek() == Some('"') {
            return Ok(Operand::literal(JsonValue::String(self.parse_string()?)));
        }
        if self.peek() == Some('[') {
            return Ok(Operand::literal(JsonValue::Array(
                self.parse_literal_list()?,
            )));
        }
        if self.eat_keyword("true") {
            return Ok(Operand::literal(JsonValue::Bool(true)));
        }
        if self.eat_keyword("false") {
            return Ok(Operand::literal(JsonValue::Bool(false)));
        }
        if self.eat_keyword("null") {
            return Ok(Operand::literal(JsonValue::Null));
        }
        // number
        let start = self.i;
        if self.eat_char('-') {
            // keep
        }
        let dig_start = self.i;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.bump();
        }
        if self.i == dig_start && self.s.as_bytes().get(start) == Some(&b'-') {
            // only minus
            self.i = start;
        } else if self.i > dig_start {
            let num = &self.s[start..self.i];
            if let Ok(i) = num.parse::<i64>() {
                return Ok(Operand::literal(JsonValue::Number(i.into())));
            }
            if let Ok(f) = num.parse::<f64>() {
                if let Some(n) = Number::from_f64(f) {
                    return Ok(Operand::literal(JsonValue::Number(n)));
                }
            }
            return Err(Error::QueryInvalid(format!("bad number `{num}`")));
        }
        // bare path as right-hand? treat as path operand
        let path_s = self.parse_path_dotted()?;
        Ok(Operand::path(Path::parse_dotted(&path_s)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_v1::{ConsistencyMode, CoveragePolicy};
    use crate::predicate::{field, param as pred_param, CompareOp, Operand, Path, Predicate};
    use residiuum_heap::CollectionId;
    use std::str::FromStr;

    fn bindings() -> CollectionBindings {
        let mut b = CollectionBindings::default();
        b.bind(
            "orders",
            CollectionId::from_str("00000000-0000-4000-8000-0000000000aa").unwrap(),
        );
        b
    }

    #[test]
    fn compile_simple_where_eq() {
        let c = compile_app_core(
            r#"from orders where status = "paid" limit 10 page size 5"#,
            &bindings(),
        )
        .unwrap();
        assert!(!c.explain);
        assert_eq!(c.profile, APP_CORE_PROFILE);
        assert_eq!(c.plan.limit, Some(10));
        assert_eq!(c.plan.page_size, 5);
        assert_eq!(c.plan.from.source_name, "orders");
    }

    #[test]
    fn reject_enrich() {
        let err = compile_app_core("from orders enrich other", &bindings()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(DIAG_RQL_FEATURE_UNAVAILABLE), "{msg}");
    }

    #[test]
    fn explain_flag() {
        let c = compile_app_core("explain from orders", &bindings()).unwrap();
        assert!(c.explain);
    }

    #[test]
    fn builder_and_rql_same_hash() {
        use crate::plan_v1::PlanBuilder;
        let b = bindings();
        let from_builder = PlanBuilder::from_source("orders")
            .where_(field("status").unwrap().eq(pred_param("st")))
            .limit(100)
            .page_size(10)
            .unwrap()
            .compile(&b)
            .unwrap();
        let from_rql = compile_app_core(
            r#"from orders where status = $st limit 100 page size 10"#,
            &b,
        )
        .unwrap()
        .plan;
        assert_eq!(from_builder.plan_hash_hex(), from_rql.plan_hash_hex());
    }

    #[test]
    fn multi_clause_hash_matches_builder() {
        use crate::plan_v1::{NullsOrder, OrderDir, PlanBuilder};
        let b = bindings();
        let from_builder = PlanBuilder::from_source("orders")
            .where_(field("status").unwrap().eq(pred_param("st")))
            .project(["id", "status"])
            .unwrap()
            .order_by_nulls("created_at", OrderDir::Desc, NullsOrder::First)
            .unwrap()
            .limit(1000)
            .page_size(100)
            .unwrap()
            .coverage(CoveragePolicy::IncompleteAllowed)
            .consistency(ConsistencyMode::Current)
            .compile(&b)
            .unwrap();
        let src = r#"
            from orders
            where status = $st
            project id, status
            order by created_at desc nulls first
            limit 1000
            page size 100
            coverage incomplete
            consistency current
        "#;
        let from_rql = compile_app_core(src, &b).unwrap().plan;
        assert_eq!(from_builder.plan_hash_hex(), from_rql.plan_hash_hex());
        assert_eq!(from_rql.order[0].nulls, NullsOrder::First);
        assert!(matches!(
            from_rql.coverage,
            CoveragePolicy::IncompleteAllowed
        ));
        assert!(matches!(from_rql.consistency, ConsistencyMode::Current));
    }

    #[test]
    fn repeated_where_and_and_or_not() {
        let b = bindings();
        let c = compile_app_core(
            r#"from orders where status = "paid" where not missing(customer.id) or present(tags)"#,
            &b,
        )
        .unwrap();
        // Two where clauses → And of (eq, Or(Not(Missing), Present))
        match &c.plan.where_pred {
            Predicate::And { args } => {
                assert_eq!(args.len(), 2);
                assert!(matches!(
                    &args[0],
                    Predicate::Cmp {
                        cmp: CompareOp::Eq,
                        ..
                    }
                ));
                assert!(matches!(&args[1], Predicate::Or { .. }));
            }
            other => panic!("expected And of two where clauses, got {other:?}"),
        }
    }

    #[test]
    fn present_missing_is_null_in_starts_contains() {
        let b = bindings();
        let c = compile_app_core(
            r#"from orders where present(status) and missing(gone) and tags is not null and region in ["us", "eu"] and starts_with(name, "Acme") and contains(notes, "rush")"#,
            &b,
        )
        .unwrap();
        // Flatten not required — just ensure it compiles and is And tree
        assert!(!matches!(c.plan.where_pred, Predicate::True));
        let c2 = compile_app_core(
            r#"from orders where status is null and code not in [1, 2, 3]"#,
            &b,
        )
        .unwrap();
        match &c2.plan.where_pred {
            Predicate::And { args } => {
                assert!(matches!(&args[0], Predicate::IsNull { negated: false, .. }));
                assert!(matches!(&args[1], Predicate::In { negated: true, .. }));
            }
            other => panic!("expected And, got {other:?}"),
        }
        // starts_with / contains forms
        let c3 = compile_app_core(
            r#"from orders where starts_with(sku, "AB") and contains(desc, "x")"#,
            &b,
        )
        .unwrap();
        match &c3.plan.where_pred {
            Predicate::And { args } => {
                assert!(matches!(&args[0], Predicate::StartsWith { prefix, .. } if prefix == "AB"));
                assert!(matches!(&args[1], Predicate::Contains { .. }));
            }
            other => panic!("expected And, got {other:?}"),
        }
        let _ = Path::parse_dotted("status").unwrap();
        let _ = Operand::path(Path::parse_dotted("x").unwrap());
    }

    #[test]
    fn array_literal_eq_ne_and_infix_contains() {
        let b = bindings();
        let empty = compile_app_core(r#"from orders where tags = []"#, &b).unwrap();
        match &empty.plan.where_pred {
            Predicate::Cmp {
                cmp: CompareOp::Eq,
                right: Operand::Literal { value },
                ..
            } => assert_eq!(value, &JsonValue::Array(vec![])),
            other => panic!("expected eq empty array, got {other:?}"),
        }
        let nonempty = compile_app_core(r#"from orders where attachments != []"#, &b).unwrap();
        assert!(matches!(
            &nonempty.plan.where_pred,
            Predicate::Cmp {
                cmp: CompareOp::Ne,
                right: Operand::Literal {
                    value: JsonValue::Array(items)
                },
                ..
            } if items.is_empty()
        ));
        let infix = compile_app_core(r#"from orders where tags contains "vip""#, &b).unwrap();
        match &infix.plan.where_pred {
            Predicate::Contains { needle, .. } => {
                assert_eq!(needle, &JsonValue::String("vip".into()));
            }
            other => panic!("expected Contains, got {other:?}"),
        }
        let fn_form = compile_app_core(r#"from orders where contains(tags, "vip")"#, &b).unwrap();
        assert_eq!(
            infix.plan.plan_hash_hex(),
            fn_form.plan.plan_hash_hex(),
            "infix and function contains must normalize to same plan"
        );
        let nested = compile_app_core(r#"from orders where tags = ["a", "b"]"#, &b).unwrap();
        match &nested.plan.where_pred {
            Predicate::Cmp {
                right:
                    Operand::Literal {
                        value: JsonValue::Array(items),
                    },
                ..
            } => assert_eq!(items.len(), 2),
            other => panic!("expected array lit cmp, got {other:?}"),
        }
    }

    #[test]
    fn comment_and_as_alias() {
        let b = bindings();
        let c = compile_app_core(
            r#"
            -- line comment
            from orders as o
            where status = "open"
            "#,
            &b,
        )
        .unwrap();
        assert_eq!(c.plan.from.source_name, "orders");
    }

    #[test]
    fn budget_clause_parsed_not_in_plan_hash() {
        let b = bindings();
        let without = compile_app_core("from orders", &b).unwrap();
        let with = compile_app_core(
            "from orders budget { documents: 10, bytes: 20, result_bytes: 30 }",
            &b,
        )
        .unwrap();
        assert!(without.budget.is_none());
        let bud = with.budget.expect("budget");
        assert_eq!(bud.max_documents, Some(10));
        assert_eq!(bud.max_bytes, Some(20));
        assert_eq!(bud.max_result_bytes, Some(30));
        // Budget is run metadata — plan hash unchanged by budget clause alone.
        assert_eq!(without.plan.plan_hash_hex(), with.plan.plan_hash_hex());
    }

    #[test]
    fn merge_budgets_stricter_wins() {
        let a = QueryBudget {
            max_documents: Some(100),
            max_bytes: Some(1000),
            max_result_bytes: None,
        };
        let b = QueryBudget {
            max_documents: Some(50),
            max_bytes: None,
            max_result_bytes: Some(9),
        };
        let m = merge_budgets(Some(a), Some(b)).unwrap();
        assert_eq!(m.max_documents, Some(50));
        assert_eq!(m.max_bytes, Some(1000));
        assert_eq!(m.max_result_bytes, Some(9));
    }

    #[test]
    fn after_compiles_as_non_plan_execution_metadata() {
        let plain = compile_app_core("from orders", &bindings()).unwrap();
        let resumed = compile_app_core("from orders after $c", &bindings()).unwrap();
        assert_eq!(resumed.after, Some(TextualAfter::Parameter("c".into())));
        assert_eq!(plain.plan.plan_hash(), resumed.plan.plan_hash());
    }

    #[test]
    fn offset_and_skip_have_stable_cursor_refusal() {
        for source in [
            "from orders order by created_at desc offset 8 limit 8",
            "from orders skip 5 limit 5",
        ] {
            let err = compile_app_core(source, &bindings()).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains(DIAG_RQL_OFFSET_DISCARD_UNSUPPORTED), "{msg}");
            assert!(msg.contains("use `after`"), "{msg}");
        }
    }

    #[test]
    fn source_oversize_rejected() {
        let huge = format!("from orders -- {}\n", "x".repeat(MAX_RQL_SOURCE_BYTES));
        assert!(huge.len() > MAX_RQL_SOURCE_BYTES);
        let err = compile_app_core(&huge, &bindings()).unwrap_err();
        assert!(err.to_string().contains("exceeds"));
    }

    /// Bounded deterministic fuzz: compiler must not panic on garbage/truncated.
    #[test]
    fn bounded_fuzz_panic_free() {
        let b = bindings();
        let samples: &[&str] = &[
            "",
            " ",
            "from",
            "from ",
            "from orders where",
            "from orders where (",
            "from orders where status =",
            "from orders project",
            "from orders order by",
            "from orders limit",
            "from orders page size 0",
            "from orders page size 99999",
            "from orders budget {",
            "from orders budget { documents: }",
            "from orders budget { documents: 1, documents: 2 }",
            "from orders coverage",
            "from orders consistency",
            "from orders \0 null byte",
            "from orders where status = \"unterminated",
            "from orders where (a = 1 and",
            "from orders where not",
            "from orders where region in [",
            "from orders where region in [1,]",
            "SELECT 1",
            ";;;;",
            "from orders where status = $",
            "from orders as",
            "explain",
            "explain from",
            "from \"",
            "from orders where starts_with(",
            "from orders where starts_with(name)",
            "from orders where contains(name, )",
            "from orders where status is",
            "from orders where status is not",
            "from orders where not in [1]",
            "from orders project [a, b",
            "from orders order by created_at nulls",
            "from orders after",
            "from orders enrich x",
            "from orders within x",
            "from orders at rank 1",
            "from orders sequential access",
            "from orders direct access",
            "from orders build access",
            "from orders where status = 1.2.3",
            "from orders where status = --",
            "from orders where ((((((((((",
            "from orders where ))))))))))",
            "from orders limit 999999999999999999999",
            "from orders where status = true and and false",
            "from orders where or true",
            "from orders where present",
            "from orders where missing()",
            "from orders budget { unknown: 1 }",
            "from orders page size 1 page size 2",
            "from orders limit 1 limit 2",
            "from orders project a project b",
        ];
        for (i, s) in samples.iter().enumerate() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = compile_app_core(s, &b);
            }))
            .unwrap_or_else(|_| panic!("fuzz sample {i} panicked: {s:?}"));
        }

        // Pseudo-random short mutations of a valid seed (deterministic LCG).
        let seed = b"from orders where status = \"paid\" limit 10 page size 5";
        let mut state: u64 = 0xC0FFEE_u64;
        for i in 0..256u64 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let mut buf = seed.to_vec();
            let n = (state as usize % 8) + 1;
            for _ in 0..n {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let idx = (state as usize) % buf.len().max(1);
                let byte = (state >> 32) as u8;
                if idx < buf.len() {
                    buf[idx] = byte;
                } else {
                    buf.push(byte);
                }
            }
            // Lossy UTF-8: skip invalid sequences by from_utf8_lossy
            let s = String::from_utf8_lossy(&buf);
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = compile_app_core(&s, &b);
            }))
            .unwrap_or_else(|_| panic!("mutated sample {i} panicked"));
        }
    }
}
