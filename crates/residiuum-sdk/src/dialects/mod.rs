//! Pluggable query **dialects**: sql/json/mongo → portable → QVM (**RQL-DQ1**).
//!
//! SDA (+ ENR1) remains the mathematical language ([`SDA_SPEC`](../../../../../SDA_SPEC.md)),
//! and dialect id `sda` still parses/passes through raw SDA. The `sql`, `json`,
//! and `mongo` dialects no longer compile to SDA text: they lower to the
//! portable [`crate::filter::Filter`] vocabulary, which the product path lowers
//! further to [`crate::predicate::Predicate`] and executes on the Query VM
//! (see [`crate::Collection::find_dialect_with`]). Dialects are comfortable,
//! imperfect frontends — never a redefinition of the algebra and **not** a
//! hybrid of co-equal languages. Foreign surfaces cannot losslessly express
//! every algebraic distinction (especially Null vs absence); when that
//! precision is required, callers use pure SDA (dialect `sda`).
//! See [doc/SDA/DIALECTS.md](../../../../../doc/SDA/DIALECTS.md).
//!
//! Builtin ids: `sda` (explicit raw SDA — parse-checked, executed via
//! `Collection::sda`/`filter_sda`), `rql` (**retired** from this surface —
//! use Query VM / `CollectionClient::rql`), `json`, `mongo` (alias of `json`),
//! `sql`, `graphql` (scaffold / refuse). Hosts may register more via
//! [`DialectRegistry`]. Custom dialects return **portable** compiles only;
//! raw SDA is restricted to builtin dialect id `sda` / [`compile_sda_source`].

mod portable;
#[cfg(test)]
mod rql; // legacy RQL→SDA compiler kept test-only (RQL-R1); product path refuses
mod sql;

pub use portable::{CompiledDialect, CompiledPortable};

use crate::error::Error;
use crate::filter::Filter;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Profile / docs tag for the dialect compilation surface.
pub const DIALECT_PROFILE: &str = "residiuum-query-dialects-v0.1";

/// Shape of the compiled pure SDA artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SdaShape {
    /// Boolean expression over a single document bound as `input`.
    ///
    /// Suitable for [`crate::Collection::filter_sda`] and for evaluating one
    /// row at a time.
    DocumentPredicate,
    /// Full SDA program. Binding `input` is host-defined (often a sequence of
    /// documents for projection dialects).
    Program,
}

/// Result of compiling dialect source into pure SDA.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledSda {
    /// Dialect id that produced this compilation (`json`, `sql`, …).
    pub dialect: String,
    /// Pure SDA source text.
    pub sda: String,
    /// Whether `sda` is a document predicate or a full program.
    pub shape: SdaShape,
    /// Non-fatal mapping notes (mimicry caveats, ignored clauses, …).
    pub notes: Vec<String>,
}

impl CompiledSda {
    /// Construct a document-predicate compilation.
    pub fn predicate(
        dialect: impl Into<String>,
        sda: impl Into<String>,
        notes: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            dialect: dialect.into(),
            sda: sda.into(),
            shape: SdaShape::DocumentPredicate,
            notes: notes.into_iter().collect(),
        }
    }

    /// Construct a full-program compilation.
    pub fn program(
        dialect: impl Into<String>,
        sda: impl Into<String>,
        notes: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            dialect: dialect.into(),
            sda: sda.into(),
            shape: SdaShape::Program,
            notes: notes.into_iter().collect(),
        }
    }
}

/// A frontend that compiles foreign notation into a portable [`Filter`] for
/// Query VM execution (**RQL-DQ1**).
///
/// Implementations MUST refuse unmappable constructs rather than silently
/// weaken semantics. Approximate mappings SHOULD attach honesty notes.
///
/// **Raw SDA is not available through this trait.** Use builtin dialect `sda`
/// / [`compile_sda_source`] / `Collection::sda` for explicit raw-SDA surfaces.
pub trait QueryDialect: Send + Sync {
    /// Stable dialect id (e.g. `"sql"`, `"json"`).
    fn id(&self) -> &str;

    /// Human-readable name.
    fn name(&self) -> &str;

    /// One-line description of coverage and limits.
    fn description(&self) -> &str;

    /// Compile `source` into a portable filter for Query VM.
    fn compile(&self, source: &str) -> Result<CompiledPortable, Error>;
}

/// Static metadata for discovery (CLI, docs, explain).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DialectInfo {
    /// Stable id.
    pub id: &'static str,
    /// Display name.
    pub name: &'static str,
    /// Coverage summary.
    pub description: &'static str,
    /// Whether compilation is implemented for a useful subset.
    pub implemented: bool,
}

/// Builtin dialect identifiers recognized by [`compile_dialect`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuiltinDialect {
    /// Pure SDA / ENR1 source (parse-checked pass-through).
    Sda,
    /// Id reserved for official RQL — **refuses** on this surface (RQL-R1).
    /// Product RQL runs via Query VM (`CollectionClient::rql` / `execute_rql_full`).
    Rql,
    /// DX/Mongo-style JSON filter object → document predicate.
    Json,
    /// Alias of [`Self::Json`] for Mongo-familiar callers.
    Mongo,
    /// Tiny SQL `SELECT` / `WHERE` mimicry (partial).
    Sql,
    /// Reserved; compilation fails closed until designed.
    Graphql,
}

impl BuiltinDialect {
    /// Parse a dialect id (case-insensitive). Unknown → `None`.
    pub fn from_id(id: &str) -> Option<Self> {
        match id.trim().to_ascii_lowercase().as_str() {
            "sda" => Some(Self::Sda),
            "rql" => Some(Self::Rql),
            "json" | "json-filter" | "filter" => Some(Self::Json),
            "mongo" | "mongodb" => Some(Self::Mongo),
            "sql" => Some(Self::Sql),
            "graphql" | "gql" => Some(Self::Graphql),
            _ => None,
        }
    }

    /// Canonical id string.
    pub const fn id(self) -> &'static str {
        match self {
            Self::Sda => "sda",
            Self::Rql => "rql",
            Self::Json => "json",
            Self::Mongo => "mongo",
            Self::Sql => "sql",
            Self::Graphql => "graphql",
        }
    }

    /// Human name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sda => "Pure SDA",
            Self::Rql => "Residiuum Query Language",
            Self::Json => "JSON filter",
            Self::Mongo => "Mongo-style filter",
            Self::Sql => "SQL mimicry",
            Self::Graphql => "GraphQL (scaffold)",
        }
    }

    /// Coverage blurb.
    pub const fn description(self) -> &'static str {
        match self {
            Self::Sda => "Mathematical SDA/ENR1 source; parse-checked identity",
            Self::Rql => {
                "Retired from dialect→SDA; use CollectionClient::rql / execute_rql_full (Query VM)"
            }
            Self::Json => "DX portable filter object; complete for §7.1 vocabulary",
            Self::Mongo => "Alias of json (Mongo-style $ops object filter)",
            Self::Sql => "Partial SELECT/WHERE → portable Filter → Query VM; not full SQL",
            Self::Graphql => "Id reserved; not implemented",
        }
    }

    /// Whether a useful subset is implemented on the dialect→SDA surface.
    pub const fn implemented(self) -> bool {
        !matches!(self, Self::Graphql | Self::Rql)
    }

    /// Compile `source` with this builtin dialect (**RQL-DQ1**: sql/json/mongo
    /// lower to a portable [`Filter`] for Query VM execution, not SDA text).
    pub fn compile(self, source: &str) -> Result<CompiledDialect, Error> {
        match self {
            Self::Sda => compile_sda(source).map(CompiledDialect::Sda),
            Self::Rql => Err(Error::QueryInvalid(
                "dialect 'rql' no longer compiles to SDA (RQL-R1): the parallel \
                 RQL→SDA executor is retired. Use CollectionClient::rql / \
                 execute_rql_full (Query VM / QVM1), or pure SDA via dialect \
                 'sda' / Collection::sda. See doc/todo/rql/RQL_WHAT_IS_LEFT.md"
                    .into(),
            )),
            Self::Json | Self::Mongo => {
                compile_json_filter_portable(self.id(), source).map(CompiledDialect::Portable)
            }
            Self::Sql => sql::compile_sql(source).map(CompiledDialect::Portable),
            Self::Graphql => Err(Error::QueryInvalid(
                "dialect 'graphql' is reserved but not implemented; \
                 use pure SDA (dialect 'sda'), json/mongo filter, or sql mimicry \
                 (see doc/SDA/DIALECTS.md). Official RQL uses CollectionClient::rql."
                    .into(),
            )),
        }
    }
}

impl QueryDialect for BuiltinDialect {
    fn id(&self) -> &str {
        BuiltinDialect::id(*self)
    }

    fn name(&self) -> &str {
        BuiltinDialect::name(*self)
    }

    fn description(&self) -> &str {
        BuiltinDialect::description(*self)
    }

    fn compile(&self, source: &str) -> Result<CompiledPortable, Error> {
        match BuiltinDialect::compile(*self, source)? {
            CompiledDialect::Portable(p) => Ok(p),
            CompiledDialect::Sda(_) => Err(Error::QueryInvalid(
                "raw SDA is not available through QueryDialect; use dialect id `sda` / compile_sda_source"
                    .into(),
            )),
        }
    }
}

/// All builtin dialect metadata (including scaffold-only ids).
pub fn list_builtin_dialects() -> &'static [DialectInfo] {
    const LIST: &[DialectInfo] = &[
        DialectInfo {
            id: "sda",
            name: "Pure SDA",
            description: "Mathematical SDA/ENR1 source; parse-checked identity",
            implemented: true,
        },
        DialectInfo {
            id: "rql",
            name: "Residiuum Query Language (retired on dialect→SDA)",
            description: "Refuse here; use CollectionClient::rql / execute_rql_full (Query VM)",
            implemented: false,
        },
        DialectInfo {
            id: "json",
            name: "JSON filter",
            description: "DX portable filter object; complete for §7.1 vocabulary",
            implemented: true,
        },
        DialectInfo {
            id: "mongo",
            name: "Mongo-style filter",
            description: "Alias of json (Mongo-style $ops object filter)",
            implemented: true,
        },
        DialectInfo {
            id: "sql",
            name: "SQL mimicry",
            description: "Partial SELECT/WHERE → portable Filter → Query VM; not full SQL",
            implemented: true,
        },
        DialectInfo {
            id: "graphql",
            name: "GraphQL (scaffold)",
            description: "Id reserved; not implemented",
            implemented: false,
        },
    ];
    LIST
}

/// Compile `source` with a builtin dialect id (`json`, `sql`, `sda`, …).
///
/// Custom dialects require a [`DialectRegistry`]. Returns a [`CompiledDialect`]
/// (**RQL-DQ1**): `sda` is raw SDA; `sql`/`json`/`mongo` are portable filters
/// bound for Query VM execution.
pub fn compile_dialect(dialect_id: &str, source: &str) -> Result<CompiledDialect, Error> {
    match BuiltinDialect::from_id(dialect_id) {
        Some(d) => d.compile(source),
        None => Err(portable::unknown_dialect(dialect_id)),
    }
}

/// Registry of builtin + caller-registered dialects.
#[derive(Clone, Default)]
pub struct DialectRegistry {
    custom: HashMap<String, Arc<dyn QueryDialect>>,
}

impl DialectRegistry {
    /// Empty registry (still resolves builtins via [`Self::compile`]).
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a custom dialect. Id is stored lowercased; must not collide
    /// with a builtin id unless intentionally shadowing (shadowing is refused).
    pub fn register(&mut self, dialect: Arc<dyn QueryDialect>) -> Result<(), Error> {
        let id = dialect.id().trim().to_ascii_lowercase();
        if id.is_empty() {
            return Err(Error::QueryInvalid("dialect id must be non-empty".into()));
        }
        if BuiltinDialect::from_id(&id).is_some() {
            return Err(Error::QueryInvalid(format!(
                "cannot register dialect {id:?}: id is reserved for a builtin"
            )));
        }
        if self.custom.contains_key(&id) {
            return Err(Error::QueryInvalid(format!(
                "dialect {id:?} is already registered"
            )));
        }
        self.custom.insert(id, dialect);
        Ok(())
    }

    /// Compile with builtin or custom dialect.
    pub fn compile(&self, dialect_id: &str, source: &str) -> Result<CompiledDialect, Error> {
        let key = dialect_id.trim().to_ascii_lowercase();
        if let Some(d) = BuiltinDialect::from_id(&key) {
            return d.compile(source);
        }
        if let Some(d) = self.custom.get(&key) {
            // Custom dialects are portable-only (raw SDA is builtin `sda` only).
            return Ok(CompiledDialect::Portable(d.compile(source)?));
        }
        let mut known: Vec<&str> = list_builtin_dialects().iter().map(|d| d.id).collect();
        known.extend(self.custom.keys().map(|s| s.as_str()));
        known.sort_unstable();
        Err(Error::QueryInvalid(format!(
            "unknown query dialect {dialect_id:?}; known: {}",
            known.join(", ")
        )))
    }

    /// List builtin metadata plus registered custom ids.
    pub fn list(&self) -> Vec<DialectInfoOwned> {
        let mut out: Vec<DialectInfoOwned> = list_builtin_dialects()
            .iter()
            .map(DialectInfoOwned::from_static)
            .collect();
        for (id, d) in &self.custom {
            out.push(DialectInfoOwned {
                id: id.clone(),
                name: d.name().to_string(),
                description: d.description().to_string(),
                implemented: true,
            });
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }
}

/// Owned dialect metadata (for registries that include custom ids).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DialectInfoOwned {
    /// Stable id.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Coverage summary.
    pub description: String,
    /// Whether compilation is implemented.
    pub implemented: bool,
}

impl DialectInfoOwned {
    fn from_static(info: &DialectInfo) -> Self {
        Self {
            id: info.id.to_string(),
            name: info.name.to_string(),
            description: info.description.to_string(),
            implemented: info.implemented,
        }
    }
}

fn compile_sda(source: &str) -> Result<CompiledSda, Error> {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return Err(Error::QueryInvalid("dialect 'sda': empty program".into()));
    }
    sda_core::Program::parse(trimmed)
        .map_err(|e| Error::QueryInvalid(format!("dialect 'sda' parse failed: {e}")))?;
    Ok(CompiledSda::program(
        "sda",
        trimmed,
        std::iter::empty::<String>(),
    ))
}

/// Compile a `json`/`mongo` filter object into a portable [`CompiledPortable`]
/// (**RQL-DQ1**: Query VM bound, not SDA text).
fn compile_json_filter_portable(dialect_id: &str, source: &str) -> Result<CompiledPortable, Error> {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return Err(Error::QueryInvalid(format!(
            "dialect '{dialect_id}': empty filter"
        )));
    }
    let value: JsonValue = serde_json::from_str(trimmed).map_err(|e| {
        Error::QueryInvalid(format!(
            "dialect '{dialect_id}': filter must be a JSON object: {e}"
        ))
    })?;
    let filter = Filter::from_json(&value)?;
    // Sanity: portable filter must lower to a Query VM predicate.
    filter.to_predicate()?;
    Ok(CompiledPortable::new(
        dialect_id,
        filter,
        None,
        vec!["RQL-DQ1: compiles to portable Filter → Query VM (not SDA)".into()],
    ))
}

/// Compile a JSON value (not a string) with the `json` dialect → portable
/// Filter for Query VM (**RQL-DQ1**; does not return raw SDA).
pub fn compile_json_value(filter: &JsonValue) -> Result<CompiledPortable, Error> {
    let f = Filter::from_json(filter)?;
    // Sanity: portable filter must lower to a Query VM predicate.
    f.to_predicate()?;
    Ok(CompiledPortable::new(
        "json",
        f,
        None,
        vec!["RQL-DQ1: compile_json_value → portable Filter → Query VM (not SDA)".into()],
    ))
}

/// Explicit raw-SDA compile surface (dialect id `sda` only).
///
/// Prefer this (or `compile_dialect("sda", …)`) over custom dialects for SDA.
pub fn compile_sda_source(source: &str) -> Result<CompiledSda, Error> {
    compile_sda(source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn list_includes_scaffolds() {
        let ids: Vec<_> = list_builtin_dialects().iter().map(|d| d.id).collect();
        assert!(ids.contains(&"sda"));
        assert!(ids.contains(&"rql"));
        assert!(ids.contains(&"json"));
        assert!(ids.contains(&"mongo"));
        assert!(ids.contains(&"sql"));
        assert!(ids.contains(&"graphql"));
        assert!(
            list_builtin_dialects()
                .iter()
                .find(|d| d.id == "rql")
                .unwrap()
                .implemented
                == false
        );
        assert!(
            !list_builtin_dialects()
                .iter()
                .find(|d| d.id == "graphql")
                .unwrap()
                .implemented
        );
    }

    #[test]
    fn rql_dialect_refuses_sda_compile() {
        let err = compile_dialect(
            "rql",
            r#"
            from orders
            enrich customer using customers
              matching customer_id = id
              expect exactly_one
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no longer compiles to SDA") || msg.contains("RQL-R1"),
            "got {msg}"
        );
    }

    #[test]
    fn json_dialect_compiles_and_matches() {
        let src = r#"{"status":"active","age":{"$gte":18}}"#;
        let compiled = compile_dialect("json", src).unwrap();
        assert_eq!(compiled.dialect_id(), "json");
        let p = compiled.as_portable().expect("portable (RQL-DQ1)");
        assert!(p.filter.matches(&json!({"status": "active", "age": 21})));
        assert!(!p.filter.matches(&json!({"status": "active", "age": 10})));
        let pred = p.filter.to_predicate().unwrap();
        let params = std::collections::BTreeMap::new();
        assert!(pred
            .eval(&json!({"status": "active", "age": 21}), &params)
            .unwrap());
    }

    #[test]
    fn mongo_alias_matches_json() {
        let src = r#"{"x":1}"#;
        let a = compile_dialect("mongo", src).unwrap();
        let b = compile_dialect("json", src).unwrap();
        let pa = a.as_portable().expect("portable");
        let pb = b.as_portable().expect("portable");
        assert_eq!(pa.filter, pb.filter);
        assert_eq!(a.dialect_id(), "mongo");
    }

    #[test]
    fn sda_pass_through() {
        let src = r#"getPath(input, Seq["a"]) = Some(1)"#;
        let c = compile_dialect("sda", src).unwrap();
        let s = c.as_sda().expect("sda");
        assert_eq!(s.shape, SdaShape::Program);
        assert_eq!(s.sda, src);
    }

    #[test]
    fn graphql_refuses() {
        let err = compile_dialect("graphql", "{ user { id } }").unwrap_err();
        assert!(err.to_string().contains("not implemented"));
    }

    #[test]
    fn unknown_dialect() {
        let err = compile_dialect("cypher", "MATCH (n)").unwrap_err();
        assert!(err.to_string().contains("unknown query dialect"));
    }

    #[test]
    fn sql_select_star_where() {
        let c = compile_dialect("sql", "SELECT * WHERE status = 'active' AND age >= 18").unwrap();
        let p = c.as_portable().expect("portable (RQL-DQ1)");
        assert!(p.project.is_none());
        assert!(p.filter.matches(&json!({"status": "active", "age": 20})));
        assert!(!p.filter.matches(&json!({"status": "active", "age": 10})));
    }

    #[test]
    fn sql_projection_program() {
        let c = compile_dialect("sql", "SELECT name, city WHERE status = 'active'").unwrap();
        let p = c.as_portable().expect("portable (RQL-DQ1)");
        assert_eq!(
            p.project,
            Some(vec!["name".to_string(), "city".to_string()])
        );
        assert!(!p.notes.is_empty());
        assert!(p
            .filter
            .matches(&json!({"name": "Ada", "city": "LA", "status": "active"})));
        assert!(!p
            .filter
            .matches(&json!({"name": "Bob", "city": "NY", "status": "idle"})));
    }

    #[test]
    fn registry_custom_dialect() {
        struct Echo;
        impl QueryDialect for Echo {
            fn id(&self) -> &str {
                "echo"
            }
            fn name(&self) -> &str {
                "Echo"
            }
            fn description(&self) -> &str {
                "test"
            }
            fn compile(&self, source: &str) -> Result<CompiledPortable, Error> {
                Ok(CompiledPortable::new(
                    "echo",
                    Filter::Always,
                    None,
                    vec![format!("echoed:{source}")],
                ))
            }
        }
        let mut reg = DialectRegistry::new();
        reg.register(Arc::new(Echo)).unwrap();
        let c = reg.compile("echo", "hi").unwrap();
        let p = c.as_portable().expect("portable only for custom dialects");
        assert_eq!(p.dialect, "echo");
        assert!(p.notes.iter().any(|n| n.contains("hi")));
        assert!(reg.register(Arc::new(Echo)).is_err());
        assert!(reg.register(Arc::new(BuiltinDialect::Json)).is_err());
    }

    #[test]
    fn compile_json_value_api() {
        let c = compile_json_value(&json!({"a": {"$exists": true}})).unwrap();
        assert_eq!(c.dialect, "json");
        // Portable path — not raw SDA.
        let _ = c.filter.to_predicate().unwrap();
        assert!(c
            .notes
            .iter()
            .any(|n| n.contains("Query VM") || n.contains("portable")));
    }
}
