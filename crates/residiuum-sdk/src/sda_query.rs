//! Raw SDA / ENR1 text query axis (DX_SPEC §7.6).
//!
//! Portable [`crate::Filter`] builders and equijoin [`crate::MultiQuery`] are the
//! everyday surfaces. Advanced users still write **SDA + ENR1 as text** and run
//! those programs against host-supplied collection values.
//!
//! Layering:
//!
//! 1. Host scans/filters named collections (IO, budgets, indexes).
//! 2. Host binds documents under `input` (single collection → array; multi → map).
//! 3. Pure [`sda_core::Program`] evaluates (standalone SDA + ENR1 kernel).
//!
//! SDA never opens collections. ENR1 (`one?` / `one!` / `merge` / `+` / `asBag`)
//! rides the same `Program::parse` path as core SDA (`sda-enr1-v0.1`).
//!
//! ```ignore
//! // Single collection (DX §7.6)
//! let active = users.sda(r#"{
//!   yield u | u in input
//!     | getPath(u, Seq["status"]) = Some("active")
//! }"#)?;
//!
//! // Multi-collection ENR1: Match + enrich pipe (preferred surface)
//! let rows = db
//!     .enr_query()
//!     .bind("orders")
//!     .bind("customers")
//!     .run(r#"
//!       orders
//!       |> enrich {
//!           customer:
//!             one!(
//!               Match(
//!                 l,
//!                 customers,
//!                 getPath(l, Seq["customer_id"]),
//!                 getPath(r, Seq["id"])
//!               )
//!             )
//!         }
//!     "#)?;
//! ```

use crate::error::Error;
use crate::filter::{Filter, QueryOptions};
use crate::residiuum::Residiuum;
use crate::subject::validate_collection_name;
use serde_json::{json, Map, Value as JsonValue};

/// Profile tag for raw SDA/ENR text query plans (serialisable later).
pub const SDA_QUERY_PROFILE: &str = "residiuum-sda-query-v1";

/// Parse and evaluate a pure SDA/ENR1 program with `input` bound to `value`.
///
/// When `value` is a JSON object (multi-collection map), each key is also bound
/// as a top-level name so programs can write `orders |> enrich { … }` without
/// `bindOpt(getPath(input, …), …)` boilerplate.
///
/// Host-side helper when the input bag was materialised separately.
pub fn eval_sda_program(program: &str, input: JsonValue) -> Result<JsonValue, Error> {
    let prog = sda_core::Program::parse(program).map_err(|e| {
        Error::QueryInvalid(format!("SDA/ENR text parse failed: {e}; src={program}"))
    })?;
    let result = if let JsonValue::Object(map) = &input {
        let mut bindings: Vec<(String, JsonValue)> =
            vec![("input".into(), JsonValue::Object(map.clone()))];
        for (alias, docs) in map {
            bindings.push((alias.clone(), docs.clone()));
        }
        prog.run_json_bindings(bindings)
    } else {
        prog.run_json("input", input)
    };
    result.map_err(|e| Error::QueryInvalid(format!("SDA/ENR text eval failed: {e}; src={program}")))
}

/// One named collection binding for a multi-collection text program.
#[derive(Debug, Clone)]
struct BindSpec {
    collection: String,
    /// JSON object key under `input` (defaults to collection name).
    alias: String,
    filter: Filter,
    options: QueryOptions,
}

/// Multi-collection raw SDA/ENR1 text query ([`Residiuum::sda_query`] / [`Residiuum::enr_query`]).
///
/// Materialises each bound collection (optional per-source filter / budget),
/// builds `input` as a map `alias → document array`, binds each alias as a
/// top-level name, then runs the program (ENR1 `Match` / `enrich` / `one!` ok).
pub struct SdaTextQuery<'a> {
    residiuum: &'a mut Residiuum,
    sources: Vec<BindSpec>,
}

impl<'a> SdaTextQuery<'a> {
    pub(crate) fn new(residiuum: &'a mut Residiuum) -> Self {
        Self {
            residiuum,
            sources: Vec::new(),
        }
    }

    /// Bind a collection under `input.<name>` (full scan of live JSON docs).
    pub fn bind(self, collection: impl Into<String>) -> Self {
        let collection = collection.into();
        self.bind_as(collection.clone(), collection)
    }

    /// Bind a collection under a different `input` key.
    pub fn bind_as(mut self, collection: impl Into<String>, alias: impl Into<String>) -> Self {
        let collection = collection.into();
        let alias = alias.into();
        self.sources.push(BindSpec {
            collection,
            alias,
            filter: Filter::always(),
            options: QueryOptions::default(),
        });
        self
    }

    /// Replace the filter on the most recently bound source.
    pub fn filter(mut self, f: Filter) -> Self {
        if let Some(src) = self.sources.last_mut() {
            src.filter = f;
        }
        self
    }

    /// AND another filter onto the most recently bound source.
    pub fn and_filter(mut self, f: Filter) -> Self {
        if let Some(src) = self.sources.last_mut() {
            src.filter = Filter::and(vec![src.filter.clone(), f]);
        }
        self
    }

    /// `field == value` on the most recent binding.
    pub fn where_eq(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).eq(value))
    }

    /// Cap how many documents are loaded from the most recent binding.
    pub fn source_limit(mut self, n: usize) -> Self {
        if let Some(src) = self.sources.last_mut() {
            src.options.limit = Some(n);
        }
        self
    }

    /// Attach a scan budget on the most recent binding.
    pub fn source_budget(mut self, budget: crate::filter::QueryBudget) -> Self {
        if let Some(src) = self.sources.last_mut() {
            src.options.budget = Some(budget);
        }
        self
    }

    /// Materialise bindings and run pure SDA/ENR1 text (`input` = map of arrays).
    pub fn run(self, program: &str) -> Result<JsonValue, Error> {
        if self.sources.is_empty() {
            return Err(Error::QueryInvalid(
                "sda_query requires at least one .bind(\"collection\") before .run".into(),
            ));
        }
        for src in &self.sources {
            validate_collection_name(&src.collection)?;
            if src.alias.is_empty() {
                return Err(Error::QueryInvalid(
                    "sda_query bind alias must be non-empty".into(),
                ));
            }
        }
        {
            let mut seen = std::collections::HashSet::new();
            for src in &self.sources {
                if !seen.insert(src.alias.as_str()) {
                    return Err(Error::QueryInvalid(format!(
                        "duplicate sda_query bind alias {:?}",
                        src.alias
                    )));
                }
            }
        }

        let SdaTextQuery { residiuum, sources } = self;
        let mut map = Map::new();
        for src in sources {
            let rows = {
                let mut col = residiuum.collection(&src.collection)?;
                col.find_with(&src.filter, src.options)?
            };
            let docs: Vec<JsonValue> = rows.into_iter().map(|(_, v)| v).collect();
            map.insert(src.alias, JsonValue::Array(docs));
        }
        eval_sda_program(program, JsonValue::Object(map))
    }

    /// Explain-ish summary without executing finds.
    pub fn describe(&self) -> JsonValue {
        let sources: Vec<JsonValue> = self
            .sources
            .iter()
            .map(|s| {
                json!({
                    "collection": s.collection,
                    "alias": s.alias,
                    "filter_sda": s.filter.to_sda(),
                    "source_limit": s.options.limit,
                })
            })
            .collect();
        json!({
            "profile": SDA_QUERY_PROFILE,
            "axis": "sda_enr_text",
            "sources": sources,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json;
    use tempfile::tempdir;

    #[test]
    fn eval_sda_program_standalone() {
        let out = eval_sda_program(r#"input<"name">!"#, json!({"name": "Ada"})).unwrap();
        assert_eq!(out, json!({"$type": "ok", "$value": "Ada"}));
    }

    #[test]
    fn multi_bind_enr1_attach_text() {
        let dir = tempdir().unwrap();
        let mut db = Residiuum::open(dir.path().join("enr-text.residiuum")).unwrap();
        {
            let mut orders = db.collection("orders").unwrap();
            orders
                .put("o1", &json!({"id": "o1", "customer_id": "c1", "qty": 2}))
                .unwrap();
            orders
                .put("o2", &json!({"id": "o2", "customer_id": "c2", "qty": 1}))
                .unwrap();
        }
        {
            let mut customers = db.collection("customers").unwrap();
            customers
                .put("c1", &json!({"id": "c1", "name": "Ada"}))
                .unwrap();
            customers
                .put("c2", &json!({"id": "c2", "name": "Bob"}))
                .unwrap();
        }

        let program = r#"
          bindOpt(getPath(input, Seq["orders"]), orders =>
          bindOpt(getPath(input, Seq["customers"]), customers =>
            Some({
              yield o + Map{
                "customer" -> one!({
                  c | c in customers
                    | getPath(c, Seq["id"]) = getPath(o, Seq["customer_id"])
                })
              }
              | o in orders
            })
          ))
        "#;

        let out = db
            .sda_query()
            .bind("orders")
            .bind("customers")
            .run(program)
            .unwrap();

        // bindOpt wraps Some(Seq[...]); order is stable key order of puts.
        let arr = out
            .get("$value")
            .and_then(|v| v.as_array())
            .expect("Some(seq)");
        assert_eq!(arr.len(), 2);
        // Find by order id (scan key order is collection-key, not insertion guarantee across keys).
        let mut by_id = std::collections::HashMap::new();
        for row in arr {
            by_id.insert(row["id"].as_str().unwrap().to_string(), row.clone());
        }
        assert_eq!(by_id["o1"]["customer"]["name"], json!("Ada"));
        assert_eq!(by_id["o2"]["customer"]["name"], json!("Bob"));
        assert_eq!(by_id["o1"]["qty"], json!(2));
    }

    #[test]
    fn describe_requires_no_io() {
        let dir = tempdir().unwrap();
        let mut db = Residiuum::open(dir.path().join("d.residiuum")).unwrap();
        let plan = db
            .sda_query()
            .bind("orders")
            .where_eq("status", "paid")
            .bind("customers")
            .describe();
        assert_eq!(plan["profile"], SDA_QUERY_PROFILE);
        assert_eq!(plan["sources"].as_array().unwrap().len(), 2);
    }
}
