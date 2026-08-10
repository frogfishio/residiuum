//! Multi-collection join query axis (Residiuum-level).
//!
//! Complementary to single-collection [`crate::QueryBuilder`] and to the raw
//! **SDA/ENR text** axis ([`crate::SdaTextQuery`] / [`crate::Residiuum::sda_query`]):
//!
//! 1. **Join axis** — `FROM` + equijoin `ON left = right` over named collections
//!    (SQL / Mongo-ish), producing a rough bag of joined JSON objects.
//! 2. **SDA axis** — optional pure SDA program over that bag for projection /
//!    normalisation (SDA never opens collections).
//!
//! When users want to write match bags, `one!` / `one?`, and attach/`+` **as
//! source text** rather than fluent equijoins, use [`crate::Residiuum::sda_query`]
//! instead of this builder.
//!
//! ```ignore
//! let rows = db
//!     .query()
//!     .from("orders")
//!     .where_eq("status", "paid")
//!     .join("customers").on("customer_id", "id")
//!     .join("products").on("product_id", "id")
//!     .collect()?;
//!
//! let normalised = db
//!     .query()
//!     .from("orders")
//!     .join("customers").on("customer_id", "id")
//!     .join("products").on("product_id", "id")
//!     .map_sda(r#"{ yield getPath(row, Seq["orders", "id"]) | row in input }"#)?;
//! ```
//!
//! Join rows are namespaced by collection (or alias):
//! `{ "orders": {…}, "customers": {…}, "products": {…} }`.
//!
//! This is a **client-side** equijoin helper (hash join), not a distributed
//! relational engine. Keep join inputs bounded or attach per-source filters /
//! budgets. Nested SDA over full cartesian products is intentionally avoided —
//! the host join does the X=Y work; SDA only shapes the result.

use crate::error::Error;
use crate::filter::{resolve_path_value, Filter, QueryBudget, QueryOptions};
use crate::residiuum::Residiuum;
use crate::subject::validate_collection_name;
use serde_json::{json, Map, Value as JsonValue};
use std::collections::HashMap;

/// Profile tag for multi-collection join plans (serialisable later).
pub const MULTI_QUERY_PROFILE: &str = "residiuum-multi-query-v1";

/// One collection source in a multi-collection query.
#[derive(Debug, Clone)]
struct SourceSpec {
    collection: String,
    alias: String,
    filter: Filter,
    options: QueryOptions,
}

/// Equijoin clause: `left_alias.left_field = <new source>.right_field`.
#[derive(Debug, Clone)]
struct JoinSpec {
    left_alias: String,
    left_field: String,
    right_field: String,
}

/// Multi-collection join query builder ([`Residiuum::query`]).
///
/// Materialises per-collection finds, hash-equijoins them, then optionally
/// runs an SDA program with `input` bound to the joined row sequence.
pub struct MultiQuery<'a> {
    residiuum: &'a mut Residiuum,
    sources: Vec<SourceSpec>,
    joins: Vec<JoinSpec>,
    /// Cap on the final joined row count (applied after all joins).
    limit: Option<usize>,
    /// When true, attach `_key` under each collection namespace.
    include_keys: bool,
}

/// Fluent state after [`MultiQuery::join`] waiting for [`JoinBuilder::on`].
pub struct JoinBuilder<'a> {
    query: MultiQuery<'a>,
    right_collection: String,
    right_alias: String,
    right_filter: Filter,
    right_options: QueryOptions,
}

impl<'a> MultiQuery<'a> {
    pub(crate) fn new(residiuum: &'a mut Residiuum) -> Self {
        Self {
            residiuum,
            sources: Vec::new(),
            joins: Vec::new(),
            limit: None,
            include_keys: false,
        }
    }

    /// Start from a collection (`FROM name`). Alias defaults to the collection name.
    pub fn from(mut self, collection: impl Into<String>) -> Self {
        let collection = collection.into();
        self.sources.push(SourceSpec {
            alias: collection.clone(),
            collection,
            filter: Filter::always(),
            options: QueryOptions::default(),
        });
        self
    }

    /// `FROM collection AS alias`.
    pub fn from_as(mut self, collection: impl Into<String>, alias: impl Into<String>) -> Self {
        let collection = collection.into();
        let alias = alias.into();
        self.sources.push(SourceSpec {
            collection,
            alias,
            filter: Filter::always(),
            options: QueryOptions::default(),
        });
        self
    }

    /// Replace the filter on the most recently added source.
    pub fn filter(mut self, f: Filter) -> Self {
        if let Some(src) = self.sources.last_mut() {
            src.filter = f;
        }
        self
    }

    /// AND another filter onto the most recently added source.
    pub fn and_filter(mut self, f: Filter) -> Self {
        if let Some(src) = self.sources.last_mut() {
            src.filter = Filter::and(vec![src.filter.clone(), f]);
        }
        self
    }

    /// `field == value` on the most recent source.
    pub fn where_eq(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).eq(value))
    }

    /// `field != value` on the most recent source.
    pub fn where_ne(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).ne(value))
    }

    /// `field < value` on the most recent source.
    pub fn where_lt(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).lt(value))
    }

    /// `field <= value` on the most recent source.
    pub fn where_lte(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).lte(value))
    }

    /// `field > value` on the most recent source.
    pub fn where_gt(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).gt(value))
    }

    /// `field >= value` on the most recent source.
    pub fn where_gte(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).gte(value))
    }

    /// `field` in `values` on the most recent source.
    pub fn where_in<I, V>(self, field: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<JsonValue>,
    {
        self.and_filter(Filter::field(field).is_in(values))
    }

    /// Cap documents examined on the most recent source (scan budget).
    pub fn source_budget(mut self, budget: QueryBudget) -> Self {
        if let Some(src) = self.sources.last_mut() {
            src.options.budget = Some(budget);
        }
        self
    }

    /// Cap documents returned from the most recent source find.
    pub fn source_limit(mut self, n: usize) -> Self {
        if let Some(src) = self.sources.last_mut() {
            src.options.limit = Some(n);
        }
        self
    }

    /// Begin an INNER JOIN of another collection (call [`.on`](JoinBuilder::on) next).
    pub fn join(self, collection: impl Into<String>) -> JoinBuilder<'a> {
        let collection = collection.into();
        JoinBuilder {
            right_alias: collection.clone(),
            right_collection: collection,
            right_filter: Filter::always(),
            right_options: QueryOptions::default(),
            query: self,
        }
    }

    /// Begin a join with an explicit alias for the right side.
    pub fn join_as(
        self,
        collection: impl Into<String>,
        alias: impl Into<String>,
    ) -> JoinBuilder<'a> {
        JoinBuilder {
            right_collection: collection.into(),
            right_alias: alias.into(),
            right_filter: Filter::always(),
            right_options: QueryOptions::default(),
            query: self,
        }
    }

    /// Cap the number of final joined rows.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Embed each collection's store key as `_key` inside that namespace object.
    ///
    /// When the document already has a `_key` field it is overwritten.
    pub fn include_keys(mut self) -> Self {
        self.include_keys = true;
        self
    }

    /// Execute equijoins and return the rough joined bag (no SDA).
    ///
    /// Each element is an object keyed by source alias → document JSON.
    pub fn collect(self) -> Result<Vec<JsonValue>, Error> {
        self.execute_join()
    }

    /// Execute equijoins, then run pure SDA with `input` = the joined sequence.
    ///
    /// Use this for normalisation / projection after the host join has already
    /// applied the X = Y relationships.
    pub fn map_sda(self, program: &str) -> Result<JsonValue, Error> {
        let rows = self.execute_join()?;
        map_joined_sda(&rows, program)
    }

    /// Explain-ish summary (human + structured) without executing finds.
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
        let joins: Vec<JsonValue> = self
            .joins
            .iter()
            .zip(self.sources.iter().skip(1))
            .map(|(j, right)| {
                json!({
                    "type": "inner",
                    "left_alias": j.left_alias,
                    "left_field": j.left_field,
                    "right_alias": right.alias,
                    "right_field": j.right_field,
                })
            })
            .collect();
        json!({
            "profile": MULTI_QUERY_PROFILE,
            "sources": sources,
            "joins": joins,
            "limit": self.limit,
            "include_keys": self.include_keys,
        })
    }

    fn execute_join(self) -> Result<Vec<JsonValue>, Error> {
        if self.sources.is_empty() {
            return Err(Error::QueryInvalid(
                "multi-query requires .from(\"collection\") before collect".into(),
            ));
        }
        if self.joins.len() + 1 != self.sources.len() {
            return Err(Error::QueryInvalid(
                "internal: join count must be sources-1 (call .on after each .join)".into(),
            ));
        }
        for src in &self.sources {
            validate_collection_name(&src.collection)?;
            if src.alias.is_empty() {
                return Err(Error::QueryInvalid(
                    "join source alias must be non-empty".into(),
                ));
            }
        }
        // Unique aliases.
        {
            let mut seen = std::collections::HashSet::new();
            for src in &self.sources {
                if !seen.insert(src.alias.as_str()) {
                    return Err(Error::QueryInvalid(format!(
                        "duplicate join alias {:?}",
                        src.alias
                    )));
                }
            }
        }

        // Materialise each source: alias → rows of (store_key, doc).
        let mut loaded: Vec<(String, Vec<(String, JsonValue)>)> =
            Vec::with_capacity(self.sources.len());
        // We need sequential mutable access to residiuum.collection — load one at a time.
        let MultiQuery {
            residiuum,
            sources,
            joins,
            limit,
            include_keys,
        } = self;

        for src in &sources {
            let mut col = residiuum.collection(&src.collection)?;
            let rows = col.find_with(&src.filter, src.options.clone())?;
            loaded.push((src.alias.clone(), rows));
        }

        // Seed working set from first source.
        let (first_alias, first_rows) = &loaded[0];
        let mut working: Vec<Map<String, JsonValue>> = first_rows
            .iter()
            .map(|(key, doc)| {
                let mut map = Map::new();
                map.insert(first_alias.clone(), namespaced_doc(doc, key, include_keys));
                map
            })
            .collect();

        // Progressive hash equijoins.
        for (join_i, join) in joins.iter().enumerate() {
            let (right_alias, right_rows) = &loaded[join_i + 1];
            // Index right side by join key.
            let mut index: HashMap<String, Vec<&(String, JsonValue)>> = HashMap::new();
            for row in right_rows {
                if let Some(jv) = resolve_path_value(&row.1, &join.right_field) {
                    let k = join_key(jv);
                    index.entry(k).or_default().push(row);
                }
            }

            let mut next: Vec<Map<String, JsonValue>> = Vec::new();
            for left in working {
                let left_doc = left.get(&join.left_alias).ok_or_else(|| {
                    Error::QueryInvalid(format!(
                        "join left alias {:?} not present in row (check .on_from)",
                        join.left_alias
                    ))
                })?;
                let Some(lv) = resolve_path_value(left_doc, &join.left_field) else {
                    continue; // missing join key → drop (inner join)
                };
                let k = join_key(lv);
                if let Some(matches) = index.get(&k) {
                    for (rkey, rdoc) in matches {
                        let mut combined = left.clone();
                        combined.insert(
                            right_alias.clone(),
                            namespaced_doc(rdoc, rkey, include_keys),
                        );
                        next.push(combined);
                        if let Some(lim) = limit {
                            if next.len() >= lim {
                                return Ok(next.into_iter().map(JsonValue::Object).collect());
                            }
                        }
                    }
                }
            }
            working = next;
        }

        if let Some(lim) = limit {
            if working.len() > lim {
                working.truncate(lim);
            }
        }

        Ok(working.into_iter().map(JsonValue::Object).collect())
    }
}

impl<'a> JoinBuilder<'a> {
    /// Filter the right-hand collection before joining.
    pub fn filter(mut self, f: Filter) -> Self {
        self.right_filter = f;
        self
    }

    /// AND a filter on the right-hand collection.
    pub fn and_filter(mut self, f: Filter) -> Self {
        self.right_filter = Filter::and(vec![self.right_filter, f]);
        self
    }

    /// `field == value` on the right-hand collection.
    pub fn where_eq(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).eq(value))
    }

    /// `field != value` on the right-hand collection.
    pub fn where_ne(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).ne(value))
    }

    /// `field < value` on the right-hand collection.
    pub fn where_lt(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).lt(value))
    }

    /// `field <= value` on the right-hand collection.
    pub fn where_lte(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).lte(value))
    }

    /// `field > value` on the right-hand collection.
    pub fn where_gt(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).gt(value))
    }

    /// `field >= value` on the right-hand collection.
    pub fn where_gte(self, field: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.and_filter(Filter::field(field).gte(value))
    }

    /// Membership on the right-hand collection.
    pub fn where_in<I, V>(self, field: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<JsonValue>,
    {
        self.and_filter(Filter::field(field).is_in(values))
    }

    /// Cap documents examined on the right source.
    pub fn source_budget(mut self, budget: QueryBudget) -> Self {
        self.right_options.budget = Some(budget);
        self
    }

    /// Cap documents returned from the right find.
    pub fn source_limit(mut self, n: usize) -> Self {
        self.right_options.limit = Some(n);
        self
    }

    /// Equijoin: `FROM/primary.left_field = right.right_field`.
    ///
    /// Left side defaults to the **first** source (the `FROM` table), matching
    /// the usual FK pattern (`orders.customer_id = customers.id`).
    pub fn on(
        self,
        left_field: impl Into<String>,
        right_field: impl Into<String>,
    ) -> MultiQuery<'a> {
        let left_alias = self
            .query
            .sources
            .first()
            .map(|s| s.alias.clone())
            .unwrap_or_default();
        self.on_from(left_alias, left_field, right_field)
    }

    /// Equijoin against an explicit left alias:
    /// `left_alias.left_field = right.right_field`.
    pub fn on_from(
        mut self,
        left_alias: impl Into<String>,
        left_field: impl Into<String>,
        right_field: impl Into<String>,
    ) -> MultiQuery<'a> {
        self.query.sources.push(SourceSpec {
            collection: self.right_collection,
            alias: self.right_alias,
            filter: self.right_filter,
            options: self.right_options,
        });
        self.query.joins.push(JoinSpec {
            left_alias: left_alias.into(),
            left_field: left_field.into(),
            right_field: right_field.into(),
        });
        self.query
    }
}

/// Run pure SDA over an already-joined bag (`input` = array of joined rows).
///
/// Host-side helper when the join was materialised separately.
pub fn map_joined_sda(rows: &[JsonValue], program: &str) -> Result<JsonValue, Error> {
    let prog = sda_core::Program::parse(program).map_err(|e| {
        Error::QueryInvalid(format!("multi-query SDA parse failed: {e}; src={program}"))
    })?;
    let input = JsonValue::Array(rows.to_vec());
    prog.run_json("input", input).map_err(|e| {
        Error::QueryInvalid(format!("multi-query SDA eval failed: {e}; src={program}"))
    })
}

fn namespaced_doc(doc: &JsonValue, key: &str, include_keys: bool) -> JsonValue {
    if !include_keys {
        return doc.clone();
    }
    match doc {
        JsonValue::Object(m) => {
            let mut out = m.clone();
            out.insert("_key".into(), JsonValue::String(key.to_string()));
            JsonValue::Object(out)
        }
        other => json!({
            "_key": key,
            "_value": other,
        }),
    }
}

/// Stable string key for JSON join values (equality-based).
fn join_key(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "null".into(),
        JsonValue::Bool(b) => format!("b:{b}"),
        JsonValue::Number(n) => format!("n:{n}"),
        JsonValue::String(s) => format!("s:{s}"),
        // Arrays/objects: canonical JSON (rare as join keys).
        other => format!("j:{}", other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json;
    use tempfile::tempdir;

    #[test]
    fn equijoin_two_collections() {
        let dir = tempdir().unwrap();
        let mut db = Residiuum::open(dir.path().join("q.residiuum")).unwrap();
        {
            let mut a = db.collection("a").unwrap();
            a.put("1", &json!({"id": 1, "x": "aa"})).unwrap();
            a.put("2", &json!({"id": 2, "x": "bb"})).unwrap();
        }
        {
            let mut b = db.collection("b").unwrap();
            b.put("10", &json!({"a_id": 1, "y": "yy"})).unwrap();
            b.put("20", &json!({"a_id": 99, "y": "zz"})).unwrap();
        }

        let rows = db
            .query()
            .from("a")
            .join("b")
            .on("id", "a_id")
            .collect()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"]["x"], json!("aa"));
        assert_eq!(rows[0]["b"]["y"], json!("yy"));
    }

    #[test]
    fn describe_plan_shape() {
        let dir = tempdir().unwrap();
        let mut db = Residiuum::open(dir.path().join("d.residiuum")).unwrap();
        let plan = db
            .query()
            .from("orders")
            .where_eq("status", "paid")
            .join("customers")
            .on("customer_id", "id")
            .describe();
        assert_eq!(plan["profile"], MULTI_QUERY_PROFILE);
        assert_eq!(plan["joins"].as_array().unwrap().len(), 1);
    }
}
