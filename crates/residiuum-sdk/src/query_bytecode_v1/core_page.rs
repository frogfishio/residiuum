//! Application Core page semantics — owned by [`crate::query_bytecode_v1`].
//!
//! Ported from the former standalone `query_exec_v1` executor (Decision 0 /
//! RQL-X2b). Do not grow a second semantic entry outside this module tree.
//!
//! Normative: CORE plan §10 / §14 APP-6 (partial). Compiles via
//! [`crate::rql_app_core::compile_app_core`], scans with `list_keys` + `get`,
//! evaluates predicates via the ENR+SDA kernel ([`super::kernel`]).
//! Core path-project goes through [`super::ir_project`] (RQL-IR1).
//! Core order / sort-tuple goes through [`super::ir_order`] (RQL-IR2).
//! Core page / coverage / cursor packing goes through [`super::ir_page`] (RQL-IR3).
//! [`Predicate::eval`] is the test oracle only.
//!
//! **T2 budgets:** `max_documents`, `max_bytes`, `max_result_bytes`.
//! **T3 multipage field-order:** cursor `last_sort_tuple` carries order-term
//! values (+ key); resume skips rows `<=` the prior page's last tuple (not
//! key-only). Key-stream order still uses key resume for streaming.
//! **T4 index pushdown:** when [`DocScan::try_equality_index_keys`] returns
//! candidates for field equalities, examine only those keys (still re-eval
//! full predicate). Fall back to full scan when no usable index.
//! **T8 deadline/cancel:** [`crate::app_v1::QueryRunOptions::deadline`] and
//! [`crate::app_v1::QueryRunOptions::cancel`] checked cooperatively between scan steps.
//!
//! **Not claimed:** product query qualification (APB-7 package accept), product
//! cursor secrets, remote op 118, snapshot reads.

use super::{HostDocument, HostGroupAggregate};
use crate::app_v1::QueryExplanation;
use crate::error::Error;
use crate::plan_v1::CollectionBindings;
use crate::predicate::{CompareOp, Operand, Predicate};
use crate::rql_app_core::compile_app_core;
use residiuum_heap::CollectionId;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

/// Profile label for this executor cut (not full product query).
pub const EXEC_PROFILE: &str = "residiuum-app-core-exec-v1";

/// Document accessor used by the scan executor (embedded or remote collection plane).
/// Document scan host adapter (crate-private; RQL-P0b).
pub(crate) trait DocScan {
    /// List keys in deterministic order.
    fn list_keys(
        &mut self,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, Error>;

    /// JSON get (None when absent).
    fn get_json(&mut self, key: &str) -> Result<Option<JsonValue>, Error>;

    /// Coverage-aware get; storage adapters override this to expose damage as
    /// hole evidence instead of aborting an incomplete-allowed query.
    fn get_json_covered(&mut self, key: &str) -> Result<HostDocument, Error> {
        Ok(match self.get_json(key)? {
            Some(value) => HostDocument::Present {
                value: value.into(),
                logical_bytes: None,
            },
            None => HostDocument::Absent,
        })
    }

    /// Coverage-aware bounded document page in deterministic key order.
    fn scan_json_covered_page(
        &mut self,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<(String, HostDocument)>, Error> {
        let keys = self.list_keys(limit, after_key)?;
        keys.into_iter()
            .map(|key| {
                let document = self.get_json_covered(&key)?;
                Ok((key, document))
            })
            .collect()
    }

    fn get_json_covered_many(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<(String, HostDocument)>, Error> {
        keys.iter()
            .map(|key| {
                let document = self.get_json_covered(key)?;
                Ok((key.clone(), document))
            })
            .collect()
    }

    /// Optional equality-index acceleration (APB-7 T4).
    ///
    /// Default: no index → caller full-scans. Implementors may return candidate
    /// application keys for a shallow AND of field equalities.
    fn try_equality_index_keys(
        &mut self,
        _equalities: &[(String, JsonValue)],
    ) -> Result<Option<Vec<String>>, Error> {
        Ok(None)
    }

    fn try_index_keys_with_constraints(
        &mut self,
        equalities: &[(String, JsonValue)],
        constrained_fields: &[String],
    ) -> Result<Option<Vec<String>>, Error> {
        let _ = constrained_fields;
        self.try_equality_index_keys(equalities)
    }

    fn try_ordered_index_keys(
        &mut self,
        _order: &[crate::plan_v1::OrderTerm],
    ) -> Result<Option<Vec<String>>, Error> {
        Ok(None)
    }

    fn try_source_frontier(&mut self) -> Result<Option<[u8; 32]>, Error> {
        Ok(None)
    }

    fn try_covering_index_values(
        &mut self,
        _fields: &[String],
    ) -> Result<Option<Vec<Vec<JsonValue>>>, Error> {
        Ok(None)
    }

    fn try_projected_values(
        &mut self,
        _fields: &[String],
    ) -> Result<Option<Vec<Vec<JsonValue>>>, Error> {
        Ok(None)
    }

    fn try_group_aggregate(
        &mut self,
        _spec: &crate::plan_v1::GroupAggSpec,
        _where_k: &super::CompiledKernelWhere,
        _candidate_keys: Option<&[String]>,
    ) -> Result<Option<HostGroupAggregate>, Error> {
        Ok(None)
    }
}

/// Explain Application Core source (plan tree + hash; no row materialization).
pub fn explain_rql_source(
    source: &str,
    collection_id: CollectionId,
    collection_name: &str,
) -> Result<QueryExplanation, Error> {
    let mut bindings = CollectionBindings {
        by_name: BTreeMap::new(),
    };
    bindings.bind(collection_name, collection_id);
    let compiled = compile_app_core(source, &bindings)?;
    let plan_profile = compiled.plan.profile.clone();
    let tree = compiled.plan.to_canonical_json();
    let qvm = super::QueryBytecodeV1::from_core_plan(compiled.plan, compiled.budget)?;
    Ok(QueryExplanation {
        plan_profile,
        // Identity of the default executable QVM, matching QueryPage::plan_hash.
        // The tree remains the canonical logical shape for human inspection.
        plan_hash: qvm.qvm_hash(),
        tree,
    })
}

/// Extract field equality constraints for index pushdown (shallow AND of `=` only).
///
/// Extracts equality conjuncts from an AND predicate. Non-equality siblings are
/// retained for final predicate evaluation and do not invalidate the equality
/// candidate superset. Parameters are resolved from `params`.
pub(crate) fn equality_constraints(
    pred: &Predicate,
    params: &BTreeMap<String, JsonValue>,
) -> Vec<(String, JsonValue)> {
    match pred {
        Predicate::True => Vec::new(),
        Predicate::Cmp {
            cmp: CompareOp::Eq,
            left,
            right,
        } => match (left, right) {
            (Operand::Path { path }, other) | (other, Operand::Path { path }) => {
                if let Some(v) = operand_as_json(other, params) {
                    vec![(path.dotted(), v)]
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        },
        Predicate::And { args } => {
            let mut out = Vec::new();
            for a in args {
                out.extend(equality_constraints(a, params));
            }
            out
        }
        _ => Vec::new(),
    }
}

/// Paths participating in comparisons in a conjunctive predicate. Constraints
/// below OR/NOT are deliberately ignored; using a sibling AND constraint is a
/// safe candidate superset because the complete predicate is re-evaluated.
pub(crate) fn constrained_paths(
    pred: &Predicate,
    params: &BTreeMap<String, JsonValue>,
) -> Vec<String> {
    match pred {
        Predicate::Cmp { left, right, .. } => match (left, right) {
            (Operand::Path { path }, other) | (other, Operand::Path { path })
                if operand_as_json(other, params).is_some() =>
            {
                vec![path.dotted()]
            }
            _ => Vec::new(),
        },
        Predicate::And { args } => args
            .iter()
            .flat_map(|predicate| constrained_paths(predicate, params))
            .collect(),
        _ => Vec::new(),
    }
}

fn operand_as_json(op: &Operand, params: &BTreeMap<String, JsonValue>) -> Option<JsonValue> {
    match op {
        Operand::Literal { value } => Some(value.clone()),
        Operand::Param { name } => params.get(name).cloned(),
        Operand::Path { .. } => None,
    }
}
