//! CSQ-2 ordered-pair / t-wise composed-failure scheduler.
//!
//! Loads `failure-combinations-v1.json` and produces a deterministic schedule
//! (or a checked rejection) for each registered combination. Full multi-fault
//! campaigns run under CSQ-5; this module owns feasibility + CI subset
//! materialization so unreachable/unscheduled pairs cannot silently pass.

use serde::Deserialize;
use std::collections::HashSet;

/// Embedded combinations registry (workspace path resolved by tests / loaders).
#[derive(Debug, Clone, Deserialize)]
pub struct FailureCombinationDoc {
    /// Schema id (must contain `failure-combinations`).
    pub schema: String,
    /// Registered combination cells.
    pub items: Vec<FailureCombination>,
}

/// One ordered failure combination cell.
#[derive(Debug, Clone, Deserialize)]
pub struct FailureCombination {
    /// Stable combination id (e.g. `CSQ-FC-io_error+enospc`).
    pub id: String,
    /// Failure-class ids from `failures-v1.json`.
    pub failures: Vec<String>,
    /// Injection order (defaults to `failures` when empty).
    #[serde(default)]
    pub order: Vec<String>,
    /// Suite id that owns execution of this cell.
    pub executable_owner: String,
    /// `scheduled` | `rejected` | `infeasible`
    pub feasibility: String,
    /// Required when feasibility is rejected/infeasible.
    #[serde(default)]
    pub rejection_reason: Option<String>,
    /// Optional CI flag (default true for small registries).
    #[serde(default = "default_true")]
    pub ci_subset: bool,
}

fn default_true() -> bool {
    true
}

/// Outcome of scheduling one combination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleDecision {
    /// Run faults in this order under `owner` suite.
    Run {
        /// Combination id.
        id: String,
        /// Ordered failure-class ids to inject.
        order: Vec<String>,
        /// Executable suite owner.
        owner: String,
    },
    /// Explicitly not run; reason is mandatory and checked.
    Reject {
        /// Combination id.
        id: String,
        /// Human-readable rejection / infeasibility reason.
        reason: String,
    },
}

/// Validate structural rules for the combination registry.
pub fn validate_combinations(doc: &FailureCombinationDoc) -> Result<(), String> {
    if !doc.schema.contains("failure-combinations") {
        return Err(format!("unexpected schema {}", doc.schema));
    }
    if doc.items.is_empty() {
        return Err("failure-combinations registry is empty".into());
    }
    let mut ids = HashSet::new();
    for c in &doc.items {
        if c.id.is_empty() {
            return Err("combination with empty id".into());
        }
        if !ids.insert(c.id.clone()) {
            return Err(format!("duplicate combination id {}", c.id));
        }
        if c.failures.len() < 2 {
            return Err(format!("{} must list at least two failures", c.id));
        }
        if c.executable_owner.is_empty() {
            return Err(format!("{} missing executable_owner", c.id));
        }
        match c.feasibility.as_str() {
            "scheduled" => {
                let order = if c.order.is_empty() {
                    &c.failures
                } else {
                    &c.order
                };
                if order.len() != c.failures.len() {
                    return Err(format!(
                        "{} order length must match failures for scheduled cells",
                        c.id
                    ));
                }
                for f in order {
                    if !c.failures.iter().any(|x| x == f) {
                        return Err(format!("{} order references unknown failure {f}", c.id));
                    }
                }
            }
            "rejected" | "infeasible" => {
                if c.rejection_reason
                    .as_deref()
                    .unwrap_or("")
                    .trim()
                    .is_empty()
                {
                    return Err(format!(
                        "{} feasibility={} requires rejection_reason",
                        c.id, c.feasibility
                    ));
                }
            }
            other => {
                return Err(format!(
                    "{} unknown feasibility {other:?} (want scheduled|rejected|infeasible)",
                    c.id
                ));
            }
        }
    }
    Ok(())
}

/// Materialize schedule decisions for every combination (or CI subset).
pub fn schedule(
    doc: &FailureCombinationDoc,
    ci_only: bool,
) -> Result<Vec<ScheduleDecision>, String> {
    validate_combinations(doc)?;
    let mut out = Vec::new();
    for c in &doc.items {
        if ci_only && !c.ci_subset {
            continue;
        }
        match c.feasibility.as_str() {
            "scheduled" => {
                let order = if c.order.is_empty() {
                    c.failures.clone()
                } else {
                    c.order.clone()
                };
                out.push(ScheduleDecision::Run {
                    id: c.id.clone(),
                    order,
                    owner: c.executable_owner.clone(),
                });
            }
            "rejected" | "infeasible" => {
                out.push(ScheduleDecision::Reject {
                    id: c.id.clone(),
                    reason: c
                        .rejection_reason
                        .clone()
                        .unwrap_or_else(|| "unspecified".into()),
                });
            }
            other => return Err(format!("unreachable feasibility {other}")),
        }
    }
    if out.is_empty() {
        return Err("composed-failure schedule is empty".into());
    }
    Ok(out)
}

/// Map a failure-class id to a default [`crate::failpoint::Action`] for drivers.
pub fn failure_class_action(class: &str) -> Option<crate::failpoint::Action> {
    use crate::failpoint::Action;
    Some(match class {
        "process_abort" => Action::Abort,
        "io_error" | "error" => Action::Error,
        "short_write" => Action::ShortWrite,
        "enospc" => Action::IoEnospc,
        "permission" => Action::IoPermission,
        "eio" => Action::Error,
        "cancellation" => Action::Cancel,
        "resource_exhaustion" => Action::AllocFail,
        "corruption" | "clock_discontinuity" | "writer_contention" => return None,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_doc() -> FailureCombinationDoc {
        FailureCombinationDoc {
            schema: "residiuum-csq-failure-combinations-v1".into(),
            items: vec![
                FailureCombination {
                    id: "CSQ-FC-a+b".into(),
                    failures: vec!["io_error".into(), "enospc".into()],
                    order: vec!["io_error".into(), "enospc".into()],
                    executable_owner: "CSQ-SUITE-CRASH".into(),
                    feasibility: "scheduled".into(),
                    rejection_reason: None,
                    ci_subset: true,
                },
                FailureCombination {
                    id: "CSQ-FC-bad".into(),
                    failures: vec!["corruption".into(), "clock_discontinuity".into()],
                    order: vec![],
                    executable_owner: "CSQ-SUITE-CRASH".into(),
                    feasibility: "rejected".into(),
                    rejection_reason: Some("no joint injection harness yet".into()),
                    ci_subset: true,
                },
            ],
        }
    }

    #[test]
    fn schedules_and_rejects() {
        let doc = sample_doc();
        let s = schedule(&doc, true).expect("schedule");
        assert_eq!(s.len(), 2);
        assert!(matches!(&s[0], ScheduleDecision::Run { id, .. } if id == "CSQ-FC-a+b"));
        assert!(matches!(&s[1], ScheduleDecision::Reject { id, .. } if id == "CSQ-FC-bad"));
    }

    #[test]
    fn rejected_without_reason_fails() {
        let mut doc = sample_doc();
        doc.items[1].rejection_reason = None;
        assert!(validate_combinations(&doc).is_err());
    }
}
