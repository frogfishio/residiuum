//! Pure closed-plan validator (`ATOMICS_SPEC` §§6, 13).
//!
//! Shared by the serial oracle and future remote admission. A refusal here
//! is a request error: no Atomic is issued and no evidence is appended.
//! Data and rule failures after accept remain the executor's job (ATM-1.2
//! does not fold preconditions into admission).

use crate::encode::account_closed_plan;
use crate::error::AtomicsError;
use crate::id::{CollectionId, HeapId};
use crate::outcome::AtomicRefuseReason;
use crate::plan::AtomicPlan;

/// Validate a closed plan against the Heap that would accept it.
///
/// Checks profile, scope, Heap binding, public-builder predicate vocabulary,
/// and applied resource ceilings. Does not evaluate data preconditions or
/// replay identity.
pub fn validate_closed_plan(plan: &AtomicPlan, bound_heap: HeapId) -> Result<(), AtomicsError> {
    if !plan.profile().execution_supported() {
        return Err(AtomicsError::Refused(
            AtomicRefuseReason::UnsupportedProfile,
        ));
    }
    if !plan.profile().execution_supports_scope(plan.scope()) {
        return Err(AtomicsError::Refused(AtomicRefuseReason::ScopeUnavailable));
    }
    if plan.heap_id() != bound_heap {
        return Err(AtomicsError::Refused(
            AtomicRefuseReason::CrossHeapCollection,
        ));
    }
    if plan
        .predicates()
        .iter()
        .any(|p| !p.kind.is_public_builder_assert())
    {
        return Err(AtomicsError::Refused(
            AtomicRefuseReason::UnsupportedPredicate,
        ));
    }
    if !plan.limits().is_within(plan.scope().hard_limits()) {
        return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded));
    }
    check_applied_limits(plan)
}

fn check_applied_limits(plan: &AtomicPlan) -> Result<(), AtomicsError> {
    let limits = plan.limits();
    let acc = account_closed_plan(plan)?;
    if acc.requested_members > limits.caller_mutations
        || acc.worst_case_generated_members > limits.total_generated_members
        || acc.requested_plan_bytes > limits.canonical_plan_bytes
        || acc.requested_value_bytes > limits.total_proposed_value_bytes
        || u32_len(plan.reads().len())? > limits.read_witnesses
        || u32_len(plan.predicates().len())? > limits.predicates
        || u32_len(plan.active_rule_revisions().len())? > limits.active_rule_revisions
        || affected_collections(plan)? > limits.affected_collections
    {
        return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded));
    }
    Ok(())
}

fn affected_collections(plan: &AtomicPlan) -> Result<u32, AtomicsError> {
    let mut ids: Vec<CollectionId> = plan.mutations().iter().map(|m| m.collection_id).collect();
    for p in plan.predicates() {
        if let Some(c) = p.collection_id {
            ids.push(c);
        }
    }
    for r in plan.reads() {
        ids.push(r.collection_id);
    }
    ids.sort_unstable();
    ids.dedup();
    u32_len(ids.len())
}

fn u32_len(n: usize) -> Result<u32, AtomicsError> {
    u32::try_from(n).map_err(|_| AtomicsError::Refused(AtomicRefuseReason::LimitExceeded))
}
