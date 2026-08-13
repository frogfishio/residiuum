//! Resource ceilings (`ATOMICS_SPEC` §13).
//!
//! Hard ceilings are a protocol freeze. Heap policy may lower them and must
//! record the applied limits in the prepare. Raising one requires a new profile.

use std::time::Duration;

/// Applied semantic limits bound into a plan and prepare.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceLimits {
    /// Maximum caller mutations in the plan.
    pub caller_mutations: u32,
    /// Maximum generated members including RRE consequences.
    pub total_generated_members: u32,
    /// Maximum canonical plan bytes.
    pub canonical_plan_bytes: u32,
    /// Maximum total proposed value bytes.
    pub total_proposed_value_bytes: u32,
    /// Maximum read witnesses.
    pub read_witnesses: u32,
    /// Maximum predicates.
    pub predicates: u32,
    /// Maximum distinct affected collections.
    pub affected_collections: u32,
    /// Maximum active rule revisions named by the plan.
    pub active_rule_revisions: u32,
    /// Construction deadline.
    pub construction_deadline: Duration,
    /// Maximum emitted violations.
    pub emitted_violations: u32,
}

impl ResourceLimits {
    /// V1 Key-scope hard ceilings.
    pub const fn hard_key() -> Self {
        Self {
            caller_mutations: 1,
            total_generated_members: 32,
            canonical_plan_bytes: 256 * 1024,
            total_proposed_value_bytes: 4 * 1024 * 1024,
            read_witnesses: 64,
            predicates: 32,
            affected_collections: 1,
            active_rule_revisions: 64,
            construction_deadline: Duration::from_secs(2),
            emitted_violations: 1024,
        }
    }

    /// V1 LocalHeap and Partition hard ceilings.
    pub const fn hard_local_heap() -> Self {
        Self {
            caller_mutations: 256,
            total_generated_members: 4096,
            canonical_plan_bytes: 1024 * 1024,
            total_proposed_value_bytes: 8 * 1024 * 1024,
            read_witnesses: 4096,
            predicates: 1024,
            affected_collections: 64,
            active_rule_revisions: 1024,
            construction_deadline: Duration::from_secs(5),
            emitted_violations: 1024,
        }
    }

    /// Partition shares LocalHeap hard ceilings until a separate profile ships.
    pub const fn hard_partition() -> Self {
        Self::hard_local_heap()
    }

    /// Public LocalHeap v1 builder defaults (stricter than the hard ceilings).
    pub const fn builder_defaults_local_heap() -> Self {
        Self {
            caller_mutations: 64,
            total_generated_members: 4096,
            canonical_plan_bytes: 512 * 1024,
            total_proposed_value_bytes: 4 * 1024 * 1024,
            read_witnesses: 4096,
            predicates: 1024,
            affected_collections: 16,
            active_rule_revisions: 1024,
            construction_deadline: Duration::from_secs(5),
            emitted_violations: 1024,
        }
    }

    /// True when every field is at or below `other` (used to detect raises).
    pub const fn is_within(self, other: Self) -> bool {
        self.caller_mutations <= other.caller_mutations
            && self.total_generated_members <= other.total_generated_members
            && self.canonical_plan_bytes <= other.canonical_plan_bytes
            && self.total_proposed_value_bytes <= other.total_proposed_value_bytes
            && self.read_witnesses <= other.read_witnesses
            && self.predicates <= other.predicates
            && self.affected_collections <= other.affected_collections
            && self.active_rule_revisions <= other.active_rule_revisions
            && duration_leq(self.construction_deadline, other.construction_deadline)
            && self.emitted_violations <= other.emitted_violations
    }
}

const fn duration_leq(a: Duration, b: Duration) -> bool {
    a.as_nanos() <= b.as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_hard_ceilings_match_spec() {
        let k = ResourceLimits::hard_key();
        assert_eq!(k.caller_mutations, 1);
        assert_eq!(k.total_generated_members, 32);
        assert_eq!(k.canonical_plan_bytes, 256 * 1024);
        assert_eq!(k.total_proposed_value_bytes, 4 * 1024 * 1024);
        assert_eq!(k.read_witnesses, 64);
        assert_eq!(k.predicates, 32);
        assert_eq!(k.affected_collections, 1);
        assert_eq!(k.active_rule_revisions, 64);
        assert_eq!(k.construction_deadline, Duration::from_secs(2));
        assert_eq!(k.emitted_violations, 1024);
    }

    #[test]
    fn local_heap_hard_ceilings_match_spec() {
        let h = ResourceLimits::hard_local_heap();
        assert_eq!(h.caller_mutations, 256);
        assert_eq!(h.total_generated_members, 4096);
        assert_eq!(h.canonical_plan_bytes, 1024 * 1024);
        assert_eq!(h.total_proposed_value_bytes, 8 * 1024 * 1024);
        assert_eq!(h.read_witnesses, 4096);
        assert_eq!(h.predicates, 1024);
        assert_eq!(h.affected_collections, 64);
        assert_eq!(h.active_rule_revisions, 1024);
        assert_eq!(h.construction_deadline, Duration::from_secs(5));
        assert_eq!(h.emitted_violations, 1024);
        assert_eq!(ResourceLimits::hard_partition(), h);
    }

    #[test]
    fn builder_defaults_are_stricter_or_equal() {
        let d = ResourceLimits::builder_defaults_local_heap();
        assert!(d.is_within(ResourceLimits::hard_local_heap()));
        assert_eq!(d.caller_mutations, 64);
        assert_eq!(d.canonical_plan_bytes, 512 * 1024);
        assert_eq!(d.total_proposed_value_bytes, 4 * 1024 * 1024);
        assert_eq!(d.affected_collections, 16);
        assert_eq!(d.construction_deadline, Duration::from_secs(5));
    }
}
