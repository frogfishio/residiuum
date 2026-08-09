//! Mandatory measured cell registry (programme §7.2).
//!
//! Plans, generators, and matrices: [`crate::cell_plan`], [`crate::dataset`],
//! [`crate::generator`].

use serde::{Deserialize, Serialize};

/// Closed set of mandatory measured cells for Gate-1 portfolio.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MandatoryCell {
    KeyGet,
    IndexedEqMultiSelectivity,
    RangeAndCompound,
    NestedAndArrayPreds,
    CoveredNonCoveredProject,
    DeterministicTopK,
    FirstAndDeepCursor,
    EnrichCardinalities,
    GroupLowHighCard,
    AggCountSumMinMaxAvg,
    ConditionalComputed,
    MixedReadWrite,
}

impl MandatoryCell {
    pub const ALL: &'static [MandatoryCell] = &[
        Self::KeyGet,
        Self::IndexedEqMultiSelectivity,
        Self::RangeAndCompound,
        Self::NestedAndArrayPreds,
        Self::CoveredNonCoveredProject,
        Self::DeterministicTopK,
        Self::FirstAndDeepCursor,
        Self::EnrichCardinalities,
        Self::GroupLowHighCard,
        Self::AggCountSumMinMaxAvg,
        Self::ConditionalComputed,
        Self::MixedReadWrite,
    ];

    pub fn id(self) -> &'static str {
        match self {
            Self::KeyGet => "cell_key_get",
            Self::IndexedEqMultiSelectivity => "cell_indexed_eq_multi_sel",
            Self::RangeAndCompound => "cell_range_compound",
            Self::NestedAndArrayPreds => "cell_nested_array_pred",
            Self::CoveredNonCoveredProject => "cell_project_covered",
            Self::DeterministicTopK => "cell_top_k",
            Self::FirstAndDeepCursor => "cell_cursor_first_deep",
            Self::EnrichCardinalities => "cell_enrich_card",
            Self::GroupLowHighCard => "cell_group_card",
            Self::AggCountSumMinMaxAvg => "cell_agg_stats",
            Self::ConditionalComputed => "cell_conditional_computed",
            Self::MixedReadWrite => "cell_mixed_rw",
        }
    }

    pub fn programme_index(self) -> u8 {
        match self {
            Self::KeyGet => 1,
            Self::IndexedEqMultiSelectivity => 2,
            Self::RangeAndCompound => 3,
            Self::NestedAndArrayPreds => 4,
            Self::CoveredNonCoveredProject => 5,
            Self::DeterministicTopK => 6,
            Self::FirstAndDeepCursor => 7,
            Self::EnrichCardinalities => 8,
            Self::GroupLowHighCard => 9,
            Self::AggCountSumMinMaxAvg => 10,
            Self::ConditionalComputed => 11,
            Self::MixedReadWrite => 12,
        }
    }

    /// Whether the cell is structurally excluded from the server lane.
    ///
    /// Bounded Full RQL is now available over op 118, so no mandatory cell is
    /// excluded merely for using enrich.
    pub fn server_lane_ineligible_by_default(self) -> bool {
        false
    }
}

/// Declared concurrency levels for scaling matrix (programme §7.2).
pub const CONCURRENCY_LEVELS: &[u32] = &[1, 2, 4, 8];

/// Placeholder for the single oversubscribed concurrency (host-specific; set in campaign).
pub const OVERSUBSCRIBED_SLOT: &str = "oversubscribed_host_declared";

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn twelve_mandatory_cells_unique_ids() {
        assert_eq!(MandatoryCell::ALL.len(), 12);
        let ids: BTreeSet<_> = MandatoryCell::ALL.iter().map(|c| c.id()).collect();
        assert_eq!(ids.len(), 12);
        for (i, c) in MandatoryCell::ALL.iter().enumerate() {
            assert_eq!(c.programme_index() as usize, i + 1);
        }
    }
}
