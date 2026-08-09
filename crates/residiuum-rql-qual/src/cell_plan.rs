//! Mandatory measured cell plans + concurrency matrix (programme §7.2).
//!
//! Q4.2: full plan registry (dataset + RQL intent + lifecycle + concurrency).
//! Execution against product engines remains Q4.3 / residual.

use crate::cells::{MandatoryCell, CONCURRENCY_LEVELS, OVERSUBSCRIBED_SLOT};
use crate::dataset::{
    CardinalityClass, DatasetSpec, DistributionKind, DocShape, MemoryRatio, PayloadClass,
    SelectivityClass,
};
use crate::lifecycle::{ColdMethod, LifecycleSpec};
use crate::metrics::LifecycleClass;
use serde::{Deserialize, Serialize};

/// Workload mix for mixed R/W cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadWriteMix {
    /// 90% read / 10% write.
    R90W10,
    /// 70% read / 30% write.
    R70W30,
}

impl ReadWriteMix {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::R90W10 => "rw_90_10",
            Self::R70W30 => "rw_70_30",
        }
    }

    pub fn read_pct(self) -> u8 {
        match self {
            Self::R90W10 => 90,
            Self::R70W30 => 70,
        }
    }
}

/// Index setup declaration for a cell (logical; adapters apply).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexPlan {
    pub name: String,
    pub fields: Vec<String>,
    pub required: bool,
}

/// Enrich cardinality expectation for §7.2 cell 8.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrichExpect {
    Optional,
    ExactlyOne,
    Many,
}

impl EnrichExpect {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Optional => "optional",
            Self::ExactlyOne => "exactly_one",
            Self::Many => "many",
        }
    }
}

/// Covered vs non-covered projection (§7.2 cell 5 / F11).
///
/// Covered: every projected field is present in at least one declared index.
/// Non-covered: at least one projected field is absent from all indexes (must
/// fetch base docs after index filter).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectCoverKind {
    Covered,
    NonCovered,
}

impl ProjectCoverKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Covered => "covered",
            Self::NonCovered => "non_covered",
        }
    }
}

/// Nested-field vs array-predicate focus (§7.2 cell 4 / F11).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NestedArrayFocus {
    /// DeeplyNested shape; predicate on nested.l1.l2.l3.flag only.
    NestedOnly,
    /// ArrayHeavy shape; predicate on contains(tags, …) only.
    ArrayOnly,
}

impl NestedArrayFocus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NestedOnly => "nested_only",
            Self::ArrayOnly => "array_only",
        }
    }
}

/// One runnable measured-cell plan (not yet a competitive result).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasuredCellPlan {
    pub cell: MandatoryCell,
    pub plan_id: String,
    pub dataset: DatasetSpec,
    pub lifecycle: LifecycleSpec,
    pub concurrency: u32,
    /// Host-declared oversubscribed slot (0 = not oversubscribed).
    pub oversubscribed: bool,
    /// Residiuum Application Core RQL source (intention; product path).
    pub rql_source: String,
    pub indexes: Vec<IndexPlan>,
    pub order_sensitive: bool,
    pub page_size: Option<u32>,
    /// When true, logical/product must multipage (first + deep) not first-only.
    pub require_deep_cursor: bool,
    pub rw_mix: Option<ReadWriteMix>,
    pub enrich_expect: Option<EnrichExpect>,
    /// Covered / non-covered projection declaration (cell 5 variants).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_cover: Option<ProjectCoverKind>,
    /// Nested-only vs array-only predicate focus (cell 4 variants).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nested_array_focus: Option<NestedArrayFocus>,
    pub server_lane_ineligible: bool,
    pub notes: String,
}

impl MeasuredCellPlan {
    /// Build the smoke-scale default plan for one mandatory cell.
    pub fn smoke_for(cell: MandatoryCell, seed: u64) -> Self {
        let mut dataset =
            DatasetSpec::smoke_default(seed.wrapping_add(cell.programme_index() as u64));
        let (rql, indexes, order_sensitive, page_size, rw_mix, shape, sel, card, notes) =
            cell_defaults(cell, &mut dataset);
        dataset.shape = shape;
        dataset.selectivity = sel;
        dataset.cardinality = card;

        let require_deep_cursor = matches!(cell, MandatoryCell::FirstAndDeepCursor);
        let enrich_expect = match cell {
            MandatoryCell::EnrichCardinalities => Some(EnrichExpect::Optional),
            _ => None,
        };
        // Smoke defaults: nested-only (DeeplyNested); covered projection; low group.
        let (project_cover, nested_array_focus) = match cell {
            MandatoryCell::CoveredNonCoveredProject => (Some(ProjectCoverKind::Covered), None),
            MandatoryCell::NestedAndArrayPreds => (None, Some(NestedArrayFocus::NestedOnly)),
            _ => (None, None),
        };
        Self {
            cell,
            plan_id: format!("{}_smoke_c1", cell.id()),
            dataset,
            lifecycle: LifecycleSpec::for_class(LifecycleClass::WarmSteady),
            concurrency: 1,
            oversubscribed: false,
            rql_source: rql,
            indexes,
            order_sensitive,
            page_size,
            require_deep_cursor,
            rw_mix,
            enrich_expect,
            project_cover,
            nested_array_focus,
            server_lane_ineligible: cell.server_lane_ineligible_by_default(),
            notes,
        }
    }

    /// Expand concurrency matrix for a base plan (1,2,4,8 + optional oversub).
    ///
    /// Preserves a variant stem in `plan_id` when present (everything before a
    /// trailing `_cN` / `_smoke_c1` is kept as context via `cell.id()` + optional
    /// variant label encoded already in `plan_id` when builders set it).
    pub fn with_concurrency(mut self, c: u32, oversubscribed: bool) -> Self {
        self.concurrency = c;
        self.oversubscribed = oversubscribed;
        // Prefer extending the current plan_id stem so variant plans stay unique
        // (e.g. cell_group_card_card_high_c4). Strip trailing concurrency suffixes.
        let stem = strip_concurrency_suffix(&self.plan_id);
        self.plan_id = if oversubscribed {
            format!("{stem}_c{c}_oversub")
        } else {
            format!("{stem}_c{c}")
        };
        self
    }

    pub fn with_lifecycle(mut self, life: LifecycleSpec) -> Self {
        self.lifecycle = life;
        self.plan_id = format!("{}_{}", self.plan_id, self.lifecycle.class.as_str());
        self
    }
}

fn cell_defaults(
    cell: MandatoryCell,
    dataset: &mut DatasetSpec,
) -> (
    String,
    Vec<IndexPlan>,
    bool,
    Option<u32>,
    Option<ReadWriteMix>,
    DocShape,
    SelectivityClass,
    CardinalityClass,
    String,
) {
    match cell {
        MandatoryCell::KeyGet => (
            r#"from docs where _key = "d-00000000""#.into(),
            vec![],
            false,
            None,
            None,
            DocShape::Flat,
            SelectivityClass::Point,
            CardinalityClass::High,
            "Key get by immutable _key.".into(),
        ),
        MandatoryCell::IndexedEqMultiSelectivity => {
            dataset.selectivity = SelectivityClass::S10;
            (
                r#"from docs where sel_bucket = "HIT""#.into(),
                vec![IndexPlan {
                    name: "by_sel_bucket".into(),
                    fields: vec!["sel_bucket".into()],
                    required: true,
                }],
                false,
                None,
                None,
                DocShape::Flat,
                SelectivityClass::S10,
                CardinalityClass::Medium,
                "Indexed equality; vary selectivity axis across matrix instances.".into(),
            )
        }
        MandatoryCell::RangeAndCompound => (
            r#"from docs where amount >= 100 and amount < 500 and region = "r0""#.into(),
            vec![IndexPlan {
                name: "by_region_amount".into(),
                fields: vec!["region".into(), "amount".into()],
                required: false,
            }],
            false,
            None,
            None,
            DocShape::Flat,
            SelectivityClass::S10,
            CardinalityClass::Medium,
            "Range + compound equality/range.".into(),
        ),
        MandatoryCell::NestedAndArrayPreds => (
            // Smoke default: nested-only on DeeplyNested (F11). Array-only via variants.
            r#"from docs where nested.l1.l2.l3.flag = true"#.into(),
            vec![],
            false,
            None,
            None,
            DocShape::DeeplyNested,
            SelectivityClass::Broad,
            CardinalityClass::Medium,
            "Nested-only smoke (DeeplyNested). Array-only via nested_array_predicate_variants()."
                .into(),
        ),
        MandatoryCell::CoveredNonCoveredProject => (
            // Smoke default: covered — index includes status + region (all projected fields).
            r#"from docs where status = "st-0000" project status, region"#.into(),
            vec![IndexPlan {
                name: "by_status_region".into(),
                fields: vec!["status".into(), "region".into()],
                required: true,
            }],
            false,
            None,
            None,
            DocShape::Flat,
            SelectivityClass::S10,
            CardinalityClass::Low,
            "Covered projection smoke (index covers status,region). Non-covered via projection_cover_variants()."
                .into(),
        ),
        MandatoryCell::DeterministicTopK => (
            r#"from docs order by score desc, _key asc limit 10"#.into(),
            vec![],
            true,
            None,
            None,
            DocShape::Flat,
            SelectivityClass::Broad,
            CardinalityClass::High,
            "Deterministic top-k with key tie-break.".into(),
        ),
        MandatoryCell::FirstAndDeepCursor => (
            "from docs order by _key asc".into(),
            vec![],
            true,
            Some(8),
            None,
            DocShape::Flat,
            SelectivityClass::Broad,
            CardinalityClass::High,
            "First + deep multipage continuation (page_size=8); full concat = unpaged."
                .into(),
        ),
        MandatoryCell::EnrichCardinalities => (
            "from docs enrich customer using customers matching customer_id = _key expect optional"
                .into(),
            vec![],
            false,
            None,
            None,
            DocShape::Flat,
            SelectivityClass::Broad,
            CardinalityClass::Medium,
            "Enrich optional default; variants exactly_one/many via enrich_variants()."
                .into(),
        ),
        MandatoryCell::GroupLowHighCard => {
            dataset.cardinality = CardinalityClass::Low;
            (
                "from docs group by status project status, count() as count".into(),
                vec![],
                false,
                None,
                None,
                DocShape::Flat,
                SelectivityClass::Broad,
                CardinalityClass::Low,
                "Grouping; flip cardinality axis Low/High in matrix.".into(),
            )
        }
        MandatoryCell::AggCountSumMinMaxAvg => (
            "from docs group by region project region, count() as count, sum(amount) as sum, min(amount) as min, max(amount) as max, avg(amount) as avg".into(),
            vec![],
            false,
            None,
            None,
            DocShape::Flat,
            SelectivityClass::Broad,
            CardinalityClass::Medium,
            "Aggregates count/sum/min/max/avg (logical always; product Core may refuse avg)."
                .into(),
        ),
        MandatoryCell::ConditionalComputed => (
            // Intention: high_band = amount when amount>=100 else null (logical).
            r#"from docs project amount, region, high_band"#.into(),
            vec![],
            false,
            None,
            None,
            DocShape::Flat,
            SelectivityClass::Broad,
            CardinalityClass::Medium,
            "Conditional high_band: amount if amount>=100 else null (logical computed).".into(),
        ),
        MandatoryCell::MixedReadWrite => (
            r#"from docs where sel_bucket = "HIT""#.into(),
            vec![],
            false,
            None,
            Some(ReadWriteMix::R90W10),
            DocShape::Flat,
            SelectivityClass::S10,
            CardinalityClass::Medium,
            "Mixed R/W 90/10 (and 70/30 via rw_mix_variants); logical performs writes."
                .into(),
        ),
    }
}

/// Smoke portfolio: one plan per mandatory cell @ concurrency 1, warm steady.
pub fn smoke_portfolio(seed: u64) -> Vec<MeasuredCellPlan> {
    MandatoryCell::ALL
        .iter()
        .map(|c| MeasuredCellPlan::smoke_for(*c, seed))
        .collect()
}

/// §7.2 expanded portfolio (F11): base 12 smoke + genuine variants + concurrency
/// expansion across **all** mandatory cells (not key-get-only metadata).
///
/// Inventory (approx):
/// - 12 smoke @ c1
/// - nested-only + array-only predicate plans
/// - covered + non-covered projection plans
/// - low + high group cardinality plans
/// - enrich optional/exactly_one/many
/// - R/W 90/10 + 70/30
/// - concurrency 1/2/4/8 + oversub per mandatory cell (from smoke bases)
pub fn section_7_2_expanded_portfolio(seed: u64) -> Vec<MeasuredCellPlan> {
    let mut out = smoke_portfolio(seed);
    out.extend(nested_array_predicate_variants(seed));
    out.extend(projection_cover_variants(seed));
    out.extend(group_cardinality_variants(seed));
    out.extend(enrich_variants(seed));
    out.extend(rw_mix_variants(seed));
    // Full concurrency matrix for every mandatory cell (programme §7.2).
    for cell in MandatoryCell::ALL {
        let base = MeasuredCellPlan::smoke_for(*cell, seed);
        out.extend(concurrency_matrix(&base, 2));
    }
    out
}

/// Strip trailing `_cN`, `_smoke_c1`, `_cN_oversub` so concurrency rewrites compose.
fn strip_concurrency_suffix(plan_id: &str) -> String {
    // _c{digits}_oversub
    if let Some(i) = plan_id.rfind("_c") {
        let rest = &plan_id[i + 2..];
        if rest.ends_with("_oversub") {
            let num = &rest[..rest.len() - "_oversub".len()];
            if !num.is_empty() && num.chars().all(|ch| ch.is_ascii_digit()) {
                return plan_id[..i].to_string();
            }
        } else if !rest.is_empty() && rest.chars().all(|ch| ch.is_ascii_digit()) {
            return plan_id[..i].to_string();
        }
    }
    // _smoke_c1
    if let Some(stem) = plan_id.strip_suffix("_smoke_c1") {
        return stem.to_string();
    }
    plan_id.to_string()
}

/// Separate nested-field vs array-predicate plans (F11 / §7.2 cell 4).
///
/// Smoke uses nested-only; this adds an explicit array-only plan and a second
/// nested-only labeled variant so inventory is not a single ArrayHeavy OR-query.
pub fn nested_array_predicate_variants(seed: u64) -> Vec<MeasuredCellPlan> {
    let mut nested = MeasuredCellPlan::smoke_for(MandatoryCell::NestedAndArrayPreds, seed);
    nested.nested_array_focus = Some(NestedArrayFocus::NestedOnly);
    nested.dataset.shape = DocShape::DeeplyNested;
    nested.rql_source = r#"from docs where nested.l1.l2.l3.flag = true"#.into();
    nested.plan_id = format!("{}_nested_only", MandatoryCell::NestedAndArrayPreds.id());
    nested.notes = "DeeplyNested shape; nested.l1.l2.l3.flag only (no array OR).".into();

    let mut array = MeasuredCellPlan::smoke_for(MandatoryCell::NestedAndArrayPreds, seed);
    array.nested_array_focus = Some(NestedArrayFocus::ArrayOnly);
    array.dataset.shape = DocShape::ArrayHeavy;
    array.rql_source = r#"from docs where contains(tags, "t0-0")"#.into();
    array.plan_id = format!("{}_array_only", MandatoryCell::NestedAndArrayPreds.id());
    array.notes = "ArrayHeavy shape; contains(tags, t0-0) only (no nested path).".into();

    vec![nested, array]
}

/// Covered vs non-covered projection with confirmed index/project sets (F11 / §7.2 cell 5).
pub fn projection_cover_variants(seed: u64) -> Vec<MeasuredCellPlan> {
    let mut covered = MeasuredCellPlan::smoke_for(MandatoryCell::CoveredNonCoveredProject, seed);
    covered.project_cover = Some(ProjectCoverKind::Covered);
    covered.indexes = vec![IndexPlan {
        name: "by_status_region".into(),
        fields: vec!["status".into(), "region".into()],
        required: true,
    }];
    covered.rql_source = r#"from docs where status = "st-0000" project status, region"#.into();
    covered.plan_id = format!(
        "{}_{}",
        MandatoryCell::CoveredNonCoveredProject.id(),
        ProjectCoverKind::Covered.as_str()
    );
    covered.notes = "Covered: index {status,region} includes every projected field.".into();

    let mut non = MeasuredCellPlan::smoke_for(MandatoryCell::CoveredNonCoveredProject, seed);
    non.project_cover = Some(ProjectCoverKind::NonCovered);
    non.indexes = vec![IndexPlan {
        name: "by_status".into(),
        fields: vec!["status".into()],
        required: true,
    }];
    non.rql_source = r#"from docs where status = "st-0000" project status, region"#.into();
    non.plan_id = format!(
        "{}_{}",
        MandatoryCell::CoveredNonCoveredProject.id(),
        ProjectCoverKind::NonCovered.as_str()
    );
    non.notes =
        "Non-covered: index {status} does not include projected region — base fetch required."
            .into();

    vec![covered, non]
}

/// Low- and high-cardinality grouping plans (F11 / §7.2 cell 9).
pub fn group_cardinality_variants(seed: u64) -> Vec<MeasuredCellPlan> {
    [CardinalityClass::Low, CardinalityClass::High]
        .into_iter()
        .map(|card| {
            let mut p = MeasuredCellPlan::smoke_for(MandatoryCell::GroupLowHighCard, seed);
            p.dataset.cardinality = card;
            p.plan_id = format!("{}_{}", MandatoryCell::GroupLowHighCard.id(), card.as_str());
            p.notes = format!("Group by status; cardinality={}", card.as_str());
            p
        })
        .collect()
}

/// Enrich optional / exactly_one / many plan variants.
pub fn enrich_variants(seed: u64) -> Vec<MeasuredCellPlan> {
    [
        EnrichExpect::Optional,
        EnrichExpect::ExactlyOne,
        EnrichExpect::Many,
    ]
    .into_iter()
    .map(|ex| {
        let mut p = MeasuredCellPlan::smoke_for(MandatoryCell::EnrichCardinalities, seed);
        p.enrich_expect = Some(ex);
        p.rql_source = match ex {
            EnrichExpect::Optional => {
                "from docs enrich customer using customers matching customer_id = _key expect optional"
                    .into()
            }
            EnrichExpect::ExactlyOne => {
                "from docs where present(customer_id) enrich customer using customers matching customer_id = _key expect exactly_one"
                    .into()
            }
            EnrichExpect::Many => {
                "from docs enrich customer using customers matching customer_id = id expect many".into()
            }
        };
        p.plan_id = format!("{}_{}", MandatoryCell::EnrichCardinalities.id(), ex.as_str());
        p.notes = format!("Enrich expect={}", ex.as_str());
        p
    })
    .collect()
}

/// Mixed R/W 90/10 and 70/30 variants.
pub fn rw_mix_variants(seed: u64) -> Vec<MeasuredCellPlan> {
    [ReadWriteMix::R90W10, ReadWriteMix::R70W30]
        .into_iter()
        .map(|mix| {
            let mut p = MeasuredCellPlan::smoke_for(MandatoryCell::MixedReadWrite, seed);
            p.rw_mix = Some(mix);
            p.plan_id = format!("{}_{}", MandatoryCell::MixedReadWrite.id(), mix.as_str());
            p.notes = format!("Mixed R/W {}", mix.as_str());
            p
        })
        .collect()
}

/// Concurrency matrix for one cell (levels 1,2,4,8 + one oversubscribed placeholder).
pub fn concurrency_matrix(base: &MeasuredCellPlan, oversub_factor: u32) -> Vec<MeasuredCellPlan> {
    let mut out = Vec::new();
    for &c in CONCURRENCY_LEVELS {
        out.push(base.clone().with_concurrency(c, false));
    }
    let over = CONCURRENCY_LEVELS
        .last()
        .copied()
        .unwrap_or(8)
        .saturating_mul(oversub_factor.max(2));
    let mut p = base.clone().with_concurrency(over, true);
    p.notes = format!(
        "{}; oversubscribed slot ({OVERSUBSCRIBED_SLOT}) concurrency={over}",
        p.notes
    );
    out.push(p);
    out
}

/// Literal matched by indexed-eq plans for a selectivity class.
/// Must match [`crate::generator`] `sel_bucket` emission (F3).
pub fn sel_bucket_literal(sel: SelectivityClass) -> &'static str {
    match sel {
        SelectivityClass::Point => "POINT",
        SelectivityClass::S0_01
        | SelectivityClass::S1
        | SelectivityClass::S10
        | SelectivityClass::Broad => "HIT",
    }
}

/// Selectivity matrix for indexed-eq cell (point / 0.01% / 1% / 10% / broad).
pub fn selectivity_matrix(seed: u64) -> Vec<MeasuredCellPlan> {
    SelectivityClass::ALL
        .iter()
        .enumerate()
        .map(|(i, sel)| {
            let mut p = MeasuredCellPlan::smoke_for(MandatoryCell::IndexedEqMultiSelectivity, seed);
            p.dataset.selectivity = *sel;
            p.dataset.seed = seed.wrapping_add(100 + i as u64);
            let lit = sel_bucket_literal(*sel);
            p.rql_source = format!(r#"from docs where sel_bucket = "{lit}""#);
            p.plan_id = format!(
                "{}_{}",
                MandatoryCell::IndexedEqMultiSelectivity.id(),
                sel.as_str()
            );
            p.notes = format!("Indexed eq selectivity={} predicate={lit}", sel.as_str());
            p
        })
        .collect()
}

/// Lifecycle matrix for key-get smoke (all §7.3 classes).
pub fn lifecycle_matrix(seed: u64) -> Vec<MeasuredCellPlan> {
    LifecycleSpec::all_programme_classes()
        .into_iter()
        .map(|life| {
            let mut p = MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, seed);
            // Larger-than-memory uses R400 scale identity.
            if life.class == LifecycleClass::LargerThanMemory {
                p.dataset.memory_ratio = MemoryRatio::R400;
                p.dataset = p.dataset.clone().with_scaled_docs(64);
            }
            p.with_lifecycle(life)
        })
        .collect()
}

/// Machine report for Q4.2 labor evidence.
pub fn q4_2_report_json() -> serde_json::Value {
    let smoke = smoke_portfolio(0x04_42);
    let expanded = section_7_2_expanded_portfolio(0x04_42);
    let conc = concurrency_matrix(&smoke[0], 2);
    let sel = selectivity_matrix(0x04_42);
    let life = lifecycle_matrix(0x04_42);
    let nested_v = nested_array_predicate_variants(0x04_42);
    let proj_v = projection_cover_variants(0x04_42);
    let group_v = group_cardinality_variants(0x04_42);
    let cells_with_c8: Vec<String> = expanded
        .iter()
        .filter(|p| p.concurrency == 8)
        .map(|p| p.cell.id().to_string())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut variant_plan_ids: Vec<String> = nested_v
        .iter()
        .chain(proj_v.iter())
        .chain(group_v.iter())
        .map(|p| p.plan_id.clone())
        .collect();
    variant_plan_ids.sort();
    serde_json::json!({
        "format": "residiuum-rql-q4-2-dataset-cells-report-v1",
        "harness_profile": crate::HARNESS_PROFILE,
        "summary": {
            "mandatory_cells": MandatoryCell::ALL.len(),
            "smoke_plans": smoke.len(),
            "section_7_2_expanded_plans": expanded.len(),
            "concurrency_matrix_len": conc.len(),
            "selectivity_matrix_len": sel.len(),
            "lifecycle_matrix_len": life.len(),
            "nested_array_variants": nested_v.len(),
            "projection_cover_variants": proj_v.len(),
            "group_cardinality_variants": group_v.len(),
            "concurrency_levels": CONCURRENCY_LEVELS,
            "oversubscribed_slot": OVERSUBSCRIBED_SLOT,
            "concurrency_c8_cells": cells_with_c8,
            "concurrency_expanded_all_cells": (cells_with_c8.len() == MandatoryCell::ALL.len()),
            "cold_reopen_claims_device_cold": false,
            "default_cold_method_reopen": ColdMethod::StoreReopen.as_str(),
            "f2_real_cells": true,
            "f11_plan_variants": true,
        },
        "payload_classes": PayloadClass::PRIMARY.iter().map(|p| p.as_str()).collect::<Vec<_>>(),
        "shapes": DocShape::ALL.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        "distributions": DistributionKind::ALL.iter().map(|d| d.as_str()).collect::<Vec<_>>(),
        "smoke_plan_ids": smoke.iter().map(|p| p.plan_id.clone()).collect::<Vec<_>>(),
        "variant_plan_ids": variant_plan_ids,
        "non_claims": [
            "not_gate1",
            "not_competitive",
            "not_q4_package_accept",
            "execute_residual_q4_3"
        ],
        "authority": "doc/todo/rql/RQL_Q4_2_DATASET_CELLS.md",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::generate_dataset;
    use crate::lifecycle::validate_cold_claim;
    use std::path::PathBuf;

    #[test]
    fn smoke_portfolio_twelve_plans() {
        let plans = smoke_portfolio(1);
        assert_eq!(plans.len(), 12);
        for p in &plans {
            assert!(!p.rql_source.is_empty());
            assert_eq!(p.concurrency, 1);
            validate_cold_claim(&p.lifecycle).unwrap();
        }
        let enrich = plans
            .iter()
            .find(|p| p.cell == MandatoryCell::EnrichCardinalities)
            .unwrap();
        assert!(!enrich.server_lane_ineligible);
        let cursor = plans
            .iter()
            .find(|p| p.cell == MandatoryCell::FirstAndDeepCursor)
            .unwrap();
        assert!(cursor.require_deep_cursor);
        let agg = plans
            .iter()
            .find(|p| p.cell == MandatoryCell::AggCountSumMinMaxAvg)
            .unwrap();
        assert!(agg.rql_source.contains("avg"));
        let cond = plans
            .iter()
            .find(|p| p.cell == MandatoryCell::ConditionalComputed)
            .unwrap();
        assert!(cond.rql_source.contains("high_band"));
    }

    #[test]
    fn section_7_2_expanded_includes_variants() {
        let plans = section_7_2_expanded_portfolio(1);
        // 12 smoke + 2 nested/array + 2 project + 2 group + 3 enrich + 2 rw
        // + 12 cells × 5 concurrency = 12+2+2+2+3+2+60 = 83
        assert!(
            plans.len() >= 80,
            "expected full F11 inventory ≥80, got {}",
            plans.len()
        );
        assert!(plans.iter().any(|p| p.plan_id.contains("exactly_one")));
        assert!(plans.iter().any(|p| p.plan_id.contains("many")));
        assert!(plans.iter().any(|p| p.plan_id.contains("rw_70_30")));
        assert!(plans.iter().any(|p| p.plan_id.contains("nested_only")));
        assert!(plans.iter().any(|p| p.plan_id.contains("array_only")));
        assert!(plans.iter().any(|p| p.plan_id.contains("non_covered")));
        assert!(plans.iter().any(|p| p.plan_id.contains("card_high")));
        assert!(plans.iter().any(|p| p.concurrency == 8));
        // Concurrency expansion is not key-get-only.
        let c8_cells: std::collections::BTreeSet<&str> = plans
            .iter()
            .filter(|p| p.concurrency == 8)
            .map(|p| p.cell.id())
            .collect();
        assert_eq!(
            c8_cells.len(),
            MandatoryCell::ALL.len(),
            "every mandatory cell must have a c8 plan"
        );
    }

    #[test]
    fn nested_array_variants_honest_shapes() {
        let v = nested_array_predicate_variants(3);
        assert_eq!(v.len(), 2);
        let nested = v
            .iter()
            .find(|p| p.plan_id.contains("nested_only"))
            .unwrap();
        assert_eq!(nested.dataset.shape, DocShape::DeeplyNested);
        assert_eq!(
            nested.nested_array_focus,
            Some(NestedArrayFocus::NestedOnly)
        );
        assert!(!nested.rql_source.contains("tags"));
        let array = v.iter().find(|p| p.plan_id.contains("array_only")).unwrap();
        assert_eq!(array.dataset.shape, DocShape::ArrayHeavy);
        assert_eq!(array.nested_array_focus, Some(NestedArrayFocus::ArrayOnly));
        assert!(!array.rql_source.contains("nested"));
    }

    #[test]
    fn projection_cover_variants_confirm_indexes() {
        let v = projection_cover_variants(3);
        let cov = v
            .iter()
            .find(|p| p.project_cover == Some(ProjectCoverKind::Covered))
            .unwrap();
        let idx: std::collections::BTreeSet<_> = cov
            .indexes
            .iter()
            .flat_map(|i| i.fields.iter().cloned())
            .collect();
        assert!(idx.contains("status") && idx.contains("region"));
        let non = v
            .iter()
            .find(|p| p.project_cover == Some(ProjectCoverKind::NonCovered))
            .unwrap();
        let idx: std::collections::BTreeSet<_> = non
            .indexes
            .iter()
            .flat_map(|i| i.fields.iter().cloned())
            .collect();
        assert!(idx.contains("status"));
        assert!(
            !idx.contains("region"),
            "non-covered must omit region from index"
        );
    }

    #[test]
    fn group_cardinality_variants_low_high() {
        let v = group_cardinality_variants(5);
        assert_eq!(v.len(), 2);
        assert!(v
            .iter()
            .any(|p| p.dataset.cardinality == CardinalityClass::Low));
        assert!(v
            .iter()
            .any(|p| p.dataset.cardinality == CardinalityClass::High));
    }

    #[test]
    fn point_selectivity_plan_matches_generator_literal() {
        use crate::engine::{EngineAdapter, LogicalHarnessEngine};
        use crate::generator::generate_dataset;
        use crate::shared_work::SharedLogicalWork;

        let matrix = selectivity_matrix(9);
        let point = matrix
            .iter()
            .find(|p| p.dataset.selectivity == SelectivityClass::Point)
            .expect("point plan");
        assert!(
            point.rql_source.contains(r#"sel_bucket = "POINT""#),
            "plan query must use POINT: {}",
            point.rql_source
        );
        assert!(!point.rql_source.contains(r#""HIT""#));

        let ds = generate_dataset(&point.dataset);
        let work = SharedLogicalWork::from_dataset(ds);
        let mut eng = LogicalHarnessEngine::new();
        eng.load_shared_work(&work).unwrap();
        let out = eng.execute_plan(point).unwrap();
        let r = out.result.expect("result");
        assert_eq!(
            r.row_count, 1,
            "point selectivity must return exactly one row"
        );
    }

    #[test]
    fn concurrency_matrix_five_slots() {
        let base = MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 9);
        let m = concurrency_matrix(&base, 2);
        assert_eq!(m.len(), 5); // 1,2,4,8 + oversub
        assert!(m.iter().any(|p| p.oversubscribed));
        assert_eq!(m[0].concurrency, 1);
        assert_eq!(m[3].concurrency, 8);
    }

    #[test]
    fn generator_matches_indexed_eq_plan() {
        let p = MeasuredCellPlan::smoke_for(MandatoryCell::IndexedEqMultiSelectivity, 3);
        let ds = generate_dataset(&p.dataset);
        assert_eq!(ds.collections["docs"].len() as u64, p.dataset.doc_count);
        assert!(!ds.content_hash.is_empty());
    }

    #[test]
    fn write_q4_2_report() {
        // F8: default → target/; RESIDIUUM_WRITE_SPEC_EVIDENCE=1 also writes spec/.
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let report = q4_2_report_json();
        let body = serde_json::to_string_pretty(&report).unwrap();
        let path = crate::evidence::write_evidence_artifact(
            root.join("target/rql-q4/q4_2_dataset_cells_report.json"),
            root.join("spec/rql/qualification/harness-v1/q4_2_dataset_cells_report.json"),
            &body,
        )
        .expect("write q4.2 report");
        assert!(path.is_file());
        assert_eq!(report["summary"]["mandatory_cells"], 12);
        assert_eq!(report["summary"]["smoke_plans"], 12);
    }

    #[test]
    fn selectivity_matrix_five() {
        assert_eq!(selectivity_matrix(0).len(), 5);
    }

    #[test]
    fn lifecycle_matrix_seven() {
        assert_eq!(lifecycle_matrix(0).len(), 7);
    }
}
