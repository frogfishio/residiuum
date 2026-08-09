//! Dataset axes for RQL-Q4 measured cells (programme §7.1).
//!
//! Logical generators share these axes across engines; host memory only scales
//! document **count**, never the axis ids.

use serde::{Deserialize, Serialize};

/// Document shape class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocShape {
    Flat,
    DeeplyNested,
    SparseHeterogeneous,
    ArrayHeavy,
}

impl DocShape {
    pub const ALL: &'static [DocShape] = &[
        Self::Flat,
        Self::DeeplyNested,
        Self::SparseHeterogeneous,
        Self::ArrayHeavy,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::DeeplyNested => "deeply_nested",
            Self::SparseHeterogeneous => "sparse_heterogeneous",
            Self::ArrayHeavy => "array_heavy",
        }
    }
}

/// Approximate logical payload size class (plus optional heavy tail flag).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadClass {
    Approx1KiB,
    Approx8KiB,
    Approx64KiB,
    /// Seeded heavy tail document (one or few oversized docs in the set).
    HeavyTail,
}

impl PayloadClass {
    pub const PRIMARY: &'static [PayloadClass] =
        &[Self::Approx1KiB, Self::Approx8KiB, Self::Approx64KiB];

    pub fn target_bytes(self) -> usize {
        match self {
            Self::Approx1KiB => 1024,
            Self::Approx8KiB => 8 * 1024,
            Self::Approx64KiB => 64 * 1024,
            Self::HeavyTail => 256 * 1024,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Approx1KiB => "approx_1kib",
            Self::Approx8KiB => "approx_8kib",
            Self::Approx64KiB => "approx_64kib",
            Self::HeavyTail => "heavy_tail",
        }
    }
}

/// Working-set size relative to controlled-host memory capacity (ratio identity is stable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryRatio {
    R25,
    R100,
    R400,
}

impl MemoryRatio {
    pub const ALL: &'static [MemoryRatio] = &[Self::R25, Self::R100, Self::R400];

    pub fn percent(self) -> u32 {
        match self {
            Self::R25 => 25,
            Self::R100 => 100,
            Self::R400 => 400,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::R25 => "mem_25pct",
            Self::R100 => "mem_100pct",
            Self::R400 => "mem_400pct",
        }
    }
}

/// Key / value access distribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributionKind {
    Uniform,
    ZipfHotKey,
    TimeOrdered,
}

impl DistributionKind {
    pub const ALL: &'static [DistributionKind] =
        &[Self::Uniform, Self::ZipfHotKey, Self::TimeOrdered];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::ZipfHotKey => "zipf_hot_key",
            Self::TimeOrdered => "time_ordered",
        }
    }
}

/// Field cardinality class for group / index cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CardinalityClass {
    Low,
    Medium,
    High,
}

impl CardinalityClass {
    pub const ALL: &'static [CardinalityClass] = &[Self::Low, Self::Medium, Self::High];

    /// Distinct values relative to document count (approx).
    pub fn distinct_ratio(self) -> f64 {
        match self {
            Self::Low => 0.01,
            Self::Medium => 0.10,
            Self::High => 0.80,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "card_low",
            Self::Medium => "card_medium",
            Self::High => "card_high",
        }
    }
}

/// Selectivity class for equality / range filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectivityClass {
    Point,
    S0_01,
    S1,
    S10,
    Broad,
}

impl SelectivityClass {
    pub const ALL: &'static [SelectivityClass] =
        &[Self::Point, Self::S0_01, Self::S1, Self::S10, Self::Broad];

    /// Approximate fraction of documents matching (point ≈ 1/n).
    pub fn approx_fraction(self) -> f64 {
        match self {
            Self::Point => 0.0, // exactly one key — fraction = 1/n at runtime
            Self::S0_01 => 0.0001,
            Self::S1 => 0.01,
            Self::S10 => 0.10,
            Self::Broad => 0.50,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Point => "sel_point",
            Self::S0_01 => "sel_0_01pct",
            Self::S1 => "sel_1pct",
            Self::S10 => "sel_10pct",
            Self::Broad => "sel_broad",
        }
    }
}

/// Full dataset axis specification for a measured cell instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetSpec {
    pub shape: DocShape,
    pub payload: PayloadClass,
    pub memory_ratio: MemoryRatio,
    pub distribution: DistributionKind,
    pub cardinality: CardinalityClass,
    pub selectivity: SelectivityClass,
    /// Document count for this host scale (scaled; identity of axes unchanged).
    pub doc_count: u64,
    pub seed: u64,
    /// Include one heavy-tail document when true.
    pub include_heavy_tail: bool,
}

impl DatasetSpec {
    /// Default smoke-scale dataset (CI-friendly; not a Q5 campaign size).
    pub fn smoke_default(seed: u64) -> Self {
        Self {
            shape: DocShape::Flat,
            payload: PayloadClass::Approx1KiB,
            memory_ratio: MemoryRatio::R25,
            distribution: DistributionKind::Uniform,
            cardinality: CardinalityClass::Medium,
            selectivity: SelectivityClass::S10,
            doc_count: 64,
            seed,
            include_heavy_tail: false,
        }
    }

    /// Scale `base_docs` by memory ratio for campaign-sized runs.
    /// Host may cap; axes remain identical.
    pub fn with_scaled_docs(mut self, base_docs_at_100pct: u64) -> Self {
        let factor = self.memory_ratio.percent() as f64 / 100.0;
        self.doc_count = ((base_docs_at_100pct as f64) * factor).max(1.0) as u64;
        self
    }

    pub fn axis_fingerprint(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}|docs={}|seed={}|ht={}",
            self.shape.as_str(),
            self.payload.as_str(),
            self.memory_ratio.as_str(),
            self.distribution.as_str(),
            self.cardinality.as_str(),
            self.selectivity.as_str(),
            self.doc_count,
            self.seed,
            self.include_heavy_tail
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn axes_closed_sets() {
        assert_eq!(DocShape::ALL.len(), 4);
        assert_eq!(PayloadClass::PRIMARY.len(), 3);
        assert_eq!(MemoryRatio::ALL.len(), 3);
        assert_eq!(DistributionKind::ALL.len(), 3);
        assert_eq!(CardinalityClass::ALL.len(), 3);
        assert_eq!(SelectivityClass::ALL.len(), 5);
    }

    #[test]
    fn memory_ratio_scales_docs() {
        let s = DatasetSpec::smoke_default(1).with_scaled_docs(1000);
        // smoke_default has R25 → 250
        assert_eq!(s.doc_count, 250);
        let mut s2 = DatasetSpec::smoke_default(1);
        s2.memory_ratio = MemoryRatio::R400;
        let s2 = s2.with_scaled_docs(1000);
        assert_eq!(s2.doc_count, 4000);
    }
}
