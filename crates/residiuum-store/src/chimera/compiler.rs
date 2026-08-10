//! Background compiler plans (FINAL DESIGN “Background compiler”).
//!
//! GC + relocation + reclustering + dictionary training + hot/cold migration +
//! lifetime-aware placement (+ optional dual representation later).
//!
//! This module **plans** work; it does not mutate the live store. The store
//! write path and a future compiler worker execute [`CompilerOp`]s under
//! generation-safe locator swaps.

use super::classify::{LocatorKind, TemperatureClass, ValueClass};
use super::ValueLocator;

/// One planned physical rewrite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompilerOp {
    /// Drop unreachable slots / log records after epoch reclaim.
    Gc {
        /// Generation that becomes reclaimable.
        reclaim_generation: u32,
    },
    /// Move a live record to a new physical placement; bump generation.
    Relocate {
        /// Subject key (diagnostic / plan key).
        key: Vec<u8>,
        /// Current locator.
        from: ValueLocator,
        /// Desired placement kind after move.
        to_kind: LocatorKind,
        /// New generation for the relocated copy.
        new_generation: u32,
    },
    /// Recluster a key range into point containers or scan extents.
    Recluster {
        /// Inclusive start key (empty = start).
        range_start: Vec<u8>,
        /// Exclusive end key (empty = end).
        range_end: Vec<u8>,
        /// Target kind for the cluster.
        target: LocatorKind,
        /// New generation for rewritten containers/extents.
        new_generation: u32,
    },
    /// Train / refresh a shared dictionary for a compressible family.
    DictionaryTrain {
        /// Family id (operator or auto-assigned).
        family_id: u32,
        /// Sample keys used for training (opaque to the planner).
        sample_keys: Vec<Vec<u8>>,
    },
    /// Migrate between temperature zones.
    HotColdMigrate {
        /// Subject key.
        key: Vec<u8>,
        /// From temperature.
        from: TemperatureClass,
        /// To temperature.
        to: TemperatureClass,
        /// New generation after migration.
        new_generation: u32,
    },
    /// Lifetime-aware zone re-placement (short vs long lived zones).
    LifetimePlace {
        /// Subject key.
        key: Vec<u8>,
        /// Target short-lived zone when true; long-lived when false.
        short_lived_zone: bool,
        /// New generation.
        new_generation: u32,
    },
    /// Optional dual representation (point + scan copies) — deferred execute.
    DualRepresent {
        /// Subject key.
        key: Vec<u8>,
        /// Keep/create point-optimized copy.
        point: bool,
        /// Keep/create scan-optimized copy.
        scan: bool,
        /// New generation.
        new_generation: u32,
    },
}

/// Ordered plan produced by one compiler pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompilerPlan {
    /// Ops in execution order (relocate before reclaim of old generation).
    pub ops: Vec<CompilerOp>,
}

impl CompilerPlan {
    /// Empty plan.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of ops.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Push one op.
    pub fn push(&mut self, op: CompilerOp) {
        self.ops.push(op);
    }
}

/// Observed stats for one live record, used by the planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordStats {
    /// Subject key.
    pub key: Vec<u8>,
    /// Current locator.
    pub locator: ValueLocator,
    /// Logical value length.
    pub value_len: usize,
    /// Measured or hinted temperature.
    pub temperature: TemperatureClass,
    /// Recent point-get count (epoch window).
    pub point_gets: u64,
    /// Recent range-scan hits (epoch window).
    pub scan_hits: u64,
    /// True when value is believed highly compressible with a family dictionary.
    pub compressible_family: bool,
    /// Family id when `compressible_family` (else ignored).
    pub family_id: u32,
    /// True when the record is short-lived (candidate for short zone).
    pub short_lived: bool,
}

/// Planner thresholds.
#[derive(Debug, Clone)]
pub struct CompilerOptions {
    /// Point-get count ≥ this → treat as point-hot.
    pub point_hot_threshold: u64,
    /// Scan-hit count ≥ this → treat as scan-hot.
    pub scan_hot_threshold: u64,
    /// Point gets below this (and not scan-hot) → cold.
    pub cold_max_point_gets: u64,
    /// Generation to assign to rewritten copies.
    pub next_generation: u32,
    /// When true, emit dual-representation ops for mixed-hot keys.
    pub enable_dual_representation: bool,
}

impl Default for CompilerOptions {
    fn default() -> Self {
        Self {
            point_hot_threshold: 64,
            scan_hot_threshold: 64,
            cold_max_point_gets: 2,
            next_generation: 1,
            enable_dual_representation: false,
        }
    }
}

/// Build a compiler plan from a batch of record stats.
///
/// Heuristics (foundation — tune with workload telemetry later):
/// - point-hot medium/large → relocate into [`LocatorKind::PointContainer`]
/// - scan-hot → [`LocatorKind::ScanExtent`]
/// - cold → larger compressed containers (still point container kind; codec later)
/// - compressible families → dictionary train ops (deduped by family_id)
/// - temperature / lifetime mismatches → migrate / lifetime place ops
pub fn plan_compile(records: &[RecordStats], opts: &CompilerOptions) -> CompilerPlan {
    let mut plan = CompilerPlan::new();
    let mut trained_families: Vec<u32> = Vec::new();
    let gen = opts.next_generation;

    for r in records {
        let kind = LocatorKind::of(&r.locator);

        // Dictionary training (once per family in this pass).
        if r.compressible_family && !trained_families.contains(&r.family_id) {
            trained_families.push(r.family_id);
            plan.push(CompilerOp::DictionaryTrain {
                family_id: r.family_id,
                sample_keys: vec![r.key.clone()],
            });
        } else if r.compressible_family {
            // Attach additional sample keys to existing train op when present.
            if let Some(CompilerOp::DictionaryTrain { sample_keys, .. }) = plan
                .ops
                .iter_mut()
                .find(|op| matches!(op, CompilerOp::DictionaryTrain { family_id, .. } if *family_id == r.family_id))
            {
                if sample_keys.len() < 32 {
                    sample_keys.push(r.key.clone());
                }
            }
        }

        let point_hot = r.point_gets >= opts.point_hot_threshold;
        let scan_hot = r.scan_hits >= opts.scan_hot_threshold;
        let cold = r.point_gets <= opts.cold_max_point_gets && r.scan_hits == 0;

        // Dual representation for truly mixed hot (opt-in).
        if opts.enable_dual_representation && point_hot && scan_hot {
            plan.push(CompilerOp::DualRepresent {
                key: r.key.clone(),
                point: true,
                scan: true,
                new_generation: gen,
            });
            continue;
        }

        let desired = if scan_hot {
            LocatorKind::ScanExtent
        } else if point_hot {
            // Tiny stays inline; large stays on value log unless we only recluster medium.
            match value_class_of_len(r.value_len) {
                ValueClass::Tiny => LocatorKind::Inline,
                ValueClass::Large => LocatorKind::LargeValueLog,
                ValueClass::Medium => LocatorKind::PointContainer,
            }
        } else if cold {
            // Cold: prefer scan extent packing for range-friendly cold storage,
            // else keep current kind (no churn for already-good placement).
            if matches!(r.temperature, TemperatureClass::ColdRangeHeavy) {
                LocatorKind::ScanExtent
            } else {
                kind
            }
        } else {
            kind
        };

        if desired != kind {
            plan.push(CompilerOp::Relocate {
                key: r.key.clone(),
                from: r.locator.clone(),
                to_kind: desired,
                new_generation: gen,
            });
        }

        // Temperature migration note when measured temperature disagrees with zone.
        let measured_temp = if point_hot {
            TemperatureClass::HotRandom
        } else if scan_hot {
            TemperatureClass::ColdRangeHeavy
        } else if cold {
            TemperatureClass::ColdRangeHeavy
        } else {
            TemperatureClass::WarmMixed
        };
        if measured_temp != r.temperature {
            plan.push(CompilerOp::HotColdMigrate {
                key: r.key.clone(),
                from: r.temperature,
                to: measured_temp,
                new_generation: gen,
            });
        }

        if r.short_lived {
            plan.push(CompilerOp::LifetimePlace {
                key: r.key.clone(),
                short_lived_zone: true,
                new_generation: gen,
            });
        }
    }

    // Always end a pass with an optional GC reclaim marker for the prior generation.
    if gen > 0 {
        plan.push(CompilerOp::Gc {
            reclaim_generation: gen.saturating_sub(1),
        });
    }

    plan
}

fn value_class_of_len(len: usize) -> ValueClass {
    super::classify::classify_value(len, &super::classify::ClassifyOptions::default())
}

/// Suggest reclustering a whole key range into one target kind.
pub fn plan_recluster_range(
    range_start: Vec<u8>,
    range_end: Vec<u8>,
    target: LocatorKind,
    new_generation: u32,
) -> CompilerPlan {
    let mut plan = CompilerPlan::new();
    plan.push(CompilerOp::Recluster {
        range_start,
        range_end,
        target,
        new_generation,
    });
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chimera::ValueLocator;

    fn stats(key: &[u8], loc: ValueLocator, len: usize) -> RecordStats {
        RecordStats {
            key: key.to_vec(),
            locator: loc,
            value_len: len,
            temperature: TemperatureClass::WarmMixed,
            point_gets: 0,
            scan_hits: 0,
            compressible_family: false,
            family_id: 0,
            short_lived: false,
        }
    }

    #[test]
    fn point_hot_relocates_medium_to_container() {
        let mut r = stats(
            b"k",
            ValueLocator::ScanExtent {
                extent_id: 1,
                offset: 0,
                len: 200,
                generation: 0,
            },
            200,
        );
        r.point_gets = 100;
        let plan = plan_compile(&[r], &CompilerOptions::default());
        assert!(plan.ops.iter().any(|op| matches!(
            op,
            CompilerOp::Relocate {
                to_kind: LocatorKind::PointContainer,
                ..
            }
        )));
    }

    #[test]
    fn scan_hot_relocates_to_extent() {
        let mut r = stats(
            b"k",
            ValueLocator::PointContainer {
                container_id: 1,
                slot: 0,
                generation: 0,
            },
            200,
        );
        r.scan_hits = 100;
        let plan = plan_compile(&[r], &CompilerOptions::default());
        assert!(plan.ops.iter().any(|op| matches!(
            op,
            CompilerOp::Relocate {
                to_kind: LocatorKind::ScanExtent,
                ..
            }
        )));
    }

    #[test]
    fn dictionary_train_deduped_by_family() {
        let mut a = stats(
            b"a",
            ValueLocator::Inline {
                bytes: b"x".to_vec(),
            },
            1,
        );
        a.compressible_family = true;
        a.family_id = 9;
        let mut b = stats(
            b"b",
            ValueLocator::Inline {
                bytes: b"y".to_vec(),
            },
            1,
        );
        b.compressible_family = true;
        b.family_id = 9;
        let plan = plan_compile(&[a, b], &CompilerOptions::default());
        let trains: Vec<_> = plan
            .ops
            .iter()
            .filter(|op| matches!(op, CompilerOp::DictionaryTrain { family_id: 9, .. }))
            .collect();
        assert_eq!(trains.len(), 1);
        if let CompilerOp::DictionaryTrain { sample_keys, .. } = trains[0] {
            assert_eq!(sample_keys.len(), 2);
        }
    }

    #[test]
    fn recluster_helper() {
        let plan = plan_recluster_range(b"a".to_vec(), b"z".to_vec(), LocatorKind::ScanExtent, 3);
        assert_eq!(plan.len(), 1);
    }
}
