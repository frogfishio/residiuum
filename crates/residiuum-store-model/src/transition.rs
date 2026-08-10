//! CSQ-4 publication kernel: ordinary transition classes and coverage.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Registered ordinary state-transition classes (CSQ-4 exit coverage).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionClass {
    /// Durable put with receipt.
    PutDurable,
    /// Durable delete with receipt.
    DeleteDurable,
    /// Buffered put (weaker than durable).
    PutBuffered,
    /// Memory put (weaker than durable).
    PutMemory,
    /// Exact operation-id retry returning original receipt.
    ExactRetry,
    /// Operation-id reuse with different content rejected.
    OpIdConflict,
    /// Interrupted put resolves old.
    InterruptOld,
    /// Interrupted put resolves new.
    InterruptNew,
    /// Interrupted put resolves unknown (no hybrid).
    InterruptUnknown,
    /// Explicit damage mark (not absence).
    MarkDamage,
    /// Durable put clears prior damage for subject.
    ClearDamageViaPut,
    /// Model reopen / snapshot restore.
    Reopen,
    /// Compaction that preserves history order.
    CompactPreserveHistory,
    /// Exclusive writer acquire success.
    WriterAcquire,
    /// Exclusive writer reject (no durable effect).
    WriterReject,
    /// Coverage-aware key scan.
    ScanKeys,
    /// Full subject history walk.
    HistoryWalk,
    /// Exact historical-value read.
    HistoricalGet,
    /// Bounded last-complete recovery.
    LastComplete,
    /// Record explicit history gap.
    RecordGap,
    /// Non-interference check (subject A vs B).
    NonInterference,
    /// Store identity stability check.
    IdentityStable,
    /// Failed derived/cache update does not roll back authority.
    DerivedCacheFailNoRollback,
    /// Visibility only after durability mode requirements.
    PublishAfterDurableBytes,
}

impl TransitionClass {
    /// Every ordinary transition that CSQ-4 coverage must reach.
    pub fn all_ordinary() -> &'static [TransitionClass] {
        use TransitionClass::*;
        &[
            PutDurable,
            DeleteDurable,
            PutBuffered,
            PutMemory,
            ExactRetry,
            OpIdConflict,
            InterruptOld,
            InterruptNew,
            InterruptUnknown,
            MarkDamage,
            ClearDamageViaPut,
            Reopen,
            CompactPreserveHistory,
            WriterAcquire,
            WriterReject,
            ScanKeys,
            HistoryWalk,
            HistoricalGet,
            LastComplete,
            RecordGap,
            NonInterference,
            IdentityStable,
            DerivedCacheFailNoRollback,
            PublishAfterDurableBytes,
        ]
    }
}

/// Tracks which transition classes have been executed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionCoverage {
    /// Reached classes.
    reached: BTreeSet<TransitionClass>,
}

impl TransitionCoverage {
    /// Empty coverage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `class` was reached.
    pub fn record(&mut self, class: TransitionClass) {
        self.reached.insert(class);
    }

    /// Whether `class` was reached.
    pub fn reached(&self, class: TransitionClass) -> bool {
        self.reached.contains(&class)
    }

    /// Number of distinct classes reached.
    pub fn count(&self) -> usize {
        self.reached.len()
    }

    /// Missing ordinary classes.
    pub fn missing_ordinary(&self) -> Vec<TransitionClass> {
        TransitionClass::all_ordinary()
            .iter()
            .copied()
            .filter(|c| !self.reached.contains(c))
            .collect()
    }

    /// True when every ordinary class was reached.
    pub fn complete_ordinary(&self) -> bool {
        self.missing_ordinary().is_empty()
    }

    /// Coverage report as sorted class names.
    pub fn report(&self) -> Vec<String> {
        self.reached.iter().map(|c| format!("{c:?}")).collect()
    }
}

/// Publication barrier relative to durability mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationPhase {
    /// Bytes not yet durable; not visible as durable authority.
    PreDurable,
    /// Durable bytes present; may publish.
    PostDurable,
    /// Derived/cache update attempted after authority publish.
    DerivedUpdate,
}

/// Result of a publication-kernel step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationStep {
    /// Transition class exercised.
    pub class: TransitionClass,
    /// Phase after the step.
    pub phase: PublicationPhase,
    /// Whether the step produced a durable receipt.
    pub durable_receipt: bool,
    /// Whether current observation became visible under durable semantics.
    pub visible_as_durable: bool,
}
