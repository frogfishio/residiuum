//! Validated AWO policy and machine-derived defaults (`policy-v1.json`).
//!
//! Mode remains **disabled** until AWO-7 principal accept. AWO-2 uses the
//! cooker/queue limits only; product admission arrives in AWO-3.

use std::time::Duration;

/// Product / runtime adaptive-write mode (plan §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdaptiveWriteMode {
    /// No AWO admission; natural store paths only (default until AWO-7).
    Disabled,
    /// Fixed batch limits; no learning (AWO-3).
    Static,
    /// Full adaptive controller (AWO-5+).
    Adaptive,
}

impl AdaptiveWriteMode {
    /// Stable wire id.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Static => "static",
            Self::Adaptive => "adaptive",
        }
    }
}

/// Validated AWO runtime policy (subset closed by `policy-v1.json`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveWritePolicy {
    /// Operating mode.
    pub mode: AdaptiveWriteMode,
    /// Max reserved queue bytes.
    pub queue_byte_limit: usize,
    /// Max reserved queue entries.
    pub queue_entry_limit: usize,
    /// Max bytes per selected batch.
    pub maximum_batch_bytes: usize,
    /// Max entries per selected batch.
    pub maximum_batch_entries: usize,
    /// Collection wait cap.
    pub maximum_collection_delay: Duration,
    /// Default completion deadline.
    pub default_completion_deadline: Duration,
    /// Minimum active cooker permits.
    pub minimum_active_cookers: usize,
    /// Thread pool size (created once; active via permits).
    pub maximum_cookers: usize,
    /// Unresolved pipeline depth limit (1..=4; V1 default 2).
    pub pipeline_depth_limit: usize,
    /// Decision margin in parts-per-million.
    pub decision_margin_ppm: u32,
    /// Estimator warm sample count.
    pub estimator_min_samples: u32,
    /// Estimator staleness.
    pub estimator_stale_after: Duration,
}

/// Policy validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyError {
    /// A hard limit was zero.
    ZeroLimit,
    /// Queue cannot hold one maximum batch.
    QueueSmallerThanBatch,
    /// Pipeline depth outside 1..=4.
    BadPipelineDepth,
    /// Maximum cookers exceeds hard max (64).
    TooManyCookers,
    /// Collection delay exceeds 10 ms hard max.
    CollectionDelayTooLarge,
    /// Deadline shorter than collection cap.
    DeadlineBeforeCollection,
    /// Minimum active cookers exceeds maximum_cookers.
    ActiveCookersOutOfRange,
}

impl AdaptiveWritePolicy {
    /// Machine defaults from `policy-v1.json` / plan §12.
    ///
    /// `maximum_cookers` uses
    /// `min(max(available_parallelism.saturating_sub(1), 1), 16)`.
    pub fn machine_defaults() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let maximum_cookers = parallelism.saturating_sub(1).max(1).min(16);
        Self {
            mode: AdaptiveWriteMode::Disabled,
            queue_byte_limit: 64 * 1024 * 1024,
            queue_entry_limit: 8192,
            maximum_batch_bytes: 16 * 1024 * 1024,
            maximum_batch_entries: 1024,
            maximum_collection_delay: Duration::from_micros(250),
            default_completion_deadline: Duration::from_secs(30),
            minimum_active_cookers: 1,
            maximum_cookers,
            pipeline_depth_limit: 2,
            decision_margin_ppm: super::types::DECISION_MARGIN_PPM_DEFAULT,
            estimator_min_samples: 32,
            estimator_stale_after: Duration::from_secs(30),
        }
    }

    /// Validate closed policy constraints (`policy-v1.json` validation block).
    pub fn validate(&self) -> Result<(), PolicyError> {
        if self.queue_byte_limit == 0
            || self.queue_entry_limit == 0
            || self.maximum_batch_bytes == 0
            || self.maximum_batch_entries == 0
            || self.maximum_cookers == 0
        {
            return Err(PolicyError::ZeroLimit);
        }
        if self.queue_entry_limit < self.maximum_batch_entries
            || self.queue_byte_limit < self.maximum_batch_bytes
        {
            return Err(PolicyError::QueueSmallerThanBatch);
        }
        if !(1..=4).contains(&self.pipeline_depth_limit) {
            return Err(PolicyError::BadPipelineDepth);
        }
        if self.maximum_cookers > 64 {
            return Err(PolicyError::TooManyCookers);
        }
        if self.maximum_collection_delay > Duration::from_millis(10) {
            return Err(PolicyError::CollectionDelayTooLarge);
        }
        if self.default_completion_deadline < self.maximum_collection_delay {
            return Err(PolicyError::DeadlineBeforeCollection);
        }
        if self.minimum_active_cookers == 0 || self.minimum_active_cookers > self.maximum_cookers {
            return Err(PolicyError::ActiveCookersOutOfRange);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_defaults_validate() {
        let p = AdaptiveWritePolicy::machine_defaults();
        assert_eq!(p.mode, AdaptiveWriteMode::Disabled);
        p.validate().expect("defaults valid");
        assert!(p.maximum_cookers >= 1 && p.maximum_cookers <= 16);
    }

    #[test]
    fn rejects_pipeline_zero() {
        let mut p = AdaptiveWritePolicy::machine_defaults();
        p.pipeline_depth_limit = 0;
        assert_eq!(p.validate(), Err(PolicyError::BadPipelineDepth));
    }
}
