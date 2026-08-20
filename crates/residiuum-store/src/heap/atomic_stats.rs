//! Constant-space physical-store telemetry for Heap Atomics.

use crate::atomic_stage::AtomicPhaseTiming;
use crate::store::{StoreOpenMetrics, WriteIoTotals};
use residiuum_atomics::AtomicOutcome;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Deployment-wide Atomic execution and recovery counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AtomicStoreStats {
    /// Store executions attempted after authority admission.
    pub executions: u64,
    /// Executions returning a committed decision.
    pub committed: u64,
    /// Executions returning a durable not-committed decision.
    pub not_committed: u64,
    /// Committed decisions served as durable replays.
    pub replayed: u64,
    /// Store executions returning an error.
    pub failed: u64,
    /// Mutation members presented across executions.
    pub members: u64,
    /// Newly issued executions that crossed physical durability boundaries.
    pub durability_cohorts: u64,
    /// Largest member count in one physical Atomic execution cohort.
    pub max_cohort_members: u64,
    /// Aggregate wait to acquire the physical writer lock.
    pub store_lock_wait_ns: u64,
    /// Largest physical writer-lock wait.
    pub max_store_lock_wait_ns: u64,
    /// Aggregate Atomic catalogue open/reconstruction time.
    pub catalog_open_ns: u64,
    /// Largest Atomic catalogue open/reconstruction time.
    pub max_catalog_open_ns: u64,
    /// Aggregate decision/publication execution time after catalogue open.
    pub decision_publish_ns: u64,
    /// Largest decision/publication execution time.
    pub max_decision_publish_ns: u64,
    /// Aggregate closed-plan/frontier validation time.
    pub validation_ns: u64,
    /// Largest closed-plan/frontier validation time.
    pub max_validation_ns: u64,
    /// Aggregate member append and stable-boundary time.
    pub member_boundary_ns: u64,
    /// Largest member append and stable-boundary time.
    pub max_member_boundary_ns: u64,
    /// Aggregate terminal-decision append and stable-boundary time.
    pub decision_boundary_ns: u64,
    /// Largest terminal-decision append and stable-boundary time.
    pub max_decision_boundary_ns: u64,
    /// Aggregate whole-delta visibility publication time.
    pub publication_ns: u64,
    /// Largest whole-delta visibility publication time.
    pub max_publication_ns: u64,
    /// Authoritative writes attributable to these Atomic executions.
    pub authoritative_write_operations: u64,
    /// Authoritative bytes attributable to these Atomic executions.
    pub authoritative_write_bytes: u64,
    /// Authoritative write time attributable to these Atomic executions.
    pub authoritative_write_ns: u64,
    /// Physical durability barriers attributable to these Atomic executions.
    pub authoritative_sync_operations: u64,
    /// Physical durability-barrier time attributable to these Atomic executions.
    pub authoritative_sync_ns: u64,
    /// Atomics reconstructed during this deployment open.
    pub recovered_atomics: u64,
    /// Accepted Atomics deterministically aborted during dirty-open recovery.
    pub recovery_aborts: u64,
    /// Publications skipped during open because material/authority was degraded.
    pub recovery_publication_degraded: u64,
    /// Atomic-stage bytes scanned during open.
    pub recovery_bytes_scanned: u64,
    /// Atomic catalogue recovery and committed-publication rebuild time.
    pub recovery_ns: u64,
    /// Atomic-stage frames verified during open.
    pub recovery_frames: u64,
}

#[derive(Default)]
pub(super) struct AtomicStoreCounters {
    executions: AtomicU64,
    committed: AtomicU64,
    not_committed: AtomicU64,
    replayed: AtomicU64,
    failed: AtomicU64,
    members: AtomicU64,
    durability_cohorts: AtomicU64,
    max_cohort_members: AtomicU64,
    store_lock_wait_ns: AtomicU64,
    max_store_lock_wait_ns: AtomicU64,
    catalog_open_ns: AtomicU64,
    max_catalog_open_ns: AtomicU64,
    decision_publish_ns: AtomicU64,
    max_decision_publish_ns: AtomicU64,
    validation_ns: AtomicU64,
    max_validation_ns: AtomicU64,
    member_boundary_ns: AtomicU64,
    max_member_boundary_ns: AtomicU64,
    decision_boundary_ns: AtomicU64,
    max_decision_boundary_ns: AtomicU64,
    publication_ns: AtomicU64,
    max_publication_ns: AtomicU64,
    authoritative_write_operations: AtomicU64,
    authoritative_write_bytes: AtomicU64,
    authoritative_write_ns: AtomicU64,
    authoritative_sync_operations: AtomicU64,
    authoritative_sync_ns: AtomicU64,
}

impl AtomicStoreCounters {
    pub(super) fn record_lock_wait(&self, elapsed: Duration) {
        record_duration(
            &self.store_lock_wait_ns,
            &self.max_store_lock_wait_ns,
            elapsed,
        );
    }

    pub(super) fn record_catalog_open(&self, elapsed: Duration) {
        record_duration(&self.catalog_open_ns, &self.max_catalog_open_ns, elapsed);
    }

    pub(super) fn record_execution(
        &self,
        members: usize,
        elapsed: Duration,
        before: WriteIoTotals,
        after: WriteIoTotals,
        phases: AtomicPhaseTiming,
        outcome: Option<&AtomicOutcome>,
    ) {
        self.executions.fetch_add(1, Ordering::Relaxed);
        self.members.fetch_add(members as u64, Ordering::Relaxed);
        self.max_cohort_members
            .fetch_max(members as u64, Ordering::Relaxed);
        record_duration(
            &self.decision_publish_ns,
            &self.max_decision_publish_ns,
            elapsed,
        );
        record_nanos(
            &self.validation_ns,
            &self.max_validation_ns,
            phases.validation_ns,
        );
        record_nanos(
            &self.member_boundary_ns,
            &self.max_member_boundary_ns,
            phases.member_boundary_ns,
        );
        record_nanos(
            &self.decision_boundary_ns,
            &self.max_decision_boundary_ns,
            phases.decision_boundary_ns,
        );
        record_nanos(
            &self.publication_ns,
            &self.max_publication_ns,
            phases.publication_ns,
        );
        self.authoritative_write_operations.fetch_add(
            after
                .write_operations
                .saturating_sub(before.write_operations),
            Ordering::Relaxed,
        );
        self.authoritative_write_bytes.fetch_add(
            after.write_bytes.saturating_sub(before.write_bytes),
            Ordering::Relaxed,
        );
        self.authoritative_write_ns.fetch_add(
            after.write_ns.saturating_sub(before.write_ns),
            Ordering::Relaxed,
        );
        self.authoritative_sync_operations.fetch_add(
            after.sync_operations.saturating_sub(before.sync_operations),
            Ordering::Relaxed,
        );
        if after.sync_operations > before.sync_operations {
            self.durability_cohorts.fetch_add(1, Ordering::Relaxed);
        }
        self.authoritative_sync_ns.fetch_add(
            after.sync_ns.saturating_sub(before.sync_ns),
            Ordering::Relaxed,
        );
        match outcome {
            Some(AtomicOutcome::Committed(receipt)) => {
                self.committed.fetch_add(1, Ordering::Relaxed);
                if receipt.replayed {
                    self.replayed.fetch_add(1, Ordering::Relaxed);
                }
            }
            Some(AtomicOutcome::NotCommitted { .. }) => {
                self.not_committed.fetch_add(1, Ordering::Relaxed);
            }
            Some(AtomicOutcome::Unknown { .. }) => {}
            None => {
                self.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn snapshot(&self, open: StoreOpenMetrics) -> AtomicStoreStats {
        AtomicStoreStats {
            executions: self.executions.load(Ordering::Relaxed),
            committed: self.committed.load(Ordering::Relaxed),
            not_committed: self.not_committed.load(Ordering::Relaxed),
            replayed: self.replayed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            members: self.members.load(Ordering::Relaxed),
            durability_cohorts: self.durability_cohorts.load(Ordering::Relaxed),
            max_cohort_members: self.max_cohort_members.load(Ordering::Relaxed),
            store_lock_wait_ns: self.store_lock_wait_ns.load(Ordering::Relaxed),
            max_store_lock_wait_ns: self.max_store_lock_wait_ns.load(Ordering::Relaxed),
            catalog_open_ns: self.catalog_open_ns.load(Ordering::Relaxed),
            max_catalog_open_ns: self.max_catalog_open_ns.load(Ordering::Relaxed),
            decision_publish_ns: self.decision_publish_ns.load(Ordering::Relaxed),
            max_decision_publish_ns: self.max_decision_publish_ns.load(Ordering::Relaxed),
            validation_ns: self.validation_ns.load(Ordering::Relaxed),
            max_validation_ns: self.max_validation_ns.load(Ordering::Relaxed),
            member_boundary_ns: self.member_boundary_ns.load(Ordering::Relaxed),
            max_member_boundary_ns: self.max_member_boundary_ns.load(Ordering::Relaxed),
            decision_boundary_ns: self.decision_boundary_ns.load(Ordering::Relaxed),
            max_decision_boundary_ns: self.max_decision_boundary_ns.load(Ordering::Relaxed),
            publication_ns: self.publication_ns.load(Ordering::Relaxed),
            max_publication_ns: self.max_publication_ns.load(Ordering::Relaxed),
            authoritative_write_operations: self
                .authoritative_write_operations
                .load(Ordering::Relaxed),
            authoritative_write_bytes: self.authoritative_write_bytes.load(Ordering::Relaxed),
            authoritative_write_ns: self.authoritative_write_ns.load(Ordering::Relaxed),
            authoritative_sync_operations: self
                .authoritative_sync_operations
                .load(Ordering::Relaxed),
            authoritative_sync_ns: self.authoritative_sync_ns.load(Ordering::Relaxed),
            recovered_atomics: u64::from(open.atomic_stage_atomics),
            recovery_aborts: u64::from(open.atomic_stage_recovery_aborts),
            recovery_publication_degraded: u64::from(open.atomic_stage_publication_degraded),
            recovery_bytes_scanned: open.atomic_stage_bytes_scanned,
            recovery_ns: open.atomic_recovery_ns,
            recovery_frames: u64::from(open.atomic_stage_frames),
        }
    }
}

fn record_duration(total: &AtomicU64, maximum: &AtomicU64, elapsed: Duration) {
    let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    record_nanos(total, maximum, nanos);
}

fn record_nanos(total: &AtomicU64, maximum: &AtomicU64, nanos: u64) {
    total.fetch_add(nanos, Ordering::Relaxed);
    maximum.fetch_max(nanos, Ordering::Relaxed);
}
