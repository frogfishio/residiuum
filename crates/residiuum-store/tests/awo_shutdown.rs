//! AWO-4: pipeline depth ≤ 2, no third reservation, seal-safe, bounded shutdown.
//!
//! Run:
//!   cargo test -p residiuum-store --features legacy-raw-store --test awo_shutdown -- --test-threads=1

use residiuum_store::adaptive_write::{
    AdaptiveWriteMode, AdaptiveWritePolicy, PipelineCoordinator, PipelineError, ReservationPhase,
};
use residiuum_store::{DurabilityMode, StoreError, StoreHost};
use std::time::{Duration, Instant};

#[test]
fn pipeline_depth_two_no_third_reservation() {
    let p = PipelineCoordinator::depth_two();
    assert_eq!(p.status().depth_limit, 2);
    let a = p.try_begin_reservation().unwrap();
    let b = p.try_begin_reservation().unwrap();
    assert_eq!(
        p.try_begin_reservation(),
        Err(PipelineError::DepthExceeded {
            limit: 2,
            in_flight: 2
        })
    );
    // Overlap shape: install A while B still cooking.
    p.note_cook_complete(a).unwrap();
    let st = p.status();
    assert_eq!(st.installing, 1);
    assert_eq!(st.cooking, 1);
    assert!(p.try_begin_reservation().is_err());

    p.note_install_complete(a).unwrap();
    // One slot free — third may enter as cook while B still cooking.
    let c = p.try_begin_reservation().unwrap();
    p.note_cook_complete(b).unwrap();
    p.note_install_complete(b).unwrap();
    p.note_cook_complete(c).unwrap();
    p.note_install_complete(c).unwrap();
    assert_eq!(p.status().in_flight, 0);
}

#[test]
fn seal_blocks_new_reservation() {
    let p = PipelineCoordinator::depth_two();
    p.begin_seal();
    assert_eq!(p.try_begin_reservation(), Err(PipelineError::Sealing));
    p.end_seal();
    let id = p.try_begin_reservation().unwrap();
    p.abort_reservation(id).unwrap();
}

#[test]
fn shutdown_rejects_new_and_wait_empty() {
    let p = PipelineCoordinator::depth_two();
    let id = p.try_begin_reservation().unwrap();
    p.begin_shutdown();
    assert_eq!(p.try_begin_reservation(), Err(PipelineError::ShuttingDown));
    p.note_cook_complete(id).unwrap();
    p.note_install_complete(id).unwrap();
    p.wait_empty(Instant::now() + Duration::from_secs(1))
        .unwrap();
    assert_eq!(p.status().in_flight, 0);
}

#[test]
fn shutdown_drain_timeout_reports_remaining() {
    let p = PipelineCoordinator::depth_two();
    let _ = p.try_begin_reservation().unwrap();
    p.begin_shutdown();
    let err = p
        .wait_empty(Instant::now() + Duration::from_millis(5))
        .unwrap_err();
    assert!(matches!(err, PipelineError::DrainTimeout { remaining: 1 }));
    // Clean up so Drop of nothing hangs — abort leftover.
    let id = p.in_flight_ids()[0];
    p.abort_reservation(id).unwrap();
}

#[test]
fn bad_phase_rejected() {
    let p = PipelineCoordinator::depth_two();
    let id = p.try_begin_reservation().unwrap();
    assert!(matches!(
        p.note_install_complete(id),
        Err(PipelineError::BadPhase {
            found: ReservationPhase::Cooking,
            expected: ReservationPhase::Installing,
        })
    ));
    p.note_cook_complete(id).unwrap();
    assert!(matches!(
        p.note_cook_complete(id),
        Err(PipelineError::BadPhase {
            found: ReservationPhase::Installing,
            expected: ReservationPhase::Cooking,
        })
    ));
    p.note_install_complete(id).unwrap();
}

#[test]
fn static_runtime_exposes_pipeline_depth_two() {
    let dir = tempfile::tempdir().unwrap();
    let mut policy = AdaptiveWritePolicy::machine_defaults();
    policy.mode = AdaptiveWriteMode::Static;
    policy.maximum_cookers = 1;
    policy.minimum_active_cookers = 1;
    policy.pipeline_depth_limit = 2;
    let host = StoreHost::create_with_adaptive_write(dir.path(), policy).unwrap();
    let st = host.adaptive_write_status().unwrap();
    assert_eq!(st.pipeline.depth_limit, 2);
    assert_eq!(st.pipeline.in_flight, 0);
    assert!(!st.pipeline.shutting_down);

    let handle = host.adaptive_write().unwrap().clone();
    let pipe = handle.pipeline().expect("pipeline attached");
    // Hold two reservations → third batch must not pass (pipeline full).
    let r1 = pipe.try_begin_reservation().unwrap();
    let r2 = pipe.try_begin_reservation().unwrap();
    assert!(pipe.try_begin_reservation().is_err());

    let physical = host.physical();
    let mut guard = physical.lock().unwrap();
    let err = handle
        .admit_put_batch(
            &mut guard,
            &[("awo/pipe/x", b"1")],
            DurabilityMode::Buffered,
        )
        .expect_err("depth exceeded");
    assert_eq!(err.as_str(), "pipeline_depth_exceeded");
    // Direct put still lease-fenced.
    assert!(matches!(
        guard.put("awo/pipe/y", b"2", DurabilityMode::Buffered),
        Err(StoreError::AdaptiveWriterActive)
    ));

    pipe.abort_reservation(r1).unwrap();
    pipe.abort_reservation(r2).unwrap();
    // After free, batch admit works and leaves pipeline empty.
    let receipts = handle
        .admit_put_batch(
            &mut guard,
            &[("awo/pipe/ok", b"z")],
            DurabilityMode::Buffered,
        )
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(pipe.status().in_flight, 0);
}

#[test]
fn drain_writes_begins_pipeline_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let mut policy = AdaptiveWritePolicy::machine_defaults();
    policy.mode = AdaptiveWriteMode::Static;
    policy.maximum_cookers = 1;
    policy.minimum_active_cookers = 1;
    let host = StoreHost::create_with_adaptive_write(dir.path(), policy).unwrap();
    let handle = host.adaptive_write().unwrap().clone();
    host.drain_writes(Instant::now() + Duration::from_secs(1))
        .unwrap();
    let st = handle.status();
    assert!(st.draining);
    assert!(st.pipeline.shutting_down);
    // New reservation refused after drain.
    assert_eq!(
        handle.pipeline().unwrap().try_begin_reservation(),
        Err(PipelineError::ShuttingDown)
    );
}
