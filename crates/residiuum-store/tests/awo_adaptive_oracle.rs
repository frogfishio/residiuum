//! AWO-5: adaptive controller oracle — EWMA, candidates, scale hysteresis.
//!
//! No wall-clock sleeps; ManualClock only.
//!
//! Run:
//!   cargo test -p residiuum-store --features legacy-raw-store --test awo_adaptive_oracle -- --test-threads=1

use residiuum_store::adaptive_write::{
    candidate_entry_counts, collection_delay_ns, decide, payload_bucket, select_plan,
    AdaptiveWriteMode, AdaptiveWritePolicy, AwoPlan, CandidateBounds, ControllerPolicy,
    ControllerSignals, EwmaEstimate, GoldenDecisionInput, ManualClock, ScaleController,
    ScaleDecision, ServiceEstimator, MIN_SAMPLES_DEFAULT,
};

#[test]
fn ewma_matches_plan_formula_first_steps() {
    let mut e = EwmaEstimate::cold();
    e.observe(1000, 0);
    assert_eq!(e.mean_ns, 1000);
    assert_eq!(e.deviation_ns, 0);
    e.observe(1800, 1);
    // m' = 1000 + (1800-1000)/8 = 1100
    assert_eq!(e.mean_ns, 1100);
    // abs(1800-1100)=700; d' = 0 + 700/8 = 87
    assert_eq!(e.deviation_ns, 87);
    assert_eq!(e.lower_ns(), 1100 - 3 * 87);
    assert_eq!(e.upper_ns(), 1100 + 3 * 87);
}

#[test]
fn service_estimator_warm_gate() {
    let clock = ManualClock::new(0);
    let mut est = ServiceEstimator::with_policy_defaults();
    for i in 0..MIN_SAMPLES_DEFAULT {
        est.observe(500 + i as u64, &clock);
        clock.advance(1);
    }
    let (warm, stale) = est.evidence_flags(&clock);
    assert!(warm);
    assert!(!stale);
    // Jump past 30s staleness.
    clock.advance(30_000_000_001);
    let (warm2, stale2) = est.evidence_flags(&clock);
    assert!(warm2);
    assert!(stale2);
}

#[test]
fn candidates_closed_set() {
    let b = CandidateBounds {
        queued_count: 100,
        entries_available: 50,
        maximum_batch_entries: 64,
        bytes_per_entry: 100,
        bytes_available: 10_000,
        maximum_batch_bytes: 16 * 1024 * 1024,
        segment_room_entries: 200,
    };
    let c = candidate_entry_counts(&b);
    assert_eq!(c[0], 1);
    assert!(c.contains(&32));
    // 64 entries * 100 bytes = 6400 ok, but maximum_batch_entries 64 and entries_available 50 → hard=50
    assert!(!c.iter().any(|&x| x > 50));
    assert!(c.contains(&50)); // queued clip includes 50? queued=100 hard=50 → include 50
}

#[test]
fn select_plan_aligns_with_decide_goldens_path() {
    // Reproduce natural_insufficient_evidence via select_plan.
    let b = CandidateBounds {
        queued_count: 4,
        entries_available: 100,
        maximum_batch_entries: 1024,
        bytes_per_entry: 0,
        bytes_available: u64::MAX,
        maximum_batch_bytes: u64::MAX,
        segment_room_entries: 1000,
    };
    let s = select_plan(&b, 100, 100_000, false, false, true, u64::MAX, |_| 1);
    assert_eq!(s.plan, AwoPlan::Natural);
    assert_eq!(s.reason, "natural_insufficient_evidence");

    // Direct decide still matches contract single request.
    let d = decide(&GoldenDecisionInput {
        queue_count: 1,
        natural_service_lower_ns: 1,
        batch_completion_upper_ns: 1,
        decision_margin_ppm: 100_000,
        evidence_warm: true,
        evidence_stale: false,
        compatible: true,
        memory_ok: true,
        earliest_deadline_ns: u64::MAX,
    });
    assert_eq!(d.reason, "natural_single_request");
}

#[test]
fn scale_controller_hysteresis_with_manual_clock() {
    let clock = ManualClock::new(0);
    let policy = ControllerPolicy {
        interval_ns: 10,
        scale_up_consecutive: 2,
        scale_down_consecutive: 2,
        scale_up_utilization_ppm: 500_000,
        scale_down_utilization_ppm: 200_000,
        ready_queue_scale_down_ppm: 900_000,
        scale_up_dwell_ns: 0,
        scale_down_dwell_ns: 0,
        minimum_active_cookers: 1,
        maximum_cookers: 3,
    };
    let mut ctl = ScaleController::new(policy, 1);
    let up = ControllerSignals {
        cook_queue_bytes_increased: true,
        cooker_utilization_ppm: 900_000,
        writer_ready_increasing: false,
        ready_queue_fill_ppm: 0,
        positive_marginal_throughput: true,
        writer_saturated: false,
        deadline_miss_from_cook: false,
    };
    assert_eq!(ctl.evaluate(&up, &clock), ScaleDecision::Hold);
    clock.advance(10);
    assert_eq!(ctl.evaluate(&up, &clock), ScaleDecision::ScaleUp);
    assert_eq!(ctl.active_cookers, 2);

    let down = ControllerSignals {
        cook_queue_bytes_increased: false,
        cooker_utilization_ppm: 100_000,
        writer_ready_increasing: false,
        ready_queue_fill_ppm: 0,
        positive_marginal_throughput: false,
        writer_saturated: false,
        deadline_miss_from_cook: false,
    };
    clock.advance(10);
    assert_eq!(ctl.evaluate(&down, &clock), ScaleDecision::Hold);
    clock.advance(10);
    assert_eq!(ctl.evaluate(&down, &clock), ScaleDecision::ScaleDown);
    assert_eq!(ctl.active_cookers, 1);
}

#[test]
fn policy_defaults_still_disabled_for_product() {
    let p = AdaptiveWritePolicy::machine_defaults();
    assert_eq!(p.mode, AdaptiveWriteMode::Disabled);
    assert_eq!(p.decision_margin_ppm, 100_000);
    assert_eq!(p.estimator_min_samples, 32);
}

#[test]
fn collection_and_payload_bucket_closed() {
    assert_eq!(collection_delay_ns(1, true, true, 250_000), 250_000);
    assert_eq!(payload_bucket(1024, 4096), 1024);
}

#[test]
fn adaptive_mode_select_plan_sizes_cold_batch_to_natural_one() {
    use residiuum_store::{DurabilityMode, StoreHost};

    let dir = tempfile::tempdir().unwrap();
    let mut policy = AdaptiveWritePolicy::machine_defaults();
    policy.mode = AdaptiveWriteMode::Adaptive;
    policy.maximum_cookers = 1;
    policy.minimum_active_cookers = 1;
    let host = StoreHost::create_with_adaptive_write(dir.path(), policy).unwrap();
    let handle = host.adaptive_write().unwrap().clone();
    let physical = host.physical();
    let mut guard = physical.lock().unwrap();

    // Cold estimator → natural → only first item of presented batch is taken.
    let receipts = handle
        .admit_put_batch(
            &mut guard,
            &[("awo/ad/a", b"1"), ("awo/ad/b", b"2"), ("awo/ad/c", b"3")],
            DurabilityMode::Buffered,
        )
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert!(guard.get("awo/ad/a").unwrap().is_some());
    assert!(guard.get("awo/ad/b").unwrap().is_none());
    let sel = handle.last_selection().expect("selection recorded");
    assert_eq!(sel.plan, AwoPlan::Natural);
    assert_eq!(sel.reason, "natural_insufficient_evidence");
}
