//! Adaptive Write Optimiser (AWO).
//!
//! Normative: `doc/todo/performance-qualification/ADAPTIVE_WRITE_OPTIMISER_*`
//! and executable contracts under `spec/performance/awo/`.
//!
//! - **AWO-0:** pure selector model + golden vectors.
//! - **AWO-1:** persist-before-publish on store batch paths + poison.
//! - **AWO-2:** persistent cooker pool, credit ledger, ordered ready ring.
//! - **AWO-3+:** product StoreHost admission (mode still disabled by default).

pub mod collection;
pub mod controller;
pub mod cooker;
pub mod coordinator;
pub mod credits;
pub mod estimator;
pub mod model;
pub mod ordered_ready;
pub mod persist;
pub mod policy;
pub mod queue;
pub mod runtime;
pub mod selector;
pub mod telemetry;
pub mod types;

pub use controller::{
    AwoClock, ControllerPolicy, ControllerSignals, InstantClock, ManualClock, ScaleController,
    ScaleDecision,
};
pub use cooker::{
    cook_item_frame, CookOutcome, CookTask, CookedMutation, FrameBufferPool, PersistentCookerPool,
};
pub use coordinator::{
    PipelineCoordinator, PipelineError, PipelineStatus, ReservationId, ReservationPhase,
};
pub use credits::{
    mutation_credit, CreditError, CreditLedger, COMPLETION_SLOT_OVERHEAD, ENVELOPE_FIXED_OVERHEAD,
    FRAME_FRAMING_OVERHEAD, REQUEST_META_OVERHEAD,
};
pub use estimator::{
    payload_bucket, EwmaEstimate, ServiceEstimator, ALPHA_DENOMINATOR, DEVIATION_MULTIPLIER,
    MIN_SAMPLES_DEFAULT, STALE_AFTER_NS_DEFAULT,
};
pub use model::{decide, Decision, GoldenDecisionInput};
pub use ordered_ready::{OrderedReadyRing, ReadyError};
pub use persist::{BatchReservation, LaneTicket, AWO_FAILPOINTS};
pub use policy::{AdaptiveWriteMode, AdaptiveWritePolicy, PolicyError};
pub use queue::{BoundedQueue, QueueError};
pub use runtime::{
    classify_delete, classify_put, AdaptiveWriteError, AdaptiveWriteHandle, AdaptiveWriteRuntime,
    AdaptiveWriteStatus, AdmissionResult, EligibilityClass, WriteCompletion,
};
pub use selector::{
    candidate_entry_counts, collection_delay_ns, select_plan, CandidateBounds, Selection,
    CANDIDATE_POW2,
};
pub use telemetry::{
    AdaptiveWriteInspect, ScaleTelemetry, BENCHMARK_DISCLOSURE, SUPPORT_MATRIX,
    UPGRADE_ROLLBACK_NOTE,
};
pub use types::{decision_reason_ids, AwoPlan, AWO_PROFILE, DECISION_MARGIN_PPM_DEFAULT};
