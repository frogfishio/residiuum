//! PQH-9: qualification campaign orchestration, evidence bundle, disclosure.
//!
//! Runs the harness matrix with multi-repetition / multi-process slots, builds
//! a hashed evidence bundle, ranks bottlenecks via the PQH-8 analyzer, and
//! emits Benchmark Disclosure + follow-up optimization *cards* (no tuning).
//!
//! **Honesty:** unit tests exercise the campaign machinery on synthetic/harness
//! cells. Absolute product MB/s claims require a controlled runner environment
//! and must carry full disclosure; this package does not invent them.

mod bundle;
mod disclosure;
mod multiproc;
mod plan;
mod reports;
mod run;
mod run_class;

pub use bundle::{verify_bundle_hashes, write_evidence_bundle, EvidenceBundle, BUNDLE_SCHEMA};
pub use disclosure::{
    build_disclosure, render_disclosure_markdown, ChecklistItem, DisclosureSummary,
};
pub use multiproc::{
    build_worker_jobs, current_worker_bin, run_spawned_workers, run_worker_job, WorkerJob,
    WorkerResult,
};
pub use plan::{
    campaign_plan_linux, campaign_plan_macos_apple_silicon, campaign_plan_synthetic, CampaignPlan,
    PlatformClass, MIN_PROCESSES, MIN_REPETITIONS,
};
pub use reports::{
    build_campaign_reports, CampaignReports, FollowUpCard, MultiprocFinding, RankedBottleneck,
    SliceRow,
};
pub use run::{
    attach_primary_bottleneck, run_campaign, run_qualification_preflight, CampaignConfig,
    CampaignResult, CellRepetition, PresentationPin, ProcessSlot,
};
pub use run_class::RunClass;

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CampaignError {
    #[error("campaign: {0}")]
    Msg(String),
    #[error("matrix: {0}")]
    Matrix(String),
    #[error("io: {0}")]
    Io(String),
    #[error("bundle: {0}")]
    Bundle(String),
}

impl From<crate::matrix::MatrixError> for CampaignError {
    fn from(e: crate::matrix::MatrixError) -> Self {
        CampaignError::Matrix(e.to_string())
    }
}

impl From<std::io::Error> for CampaignError {
    fn from(e: std::io::Error) -> Self {
        CampaignError::Io(e.to_string())
    }
}
