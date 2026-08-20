//! Non-product ATM-5 public-driver qualification dipstick.

use clap::{Parser, ValueEnum};
use residiuum_atomics::ResourceLimits;
use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapCap, HeapId, HeapSecuritySnapshot, HeapSlot, Rights,
    SecurityRevision, TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::driver::{
    atomics::{AtomicId, AtomicOptions, AtomicOutcome},
    Client, Collection, CreateCollectionOptions, EmbeddedOptions,
};
use residiuum_sdk::ResidiuumDeployment;
use residiuum_store::{
    publish_staged_genesis, stage_heap_genesis, AtomicStoreStats, HeapMetaLayout,
};
use serde::Serialize;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::ExitCode;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(name = "residiuum-atomic-qual")]
#[command(about = "ATM-5 public-driver performance dipstick; diagnostic, not a product benchmark")]
struct Cli {
    /// New store root. Existing paths are refused to protect prior evidence.
    #[arg(long)]
    root: PathBuf,
    /// JSON evidence output. Defaults to <root>.atomic-qual.json.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Repetitions per cell.
    #[arg(long, default_value_t = 3)]
    iterations: usize,
    /// Matrix breadth.
    #[arg(long, value_enum, default_value_t = Profile::Dipstick)]
    profile: Profile,
}

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum Profile {
    /// Seven high-information cells for rapid bottleneck location.
    Dipstick,
    /// Declared member/payload cross-product; structurally invalid cells are recorded as skipped.
    MemberPayload,
}

#[derive(Clone, Copy)]
struct Case {
    members: usize,
    payload_bytes: usize,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    diagnostic_only: bool,
    profile: Profile,
    generated_unix_ms: u128,
    target_os: &'static str,
    target_arch: &'static str,
    build_profile: &'static str,
    iterations: usize,
    root: String,
    cells: Vec<CellReport>,
}

#[derive(Serialize)]
struct CellReport {
    members: usize,
    payload_bytes_per_member: usize,
    status: &'static str,
    detail: Option<String>,
    completed: usize,
    logical_value_bytes: u64,
    physical_write_bytes: u64,
    write_amplification: Option<f64>,
    authoritative_write_operations: u64,
    authoritative_sync_operations: u64,
    durability_cohorts: u64,
    commits_per_second: f64,
    member_mutations_per_second: f64,
    end_to_end_ns: Percentiles,
    store_lock_wait_ns: Percentiles,
    catalog_open_ns: Percentiles,
    validation_ns: Percentiles,
    member_boundary_ns: Percentiles,
    decision_boundary_ns: Percentiles,
    publication_ns: Percentiles,
}

#[derive(Clone, Copy, Default, Serialize)]
struct Percentiles {
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

#[derive(Default)]
struct Samples {
    end_to_end: Vec<u64>,
    lock_wait: Vec<u64>,
    catalog_open: Vec<u64>,
    validation: Vec<u64>,
    member_boundary: Vec<u64>,
    decision_boundary: Vec<u64>,
    publication: Vec<u64>,
}

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("atomic qualification failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), String> {
    if cli.iterations == 0 {
        return Err("--iterations must be non-zero".into());
    }
    if cli.root.exists() {
        return Err(format!(
            "refusing existing evidence/store path {}",
            cli.root.display()
        ));
    }
    let output = cli
        .out
        .clone()
        .unwrap_or_else(|| cli.root.with_extension("atomic-qual.json"));
    if output.exists() {
        return Err(format!("refusing existing output {}", output.display()));
    }
    let capability = prepare_deployment(&cli.root)?;
    let client = block_on(Client::open_embedded(EmbeddedOptions::new(&cli.root)))
        .map_err(|error| error.to_string())?;
    let heap = block_on(client.open_heap(capability)).map_err(|error| error.to_string())?;
    let collection: Collection<String> = block_on(
        heap.create_collection("atomic-qualification", CreateCollectionOptions::default()),
    )
    .map_err(|error| error.to_string())?;

    let mut cells = Vec::new();
    for case in cases(cli.profile) {
        let report = run_case(&client, &heap, &collection, case, cli.iterations)?;
        println!(
            "members={:<3} payload={:<8} status={:<9} commits/s={:>9.2} p50={:>9}ns catalog_p50={:>9}ns syncs={}",
            report.members,
            report.payload_bytes_per_member,
            report.status,
            report.commits_per_second,
            report.end_to_end_ns.p50,
            report.catalog_open_ns.p50,
            report.authoritative_sync_operations,
        );
        if let Some(detail) = &report.detail {
            println!("  detail={detail}");
        }
        cells.push(report);
    }
    block_on(client.close()).map_err(|error| error.to_string())?;

    let report = Report {
        schema: "residiuum.atomic-qualification.v1",
        diagnostic_only: true,
        profile: cli.profile,
        generated_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        target_os: std::env::consts::OS,
        target_arch: std::env::consts::ARCH,
        build_profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        iterations: cli.iterations,
        root: cli.root.display().to_string(),
        cells,
    };
    let bytes = serde_json::to_vec_pretty(&report).map_err(|error| error.to_string())?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    fs::write(&output, bytes).map_err(|error| error.to_string())?;
    println!("evidence={}", output.display());
    Ok(())
}

fn run_case(
    client: &Client,
    heap: &residiuum_sdk::driver::HeapClient,
    collection: &Collection<String>,
    case: Case,
    iterations: usize,
) -> Result<CellReport, String> {
    let payload = "x".repeat(case.payload_bytes);
    let mut samples = Samples::default();
    let mut completed = 0usize;
    let mut logical = 0u64;
    let mut physical = 0u64;
    let mut writes = 0u64;
    let mut syncs = 0u64;
    let mut cohorts = 0u64;
    let mut elapsed_total = Duration::ZERO;

    for iteration in 0..iterations {
        let mut builder = heap
            .atomic(
                AtomicOptions::new(AtomicId::random().map_err(|error| error.to_string())?)
                    .with_limits(ResourceLimits::hard_local_heap()),
            )
            .map_err(|error| error.to_string())?;
        for member in 0..case.members {
            if let Err(error) = builder.create(
                collection,
                format!(
                    "m{}-p{}-i{}-k{}",
                    case.members, case.payload_bytes, iteration, member
                ),
                &payload,
            ) {
                return Ok(skipped(case, error.to_string()));
            }
        }
        let plan = match builder.build() {
            Ok(plan) => plan,
            Err(error) => return Ok(skipped(case, error.to_string())),
        };
        let before = store_stats(client)?;
        let started = Instant::now();
        let outcome = match block_on(heap.commit_atomic(plan)) {
            Ok(outcome) => outcome,
            Err(error) => {
                return Ok(skipped(
                    case,
                    format!(
                        "driver error code={:?} atomic_code={:?}: {}",
                        error.code, error.atomic_code, error
                    ),
                ));
            }
        };
        let elapsed = started.elapsed();
        let after = store_stats(client)?;
        if !matches!(outcome, AtomicOutcome::Committed(_)) {
            let detail = match outcome {
                AtomicOutcome::Unknown { atomic_id, .. } => {
                    let status = block_on(heap.atomic_status(atomic_id))
                        .map(|status| format!("{status:?}"))
                        .unwrap_or_else(|error| format!("status error: {error}"));
                    format!("unexpected unknown; resolved status={status}")
                }
                other => format!("unexpected outcome: {other:?}"),
            };
            return Ok(skipped(case, detail));
        }
        completed += 1;
        logical = logical.saturating_add((case.members * case.payload_bytes) as u64);
        physical = physical.saturating_add(delta(
            after.authoritative_write_bytes,
            before.authoritative_write_bytes,
        ));
        writes = writes.saturating_add(delta(
            after.authoritative_write_operations,
            before.authoritative_write_operations,
        ));
        syncs = syncs.saturating_add(delta(
            after.authoritative_sync_operations,
            before.authoritative_sync_operations,
        ));
        cohorts =
            cohorts.saturating_add(delta(after.durability_cohorts, before.durability_cohorts));
        elapsed_total = elapsed_total.saturating_add(elapsed);
        samples.end_to_end.push(nanos(elapsed));
        samples
            .lock_wait
            .push(delta(after.store_lock_wait_ns, before.store_lock_wait_ns));
        samples
            .catalog_open
            .push(delta(after.catalog_open_ns, before.catalog_open_ns));
        samples
            .validation
            .push(delta(after.validation_ns, before.validation_ns));
        samples
            .member_boundary
            .push(delta(after.member_boundary_ns, before.member_boundary_ns));
        samples.decision_boundary.push(delta(
            after.decision_boundary_ns,
            before.decision_boundary_ns,
        ));
        samples
            .publication
            .push(delta(after.publication_ns, before.publication_ns));
    }

    let seconds = elapsed_total.as_secs_f64().max(f64::MIN_POSITIVE);
    Ok(CellReport {
        members: case.members,
        payload_bytes_per_member: case.payload_bytes,
        status: "completed",
        detail: None,
        completed,
        logical_value_bytes: logical,
        physical_write_bytes: physical,
        write_amplification: (logical != 0).then_some(physical as f64 / logical as f64),
        authoritative_write_operations: writes,
        authoritative_sync_operations: syncs,
        durability_cohorts: cohorts,
        commits_per_second: completed as f64 / seconds,
        member_mutations_per_second: (completed * case.members) as f64 / seconds,
        end_to_end_ns: percentiles(samples.end_to_end),
        store_lock_wait_ns: percentiles(samples.lock_wait),
        catalog_open_ns: percentiles(samples.catalog_open),
        validation_ns: percentiles(samples.validation),
        member_boundary_ns: percentiles(samples.member_boundary),
        decision_boundary_ns: percentiles(samples.decision_boundary),
        publication_ns: percentiles(samples.publication),
    })
}

fn skipped(case: Case, detail: String) -> CellReport {
    CellReport {
        members: case.members,
        payload_bytes_per_member: case.payload_bytes,
        status: "skipped",
        detail: Some(detail),
        completed: 0,
        logical_value_bytes: 0,
        physical_write_bytes: 0,
        write_amplification: None,
        authoritative_write_operations: 0,
        authoritative_sync_operations: 0,
        durability_cohorts: 0,
        commits_per_second: 0.0,
        member_mutations_per_second: 0.0,
        end_to_end_ns: Percentiles::default(),
        store_lock_wait_ns: Percentiles::default(),
        catalog_open_ns: Percentiles::default(),
        validation_ns: Percentiles::default(),
        member_boundary_ns: Percentiles::default(),
        decision_boundary_ns: Percentiles::default(),
        publication_ns: Percentiles::default(),
    }
}

fn cases(profile: Profile) -> Vec<Case> {
    match profile {
        Profile::Dipstick => vec![
            Case {
                members: 1,
                payload_bytes: 0,
            },
            Case {
                members: 1,
                payload_bytes: 8 * 1024,
            },
            Case {
                members: 1,
                payload_bytes: 1024 * 1024,
            },
            Case {
                members: 3,
                payload_bytes: 256,
            },
            Case {
                members: 10,
                payload_bytes: 8 * 1024,
            },
            Case {
                members: 64,
                payload_bytes: 256,
            },
            Case {
                members: 256,
                payload_bytes: 0,
            },
        ],
        Profile::MemberPayload => {
            let mut cases = Vec::new();
            for members in [1, 2, 3, 10, 64, 256] {
                for payload_bytes in [0, 256, 8 * 1024, 128 * 1024, 1024 * 1024] {
                    cases.push(Case {
                        members,
                        payload_bytes,
                    });
                }
            }
            cases
        }
    }
}

fn prepare_deployment(root: &Path) -> Result<HeapCap, String> {
    let deployment = ResidiuumDeployment::create(root).map_err(|error| error.to_string())?;
    let layout = HeapMetaLayout::new(root);
    let deployment_id = DeploymentId::new_random().map_err(|error| error.to_string())?;
    let heap_id = HeapId::new_random().map_err(|error| error.to_string())?;
    let collection_seed = *residiuum_heap::CollectionId::new_random()
        .map_err(|error| error.to_string())?
        .as_bytes();
    let staged = stage_heap_genesis(
        &layout,
        *deployment_id.as_bytes(),
        *heap_id.as_bytes(),
        collection_seed,
        "atomic-qualification",
    )
    .map_err(|error| error.to_string())?;
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash)
        .map_err(|error| error.to_string())?;
    let capability = mint_cap_for(heap_id, deployment_id)?;
    drop(deployment);
    Ok(capability)
}

fn mint_cap_for(heap: HeapId, deployment: DeploymentId) -> Result<HeapCap, String> {
    let snapshot = HeapSecuritySnapshot {
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).map_err(|error| error.to_string())?,
        authority_generation: AuthorityGeneration::new(1).map_err(|error| error.to_string())?,
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key: [7; 32],
        previous_master_public_key: None,
        security_revision: SecurityRevision::new(1).map_err(|error| error.to_string())?,
        authority_chain_head_hash: [9; 32],
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    };
    let certificate = VerifiedCertificate {
        cose_bytes: vec![1],
        fingerprint: [3; 32],
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).map_err(|error| error.to_string())?,
        authority_generation: AuthorityGeneration::new(1).map_err(|error| error.to_string())?,
        certificate_id: CertificateId::new_random().map_err(|error| error.to_string())?,
        holder_public_key: [4; 32],
        rights: Rights::from_bits_certificate(0x0d).map_err(|error| error.to_string())?,
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: [5; 32],
    };
    mint_capability(
        Arc::new(HeapSlot::new(snapshot)),
        &certificate,
        TrustedInstant {
            unix_s: 1_700_000_000,
        },
    )
    .map_err(|error| error.to_string())
}

fn store_stats(client: &Client) -> Result<AtomicStoreStats, String> {
    client
        .inspect()
        .atomics
        .store
        .ok_or_else(|| "physical Atomic telemetry unavailable".into())
}

fn delta(after: u64, before: u64) -> u64 {
    after.saturating_sub(before)
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn percentiles(mut samples: Vec<u64>) -> Percentiles {
    if samples.is_empty() {
        return Percentiles::default();
    }
    samples.sort_unstable();
    Percentiles {
        p50: percentile(&samples, 50),
        p95: percentile(&samples, 95),
        p99: percentile(&samples, 99),
        max: *samples.last().unwrap_or(&0),
    }
}

fn percentile(samples: &[u64], percentile: usize) -> u64 {
    let index = (samples.len().saturating_sub(1) * percentile).div_ceil(100);
    samples[index.min(samples.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_matrix_has_all_member_payload_cells() {
        let matrix = cases(Profile::MemberPayload);
        assert_eq!(matrix.len(), 30);
        assert!(matrix
            .iter()
            .any(|cell| cell.members == 256 && cell.payload_bytes == 1024 * 1024));
    }

    #[test]
    fn percentiles_use_nearest_rank_without_exceeding_bounds() {
        let measured = percentiles(vec![50, 10, 40, 20, 30]);
        assert_eq!(measured.p50, 30);
        assert_eq!(measured.p95, 50);
        assert_eq!(measured.p99, 50);
        assert_eq!(measured.max, 50);
        assert_eq!(percentiles(Vec::new()).max, 0);
    }
}
