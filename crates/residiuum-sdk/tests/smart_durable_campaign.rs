//! Explicit retained-media campaign for the production smart-client write path.
//!
//! This is ignored by ordinary test runs. The caller must supply a dedicated,
//! empty root and an explicit logical-byte target.

#![recursion_limit = "256"]

use residiuum_authority::{
    bootstrap_development_file_product, DevelopmentFileProductBootstrap, ProductHeapRequest,
};
use residiuum_heap::Rights;
use residiuum_sdk::driver::{
    Client, ClientInspection, Collection, CreateCollectionOptions, EmbeddedOptions, PutManyEntry,
    ScanOptions, DEFAULT_EMBEDDED_QUEUE_CAPACITY, MAX_SCAN_PAGE_SIZE,
};
use residiuum_sdk::ResidiuumDeployment;
use residiuum_store::{RecoveryMode, StoreHost};
use serde_json::{json, Value};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

const GIB: u64 = 1024 * 1024 * 1024;

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

fn required_u64(name: &str) -> u64 {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set explicitly"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an unsigned integer"))
}

fn optional_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) if value == "1" || value.eq_ignore_ascii_case("true") => true,
        Ok(value) if value == "0" || value.eq_ignore_ascii_case("false") => false,
        Ok(_) => panic!("{name} must be true, false, 1, or 0"),
        Err(_) => default,
    }
}

fn campaign_root() -> PathBuf {
    let root = PathBuf::from(
        std::env::var("RESIDIUUM_SMART_DURABLE_ROOT")
            .expect("RESIDIUUM_SMART_DURABLE_ROOT must be set"),
    );
    assert!(root.is_absolute(), "campaign root must be absolute");
    assert!(
        root.components().count() >= 4,
        "campaign root is too broad: {}",
        root.display()
    );
    if root.exists() {
        assert!(root.is_dir(), "campaign root must be a directory");
        assert!(
            std::fs::read_dir(&root).unwrap().next().is_none(),
            "campaign root must be empty: {}",
            root.display()
        );
    } else {
        std::fs::create_dir_all(&root).unwrap();
    }
    std::fs::write(root.join(".residiuum-smart-durable-campaign"), b"v1\n").unwrap();
    root
}

fn application_rights() -> Rights {
    Rights::from_bits_certificate(
        Rights::READ.bits()
            | Rights::WRITE.bits()
            | Rights::INDEX_ADMIN.bits()
            | Rights::HEAP_ADMIN.bits(),
    )
    .unwrap()
}

fn available_bytes(path: &Path) -> u64 {
    let output = std::process::Command::new("df")
        .args(["-Pk"])
        .arg(path)
        .output()
        .expect("run df");
    assert!(output.status.success(), "df failed");
    let text = String::from_utf8(output.stdout).unwrap();
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .next_back()
        .and_then(|line| line.split_whitespace().nth(3))
        .and_then(|value| value.parse::<u64>().ok())
        .expect("parse df available bytes")
        .saturating_mul(1024)
}

fn resident_bytes() -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p"])
        .arg(std::process::id().to_string())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|kib| kib.saturating_mul(1024))
}

fn payload_pool(payload_bytes: usize) -> Arc<Vec<Arc<Value>>> {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut documents = Vec::with_capacity(1024);
    for variant in 0..1024u64 {
        state ^= variant.wrapping_mul(0xd6e8_feb8_6659_fd93);
        let mut payload = String::with_capacity(payload_bytes);
        for _ in 0..payload_bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            payload.push(ALPHABET[(state as usize) & 63] as char);
        }
        documents.push(Arc::new(json!({"payload": payload})));
    }
    Arc::new(documents)
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len() - 1).saturating_mul(percentile) / 100;
    sorted[index]
}

fn directory_sizes(path: &Path) -> (u64, u64) {
    let mut logical = 0u64;
    let mut allocated = 0u64;
    let mut pending = vec![path.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(next).unwrap() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                logical = logical.saturating_add(metadata.len());
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    allocated = allocated.saturating_add(metadata.blocks().saturating_mul(512));
                }
                #[cfg(not(unix))]
                {
                    allocated = allocated.saturating_add(metadata.len());
                }
            }
        }
    }
    (logical, allocated)
}

fn delta(after: u64, before: u64) -> u64 {
    after.saturating_sub(before)
}

fn sustained_sample(
    milestone_payload_bytes: u64,
    observed_payload_bytes: u64,
    elapsed_ns: u64,
    previous_payload_bytes: u64,
    previous_elapsed_ns: u64,
    free_bytes: u64,
    resident_bytes: Option<u64>,
    previous: &ClientInspection,
    current: &ClientInspection,
) -> Value {
    let interval_bytes = observed_payload_bytes.saturating_sub(previous_payload_bytes);
    let interval_ns = elapsed_ns.saturating_sub(previous_elapsed_ns);
    let interval_seconds = interval_ns as f64 / 1_000_000_000.0;
    let commits = current.operation_commits;
    let prior_commits = previous.operation_commits;
    let write_path = current.write_path;
    let prior_write_path = previous.write_path;
    json!({
        "milestone_payload_bytes": milestone_payload_bytes,
        "observed_payload_bytes": observed_payload_bytes,
        "elapsed_ns": elapsed_ns,
        "interval_payload_bytes": interval_bytes,
        "interval_ns": interval_ns,
        "interval_payload_mib_per_second": if interval_seconds > 0.0 {
            interval_bytes as f64 / interval_seconds / (1024.0 * 1024.0)
        } else { 0.0 },
        "cumulative_payload_mib_per_second": if elapsed_ns > 0 {
            observed_payload_bytes as f64 / (elapsed_ns as f64 / 1_000_000_000.0)
                / (1024.0 * 1024.0)
        } else { 0.0 },
        "free_bytes": free_bytes,
        "resident_bytes": resident_bytes,
        "scheduler": {
            "queued": current.queued,
            "running": current.running,
            "inflight_mutations": current.inflight_mutations,
            "admitted_bytes": current.admitted_bytes,
            "refused_delta": delta(current.refused, previous.refused),
            "byte_refused_delta": delta(current.byte_refused, previous.byte_refused),
        },
        "group_commit_delta": {
            "submitted": delta(commits.submitted, prior_commits.submitted),
            "submitted_bytes": delta(commits.submitted_bytes, prior_commits.submitted_bytes),
            "cohorts": delta(commits.cohorts, prior_commits.cohorts),
            "committed": delta(commits.committed, prior_commits.committed),
            "failed": delta(commits.failed, prior_commits.failed),
            "byte_admission_waits": delta(
                commits.byte_admission_waits,
                prior_commits.byte_admission_waits,
            ),
            "reserved_cooked_cohorts": delta(
                commits.reserved_cooked_cohorts,
                prior_commits.reserved_cooked_cohorts,
            ),
            "reserved_cooked_operations": delta(
                commits.reserved_cooked_operations,
                prior_commits.reserved_cooked_operations,
            ),
            "overlapped_cooked_cohorts": delta(
                commits.overlapped_cooked_cohorts,
                prior_commits.overlapped_cooked_cohorts,
            ),
            "overlapped_cooked_operations": delta(
                commits.overlapped_cooked_operations,
                prior_commits.overlapped_cooked_operations,
            ),
            "phase_ns": {
                "total": delta(commits.cohort_total_ns, prior_commits.cohort_total_ns),
                "prepare": delta(commits.cohort_prepare_ns, prior_commits.cohort_prepare_ns),
                "cook_install_publish": delta(
                    commits.cohort_cook_install_publish_ns,
                    prior_commits.cohort_cook_install_publish_ns,
                ),
                "media_boundary": delta(
                    commits.cohort_media_boundary_ns,
                    prior_commits.cohort_media_boundary_ns,
                ),
                "rotation": delta(commits.cohort_rotation_ns, prior_commits.cohort_rotation_ns),
                "outcome_journal": delta(
                    commits.cohort_outcome_journal_ns,
                    prior_commits.cohort_outcome_journal_ns,
                ),
            },
        },
        "write_path": write_path.map(|path| {
            let prior = prior_write_path.unwrap_or_default();
            json!({
                "current": {
                    "primary_index_entries": path.primary_index_entries,
                    "durable_index_entries": path.durable_index_entries,
                    "write_dedup_entries": path.write_dedup_entries,
                    "write_dedup_capacity": path.write_dedup_capacity,
                    "derived_ops_since_checkpoint": path.derived_ops_since_checkpoint,
                    "enrichment_backlog": path.enrichment_backlog,
                    "async_lifecycle_enabled": path.async_lifecycle_enabled,
                    "shadow_dual_stream": path.shadow_dual_stream,
                },
                "authoritative_io_delta": {
                    "write_operations": delta(
                        path.authoritative_io.write_operations,
                        prior.authoritative_io.write_operations,
                    ),
                    "write_bytes": delta(
                        path.authoritative_io.write_bytes,
                        prior.authoritative_io.write_bytes,
                    ),
                    "write_ns": delta(
                        path.authoritative_io.write_ns,
                        prior.authoritative_io.write_ns,
                    ),
                    "sync_operations": delta(
                        path.authoritative_io.sync_operations,
                        prior.authoritative_io.sync_operations,
                    ),
                    "sync_ns": delta(
                        path.authoritative_io.sync_ns,
                        prior.authoritative_io.sync_ns,
                    ),
                },
                "shadow_io_delta": {
                    "write_operations": delta(
                        path.shadow_io.write_operations,
                        prior.shadow_io.write_operations,
                    ),
                    "write_bytes": delta(
                        path.shadow_io.write_bytes,
                        prior.shadow_io.write_bytes,
                    ),
                    "write_ns": delta(
                        path.shadow_io.write_ns,
                        prior.shadow_io.write_ns,
                    ),
                    "sync_operations": delta(
                        path.shadow_io.sync_operations,
                        prior.shadow_io.sync_operations,
                    ),
                    "sync_ns": delta(
                        path.shadow_io.sync_ns,
                        prior.shadow_io.sync_ns,
                    ),
                },
                "rotation_delta": {
                    "rotations": delta(path.rotation.rotations, prior.rotation.rotations),
                    "flush_ns": delta(path.rotation.flush_ns, prior.rotation.flush_ns),
                    "rename_pending_ns": delta(
                        path.rotation.rename_pending_ns,
                        prior.rotation.rename_pending_ns,
                    ),
                    "start_active_ns": delta(
                        path.rotation.start_active_ns,
                        prior.rotation.start_active_ns,
                    ),
                    "backpressure_wait_ns": delta(
                        path.rotation.backpressure_wait_ns,
                        prior.rotation.backpressure_wait_ns,
                    ),
                    "auth_publish_ns": delta(
                        path.rotation.auth_publish_ns,
                        prior.rotation.auth_publish_ns,
                    ),
                    "catalog_apply_ns": delta(
                        path.rotation.catalog_apply_ns,
                        prior.rotation.catalog_apply_ns,
                    ),
                },
                "shadow_delta": {
                    "published": delta(
                        path.shadow_dual_published,
                        prior.shadow_dual_published,
                    ),
                    "finalize_ns": delta(
                        path.shadow_dual_finalize_ns,
                        prior.shadow_dual_finalize_ns,
                    ),
                },
                "enrichment_delta": {
                    "samples": delta(path.enrichment.samples, prior.enrichment.samples),
                    "gap_wait_ns": delta(
                        path.enrichment.gap_wait_ns,
                        prior.enrichment.gap_wait_ns,
                    ),
                    "read_ns": delta(path.enrichment.read_ns, prior.enrichment.read_ns),
                    "decode_ns": delta(path.enrichment.decode_ns, prior.enrichment.decode_ns),
                    "blake3_ns": delta(path.enrichment.blake3_ns, prior.enrichment.blake3_ns),
                    "hydra_construct_ns": delta(
                        path.enrichment.hydra_construct_ns,
                        prior.enrichment.hydra_construct_ns,
                    ),
                    "hydra_persist_ns": delta(
                        path.enrichment.hydra_persist_ns,
                        prior.enrichment.hydra_persist_ns,
                    ),
                    "chimera_construct_ns": delta(
                        path.enrichment.chimera_construct_ns,
                        prior.enrichment.chimera_construct_ns,
                    ),
                    "chimera_persist_ns": delta(
                        path.enrichment.chimera_persist_ns,
                        prior.enrichment.chimera_persist_ns,
                    ),
                    "catalog_ns": delta(
                        path.enrichment.catalog_ns,
                        prior.enrichment.catalog_ns,
                    ),
                    "wall_ns": delta(path.enrichment.wall_ns, prior.enrichment.wall_ns),
                    "cpu_ns": delta(path.enrichment.cpu_ns, prior.enrichment.cpu_ns),
                    "bytes_read": delta(
                        path.enrichment.bytes_read,
                        prior.enrichment.bytes_read,
                    ),
                    "bytes_written": delta(
                        path.enrichment.bytes_written,
                        prior.enrichment.bytes_written,
                    ),
                },
            })
        }),
    })
}

#[test]
#[ignore = "explicit retained-media campaign; requires dedicated root and byte target"]
fn smart_client_durable_retained_media_campaign() {
    let root = campaign_root();
    let logical_target = required_u64("RESIDIUUM_SMART_DURABLE_LOGICAL_BYTES");
    let payload_bytes = std::env::var("RESIDIUUM_SMART_DURABLE_PAYLOAD_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(8 * 1024);
    let concurrency = std::env::var("RESIDIUUM_SMART_DURABLE_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20);
    let read_workers = std::env::var("RESIDIUUM_SMART_DURABLE_READ_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4);
    let client_batch = std::env::var("RESIDIUUM_SMART_DURABLE_CLIENT_BATCH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    let enrichment_enabled =
        optional_bool("RESIDIUUM_SMART_DURABLE_ENRICHMENT_ENABLED", true);
    let recovery_mode = match std::env::var("RESIDIUUM_SMART_DURABLE_RECOVERY_MODE")
        .as_deref()
        .unwrap_or("compact_shadow")
    {
        "compact_shadow" => RecoveryMode::CompactShadow,
        "materialized" => RecoveryMode::Materialized,
        _ => panic!(
            "RESIDIUUM_SMART_DURABLE_RECOVERY_MODE must be compact_shadow or materialized"
        ),
    };
    let minimum_free = std::env::var("RESIDIUUM_SMART_DURABLE_MIN_FREE_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30 * GIB);
    let scheduler_queue_capacity = std::env::var("RESIDIUUM_SMART_DURABLE_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_else(|| {
            concurrency
                .saturating_mul(client_batch.max(4))
                .max(DEFAULT_EMBEDDED_QUEUE_CAPACITY)
        });
    assert!(logical_target > 0);
    assert!(payload_bytes > 0);
    assert!(concurrency > 0);
    assert!(read_workers > 0);
    assert!(client_batch > 0);
    let free_at_start = available_bytes(&root);
    assert!(
        free_at_start >= minimum_free,
        "insufficient free space: available={free_at_start} required={minimum_free}"
    );

    let store = root.join("store");
    let authority = root.join("authority");
    let credentials = root.join("credentials/smart-durable.v1.cbor");
    match recovery_mode {
        RecoveryMode::CompactShadow => drop(ResidiuumDeployment::create(&store).unwrap()),
        RecoveryMode::Materialized => {
            drop(StoreHost::create_with_recovery_mode(
                &store,
                RecoveryMode::Materialized,
            ).unwrap())
        }
        RecoveryMode::Transitioning => unreachable!("campaign never creates transitioning mode"),
    }
    let bootstrap_config = DevelopmentFileProductBootstrap::new(
        &store,
        &authority,
        &credentials,
        "io.frogfish.residiuum.smart-durable-campaign",
        vec![ProductHeapRequest::new("campaign", application_rights())],
    );
    let bootstrapped = bootstrap_development_file_product(&bootstrap_config).unwrap();
    let capability = bootstrapped.heaps[0].capability.clone();
    drop(bootstrapped);

    let open_started = Instant::now();
    let client = block_on(Client::open_embedded(
        EmbeddedOptions::new(&store)
            .workers(read_workers)
            .queue_capacity(scheduler_queue_capacity)
            .enrichment_enabled(enrichment_enabled),
    ))
    .unwrap();
    let open_ns = open_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    let heap = block_on(client.open_named_heap("campaign", capability)).unwrap();
    let records: Collection<Value> =
        block_on(heap.create_collection("records", CreateCollectionOptions::default())).unwrap();

    let documents = payload_pool(payload_bytes);
    let total_operations = logical_target
        .saturating_add(payload_bytes as u64 - 1)
        .saturating_div(payload_bytes as u64);
    let acknowledged_payload = Arc::new(AtomicU64::new(0));
    let monitor_stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(concurrency + 2));
    let mut handles = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let records = records.clone();
        let documents = Arc::clone(&documents);
        let acknowledged_payload = Arc::clone(&acknowledged_payload);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || -> Result<Vec<u64>, String> {
            let mut latencies = Vec::new();
            barrier.wait();
            let mut operation = worker as u64;
            while operation < total_operations {
                let started = Instant::now();
                let submitted = if client_batch == 1 {
                    let document = &documents[(operation as usize) & (documents.len() - 1)];
                    block_on(records.put(format!("record-{operation:016}"), document.as_ref()))
                        .map_err(|error| error.to_string())?;
                    operation = operation.saturating_add(concurrency as u64);
                    1
                } else {
                    let mut entries = Vec::with_capacity(client_batch);
                    while operation < total_operations && entries.len() < client_batch {
                        let document = documents[(operation as usize) & (documents.len() - 1)]
                            .as_ref()
                            .clone();
                        entries.push(PutManyEntry::new(
                            format!("record-{operation:016}"),
                            document,
                        ));
                        operation = operation.saturating_add(concurrency as u64);
                    }
                    let outcomes =
                        block_on(records.put_many(entries)).map_err(|error| error.to_string())?;
                    let submitted = outcomes.len();
                    for outcome in outcomes {
                        outcome
                            .result
                            .map_err(|error| format!("bulk key {}: {error}", outcome.key))?;
                    }
                    submitted
                };
                let latency = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                latencies.extend(std::iter::repeat(latency).take(submitted));
                for _ in 0..submitted {
                    let before =
                        acknowledged_payload.fetch_add(payload_bytes as u64, Ordering::Relaxed);
                    let after = before.saturating_add(payload_bytes as u64);
                    if before / GIB != after / GIB {
                        eprintln!("acknowledged_payload_gib={}", after / GIB);
                    }
                }
            }
            Ok(latencies)
        }));
    }

    let acknowledgement_started = Instant::now();
    let monitor = {
        let client = client.clone();
        let root = root.clone();
        let acknowledged_payload = Arc::clone(&acknowledged_payload);
        let monitor_stop = Arc::clone(&monitor_stop);
        let barrier = Arc::clone(&barrier);
        let mut previous = client.inspect();
        std::thread::spawn(move || {
            let mut samples = Vec::new();
            let mut next_milestone = GIB;
            let mut previous_payload = 0u64;
            let mut previous_elapsed_ns = 0u64;
            barrier.wait();
            loop {
                let observed = acknowledged_payload.load(Ordering::Acquire);
                let stopping = monitor_stop.load(Ordering::Acquire);
                let reached_milestone = observed >= next_milestone;
                let needs_final_sample = stopping && observed > previous_payload;
                if reached_milestone || needs_final_sample {
                    let milestone = if reached_milestone {
                        next_milestone
                    } else {
                        observed
                    };
                    let elapsed_ns = acknowledgement_started
                        .elapsed()
                        .as_nanos()
                        .min(u64::MAX as u128) as u64;
                    let current = client.inspect();
                    let sample = sustained_sample(
                        milestone,
                        observed,
                        elapsed_ns,
                        previous_payload,
                        previous_elapsed_ns,
                        available_bytes(&root),
                        resident_bytes(),
                        &previous,
                        &current,
                    );
                    eprintln!(
                        "sustained_sample={}",
                        serde_json::to_string(&sample).unwrap()
                    );
                    samples.push(sample);
                    previous = current;
                    previous_payload = observed;
                    previous_elapsed_ns = elapsed_ns;
                    next_milestone = observed
                        .saturating_div(GIB)
                        .saturating_add(1)
                        .saturating_mul(GIB);
                }
                if stopping && observed <= previous_payload {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            samples
        })
    };
    barrier.wait();
    let mut latencies = Vec::with_capacity(total_operations.min(usize::MAX as u64) as usize);
    for handle in handles {
        latencies.extend(
            handle
                .join()
                .expect("writer thread panicked")
                .expect("write failed"),
        );
    }
    monitor_stop.store(true, Ordering::Release);
    let sustained_samples = monitor.join().expect("monitor thread panicked");
    let acknowledgement_ns = acknowledgement_started
        .elapsed()
        .as_nanos()
        .min(u64::MAX as u128) as u64;
    latencies.sort_unstable();
    let inspection = client.inspect();
    assert_eq!(inspection.operation_commits.submitted, total_operations);
    assert_eq!(inspection.operation_commits.committed, total_operations);
    assert_eq!(inspection.operation_commits.deduplicated, 0);
    assert_eq!(inspection.operation_commits.failed, 0);

    let close_started = Instant::now();
    block_on(client.close()).unwrap();
    drop(records);
    drop(heap);
    drop(client);
    let close_ns = close_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    let (file_logical_bytes_after_close, allocated_bytes_after_close) = directory_sizes(&root);

    let reopen_started = Instant::now();
    let restarted = bootstrap_development_file_product(&bootstrap_config).unwrap();
    let capability = restarted.heaps[0].capability.clone();
    drop(restarted);
    let reopened = block_on(Client::open_embedded(
        EmbeddedOptions::new(&store)
            .workers(read_workers)
            .queue_capacity(scheduler_queue_capacity),
    ))
    .unwrap();
    let reopened_heap = block_on(reopened.open_named_heap("campaign", capability)).unwrap();
    let reopened_records: Collection<Value> =
        block_on(reopened_heap.open_collection("records")).unwrap();
    let reopen_ns = reopen_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;

    let scan_started = Instant::now();
    let mut continuation = None;
    let mut scanned_operations = 0u64;
    let mut scanned_payload = 0u64;
    loop {
        let page = block_on(reopened_records.scan_page(ScanOptions {
            page_size: MAX_SCAN_PAGE_SIZE,
            continuation,
            ..ScanOptions::default()
        }))
        .unwrap();
        assert!(page.complete, "reopen scan reported incomplete coverage");
        assert!(page.incomplete.is_empty());
        for row in page.rows {
            let payload = row.value["payload"].as_str().expect("payload string");
            assert_eq!(payload.len(), payload_bytes);
            scanned_operations = scanned_operations.saturating_add(1);
            scanned_payload = scanned_payload.saturating_add(payload.len() as u64);
        }
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    let scan_ns = scan_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    assert_eq!(scanned_operations, total_operations);
    assert!(scanned_payload >= logical_target);

    block_on(reopened.close()).unwrap();
    drop(reopened_records);
    drop(reopened_heap);
    drop(reopened);

    let seconds = acknowledgement_ns as f64 / 1_000_000_000.0;
    let report = json!({
        "format": "residiuum-smart-durable-campaign-v1",
        "git_commit": std::env::var("RESIDIUUM_CAMPAIGN_COMMIT").ok(),
        "logical_target_bytes": logical_target,
        "acknowledged_payload_bytes": acknowledged_payload.load(Ordering::Relaxed),
        "payload_bytes_per_operation": payload_bytes,
        "operations": total_operations,
        "concurrency": concurrency,
        "client_batch": client_batch,
        "enrichment_enabled": enrichment_enabled,
        "recovery_mode": match recovery_mode {
            RecoveryMode::CompactShadow => "compact_shadow",
            RecoveryMode::Materialized => "materialized",
            RecoveryMode::Transitioning => "transitioning",
        },
        "read_query_workers": read_workers,
        "scheduler_queue_capacity": scheduler_queue_capacity,
        "free_bytes_at_start": free_at_start,
        "file_logical_bytes_after_close": file_logical_bytes_after_close,
        "allocated_bytes_after_close": allocated_bytes_after_close,
        "open_ns": open_ns,
        "acknowledgement_ns": acknowledgement_ns,
        "close_ns": close_ns,
        "reopen_ns": reopen_ns,
        "full_validation_scan_ns": scan_ns,
        "operations_per_second": total_operations as f64 / seconds,
        "payload_mib_per_second": acknowledged_payload.load(Ordering::Relaxed) as f64 / seconds / (1024.0 * 1024.0),
        "sustained_samples": sustained_samples,
        "latency_ns": {
            "min": latencies.first().copied().unwrap_or(0),
            "p50": percentile(&latencies, 50),
            "p95": percentile(&latencies, 95),
            "p99": percentile(&latencies, 99),
            "max": latencies.last().copied().unwrap_or(0),
        },
        "scheduler": {
            "workers": inspection.workers,
            "queue_capacity": inspection.queue_capacity,
            "queue_byte_capacity": inspection.queue_byte_capacity,
            "admitted_bytes": inspection.admitted_bytes,
            "peak_admitted_bytes": inspection.peak_admitted_bytes,
            "byte_refused": inspection.byte_refused,
            "inflight_mutations": inspection.inflight_mutations,
            "peak_inflight_mutations": inspection.peak_inflight_mutations,
            "peak_running": inspection.peak_running,
            "refused": inspection.refused,
        },
        "group_commit": {
            "submitted": inspection.operation_commits.submitted,
            "submitted_bytes": inspection.operation_commits.submitted_bytes,
            "admitted_byte_capacity": inspection.operation_commits.admitted_byte_capacity,
            "admitted_bytes": inspection.operation_commits.admitted_bytes,
            "peak_admitted_bytes": inspection.operation_commits.peak_admitted_bytes,
            "byte_admission_waits": inspection.operation_commits.byte_admission_waits,
            "oversized_admissions": inspection.operation_commits.oversized_admissions,
            "cohorts": inspection.operation_commits.cohorts,
            "committed": inspection.operation_commits.committed,
            "deduplicated": inspection.operation_commits.deduplicated,
            "failed": inspection.operation_commits.failed,
            "successful_media_sync_cohorts": inspection.operation_commits.successful_media_sync_cohorts,
            "successful_journal_sync_cohorts": inspection.operation_commits.successful_journal_sync_cohorts,
            "successful_journal_append_cohorts": inspection.operation_commits.successful_journal_append_cohorts,
            "max_cohort_entries": inspection.operation_commits.max_cohort_entries,
            "max_cohort_bytes": inspection.operation_commits.max_cohort_bytes,
            "parallel_cooked_cohorts": inspection.operation_commits.parallel_cooked_cohorts,
            "parallel_cooked_operations": inspection.operation_commits.parallel_cooked_operations,
            "reserved_cooked_cohorts": inspection.operation_commits.reserved_cooked_cohorts,
            "reserved_cooked_operations": inspection.operation_commits.reserved_cooked_operations,
            "overlapped_cooked_cohorts": inspection.operation_commits.overlapped_cooked_cohorts,
            "overlapped_cooked_operations": inspection.operation_commits.overlapped_cooked_operations,
            "phase_ns_aggregate_may_overlap": inspection.operation_commits.overlapped_cooked_cohorts > 0,
            "phase_ns": {
                "total": inspection.operation_commits.cohort_total_ns,
                "prepare": inspection.operation_commits.cohort_prepare_ns,
                "cook_install_publish": inspection.operation_commits.cohort_cook_install_publish_ns,
                "media_boundary": inspection.operation_commits.cohort_media_boundary_ns,
                "rotation": inspection.operation_commits.cohort_rotation_ns,
                "outcome_journal": inspection.operation_commits.cohort_outcome_journal_ns,
            },
        },
        "reopen_validation": {
            "complete": true,
            "scanned_operations": scanned_operations,
            "scanned_payload_bytes": scanned_payload,
        }
    });
    let report_path = root.join("report.json");
    std::fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    println!("report_path={}", report_path.display());
}
