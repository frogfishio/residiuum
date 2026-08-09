//! Explicit retained-media campaign for the production smart-client write path.
//!
//! This is ignored by ordinary test runs. The caller must supply a dedicated,
//! empty root and an explicit logical-byte target.

use residiuum_authority::{
    bootstrap_development_file_product, DevelopmentFileProductBootstrap, ProductHeapRequest,
};
use residiuum_heap::Rights;
use residiuum_sdk::driver::{
    Client, Collection, CreateCollectionOptions, EmbeddedOptions, ScanOptions, MAX_SCAN_PAGE_SIZE,
};
use residiuum_sdk::ResidiuumDeployment;
use serde_json::{json, Value};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

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
    let minimum_free = std::env::var("RESIDIUUM_SMART_DURABLE_MIN_FREE_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30 * GIB);
    assert!(logical_target > 0);
    assert!(payload_bytes > 0);
    assert!(concurrency > 0);
    let free_at_start = available_bytes(&root);
    assert!(
        free_at_start >= minimum_free,
        "insufficient free space: available={free_at_start} required={minimum_free}"
    );

    let store = root.join("store");
    let authority = root.join("authority");
    let credentials = root.join("credentials/smart-durable.v1.cbor");
    drop(ResidiuumDeployment::create(&store).unwrap());
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
            .workers(concurrency)
            .queue_capacity(concurrency.saturating_mul(4)),
    ))
    .unwrap();
    let open_ns = open_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    let heap = block_on(client.open_named_heap("campaign", capability)).unwrap();
    let records: Collection<Value> = block_on(
        heap.create_collection("records", CreateCollectionOptions::default()),
    )
    .unwrap();

    let documents = payload_pool(payload_bytes);
    let total_operations = logical_target
        .saturating_add(payload_bytes as u64 - 1)
        .saturating_div(payload_bytes as u64);
    let acknowledged_payload = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
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
                let document = &documents[(operation as usize) & (documents.len() - 1)];
                let started = Instant::now();
                block_on(records.put(format!("record-{operation:016}"), document.as_ref()))
                    .map_err(|error| error.to_string())?;
                latencies.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
                let before = acknowledged_payload.fetch_add(payload_bytes as u64, Ordering::Relaxed);
                let after = before.saturating_add(payload_bytes as u64);
                if before / GIB != after / GIB {
                    eprintln!("acknowledged_payload_gib={}", after / GIB);
                }
                operation = operation.saturating_add(concurrency as u64);
            }
            Ok(latencies)
        }));
    }

    let acknowledgement_started = Instant::now();
    barrier.wait();
    let mut latencies = Vec::with_capacity(total_operations.min(usize::MAX as u64) as usize);
    for handle in handles {
        latencies.extend(handle.join().expect("writer thread panicked").expect("write failed"));
    }
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
        EmbeddedOptions::new(&store).workers(concurrency).queue_capacity(concurrency * 4),
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
