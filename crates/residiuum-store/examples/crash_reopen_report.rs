//! Measure writable reopen and the first operation-bearing mutation separately.
//!
//! The optional `--hold` keeps the process alive after the mutation so an
//! external campaign can SIGKILL it and qualify repeated unclean restarts.

use residiuum_store::{content_identity, DurabilityMode, Store, WriteCondition};
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn main() {
    let mut args = std::env::args_os().skip(1);
    let path = args
        .next()
        .map(PathBuf::from)
        .expect("usage: crash_reopen_report STORE OPERATION_NUMBER [--hold]");
    let operation_number = args
        .next()
        .and_then(|value| value.to_string_lossy().parse::<u64>().ok())
        .expect("OPERATION_NUMBER must be an unsigned integer");
    let hold = args.any(|value| value == "--hold");

    let opened_at = Instant::now();
    let mut store = Store::open(&path).unwrap_or_else(|error| {
        eprintln!("open failed after {} ns: {error}", elapsed_ns(opened_at));
        std::process::exit(1);
    });
    let open_ns = elapsed_ns(opened_at);
    let report = store.open_report();

    let key = format!("__crash_qualification__/{operation_number:016}");
    let body = operation_number.to_le_bytes();
    let mut operation_id = [0u8; 16];
    operation_id[..8].copy_from_slice(&operation_number.to_le_bytes());
    operation_id[8..].copy_from_slice(b"CRASHRQL");
    let hash = content_identity("put", "", &key, &body);
    let mutation_at = Instant::now();
    let (_, replayed) = store
        .put_subject_bytes_with_operation(
            key.as_bytes(),
            &body,
            DurabilityMode::Durable,
            WriteCondition::Unconditional,
            operation_id,
            hash,
        )
        .unwrap_or_else(|error| {
            eprintln!(
                "first mutation failed after {} ns: {error}",
                elapsed_ns(mutation_at)
            );
            std::process::exit(1);
        });
    let first_mutation_ns = elapsed_ns(mutation_at);
    let write_path = store.write_path_stats();

    println!(
        "open_ns={open_ns} first_mutation_ns={first_mutation_ns} replayed={replayed} \
         index_disposition={:?} index_cache_decision={:?} \
         index_full_scan_bytes={} index_sealed_replay_bytes={} index_active_replay_bytes={} \
         pending_seals_recovered={} protected_pairs_recovered={} \
         dedup_recovery_segments_examined={} dedup_recovery_scan_bytes={}",
        report.index_disposition,
        report.index_cache_decision,
        report.index_full_scan_bytes,
        report.index_sealed_replay_bytes,
        report.index_active_replay_bytes,
        report.pending_seals_recovered,
        report.protected_pairs_recovered,
        write_path.write_dedup_recovery_segments_examined,
        write_path.write_dedup_recovery_scan_bytes,
    );
    io::stdout().flush().expect("flush report");

    while hold {
        std::thread::sleep(Duration::from_secs(60));
    }
}
