//! DEF-SCAN-001 forensics: open_inspect a store and classify live-index resolve failures.
//!
//! Usage:
//! ```text
//! cargo run -p residiuum-store --example inspect_unresolved_locators -- /path/to/store
//! ```
//!
//! Does **not** take the exclusive writer lock (safe while a daemon holds the store).

use residiuum_store::{hex16, Store, StoreError};
use std::collections::HashMap;
use std::env;
use std::process;

fn main() {
    let path = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: inspect_unresolved_locators <store-root>");
        process::exit(2);
    });

    let store = match Store::open_inspect(&path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("open_inspect failed: {e}");
            process::exit(1);
        }
    };

    let live = store.live_count();
    let subjects = store.index_live_after(None, None);
    let mut ok = 0u64;
    let mut absent = 0u64;
    let mut by_err: HashMap<String, u64> = HashMap::new();
    let mut sample: Vec<String> = Vec::new();

    for subject in &subjects {
        match store.get_subject_bytes(subject) {
            Ok(Some(_)) => ok += 1,
            Ok(None) => absent += 1,
            Err(e) => {
                let key = err_class(&e);
                *by_err.entry(key.clone()).or_default() += 1;
                if sample.len() < 12 {
                    let subj = String::from_utf8_lossy(subject);
                    sample.push(format!(
                        "{key}: subject_len={} preview={subj:.80?}",
                        subject.len()
                    ));
                }
            }
        }
    }

    println!("store={}", path);
    println!("live_count_api={live}");
    println!("subjects_walked={}", subjects.len());
    println!("resolve_ok={ok}");
    println!("resolve_absent_tombstone={absent}");
    println!("resolve_err_total={}", by_err.values().sum::<u64>());
    let mut classes: Vec<_> = by_err.into_iter().collect();
    classes.sort_by(|a, b| b.1.cmp(&a.1));
    for (k, v) in &classes {
        println!("err_class\t{k}\t{v}");
    }
    if !sample.is_empty() {
        println!("samples:");
        for s in sample {
            println!("  {s}");
        }
    }

    // Media inventory vs index (segment files on disk).
    let paths = residiuum_store::StorePaths::new(&path);
    let on_disk = match std::fs::read_dir(paths.segments_dir()) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .count(),
        Err(_) => 0,
    };
    println!("segments_dir_files={on_disk}");
    let _ = hex16(&[0u8; 16]);
}

fn err_class(e: &StoreError) -> String {
    match e {
        StoreError::SegmentNotFound => "SegmentNotFound".into(),
        StoreError::PayloadPartial => "PayloadPartial".into(),
        StoreError::PayloadConflict => "PayloadConflict".into(),
        StoreError::TierOffline(t) => format!("TierOffline({t})"),
        StoreError::CorruptMeta(m) => format!("CorruptMeta({m})"),
        StoreError::LocatorFault(f) => format!("LocatorFault({f})"),
        other => format!("Other({other})"),
    }
}
