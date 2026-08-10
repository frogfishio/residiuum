//! HP-006 Accept: crash-resumable heap migration; phase 6 refuses unlabelled frames.

use residiuum_store::{
    arm_failpoint_once, clear_failpoints, CutoverGate, FailpointAction, HeapMigrationJob,
    InventoryFrame, InventorySegment, MigrationPhase, SourceInventory,
};
use std::sync::{Mutex, MutexGuard, OnceLock};
use tempfile::TempDir;

fn failpoint_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

fn uuidish(seed: u8) -> [u8; 16] {
    let mut id = [seed; 16];
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    id
}

fn hash_bytes(label: &str) -> [u8; 32] {
    *blake3::hash(label.as_bytes()).as_bytes()
}

fn sample_inventory(unlabelled: usize, labelled: usize, quarantine: usize) -> SourceInventory {
    let seg = uuidish(0x10);
    let mut frames = Vec::new();
    let mut n = 0u8;
    for _ in 0..unlabelled {
        n = n.wrapping_add(1);
        frames.push(InventoryFrame {
            frame_id: uuidish(0x20u8.wrapping_add(n)),
            segment_id: seg,
            content_hash: hash_bytes(&format!("u{n}")),
            labelled: false,
            quarantine: false,
        });
    }
    for _ in 0..labelled {
        n = n.wrapping_add(1);
        frames.push(InventoryFrame {
            frame_id: uuidish(0x20u8.wrapping_add(n)),
            segment_id: seg,
            content_hash: hash_bytes(&format!("l{n}")),
            labelled: true,
            quarantine: false,
        });
    }
    for _ in 0..quarantine {
        n = n.wrapping_add(1);
        frames.push(InventoryFrame {
            frame_id: uuidish(0x20u8.wrapping_add(n)),
            segment_id: seg,
            content_hash: hash_bytes(&format!("q{n}")),
            labelled: false,
            quarantine: true,
        });
    }
    SourceInventory {
        segments: vec![InventorySegment {
            segment_id: seg,
            byte_length: 100,
            content_hash: hash_bytes("seg"),
        }],
        frames,
    }
}

fn run_to_dual_read(root: &std::path::Path, inv: SourceInventory) -> HeapMigrationJob {
    let mut job =
        HeapMigrationJob::begin_preflight(root, uuidish(0x01), uuidish(0x02), uuidish(0x03), inv)
            .unwrap();
    job.run_establish_and_identify(&["users", "orders"])
        .unwrap();
    assert_eq!(job.state().phase, MigrationPhase::DualRead);
    job
}

#[test]
fn phase6_refuses_unlabelled_active_frames() {
    let _failpoint_guard = failpoint_lock();
    clear_failpoints();
    let tmp = TempDir::new().unwrap();
    let inv = sample_inventory(2, 0, 0);
    let mut job = run_to_dual_read(tmp.path(), inv);
    let gate = job.cutover_gate();
    assert_eq!(gate.unlabelled_active_frames, 2);
    assert!(!gate.allows_cutover());

    job.run_rewrite().unwrap();
    let gate = job.cutover_gate();
    assert_eq!(gate.unlabelled_active_frames, 0);
    assert!(gate.allows_cutover());
    assert_eq!(
        gate.source_frames,
        gate.rewritten_frames + gate.intentionally_quarantined_frames
    );
    job.run_verify_and_cutover().unwrap();
    assert_eq!(job.state().phase, MigrationPhase::CutOver);
    job.run_quarantine().unwrap();
    assert_eq!(job.state().phase, MigrationPhase::Quarantine);
    clear_failpoints();
}

#[test]
fn crash_injection_converges_without_duplicate_or_lost_frames() {
    let _failpoint_guard = failpoint_lock();
    clear_failpoints();
    let tmp = TempDir::new().unwrap();
    let inv = sample_inventory(3, 1, 1);
    let source_count = inv.source_frame_count();

    let mut job = HeapMigrationJob::begin_preflight(
        tmp.path(),
        uuidish(0x11),
        uuidish(0x12),
        uuidish(0x13),
        inv.clone(),
    )
    .unwrap();
    let job_id = job.state().job_id;
    job.run_establish_and_identify(&["c1"]).unwrap();

    let failures = Mutex::new(0u32);
    loop {
        clear_failpoints();
        let n = {
            let mut g = failures.lock().unwrap();
            *g += 1;
            *g
        };
        if n <= 4 {
            arm_failpoint_once("heap_migration.after_frame_admit", FailpointAction::Error);
        }
        match job.run_rewrite() {
            Ok(()) => break,
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("failpoint") || msg.contains("Failpoint"),
                    "unexpected error: {msg}"
                );
                job = HeapMigrationJob::open(tmp.path(), job_id, inv.clone()).unwrap();
                assert_eq!(job.state().phase, MigrationPhase::Rewrite);
            }
        }
        if n > 20 {
            panic!("did not converge");
        }
    }

    let gate = job.cutover_gate();
    assert_eq!(gate.source_frames, source_count);
    assert_eq!(
        gate.rewritten_frames + gate.intentionally_quarantined_frames,
        source_count
    );
    assert_eq!(gate.unlabelled_active_frames, 0);
    assert!(gate.allows_cutover());

    clear_failpoints();
    arm_failpoint_once("heap_migration.after_phase_advance", FailpointAction::Error);
    let cut_err = job.run_verify_and_cutover();
    if cut_err.is_err() {
        job = HeapMigrationJob::open(tmp.path(), job_id, inv.clone()).unwrap();
        if job.state().phase == MigrationPhase::Verify {
            job.run_verify_and_cutover().unwrap();
        } else {
            assert_eq!(job.state().phase, MigrationPhase::CutOver);
        }
    }
    assert_eq!(job.state().phase, MigrationPhase::CutOver);

    let rewritten = job.state().rewritten_frames;
    let quarantined = job.state().quarantined_frames;
    assert_eq!(rewritten + quarantined, source_count);

    let final_gate = CutoverGate {
        source_frames: source_count,
        rewritten_frames: rewritten,
        intentionally_quarantined_frames: quarantined,
        unlabelled_active_frames: 0,
        cross_heap_segments: 0,
    };
    assert!(final_gate.allows_cutover());
    clear_failpoints();
}

#[test]
fn inventory_hash_rejects_duplicate_segment_ids() {
    let seg = uuidish(0x55);
    let inv = SourceInventory {
        segments: vec![
            InventorySegment {
                segment_id: seg,
                byte_length: 1,
                content_hash: hash_bytes("a"),
            },
            InventorySegment {
                segment_id: seg,
                byte_length: 2,
                content_hash: hash_bytes("b"),
            },
        ],
        frames: vec![],
    };
    assert!(inv.inventory_hash().is_err());
}
