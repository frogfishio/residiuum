//! AWO-2: credit ledger bounds + cooker isolation + frame equivalence.
//!
//! Gate AWO-G3 labor floor (not package accept).

use residiuum_store::adaptive_write::{
    cook_item_frame, mutation_credit, AdaptiveWriteMode, AdaptiveWritePolicy, BoundedQueue,
    CookOutcome, CookTask, CreditError, CreditLedger, OrderedReadyRing, PersistentCookerPool,
    QueueError, LaneTicket, FRAME_FRAMING_OVERHEAD,
};
use residiuum_store::EventKind;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn credit_reserve_hits_entry_and_byte_limits() {
    let c = mutation_credit(8, 64).expect("credit");
    assert!(c > FRAME_FRAMING_OVERHEAD);
    // Entry limit binds first: room for exactly 3 full credits by bytes.
    let ledger = CreditLedger::new(3, c.saturating_mul(10));

    ledger.try_reserve(1, c).unwrap();
    ledger.try_reserve(1, c).unwrap();
    ledger.try_reserve(1, c).unwrap();
    assert_eq!(
        ledger.try_reserve(1, c).unwrap_err(),
        CreditError::EntriesExhausted
    );
    assert_eq!(ledger.entries_used(), 3);

    ledger.release(1, c).unwrap();
    assert_eq!(ledger.entries_available(), 1);

    // Byte exhaustion with entries remaining.
    let tight = CreditLedger::new(10, c);
    tight.try_reserve(1, c).unwrap();
    assert_eq!(
        tight.try_reserve(1, 1).unwrap_err(),
        CreditError::BytesExhausted
    );
    assert_eq!(tight.entries_used(), 1);
    assert_eq!(tight.bytes_used(), c);
}

#[test]
fn credit_release_on_failed_enqueue_path() {
    // Plan §8: failure before enqueue returns credit.
    let ledger = CreditLedger::new(4, 10_000);
    let q: BoundedQueue<u32> = BoundedQueue::new(1);
    let c = mutation_credit(1, 1).unwrap();

    ledger.try_reserve(1, c).unwrap();
    q.try_push(1).unwrap();

    ledger.try_reserve(1, c).unwrap();
    match q.try_push(2) {
        Err(QueueError::Full) => {
            ledger.release(1, c).unwrap();
        }
        other => panic!("expected full, got {other:?}"),
    }
    assert_eq!(ledger.entries_used(), 1);
    assert_eq!(ledger.bytes_used(), c);
}

#[test]
fn policy_defaults_disabled_and_valid() {
    let p = AdaptiveWritePolicy::machine_defaults();
    assert_eq!(p.mode, AdaptiveWriteMode::Disabled);
    p.validate().expect("valid");
    assert!(p.maximum_cookers >= 1 && p.maximum_cookers <= 16);
    assert_eq!(p.pipeline_depth_limit, 2);
}

#[test]
fn cooker_frame_equivalence_vs_serial_encode() {
    let task = CookTask {
        ticket: LaneTicket { ticket: 7 },
        subject: Arc::<[u8]>::from(b"awo/eq/subject".as_slice()),
        body: Arc::<[u8]>::from(b"payload-bytes-for-equivalence".as_slice()),
        store_id: [0x11; 16],
        segment_id: [0x22; 16],
        item_id: [0x33; 16],
        event_id: [0x44; 16],
        writer_sequence: 42,
        created_ns: 9_001,
        event_kind: EventKind::Put,
        operation_id: None,
        operation_content_hash: None,
    };
    let serial_a = cook_item_frame(&task).unwrap();
    let serial_b = cook_item_frame(&task).unwrap();
    assert_eq!(serial_a, serial_b, "pure cook is deterministic");

    let pool = PersistentCookerPool::start(2, 2, 16, 4 * 1024 * 1024, 7);
    pool.try_submit(task.clone()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut frame = None;
    while Instant::now() < deadline {
        if let Some((ticket, outcome)) = pool.ready().try_pop_next() {
            assert_eq!(ticket.ticket, 7);
            match outcome {
                CookOutcome::Ok(c) => {
                    frame = Some(c.encoded_frame);
                    break;
                }
                CookOutcome::Err { message, .. } => panic!("{message}"),
            }
        }
        std::thread::yield_now();
    }
    let cooked = frame.expect("cooker produced frame");
    assert_eq!(
        cooked, serial_a,
        "persistent cooker frame must match serial pure encode"
    );
    // No post-warm thread create.
    assert_eq!(pool.threads_created(), 2);
    pool.set_active_cookers(1);
    assert_eq!(pool.threads_created(), 2);
    pool.shutdown();
}

#[test]
fn ordered_ready_preserves_ticket_order_under_parallel_cook() {
    let pool = PersistentCookerPool::start(4, 4, 64, 8 * 1024 * 1024, 0);
    for i in 0..16u64 {
        let task = CookTask {
            ticket: LaneTicket { ticket: i },
            subject: Arc::<[u8]>::from(format!("s{i}").into_bytes()),
            body: Arc::<[u8]>::from(vec![i as u8; 32]),
            store_id: [0; 16],
            segment_id: [1; 16],
            item_id: [2; 16],
            event_id: {
                let mut e = [0u8; 16];
                e[0] = i as u8;
                e
            },
            writer_sequence: i,
            created_ns: i * 10,
            event_kind: EventKind::Put,
            operation_id: None,
            operation_content_hash: None,
        };
        pool.try_submit(task).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut tickets = Vec::new();
    while tickets.len() < 16 && Instant::now() < deadline {
        if let Some((t, outcome)) = pool.ready().try_pop_next() {
            assert!(matches!(outcome, CookOutcome::Ok(_)));
            tickets.push(t.ticket);
        } else {
            std::thread::yield_now();
        }
    }
    assert_eq!(tickets, (0..16).collect::<Vec<_>>());
    pool.shutdown();
}

#[test]
fn ordered_ready_standalone_out_of_order_buffer() {
    let ring = OrderedReadyRing::<&'static str>::new(10, 1000);
    ring.push(LaneTicket { ticket: 12 }, "c", 1).unwrap();
    ring.push(LaneTicket { ticket: 10 }, "a", 1).unwrap();
    assert_eq!(ring.try_pop_next().unwrap().1, "a");
    assert!(ring.try_pop_next().is_none());
    ring.push(LaneTicket { ticket: 11 }, "b", 1).unwrap();
    assert_eq!(ring.try_pop_next().unwrap().1, "b");
    assert_eq!(ring.try_pop_next().unwrap().1, "c");
}
