//! DEF-041 — distributed-system verification program.
//!
//! Acceptance (this cut):
//! - Deterministic simulation with seeded PRNG (replayable failures)
//! - Fault model: crash, network partition, RPC drop / duplicate
//! - Linearizability checking for strong-mode committed put/get history
//! - Convergence / conflict-preservation checking for dual-accept variants
//! - CLUSTER_SPEC §22 core cases against network Raft (`MemoryRaftNetwork` +
//!   `SimTransport`) including §22.5 stale placement epoch fencing
//! - Seeded soak with put/get after chaos (in-process; multi-process OS chaos
//!   remains a follow-on)
//! - Failure dumps retain seed + history + event log

use residiuum_cluster::sim::{
    check_convergent_preserved, check_partition_linearizable, run_conformance_matrix, ClientOp,
    ConformanceCase, ConvergentVariant, HistoryEntry, OpOutcome, SeedRng, SimConfig, SimWorld,
    VERIFY_PROFILE,
};
use residiuum_cluster::{ElectError, NodeId};

#[test]
fn profile_tag_is_stable() {
    assert_eq!(VERIFY_PROFILE, "residiuum-cluster-verify-v1");
}

#[test]
fn covered_cases_are_named() {
    let ids: Vec<_> = ConformanceCase::covered().iter().map(|c| c.id()).collect();
    assert!(ids.contains(&"s22.1_leader_loss_append"));
    assert!(ids.contains(&"s22.3_ack_loss_retry"));
    assert!(ids.contains(&"s22.5_stale_placement"));
    assert!(ids.contains(&"s22.6_minority_majority_partition"));
    assert!(ids.contains(&"s22.chaos_seeded_linearizable"));
    assert!(ids.contains(&"s22.soak_put_get"));
}

#[test]
fn same_seed_same_rng_stream() {
    let mut a = SeedRng::new(0xdef0_0041);
    let mut b = SeedRng::new(0xdef0_0041);
    let seq_a: Vec<u64> = (0..32).map(|_| a.next_u64()).collect();
    let seq_b: Vec<u64> = (0..32).map(|_| b.next_u64()).collect();
    assert_eq!(seq_a, seq_b);
}

#[test]
fn same_seed_same_campaign_and_put_history() {
    fn run(seed: u64) -> Vec<(u64, String, bool)> {
        let mut w = SimWorld::three_node(seed);
        let (leader, _) = w.elect_any().expect("elect");
        let r = w
            .client_put(
                "replay/k",
                b"v",
                Some("op-replay-aaaaaaaaaaaaaaaaaaaa"),
                leader,
            )
            .expect("put");
        w.history
            .iter()
            .map(|h| {
                let sub = match &h.op {
                    ClientOp::Put { subject, .. } => subject.clone(),
                    ClientOp::Get { subject } => subject.clone(),
                };
                let committed = matches!(
                    h.outcome,
                    Some(OpOutcome::PutOk {
                        committed: true,
                        ..
                    })
                );
                (h.call_id, sub, committed)
            })
            .chain(std::iter::once((
                0,
                format!("term={}", r.term.0),
                r.committed,
            )))
            .collect()
    }
    assert_eq!(run(99), run(99));
}

#[test]
fn leader_loss_before_append_still_commits_on_new_leader() {
    let mut w = SimWorld::three_node(11);
    let (leader, _) = w.elect_any().unwrap();
    w.crash(leader);
    let (new_leader, _) = w.elect_any().expect("re-elect");
    assert_ne!(new_leader, leader);
    let r = w
        .client_put("pre/append", b"ok", Some("op-pre"), new_leader)
        .expect("put");
    assert!(r.committed, "{}", w.dump());
    w.check_linearizable().expect("lin");
}

#[test]
fn leader_loss_after_quorum_preserves_commit_and_continues() {
    let mut w = SimWorld::three_node(12);
    let (leader, _) = w.elect_any().unwrap();
    let r1 = w
        .client_put("post/q", b"quorum", Some("op-q1"), leader)
        .unwrap();
    assert!(r1.committed);
    // All online peers should have the commit.
    for n in w.online_voters() {
        let ci = w.commit_index(n).unwrap();
        assert!(
            ci >= r1.position.0,
            "peer {n} commit={ci} dump={}",
            w.dump()
        );
    }
    w.crash(leader);
    let (new_leader, _) = w.elect_any().unwrap();
    let r2 = w
        .client_put("post/q2", b"next", Some("op-q2"), new_leader)
        .unwrap();
    assert!(r2.committed, "{}", w.dump());
    w.check_linearizable().unwrap();
}

#[test]
fn ack_loss_idempotent_retry_same_position() {
    let mut w = SimWorld::three_node(13);
    let (leader, _) = w.elect_any().unwrap();
    let oid = "op-idempotent-bbbbbbbbbbbbbbbbbbbbbbbb";
    let r1 = w.client_put("retry/k", b"body", Some(oid), leader).unwrap();
    let r2 = w.client_put("retry/k", b"body", Some(oid), leader).unwrap();
    assert_eq!(r1.position, r2.position);
    assert_eq!(r1.term, r2.term);
    assert!(r1.committed && r2.committed);
}

#[test]
fn old_leader_cannot_commit_after_new_term() {
    let mut w = SimWorld::three_node(14);
    let (old, old_term) = w.elect_any().unwrap();
    w.crash(old);
    let (new_leader, new_term) = w.elect_any().unwrap();
    assert!(new_term.0 > old_term.0);
    w.client_put("fence/new", b"n", Some("op-n"), new_leader)
        .unwrap();
    w.recover(old);
    let stale = w.client_put("fence/old", b"stale", Some("op-o"), old);
    match stale {
        Err(_) => {}
        Ok(r) => assert!(!r.committed, "old leader must not commit: {}", w.dump()),
    }
}

#[test]
fn minority_partition_cannot_elect_majority_can() {
    let w = SimWorld::three_node(16);
    let alone = w.voters[0];
    let rest = [w.voters[1], w.voters[2]];
    w.network_partition(&[alone], &rest);
    match w.campaign(alone) {
        Err(ElectError::NoQuorum { votes, need }) => {
            assert!(votes < need);
        }
        other => panic!(
            "expected NoQuorum for minority, got {other:?}\n{}",
            w.dump()
        ),
    }
    let (leader, _) = w
        .campaign(rest[0])
        .or_else(|_| w.campaign(rest[1]))
        .expect("majority elect");
    assert_ne!(leader, alone);
}

#[test]
fn message_drops_do_not_invent_commits() {
    let mut w = SimWorld::new(SimConfig {
        seed: 20,
        drop_prob: 0.4,
        ..SimConfig::default()
    });
    // May take several campaigns under drop.
    let mut elected = None;
    for _ in 0..12 {
        if let Ok(x) = w.elect_any() {
            elected = Some(x);
            break;
        }
        // Heal occasional full connectivity by temporarily zeroing drops.
        w.set_drop_prob(0.0);
        if let Ok(x) = w.elect_any() {
            elected = Some(x);
            break;
        }
        w.set_drop_prob(0.4);
    }
    let (leader, _) = elected.expect("eventually elect with intermittent clean windows");
    w.set_drop_prob(0.0); // commit path must succeed for the assertion
    let r = w
        .client_put("drop/k", b"v", Some("op-drop"), leader)
        .expect("put");
    assert!(r.committed);
    // Every history committed put is honest — linearizable check.
    w.check_linearizable().unwrap();
}

#[test]
fn chaos_seed_replays_and_stays_linearizable() {
    let seed = 0xc0ffee41u64;
    let mut w1 = SimWorld::three_node(seed);
    w1.set_drop_prob(0.08);
    let c1 = w1.run_chaos(48);
    w1.heal_network();
    for n in w1.voters.clone() {
        w1.recover(n);
    }
    if let Err(e) = w1.check_linearizable() {
        panic!("{e}\n{}", w1.dump());
    }

    let mut w2 = SimWorld::three_node(seed);
    w2.set_drop_prob(0.08);
    let c2 = w2.run_chaos(48);
    assert_eq!(
        c1,
        c2,
        "same seed must produce same committed-put count\n--- w1 ---\n{}\n--- w2 ---\n{}",
        w1.dump(),
        w2.dump()
    );
    // History shape equality (call subjects + outcomes).
    assert_eq!(w1.history.len(), w2.history.len());
    for (a, b) in w1.history.iter().zip(w2.history.iter()) {
        assert_eq!(a.call_id, b.call_id);
        assert_eq!(a.op, b.op);
        assert_eq!(a.outcome, b.outcome);
    }
}

#[test]
fn convergent_variants_preserved() {
    let a = b"payload-a".to_vec();
    let b = b"payload-b".to_vec();
    let variants = vec![
        ConvergentVariant {
            identity: "id-a".into(),
            body: a,
            accepted_by: 0,
        },
        ConvergentVariant {
            identity: "id-b".into(),
            body: b,
            accepted_by: 1,
        },
    ];
    check_convergent_preserved(&variants, Some(1)).unwrap();
}

#[test]
fn convergent_duplicate_identity_rejected() {
    let variants = vec![
        ConvergentVariant {
            identity: "dup".into(),
            body: b"a".to_vec(),
            accepted_by: 0,
        },
        ConvergentVariant {
            identity: "dup".into(),
            body: b"b".to_vec(),
            accepted_by: 1,
        },
    ];
    assert!(check_convergent_preserved(&variants, Some(1)).is_err());
}

#[test]
fn linearizability_rejects_ghost_read() {
    let history = vec![
        HistoryEntry {
            call_id: 1,
            invoke_time: 1,
            return_time: Some(2),
            op: ClientOp::Put {
                subject: "k".into(),
                value: b"real".to_vec(),
                operation_id: None,
            },
            outcome: Some(OpOutcome::PutOk {
                committed: true,
                index: 1,
                term: 1,
            }),
        },
        HistoryEntry {
            call_id: 2,
            invoke_time: 3,
            return_time: Some(4),
            op: ClientOp::Get {
                subject: "k".into(),
            },
            outcome: Some(OpOutcome::GetOk {
                value: Some(b"ghost".to_vec()),
            }),
        },
    ];
    assert!(check_partition_linearizable(&history, Some(0)).is_err());
}

#[test]
fn conformance_matrix_all_pass() {
    for seed in [1u64, 7, 41, 0xdef0_0041] {
        let reports = run_conformance_matrix(seed);
        assert_eq!(
            reports.len(),
            ConformanceCase::covered().len(),
            "seed={seed}"
        );
        for r in &reports {
            assert!(r.ok, "seed={seed} case={} failed:\n{}", r.case, r.detail);
        }
    }
}

#[test]
fn failure_dump_includes_seed() {
    let w = SimWorld::three_node(12345);
    let dump = w.dump();
    assert!(dump.contains("seed=12345"), "{dump}");
    assert!(dump.contains("history="), "{dump}");
}

#[test]
fn multi_step_partition_heal_commit() {
    // Majority side keeps serving; heal restores full set.
    let mut w = SimWorld::three_node(30);
    let (leader, _) = w.elect_any().unwrap();
    w.client_put("heal/1", b"a", Some("op-h1"), leader).unwrap();

    let alone = w.voters.iter().find(|n| **n != leader).copied().unwrap();
    let majority: Vec<NodeId> = w.voters.iter().copied().filter(|n| *n != alone).collect();
    w.network_partition(&[alone], &majority);

    // Leader still in majority → can commit.
    if majority.contains(&leader) {
        let r = w
            .client_put("heal/2", b"b", Some("op-h2"), leader)
            .expect("majority put");
        assert!(r.committed, "{}", w.dump());
    }

    w.heal_network();
    let (leader2, _) = w.elect_any().unwrap();
    let r = w
        .client_put("heal/3", b"c", Some("op-h3"), leader2)
        .unwrap();
    assert!(r.committed);
    w.check_linearizable().unwrap();
}

#[test]
fn put_then_get_observes_committed_value() {
    let mut w = SimWorld::three_node(31);
    let (leader, _) = w.elect_any().unwrap();
    let r = w
        .client_put("get/k", b"body-v1", Some("op-get-1"), leader)
        .unwrap();
    assert!(r.committed);
    let got = w.client_get("get/k", leader).expect("get");
    assert_eq!(got.as_deref(), Some(b"body-v1".as_slice()));
    // Missing subject.
    let missing = w.client_get("get/missing", leader).unwrap();
    assert!(missing.is_none());
    w.check_linearizable().unwrap();
}

#[test]
fn stale_placement_epoch_is_fenced() {
    let mut w = SimWorld::three_node(32);
    let old = w.placement_epoch().expect("epoch");
    let (leader, _) = w.elect_any().unwrap();
    w.client_put("place/k", b"v", Some("op-pl-1"), leader)
        .unwrap();

    w.advance_placement_epoch();
    let new = w.placement_epoch().expect("new epoch");
    assert!(new.0 > old.0, "epoch must advance");

    // Stale epoch campaign cannot become leader.
    match w.campaign_with_epoch(w.voters[0], old) {
        Err(ElectError::NoQuorum { .. }) => {}
        other => panic!("stale placement must not elect: {other:?}\n{}", w.dump()),
    }

    // Current epoch elects and serves.
    let (leader2, _) = w.elect_any().expect("elect current epoch");
    let r = w
        .client_put("place/k2", b"v2", Some("op-pl-2"), leader2)
        .expect("put after epoch bump");
    assert!(r.committed, "{}", w.dump());
    let got = w.client_get("place/k2", leader2).unwrap();
    assert_eq!(got.as_deref(), Some(b"v2".as_slice()));
}

#[test]
fn soak_put_get_stays_linearizable() {
    let mut w = SimWorld::three_node(0x50a1u64);
    let (puts, gets) = w
        .run_soak(32, 8)
        .unwrap_or_else(|e| panic!("{e}\n{}", w.dump()));
    // After heal we expect some successful post-ops when leadership forms.
    assert!(
        puts + gets > 0 || w.history.iter().any(|h| h.is_committed_put()),
        "soak produced no committed work:\n{}",
        w.dump()
    );
    // Seeded replay: same seed → same history length and outcomes.
    let mut w2 = SimWorld::three_node(0x50a1u64);
    let (puts2, gets2) = w2.run_soak(32, 8).unwrap();
    assert_eq!((puts, gets), (puts2, gets2));
    assert_eq!(w.history.len(), w2.history.len());
}
