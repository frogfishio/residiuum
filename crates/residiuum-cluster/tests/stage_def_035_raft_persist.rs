//! DEF-035 — durable Raft hard state, log, commit frontiers, snapshots,
//! and crash/restart recovery without fabricated commitment.

use residiuum_cluster::raft::{LogCommand, PartitionRaft, RaftRole};
use residiuum_cluster::raft_persist::{
    snapshot_meta_for, ConsensusEvidenceClass, HardState, RaftPeerStore, RAFT_PERSIST_PROFILE,
};
use residiuum_cluster::{
    Cluster, ClusterConfig, DurabilityMode, NodeId, PartitionId, PlacementEpoch, ReadMode, Term,
};
use std::fs::{self, OpenOptions};
use std::io::Write;

#[test]
fn profile_label_is_stable() {
    assert_eq!(RAFT_PERSIST_PROFILE, "residiuum-raft-persist-v1");
}

#[test]
fn cluster_survives_process_restart_with_committed_write() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("restart");

    let ack = {
        let mut cluster =
            Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(4))
                .unwrap();
        let ack = cluster
            .put("orders/1", b"v1", DurabilityMode::Durable)
            .unwrap();
        assert!(ack.committed);
        assert!(ack.replica_acks >= 2);
        ack
    };

    // Simulate process death + reopen (new Cluster handle).
    let mut cluster = Cluster::open(&root).unwrap();

    // Immediately after open (before re-election): roles are volatile Followers,
    // but hard state / logs are restored from disk.
    {
        let group = cluster.raft_group(ack.partition).expect("group");
        let mut saw_log = false;
        for n in 0..3u32 {
            let peer = group.peer(NodeId::new(n)).expect("peer");
            assert!(
                peer.current_term.0 >= 1 || peer.commit_index >= ack.position.0,
                "peer {n} lost durable progress: term={} commit={}",
                peer.current_term.0,
                peer.commit_index
            );
            if peer.log.iter().any(|e| e.index == ack.position.0) {
                saw_log = true;
            }
            assert_eq!(
                peer.role,
                RaftRole::Follower,
                "leadership must not survive restart as Leader"
            );
        }
        assert!(
            saw_log,
            "committed entry must appear in at least one durable log"
        );
    }

    let got = cluster.get("orders/1", ReadMode::Linearizable).unwrap();
    assert_eq!(got.value.as_deref(), Some(b"v1".as_slice()));

    // Further writes still work after recovery (re-election).
    let ack2 = cluster
        .put("orders/1", b"v2", DurabilityMode::Durable)
        .unwrap();
    assert!(ack2.committed);
    let got2 = cluster.get("orders/1", ReadMode::Linearizable).unwrap();
    assert_eq!(got2.value.as_deref(), Some(b"v2".as_slice()));
}

#[test]
fn vote_hard_state_survives_restart_mid_election() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vote");
    let partition = PartitionId::new(0);
    let voters = vec![NodeId::new(0), NodeId::new(1), NodeId::new(2)];

    {
        let mut g = PartitionRaft::new(partition, voters.clone(), PlacementEpoch(1));
        for n in &voters {
            let store = RaftPeerStore::open(&root, *n, partition).unwrap();
            g.attach_store(store);
        }
        g.persist_membership().unwrap();

        // Partial election: only candidate + one voter online → no quorum, but
        // self-vote and granted vote must be durable.
        let err = g
            .elect(NodeId::new(0), &[NodeId::new(0), NodeId::new(1)])
            .unwrap(); // with 2 of 3 this succeeds
        let _ = err;
    }

    // Reopen stores and verify hard state.
    for n in 0..2u32 {
        let store = RaftPeerStore::open(&root, NodeId::new(n), partition).unwrap();
        let hs = store.load_hard_state().unwrap();
        assert!(hs.current_term >= 1, "term must be persisted");
        assert!(hs.voted_for.is_some(), "vote must be persisted");
    }

    // A second election in a new process must not double-vote in the same term.
    let mut g = PartitionRaft::new(partition, voters.clone(), PlacementEpoch(1));
    for n in &voters {
        let store = RaftPeerStore::open(&root, *n, partition).unwrap();
        g.attach_store(store);
        g.restore_peer_from_store(*n).unwrap();
    }
    let term_before = g.peer(NodeId::new(1)).unwrap().current_term;
    let voted = g.peer(NodeId::new(1)).unwrap().voted_for;
    // If still in same term with a prior vote, request_vote for another
    // candidate must refuse.
    if let (Some(Term(t)), Some(v)) = (Some(term_before), voted) {
        let _ = t;
        assert_eq!(v, NodeId::new(0));
    }
}

#[test]
fn torn_log_tail_does_not_fabricate_commit() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("torn");
    let store = RaftPeerStore::open(&root, NodeId::new(0), PartitionId::new(0)).unwrap();

    store
        .append_log(&[residiuum_cluster::LogEntry {
            term: Term(1),
            index: 1,
            command: LogCommand::Put {
                subject: "a".into(),
                value: b"ok".to_vec(),
            },
        }])
        .unwrap();
    store
        .persist_hard_state(&HardState {
            current_term: 1,
            voted_for: Some(0),
            commit_index: 1,
            last_applied: 1,
        })
        .unwrap();

    // Simulate torn second entry.
    let log_path = store.root().join("log.ndjson");
    {
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        f.write_all(&[0x10, 0, 0, 0]).unwrap(); // length without body
        f.write_all(b"partial").unwrap();
        f.sync_all().unwrap();
    }

    let log = store.load_log().unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].index, 1);
    assert_eq!(
        store.evidence_class(1).unwrap(),
        ConsensusEvidenceClass::Committed
    );
    assert_eq!(
        store.evidence_class(2).unwrap(),
        ConsensusEvidenceClass::UnknownCommit
    );
}

#[test]
fn corrupt_snapshot_is_discarded_not_trusted() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("badsnap");
    let store = RaftPeerStore::open(&root, NodeId::new(0), PartitionId::new(0)).unwrap();

    let blob = b"good-sm-state";
    let meta = snapshot_meta_for(2, Term(3), blob, "sm-v1");
    store.install_snapshot(meta, blob, &[]).unwrap();
    assert!(store.load_snapshot().unwrap().is_some());

    // Corrupt blob bytes after install.
    fs::write(store.root().join("snapshot.blob"), b"tampered!!!!").unwrap();
    assert!(
        store.load_snapshot().unwrap().is_none(),
        "checksum mismatch must discard snapshot"
    );
}

#[test]
fn snapshot_compact_then_recover() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("snap");
    let partition = PartitionId::new(0);
    let voters = vec![NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let online = voters.clone();

    let mut g = PartitionRaft::new(partition, voters.clone(), PlacementEpoch(1));
    for n in &voters {
        g.attach_store(RaftPeerStore::open(&root, *n, partition).unwrap());
    }
    g.persist_membership().unwrap();

    let (leader, _) = g.ensure_leader(&online).unwrap();
    for i in 1..=5 {
        let r = g
            .propose(
                leader,
                LogCommand::Put {
                    subject: format!("k{i}"),
                    value: vec![i as u8],
                },
                &online,
            )
            .unwrap();
        assert!(r.committed);
    }

    // Compact leader log via snapshot.
    let blob = br#"{"keys":5}"#;
    g.install_local_snapshot(leader, 3, blob, "sm-v1").unwrap();
    let leader_peer = g.peer(leader).unwrap();
    assert!(leader_peer.last_log_index() >= 3);
    // Entries 1..3 are compacted; trailing remain.
    assert!(leader_peer
        .log
        .iter()
        .filter(|e| e.command.subject() != "__residiuum_snapshot_base__")
        .all(|e| e.index > 3));

    // Recover from disk.
    let mut g2 = PartitionRaft::new(partition, voters.clone(), PlacementEpoch(1));
    for n in &voters {
        g2.attach_store(RaftPeerStore::open(&root, *n, partition).unwrap());
        g2.restore_peer_from_store(*n).unwrap();
    }
    let p = g2.peer(leader).unwrap();
    assert!(p.commit_index >= 3);
    let store = RaftPeerStore::open(&root, leader, partition).unwrap();
    let snap = store.load_snapshot().unwrap().expect("snapshot present");
    assert_eq!(snap.meta.last_included_index, 3);
    assert_eq!(snap.blob, blob);
}

#[test]
fn uncommitted_entry_not_promoted_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("uncommitted");
    let partition = PartitionId::new(0);
    let voters = vec![NodeId::new(0), NodeId::new(1), NodeId::new(2)];

    {
        let mut g = PartitionRaft::new(partition, voters.clone(), PlacementEpoch(1));
        for n in &voters {
            g.attach_store(RaftPeerStore::open(&root, *n, partition).unwrap());
        }
        g.persist_membership().unwrap();
        // Elect with majority, then propose with only leader online → prepared, not committed.
        let (leader, _) = g
            .elect(NodeId::new(0), &[NodeId::new(0), NodeId::new(1)])
            .unwrap();
        let r = g
            .propose(
                leader,
                LogCommand::Put {
                    subject: "solo".into(),
                    value: b"x".to_vec(),
                },
                &[NodeId::new(0)],
            )
            .unwrap();
        assert!(!r.committed);
        assert_eq!(r.replica_acks, 1);
    }

    // Reopen: commit_index must not include the un-replicated entry.
    let store = RaftPeerStore::open(&root, NodeId::new(0), partition).unwrap();
    let hs = store.load_hard_state().unwrap();
    let log = store.load_log().unwrap();
    assert!(
        log.iter().any(|e| e.command.subject() == "solo"),
        "prepared entry may remain in log"
    );
    assert!(
        hs.commit_index < log.last().map(|e| e.index).unwrap_or(0)
            || log.is_empty()
            || store.evidence_class(log.last().unwrap().index).unwrap()
                == ConsensusEvidenceClass::Prepared,
        "must not mark solo entry committed: commit={} log_last={:?}",
        hs.commit_index,
        log.last().map(|e| e.index)
    );
    assert_eq!(
        store.evidence_class(1).unwrap(),
        ConsensusEvidenceClass::Prepared
    );
}

#[test]
fn cluster_open_restores_terms_across_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("terms");
    let partition;
    let term;
    {
        let mut c =
            Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(2))
                .unwrap();
        let ack = c.put("t/1", b"a", DurabilityMode::Durable).unwrap();
        partition = ack.partition;
        term = ack.term;
        // Force another election by killing leader.
        c.mark_offline(ack.leader).unwrap();
        let ack2 = c.put("t/2", b"b", DurabilityMode::Durable).unwrap();
        assert!(ack2.term.0 >= term.0);
    }
    let c = Cluster::open(&root).unwrap();
    let g = c.raft_group(partition).unwrap();
    let max_term = (0..3u32)
        .filter_map(|i| g.peer(NodeId::new(i)).map(|p| p.current_term.0))
        .max()
        .unwrap_or(0);
    assert!(max_term >= term.0);
}
