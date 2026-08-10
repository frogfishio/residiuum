//! DEF-036 — network Raft RPC: RequestVote, AppendEntries, InstallSnapshot,
//! ReadIndex over a transport with term / membership / placement-epoch fences.
//!
//! Acceptance (control plane):
//! - Three independent peer states provide quorum durability
//! - Minority cannot commit strong writes
//! - Old leaders cannot write after a new term
//! - Response loss / retries preserve one event identity (`operation_id`)
//! - Endpoint routing is never write authority (epoch/cluster fences)

use residiuum_cluster::raft::LogCommand;
use residiuum_cluster::{
    ClusterId, ElectError, MemoryRaftNetwork, NetworkRaftNode, NodeId, PartitionId, PlacementEpoch,
    ProposeError, RaftPeerStore, RequestVoteRequest, RAFT_RPC_PROFILE,
};
use tempfile::tempdir;

fn three_peers(partition: PartitionId) -> (MemoryRaftNetwork, ClusterId, Vec<NodeId>) {
    let cluster_id = ClusterId::from_seed(b"stage-def-036");
    let voters: Vec<NodeId> = (0..3).map(NodeId::new).collect();
    let net = MemoryRaftNetwork::new();
    for v in &voters {
        net.register(NetworkRaftNode::new(
            cluster_id,
            partition,
            *v,
            voters.clone(),
            PlacementEpoch(1),
        ));
    }
    (net, cluster_id, voters)
}

#[test]
fn profile_tag_is_stable() {
    assert_eq!(RAFT_RPC_PROFILE, "residiuum-raft-rpc-v1");
}

#[test]
fn three_peers_quorum_commit() {
    let p = PartitionId(0);
    let (net, _, voters) = three_peers(p);
    let (leader, term) = net.campaign(p, voters[0]).expect("elect");
    assert!(term.0 >= 1);

    let r = net
        .propose(
            p,
            leader,
            LogCommand::Put {
                subject: "orders/1".into(),
                value: b"committed".to_vec(),
            },
            Some("op-def036aaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .expect("propose");
    assert!(r.committed);
    assert!(r.replica_acks >= 2);

    for v in &voters {
        let ci = net.with_node(p, *v, |n| n.commit_index()).unwrap();
        assert!(ci >= r.position.0, "peer {v} missing commit");
    }
}

#[test]
fn minority_partition_cannot_commit() {
    let p = PartitionId(0);
    let (net, _, voters) = three_peers(p);
    net.mark_offline(voters[1]);
    net.mark_offline(voters[2]);
    let err = net.campaign(p, voters[0]).unwrap_err();
    assert!(matches!(err, ElectError::NoQuorum { votes: 1, need: 2 }));
}

#[test]
fn old_leader_fenced_after_new_term() {
    let p = PartitionId(0);
    let (net, _, voters) = three_peers(p);
    let (old, old_term) = net.campaign(p, voters[0]).unwrap();
    net.mark_offline(old);
    let (new_leader, new_term) = net.campaign(p, voters[1]).unwrap();
    assert!(new_term.0 > old_term.0);
    assert!(
        net.propose(
            p,
            new_leader,
            LogCommand::Put {
                subject: "k".into(),
                value: b"new".to_vec(),
            },
            None,
        )
        .unwrap()
        .committed
    );

    net.mark_online(old);
    match net.propose(
        p,
        old,
        LogCommand::Put {
            subject: "k".into(),
            value: b"stale".to_vec(),
        },
        None,
    ) {
        Err(ProposeError::NotLeader | ProposeError::SteppedDown(_)) => {}
        Ok(r) => assert!(!r.committed, "stale leader must not commit"),
        Err(e) => panic!("unexpected {e:?}"),
    }
}

#[test]
fn operation_id_retry_same_log_index() {
    let p = PartitionId(0);
    let (net, _, voters) = three_peers(p);
    let (leader, _) = net.campaign(p, voters[0]).unwrap();
    let oid = "op-retry00000000000000000000000001";
    let cmd = LogCommand::Put {
        subject: "idem".into(),
        value: b"once".to_vec(),
    };
    let a = net.propose(p, leader, cmd.clone(), Some(oid)).unwrap();
    let b = net.propose(p, leader, cmd, Some(oid)).unwrap();
    assert!(a.committed && b.committed);
    assert_eq!(a.position, b.position);
}

#[test]
fn placement_epoch_is_write_authority_not_endpoints() {
    let p = PartitionId(0);
    let (net, cluster_id, voters) = three_peers(p);
    // Even if we "know" a peer endpoint, a wrong epoch is rejected.
    let bad = RequestVoteRequest {
        cluster_id: cluster_id.to_hex(),
        partition: p.0,
        placement_epoch: 999,
        term: 5,
        candidate_id: voters[0].index(),
        last_log_index: 0,
        last_log_term: 0,
    };
    let err = net
        .with_node_mut(p, voters[1], |n| n.handle_request_vote(&bad))
        .unwrap()
        .unwrap_err();
    assert_eq!(err.code(), "raft_fenced");
}

#[test]
fn durable_peers_on_separate_roots() {
    // Three independent storage roots (process simulation).
    let d0 = tempdir().unwrap();
    let d1 = tempdir().unwrap();
    let d2 = tempdir().unwrap();
    let roots = [d0.path(), d1.path(), d2.path()];
    let cluster_id = ClusterId::from_seed(b"multi-root-036");
    let p = PartitionId(7);
    let voters: Vec<NodeId> = (0..3).map(NodeId::new).collect();
    let net = MemoryRaftNetwork::new();
    for (i, root) in roots.iter().enumerate() {
        let node_id = NodeId::new(i as u32);
        let store = RaftPeerStore::open(root, node_id, p).unwrap();
        let mut node =
            NetworkRaftNode::new(cluster_id, p, node_id, voters.clone(), PlacementEpoch(1));
        node.attach_store(store).unwrap();
        net.register(node);
    }

    let (leader, _) = net.campaign(p, voters[0]).unwrap();
    let r = net
        .propose(
            p,
            leader,
            LogCommand::Put {
                subject: "multi".into(),
                value: b"root".to_vec(),
            },
            None,
        )
        .unwrap();
    assert!(r.committed);

    // Each root has durable hard state / log after commit.
    for (i, root) in roots.iter().enumerate() {
        let store = RaftPeerStore::open(root, NodeId::new(i as u32), p).unwrap();
        let peer = store.load_peer().unwrap();
        assert!(peer.current_term.0 >= 1);
        assert!(peer.last_log_index() >= 1 || peer.commit_index >= 1);
    }
}
