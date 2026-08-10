//! DEF-038 — durable control-plane + rebalance workflows across restarts.
//!
//! Acceptance (doc/done/incidents/DEFECTS.md):
//! - Restart at every rebalance phase leaves old placement authoritative or a
//!   valid joint configuration.
//! - Loss of the coordinator does not lose the operation state.
//! - Missing nodes are visible in health, coverage, and operator output.

use residiuum_cluster::{
    upsert_endpoint, upsert_endpoint_authenticated, Cluster, ClusterConfig, ClusterMeta,
    DurabilityMode, NodeId, PartitionId, RebalancePhase, REBALANCE_CONTROL_PROFILE,
    REBALANCE_JOBS_FILE,
};
use std::fs;

#[test]
fn profile_label_is_stable() {
    assert_eq!(REBALANCE_CONTROL_PROFILE, "residiuum-rebalance-control-v1");
    assert_eq!(REBALANCE_JOBS_FILE, "rebalance_jobs.json");
}

#[test]
fn rebalance_job_survives_coordinator_restart_at_every_phase() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("rb");

    let phases = [
        RebalancePhase::PlanCommitted,
        RebalancePhase::LearnersAdded,
        RebalancePhase::SegmentsCopied,
        RebalancePhase::LogCaughtUp,
        RebalancePhase::MembershipChanged,
        RebalancePhase::EpochActivated,
        RebalancePhase::SafetyWindow,
    ];

    for stop_at in phases {
        let root_phase = root.join(format!("phase-{}", stop_at.as_str()));
        let p = PartitionId::new(1);

        {
            let mut cluster = Cluster::create(
                ClusterConfig::dependable_local(&root_phase).with_virtual_partitions(4),
            )
            .unwrap();
            // Seed data on partition.
            for i in 0..30 {
                let s = format!("p/{i}");
                if cluster.partition_for_subject(&s) == p {
                    cluster.put(&s, b"seed", DurabilityMode::Durable).unwrap();
                }
            }

            cluster
                .begin_rebalance(p, vec![NodeId::new(0), NodeId::new(1)])
                .unwrap();
            // Advance until we land on stop_at (begin leaves PlanCommitted).
            while cluster.rebalance_job(p).unwrap().phase != stop_at {
                cluster.advance_rebalance(p).unwrap();
            }
            assert_eq!(cluster.rebalance_job(p).unwrap().phase, stop_at);
            // Job file must exist on disk before drop (coordinator loss).
            assert!(root_phase.join(REBALANCE_JOBS_FILE).is_file());
        }

        // Coordinator restart.
        let mut cluster = Cluster::open(&root_phase).unwrap();
        let job = cluster
            .rebalance_job(p)
            .expect("job must survive restart")
            .clone();
        assert_eq!(job.phase, stop_at, "phase lost across restart");

        // Placement safety: old authoritative or joint or new.
        if stop_at.old_placement_authoritative() {
            assert!(
                job.phase.old_placement_authoritative(),
                "old placement must remain authoritative at {}",
                stop_at.as_str()
            );
            let group = cluster.raft_group(p).unwrap();
            assert!(!group.joint);
            assert_eq!(group.voters, job.old_replicas);
        } else if stop_at.is_joint() {
            assert!(job.joint || job.phase.is_joint());
            let group = cluster.raft_group(p).unwrap();
            assert!(group.joint, "joint membership must restore");
            let mut expected = job.old_replicas.clone();
            for n in &job.new_replicas {
                if !expected.contains(n) {
                    expected.push(*n);
                }
            }
            expected.sort();
            assert_eq!(group.voters, expected);
            // Directory still holds pre-activation placement (no ownership gap).
            assert_eq!(
                cluster.directory().get(p).unwrap().replicas,
                job.old_replicas
            );
        } else {
            // EpochActivated / SafetyWindow: new placement authoritative.
            assert!(job.phase.new_placement_authoritative());
            assert_eq!(
                cluster.directory().get(p).unwrap().replicas,
                job.new_replicas
            );
        }

        // Resume to completion after restart.
        while cluster.rebalance_job(p).is_some() {
            let phase = cluster.rebalance_job(p).unwrap().phase;
            if phase == RebalancePhase::Reclaimed {
                break;
            }
            cluster.advance_rebalance(p).unwrap();
        }
        assert!(cluster.rebalance_job(p).is_none());
        assert_eq!(
            cluster.directory().get(p).unwrap().replicas,
            vec![NodeId::new(0), NodeId::new(1)]
        );
    }
}

#[test]
fn health_reports_missing_nodes_and_inflight_rebalance() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("health");

    {
        let mut cluster =
            Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(4))
                .unwrap();
        assert!(!cluster.health().degraded);
        let p = PartitionId::new(0);
        cluster
            .begin_rebalance(p, vec![NodeId::new(0), NodeId::new(1)])
            .unwrap();
        let h = cluster.health();
        assert_eq!(h.expected_nodes, 3);
        assert_eq!(h.rebalance_phases.len(), 1);
        assert_eq!(h.rebalance_phases[0].1, RebalancePhase::PlanCommitted);
    }

    // Remove one node store path to simulate missing expected node.
    let node2 = root.join("nodes").join("node-2");
    fs::remove_dir_all(&node2).unwrap();

    let cluster = Cluster::open(&root).unwrap();
    let h = cluster.health();
    assert!(h.degraded, "missing store must mark degraded");
    assert!(
        h.missing_store_paths.iter().any(|n| n.index() == 2),
        "node 2 must appear in missing_store_paths: {:?}",
        h.missing_store_paths
    );
    assert!(
        cluster.rebalance_job(PartitionId::new(0)).is_some(),
        "rebalance job still loaded while degraded"
    );
}

#[test]
fn missing_placement_refuses_silent_synthetic_open() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("no-place");
    {
        let _cluster =
            Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(4))
                .unwrap();
    }
    fs::remove_file(root.join("placement.json")).ok();
    fs::remove_file(root.join("placement.json.prev")).ok();
    match Cluster::open(&root) {
        Ok(_) => panic!("expected open to fail without placement"),
        Err(err) => assert_eq!(err.code(), "corrupt_meta"),
    }
}

#[test]
fn endpoint_registration_requires_secret_when_configured() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("ep");
    {
        let _cluster =
            Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(2))
                .unwrap();
    }

    // Without secret: unauthenticated upsert works.
    upsert_endpoint(&root, 0, "127.0.0.1:9000").unwrap();

    ClusterMeta::set_registration_secret(&root, "s3cr3t").unwrap();

    let err = upsert_endpoint(&root, 1, "127.0.0.1:9001").unwrap_err();
    assert_eq!(err.code(), "replication_rejected");

    let bad = upsert_endpoint_authenticated(&root, 1, "127.0.0.1:9001", "wrong").unwrap_err();
    assert_eq!(bad.code(), "replication_rejected");

    let map = upsert_endpoint_authenticated(&root, 1, "127.0.0.1:9001", "s3cr3t").unwrap();
    assert_eq!(map.get(&0).map(String::as_str), Some("127.0.0.1:9000"));
    assert_eq!(map.get(&1).map(String::as_str), Some("127.0.0.1:9001"));
}

#[test]
fn joint_membership_persisted_on_peer_stores() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("joint");
    let p = PartitionId::new(2);

    {
        let mut cluster =
            Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(4))
                .unwrap();
        cluster
            .begin_rebalance(p, vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        // Advance to MembershipChanged.
        for _ in 0..4 {
            cluster.advance_rebalance(p).unwrap();
        }
        assert_eq!(
            cluster.rebalance_job(p).unwrap().phase,
            RebalancePhase::MembershipChanged
        );
        let group = cluster.raft_group(p).unwrap();
        assert!(group.joint);
        assert!(!group.outgoing.is_empty());
        assert!(!group.incoming.is_empty());
    }

    let cluster = Cluster::open(&root).unwrap();
    let group = cluster.raft_group(p).unwrap();
    assert!(group.joint, "joint flag must restore from job/membership");
    let job = cluster.rebalance_job(p).unwrap();
    assert_eq!(job.phase, RebalancePhase::MembershipChanged);
    assert_eq!(group.outgoing, job.old_replicas);
    assert_eq!(group.incoming, job.new_replicas);
}
