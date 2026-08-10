//! S3 product capacity path — multi-partition batch put on residiuum-cluster.
//!
//! Product scale is independent partition leaders / node stores (WORK_HORIZON
//! S3), not testrig `--stores N`. This cut:
//! - `Cluster::put_many` groups by virtual partition and writes per-leader
//! - dependable-local profile spreads leaders across nodes
//! - acks remain honest (replica_acks, committed, leader, partition)

use residiuum_cluster::{
    Cluster, ClusterConfig, ConsistencyMode, DeploymentProfile, NodeId, ReadMode,
};
use residiuum_store::DurabilityMode;
use std::collections::{HashMap, HashSet};

#[test]
fn put_many_empty_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Cluster::create(
        ClusterConfig::development(dir.path().join("e")).with_virtual_partitions(4),
    )
    .unwrap();
    let acks = c.put_many(&[], DurabilityMode::Memory).unwrap();
    assert!(acks.is_empty());
}

#[test]
fn put_many_preserves_order_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Cluster::create(
        ClusterConfig::development(dir.path().join("o")).with_virtual_partitions(8),
    )
    .unwrap();

    let items: Vec<(&str, &[u8])> = vec![
        ("batch/a", b"1"),
        ("batch/b", b"2"),
        ("batch/c", b"3"),
        ("batch/d", b"4"),
        ("batch/e", b"5"),
    ];
    let acks = c.put_many(&items, DurabilityMode::Durable).unwrap();
    assert_eq!(acks.len(), items.len());
    for (i, ack) in acks.iter().enumerate() {
        assert!(ack.committed, "item {i} not committed");
        assert_eq!(ack.consistency_mode, ConsistencyMode::PartitionLinearizable);
        let got = c.get(items[i].0, ReadMode::Linearizable).unwrap();
        assert_eq!(got.value.as_deref(), Some(items[i].1));
    }
}

#[test]
fn put_many_spreads_across_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Cluster::create(
        ClusterConfig::development(dir.path().join("s")).with_virtual_partitions(16),
    )
    .unwrap();

    let owned: Vec<String> = (0..48).map(|i| format!("spread/{i}")).collect();
    let items: Vec<(&str, &[u8])> = owned.iter().map(|s| (s.as_str(), b"v" as &[u8])).collect();
    let acks = c.put_many(&items, DurabilityMode::Memory).unwrap();

    let parts: HashSet<u32> = acks.iter().map(|a| a.partition.get()).collect();
    assert!(
        parts.len() >= 6,
        "expected multi-partition fan-out, got {parts:?}"
    );
}

#[test]
fn dependable_local_put_many_uses_independent_leaders() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("dl");
    let mut c = Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(32))
        .unwrap();
    assert_eq!(c.profile(), DeploymentProfile::DependableLocal);
    assert_eq!(c.online_node_count(), 3);
    assert_eq!(c.write_quorum(), 2);

    let owned: Vec<String> = (0..64).map(|i| format!("cap/{i}")).collect();
    let items: Vec<(&str, &[u8])> = owned
        .iter()
        .map(|s| (s.as_str(), b"payload" as &[u8]))
        .collect();
    let acks = c.put_many(&items, DurabilityMode::Durable).unwrap();
    assert_eq!(acks.len(), 64);

    // Leaders should span multiple nodes (product capacity: not one serial store).
    let mut leaders: HashSet<u32> = HashSet::new();
    let mut partitions: HashSet<u32> = HashSet::new();
    let mut by_leader: HashMap<u32, u32> = HashMap::new();
    for ack in &acks {
        assert!(ack.committed, "ack not committed: {ack:?}");
        // Dependable-local write quorum is 2 of 3.
        assert!(
            ack.replica_acks >= c.write_quorum(),
            "replica_acks={} < quorum={}",
            ack.replica_acks,
            c.write_quorum()
        );
        leaders.insert(ack.leader.index());
        partitions.insert(ack.partition.get());
        *by_leader.entry(ack.leader.index()).or_default() += 1;
    }
    assert!(
        leaders.len() >= 2,
        "expected ≥2 independent partition leaders, got {leaders:?} counts={by_leader:?}"
    );
    assert!(
        partitions.len() >= 4,
        "expected multi-partition spread, got {partitions:?}"
    );

    // Every value is readable under linearizable mode.
    for key in &owned {
        let got = c.get(key, ReadMode::Linearizable).unwrap();
        assert_eq!(got.value.as_deref(), Some(b"payload".as_slice()));
    }
}

#[test]
fn put_many_ack_partition_matches_routing() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Cluster::create(
        ClusterConfig::development(dir.path().join("r")).with_virtual_partitions(8),
    )
    .unwrap();
    let items: Vec<(&str, &[u8])> = vec![("route/x", b"1"), ("route/y", b"2"), ("route/z", b"3")];
    let acks = c.put_many(&items, DurabilityMode::Durable).unwrap();
    for (i, ack) in acks.iter().enumerate() {
        let expected = c.partition_for_subject(items[i].0);
        assert_eq!(ack.partition, expected);
        // Development profile: single node is the only leader.
        assert_eq!(ack.leader, NodeId::new(0));
        assert_eq!(ack.replica_acks, 1);
    }
}
