//! Stage 8e — SDK distributed find coverage honesty.

use residiuum_cluster::{ClusterConfig, NodeId, PartitionId};
use residiuum_sdk::{json, ErrorCode, Filter, QueryOptions, Residiuum};

#[test]
fn cluster_find_complete_when_healthy() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Residiuum::create_cluster(
        ClusterConfig::dependable_local(dir.path().join("c")).with_virtual_partitions(8),
    )
    .unwrap();

    {
        let mut users = db.collection("users").unwrap();
        users.put("alice", &json!({"status": "active"})).unwrap();
        users.put("bob", &json!({"status": "idle"})).unwrap();
    }

    let found = db
        .collection("users")
        .unwrap()
        .find(&Filter::field("status").eq("active"))
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].0, "alice");
}

#[test]
fn incomplete_coverage_errors_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Residiuum::create_cluster(
        ClusterConfig::dependable_local(dir.path().join("c")).with_virtual_partitions(8),
    )
    .unwrap();

    {
        let mut coll = db.collection("items").unwrap();
        for i in 0..30 {
            coll.put(&format!("k{i}"), &json!({"i": i})).unwrap();
        }
    }

    // Rebalance partition 0 to node 0 only, then take node 0 offline.
    let backend = db.cluster_backend_mut().unwrap();
    let p = PartitionId::new(0);
    backend
        .cluster_mut()
        .rebalance_partition(p, vec![NodeId::new(0)])
        .unwrap();
    backend.cluster_mut().mark_offline(NodeId::new(0)).unwrap();

    let err = db
        .collection("items")
        .unwrap()
        .find(&Filter::Always)
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::CoverageIncomplete);
}

#[test]
fn allow_partial_coverage_returns_matches_and_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Residiuum::create_cluster(
        ClusterConfig::dependable_local(dir.path().join("c")).with_virtual_partitions(8),
    )
    .unwrap();

    {
        let mut coll = db.collection("items").unwrap();
        for i in 0..30 {
            coll.put(&format!("k{i}"), &json!({"i": i})).unwrap();
        }
    }

    let backend = db.cluster_backend_mut().unwrap();
    let p = PartitionId::new(0);
    backend
        .cluster_mut()
        .rebalance_partition(p, vec![NodeId::new(0)])
        .unwrap();
    backend.cluster_mut().mark_offline(NodeId::new(0)).unwrap();

    let result = db
        .collection("items")
        .unwrap()
        .find_with_coverage(
            &Filter::Always,
            QueryOptions::new().allow_partial_coverage(),
        )
        .unwrap();
    assert!(result.coverage.is_incomplete());
    assert!(result.coverage.unavailable.contains(&p));
    assert!(result.query_id.starts_with("q-"));
    // Matches from other partitions may still appear.
    // Incomplete must never look like "complete empty".
    assert!(!result.coverage.is_complete());
}
