//! Stage 8e — distributed scan/find coverage + partial-query honesty
//! (CLUSTER_SPEC §6.7, §17, §22 item 15).

use residiuum_cluster::{
    Cluster, ClusterConfig, DurabilityMode, NodeId, PartitionId, ReadMode, ScanOptions,
};

#[test]
fn full_scan_reports_complete_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::create(
        ClusterConfig::dependable_local(dir.path().join("c")).with_virtual_partitions(8),
    )
    .unwrap();

    for i in 0..20 {
        cluster
            .put(
                &format!("item/{i}"),
                format!("v{i}").as_bytes(),
                DurabilityMode::Buffered,
            )
            .unwrap();
    }

    let scan = cluster.scan_all().unwrap();
    assert!(scan.coverage.is_complete());
    assert_eq!(scan.coverage.unavailable.len(), 0);
    assert!(!scan.entries.is_empty());
    assert_eq!(scan.coverage.requested.len(), 8);
}

#[test]
fn prefix_find_carries_query_id_and_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::create(
        ClusterConfig::development(dir.path().join("d")).with_virtual_partitions(4),
    )
    .unwrap();

    cluster
        .put("users/alice", br#"{"n":1}"#, DurabilityMode::Durable)
        .unwrap();
    cluster
        .put("users/bob", br#"{"n":2}"#, DurabilityMode::Durable)
        .unwrap();
    cluster
        .put("orders/1", br#"{"n":3}"#, DurabilityMode::Durable)
        .unwrap();

    let found = cluster.find(ScanOptions::new().prefix("users/")).unwrap();
    assert!(found.coverage.is_complete());
    assert!(found.query_id.starts_with("q-"));
    assert_eq!(found.entries.len(), 2);
    assert!(found.entries.iter().all(|(s, _)| s.starts_with("users/")));
}

#[test]
fn partial_query_when_partition_replicas_all_offline() {
    // Place one partition solely on node 0, take node 0 offline → that
    // partition is unavailable while others remain complete.
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::create(
        ClusterConfig::dependable_local(dir.path().join("c")).with_virtual_partitions(8),
    )
    .unwrap();

    // Seed data across partitions.
    for i in 0..40 {
        cluster
            .put(
                &format!("k/{i}"),
                format!("v{i}").as_bytes(),
                DurabilityMode::Buffered,
            )
            .unwrap();
    }

    let target = PartitionId::new(0);
    // Rebalance partition 0 onto node 0 only.
    let report = cluster
        .rebalance_partition(target, vec![NodeId::new(0)])
        .unwrap();
    assert_eq!(
        report.job.phase,
        residiuum_cluster::RebalancePhase::Reclaimed
    );
    assert_eq!(
        cluster.directory().get(target).unwrap().replicas,
        vec![NodeId::new(0)]
    );

    // Write a key that hashes to partition 0 while node 0 is still up.
    let mut subject_on_p0 = None;
    for i in 0..200 {
        let s = format!("solo/{i}");
        if cluster.partition_for_subject(&s) == target {
            cluster
                .put(&s, b"only-on-0", DurabilityMode::Durable)
                .unwrap();
            subject_on_p0 = Some(s);
            break;
        }
    }
    assert!(subject_on_p0.is_some());

    cluster.mark_offline(NodeId::new(0)).unwrap();

    let found = cluster.scan_with(ScanOptions::default()).unwrap();
    assert!(found.coverage.is_incomplete());
    assert!(found.coverage.unavailable.contains(&target));
    // Must not present unavailable partition as empty success.
    assert!(!found.coverage.completed.contains(&target));
    // Incomplete coverage is the honesty contract (other partitions may still
    // return data under the same report).
    assert!(
        found.coverage.is_incomplete(),
        "incomplete coverage recorded"
    );
}

#[test]
fn resource_budget_marks_incomplete_not_empty_success() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::create(
        ClusterConfig::development(dir.path().join("d")).with_virtual_partitions(4),
    )
    .unwrap();

    for i in 0..30 {
        cluster
            .put(
                &format!("doc/{i:02}"),
                format!("{i}").as_bytes(),
                DurabilityMode::Memory,
            )
            .unwrap();
    }

    let found = cluster
        .find(ScanOptions::new().max_docs_scanned(3))
        .unwrap();
    assert!(found.coverage.is_incomplete());
    assert!(found.coverage.resource_limit_reached);
    // Partial data is still returned.
    assert!(!found.entries.is_empty());
    assert!(found.entries.len() <= 3);
}

#[test]
fn limit_truncates_with_flag() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::create(
        ClusterConfig::development(dir.path().join("d")).with_virtual_partitions(4),
    )
    .unwrap();
    for i in 0..10 {
        cluster
            .put(&format!("x/{i}"), b"v", DurabilityMode::Memory)
            .unwrap();
    }
    let found = cluster.find(ScanOptions::new().limit(3)).unwrap();
    assert!(found.truncated);
    assert_eq!(found.entries.len(), 3);
    // Limit alone does not make coverage incomplete (all partitions examined).
    assert!(found.coverage.is_complete() || !found.coverage.resource_limit_reached);
}

#[test]
fn scan_with_available_mode_note() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::create(
        ClusterConfig::development(dir.path().join("d")).with_virtual_partitions(2),
    )
    .unwrap();
    cluster.put("a", b"1", DurabilityMode::Durable).unwrap();
    let mut opts = ScanOptions::new();
    opts.read_mode = ReadMode::Available;
    let found = cluster.scan_with(opts).unwrap();
    assert_eq!(found.coverage.read_mode.as_deref(), Some("available"));
}
