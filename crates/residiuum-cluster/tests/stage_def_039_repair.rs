//! DEF-039 — anti-entropy inventory and replica repair.
//!
//! Acceptance (doc/done/incidents/DEFECTS.md / CLUSTER_SPEC §15.3):
//! - Corrupt / newer-mtime replicas never overwrite healthy evidence.
//! - Random deletion/corruption converges to policy while preserving
//!   explicit irrecoverable holes.
//! - Repair sources selected by integrity + majority, never mtime.
//! - Every repair is audited; rate limits bound a pass.

use residiuum_cluster::{
    Cluster, ClusterConfig, DurabilityMode, NodeId, RepairActionKind, RepairOptions,
    ReplicaObservation, ANTI_ENTROPY_PROFILE, REPAIR_AUDIT_FILE,
};

#[test]
fn profile_label_is_stable() {
    assert_eq!(ANTI_ENTROPY_PROFILE, "residiuum-anti-entropy-v1");
    assert_eq!(REPAIR_AUDIT_FILE, "repair_audit.json");
    assert_eq!(Cluster::anti_entropy_profile(), ANTI_ENTROPY_PROFILE);
}

#[test]
fn missing_replica_converges_from_majority() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("miss");
    let mut cluster =
        Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(4)).unwrap();

    let subject = "repair/missing-key";
    cluster
        .put(subject, b"canonical-body", DurabilityMode::Durable)
        .unwrap();
    let p = cluster.partition_for_subject(subject);
    let replicas = cluster.directory().get(p).unwrap().replicas.clone();
    assert!(replicas.len() >= 2, "dependable-local needs multi-replica");

    // Delete live subject from one online replica only (simulates random loss).
    let victim = replicas[0];
    cluster
        .store_delete_local(victim, subject, DurabilityMode::Durable)
        .unwrap();
    assert!(cluster.store_get_local(victim, subject).unwrap().is_none());

    let inv = cluster.inventory_partition(p).unwrap();
    let subj = inv
        .subjects
        .iter()
        .find(|s| s.subject == subject)
        .expect("subject in inventory");
    assert!(subj.target_hash_hex.is_some());
    assert!(!subj.irrecoverable);
    assert!(inv.needs_repair >= 1);

    let report = cluster
        .repair_partition(p, RepairOptions::unlimited())
        .unwrap();
    assert!(
        report.subjects_repaired >= 1 || report.copies_written >= 1,
        "expected repair work: {:?}",
        report
    );

    for n in &replicas {
        if !cluster.is_online(*n) {
            continue;
        }
        let body = cluster
            .store_get_local(*n, subject)
            .unwrap()
            .expect("restored");
        assert_eq!(body, b"canonical-body", "node {}", n.index());
    }

    let after = cluster.inventory_partition(p).unwrap();
    let subj_after = after
        .subjects
        .iter()
        .find(|s| s.subject == subject)
        .unwrap();
    let target = subj_after.target_hash_hex.clone().unwrap();
    assert!(
        subj_after.views.iter().all(|v| {
            v.observation == ReplicaObservation::Healthy && v.content_hash_hex == target
        }),
        "all healthy after repair: {:?}",
        subj_after.views
    );
}

#[test]
fn divergent_newer_body_never_overwrites_majority() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("div");
    let mut cluster =
        Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(4)).unwrap();

    let subject = "repair/divergent-key";
    cluster
        .put(subject, b"healthy-majority", DurabilityMode::Durable)
        .unwrap();
    let p = cluster.partition_for_subject(subject);
    let replicas = cluster.directory().get(p).unwrap().replicas.clone();
    assert!(replicas.len() >= 3, "need 3 replicas for majority story");

    // Overwrite one replica with evil body AFTER the healthy put (newer wall
    // clock / newer segment position). CLUSTER_SPEC §15.3: must not win.
    let evil = replicas[0];
    cluster
        .store_put_local(evil, subject, b"evil-newer-mtime", DurabilityMode::Durable)
        .unwrap();

    let report = cluster.anti_entropy_once().unwrap();
    assert!(
        report.copies_written >= 1 || report.subjects_repaired >= 1,
        "divergent replica must be repaired: {:?}",
        report
    );

    for n in &replicas {
        if !cluster.is_online(*n) {
            continue;
        }
        let body = cluster.store_get_local(*n, subject).unwrap().unwrap();
        assert_eq!(
            body,
            b"healthy-majority",
            "node {} must not keep evil body",
            n.index()
        );
    }

    let audit = cluster.repair_audit().unwrap();
    assert!(!audit.entries.is_empty());
    assert!(audit
        .entries
        .iter()
        .any(|e| e.action == RepairActionKind::Copied && e.subject == subject));
    assert!(audit.entries.iter().any(|e| {
        e.action == RepairActionKind::Copied && e.subject == subject && e.destination == Some(evil)
    }));
}

#[test]
fn equal_split_preserves_conflict_without_mtime_winner() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("conf");
    let mut cluster =
        Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(4)).unwrap();

    let subject = "repair/conflict-key";
    let p = cluster.partition_for_subject(subject);
    let mut replicas = cluster.directory().get(p).unwrap().replicas.clone();
    replicas.sort();
    // Offline all but two to create a 1-1 split among online voters.
    for n in replicas.iter().skip(2).copied().collect::<Vec<_>>() {
        cluster.mark_offline(n).unwrap();
    }
    let a = replicas[0];
    let b = replicas[1];
    cluster
        .store_put_local(a, subject, b"variant-a", DurabilityMode::Durable)
        .unwrap();
    cluster
        .store_put_local(b, subject, b"variant-b", DurabilityMode::Durable)
        .unwrap();

    let inv = cluster.inventory_partition(p).unwrap();
    let subj = inv
        .subjects
        .iter()
        .find(|s| s.subject == subject)
        .expect("conflict subject inventoried");
    assert!(subj.conflicting, "1-1 split must be conflicting");
    assert!(subj.target_hash_hex.is_none());

    let report = cluster
        .repair_partition(p, RepairOptions::unlimited())
        .unwrap();
    assert!(report.conflicts_preserved >= 1);
    assert_eq!(report.subjects_repaired, 0);

    assert_eq!(
        cluster.store_get_local(a, subject).unwrap().unwrap(),
        b"variant-a"
    );
    assert_eq!(
        cluster.store_get_local(b, subject).unwrap().unwrap(),
        b"variant-b"
    );
}

#[test]
fn empty_cluster_repair_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("hole");
    let mut cluster =
        Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(2)).unwrap();

    let inv = cluster.inventory_cluster().unwrap();
    assert_eq!(inv.needs_repair, 0);
    let report = cluster.anti_entropy_once().unwrap();
    assert_eq!(report.subjects_repaired, 0);
    assert_eq!(report.irrecoverable_holes, 0);
}

#[test]
fn rate_limit_bounds_repair_pass() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("rate");
    let mut cluster =
        Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(8)).unwrap();

    for i in 0..40 {
        let s = format!("repair/rate/{i}");
        let _ = cluster.put(&s, b"body", DurabilityMode::Durable);
    }

    // Delete every live subject on node 0.
    let subjects = cluster.store_live_subjects_local(NodeId::new(0)).unwrap();
    for s in &subjects {
        cluster
            .store_delete_local(NodeId::new(0), s, DurabilityMode::Durable)
            .unwrap();
    }

    // Ensure at least one repair need exists.
    let before = cluster.inventory_cluster().unwrap();
    if before.needs_repair == 0 {
        let s = "repair/rate/0";
        let p = cluster.partition_for_subject(s);
        let victim = cluster.directory().get(p).unwrap().replicas[0];
        cluster
            .store_delete_local(victim, s, DurabilityMode::Durable)
            .unwrap();
    }

    let report = cluster
        .repair_cluster(RepairOptions::unlimited().max_subjects(1))
        .unwrap();
    assert!(
        report.budget_exhausted || report.subjects_repaired <= 1,
        "rate limit should bound work: {:?}",
        report
    );
    assert!(report.subjects_repaired <= 1);
}

#[test]
fn audit_survives_coordinator_restart() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("aud");
    {
        let mut cluster =
            Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(4))
                .unwrap();
        let subject = "repair/audit-key";
        cluster
            .put(subject, b"v1", DurabilityMode::Durable)
            .unwrap();
        let p = cluster.partition_for_subject(subject);
        let victim = cluster.directory().get(p).unwrap().replicas[0];
        cluster
            .store_delete_local(victim, subject, DurabilityMode::Durable)
            .unwrap();
        cluster.anti_entropy_once().unwrap();
        assert!(root.join(REPAIR_AUDIT_FILE).is_file());
    }
    let cluster = Cluster::open(&root).unwrap();
    let audit = cluster.repair_audit().unwrap();
    assert!(!audit.entries.is_empty());
    assert_eq!(audit.format, ANTI_ENTROPY_PROFILE);
}

#[test]
fn segment_fingerprints_appear_in_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("seg");
    let mut cluster =
        Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(4)).unwrap();
    cluster
        .put("repair/seg", b"x", DurabilityMode::Durable)
        .unwrap();
    let p = cluster.partition_for_subject("repair/seg");
    let inv = cluster.inventory_partition(p).unwrap();
    assert!(!inv.segment_fingerprints.is_empty());
    assert!(inv
        .segment_fingerprints
        .iter()
        .any(|(_, fp)| !fp.is_empty()));
}
