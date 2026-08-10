//! DEF-040 — complete distributed query semantics.
//!
//! Acceptance (doc/done/incidents/DEFECTS.md):
//! - Randomized worker ordering produces identical sequence results.
//! - Coordinator failover neither silently duplicates nor omits rows.
//! - Partial partitions are never represented as empty complete partitions.
//!
//! Also: coverage on every page; integrity-tagged continuation; per-partition
//! frontiers + read mode; index/tier/resource fields end-to-end.

use residiuum_cluster::{
    Cluster, ClusterConfig, DurabilityMode, NodeId, PartitionId, QueryContinuation, ReadMode,
    ScanOptions,
};
use std::collections::HashSet;

fn seed_cluster(n: usize) -> (tempfile::TempDir, Cluster) {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::create(
        ClusterConfig::dependable_local(dir.path().join("c")).with_virtual_partitions(8),
    )
    .unwrap();
    for i in 0..n {
        // Zero-pad so subject order matches numeric intent.
        cluster
            .put(
                &format!("item/{i:04}"),
                format!("v{i}").as_bytes(),
                DurabilityMode::Buffered,
            )
            .unwrap();
    }
    (dir, cluster)
}

#[test]
fn randomized_visit_order_identical_merge() {
    let (_dir, mut cluster) = seed_cluster(40);
    let full = cluster.scan_with(ScanOptions::new()).unwrap();
    assert!(full.coverage.is_complete());
    assert!(!full.entries.is_empty());

    let mut parts: Vec<PartitionId> = full.coverage.requested.clone();
    // Reverse visit order (deterministic "shuffle").
    parts.reverse();
    let reversed = cluster
        .scan_with(ScanOptions::new().visit_order(parts.clone()))
        .unwrap();
    assert_eq!(
        full.entries, reversed.entries,
        "merge order must be independent of worker/visit order"
    );

    // Another permutation: rotate.
    let mut rotated = parts;
    if !rotated.is_empty() {
        let first = rotated.remove(0);
        rotated.push(first);
    }
    let rot = cluster
        .scan_with(ScanOptions::new().visit_order(rotated))
        .unwrap();
    assert_eq!(full.entries, rot.entries);

    // Subject order is strict ascending.
    for w in full.entries.windows(2) {
        assert!(w[0].0 < w[1].0);
    }
}

#[test]
fn multi_page_coverage_and_deterministic_sequence() {
    let (_dir, mut cluster) = seed_cluster(25);
    let full = cluster.scan_with(ScanOptions::new()).unwrap();
    let expected: Vec<String> = full.entries.iter().map(|(s, _)| s.clone()).collect();

    let mut gathered = Vec::new();
    let mut opts = ScanOptions::new().page_size(5);
    let mut pages = 0;
    let mut query_id: Option<String> = None;
    loop {
        let page = cluster.scan_page(opts).unwrap();
        pages += 1;
        assert!(
            page.coverage.read_mode.is_some(),
            "every page carries read mode"
        );
        assert!(
            page.coverage.indexes_used.contains(&"primary-scan".into()),
            "index limitations carried end-to-end"
        );
        assert!(
            page.coverage.tiers_searched.contains(&"hot".into()),
            "tier searched carried end-to-end"
        );
        assert!(!page.coverage.frontiers.is_empty() || page.coverage.is_incomplete());
        for (s, _) in &page.entries {
            gathered.push(s.clone());
        }
        match &query_id {
            None => query_id = Some(page.query_id.clone()),
            Some(qid) => assert_eq!(&page.query_id, qid, "query_id stable across pages"),
        }
        if !page.has_more {
            assert!(page.continuation.is_none());
            break;
        }
        let tok = page.continuation.expect("has_more implies continuation");
        // Token must authenticate for this cluster with secret keyring (DEF-097).
        let decoded =
            QueryContinuation::decode(cluster.cluster_id(), cluster.continuation_keyring(), &tok)
                .unwrap();
        assert_eq!(&decoded.query_id, query_id.as_ref().unwrap());
        opts = ScanOptions::new().continuation(tok);
        assert!(pages < 20, "page loop must terminate");
    }

    assert_eq!(
        gathered, expected,
        "paged scan must equal one-shot sequence"
    );
    assert!(
        pages >= 2,
        "expected multiple pages for page_size=5 over 25 keys"
    );
}

#[test]
fn coordinator_failover_resume_no_dup_no_omit() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("c");
    let mut cluster =
        Cluster::create(ClusterConfig::dependable_local(&root).with_virtual_partitions(8)).unwrap();
    for i in 0..30 {
        cluster
            .put(
                &format!("doc/{i:03}"),
                format!("b{i}").as_bytes(),
                DurabilityMode::Durable,
            )
            .unwrap();
    }
    let full: Vec<String> = cluster
        .scan_with(ScanOptions::new())
        .unwrap()
        .entries
        .into_iter()
        .map(|(s, _)| s)
        .collect();

    // First coordinator takes page 1.
    let page1 = cluster
        .scan_page(ScanOptions::new().page_size(7).prefix("doc/"))
        .unwrap();
    assert!(page1.has_more);
    assert_eq!(page1.entries.len(), 7);
    let tok = page1.continuation.clone().unwrap();
    let qid = page1.query_id.clone();
    drop(cluster);

    // Replacement coordinator re-opens the same cluster root (failover).
    let mut coord2 = Cluster::open(&root).unwrap();
    let mut gathered: Vec<String> = page1.entries.into_iter().map(|(s, _)| s).collect();
    let mut next = Some(tok);
    while let Some(token) = next {
        let page = coord2
            .scan_page(ScanOptions::new().continuation(token))
            .unwrap();
        assert_eq!(page.query_id, qid);
        // Coverage still honest after coordinator replacement.
        assert_eq!(page.coverage.read_mode.as_deref(), Some("linearizable"));
        for (s, _) in &page.entries {
            gathered.push(s.clone());
        }
        next = page.continuation;
        if !page.has_more {
            break;
        }
    }

    // No silent duplicates.
    let set: HashSet<_> = gathered.iter().cloned().collect();
    assert_eq!(
        set.len(),
        gathered.len(),
        "coordinator resume must not duplicate"
    );
    // No silent omissions relative to full prefix scan.
    let expected: Vec<String> = full.into_iter().filter(|s| s.starts_with("doc/")).collect();
    assert_eq!(gathered, expected, "coordinator resume must not omit rows");
}

#[test]
fn partial_partition_never_empty_complete_on_page() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::create(
        ClusterConfig::dependable_local(dir.path().join("c")).with_virtual_partitions(8),
    )
    .unwrap();
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
    cluster
        .rebalance_partition(target, vec![NodeId::new(0)])
        .unwrap();
    cluster.mark_offline(NodeId::new(0)).unwrap();

    // One-shot.
    let one = cluster.scan_with(ScanOptions::new()).unwrap();
    assert!(one.coverage.is_incomplete());
    assert!(one.coverage.unavailable.contains(&target));
    assert!(!one.coverage.completed.contains(&target));
    // Incomplete must never look like complete empty success.
    assert!(!one.coverage.is_complete());

    // Paged: every page reports the same unavailable partition honesty.
    let mut opts = ScanOptions::new().page_size(4);
    for _ in 0..8 {
        let page = cluster.scan_page(opts.clone()).unwrap();
        assert!(
            page.coverage.is_incomplete(),
            "partial partitions never complete on a page"
        );
        assert!(
            page.coverage.unavailable.contains(&target),
            "unavailable partition must appear on the page coverage"
        );
        // Entries from completed partitions are fine; absence of matches is not
        // proof of global emptiness when coverage is incomplete.
        if page.entries.is_empty() {
            assert!(
                page.coverage.is_incomplete(),
                "empty page under partial coverage is still incomplete"
            );
        }
        if !page.has_more {
            break;
        }
        opts = ScanOptions::new().continuation(page.continuation.unwrap());
    }
}

#[test]
fn resource_budget_carried_on_page_coverage() {
    let (_dir, mut cluster) = seed_cluster(50);
    let page = cluster
        .scan_page(
            ScanOptions::new()
                .page_size(10)
                .max_docs_scanned(5)
                .read_mode(ReadMode::Available),
        )
        .unwrap();
    assert!(page.coverage.resource_limit_reached);
    assert!(page.coverage.is_incomplete());
    assert_eq!(page.coverage.read_mode.as_deref(), Some("available"));
}

#[test]
fn continuation_rejects_tampered_token() {
    let (_dir, mut cluster) = seed_cluster(20);
    let page = cluster.scan_page(ScanOptions::new().page_size(3)).unwrap();
    assert!(page.has_more);
    let mut tok = page.continuation.unwrap();
    let i = tok.len() / 2;
    tok[i] ^= 0x5a;
    let err = cluster
        .scan_page(ScanOptions::new().continuation(tok))
        .unwrap_err();
    assert_eq!(err.code(), "continuation_invalid");
}

#[test]
fn frontiers_preserved_per_partition() {
    let (_dir, mut cluster) = seed_cluster(15);
    let page = cluster
        .scan_page(
            ScanOptions::new()
                .page_size(100)
                .read_mode(ReadMode::Linearizable),
        )
        .unwrap();
    assert!(page.coverage.is_complete());
    for p in &page.coverage.requested {
        assert!(
            page.coverage.frontiers.iter().any(|f| f.partition == *p),
            "frontier required for completed partition {}",
            p.get()
        );
    }
    assert_eq!(page.coverage.frontiers.len(), page.coverage.completed.len());
}
