//! Watermark growth must not break seal, and seal must not publish prealloc tails.
use residiuum_store::{DiagnosticIoSink, DurabilityMode, SegmentGrowthPolicy, Store};
use std::path::PathBuf;

fn put_n(store: &mut Store, n: usize) {
    let payload = vec![0xABu8; 8192];
    for i in 0..n {
        let k = format!("peer/{i:020}");
        store.put(&k, &payload, DurabilityMode::Buffered).unwrap();
    }
}

fn sealed_sizes(store_root: &PathBuf) -> Vec<u64> {
    let dir = store_root.join("segments");
    let Ok(rd) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    rd.filter_map(|e| e.ok())
        .filter_map(|e| std::fs::metadata(e.path()).ok().map(|m| m.len()))
        .collect()
}

#[test]
fn diag_watermark_seal_succeeds_without_clobbering_prefix() {
    let root = PathBuf::from(std::env::temp_dir()).join("wm_seal_diag_fix");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let mut store = Store::create_with_shards(&root, 1).unwrap();
    store.set_seal_threshold(512 * 1024 * 1024);
    store
        .set_diagnostic_io_sink(DiagnosticIoSink::RealPreallocWatermark)
        .unwrap();
    put_n(&mut store, 1000);
    store
        .seal_active()
        .expect("diag watermark seal must succeed");
    let sizes = sealed_sizes(&root);
    assert_eq!(sizes.len(), 1, "expected one sealed segment, got {sizes:?}");
    // Must not rename the full 512 MiB prealloc tail.
    assert!(
        sizes[0] < 64 * 1024 * 1024,
        "sealed size {} still looks like prealloc capacity",
        sizes[0]
    );
}

#[test]
fn product_watermark_seal_truncates_prealloc_tail() {
    let root = PathBuf::from(std::env::temp_dir()).join("wm_seal_product_fix");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let mut store = Store::create_with_shards(&root, 1).unwrap();
    store.set_seal_threshold(512 * 1024 * 1024);
    store
        .set_segment_growth_policy(SegmentGrowthPolicy::watermark_default())
        .unwrap();
    put_n(&mut store, 1000);
    store
        .seal_active()
        .expect("product watermark seal must succeed");
    let sizes = sealed_sizes(&root);
    assert_eq!(sizes.len(), 1, "expected one sealed segment, got {sizes:?}");
    assert!(
        sizes[0] < 64 * 1024 * 1024,
        "sealed size {} still looks like prealloc capacity",
        sizes[0]
    );
}
