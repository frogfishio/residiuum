//! HP-004 Accept: delete rebuildable catalogs/indexes and reconstruct the same
//! names, aliases, IDs, and owner heap from surviving descriptors.

use residiuum_format::{HeapDescriptorState, ObjectDescriptorState};
use residiuum_store::{
    create_object, delete_rebuildable_catalogs, publish_staged_genesis,
    rebuild_and_persist_all_catalogs, rename_heap, rename_object, retire_object,
    stage_heap_genesis, try_load_collections_catalog, try_load_heap_catalog,
    try_load_streams_catalog, HeapMetaLayout, ObjectKind,
};
use tempfile::tempdir;

#[test]
fn delete_catalogs_rebuilds_identical_names_aliases_ids_owners() {
    let dir = tempdir().unwrap();
    let layout = HeapMetaLayout::new(dir.path());

    let heap_a = [0xAAu8; 16];
    let heap_b = [0xBBu8; 16];
    let deploy = [0xDDu8; 16];

    let staged_a = stage_heap_genesis(&layout, deploy, heap_a, [0x01u8; 16], "heap-a").unwrap();
    let staged_b = stage_heap_genesis(&layout, deploy, heap_b, [0x02u8; 16], "heap-b").unwrap();
    // Staged heaps are not discoverable via published catalog.
    assert!(try_load_heap_catalog(&layout).unwrap().is_none());

    publish_staged_genesis(&layout, &staged_a.staging_id, &staged_a.descriptor_hash).unwrap();
    publish_staged_genesis(&layout, &staged_b.staging_id, &staged_b.descriptor_hash).unwrap();

    rename_heap(&layout, &heap_a, "accounts").unwrap();

    let coll = [0xC1u8; 16];
    let stream = [0x51u8; 16];
    create_object(
        &layout,
        &heap_a,
        ObjectKind::Collection,
        coll,
        [0x31u8; 16],
        "users",
    )
    .unwrap();
    create_object(
        &layout,
        &heap_a,
        ObjectKind::Stream,
        stream,
        [0x32u8; 16],
        "events",
    )
    .unwrap();
    // Same collection name in another heap is independent.
    create_object(
        &layout,
        &heap_b,
        ObjectKind::Collection,
        [0xC2u8; 16],
        [0x33u8; 16],
        "users",
    )
    .unwrap();

    rename_object(&layout, &heap_a, ObjectKind::Collection, &coll, "people").unwrap();
    retire_object(&layout, &heap_a, ObjectKind::Stream, &stream).unwrap();

    let (heaps_before, objects_before) = rebuild_and_persist_all_catalogs(&layout).unwrap();
    let cat_before = try_load_heap_catalog(&layout).unwrap().unwrap();
    let cols_before = try_load_collections_catalog(&layout, &heap_a)
        .unwrap()
        .unwrap();
    let streams_before = try_load_streams_catalog(&layout, &heap_a).unwrap().unwrap();

    assert_eq!(heaps_before, cat_before);
    assert_eq!(cols_before.len(), 1);
    assert_eq!(cols_before[0].object_id, coll);
    assert_eq!(cols_before[0].name, "people");
    assert!(cols_before[0].aliases.iter().any(|a| a == "users"));
    assert_eq!(cols_before[0].heap_id, heap_a);
    assert_eq!(streams_before[0].state, ObjectDescriptorState::Retired);

    let accounts = cat_before.iter().find(|e| e.heap_id == heap_a).unwrap();
    assert_eq!(accounts.name, "accounts");
    assert!(accounts.aliases.iter().any(|a| a == "heap-a"));
    assert_eq!(accounts.state, HeapDescriptorState::Active);

    // Accept criterion: wipe accelerators, rebuild from descriptor chains only.
    delete_rebuildable_catalogs(&layout).unwrap();
    assert!(try_load_heap_catalog(&layout).unwrap().is_none());
    assert!(try_load_collections_catalog(&layout, &heap_a)
        .unwrap()
        .is_none());

    let (heaps_after, objects_after) = rebuild_and_persist_all_catalogs(&layout).unwrap();
    assert_eq!(heaps_before, heaps_after);
    assert_eq!(objects_before, objects_after);

    let cat_after = try_load_heap_catalog(&layout).unwrap().unwrap();
    assert_eq!(cat_before, cat_after);
    assert_eq!(
        cols_before,
        try_load_collections_catalog(&layout, &heap_a)
            .unwrap()
            .unwrap()
    );
    assert_eq!(
        streams_before,
        try_load_streams_catalog(&layout, &heap_a).unwrap().unwrap()
    );

    // Cross-heap isolation of collection namespaces survives rebuild.
    let b_cols = try_load_collections_catalog(&layout, &heap_b)
        .unwrap()
        .unwrap();
    assert_eq!(b_cols[0].name, "users");
    assert_eq!(b_cols[0].heap_id, heap_b);
    assert_ne!(b_cols[0].object_id, coll);
}
