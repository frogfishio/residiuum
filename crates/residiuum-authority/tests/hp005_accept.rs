//! HP-005 Accept: two-slot atomicity, fork fail-closed, genesis crash boundaries,
//! reload cannot mutate, data-server does not link authority.

use residiuum_authority::{
    apply_reload_request, commit_genesis, commit_prepared_genesis, issue_heap_key,
    peek_reload_request, prepare_genesis, AuthorityError, AuthorityPaths, AuthorityStoreError,
    EphemeralMasterKeyProvider, GenesisRequest, IssueRequest, MasterAuthorityStore,
    MasterKeyProvider,
};
use residiuum_heap::{DeploymentId, HeapId, Rights};
use residiuum_store::{try_load_heap_catalog, HeapMetaLayout};
use std::fs;
use tempfile::tempdir;

fn ids() -> ([u8; 16], [u8; 16], [u8; 16]) {
    let d = DeploymentId::new_random().unwrap();
    let h = HeapId::new_random().unwrap();
    let mut c = [0u8; 16];
    getrandom::fill(&mut c).unwrap();
    c[6] = (c[6] & 0x0f) | 0x40;
    c[8] = (c[8] & 0x3f) | 0x80;
    (*d.as_bytes(), *h.as_bytes(), c)
}

#[test]
fn genesis_commit_publishes_and_issues() {
    let dir = tempdir().unwrap();
    let auth = dir.path().join("auth");
    let data = dir.path().join("data");
    fs::create_dir_all(&auth).unwrap();
    fs::create_dir_all(&data).unwrap();
    let (deployment, heap, creation) = ids();
    let provider = EphemeralMasterKeyProvider::generate().unwrap();
    let result = commit_genesis(
        &provider,
        GenesisRequest {
            authority_root: auth.clone(),
            data_root: data.clone(),
            deployment_id: deployment,
            heap_id: heap,
            name: "prod".into(),
            creation_event_id: creation,
            effective_at: 1_700_000_000,
        },
    )
    .unwrap();

    let store = MasterAuthorityStore::open(AuthorityPaths::new(&auth, &deployment, &heap)).unwrap();
    let head = store.load_head().unwrap().expect("head");
    assert_eq!(head.storage_genesis_hash, result.descriptor_hash);
    assert_eq!(head.current_descriptor_hash, result.descriptor_hash);
    assert_eq!(head.master_public_key, result.master_public_key);

    let cat = try_load_heap_catalog(&HeapMetaLayout::new(&data))
        .unwrap()
        .expect("published catalog");
    assert_eq!(cat[0].heap_id, heap);
    assert_eq!(cat[0].name, "prod");

    let holder = EphemeralMasterKeyProvider::generate().unwrap();
    let issued = issue_heap_key(
        &store,
        &provider,
        IssueRequest {
            holder_public_key: holder.public_key(),
            rights: Rights::from_bits_certificate(0x5).unwrap(),
            not_before: 1_700_000_000,
            expires_at: 1_700_003_600,
        },
    )
    .unwrap();
    assert_ne!(issued.fingerprint, [0u8; 32]);
}

#[test]
fn staged_only_before_authority_commit_is_invisible() {
    let dir = tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let layout = HeapMetaLayout::new(&data);
    let (deployment, heap, creation) = ids();
    let staged =
        residiuum_store::stage_heap_genesis(&layout, deployment, heap, creation, "ghost").unwrap();
    assert!(try_load_heap_catalog(&layout).unwrap().is_none());
    // Authority never committed — published discovery stays empty.
    assert!(
        residiuum_store::load_staged_genesis(&layout, &staged.staging_id)
            .unwrap()
            .is_some()
    );
}

#[test]
fn exact_prepared_genesis_retry_is_idempotent() {
    let dir = tempdir().unwrap();
    let auth = dir.path().join("auth");
    let data = dir.path().join("data");
    fs::create_dir_all(&auth).unwrap();
    fs::create_dir_all(&data).unwrap();
    let (deployment, heap, creation) = ids();
    let provider = EphemeralMasterKeyProvider::generate().unwrap();
    let prepared = prepare_genesis(GenesisRequest {
        authority_root: auth,
        data_root: data.clone(),
        deployment_id: deployment,
        heap_id: heap,
        name: "restart-safe".into(),
        creation_event_id: creation,
        effective_at: 1_700_000_000,
    })
    .unwrap();
    let first = commit_prepared_genesis(&provider, &prepared).unwrap();
    let second = commit_prepared_genesis(&provider, &prepared).unwrap();
    assert_eq!(first.descriptor_hash, second.descriptor_hash);
    assert_eq!(
        first.authority_chain_head_hash,
        second.authority_chain_head_hash
    );
    let catalog = try_load_heap_catalog(&HeapMetaLayout::new(data))
        .unwrap()
        .unwrap();
    assert_eq!(catalog.len(), 1);
    assert_eq!(catalog[0].name, "restart-safe");
}

#[test]
fn equal_sequence_unequal_payloads_are_fork() {
    let dir = tempdir().unwrap();
    let auth = dir.path().join("auth");
    let data = dir.path().join("data");
    fs::create_dir_all(&auth).unwrap();
    fs::create_dir_all(&data).unwrap();
    let (deployment, heap, creation) = ids();
    let provider = EphemeralMasterKeyProvider::generate().unwrap();
    commit_genesis(
        &provider,
        GenesisRequest {
            authority_root: auth.clone(),
            data_root: data,
            deployment_id: deployment,
            heap_id: heap,
            name: "fork-me".into(),
            creation_event_id: creation,
            effective_at: 1_700_000_000,
        },
    )
    .unwrap();

    let store = MasterAuthorityStore::open(AuthorityPaths::new(&auth, &deployment, &heap)).unwrap();
    let head = store.load_head().unwrap().unwrap();
    let payload_a = head.encode_payload().unwrap();
    residiuum_authority::AuthorityHead::decode_payload(&payload_a).unwrap();

    let mut head_b = head.clone();
    head_b.current_descriptor_hash = [0xABu8; 32];
    let payload_b = head_b.encode_payload().unwrap();
    residiuum_authority::AuthorityHead::decode_payload(&payload_b).unwrap();
    assert_ne!(payload_a, payload_b);
    assert_eq!(head.file_sequence, head_b.file_sequence);

    let paths = AuthorityPaths::new(&auth, &deployment, &heap);
    fs::write(
        paths.heap_dir().join("head.a.cbor"),
        residiuum_authority::slot_encode_for_test(&payload_a),
    )
    .unwrap();
    fs::write(
        paths.heap_dir().join("head.b.cbor"),
        residiuum_authority::slot_encode_for_test(&payload_b),
    )
    .unwrap();

    let store = MasterAuthorityStore::open(paths).unwrap();
    let err = store.detect_slot_fork().unwrap_err();
    assert!(matches!(
        err,
        AuthorityError::Store(AuthorityStoreError::Fork)
    ));
}

#[test]
fn reload_reads_head_but_cannot_commit() {
    let dir = tempdir().unwrap();
    let auth = dir.path().join("auth");
    let data = dir.path().join("data");
    fs::create_dir_all(&auth).unwrap();
    fs::create_dir_all(&data).unwrap();
    let (deployment, heap, creation) = ids();
    let provider = EphemeralMasterKeyProvider::generate().unwrap();
    let result = commit_genesis(
        &provider,
        GenesisRequest {
            authority_root: auth,
            data_root: data.clone(),
            deployment_id: deployment,
            heap_id: heap,
            name: "reload".into(),
            creation_event_id: creation,
            effective_at: 1_700_000_000,
        },
    )
    .unwrap();

    assert!(peek_reload_request(&data).unwrap().is_some());
    let snap = apply_reload_request(&data).unwrap().expect("snap");
    assert_eq!(
        snap.authority_chain_head_hash,
        result.authority_chain_head_hash
    );
    // Pending cleared; second apply is a no-op.
    assert!(apply_reload_request(&data).unwrap().is_none());
    // Reload API surface has no commit_head — enforced by type system / crate split.
}

#[test]
fn residiuum_server_does_not_depend_on_residiuum_authority() {
    let toml = fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../residiuum-server/Cargo.toml"),
    )
    .unwrap();
    assert!(
        !toml.contains("residiuum-authority"),
        "residiuum-server must not depend on residiuum-authority"
    );
    assert!(
        !toml.contains("authority-provisioning"),
        "residiuum-server must not enable authority-provisioning"
    );
}
