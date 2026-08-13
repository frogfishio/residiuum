//! Write ATM-0.3 protocol vector fixtures. Run from repo root:
//! `cargo run -p residiuum-atomics --example dump_vectors`

use residiuum_atomics::{
    encode_canonical_plan, plan_content_root, AtomicId, AtomicPlan, AtomicPlanParts, AtomicProfile,
    CanonicalKey, CollectionId, CoordinationScope, HeapId, MutationKind, PlanMutation,
    PlanPredicate, PredicateKind, ReadWitness, ResourceLimits, VersionId,
};
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

fn hid(n: u8) -> HeapId {
    let mut b = [0u8; 16];
    b[0] = n;
    HeapId::from_bytes(b).unwrap()
}
fn cid(n: u8) -> CollectionId {
    let mut b = [0u8; 16];
    b[0] = n;
    CollectionId::from_bytes(b).unwrap()
}
fn aid(n: u8) -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = n;
    AtomicId::from_bytes(b).unwrap()
}
fn vid(n: u8) -> VersionId {
    let mut b = [0u8; 16];
    b[0] = n;
    VersionId::from_bytes(b).unwrap()
}
fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn create(coll: u8, k: &str, val: &[u8]) -> PlanMutation {
    PlanMutation {
        kind: MutationKind::Create,
        collection_id: cid(coll),
        key: CanonicalKey::String(k.to_owned()),
        encoded_value: Some(val.to_vec()),
        if_version: None,
    }
}

fn parts(profile: AtomicProfile, mutations: Vec<PlanMutation>) -> AtomicPlanParts {
    AtomicPlanParts {
        profile,
        atomic_id: aid(9),
        heap_id: hid(1),
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: Vec::new(),
        predicates: Vec::new(),
        mutations,
        active_rule_revisions: Vec::new(),
        limits: ResourceLimits::builder_defaults_local_heap(),
    }
}

fn emit(name: &str, note: &str, plan: AtomicPlan) -> serde_json::Value {
    let bytes = encode_canonical_plan(&plan).unwrap();
    let root = plan_content_root(&plan).unwrap();
    serde_json::json!({
        "name": name,
        "note": note,
        "bytes_hex": hex(&bytes),
        "content_root_hex": hex(root.as_bytes()),
    })
}

fn rejected(name: &str, note: &str, bytes_hex: &str, reason: &str) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "note": note,
        "bytes_hex": bytes_hex,
        "reason": reason,
    })
}

fn main() {
    let accepted = vec![
        emit(
            "create_one",
            "single create-if-absent",
            AtomicPlan::close(parts(
                AtomicProfile::LocalHeapV1,
                vec![create(1, "k", b"v")],
            ))
            .unwrap(),
        ),
        emit(
            "create_two_reordered",
            "two creates; builder order must not appear in bytes",
            AtomicPlan::close(parts(
                AtomicProfile::LocalHeapV1,
                vec![create(1, "zeta", b"z"), create(1, "alpha", b"a")],
            ))
            .unwrap(),
        ),
        emit(
            "put_unconditional",
            "explicit blind upsert",
            AtomicPlan::close(parts(
                AtomicProfile::LocalHeapV1,
                vec![PlanMutation {
                    kind: MutationKind::Put,
                    collection_id: cid(1),
                    key: CanonicalKey::String("k".into()),
                    encoded_value: Some(b"v".to_vec()),
                    if_version: None,
                }],
            ))
            .unwrap(),
        ),
        emit(
            "replace_versioned",
            "version-replace",
            AtomicPlan::close(parts(
                AtomicProfile::LocalHeapV1,
                vec![PlanMutation {
                    kind: MutationKind::Replace,
                    collection_id: cid(1),
                    key: CanonicalKey::String("k".into()),
                    encoded_value: Some(b"v2".to_vec()),
                    if_version: Some(vid(3)),
                }],
            ))
            .unwrap(),
        ),
        emit(
            "delete_versioned",
            "version-delete",
            AtomicPlan::close(parts(
                AtomicProfile::LocalHeapV1,
                vec![PlanMutation {
                    kind: MutationKind::Delete,
                    collection_id: cid(1),
                    key: CanonicalKey::String("k".into()),
                    encoded_value: None,
                    if_version: Some(vid(3)),
                }],
            ))
            .unwrap(),
        ),
        {
            let mut p = parts(AtomicProfile::LocalHeapV1, vec![create(1, "k", b"v")]);
            p.predicates.push(PlanPredicate {
                kind: PredicateKind::AssertAbsent,
                collection_id: Some(cid(2)),
                key: Some(CanonicalKey::String("lock".into())),
                version: None,
                encoded: None,
            });
            emit(
                "create_plus_assert_absent",
                "mutation plus public-builder assert",
                AtomicPlan::close(p).unwrap(),
            )
        },
        {
            let mut p = parts(AtomicProfile::LocalHeapV1, vec![create(1, "k", b"v")]);
            p.read_frontier = Some([7u8; 32]);
            p.reads.push(ReadWitness {
                collection_id: cid(1),
                key: CanonicalKey::String("k".into()),
                observed_version: None,
                projection_hash: [8u8; 32],
            });
            emit(
                "create_with_read_frontier",
                "write-plus-witness; frontier present",
                AtomicPlan::close(p).unwrap(),
            )
        },
        emit(
            "unknown_profile_preserved",
            "unknown profile decodes; execution_supported is false",
            AtomicPlan::close(parts(
                AtomicProfile::from_wire_code(99),
                vec![create(1, "k", b"v")],
            ))
            .unwrap(),
        ),
    ];

    let rejected = vec![
        rejected("empty", "no bytes", "", "cbor"),
        rejected("empty_array", "top-level array, not a map", "80", "cbor"),
        rejected(
            "empty_map",
            "map missing required plan fields",
            "a0",
            "malformed_input",
        ),
        rejected("profile_only", "only field 1", "a10101", "malformed_input"),
        rejected("bool_false", "simple value, not a map", "f4", "cbor"),
        rejected(
            "nested_duplicate_key",
            "nested map inside field 1 has duplicate key 1",
            "a101a201000101",
            "cbor",
        ),
        rejected(
            "nested_unsorted_keys",
            "nested map inside field 1 has keys 2 then 1",
            "a101a202000100",
            "cbor",
        ),
        serde_json::json!({
            "name": "duplicate_mutation_target",
            "note": "same (collection_id, canonical key) twice as a mutation",
            "reason": "duplicate_target",
            "stage": "close"
        }),
        serde_json::json!({
            "name": "create_missing_value",
            "note": "create without encoded_value",
            "reason": "invalid_value",
            "stage": "close"
        }),
        serde_json::json!({
            "name": "duplicate_read_identity",
            "note": "same (collection_id, canonical key) twice as a read witness",
            "reason": "malformed_input",
            "stage": "close"
        }),
        serde_json::json!({
            "name": "reads_without_frontier",
            "note": "prior-read witnesses require read_frontier",
            "reason": "malformed_input",
            "stage": "close"
        }),
    ];

    let proto = serde_json::json!({
        "profile": "residiuum-atomics-v1",
        "vectors": accepted,
    });
    let rej = serde_json::json!({
        "profile": "residiuum-atomics-v1",
        "vectors": rejected,
    });

    write_spec(
        "protocol-vectors.json",
        &format!("{}\n", serde_json::to_string_pretty(&proto).unwrap()),
    );
    write_spec(
        "rejected-vectors.json",
        &format!("{}\n", serde_json::to_string_pretty(&rej).unwrap()),
    );
}

fn write_spec(name: &str, body: &str) {
    let crate_spec = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("spec");
    fs::create_dir_all(&crate_spec).unwrap();
    fs::write(crate_spec.join(name), body).unwrap();
    eprintln!("wrote {}", crate_spec.join(name).display());
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spec/atomics");
    if workspace.is_dir() {
        fs::write(workspace.join(name), body).unwrap();
    }
}
