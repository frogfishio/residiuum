//! Write ATM-0.7 evidence vector fixtures. Run from repo root:
//! `cargo run -p residiuum-atomics --example dump_evidence`

use residiuum_atomics::{
    decision_hash, encode_decision, encode_member, encode_prepare, encode_tombstone, member_hash,
    ordered_member_manifest_root, prepare_hash, tombstone_hash, AtomicAbortReason, AtomicDecision,
    AtomicId, AtomicMember, AtomicPrepare, CanonicalKey, CollectionId, ContentRoot,
    CoordinationScope, HeapId, MutationKind, ObjectIdentity, ResourceLimits, VersionId,
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

fn ev(name: &str, record: &str, note: &str, bytes: &[u8], hash: [u8; 32]) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "record": record,
        "note": note,
        "bytes_hex": hex(bytes),
        "hash_hex": hex(&hash),
    })
}

fn ev_rej(
    name: &str,
    record: &str,
    note: &str,
    bytes_hex: &str,
    reason: &str,
) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "record": record,
        "note": note,
        "bytes_hex": bytes_hex,
        "reason": reason,
    })
}

fn main() {
    let content_root = ContentRoot::from_bytes([7u8; 32]).unwrap();
    let member = AtomicMember {
        atomic_id: aid(9),
        ordinal: 0,
        object_identity: ObjectIdentity::new(cid(1), CanonicalKey::String("k".into())),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some([8u8; 32]),
        event_id: vid(3),
    };
    let member_b = encode_member(&member).unwrap();
    let member_h = member_hash(&member).unwrap();
    let put_overwrite = AtomicMember {
        atomic_id: member.atomic_id,
        ordinal: member.ordinal,
        object_identity: member.object_identity.clone(),
        member_kind: MutationKind::Put,
        before_version: Some(vid(3)),
        after_content_hash: member.after_content_hash,
        event_id: member.event_id,
    };
    let put_b = encode_member(&put_overwrite).unwrap();
    let put_h = member_hash(&put_overwrite).unwrap();
    let manifest = ordered_member_manifest_root(hid(1), std::slice::from_ref(&member)).unwrap();
    let prepare = AtomicPrepare {
        atomic_id: aid(9),
        heap_id: hid(1),
        scope: CoordinationScope::LocalHeap,
        content_root,
        frontier: [1u8; 32],
        ordered_member_manifest_root: manifest,
        read_set_root: [3u8; 32],
        predicate_set_root: [4u8; 32],
        active_rule_revision_root: [5u8; 32],
        limits: ResourceLimits::builder_defaults_local_heap(),
    };
    let prepare_b = encode_prepare(&prepare).unwrap();
    let prepare_h = prepare_hash(&prepare).unwrap();
    let committed = AtomicDecision::committed(aid(9), prepare_h, manifest, 1, 1).unwrap();
    let committed_b = encode_decision(&committed).unwrap();
    let committed_h = decision_hash(&committed).unwrap();
    let committed_stone = committed.tombstone(content_root, committed_h);
    let committed_tb = encode_tombstone(&committed_stone).unwrap();
    let committed_th = tombstone_hash(&committed_stone).unwrap();
    let aborted = AtomicDecision::not_committed(
        aid(9),
        prepare_h,
        manifest,
        0,
        AtomicAbortReason::PreconditionConflict,
    );
    let aborted_b = encode_decision(&aborted).unwrap();
    let aborted_h = decision_hash(&aborted).unwrap();
    let aborted_stone = aborted.tombstone(content_root, aborted_h);
    let aborted_tb = encode_tombstone(&aborted_stone).unwrap();
    let aborted_th = tombstone_hash(&aborted_stone).unwrap();

    let accepted = vec![
        ev(
            "prepare_local_heap",
            "prepare",
            "prepare bound to the member manifest root",
            &prepare_b,
            prepare_h,
        ),
        ev(
            "member_create_with_object_identity",
            "member",
            "create member names collection plus canonical key",
            &member_b,
            member_h,
        ),
        ev(
            "member_put_overwrite_with_before_version",
            "member",
            "put overwrite records before_version and after_content_hash",
            &put_b,
            put_h,
        ),
        ev(
            "decision_committed",
            "decision",
            "committed decision omits abort_reason",
            &committed_b,
            committed_h,
        ),
        ev(
            "decision_not_committed_precondition",
            "decision",
            "not_committed preserves AtomicAbortReason::PreconditionConflict",
            &aborted_b,
            aborted_h,
        ),
        ev(
            "tombstone_committed",
            "tombstone",
            "committed tombstone omits abort_reason",
            &committed_tb,
            committed_th,
        ),
        ev(
            "tombstone_not_committed_precondition",
            "tombstone",
            "tombstone copies abort_reason for outcome reconstruction",
            &aborted_tb,
            aborted_th,
        ),
    ];
    let rejected = vec![
        ev_rej("empty", "prepare", "no bytes", "", "cbor"),
        ev_rej(
            "nested_duplicate_in_object_identity_key",
            "member",
            "object_identity.key map has duplicate field 1",
            "a101a201000101",
            "cbor",
        ),
        ev_rej(
            "decision_code_unknown",
            "decision",
            "map{1:1} is not a decision record; unknown decision codes are unit-tested",
            "a10101",
            "cbor",
        ),
        ev_rej(
            "member_create_without_after_hash",
            "member",
            "create requires after_content_hash",
            "a50158200900000000000000000000000000000000000000000000000000000000000000020003a201500100000000000000000000000000000002a2010102416b0401075003000000000000000000000000000000",
            "invalid_value",
        ),
        ev_rej(
            "member_create_with_before_version",
            "member",
            "create must omit before_version",
            "a70158200900000000000000000000000000000000000000000000000000000000000000020003a201500100000000000000000000000000000002a2010102416b04010550030000000000000000000000000000000658200808080808080808080808080808080808080808080808080808080808080808075003000000000000000000000000000000",
            "invalid_value",
        ),
        ev_rej(
            "member_create_with_before_without_after",
            "member",
            "create with before_version and no after_content_hash",
            "a60158200900000000000000000000000000000000000000000000000000000000000000020003a201500100000000000000000000000000000002a2010102416b0401055003000000000000000000000000000000075003000000000000000000000000000000",
            "invalid_value",
        ),
        ev_rej(
            "member_put_without_after_hash",
            "member",
            "put requires after_content_hash",
            "a50158200900000000000000000000000000000000000000000000000000000000000000020003a201500100000000000000000000000000000002a2010102416b0402075003000000000000000000000000000000",
            "invalid_value",
        ),
        ev_rej(
            "member_put_with_before_without_after",
            "member",
            "put with before_version and no after_content_hash",
            "a60158200900000000000000000000000000000000000000000000000000000000000000020003a201500100000000000000000000000000000002a2010102416b0402055003000000000000000000000000000000075003000000000000000000000000000000",
            "invalid_value",
        ),
        ev_rej(
            "member_replace_without_before_version",
            "member",
            "replace requires before_version",
            "a60158200900000000000000000000000000000000000000000000000000000000000000020003a201500100000000000000000000000000000002a2010102416b04030658200808080808080808080808080808080808080808080808080808080808080808075003000000000000000000000000000000",
            "invalid_value",
        ),
        ev_rej(
            "member_replace_without_either",
            "member",
            "replace requires before_version and after_content_hash",
            "a50158200900000000000000000000000000000000000000000000000000000000000000020003a201500100000000000000000000000000000002a2010102416b0403075003000000000000000000000000000000",
            "invalid_value",
        ),
        ev_rej(
            "member_replace_without_after_hash",
            "member",
            "replace requires after_content_hash",
            "a60158200900000000000000000000000000000000000000000000000000000000000000020003a201500100000000000000000000000000000002a2010102416b0403055003000000000000000000000000000000075003000000000000000000000000000000",
            "invalid_value",
        ),
        ev_rej(
            "member_delete_with_after_hash",
            "member",
            "delete must omit after_content_hash",
            "a70158200900000000000000000000000000000000000000000000000000000000000000020003a201500100000000000000000000000000000002a2010102416b04040550030000000000000000000000000000000658200808080808080808080808080808080808080808080808080808080808080808075003000000000000000000000000000000",
            "invalid_value",
        ),
        ev_rej(
            "member_delete_without_before_version",
            "member",
            "delete requires before_version",
            "a50158200900000000000000000000000000000000000000000000000000000000000000020003a201500100000000000000000000000000000002a2010102416b0404075003000000000000000000000000000000",
            "invalid_value",
        ),
        ev_rej(
            "member_delete_without_before_with_after",
            "member",
            "delete with after_content_hash and no before_version",
            "a60158200900000000000000000000000000000000000000000000000000000000000000020003a201500100000000000000000000000000000002a2010102416b04040658200808080808080808080808080808080808080808080808080808080808080808075003000000000000000000000000000000",
            "invalid_value",
        ),
    ];
    let doc = serde_json::json!({
        "profile": "residiuum-atomics-v1",
        "accepted": accepted,
        "rejected": rejected,
    });
    let body = serde_json::to_string_pretty(&doc).unwrap() + "\n";
    let crate_spec = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("spec");
    fs::create_dir_all(&crate_spec).unwrap();
    fs::write(crate_spec.join("evidence-vectors.json"), &body).unwrap();
    eprintln!(
        "wrote {}",
        crate_spec.join("evidence-vectors.json").display()
    );
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spec/atomics");
    if workspace.is_dir() {
        fs::write(workspace.join("evidence-vectors.json"), body).unwrap();
    }
}
