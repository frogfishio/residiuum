//! Format-side §38.1 / HP-000 vector checks (decode committed bytes; no encode oracle).

use residiuum_format::{
    admit_frame_to_heap, decode_heap_descriptor, decode_object_descriptor, decode_subject_v2,
    descriptor_hash, parse_ownership_envelope, AdmitDecision, OwnershipEvidence, SubjectObjectKind,
};
use std::fs;
use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn hex(s: &str) -> Vec<u8> {
    hex::decode(s).unwrap()
}

fn b16(s: &str) -> [u8; 16] {
    hex(s).try_into().unwrap()
}

#[test]
fn format_vectors_decode_without_encoder_oracle() {
    let doc: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(workspace_root().join("spec/heap/vectors-v1.json")).unwrap(),
    )
    .unwrap();
    let fv = &doc["format_vectors"];
    let inputs = &fv["inputs"];
    let heap = b16(inputs["heap_id"].as_str().unwrap());
    let deployment = b16(inputs["deployment_id"].as_str().unwrap());
    let creation = b16(inputs["creation_event_id"].as_str().unwrap());
    let collection = b16(inputs["collection_id"].as_str().unwrap());
    let stream = b16(inputs["stream_id"].as_str().unwrap());

    for v in fv["accepted"].as_array().unwrap() {
        match v["kind"].as_str().unwrap() {
            "subject_v2" => {
                let bytes = hex(v["bytes"].as_str().unwrap());
                let s = decode_subject_v2(&bytes).expect(v["id"].as_str().unwrap());
                assert_eq!(*s.heap_id, heap);
                assert_eq!(
                    s.object_kind as u8,
                    v["object_kind"].as_u64().unwrap() as u8
                );
                match s.object_kind {
                    SubjectObjectKind::HeapMetadata => assert_eq!(*s.object_id, [0u8; 16]),
                    SubjectObjectKind::Collection => assert_eq!(*s.object_id, collection),
                    SubjectObjectKind::Stream => assert_eq!(*s.object_id, stream),
                }
            }
            "heap_descriptor" => {
                let body = hex(v["body"].as_str().unwrap());
                let d = decode_heap_descriptor(&body).unwrap();
                assert_eq!(d.heap_id, heap);
                assert_eq!(d.origin_deployment_id, deployment);
                assert_eq!(d.creation_event_id, creation);
                assert_eq!(d.sequence, v["sequence"].as_u64().unwrap());
                assert_eq!(d.name, v["name"].as_str().unwrap());
                assert_eq!(
                    hex::encode(descriptor_hash(&body)),
                    v["descriptor_hash"].as_str().unwrap()
                );
                // Second decoder: map must have exactly 11 uint keys.
                let map = residiuum_format::decode_deterministic_uint_map(&body).unwrap();
                assert_eq!(map.len(), 11);
            }
            "object_descriptor" => {
                let body = hex(v["body"].as_str().unwrap());
                let d = decode_object_descriptor(&body).unwrap();
                assert_eq!(d.heap_id, heap);
                assert_eq!(d.object_id, collection);
                assert_eq!(d.name, v["name"].as_str().unwrap());
                assert_eq!(
                    hex::encode(descriptor_hash(&body)),
                    v["descriptor_hash"].as_str().unwrap()
                );
            }
            "ownership_envelope" => {
                let bytes = hex(v["bytes"].as_str().unwrap());
                match parse_ownership_envelope(&bytes).unwrap() {
                    OwnershipEvidence::Known { heap_id, .. } => assert_eq!(heap_id, heap),
                    other => panic!("expected known: {other:?}"),
                }
            }
            "admit_decision" => {
                let bound = b16(v["bound_heap"].as_str().unwrap());
                let seg = hex(v["segment_envelope"].as_str().unwrap());
                let frame = hex(v["frame_envelope"].as_str().unwrap());
                let subj = v["subject"].as_str().map(hex);
                let d = admit_frame_to_heap(&bound, &seg, &frame, subj.as_deref());
                assert!(
                    matches!(d, AdmitDecision::Admit { .. }),
                    "{} => {d:?}",
                    v["id"]
                );
            }
            other => panic!("unknown kind {other}"),
        }
    }

    for v in fv["rejected"].as_array().unwrap() {
        match v["kind"].as_str().unwrap() {
            "admit_decision" => {
                let bound = b16(v["bound_heap"].as_str().unwrap());
                let seg = hex(v["segment_envelope"].as_str().unwrap());
                let frame = hex(v["frame_envelope"].as_str().unwrap());
                let d = admit_frame_to_heap(&bound, &seg, &frame, None);
                match v["decision"].as_str().unwrap() {
                    "reject_conflict_or_wrong_heap" => assert!(
                        matches!(
                            d,
                            AdmitDecision::RejectConflict | AdmitDecision::RejectWrongHeap { .. }
                        ),
                        "{d:?}"
                    ),
                    "reject_malformed" => {
                        assert!(matches!(d, AdmitDecision::RejectMalformed), "{d:?}")
                    }
                    other => panic!("{other}"),
                }
            }
            "subject_v2" => {
                assert!(decode_subject_v2(&hex(v["bytes"].as_str().unwrap())).is_err());
            }
            "heap_descriptor" => {
                assert!(decode_heap_descriptor(&hex(v["body"].as_str().unwrap())).is_err());
            }
            other => panic!("unknown reject kind {other}"),
        }
    }
}
