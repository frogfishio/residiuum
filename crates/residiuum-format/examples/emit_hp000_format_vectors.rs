//! One-shot emitter for committed HP-000 format vectors (not a test oracle).
//! Run: `cargo run -p residiuum-format --example emit_hp000_format_vectors`
//! Capture stdout and paste into `spec/heap/vectors-v1.json` under `format_vectors`.

use residiuum_format::{
    descriptor_hash, encode_heap_binding_envelope, encode_heap_descriptor,
    encode_object_descriptor, encode_subject_v2, HeapDescriptor, HeapDescriptorState,
    ObjectDescriptor, ObjectDescriptorState, SubjectObjectKind,
};

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn main() {
    // Literal inputs matching vectors-v1.json (and fixed collection/stream ids).
    let heap: [u8; 16] = [
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x46, 0x17, 0x98, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f,
    ];
    let deployment: [u8; 16] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x46, 0x07, 0x88, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];
    let creation: [u8; 16] = [
        0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x46, 0x27, 0xa8, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e,
        0x2f,
    ];
    let collection = [0xaau8; 16];
    let stream = [0xbbu8; 16];

    let subj_meta =
        encode_subject_v2(&heap, SubjectObjectKind::HeapMetadata, &[0u8; 16], b"\x01").unwrap();
    let subj_coll =
        encode_subject_v2(&heap, SubjectObjectKind::Collection, &collection, b"user-1").unwrap();
    let subj_stream = encode_subject_v2(&heap, SubjectObjectKind::Stream, &stream, b"evt").unwrap();

    let heap_desc = HeapDescriptor {
        origin_deployment_id: deployment,
        heap_id: heap,
        creation_event_id: creation,
        created_at: 1_700_000_000,
        predecessor_hash: None,
        sequence: 1,
        state: HeapDescriptorState::Active,
        name: "accounts".into(),
        aliases: vec![],
    };
    let heap_body = encode_heap_descriptor(&heap_desc).unwrap();
    let heap_hash = descriptor_hash(&heap_body);

    let obj_desc = ObjectDescriptor {
        heap_id: heap,
        object_id: collection,
        creation_event_id: creation,
        created_at: 1_700_000_000,
        predecessor_hash: None,
        sequence: 1,
        name: "users".into(),
        aliases: vec![],
        state: ObjectDescriptorState::Active,
    };
    let obj_body = encode_object_descriptor(&obj_desc).unwrap();
    let obj_hash = descriptor_hash(&obj_body);

    let env = encode_heap_binding_envelope(&heap).unwrap();

    println!("subject_heap_metadata={}", hex(&subj_meta));
    println!("subject_collection={}", hex(&subj_coll));
    println!("subject_stream={}", hex(&subj_stream));
    println!("heap_descriptor_body={}", hex(&heap_body));
    println!("heap_descriptor_hash={}", hex(&heap_hash));
    println!("object_descriptor_body={}", hex(&obj_body));
    println!("object_descriptor_hash={}", hex(&obj_hash));
    println!("heap_binding_envelope={}", hex(&env));
}
