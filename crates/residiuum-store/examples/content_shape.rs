//! Inspect Heap SubjectV2 key distribution and bounded payload samples.

use residiuum_format::decode_subject_v2;
use residiuum_store::{
    hex16, try_load_collections_catalog, try_load_heap_catalog, HeapMetaLayout, Store,
};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Default)]
struct Shape {
    count: u64,
    key_bytes: u64,
    first_keys: Vec<Vec<u8>>,
    last_key: Vec<u8>,
}

fn main() {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: content_shape STORE");
    let layout = HeapMetaLayout::new(&path);
    let store = Store::open_inspect(&path).expect("read-only inspect open");
    let mut names = BTreeMap::<([u8; 16], [u8; 16]), (String, String)>::new();
    if let Some(heaps) = try_load_heap_catalog(&layout).expect("heap catalog") {
        for heap in heaps {
            if let Some(objects) =
                try_load_collections_catalog(&layout, &heap.heap_id).expect("collection catalog")
            {
                for object in objects {
                    names.insert(
                        (heap.heap_id, object.object_id),
                        (heap.name.clone(), object.name),
                    );
                }
            }
        }
    }

    let mut shapes = BTreeMap::<([u8; 16], [u8; 16]), Shape>::new();
    let mut undecodable = 0u64;
    for subject in store.index_live_after(None, None) {
        let Ok(decoded) = decode_subject_v2(&subject) else {
            undecodable += 1;
            continue;
        };
        let key = (*decoded.heap_id, *decoded.object_id);
        let shape = shapes.entry(key).or_default();
        shape.count += 1;
        shape.key_bytes += decoded.key.len() as u64;
        if shape.first_keys.len() < 2 {
            shape.first_keys.push(subject.clone());
        }
        shape.last_key = subject;
    }

    println!("live_subjects={}", store.live_count());
    println!("undecodable_subjects={undecodable}");
    for ((heap_id, object_id), shape) in shapes {
        let (heap_name, collection_name) = names
            .get(&(heap_id, object_id))
            .cloned()
            .unwrap_or_else(|| (hex16(&heap_id), hex16(&object_id)));
        println!(
            "heap={heap_name} collection={collection_name} id={} records={} average_key_bytes={:.1}",
            hex16(&object_id),
            shape.count,
            shape.key_bytes as f64 / shape.count.max(1) as f64
        );
        let mut samples = shape.first_keys;
        if !shape.last_key.is_empty() && !samples.iter().any(|key| key == &shape.last_key) {
            samples.push(shape.last_key);
        }
        for subject in samples {
            let decoded = decode_subject_v2(&subject).expect("previously decoded subject");
            let key = String::from_utf8_lossy(decoded.key);
            match store.get_subject_bytes(&subject) {
                Ok(Some(body)) => println!(
                    "  key={key:?} body_bytes={} body={}",
                    body.len(),
                    render_body(&body)
                ),
                Ok(None) => println!("  key={key:?} body=(absent)"),
                Err(error) => println!("  key={key:?} body_error={error}"),
            }
        }
    }
}

fn render_body(body: &[u8]) -> String {
    let payload = body.strip_prefix(&[1]).unwrap_or(body);
    let rendered = String::from_utf8_lossy(payload).replace('\n', "\\n");
    let mut chars = rendered.chars();
    let prefix: String = chars.by_ref().take(1200).collect();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}
