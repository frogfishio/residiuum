//! Heap and object catalogs (`HEAP_SPEC` §34.2 / HP-004).
//!
//! Authoritative mapping lives in integrity-valid descriptor chains. Rebuildable
//! CBOR catalogs and indexes are accelerators only: deleting them and rebuilding
//! from surviving descriptors MUST restore the same names, aliases, IDs, and
//! owner heap.
//!
//! Staged genesis writes under `meta/staging/` are invisible to published-heap
//! discovery. Publishing installs descriptor-chain bytes only; this package does
//! **not** bind authority (HP-005).

use crate::atomic_file::write_atomic;
use crate::error::StoreError;
use crate::ids::random_id;
use crate::layout::{hex16, unhex16};
use residiuum_format::{
    decode_frame, decode_heap_descriptor, decode_object_descriptor, descriptor_hash,
    encode_collection_binding_envelope, encode_deterministic_uint_map, encode_frame,
    encode_heap_binding_envelope, encode_heap_descriptor, encode_object_descriptor,
    encode_stream_binding_envelope, encode_subject_v2, CborValue, DecodedFrame, FrameHeader,
    FrameKind, FrameParts, HeapDescriptor, HeapDescriptorState, ObjectDescriptor,
    ObjectDescriptorState, SafetyLimits, SubjectObjectKind,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Deployment-wide rebuildable heap-name index filename under `meta/`.
pub const HEAP_CATALOG_FILE: &str = "heap-catalog.v1.cbor";
/// Per-heap rebuildable collection catalog.
pub const COLLECTIONS_CATALOG_FILE: &str = "collections.v1.cbor";
/// Per-heap rebuildable stream catalog.
pub const STREAMS_CATALOG_FILE: &str = "streams.v1.cbor";
/// Rebuildable ASCII tip-hash hint.
pub const DESCRIPTOR_HEAD_FILE: &str = "descriptor-head";
/// Staging marker filename.
pub const STAGED_MANIFEST_FILE: &str = "manifest.cbor";
/// Staged / chain frame filename pattern uses `.frame` suffix.
pub const FRAME_SUFFIX: &str = ".frame";

/// Kind of object catalog entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ObjectKind {
    /// Collection.
    Collection = 1,
    /// Stream.
    Stream = 2,
}

impl ObjectKind {
    fn as_frame_kind(self) -> FrameKind {
        match self {
            Self::Collection => FrameKind::CollectionDescriptor,
            Self::Stream => FrameKind::StreamDescriptor,
        }
    }

    fn subject_kind(self) -> SubjectObjectKind {
        match self {
            Self::Collection => SubjectObjectKind::Collection,
            Self::Stream => SubjectObjectKind::Stream,
        }
    }

    fn from_u64(v: u64) -> Result<Self, StoreError> {
        match v {
            1 => Ok(Self::Collection),
            2 => Ok(Self::Stream),
            _ => Err(StoreError::HeapAdmit("unknown object kind".into())),
        }
    }
}

/// One published heap tip reconstructed from its descriptor chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapCatalogEntry {
    /// Owner heap id.
    pub heap_id: [u8; 16],
    /// Canonical name at tip.
    pub name: String,
    /// Live aliases at tip.
    pub aliases: Vec<String>,
    /// Tip descriptor hash (§34.7).
    pub descriptor_hash: [u8; 32],
    /// Tip sequence.
    pub sequence: u64,
    /// Administrative state at tip.
    pub state: HeapDescriptorState,
    /// Origin deployment preserved from genesis.
    pub origin_deployment_id: [u8; 16],
}

/// One collection or stream tip reconstructed from its descriptor chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCatalogEntry {
    /// Owner heap.
    pub heap_id: [u8; 16],
    /// Immutable object id.
    pub object_id: [u8; 16],
    /// Collection or stream.
    pub kind: ObjectKind,
    /// Canonical name at tip.
    pub name: String,
    /// Live aliases at tip.
    pub aliases: Vec<String>,
    /// Tip descriptor hash.
    pub descriptor_hash: [u8; 32],
    /// Tip sequence.
    pub sequence: u64,
    /// State at tip.
    pub state: ObjectDescriptorState,
}

/// Result of staging a heap-storage genesis descriptor (not published).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedGenesis {
    /// Opaque staging directory id (not a heap id).
    pub staging_id: [u8; 16],
    /// Heap id allocated for this genesis.
    pub heap_id: [u8; 16],
    /// §34.7 hash of the staged sequence-1 body.
    pub descriptor_hash: [u8; 32],
    /// Canonical name.
    pub name: String,
}

/// Local administrative receipt (forensic; not an authorization database).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminReceipt {
    /// Receipt id.
    pub receipt_id: [u8; 16],
    /// Operation tag.
    pub operation: String,
    /// Heap id.
    pub heap_id: [u8; 16],
    /// Optional object id.
    pub object_id: Option<[u8; 16]>,
    /// Descriptor hash involved.
    pub descriptor_hash: [u8; 32],
    /// Unix seconds.
    pub created_at: u64,
}

/// Paths under a data root for heap catalog / descriptor history (§34.2).
#[derive(Debug, Clone)]
pub struct HeapMetaLayout {
    data_root: PathBuf,
}

impl HeapMetaLayout {
    /// Construct from the store / data root.
    pub fn new(data_root: impl Into<PathBuf>) -> Self {
        Self {
            data_root: data_root.into(),
        }
    }

    /// `meta/`
    pub fn meta_dir(&self) -> PathBuf {
        self.data_root.join("meta")
    }

    /// Deployment-wide rebuildable heap catalog.
    pub fn heap_catalog_path(&self) -> PathBuf {
        self.meta_dir().join(HEAP_CATALOG_FILE)
    }

    /// Non-discoverable staging root (never scanned for published heaps).
    pub fn staging_root(&self) -> PathBuf {
        self.meta_dir().join("staging")
    }

    /// Staging directory for one genesis attempt.
    pub fn staging_dir(&self, staging_id: &[u8; 16]) -> PathBuf {
        self.staging_root().join(hex16(staging_id))
    }

    /// `meta/heaps/<heap-id-hex>/`
    pub fn heap_dir(&self, heap_id: &[u8; 16]) -> PathBuf {
        self.meta_dir().join("heaps").join(hex16(heap_id))
    }

    /// Heap descriptor-chain directory.
    pub fn heap_chain_dir(&self, heap_id: &[u8; 16]) -> PathBuf {
        self.heap_dir(heap_id).join("descriptor-chain")
    }

    /// Heap descriptor-head tip hint.
    pub fn heap_head_path(&self, heap_id: &[u8; 16]) -> PathBuf {
        self.heap_dir(heap_id).join(DESCRIPTOR_HEAD_FILE)
    }

    /// Rebuildable collections catalog for one heap.
    pub fn collections_catalog_path(&self, heap_id: &[u8; 16]) -> PathBuf {
        self.heap_dir(heap_id).join(COLLECTIONS_CATALOG_FILE)
    }

    /// Rebuildable streams catalog for one heap.
    pub fn streams_catalog_path(&self, heap_id: &[u8; 16]) -> PathBuf {
        self.heap_dir(heap_id).join(STREAMS_CATALOG_FILE)
    }

    /// Heap-scoped derived index directory (`indexes/{heap_hex}/`).
    ///
    /// Derived indexes MUST never be shared across heaps (Gate H3 / §26.4).
    pub fn heap_index_dir(&self, heap_id: &[u8; 16]) -> PathBuf {
        self.data_root.join("indexes").join(hex16(heap_id))
    }

    /// Object descriptor root (collections or streams).
    pub fn object_dir(
        &self,
        heap_id: &[u8; 16],
        kind: ObjectKind,
        object_id: &[u8; 16],
    ) -> PathBuf {
        let sub = match kind {
            ObjectKind::Collection => "collections",
            ObjectKind::Stream => "streams",
        };
        self.heap_dir(heap_id).join(sub).join(hex16(object_id))
    }

    /// Object descriptor-chain directory.
    pub fn object_chain_dir(
        &self,
        heap_id: &[u8; 16],
        kind: ObjectKind,
        object_id: &[u8; 16],
    ) -> PathBuf {
        self.object_dir(heap_id, kind, object_id)
            .join("descriptor-chain")
    }

    /// Object descriptor-head tip hint.
    pub fn object_head_path(
        &self,
        heap_id: &[u8; 16],
        kind: ObjectKind,
        object_id: &[u8; 16],
    ) -> PathBuf {
        self.object_dir(heap_id, kind, object_id)
            .join(DESCRIPTOR_HEAD_FILE)
    }

    /// Local receipts directory for a heap.
    pub fn receipts_dir(&self, heap_id: &[u8; 16]) -> PathBuf {
        self.heap_dir(heap_id).join("receipts")
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn hex32(h: &[u8; 32]) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn chain_frame_name(sequence: u64, hash: &[u8; 32]) -> String {
    format!("{sequence:020}-{hex}", hex = hex32(hash)) + FRAME_SUFFIX
}

fn expect_b16(v: &CborValue) -> Result<[u8; 16], StoreError> {
    match v {
        CborValue::Bytes(b) if b.len() == 16 => {
            let mut a = [0u8; 16];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(StoreError::HeapAdmit("catalog bstr16".into())),
    }
}

fn expect_b32(v: &CborValue) -> Result<[u8; 32], StoreError> {
    match v {
        CborValue::Bytes(b) if b.len() == 32 => {
            let mut a = [0u8; 32];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(StoreError::HeapAdmit("catalog bstr32".into())),
    }
}

fn expect_text(v: &CborValue) -> Result<String, StoreError> {
    match v {
        CborValue::Text(s) => Ok(s.clone()),
        _ => Err(StoreError::HeapAdmit("catalog text".into())),
    }
}

fn expect_uint(v: &CborValue) -> Result<u64, StoreError> {
    match v {
        CborValue::Uint(u) => Ok(*u),
        _ => Err(StoreError::HeapAdmit("catalog uint".into())),
    }
}

fn expect_aliases(v: &CborValue) -> Result<Vec<String>, StoreError> {
    match v {
        CborValue::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(expect_text(item)?);
            }
            Ok(out)
        }
        _ => Err(StoreError::HeapAdmit("catalog aliases".into())),
    }
}

fn encode_heap_frame(desc: &HeapDescriptor) -> Result<(Vec<u8>, [u8; 32]), StoreError> {
    let body = encode_heap_descriptor(desc).map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let hash = descriptor_hash(&body);
    let envelope = encode_heap_binding_envelope(&desc.heap_id)
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let mut key = vec![0x01u8];
    key.extend_from_slice(&desc.sequence.to_be_bytes());
    let _subject = encode_subject_v2(
        &desc.heap_id,
        SubjectObjectKind::HeapMetadata,
        &[0u8; 16],
        &key,
    )
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let header = FrameHeader::new_draft(
        FrameKind::HeapDescriptor,
        envelope.len() as u32,
        body.len() as u64,
        desc.creation_event_id,
    );
    let frame = encode_frame(&FrameParts {
        header,
        envelope,
        body,
    })
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    Ok((frame, hash))
}

fn encode_object_frame(
    kind: ObjectKind,
    desc: &ObjectDescriptor,
) -> Result<(Vec<u8>, [u8; 32]), StoreError> {
    let body = encode_object_descriptor(desc).map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let hash = descriptor_hash(&body);
    let envelope = match kind {
        ObjectKind::Collection => {
            encode_collection_binding_envelope(&desc.heap_id, &desc.object_id)
        }
        ObjectKind::Stream => encode_stream_binding_envelope(&desc.heap_id, &desc.object_id),
    }
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let mut key = vec![0x00u8];
    key.extend_from_slice(&desc.sequence.to_be_bytes());
    let _subject = encode_subject_v2(&desc.heap_id, kind.subject_kind(), &desc.object_id, &key)
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let header = FrameHeader::new_draft(
        kind.as_frame_kind(),
        envelope.len() as u32,
        body.len() as u64,
        desc.creation_event_id,
    );
    let frame = encode_frame(&FrameParts {
        header,
        envelope,
        body,
    })
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    Ok((frame, hash))
}

fn write_chain_frame(
    dir: &Path,
    sequence: u64,
    hash: &[u8; 32],
    frame: &[u8],
) -> Result<(), StoreError> {
    fs::create_dir_all(dir)?;
    let path = dir.join(chain_frame_name(sequence, hash));
    if path.exists() {
        let existing = fs::read(&path)?;
        if existing != frame {
            return Err(StoreError::HeapAdmit("descriptor chain conflict".into()));
        }
        return Ok(());
    }
    write_atomic(&path, frame)?;
    Ok(())
}

fn write_head_hint(path: &Path, hash: &[u8; 32]) -> Result<(), StoreError> {
    write_atomic(path, format!("{}\n", hex32(hash)).as_bytes())
}

fn list_chain_frames(dir: &Path) -> Result<Vec<(u64, [u8; 32], PathBuf)>, StoreError> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for ent in fs::read_dir(dir)? {
        let ent = ent?;
        let name = ent.file_name();
        let name = name.to_string_lossy();
        if !name.ends_with(FRAME_SUFFIX) {
            continue;
        }
        let stem = name.trim_end_matches(FRAME_SUFFIX);
        let (seq_s, hash_s) = stem
            .split_once('-')
            .ok_or_else(|| StoreError::HeapAdmit("bad chain filename".into()))?;
        let sequence: u64 = seq_s
            .parse()
            .map_err(|_| StoreError::HeapAdmit("bad chain sequence".into()))?;
        let hash = unhex32(hash_s).ok_or_else(|| StoreError::HeapAdmit("bad chain hash".into()))?;
        out.push((sequence, hash, ent.path()));
    }
    out.sort_by_key(|(s, _, _)| *s);
    Ok(out)
}

fn load_verified_frame(path: &Path) -> Result<DecodedFrame, StoreError> {
    let bytes = fs::read(path)?;
    decode_frame(&bytes, SafetyLimits::default())
        .map_err(|e| StoreError::HeapAdmit(format!("frame: {e}")))
}

/// Reconstruct the unique contiguous heap descriptor tip from chain frames.
pub fn rebuild_heap_entry_from_chain(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
) -> Result<Option<HeapCatalogEntry>, StoreError> {
    let frames = list_chain_frames(&layout.heap_chain_dir(heap_id))?;
    if frames.is_empty() {
        return Ok(None);
    }
    let mut expected_seq = 1u64;
    let mut pred: Option<[u8; 32]> = None;
    let mut tip: Option<HeapCatalogEntry> = None;
    for (sequence, file_hash, path) in frames {
        if sequence != expected_seq {
            // Missing middle → stop at last contiguous tip.
            break;
        }
        let decoded = load_verified_frame(&path)?;
        if decoded.header.known_kind() != Some(FrameKind::HeapDescriptor) {
            return Err(StoreError::HeapAdmit("non-heap frame in heap chain".into()));
        }
        let body_hash = descriptor_hash(&decoded.body);
        if body_hash != file_hash {
            return Err(StoreError::HeapAdmit("chain filename hash mismatch".into()));
        }
        let desc = decode_heap_descriptor(&decoded.body)
            .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
        if &desc.heap_id != heap_id {
            return Err(StoreError::HeapAdmit("descriptor heap mismatch".into()));
        }
        if desc.sequence != sequence {
            return Err(StoreError::HeapAdmit("descriptor sequence mismatch".into()));
        }
        if desc.predecessor_hash != pred {
            return Err(StoreError::HeapAdmit("predecessor hash mismatch".into()));
        }
        tip = Some(HeapCatalogEntry {
            heap_id: desc.heap_id,
            name: desc.name,
            aliases: desc.aliases,
            descriptor_hash: body_hash,
            sequence: desc.sequence,
            state: desc.state,
            origin_deployment_id: desc.origin_deployment_id,
        });
        pred = Some(body_hash);
        expected_seq += 1;
    }
    Ok(tip)
}

/// Reconstruct one object tip from its chain.
pub fn rebuild_object_entry_from_chain(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    kind: ObjectKind,
    object_id: &[u8; 16],
) -> Result<Option<ObjectCatalogEntry>, StoreError> {
    let frames = list_chain_frames(&layout.object_chain_dir(heap_id, kind, object_id))?;
    if frames.is_empty() {
        return Ok(None);
    }
    let mut expected_seq = 1u64;
    let mut pred: Option<[u8; 32]> = None;
    let mut tip: Option<ObjectCatalogEntry> = None;
    for (sequence, file_hash, path) in frames {
        if sequence != expected_seq {
            break;
        }
        let decoded = load_verified_frame(&path)?;
        if decoded.header.known_kind() != Some(kind.as_frame_kind()) {
            return Err(StoreError::HeapAdmit(
                "object kind mismatch in chain".into(),
            ));
        }
        let body_hash = descriptor_hash(&decoded.body);
        if body_hash != file_hash {
            return Err(StoreError::HeapAdmit("object chain hash mismatch".into()));
        }
        let desc = decode_object_descriptor(&decoded.body)
            .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
        if &desc.heap_id != heap_id || &desc.object_id != object_id {
            return Err(StoreError::HeapAdmit("object ownership mismatch".into()));
        }
        if desc.sequence != sequence || desc.predecessor_hash != pred {
            return Err(StoreError::HeapAdmit("object chain link invalid".into()));
        }
        tip = Some(ObjectCatalogEntry {
            heap_id: desc.heap_id,
            object_id: desc.object_id,
            kind,
            name: desc.name,
            aliases: desc.aliases,
            descriptor_hash: body_hash,
            sequence: desc.sequence,
            state: desc.state,
        });
        pred = Some(body_hash);
        expected_seq += 1;
    }
    Ok(tip)
}

fn list_object_ids(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    kind: ObjectKind,
) -> Result<Vec<[u8; 16]>, StoreError> {
    let sub = match kind {
        ObjectKind::Collection => "collections",
        ObjectKind::Stream => "streams",
    };
    let dir = layout.heap_dir(heap_id).join(sub);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for ent in fs::read_dir(dir)? {
        let ent = ent?;
        if !ent.file_type()?.is_dir() {
            continue;
        }
        let name = ent.file_name();
        let Some(id) = unhex16(&name.to_string_lossy()) else {
            continue;
        };
        ids.push(id);
    }
    ids.sort();
    Ok(ids)
}

fn list_published_heap_ids(layout: &HeapMetaLayout) -> Result<Vec<[u8; 16]>, StoreError> {
    let dir = layout.meta_dir().join("heaps");
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for ent in fs::read_dir(dir)? {
        let ent = ent?;
        if !ent.file_type()?.is_dir() {
            continue;
        }
        let name = ent.file_name();
        let Some(id) = unhex16(&name.to_string_lossy()) else {
            continue;
        };
        // Only published heaps have a descriptor-chain directory with frames.
        if layout.heap_chain_dir(&id).is_dir() {
            ids.push(id);
        }
    }
    ids.sort();
    Ok(ids)
}

fn encode_heap_catalog(entries: &[HeapCatalogEntry]) -> Result<Vec<u8>, StoreError> {
    let mut arr = Vec::with_capacity(entries.len());
    for e in entries {
        arr.push(CborValue::Map(vec![
            (1u64, CborValue::Bytes(e.heap_id.to_vec())),
            (2, CborValue::Text(e.name.clone())),
            (
                3,
                CborValue::Array(e.aliases.iter().cloned().map(CborValue::Text).collect()),
            ),
            (4, CborValue::Bytes(e.descriptor_hash.to_vec())),
            (5, CborValue::Uint(e.state as u64)),
            (6, CborValue::Uint(e.sequence)),
            (7, CborValue::Bytes(e.origin_deployment_id.to_vec())),
        ]));
    }
    encode_deterministic_uint_map(&[(1u64, CborValue::Uint(1)), (2, CborValue::Array(arr))])
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))
}

fn decode_heap_catalog(bytes: &[u8]) -> Result<Vec<HeapCatalogEntry>, StoreError> {
    let map = residiuum_format::decode_deterministic_uint_map(bytes)
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let mut version = None;
    let mut entries = None;
    for (k, v) in map {
        match k {
            1 => version = Some(expect_uint(&v)?),
            2 => match v {
                CborValue::Array(items) => {
                    let mut out = Vec::with_capacity(items.len());
                    for item in items {
                        let CborValue::Map(fields) = item else {
                            return Err(StoreError::HeapAdmit("heap catalog entry".into()));
                        };
                        let mut heap_id = None;
                        let mut name = None;
                        let mut aliases = None;
                        let mut hash = None;
                        let mut state = None;
                        let mut sequence = None;
                        let mut origin = None;
                        for (fk, fv) in fields {
                            match fk {
                                1 => heap_id = Some(expect_b16(&fv)?),
                                2 => name = Some(expect_text(&fv)?),
                                3 => aliases = Some(expect_aliases(&fv)?),
                                4 => hash = Some(expect_b32(&fv)?),
                                5 => {
                                    state = Some(
                                        HeapDescriptorState::from_u64(expect_uint(&fv)?)
                                            .map_err(|e| StoreError::HeapAdmit(e.to_string()))?,
                                    )
                                }
                                6 => sequence = Some(expect_uint(&fv)?),
                                7 => origin = Some(expect_b16(&fv)?),
                                _ => {}
                            }
                        }
                        out.push(HeapCatalogEntry {
                            heap_id: heap_id.ok_or_else(|| {
                                StoreError::HeapAdmit("heap catalog heap_id".into())
                            })?,
                            name: name
                                .ok_or_else(|| StoreError::HeapAdmit("heap catalog name".into()))?,
                            aliases: aliases.unwrap_or_default(),
                            descriptor_hash: hash
                                .ok_or_else(|| StoreError::HeapAdmit("heap catalog hash".into()))?,
                            sequence: sequence.unwrap_or(1),
                            state: state.unwrap_or(HeapDescriptorState::Active),
                            origin_deployment_id: origin.unwrap_or([0u8; 16]),
                        });
                    }
                    entries = Some(out);
                }
                _ => return Err(StoreError::HeapAdmit("heap catalog array".into())),
            },
            _ => {}
        }
    }
    if version != Some(1) {
        return Err(StoreError::HeapAdmit("heap catalog version".into()));
    }
    Ok(entries.unwrap_or_default())
}

fn encode_object_catalog(
    heap_id: &[u8; 16],
    entries: &[ObjectCatalogEntry],
) -> Result<Vec<u8>, StoreError> {
    let mut arr = Vec::with_capacity(entries.len());
    for e in entries {
        arr.push(CborValue::Map(vec![
            (1u64, CborValue::Bytes(e.object_id.to_vec())),
            (2, CborValue::Text(e.name.clone())),
            (
                3,
                CborValue::Array(e.aliases.iter().cloned().map(CborValue::Text).collect()),
            ),
            (4, CborValue::Bytes(e.descriptor_hash.to_vec())),
            (5, CborValue::Uint(e.state as u64)),
            (6, CborValue::Uint(e.sequence)),
            (7, CborValue::Uint(e.kind as u64)),
        ]));
    }
    encode_deterministic_uint_map(&[
        (1u64, CborValue::Uint(1)),
        (2, CborValue::Bytes(heap_id.to_vec())),
        (3, CborValue::Array(arr)),
    ])
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))
}

fn decode_object_catalog(bytes: &[u8]) -> Result<([u8; 16], Vec<ObjectCatalogEntry>), StoreError> {
    let map = residiuum_format::decode_deterministic_uint_map(bytes)
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let mut version = None;
    let mut heap_id = None;
    let mut entries = None;
    for (k, v) in map {
        match k {
            1 => version = Some(expect_uint(&v)?),
            2 => heap_id = Some(expect_b16(&v)?),
            3 => match v {
                CborValue::Array(items) => {
                    let hid = heap_id.ok_or_else(|| {
                        StoreError::HeapAdmit("object catalog heap before entries".into())
                    })?;
                    let mut out = Vec::with_capacity(items.len());
                    for item in items {
                        let CborValue::Map(fields) = item else {
                            return Err(StoreError::HeapAdmit("object catalog entry".into()));
                        };
                        let mut object_id = None;
                        let mut name = None;
                        let mut aliases = None;
                        let mut hash = None;
                        let mut state = None;
                        let mut sequence = None;
                        let mut kind = None;
                        for (fk, fv) in fields {
                            match fk {
                                1 => object_id = Some(expect_b16(&fv)?),
                                2 => name = Some(expect_text(&fv)?),
                                3 => aliases = Some(expect_aliases(&fv)?),
                                4 => hash = Some(expect_b32(&fv)?),
                                5 => {
                                    state = Some(
                                        ObjectDescriptorState::from_u64(expect_uint(&fv)?)
                                            .map_err(|e| StoreError::HeapAdmit(e.to_string()))?,
                                    )
                                }
                                6 => sequence = Some(expect_uint(&fv)?),
                                7 => kind = Some(ObjectKind::from_u64(expect_uint(&fv)?)?),
                                _ => {}
                            }
                        }
                        out.push(ObjectCatalogEntry {
                            heap_id: hid,
                            object_id: object_id
                                .ok_or_else(|| StoreError::HeapAdmit("object id".into()))?,
                            kind: kind.unwrap_or(ObjectKind::Collection),
                            name: name
                                .ok_or_else(|| StoreError::HeapAdmit("object name".into()))?,
                            aliases: aliases.unwrap_or_default(),
                            descriptor_hash: hash
                                .ok_or_else(|| StoreError::HeapAdmit("object hash".into()))?,
                            sequence: sequence.unwrap_or(1),
                            state: state.unwrap_or(ObjectDescriptorState::Active),
                        });
                    }
                    entries = Some(out);
                }
                _ => return Err(StoreError::HeapAdmit("object catalog array".into())),
            },
            _ => {}
        }
    }
    if version != Some(1) {
        return Err(StoreError::HeapAdmit("object catalog version".into()));
    }
    Ok((
        heap_id.ok_or_else(|| StoreError::HeapAdmit("object catalog heap".into()))?,
        entries.unwrap_or_default(),
    ))
}

fn write_admin_receipt(layout: &HeapMetaLayout, receipt: &AdminReceipt) -> Result<(), StoreError> {
    let dir = layout.receipts_dir(&receipt.heap_id);
    fs::create_dir_all(&dir)?;
    let obj = match receipt.object_id {
        None => CborValue::Null,
        Some(id) => CborValue::Bytes(id.to_vec()),
    };
    let bytes = encode_deterministic_uint_map(&[
        (1u64, CborValue::Uint(1)),
        (2, CborValue::Bytes(receipt.receipt_id.to_vec())),
        (3, CborValue::Text(receipt.operation.clone())),
        (4, CborValue::Bytes(receipt.heap_id.to_vec())),
        (5, obj),
        (6, CborValue::Bytes(receipt.descriptor_hash.to_vec())),
        (7, CborValue::Uint(receipt.created_at)),
    ])
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let name = format!(
        "{:020}-{}.cbor",
        receipt.created_at,
        hex16(&receipt.receipt_id)
    );
    write_atomic(&dir.join(name), &bytes)
}

fn mint_receipt(
    operation: &str,
    heap_id: [u8; 16],
    object_id: Option<[u8; 16]>,
    descriptor_hash: [u8; 32],
) -> Result<AdminReceipt, StoreError> {
    Ok(AdminReceipt {
        receipt_id: random_id()?,
        operation: operation.into(),
        heap_id,
        object_id,
        descriptor_hash,
        created_at: now_secs(),
    })
}

/// Stage a non-discoverable heap-storage genesis (sequence-1 descriptor).
///
/// Does not update published catalogs. HP-005 binds authority to this hash.
pub fn stage_heap_genesis(
    layout: &HeapMetaLayout,
    origin_deployment_id: [u8; 16],
    heap_id: [u8; 16],
    creation_event_id: [u8; 16],
    name: &str,
) -> Result<StagedGenesis, StoreError> {
    let staging_id = random_id()?;
    let desc = HeapDescriptor {
        origin_deployment_id,
        heap_id,
        creation_event_id,
        created_at: now_secs(),
        predecessor_hash: None,
        sequence: 1,
        state: HeapDescriptorState::Active,
        name: name.to_string(),
        aliases: vec![],
    };
    let (frame, hash) = encode_heap_frame(&desc)?;
    let dir = layout.staging_dir(&staging_id);
    fs::create_dir_all(&dir)?;
    write_atomic(
        &dir.join(format!("00000000000000000001-{}.frame", hex32(&hash))),
        &frame,
    )?;
    let manifest = encode_deterministic_uint_map(&[
        (1u64, CborValue::Uint(1)),
        (2, CborValue::Bytes(staging_id.to_vec())),
        (3, CborValue::Bytes(heap_id.to_vec())),
        (4, CborValue::Bytes(hash.to_vec())),
        (5, CborValue::Text(name.to_string())),
        (6, CborValue::Bytes(origin_deployment_id.to_vec())),
        (7, CborValue::Bytes(creation_event_id.to_vec())),
    ])
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    write_atomic(&dir.join(STAGED_MANIFEST_FILE), &manifest)?;
    Ok(StagedGenesis {
        staging_id,
        heap_id,
        descriptor_hash: hash,
        name: name.to_string(),
    })
}

/// Load staged genesis metadata; returns `None` if absent.
pub fn load_staged_genesis(
    layout: &HeapMetaLayout,
    staging_id: &[u8; 16],
) -> Result<Option<StagedGenesis>, StoreError> {
    let path = layout.staging_dir(staging_id).join(STAGED_MANIFEST_FILE);
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    let map = residiuum_format::decode_deterministic_uint_map(&bytes)
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let mut staging = None;
    let mut heap_id = None;
    let mut hash = None;
    let mut name = None;
    for (k, v) in map {
        match k {
            2 => staging = Some(expect_b16(&v)?),
            3 => heap_id = Some(expect_b16(&v)?),
            4 => hash = Some(expect_b32(&v)?),
            5 => name = Some(expect_text(&v)?),
            _ => {}
        }
    }
    Ok(Some(StagedGenesis {
        staging_id: staging.unwrap_or(*staging_id),
        heap_id: heap_id.ok_or_else(|| StoreError::HeapAdmit("staged heap_id".into()))?,
        descriptor_hash: hash.ok_or_else(|| StoreError::HeapAdmit("staged hash".into()))?,
        name: name.ok_or_else(|| StoreError::HeapAdmit("staged name".into()))?,
    }))
}

/// Publish byte-identical staged genesis into the heap descriptor chain.
///
/// Updates rebuildable catalogs. Does **not** activate authority (HP-005).
pub fn publish_staged_genesis(
    layout: &HeapMetaLayout,
    staging_id: &[u8; 16],
    expected_hash: &[u8; 32],
) -> Result<AdminReceipt, StoreError> {
    let staged = load_staged_genesis(layout, staging_id)?
        .ok_or_else(|| StoreError::HeapAdmit("staged genesis missing".into()))?;
    if &staged.descriptor_hash != expected_hash {
        return Err(StoreError::HeapAdmit("staged hash mismatch".into()));
    }
    let dir = layout.staging_dir(staging_id);
    let frame_name = format!(
        "00000000000000000001-{}.frame",
        hex32(&staged.descriptor_hash)
    );
    let frame_path = dir.join(&frame_name);
    let frame = fs::read(&frame_path)?;
    let decoded = decode_frame(&frame, SafetyLimits::default())
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let body_hash = descriptor_hash(&decoded.body);
    if body_hash != staged.descriptor_hash {
        return Err(StoreError::HeapAdmit("staged body hash conflict".into()));
    }
    let desc =
        decode_heap_descriptor(&decoded.body).map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    if desc.heap_id != staged.heap_id || desc.sequence != 1 {
        return Err(StoreError::HeapAdmit("staged descriptor invalid".into()));
    }
    write_chain_frame(
        &layout.heap_chain_dir(&staged.heap_id),
        1,
        &body_hash,
        &frame,
    )?;
    write_head_hint(&layout.heap_head_path(&staged.heap_id), &body_hash)?;
    // Remove staging (published bytes live only under heaps/).
    let _ = fs::remove_dir_all(&dir);
    rebuild_and_persist_all_catalogs(layout)?;
    let receipt = mint_receipt("publish_staged_genesis", staged.heap_id, None, body_hash)?;
    write_admin_receipt(layout, &receipt)?;
    Ok(receipt)
}

fn tip_heap_descriptor(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
) -> Result<(HeapDescriptor, [u8; 32]), StoreError> {
    let entry = rebuild_heap_entry_from_chain(layout, heap_id)?
        .ok_or_else(|| StoreError::HeapAdmit("heap not published".into()))?;
    let frames = list_chain_frames(&layout.heap_chain_dir(heap_id))?;
    let (_, _, path) = frames
        .into_iter()
        .find(|(s, h, _)| *s == entry.sequence && h == &entry.descriptor_hash)
        .ok_or_else(|| StoreError::HeapAdmit("tip frame missing".into()))?;
    let decoded = load_verified_frame(&path)?;
    let desc =
        decode_heap_descriptor(&decoded.body).map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    Ok((desc, entry.descriptor_hash))
}

/// Append a rename descriptor; prior name becomes an alias.
pub fn rename_heap(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    new_name: &str,
) -> Result<AdminReceipt, StoreError> {
    let (prev, prev_hash) = tip_heap_descriptor(layout, heap_id)?;
    let mut aliases: BTreeSet<String> = prev.aliases.into_iter().collect();
    aliases.insert(prev.name.clone());
    aliases.remove(new_name);
    let mut aliases: Vec<String> = aliases.into_iter().collect();
    aliases.sort();
    let next = HeapDescriptor {
        origin_deployment_id: prev.origin_deployment_id,
        heap_id: prev.heap_id,
        creation_event_id: prev.creation_event_id,
        created_at: prev.created_at,
        predecessor_hash: Some(prev_hash),
        sequence: prev.sequence + 1,
        state: prev.state,
        name: new_name.to_string(),
        aliases,
    };
    let (frame, hash) = encode_heap_frame(&next)?;
    write_chain_frame(
        &layout.heap_chain_dir(heap_id),
        next.sequence,
        &hash,
        &frame,
    )?;
    write_head_hint(&layout.heap_head_path(heap_id), &hash)?;
    rebuild_and_persist_all_catalogs(layout)?;
    let receipt = mint_receipt("rename_heap", *heap_id, None, hash)?;
    write_admin_receipt(layout, &receipt)?;
    Ok(receipt)
}

/// Append a retire (state=retired) descriptor.
pub fn retire_heap(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
) -> Result<AdminReceipt, StoreError> {
    let (prev, prev_hash) = tip_heap_descriptor(layout, heap_id)?;
    let next = HeapDescriptor {
        state: HeapDescriptorState::Retired,
        predecessor_hash: Some(prev_hash),
        sequence: prev.sequence + 1,
        ..prev
    };
    let (frame, hash) = encode_heap_frame(&next)?;
    write_chain_frame(
        &layout.heap_chain_dir(heap_id),
        next.sequence,
        &hash,
        &frame,
    )?;
    write_head_hint(&layout.heap_head_path(heap_id), &hash)?;
    rebuild_and_persist_all_catalogs(layout)?;
    let receipt = mint_receipt("retire_heap", *heap_id, None, hash)?;
    write_admin_receipt(layout, &receipt)?;
    Ok(receipt)
}

fn tip_object_descriptor(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    kind: ObjectKind,
    object_id: &[u8; 16],
) -> Result<(ObjectDescriptor, [u8; 32]), StoreError> {
    let entry = rebuild_object_entry_from_chain(layout, heap_id, kind, object_id)?
        .ok_or_else(|| StoreError::HeapAdmit("object not published".into()))?;
    let frames = list_chain_frames(&layout.object_chain_dir(heap_id, kind, object_id))?;
    let (_, _, path) = frames
        .into_iter()
        .find(|(s, h, _)| *s == entry.sequence && h == &entry.descriptor_hash)
        .ok_or_else(|| StoreError::HeapAdmit("object tip missing".into()))?;
    let decoded = load_verified_frame(&path)?;
    let desc = decode_object_descriptor(&decoded.body)
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    Ok((desc, entry.descriptor_hash))
}

fn assert_name_free(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    kind: ObjectKind,
    name: &str,
    except: Option<[u8; 16]>,
) -> Result<(), StoreError> {
    for id in list_object_ids(layout, heap_id, kind)? {
        if except == Some(id) {
            continue;
        }
        if let Some(e) = rebuild_object_entry_from_chain(layout, heap_id, kind, &id)? {
            if e.state == ObjectDescriptorState::Retired {
                continue;
            }
            if e.name == name || e.aliases.iter().any(|a| a == name) {
                return Err(StoreError::HeapAdmit("name/alias conflict".into()));
            }
        }
    }
    Ok(())
}

/// Create a collection or stream with an immutable object id (sequence-1).
pub fn create_object(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    kind: ObjectKind,
    object_id: [u8; 16],
    creation_event_id: [u8; 16],
    name: &str,
) -> Result<AdminReceipt, StoreError> {
    // Heap must be published (catalog cannot invent owners).
    let _ = rebuild_heap_entry_from_chain(layout, heap_id)?
        .ok_or_else(|| StoreError::HeapAdmit("heap not published".into()))?;
    if rebuild_object_entry_from_chain(layout, heap_id, kind, &object_id)?.is_some() {
        return Err(StoreError::HeapAdmit("object id already exists".into()));
    }
    assert_name_free(layout, heap_id, kind, name, None)?;
    let desc = ObjectDescriptor {
        heap_id: *heap_id,
        object_id,
        creation_event_id,
        created_at: now_secs(),
        predecessor_hash: None,
        sequence: 1,
        name: name.to_string(),
        aliases: vec![],
        state: ObjectDescriptorState::Active,
    };
    let (frame, hash) = encode_object_frame(kind, &desc)?;
    write_chain_frame(
        &layout.object_chain_dir(heap_id, kind, &object_id),
        1,
        &hash,
        &frame,
    )?;
    write_head_hint(&layout.object_head_path(heap_id, kind, &object_id), &hash)?;
    rebuild_and_persist_all_catalogs(layout)?;
    let receipt = mint_receipt(
        match kind {
            ObjectKind::Collection => "create_collection",
            ObjectKind::Stream => "create_stream",
        },
        *heap_id,
        Some(object_id),
        hash,
    )?;
    write_admin_receipt(layout, &receipt)?;
    Ok(receipt)
}

/// Rename a collection/stream; prior name becomes an alias. ID is unchanged.
pub fn rename_object(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    kind: ObjectKind,
    object_id: &[u8; 16],
    new_name: &str,
) -> Result<AdminReceipt, StoreError> {
    assert_name_free(layout, heap_id, kind, new_name, Some(*object_id))?;
    let (prev, prev_hash) = tip_object_descriptor(layout, heap_id, kind, object_id)?;
    let mut aliases: BTreeSet<String> = prev.aliases.into_iter().collect();
    aliases.insert(prev.name.clone());
    aliases.remove(new_name);
    let mut aliases: Vec<String> = aliases.into_iter().collect();
    aliases.sort();
    let next = ObjectDescriptor {
        predecessor_hash: Some(prev_hash),
        sequence: prev.sequence + 1,
        name: new_name.to_string(),
        aliases,
        ..prev
    };
    let (frame, hash) = encode_object_frame(kind, &next)?;
    write_chain_frame(
        &layout.object_chain_dir(heap_id, kind, object_id),
        next.sequence,
        &hash,
        &frame,
    )?;
    write_head_hint(&layout.object_head_path(heap_id, kind, object_id), &hash)?;
    rebuild_and_persist_all_catalogs(layout)?;
    let receipt = mint_receipt("rename_object", *heap_id, Some(*object_id), hash)?;
    write_admin_receipt(layout, &receipt)?;
    Ok(receipt)
}

/// Retire a collection/stream (immutable id retained).
pub fn retire_object(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    kind: ObjectKind,
    object_id: &[u8; 16],
) -> Result<AdminReceipt, StoreError> {
    let (prev, prev_hash) = tip_object_descriptor(layout, heap_id, kind, object_id)?;
    let next = ObjectDescriptor {
        state: ObjectDescriptorState::Retired,
        predecessor_hash: Some(prev_hash),
        sequence: prev.sequence + 1,
        ..prev
    };
    let (frame, hash) = encode_object_frame(kind, &next)?;
    write_chain_frame(
        &layout.object_chain_dir(heap_id, kind, object_id),
        next.sequence,
        &hash,
        &frame,
    )?;
    write_head_hint(&layout.object_head_path(heap_id, kind, object_id), &hash)?;
    rebuild_and_persist_all_catalogs(layout)?;
    let receipt = mint_receipt("retire_object", *heap_id, Some(*object_id), hash)?;
    write_admin_receipt(layout, &receipt)?;
    Ok(receipt)
}

/// Delete every rebuildable catalog and tip-head hint (chains retained).
///
/// Also removes any heap-scoped derived index directories under `indexes/`.
pub fn delete_rebuildable_catalogs(layout: &HeapMetaLayout) -> Result<(), StoreError> {
    let heap_cat = layout.heap_catalog_path();
    if heap_cat.exists() {
        fs::remove_file(&heap_cat)?;
    }
    for heap_id in list_published_heap_ids(layout)? {
        for path in [
            layout.collections_catalog_path(&heap_id),
            layout.streams_catalog_path(&heap_id),
            layout.heap_head_path(&heap_id),
        ] {
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
        for kind in [ObjectKind::Collection, ObjectKind::Stream] {
            for oid in list_object_ids(layout, &heap_id, kind)? {
                let head = layout.object_head_path(&heap_id, kind, &oid);
                if head.exists() {
                    fs::remove_file(head)?;
                }
            }
        }
        let index_dir = layout.heap_index_dir(&heap_id);
        if index_dir.is_dir() {
            fs::remove_dir_all(index_dir)?;
        }
    }
    Ok(())
}

/// Result of rebuilding published catalogs from descriptor chains.
pub type RebuiltCatalogs = (
    Vec<HeapCatalogEntry>,
    BTreeMap<[u8; 16], Vec<ObjectCatalogEntry>>,
);

/// Rebuild all rebuildable catalogs from surviving descriptor chains.
pub fn rebuild_and_persist_all_catalogs(
    layout: &HeapMetaLayout,
) -> Result<RebuiltCatalogs, StoreError> {
    fs::create_dir_all(layout.meta_dir())?;
    let mut heaps = Vec::new();
    let mut objects: BTreeMap<[u8; 16], Vec<ObjectCatalogEntry>> = BTreeMap::new();
    for heap_id in list_published_heap_ids(layout)? {
        if let Some(entry) = rebuild_heap_entry_from_chain(layout, &heap_id)? {
            write_head_hint(&layout.heap_head_path(&heap_id), &entry.descriptor_hash)?;
            heaps.push(entry);
        }
        let mut cols = Vec::new();
        let mut streams = Vec::new();
        for oid in list_object_ids(layout, &heap_id, ObjectKind::Collection)? {
            if let Some(e) =
                rebuild_object_entry_from_chain(layout, &heap_id, ObjectKind::Collection, &oid)?
            {
                write_head_hint(
                    &layout.object_head_path(&heap_id, ObjectKind::Collection, &oid),
                    &e.descriptor_hash,
                )?;
                cols.push(e.clone());
            }
        }
        for oid in list_object_ids(layout, &heap_id, ObjectKind::Stream)? {
            if let Some(e) =
                rebuild_object_entry_from_chain(layout, &heap_id, ObjectKind::Stream, &oid)?
            {
                write_head_hint(
                    &layout.object_head_path(&heap_id, ObjectKind::Stream, &oid),
                    &e.descriptor_hash,
                )?;
                streams.push(e.clone());
            }
        }
        cols.sort_by(|a, b| a.object_id.cmp(&b.object_id));
        streams.sort_by(|a, b| a.object_id.cmp(&b.object_id));
        write_atomic(
            &layout.collections_catalog_path(&heap_id),
            &encode_object_catalog(&heap_id, &cols)?,
        )?;
        write_atomic(
            &layout.streams_catalog_path(&heap_id),
            &encode_object_catalog(&heap_id, &streams)?,
        )?;
        let mut all = cols;
        all.extend(streams);
        objects.insert(heap_id, all);
    }
    heaps.sort_by(|a, b| a.heap_id.cmp(&b.heap_id));
    write_atomic(&layout.heap_catalog_path(), &encode_heap_catalog(&heaps)?)?;
    Ok((heaps, objects))
}

/// Load the deployment-wide rebuildable heap catalog (accelerator only).
pub fn try_load_heap_catalog(
    layout: &HeapMetaLayout,
) -> Result<Option<Vec<HeapCatalogEntry>>, StoreError> {
    let path = layout.heap_catalog_path();
    if !path.is_file() {
        return Ok(None);
    }
    Ok(Some(decode_heap_catalog(&fs::read(path)?)?))
}

/// Load per-heap collections catalog accelerator.
pub fn try_load_collections_catalog(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
) -> Result<Option<Vec<ObjectCatalogEntry>>, StoreError> {
    let path = layout.collections_catalog_path(heap_id);
    if !path.is_file() {
        return Ok(None);
    }
    let (hid, entries) = decode_object_catalog(&fs::read(path)?)?;
    if &hid != heap_id {
        return Err(StoreError::HeapAdmit(
            "collections catalog heap mismatch".into(),
        ));
    }
    Ok(Some(entries))
}

/// Load per-heap streams catalog accelerator.
pub fn try_load_streams_catalog(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
) -> Result<Option<Vec<ObjectCatalogEntry>>, StoreError> {
    let path = layout.streams_catalog_path(heap_id);
    if !path.is_file() {
        return Ok(None);
    }
    let (hid, entries) = decode_object_catalog(&fs::read(path)?)?;
    if &hid != heap_id {
        return Err(StoreError::HeapAdmit(
            "streams catalog heap mismatch".into(),
        ));
    }
    Ok(Some(entries))
}

/// Whether any staged genesis directories exist (must never feed published discovery).
pub fn staging_is_non_discoverable(layout: &HeapMetaLayout) -> Result<bool, StoreError> {
    // Published list must ignore staging entirely.
    let published = list_published_heap_ids(layout)?;
    let staging = layout.staging_root();
    if !staging.is_dir() {
        return Ok(true);
    }
    for ent in fs::read_dir(staging)? {
        let ent = ent?;
        if !ent.file_type()?.is_dir() {
            continue;
        }
        let Some(staging_id) = unhex16(&ent.file_name().to_string_lossy()) else {
            continue;
        };
        if let Some(staged) = load_staged_genesis(layout, &staging_id)? {
            if published.contains(&staged.heap_id) {
                // Staging leftover after publish is ok only if chain exists;
                // heap must not appear *only* via staging.
            }
            // Staged-only heaps must not appear in published set.
            if !layout.heap_chain_dir(&staged.heap_id).is_dir()
                && published.contains(&staged.heap_id)
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn staged_genesis_invisible_until_publish() {
        let dir = tempdir().unwrap();
        let layout = HeapMetaLayout::new(dir.path());
        let heap = [0x11u8; 16];
        let staged =
            stage_heap_genesis(&layout, [0x22u8; 16], heap, [0x33u8; 16], "alpha").unwrap();
        assert!(list_published_heap_ids(&layout).unwrap().is_empty());
        assert!(try_load_heap_catalog(&layout).unwrap().is_none());
        assert!(staging_is_non_discoverable(&layout).unwrap());
        publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
        let cat = try_load_heap_catalog(&layout).unwrap().unwrap();
        assert_eq!(cat.len(), 1);
        assert_eq!(cat[0].heap_id, heap);
        assert_eq!(cat[0].name, "alpha");
    }
}
