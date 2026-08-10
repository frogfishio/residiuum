//! Chunked payload write/read (FORMAT_SPEC §8, OVERVIEW §5.3).
//!
//! Large values may be stored as independently verifiable payload-chunk frames
//! plus a small item-event manifest. Reassembly never silently fills missing
//! extents; callers receive partial maps with explicit completeness.

use crate::error::StoreError;
use residiuum_format::{
    body_hash, decode_chunk_body, encode_chunk_body, reassemble_chunks, ChunkPiece, LogicalExtent,
    ReassemblyState, BODY_HASH_LEN,
};

/// Magic identifying a draft chunked-payload manifest in an item-event body.
pub const CHUNK_MANIFEST_MAGIC: &[u8; 8] = b"RCHM0001";

/// Default soft threshold: bodies larger than this MAY be written chunked.
///
/// Stage 6 tests may override via [`crate::Store::set_chunk_threshold`].
pub const DEFAULT_CHUNK_THRESHOLD: usize = 64 * 1024;

/// Default max payload bytes per chunk frame (exclusive of chunk header).
pub const DEFAULT_CHUNK_SIZE: usize = 16 * 1024;

/// Draft chunk manifest body layout (item-event body when chunked):
///
/// ```text
/// magic[8] = b"RCHM0001"
/// content_hash[32]
/// total_chunks:u32 LE
/// logical_total_len:u64 LE
/// for i in 0..total_chunks:
///   chunk_event_id[16]
///   logical_len:u64 LE
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkManifest {
    /// BLAKE3-256 of the full logical payload.
    pub content_hash: [u8; BODY_HASH_LEN],
    /// Ordered chunk descriptors.
    pub chunks: Vec<ChunkSlot>,
    /// Total logical payload length.
    pub logical_total_len: u64,
}

/// One slot in a chunk manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSlot {
    /// Event id of the payload-chunk frame (also used as chunk identity).
    pub chunk_event_id: [u8; 16],
    /// Logical length of this piece.
    pub logical_len: u64,
}

/// Result of reading a (possibly chunked) payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadResult {
    /// Full logical body available and verified.
    Complete {
        /// Concatenated payload bytes.
        body: Vec<u8>,
    },
    /// Some but not all chunks available.
    Partial {
        /// Ordered extent map (every index appears once).
        extents: Vec<LogicalExtent>,
        /// Missing chunk indices.
        missing: Vec<u32>,
        /// Declared content hash from the manifest (when known).
        content_hash: [u8; BODY_HASH_LEN],
        /// Surviving chunk bodies keyed by index (only present slots).
        present_bodies: Vec<(u32, Vec<u8>)>,
    },
    /// Manifest present but no chunk bodies found.
    Unavailable {
        /// Declared content hash from the manifest.
        content_hash: [u8; BODY_HASH_LEN],
        /// Expected chunk count.
        total_chunks: u32,
    },
    /// Conflicting chunk content at a manifest position.
    Conflicting {
        /// Index in conflict.
        index: u32,
    },
}

impl PayloadResult {
    /// Complete body when fully available.
    pub fn complete_body(&self) -> Option<&[u8]> {
        match self {
            Self::Complete { body } => Some(body.as_slice()),
            _ => None,
        }
    }

    /// Whether the payload is fully available.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete { .. })
    }

    /// Completeness label for diagnostics.
    pub fn completeness(&self) -> &'static str {
        match self {
            Self::Complete { .. } => "complete",
            Self::Partial { .. } => "partial",
            Self::Unavailable { .. } => "unavailable",
            Self::Conflicting { .. } => "conflicting",
        }
    }
}

/// Whether `body` is a draft chunk manifest (not the logical payload).
pub fn is_chunk_manifest(body: &[u8]) -> bool {
    body.len() >= 8 + BODY_HASH_LEN + 4 + 8 && body.starts_with(CHUNK_MANIFEST_MAGIC)
}

/// Encode a chunk manifest.
pub fn encode_chunk_manifest(manifest: &ChunkManifest) -> Vec<u8> {
    let n = manifest.chunks.len();
    let mut out = Vec::with_capacity(8 + BODY_HASH_LEN + 4 + 8 + n * (16 + 8));
    out.extend_from_slice(CHUNK_MANIFEST_MAGIC);
    out.extend_from_slice(&manifest.content_hash);
    out.extend_from_slice(&(n as u32).to_le_bytes());
    out.extend_from_slice(&manifest.logical_total_len.to_le_bytes());
    for slot in &manifest.chunks {
        out.extend_from_slice(&slot.chunk_event_id);
        out.extend_from_slice(&slot.logical_len.to_le_bytes());
    }
    out
}

/// Decode a chunk manifest body.
///
/// Hostile `chunk_count` values must not allocate before length checks
/// (DEF-091-F). Capacity is bounded by remaining body bytes / slot size.
pub fn decode_chunk_manifest(body: &[u8]) -> Option<ChunkManifest> {
    if !is_chunk_manifest(body) {
        return None;
    }
    let content_hash: [u8; BODY_HASH_LEN] = body[8..8 + BODY_HASH_LEN].try_into().ok()?;
    let total = u32::from_le_bytes(
        body[8 + BODY_HASH_LEN..8 + BODY_HASH_LEN + 4]
            .try_into()
            .ok()?,
    ) as usize;
    let logical_total_len = u64::from_le_bytes(
        body[8 + BODY_HASH_LEN + 4..8 + BODY_HASH_LEN + 4 + 8]
            .try_into()
            .ok()?,
    );
    let mut cursor = 8 + BODY_HASH_LEN + 4 + 8;
    // Each slot is 16-byte event_id + 8-byte len. Exact remaining body must match
    // declared total; never allocate from a hostile `chunk_count` (DEF-091-F).
    const SLOT: usize = 16 + 8;
    let remaining = body.len().saturating_sub(cursor);
    let need = total.checked_mul(SLOT)?;
    if need != remaining {
        return None;
    }
    let mut chunks = Vec::with_capacity(total);
    for _ in 0..total {
        if cursor + SLOT > body.len() {
            return None;
        }
        let chunk_event_id: [u8; 16] = body[cursor..cursor + 16].try_into().ok()?;
        cursor += 16;
        let logical_len = u64::from_le_bytes(body[cursor..cursor + 8].try_into().ok()?);
        cursor += 8;
        chunks.push(ChunkSlot {
            chunk_event_id,
            logical_len,
        });
    }
    if cursor != body.len() {
        return None;
    }
    Some(ChunkManifest {
        content_hash,
        chunks,
        logical_total_len,
    })
}

/// Split a logical payload into chunk pieces for writing.
///
/// Each piece uses the shared `item_id` (parent item lineage) and sequential
/// indices. Callers mint `chunk_event_id`s when encoding frames.
pub fn split_into_pieces(
    item_id: [u8; 16],
    payload: &[u8],
    chunk_size: usize,
) -> Result<Vec<ChunkPiece>, StoreError> {
    if chunk_size == 0 {
        return Err(StoreError::CorruptMeta("chunk_size must be non-zero"));
    }
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    let total = payload.len().div_ceil(chunk_size);
    if total > u32::MAX as usize {
        return Err(StoreError::PayloadTooLarge);
    }
    let total_u = total as u32;
    let mut pieces = Vec::with_capacity(total);
    for (i, chunk) in payload.chunks(chunk_size).enumerate() {
        pieces.push(ChunkPiece {
            item_id,
            index: i as u32,
            total: total_u,
            logical_len: chunk.len() as u64,
            body: chunk.to_vec(),
        });
    }
    Ok(pieces)
}

/// Build a manifest from pieces and their assigned chunk event ids.
pub fn manifest_from_pieces(
    pieces: &[ChunkPiece],
    chunk_event_ids: &[[u8; 16]],
    full_payload: &[u8],
) -> Result<ChunkManifest, StoreError> {
    if pieces.len() != chunk_event_ids.len() {
        return Err(StoreError::CorruptMeta("chunk id count mismatch"));
    }
    let content_hash = body_hash(full_payload);
    let chunks = pieces
        .iter()
        .zip(chunk_event_ids.iter())
        .map(|(p, id)| ChunkSlot {
            chunk_event_id: *id,
            logical_len: p.logical_len,
        })
        .collect();
    Ok(ChunkManifest {
        content_hash,
        chunks,
        logical_total_len: full_payload.len() as u64,
    })
}

/// Encode a chunk piece as a payload-chunk frame body.
pub fn encode_piece_body(piece: &ChunkPiece) -> Vec<u8> {
    encode_chunk_body(piece)
}

/// Decode a verified payload-chunk frame body into a piece (if layout matches).
pub fn decode_piece_body(body: &[u8]) -> Option<ChunkPiece> {
    decode_chunk_body(body)
}

/// One verified payload-chunk frame selected for reassembly (DEF-098).
///
/// The reassembler validates `frame_event_id` against the current manifest
/// slot; unrelated same-`item_id` chunks from other generations are ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedChunk {
    /// Frame event id (must equal the manifest slot's `chunk_event_id`).
    pub frame_event_id: [u8; 16],
    /// Segment that holds the frame (diagnostic / locator).
    pub segment_id: [u8; 16],
    /// Byte offset of the frame in `segment_id` (0 when unknown).
    pub frame_offset: u64,
    /// Decoded chunk piece from the verified frame body.
    pub piece: ChunkPiece,
    /// BLAKE3-256 of the chunk payload body (not the full frame body layout).
    pub verified_body_hash: [u8; BODY_HASH_LEN],
}

/// Build a [`ResolvedChunk`] from a piece and its assigned frame event id.
pub fn resolve_piece(
    frame_event_id: [u8; 16],
    piece: ChunkPiece,
    segment_id: [u8; 16],
    frame_offset: u64,
) -> ResolvedChunk {
    let verified_body_hash = body_hash(&piece.body);
    ResolvedChunk {
        frame_event_id,
        segment_id,
        frame_offset,
        piece,
        verified_body_hash,
    }
}

/// Reassemble using the **current** manifest and generation-exact resolved chunks.
///
/// DEF-098: only frames whose `frame_event_id` appears in `manifest.chunks` are
/// considered. Same-index pieces from other generations are ignored (not mixed).
/// Slot metadata (`item_id`, index, total, logical_len) is enforced here.
pub fn reassemble_with_manifest(
    expected_item_id: [u8; 16],
    manifest: &ChunkManifest,
    resolved: &[ResolvedChunk],
) -> PayloadResult {
    let total = manifest.chunks.len() as u32;
    if total == 0 {
        return PayloadResult::Unavailable {
            content_hash: manifest.content_hash,
            total_chunks: 0,
        };
    }

    // Manifest membership map: event_id → (slot index, logical_len).
    // Duplicate event IDs in one manifest are conflicting evidence.
    let mut expected: std::collections::BTreeMap<[u8; 16], (u32, u64)> =
        std::collections::BTreeMap::new();
    for (i, slot) in manifest.chunks.iter().enumerate() {
        let idx = i as u32;
        if expected
            .insert(slot.chunk_event_id, (idx, slot.logical_len))
            .is_some()
        {
            return PayloadResult::Conflicting { index: idx };
        }
    }

    // Group candidates by expected event id; ignore foreign event ids.
    let mut by_event: std::collections::BTreeMap<[u8; 16], Vec<&ResolvedChunk>> =
        std::collections::BTreeMap::new();
    for r in resolved {
        if expected.contains_key(&r.frame_event_id) {
            by_event.entry(r.frame_event_id).or_default().push(r);
        }
    }

    let mut selected: Vec<ChunkPiece> = Vec::with_capacity(total as usize);
    let mut missing: Vec<u32> = Vec::new();

    for (i, slot) in manifest.chunks.iter().enumerate() {
        let idx = i as u32;
        let Some(group) = by_event.get(&slot.chunk_event_id) else {
            missing.push(idx);
            continue;
        };

        for r in group {
            // Exact slot membership (DEF-098 normative invariants).
            if r.frame_event_id != slot.chunk_event_id
                || r.piece.item_id != expected_item_id
                || r.piece.index != idx
                || r.piece.total != total
                || r.piece.logical_len != slot.logical_len
                || r.piece.logical_len != r.piece.body.len() as u64
            {
                return PayloadResult::Conflicting { index: idx };
            }
        }

        let first_hash = group[0].verified_body_hash;
        if group
            .iter()
            .any(|r| r.verified_body_hash != first_hash || r.piece.body != group[0].piece.body)
        {
            return PayloadResult::Conflicting { index: idx };
        }
        selected.push(group[0].piece.clone());
    }

    if selected.is_empty() {
        return PayloadResult::Unavailable {
            content_hash: manifest.content_hash,
            total_chunks: total,
        };
    }

    match reassemble_chunks(&selected, Some(manifest.content_hash)) {
        ReassemblyState::Complete { body, .. } => PayloadResult::Complete { body },
        ReassemblyState::Partial { extents, missing } => {
            let present_bodies: Vec<_> =
                selected.iter().map(|p| (p.index, p.body.clone())).collect();
            PayloadResult::Partial {
                extents,
                missing,
                content_hash: manifest.content_hash,
                present_bodies,
            }
        }
        ReassemblyState::Unavailable => PayloadResult::Unavailable {
            content_hash: manifest.content_hash,
            total_chunks: total,
        },
        ReassemblyState::Conflicting { index, .. } => PayloadResult::Conflicting { index },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved_for(pieces: &[ChunkPiece], ids: &[[u8; 16]]) -> Vec<ResolvedChunk> {
        pieces
            .iter()
            .zip(ids.iter())
            .map(|(p, id)| resolve_piece(*id, p.clone(), [0u8; 16], 0))
            .collect()
    }

    #[test]
    fn manifest_roundtrip() {
        let payload = b"hello world, this is a longer payload for chunking tests!!";
        let item = [7u8; 16];
        let pieces = split_into_pieces(item, payload, 8).unwrap();
        let ids: Vec<[u8; 16]> = (0..pieces.len())
            .map(|i| {
                let mut id = [0u8; 16];
                id[0] = i as u8;
                id
            })
            .collect();
        let man = manifest_from_pieces(&pieces, &ids, payload).unwrap();
        let enc = encode_chunk_manifest(&man);
        let dec = decode_chunk_manifest(&enc).unwrap();
        assert_eq!(dec, man);
        let resolved = resolved_for(&pieces, &ids);
        let result = reassemble_with_manifest(item, &man, &resolved);
        assert_eq!(result.complete_body(), Some(payload.as_slice()));
    }

    /// DEF-091-F: hostile chunk_count must not allocate multi-GiB before length checks.
    #[test]
    fn hostile_chunk_count_does_not_allocate() {
        // Magic + hash + total=u32::MAX + logical_len, no slots.
        let mut body = Vec::new();
        body.extend_from_slice(b"RCHM0001");
        body.extend_from_slice(&[0u8; BODY_HASH_LEN]);
        body.extend_from_slice(&u32::MAX.to_le_bytes());
        body.extend_from_slice(&0u64.to_le_bytes());
        // Must return None without OOM (previously with_capacity(u32::MAX)).
        assert!(decode_chunk_manifest(&body).is_none());
    }

    #[test]
    fn partial_missing_middle() {
        let payload = b"ABCDEFGHI";
        let item = [1u8; 16];
        let pieces = split_into_pieces(item, payload, 3).unwrap();
        assert_eq!(pieces.len(), 3);
        let ids = [[0u8; 16], [1u8; 16], [2u8; 16]];
        let man = manifest_from_pieces(&pieces, &ids, payload).unwrap();
        // Drop middle chunk.
        let surviving = vec![pieces[0].clone(), pieces[2].clone()];
        let surviving_ids = [ids[0], ids[2]];
        let resolved = resolved_for(&surviving, &surviving_ids);
        match reassemble_with_manifest(item, &man, &resolved) {
            PayloadResult::Partial {
                missing, extents, ..
            } => {
                assert_eq!(missing, vec![1]);
                assert_eq!(extents.len(), 3);
                assert!(extents[0].present);
                assert!(!extents[1].present);
                assert!(extents[2].present);
            }
            other => panic!("expected partial: {other:?}"),
        }
    }

    #[test]
    fn ignores_other_generation_same_index() {
        // Current generation B complete; older generation A at same indexes must not conflict.
        let item = [9u8; 16];
        let payload_a = b"AAAAAAAA";
        let payload_b = b"BBBBBBBB";
        let pieces_a = split_into_pieces(item, payload_a, 4).unwrap();
        let pieces_b = split_into_pieces(item, payload_b, 4).unwrap();
        let ids_a = [[10u8; 16], [11u8; 16]];
        let ids_b = [[20u8; 16], [21u8; 16]];
        let man_b = manifest_from_pieces(&pieces_b, &ids_b, payload_b).unwrap();

        let mut mixed = resolved_for(&pieces_b, &ids_b);
        // Inject older generation pieces claiming the same indexes.
        mixed.extend(resolved_for(&pieces_a, &ids_a));

        let result = reassemble_with_manifest(item, &man_b, &mixed);
        assert_eq!(result.complete_body(), Some(payload_b.as_slice()));
    }

    #[test]
    fn wrong_event_id_for_slot_is_ignored_not_mixed() {
        let item = [3u8; 16];
        let payload = b"12345678";
        let pieces = split_into_pieces(item, payload, 4).unwrap();
        let ids = [[1u8; 16], [2u8; 16]];
        let man = manifest_from_pieces(&pieces, &ids, payload).unwrap();
        // Present piece 0 under wrong event id → treated as missing slot 0.
        let resolved = vec![
            resolve_piece([99u8; 16], pieces[0].clone(), [0u8; 16], 0),
            resolve_piece(ids[1], pieces[1].clone(), [0u8; 16], 0),
        ];
        match reassemble_with_manifest(item, &man, &resolved) {
            PayloadResult::Partial { missing, .. } => assert_eq!(missing, vec![0]),
            other => panic!("expected partial missing 0: {other:?}"),
        }
    }
}
