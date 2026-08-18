//! Versioned, checksummed first-stable-boundary record (CR-R2-002).

use crate::error::LaneError;
use residiuum_atomics::{AtomicId, ContentRoot, HeapId};

const MAGIC: &[u8; 7] = b"R2SEAL1";
const VERSION: u8 = 1;
const DOMAIN: &[u8] = b"RESIDIUUM-ATOMIC-LANE-SEAL-V1";
const LEN: usize = 7 + 1 + 16 + 32 + 32 + 32 + 4 + 32;

/// Authenticated member-stable boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealRecord {
    pub heap_id: HeapId,
    pub atomic_id: AtomicId,
    pub content_root: ContentRoot,
    pub manifest_root: [u8; 32],
    pub member_count: u32,
}

pub fn encode_seal(record: &SealRecord) -> Vec<u8> {
    let mut body = Vec::with_capacity(LEN);
    body.extend_from_slice(MAGIC);
    body.push(VERSION);
    body.extend_from_slice(record.heap_id.as_bytes());
    body.extend_from_slice(record.atomic_id.as_bytes());
    body.extend_from_slice(record.content_root.as_bytes());
    body.extend_from_slice(&record.manifest_root);
    body.extend_from_slice(&record.member_count.to_be_bytes());
    let digest = checksum(&body);
    body.extend_from_slice(&digest);
    body
}

pub fn decode_seal(bytes: &[u8]) -> Result<SealRecord, LaneError> {
    if bytes.len() != LEN {
        return Err(LaneError::Corrupt("seal length"));
    }
    if &bytes[0..7] != MAGIC {
        return Err(LaneError::Corrupt("seal magic"));
    }
    if bytes[7] != VERSION {
        return Err(LaneError::Corrupt("seal version"));
    }
    let expect = checksum(&bytes[..LEN - 32]);
    if bytes[LEN - 32..] != expect {
        return Err(LaneError::Corrupt("seal checksum"));
    }
    let heap_id = HeapId::from_bytes(bytes[8..24].try_into().unwrap())?;
    let atomic_id = AtomicId::from_bytes(bytes[24..56].try_into().unwrap())?;
    let content_root = ContentRoot::from_bytes(bytes[56..88].try_into().unwrap())?;
    let manifest_root: [u8; 32] = bytes[88..120].try_into().unwrap();
    let member_count = u32::from_be_bytes(bytes[120..124].try_into().unwrap());
    Ok(SealRecord {
        heap_id,
        atomic_id,
        content_root,
        manifest_root,
        member_count,
    })
}

fn checksum(prefix: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    hasher.update(prefix);
    *hasher.finalize().as_bytes()
}
