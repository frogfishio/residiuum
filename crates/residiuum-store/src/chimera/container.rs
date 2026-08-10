//! Point micro-page containers — independently readable record slots.
//!
//! Layout (FINAL DESIGN §2):
//!
//! ```text
//! magic[8] = b"RMPC0001"
//! version:u32 LE
//! generation:u32 LE
//! record_count:u32 LE
//! dict_len:u32 LE
//! dictionary[dict_len]
//! for i in 0..record_count:
//!   offset:u32 LE   // relative to values region start
//!   len:u32 LE
//!   codec:u8        // 0 = raw (foundation); future dictionary/zstd
//!   reserved:u8×3
//!   checksum:u32 LE // CRC32C of stored value bytes
//! values region (concatenated record payloads)
//! ```
//!
//! A point read seeks to one slot's aligned region; it never needs to decompress
//! sibling records. Containers are **immutable once sealed**.

use crate::error::StoreError;
use std::io;

/// Magic for micro-page point containers.
pub const CONTAINER_MAGIC: &[u8; 8] = b"RMPC0001";
/// Codec version.
pub const CONTAINER_VERSION: u32 = 1;
/// Value stored raw (no compression).
pub const CODEC_RAW: u8 = 0;

/// Default target sealed container size band (64–256 KiB design range).
pub const DEFAULT_CONTAINER_TARGET: usize = 128 * 1024;

/// One sealed point-optimized micro-page container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointContainer {
    /// Relocation generation (epoch-safe locator updates).
    pub generation: u32,
    /// Optional shared dictionary bytes (empty until dictionary training lands).
    pub dictionary: Vec<u8>,
    /// Sealed slots in slot-index order.
    pub slots: Vec<ContainerSlot>,
}

/// One independently addressable record inside a container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerSlot {
    /// Stored payload bytes (codec-applied form; raw for foundation).
    pub value: Vec<u8>,
    /// Compression / encoding kind (`CODEC_RAW` today).
    pub codec: u8,
}

impl PointContainer {
    /// Build a sealed container from uncompressed logical values.
    pub fn seal(generation: u32, dictionary: Vec<u8>, values: &[Vec<u8>]) -> Self {
        let slots = values
            .iter()
            .map(|v| ContainerSlot {
                value: v.clone(),
                codec: CODEC_RAW,
            })
            .collect();
        Self {
            generation,
            dictionary,
            slots,
        }
    }

    /// Number of slots.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Logical (decompressed) value for `slot`, or `None` if out of range / unsupported codec.
    pub fn get(&self, slot: u32) -> Option<Vec<u8>> {
        let s = self.slots.get(slot as usize)?;
        match s.codec {
            CODEC_RAW => Some(s.value.clone()),
            _ => None,
        }
    }

    /// Encode to on-disk bytes.
    pub fn encode(&self) -> Vec<u8> {
        encode_container(self)
    }

    /// Decode from on-disk bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        decode_container(bytes)
    }
}

/// Builder that packs values until a target size, then seals.
#[derive(Debug, Clone)]
pub struct ContainerBuilder {
    generation: u32,
    dictionary: Vec<u8>,
    values: Vec<Vec<u8>>,
    /// Approximate encoded size so far (slots + values).
    approx_bytes: usize,
    target_bytes: usize,
}

impl ContainerBuilder {
    /// Start a builder for one generation with optional dictionary.
    pub fn new(generation: u32, dictionary: Vec<u8>, target_bytes: usize) -> Self {
        let base = 8 + 4 + 4 + 4 + 4 + dictionary.len();
        Self {
            generation,
            dictionary,
            values: Vec::new(),
            approx_bytes: base,
            target_bytes: target_bytes.max(base + 64),
        }
    }

    /// Try to push a value. Returns `false` when the container is full (caller should seal).
    ///
    /// Always accepts the first value even if it alone exceeds the target.
    pub fn try_push(&mut self, value: Vec<u8>) -> bool {
        let add = 16 + value.len(); // slot header + bytes
        if !self.values.is_empty() && self.approx_bytes + add > self.target_bytes {
            return false;
        }
        self.approx_bytes += add;
        self.values.push(value);
        true
    }

    /// Current slot count.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether no values were pushed.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Seal into an immutable container (consumes builder contents).
    pub fn seal(self) -> PointContainer {
        PointContainer::seal(self.generation, self.dictionary, &self.values)
    }
}

fn crc32c(bytes: &[u8]) -> u32 {
    // Lightweight CRC32C (Castagnoli) for per-record integrity.
    // Prefer consistency with residiuum-format if it exposes one later.
    crc32c_slice(bytes)
}

fn crc32c_slice(mut data: &[u8]) -> u32 {
    let mut crc: u32 = 0xffff_ffff;
    while !data.is_empty() {
        crc ^= u32::from(data[0]);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
        data = &data[1..];
    }
    !crc
}

fn encode_container(c: &PointContainer) -> Vec<u8> {
    let n = c.slots.len() as u32;
    let dict_len = c.dictionary.len() as u32;
    let mut values = Vec::new();
    let mut table = Vec::with_capacity(c.slots.len() * 16);
    for s in &c.slots {
        let offset = values.len() as u32;
        let len = s.value.len() as u32;
        let sum = crc32c(&s.value);
        table.extend_from_slice(&offset.to_le_bytes());
        table.extend_from_slice(&len.to_le_bytes());
        table.push(s.codec);
        table.extend_from_slice(&[0u8; 3]);
        table.extend_from_slice(&sum.to_le_bytes());
        values.extend_from_slice(&s.value);
    }

    let mut out = Vec::with_capacity(24 + c.dictionary.len() + table.len() + values.len());
    out.extend_from_slice(CONTAINER_MAGIC);
    out.extend_from_slice(&CONTAINER_VERSION.to_le_bytes());
    out.extend_from_slice(&c.generation.to_le_bytes());
    out.extend_from_slice(&n.to_le_bytes());
    out.extend_from_slice(&dict_len.to_le_bytes());
    out.extend_from_slice(&c.dictionary);
    out.extend_from_slice(&table);
    out.extend_from_slice(&values);
    out
}

fn decode_container(bytes: &[u8]) -> Result<PointContainer, StoreError> {
    if bytes.len() < 24 {
        return Err(bad("container too short"));
    }
    if &bytes[0..8] != CONTAINER_MAGIC.as_slice() {
        return Err(bad("container magic mismatch"));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if version != CONTAINER_VERSION {
        return Err(bad("container version unsupported"));
    }
    let generation = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let record_count = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let dict_len = u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize;
    let mut off = 24;
    if bytes.len() < off + dict_len {
        return Err(bad("container dictionary truncated"));
    }
    let dictionary = bytes[off..off + dict_len].to_vec();
    off += dict_len;

    let table_bytes = record_count
        .checked_mul(16)
        .ok_or_else(|| bad("table overflow"))?;
    if bytes.len() < off + table_bytes {
        return Err(bad("container slot table truncated"));
    }
    let table = &bytes[off..off + table_bytes];
    off += table_bytes;
    let values_region = &bytes[off..];

    let mut slots = Vec::with_capacity(record_count);
    for i in 0..record_count {
        let base = i * 16;
        let v_off = u32::from_le_bytes(table[base..base + 4].try_into().unwrap()) as usize;
        let v_len = u32::from_le_bytes(table[base + 4..base + 8].try_into().unwrap()) as usize;
        let codec = table[base + 8];
        // base+9..12 reserved
        let expect = u32::from_le_bytes(table[base + 12..base + 16].try_into().unwrap());
        let end = v_off
            .checked_add(v_len)
            .ok_or_else(|| bad("slot range overflow"))?;
        if end > values_region.len() {
            return Err(bad("slot points past values region"));
        }
        let value = values_region[v_off..end].to_vec();
        if crc32c(&value) != expect {
            return Err(bad("slot checksum mismatch"));
        }
        slots.push(ContainerSlot { value, codec });
    }

    Ok(PointContainer {
        generation,
        dictionary,
        slots,
    })
}

fn bad(msg: &'static str) -> StoreError {
    StoreError::Io(io::Error::new(io::ErrorKind::InvalidData, msg))
}

/// Fetch one slot from encoded container bytes without fully cloning all values
/// into intermediate structures beyond the decode (foundation: full decode).
///
/// Future: true partial read from an mmap / pread of the slot region only.
pub fn read_slot(bytes: &[u8], slot: u32) -> Result<Option<Vec<u8>>, StoreError> {
    let c = PointContainer::decode(bytes)?;
    Ok(c.get(slot))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_multiple_slots() {
        let values = vec![b"alpha".to_vec(), b"bravo".to_vec(), vec![0u8; 100]];
        let c = PointContainer::seal(7, b"dict".to_vec(), &values);
        let bytes = c.encode();
        let d = PointContainer::decode(&bytes).unwrap();
        assert_eq!(d.generation, 7);
        assert_eq!(d.dictionary, b"dict");
        assert_eq!(d.len(), 3);
        assert_eq!(d.get(0).unwrap(), b"alpha");
        assert_eq!(d.get(1).unwrap(), b"bravo");
        assert_eq!(d.get(2).unwrap(), vec![0u8; 100]);
        assert!(d.get(3).is_none());
    }

    #[test]
    fn builder_respects_target() {
        let mut b = ContainerBuilder::new(1, Vec::new(), 200);
        assert!(b.try_push(vec![1u8; 50]));
        assert!(b.try_push(vec![2u8; 50]));
        // further push may fail depending on approx size
        let accepted = b.try_push(vec![3u8; 200]);
        if !accepted {
            assert_eq!(b.len(), 2);
        }
        let sealed = b.seal();
        assert!(!sealed.is_empty());
    }

    #[test]
    fn corrupt_checksum_rejected() {
        let c = PointContainer::seal(1, Vec::new(), &[b"x".to_vec()]);
        let mut bytes = c.encode();
        // Flip a byte in the values region (last byte).
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(PointContainer::decode(&bytes).is_err());
    }
}
