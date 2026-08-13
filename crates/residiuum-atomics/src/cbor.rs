//! Deterministic CBOR (FORMAT_SPEC §4.4 / §5 condition 6).
//!
//! Kept inside this crate so `residiuum-atomics` does not depend on format,
//! store, heap, or SDK. Length is bounded by remaining bytes before allocation.
//! Nesting is bounded by [`MAX_CBOR_DEPTH`] (ATM-0.4).

use crate::error::{AtomicsError, CborError};

/// Maximum nested map/array depth. Plan maps nest at most three levels.
pub const MAX_CBOR_DEPTH: u8 = 8;

pub(crate) type Map = Vec<(u64, Value)>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Value {
    Uint(u64),
    Nint(u64),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Map(Map),
}

pub(crate) fn encode_map(entries: &[(u64, Value)]) -> Result<Vec<u8>, AtomicsError> {
    let mut sorted = entries.to_vec();
    sorted.sort_by_key(|(k, _)| *k);
    for w in sorted.windows(2) {
        if w[0].0 == w[1].0 {
            return Err(CborError::DuplicateKey.into());
        }
    }
    let mut out = Vec::new();
    write_type_arg(&mut out, 5, sorted.len() as u64);
    for (k, v) in &sorted {
        write_type_arg(&mut out, 0, *k);
        write_value(&mut out, v)?;
    }
    validate_map(&out)?;
    Ok(out)
}

pub(crate) fn decode_map(bytes: &[u8]) -> Result<Map, AtomicsError> {
    validate_map(bytes)?;
    let mut cur = Cursor::new(bytes);
    let n = cur.read_map_len()?;
    let mut out = Vec::new();
    for _ in 0..n {
        let key = cur.read_uint()?;
        let val = cur.read_value(1)?;
        out.push((key, val));
    }
    if !cur.is_empty() {
        return Err(CborError::TrailingBytes.into());
    }
    Ok(out)
}

fn validate_map(bytes: &[u8]) -> Result<(), AtomicsError> {
    let mut cur = Cursor::new(bytes);
    cur.skip_map(0)?;
    if !cur.is_empty() {
        return Err(CborError::TrailingBytes.into());
    }
    Ok(())
}

fn write_value(out: &mut Vec<u8>, v: &Value) -> Result<(), AtomicsError> {
    match v {
        Value::Uint(n) => write_type_arg(out, 0, *n),
        Value::Nint(n) => write_type_arg(out, 1, *n),
        Value::Bytes(b) => {
            write_type_arg(out, 2, b.len() as u64);
            out.extend_from_slice(b);
        }
        Value::Array(items) => {
            write_type_arg(out, 4, items.len() as u64);
            for item in items {
                write_value(out, item)?;
            }
        }
        Value::Map(entries) => {
            let encoded = encode_map(entries)?;
            out.extend_from_slice(&encoded);
        }
    }
    Ok(())
}

fn write_type_arg(out: &mut Vec<u8>, major: u8, arg: u64) {
    let mt = major << 5;
    if arg <= 23 {
        out.push(mt | arg as u8);
    } else if arg <= 0xff {
        out.push(mt | 24);
        out.push(arg as u8);
    } else if arg <= 0xffff {
        out.push(mt | 25);
        out.extend_from_slice(&(arg as u16).to_be_bytes());
    } else if arg <= 0xffff_ffff {
        out.push(mt | 26);
        out.extend_from_slice(&(arg as u32).to_be_bytes());
    } else {
        out.push(mt | 27);
        out.extend_from_slice(&arg.to_be_bytes());
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], AtomicsError> {
        if self.remaining() < n {
            return Err(CborError::Truncated.into());
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_head(&mut self) -> Result<(u8, u64), AtomicsError> {
        let head = self.take(1)?[0];
        let major = head >> 5;
        let ai = head & 0x1f;
        let arg = match ai {
            n @ 0..=23 => n as u64,
            24 => {
                let b = self.take(1)?[0] as u64;
                if b < 24 {
                    return Err(CborError::NonShortest.into());
                }
                b
            }
            25 => {
                let raw = self.take(2)?;
                let v = u16::from_be_bytes([raw[0], raw[1]]) as u64;
                if v < 256 {
                    return Err(CborError::NonShortest.into());
                }
                v
            }
            26 => {
                let raw = self.take(4)?;
                let v = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as u64;
                if v < 65536 {
                    return Err(CborError::NonShortest.into());
                }
                v
            }
            27 => {
                let raw = self.take(8)?;
                let v = u64::from_be_bytes([
                    raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                ]);
                if v < 0x1_0000_0000 {
                    return Err(CborError::NonShortest.into());
                }
                v
            }
            31 => return Err(CborError::Indefinite.into()),
            _ => return Err(CborError::Unsupported.into()),
        };
        Ok((major, arg))
    }

    fn bound_len(&self, arg: u64, min_item: usize) -> Result<usize, AtomicsError> {
        let n = usize::try_from(arg).map_err(|_| CborError::Truncated)?;
        if n > self.remaining() / min_item.max(1) {
            return Err(CborError::Truncated.into());
        }
        Ok(n)
    }

    fn read_map_len(&mut self) -> Result<usize, AtomicsError> {
        let (major, arg) = self.read_head()?;
        if major != 5 {
            return Err(CborError::NotMap.into());
        }
        self.bound_len(arg, 2)
    }

    fn read_uint(&mut self) -> Result<u64, AtomicsError> {
        let (major, arg) = self.read_head()?;
        if major != 0 {
            return Err(CborError::NonUintKey.into());
        }
        Ok(arg)
    }

    fn skip_map(&mut self, depth: u8) -> Result<(), AtomicsError> {
        if depth > MAX_CBOR_DEPTH {
            return Err(CborError::DepthExceeded.into());
        }
        let n = self.read_map_len()?;
        let mut prev: Option<Vec<u8>> = None;
        let mut seen = Vec::new();
        for _ in 0..n {
            let start = self.pos;
            let key = self.read_uint()?;
            let enc = self.bytes[start..self.pos].to_vec();
            if seen.contains(&key) {
                return Err(CborError::DuplicateKey.into());
            }
            seen.push(key);
            if let Some(p) = &prev {
                if enc.as_slice() <= p.as_slice() {
                    return Err(CborError::UnsortedKeys.into());
                }
            }
            prev = Some(enc);
            self.skip_value(depth)?;
        }
        Ok(())
    }

    fn skip_value(&mut self, depth: u8) -> Result<(), AtomicsError> {
        let _ = self.read_value(depth)?;
        Ok(())
    }

    fn read_value(&mut self, depth: u8) -> Result<Value, AtomicsError> {
        if depth > MAX_CBOR_DEPTH {
            return Err(CborError::DepthExceeded.into());
        }
        if self.remaining() == 0 {
            return Err(CborError::Truncated.into());
        }
        let major = self.bytes[self.pos] >> 5;
        match major {
            0 => Ok(Value::Uint(self.read_uint()?)),
            1 => {
                let (_, arg) = self.read_head()?;
                Ok(Value::Nint(arg))
            }
            2 => {
                let (m, arg) = self.read_head()?;
                debug_assert_eq!(m, 2);
                let n = usize::try_from(arg).map_err(|_| CborError::Truncated)?;
                Ok(Value::Bytes(self.take(n)?.to_vec()))
            }
            4 => {
                let (m, arg) = self.read_head()?;
                if m != 4 {
                    return Err(CborError::Unsupported.into());
                }
                let n = self.bound_len(arg, 1)?;
                let mut items = Vec::new();
                for _ in 0..n {
                    items.push(self.read_value(depth + 1)?);
                }
                Ok(Value::Array(items))
            }
            5 => Ok(Value::Map(self.read_canonical_map(depth)?)),
            6 => Err(CborError::TagRejected.into()),
            7 => Err(CborError::Unsupported.into()),
            3 => Err(CborError::Unsupported.into()),
            _ => Err(CborError::Unsupported.into()),
        }
    }

    /// Decode a definite map and refuse duplicate or unsorted integer keys.
    /// Applied at every nesting level, not only the top-level item.
    fn read_canonical_map(&mut self, depth: u8) -> Result<Map, AtomicsError> {
        let n = self.read_map_len()?;
        let mut items = Vec::new();
        let mut prev: Option<Vec<u8>> = None;
        let mut seen = Vec::new();
        for _ in 0..n {
            let start = self.pos;
            let k = self.read_uint()?;
            let enc = self.bytes[start..self.pos].to_vec();
            if seen.contains(&k) {
                return Err(CborError::DuplicateKey.into());
            }
            seen.push(k);
            if let Some(p) = &prev {
                if enc.as_slice() <= p.as_slice() {
                    return Err(CborError::UnsortedKeys.into());
                }
            }
            prev = Some(enc);
            let v = self.read_value(depth + 1)?;
            items.push((k, v));
        }
        Ok(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_duplicate_key_is_refused() {
        // map{1: map{1:0, 1:1}}
        let bytes = [0xa1, 0x01, 0xa2, 0x01, 0x00, 0x01, 0x01];
        let err = decode_map(&bytes).unwrap_err();
        assert!(matches!(err, AtomicsError::Cbor(CborError::DuplicateKey)));
    }

    #[test]
    fn nested_unsorted_keys_are_refused() {
        // map{1: map{2:0, 1:0}}
        let bytes = [0xa1, 0x01, 0xa2, 0x02, 0x00, 0x01, 0x00];
        let err = decode_map(&bytes).unwrap_err();
        assert!(matches!(err, AtomicsError::Cbor(CborError::UnsortedKeys)));
    }
}
