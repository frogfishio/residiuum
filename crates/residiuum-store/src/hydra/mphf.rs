//! Minimal perfect hash + fingerprint for point-only immutable key sets.
//!
//! Construction uses a CHD-style "hash and displace" scheme:
//! 1. Group keys into buckets by `h0`.
//! 2. Process large buckets first; find a displacement `d` such that
//!    `h1(key, d)` lands in free table slots for every key in the bucket.
//! 3. Store per-bucket displacements and a dense table of fingerprints + offsets.
//!
//! Lookups: `slot = h1(key, disp[h0(key)])`; accept iff fingerprint matches.

/// Point-only MPHF index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MphfIndex {
    /// Per-bucket displacement (or u32::MAX if empty bucket).
    displacements: Vec<u32>,
    /// Table slots: fingerprint (0 = empty sentinel avoided via occupied bit).
    fingerprints: Vec<u64>,
    /// Parallel offsets (only meaningful when fingerprint != 0 or occupied).
    offsets: Vec<u64>,
    /// Occupied bitmap packed as bytes (1 bit per slot).
    occupied: Vec<u8>,
    n_keys: u32,
    n_buckets: u32,
    table_size: u32,
    /// Salt mixed into hashes (segment-local).
    salt: u64,
}

const EMPTY_DISP: u32 = u32::MAX;

impl MphfIndex {
    /// Build from unique keys (order irrelevant; sorted is fine).
    pub fn build(sorted: &[(Vec<u8>, u64)]) -> Self {
        let n = sorted.len();
        if n == 0 {
            return Self {
                displacements: Vec::new(),
                fingerprints: Vec::new(),
                offsets: Vec::new(),
                occupied: Vec::new(),
                n_keys: 0,
                n_buckets: 0,
                table_size: 0,
                salt: 0xD160_D160_D160_D160,
            };
        }
        let salt = 0xD160_D160_D160_D160 ^ (n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        // λ ≈ 4 keys/bucket; table load ≈ 0.9 → size ≈ n / 0.9 next power of two-ish.
        let n_buckets = n.div_ceil(4).max(1);
        let table_size = next_pow2(((n as f64 / 0.85).ceil() as usize).max(n + 1).max(8));

        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); n_buckets];
        for (i, (k, _)) in sorted.iter().enumerate() {
            let b = (h0(k, salt) as usize) % n_buckets;
            buckets[b].push(i);
        }
        // Process largest buckets first.
        let mut order: Vec<usize> = (0..n_buckets).collect();
        order.sort_by_key(|&b| std::cmp::Reverse(buckets[b].len()));

        let mut displacements = vec![EMPTY_DISP; n_buckets];
        let mut fingerprints = vec![0u64; table_size];
        let mut offsets = vec![0u64; table_size];
        let mut occupied = vec![0u8; table_size.div_ceil(8)];

        for b in order {
            if buckets[b].is_empty() {
                continue;
            }
            let mut found = None;
            // Try displacements; for large buckets this is still cheap.
            'disp: for d in 0..u32::MAX {
                let mut slots = Vec::with_capacity(buckets[b].len());
                let mut ok = true;
                for &ki in &buckets[b] {
                    let slot = (h1(&sorted[ki].0, salt, d) as usize) % table_size;
                    if get_bit(&occupied, slot) || slots.contains(&slot) {
                        ok = false;
                        break;
                    }
                    slots.push(slot);
                }
                if ok {
                    found = Some((d, slots));
                    break 'disp;
                }
                // Safety valve: extremely pathological — fall through with linear probe table.
                if d > 10_000 && buckets[b].len() == 1 {
                    // Single-key bucket: scan for any free slot via d.
                    continue;
                }
                if d > 100_000 {
                    // Should be vanishingly rare with table_size load 0.85.
                    panic!("mphf: failed to place bucket size {}", buckets[b].len());
                }
            }
            let (d, slots) = found.expect("mphf displacement");
            displacements[b] = d;
            for (j, &ki) in buckets[b].iter().enumerate() {
                let slot = slots[j];
                set_bit(&mut occupied, slot);
                fingerprints[slot] = fingerprint(&sorted[ki].0, salt);
                // Avoid 0 fingerprint collision with empty by OR 1 when zero.
                if fingerprints[slot] == 0 {
                    fingerprints[slot] = 1;
                }
                offsets[slot] = sorted[ki].1;
            }
        }

        Self {
            displacements,
            fingerprints,
            offsets,
            occupied,
            n_keys: n as u32,
            n_buckets: n_buckets as u32,
            table_size: table_size as u32,
            salt,
        }
    }

    pub fn len(&self) -> usize {
        self.n_keys as usize
    }

    pub fn get(&self, key: &[u8]) -> Option<u64> {
        if self.n_keys == 0 {
            return None;
        }
        let b = (h0(key, self.salt) as usize) % self.n_buckets as usize;
        let d = self.displacements[b];
        if d == EMPTY_DISP {
            return None;
        }
        let slot = (h1(key, self.salt, d) as usize) % self.table_size as usize;
        if !get_bit(&self.occupied, slot) {
            return None;
        }
        let mut fp = fingerprint(key, self.salt);
        if fp == 0 {
            fp = 1;
        }
        if self.fingerprints[slot] != fp {
            return None;
        }
        Some(self.offsets[slot])
    }
}

fn h0(key: &[u8], salt: u64) -> u64 {
    mix(hash_bytes(key, salt ^ 0xA5A5_A5A5_A5A5_A5A5))
}

fn h1(key: &[u8], salt: u64, d: u32) -> u64 {
    mix(hash_bytes(
        key,
        salt ^ (d as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
    ))
}

fn fingerprint(key: &[u8], salt: u64) -> u64 {
    mix(hash_bytes(key, salt ^ 0xC0FF_EE00_D15C_0001))
}

fn hash_bytes(key: &[u8], seed: u64) -> u64 {
    // Fast non-cryptographic 64-bit mix (FNV-ish + finalizer). Good enough for MPHF.
    let mut h = seed ^ 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn next_pow2(n: usize) -> usize {
    n.next_power_of_two()
}

fn get_bit(bits: &[u8], i: usize) -> bool {
    bits[i / 8] & (1 << (i % 8)) != 0
}

fn set_bit(bits: &mut [u8], i: usize) {
    bits[i / 8] |= 1 << (i % 8);
}

pub(crate) fn encode(index: &MphfIndex, out: &mut Vec<u8>) {
    out.extend_from_slice(&index.n_keys.to_le_bytes());
    out.extend_from_slice(&index.n_buckets.to_le_bytes());
    out.extend_from_slice(&index.table_size.to_le_bytes());
    out.extend_from_slice(&index.salt.to_le_bytes());
    for &d in &index.displacements {
        out.extend_from_slice(&d.to_le_bytes());
    }
    for &fp in &index.fingerprints {
        out.extend_from_slice(&fp.to_le_bytes());
    }
    for &o in &index.offsets {
        out.extend_from_slice(&o.to_le_bytes());
    }
    out.extend_from_slice(&(index.occupied.len() as u32).to_le_bytes());
    out.extend_from_slice(&index.occupied);
}

pub(crate) fn decode(bytes: &[u8]) -> Option<MphfIndex> {
    if bytes.len() < 4 + 4 + 4 + 8 {
        return None;
    }
    let n_keys = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
    let n_buckets = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
    let table_size = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    let salt = u64::from_le_bytes(bytes[12..20].try_into().ok()?);
    let mut off = 20;
    let mut displacements = Vec::with_capacity(n_buckets as usize);
    for _ in 0..n_buckets {
        if off + 4 > bytes.len() {
            return None;
        }
        displacements.push(u32::from_le_bytes(bytes[off..off + 4].try_into().ok()?));
        off += 4;
    }
    let mut fingerprints = Vec::with_capacity(table_size as usize);
    for _ in 0..table_size {
        if off + 8 > bytes.len() {
            return None;
        }
        fingerprints.push(u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?));
        off += 8;
    }
    let mut offsets = Vec::with_capacity(table_size as usize);
    for _ in 0..table_size {
        if off + 8 > bytes.len() {
            return None;
        }
        offsets.push(u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?));
        off += 8;
    }
    if off + 4 > bytes.len() {
        return None;
    }
    let occ_len = u32::from_le_bytes(bytes[off..off + 4].try_into().ok()?) as usize;
    off += 4;
    if off + occ_len > bytes.len() {
        return None;
    }
    let occupied = bytes[off..off + occ_len].to_vec();
    Some(MphfIndex {
        displacements,
        fingerprints,
        offsets,
        occupied,
        n_keys,
        n_buckets,
        table_size,
        salt,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mphf_all_hits_no_false() {
        let sorted: Vec<_> = (0..1000u64)
            .map(|i| (format!("key-{i}").into_bytes(), i * 13))
            .collect();
        let idx = MphfIndex::build(&sorted);
        assert_eq!(idx.len(), 1000);
        for (k, o) in &sorted {
            assert_eq!(idx.get(k), Some(*o));
        }
        for i in 1000..1100u64 {
            assert!(idx.get(format!("key-{i}").as_bytes()).is_none());
        }
    }

    #[test]
    fn mphf_empty() {
        let idx = MphfIndex::build(&[]);
        assert!(idx.get(b"x").is_none());
        assert_eq!(idx.len(), 0);
    }
}
