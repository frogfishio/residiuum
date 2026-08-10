//! Adaptive index selection for one sealed segment's key sample.

/// Default key count at or below which Eytzinger wins on simplicity and locality.
pub const DEFAULT_TINY_THRESHOLD: usize = 64;

/// Physical index kinds Hydra can compile per segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum IndexKind {
    /// Sorted Eytzinger array (tiny segments).
    Eytzinger = 1,
    /// Piecewise-linear PGM++ style model (ordered numeric, irregular spacing).
    Pgm = 2,
    /// Radix table + spline (ordered numeric, dense / regular).
    RadixSpline = 3,
    /// Path-compressed ART / radix (ordered strings / irregular keys).
    CompressedRadix = 4,
    /// Minimal perfect hash + fingerprint (point-only immutable sets).
    Mphf = 5,
}

impl IndexKind {
    /// Wire discriminant.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse wire discriminant.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Eytzinger),
            2 => Some(Self::Pgm),
            3 => Some(Self::RadixSpline),
            4 => Some(Self::CompressedRadix),
            5 => Some(Self::Mphf),
            _ => None,
        }
    }

    /// Human-readable label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eytzinger => "eytzinger",
            Self::Pgm => "pgm",
            Self::RadixSpline => "radix_spline",
            Self::CompressedRadix => "compressed_radix",
            Self::Mphf => "mphf",
        }
    }
}

/// Observed key-distribution shape (for diagnostics / selection).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyShape {
    /// Empty key set.
    Empty,
    /// Small enough that a flat Eytzinger array is optimal.
    Tiny,
    /// Fixed-width numeric keys with roughly regular spacing.
    OrderedNumericDense,
    /// Fixed-width numeric keys with irregular gaps.
    OrderedNumericSparse,
    /// Variable / string / irregular keys that still sort lexicographically.
    OrderedBytes,
}

/// Options controlling Hydra construction and selection.
#[derive(Debug, Clone)]
pub struct HydraBuildOptions {
    /// Force MPHF path (point lookups only; no ordered scan).
    pub point_only: bool,
    /// Key count ≤ this → Eytzinger (unless `point_only`).
    pub tiny_threshold: usize,
    /// PGM error bound (positions). Smaller → more model segments, tighter search.
    pub pgm_epsilon: u32,
    /// Top radix bits for RadixSpline table (4..=16 practical).
    pub spline_radix_bits: u8,
    /// Worker count for `build_many` (0 = estimate from available parallelism).
    pub threads: usize,
    /// Force a specific kind (tests / operator override). `None` = adaptive.
    pub force_kind: Option<IndexKind>,
}

impl Default for HydraBuildOptions {
    fn default() -> Self {
        Self {
            point_only: false,
            tiny_threshold: DEFAULT_TINY_THRESHOLD,
            pgm_epsilon: 32,
            spline_radix_bits: 8,
            threads: 0,
            force_kind: None,
        }
    }
}

impl HydraBuildOptions {
    /// Resolve worker count for parallel builds.
    pub fn effective_threads(&self) -> usize {
        if self.threads > 0 {
            return self.threads;
        }
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .clamp(1, 32)
    }
}

/// Classify a sorted unique key list.
pub fn classify_keys(sorted: &[(Vec<u8>, u64)], tiny_threshold: usize) -> KeyShape {
    if sorted.is_empty() {
        return KeyShape::Empty;
    }
    if sorted.len() <= tiny_threshold {
        return KeyShape::Tiny;
    }
    if is_fixed_width_numeric(sorted) {
        if is_dense_numeric(sorted) {
            KeyShape::OrderedNumericDense
        } else {
            KeyShape::OrderedNumericSparse
        }
    } else {
        KeyShape::OrderedBytes
    }
}

/// Choose the physical index for this segment.
pub fn select_index_kind(sorted: &[(Vec<u8>, u64)], opts: &HydraBuildOptions) -> IndexKind {
    if let Some(k) = opts.force_kind {
        return k;
    }
    if opts.point_only {
        // Empty and tiny point-only sets still use MPHF (uniform path).
        return IndexKind::Mphf;
    }
    match classify_keys(sorted, opts.tiny_threshold) {
        KeyShape::Empty | KeyShape::Tiny => IndexKind::Eytzinger,
        KeyShape::OrderedNumericDense => IndexKind::RadixSpline,
        KeyShape::OrderedNumericSparse => IndexKind::Pgm,
        KeyShape::OrderedBytes => IndexKind::CompressedRadix,
    }
}

/// Fixed-width keys that decode as big-endian integers of width 4 or 8.
fn is_fixed_width_numeric(sorted: &[(Vec<u8>, u64)]) -> bool {
    if sorted.is_empty() {
        return false;
    }
    let w = sorted[0].0.len();
    if w != 4 && w != 8 {
        return false;
    }
    sorted.iter().all(|(k, _)| k.len() == w)
}

/// Dense when the span of numeric values is O(n) (few holes).
fn is_dense_numeric(sorted: &[(Vec<u8>, u64)]) -> bool {
    let n = sorted.len() as u64;
    if n < 2 {
        return true;
    }
    let first = be_u64(&sorted[0].0);
    let last = be_u64(&sorted[n as usize - 1].0);
    let span = last.saturating_sub(first).saturating_add(1);
    // Dense if fewer than ~50% holes in the value span.
    span <= n.saturating_mul(2)
}

/// Interpret 4- or 8-byte BE key as u64.
pub(crate) fn be_u64(key: &[u8]) -> u64 {
    match key.len() {
        8 => u64::from_be_bytes(key.try_into().unwrap_or([0; 8])),
        4 => u32::from_be_bytes(key.try_into().unwrap_or([0; 4])) as u64,
        _ => {
            // Order-preserving rank: first 8 bytes as BE (pad right with zeros).
            let mut buf = [0u8; 8];
            let n = key.len().min(8);
            buf[..n].copy_from_slice(&key[..n]);
            u64::from_be_bytes(buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(xs: &[&[u8]]) -> Vec<(Vec<u8>, u64)> {
        xs.iter()
            .enumerate()
            .map(|(i, k)| (k.to_vec(), i as u64))
            .collect()
    }

    #[test]
    fn tiny_and_point_only() {
        let k = keys(&[b"a", b"b", b"c"]);
        assert_eq!(
            select_index_kind(&k, &HydraBuildOptions::default()),
            IndexKind::Eytzinger
        );
        let opts = HydraBuildOptions {
            point_only: true,
            ..Default::default()
        };
        assert_eq!(select_index_kind(&k, &opts), IndexKind::Mphf);
    }

    #[test]
    fn dense_vs_sparse_numeric() {
        // Dense u64 sequence 0..200
        let dense: Vec<_> = (0..200u64).map(|i| (i.to_be_bytes().to_vec(), i)).collect();
        assert_eq!(
            select_index_kind(&dense, &HydraBuildOptions::default()),
            IndexKind::RadixSpline
        );
        // Sparse: squares
        let sparse: Vec<_> = (0..200u64)
            .map(|i| {
                let v = i * i * 1000;
                (v.to_be_bytes().to_vec(), i)
            })
            .collect();
        assert_eq!(
            select_index_kind(&sparse, &HydraBuildOptions::default()),
            IndexKind::Pgm
        );
    }

    #[test]
    fn strings_are_radix() {
        let s: Vec<_> = (0..100u64)
            .map(|i| (format!("name-{i}").into_bytes(), i))
            .collect();
        assert_eq!(
            select_index_kind(&s, &HydraBuildOptions::default()),
            IndexKind::CompressedRadix
        );
    }
}
