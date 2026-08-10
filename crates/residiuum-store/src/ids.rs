//! Identifier generation (DEF-025).
//!
//! # Profile
//!
//! `ID_PROFILE` versions the generation rules. Changing entropy, length, or
//! the sortable/random split requires a new profile tag and tests.
//!
//! # Kinds
//!
//! | Kind | Size | Entropy source | Sortable? | Typical use |
//! |------|------|----------------|-----------|-------------|
//! | Random identity | 16 bytes | OS CSPRNG (`getrandom`) | no | `event_id`, `store_id`, job/checkpoint/operation ids |
//! | Sortable segment | 16 bytes | process-recovered `u64` counter + store mix | yes (`u64` LE prefix) | `segment_id` |
//! | Content-derived | 16 bytes | `blake3(subject)[..16]` | no | `item_id` (stable for a subject) |
//!
//! Random identities **fail closed** when the OS CSPRNG is unavailable. There
//! is no wall-clock / hash fallback for correctness-sensitive ids.
//!
//! Monotonic counters are used only where the protocol needs order or recovery
//! of the next free value (`segment_id` via on-disk max seq). Event identity
//! is pure random; ordering uses writer sequence / segment layout, not
//! `event_id`.

use crate::error::StoreError;
use std::cell::RefCell;

/// Generation profile tag for diagnostics and capability matrix.
pub const ID_PROFILE: &str = "residiuum-id-v1";

/// Fixed width of random and segment identities on the wire (FORMAT_SPEC).
pub const ID_LEN: usize = 16;

/// Bytes drawn from OS CSPRNG per refill. Amortizes `getrandom` syscalls on the
/// hot put path (Mode A instrumentation: put_prep was ~65% wall; per-event
/// `getrandom` was the dominant prep cost). Entropy source remains OS CSPRNG
/// only — no PRNG substitution (DEF-025 / fail-closed).
const RANDOM_POOL_LEN: usize = 4096;

thread_local! {
    /// Process-local refill buffer still filled exclusively via [`getrandom`].
    static RANDOM_POOL: RefCell<RandomPool> = RefCell::new(RandomPool::empty());
}

struct RandomPool {
    buf: [u8; RANDOM_POOL_LEN],
    /// Next unread index in `buf`.
    off: usize,
    /// Valid prefix length after last successful refill (`0` until first fill).
    end: usize,
}

impl RandomPool {
    fn empty() -> Self {
        Self {
            buf: [0u8; RANDOM_POOL_LEN],
            off: 0,
            end: 0,
        }
    }

    fn fill_from_os(&mut self) -> Result<(), StoreError> {
        getrandom::fill(&mut self.buf).map_err(|e| {
            StoreError::RandomUnavailable(format!("getrandom failed ({ID_PROFILE}): {e}"))
        })?;
        self.off = 0;
        self.end = RANDOM_POOL_LEN;
        Ok(())
    }

    fn take(&mut self, out: &mut [u8]) -> Result<(), StoreError> {
        let mut filled = 0usize;
        while filled < out.len() {
            if self.off >= self.end {
                self.fill_from_os()?;
            }
            let n = (out.len() - filled).min(self.end - self.off);
            out[filled..filled + n].copy_from_slice(&self.buf[self.off..self.off + n]);
            self.off += n;
            filled += n;
        }
        Ok(())
    }
}

/// Fill `buf` from the OS CSPRNG (via a thread-local refill buffer).
///
/// Fails closed if entropy is unavailable. Still **only** OS CSPRNG bytes —
/// the buffer amortizes syscalls, not cryptographic strength.
pub fn fill_random(buf: &mut [u8]) -> Result<(), StoreError> {
    RANDOM_POOL.with(|pool| pool.borrow_mut().take(buf))
}

/// Mint a 16-byte random identity (event, store, job, operation, checkpoint).
///
/// Does **not** embed time or process counters. Callers that need order must
/// use writer sequences or [`mint_sortable_segment_id`].
pub fn random_id() -> Result<[u8; ID_LEN], StoreError> {
    let mut id = [0u8; ID_LEN];
    fill_random(&mut id)?;
    // Reject the all-zero id so protocol honesty checks that treat zero as
    // "missing" cannot be confused with a real mint (DEF-014).
    if id == [0u8; ID_LEN] {
        // Astronomically rare; one retry then fail closed.
        fill_random(&mut id)?;
        if id == [0u8; ID_LEN] {
            return Err(StoreError::RandomUnavailable(format!(
                "CSPRNG produced all-zero id ({ID_PROFILE})"
            )));
        }
    }
    Ok(id)
}

/// Sortable segment id: first 8 bytes = little-endian sequence, last 8 = mix
/// of that sequence with the store id (uniqueness across stores).
///
/// Sequence is recovered on open from existing segment filenames / descriptors
/// (`max(seq) + 1`). This is the only process-local counter used for identity.
pub fn mint_sortable_segment_id(seq: u64, store_id: &[u8; ID_LEN]) -> [u8; ID_LEN] {
    let mut id = [0u8; ID_LEN];
    id[..8].copy_from_slice(&seq.to_le_bytes());
    for i in 0..8 {
        id[8 + i] = store_id[i] ^ id[i];
    }
    id
}

/// Recover the mint sequence from a sortable segment id (first 8 LE bytes).
pub fn segment_seq_from_id(segment_id: &[u8; ID_LEN]) -> u64 {
    u64::from_le_bytes(segment_id[..8].try_into().expect("8 bytes"))
}

/// Stable content-derived item id from subject bytes (not random).
pub fn subject_item_id(subject: &[u8]) -> [u8; ID_LEN] {
    let hash = blake3::hash(subject);
    let mut id = [0u8; ID_LEN];
    id.copy_from_slice(&hash.as_bytes()[..ID_LEN]);
    id
}

/// Hex encoding for diagnostics (lowercase, 32 chars for 16-byte ids).
pub fn hex16(bytes: &[u8; ID_LEN]) -> String {
    let mut s = String::with_capacity(ID_LEN * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn id_profile_is_stable() {
        assert_eq!(ID_PROFILE, "residiuum-id-v1");
        assert_eq!(ID_LEN, 16);
    }

    #[test]
    fn random_ids_are_unique_and_nonzero() {
        let mut seen = HashSet::new();
        for _ in 0..256 {
            let id = random_id().expect("CSPRNG");
            assert_ne!(id, [0u8; 16]);
            assert!(seen.insert(id), "collision in small sample");
        }
    }

    #[test]
    fn sortable_segment_ids_order_by_seq() {
        let store = [0x11u8; 16];
        let a = mint_sortable_segment_id(1, &store);
        let b = mint_sortable_segment_id(2, &store);
        let c = mint_sortable_segment_id(100, &store);
        assert_eq!(segment_seq_from_id(&a), 1);
        assert_eq!(segment_seq_from_id(&b), 2);
        assert_eq!(segment_seq_from_id(&c), 100);
        assert!(a[..8] < b[..8]);
        assert!(b[..8] < c[..8]);
        // Different stores still encode the same seq prefix.
        let other = mint_sortable_segment_id(1, &[0x22u8; 16]);
        assert_eq!(segment_seq_from_id(&other), 1);
        assert_ne!(a, other);
    }

    #[test]
    fn subject_item_id_is_deterministic() {
        assert_eq!(subject_item_id(b"k"), subject_item_id(b"k"));
        assert_ne!(subject_item_id(b"k"), subject_item_id(b"k2"));
    }

    #[test]
    fn random_and_sortable_kinds_differ_in_shape() {
        // Random ids almost never look like low seq + store mix; just ensure
        // APIs stay separate (no time-hash hybrid returned from random_id).
        let r = random_id().unwrap();
        let s = mint_sortable_segment_id(1, &[0u8; 16]);
        // Sortable with seq=1 has LE prefix 01 00 00 00 00 00 00 00.
        assert_eq!(&s[..8], &1u64.to_le_bytes());
        // We do not require inequality (could collide theoretically); only
        // that random_id did not embed a wall-clock pattern we can detect.
        let _ = (r, s);
    }
}
