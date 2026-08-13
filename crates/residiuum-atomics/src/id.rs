//! Atomic and related identities (`ATOMICS_SPEC` §5).
//!
//! Widths are frozen here so later codec cards cannot silently shrink them.
//! UUID validation of Heap/collection IDs remains `residiuum-heap`; this crate
//! only requires non-zero opaque bytes so it stays free of heap/store/SDK.

use crate::canonical::{DOMAIN_ATOMIC_CONTENT, DOMAIN_ATOMIC_ID};
use crate::error::AtomicsError;
use std::fmt;

/// 32-byte Atomic identity. Distinct from the driver's 16-byte `OperationId`
/// and 16-byte `RequestId`; it MUST NOT be truncated into either namespace.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AtomicId([u8; 32]);

/// BLAKE3-256 content root of one canonical plan (`ATOMICS_SPEC` §5).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentRoot([u8; 32]);

/// Heap identity bytes. Same width as `residiuum_heap::HeapId`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeapId([u8; 16]);

/// Collection identity bytes. Same width as `residiuum_heap::CollectionId`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CollectionId([u8; 16]);

/// Version or event identity bytes used as preconditions and receipts.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VersionId([u8; 16]);

macro_rules! sixteen {
    ($name:ident, $label:expr) => {
        impl $name {
            /// Width in bytes.
            pub const BYTE_LEN: usize = 16;

            /// Reject the all-zero identity.
            pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, AtomicsError> {
                if bytes == [0u8; 16] {
                    return Err(AtomicsError::InvalidIdentity(concat!($label, " is zero")));
                }
                Ok(Self(bytes))
            }

            /// Borrowed bytes.
            pub fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            /// Copied bytes.
            pub fn to_bytes(self) -> [u8; 16] {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), hex16(&self.0))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&hex16(&self.0))
            }
        }
    };
}

sixteen!(HeapId, "HeapId");
sixteen!(CollectionId, "CollectionId");
sixteen!(VersionId, "VersionId");

impl AtomicId {
    /// Width in bytes. Never 16.
    pub const BYTE_LEN: usize = 32;

    /// Reject the all-zero identity.
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self, AtomicsError> {
        if bytes == [0u8; 32] {
            return Err(AtomicsError::InvalidIdentity("AtomicId is zero"));
        }
        Ok(Self(bytes))
    }

    /// Caller-generated identity from a cryptographically secure source.
    pub fn random() -> Result<Self, AtomicsError> {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).map_err(|_| AtomicsError::EntropyUnavailable)?;
        Self::from_bytes(bytes).or(Err(AtomicsError::EntropyUnavailable))
    }

    /// Engine-generated identity (`ATOMICS_SPEC` §5).
    ///
    /// `BLAKE3-256("RESIDIUUM-ATOMIC-ID-V1" || heap_id || source_operation_id || invariant_or_job_id)`
    pub fn derive_engine(
        heap_id: HeapId,
        source_operation_id: &[u8; 16],
        invariant_or_job_id: &[u8],
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(DOMAIN_ATOMIC_ID);
        hasher.update(heap_id.as_bytes());
        hasher.update(source_operation_id);
        hasher.update(invariant_or_job_id);
        Self(*hasher.finalize().as_bytes())
    }

    /// Borrowed bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Copied bytes.
    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl ContentRoot {
    /// Width in bytes.
    pub const BYTE_LEN: usize = 32;

    /// Hash already-canonical plan bytes. Canonicalization itself is ATM-0.2.
    pub fn from_canonical_plan_bytes(canonical_plan_bytes: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(DOMAIN_ATOMIC_CONTENT);
        hasher.update(canonical_plan_bytes);
        Self(*hasher.finalize().as_bytes())
    }

    /// Reject the all-zero root.
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self, AtomicsError> {
        if bytes == [0u8; 32] {
            return Err(AtomicsError::InvalidIdentity("ContentRoot is zero"));
        }
        Ok(Self(bytes))
    }

    /// Borrowed bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Copied bytes.
    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for AtomicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AtomicId({})", hex32(&self.0))
    }
}

impl fmt::Display for AtomicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex32(&self.0))
    }
}

impl fmt::Debug for ContentRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentRoot({})", hex32(&self.0))
    }
}

impl fmt::Display for ContentRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex32(&self.0))
    }
}

fn hex16(bytes: &[u8; 16]) -> String {
    hex_of(bytes)
}

fn hex32(bytes: &[u8; 32]) -> String {
    hex_of(bytes)
}

fn hex_of(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = fmt::Write::write_fmt(&mut out, format_args!("{b:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn heap(n: u8) -> HeapId {
        let mut b = [0u8; 16];
        b[0] = n;
        HeapId::from_bytes(b).unwrap()
    }

    #[test]
    fn atomic_id_is_32_bytes_and_rejects_zero() {
        assert_eq!(AtomicId::BYTE_LEN, 32);
        assert!(AtomicId::from_bytes([0u8; 32]).is_err());
        let mut one = [0u8; 32];
        one[0] = 1;
        assert!(AtomicId::from_bytes(one).is_ok());
    }

    #[test]
    fn random_id_is_nonzero() {
        let id = AtomicId::random().expect("entropy");
        assert_ne!(id.to_bytes(), [0u8; 32]);
    }

    #[test]
    fn engine_id_is_deterministic_and_heap_sensitive() {
        let op = [7u8; 16];
        let a = AtomicId::derive_engine(heap(1), &op, b"job");
        let b = AtomicId::derive_engine(heap(1), &op, b"job");
        let c = AtomicId::derive_engine(heap(2), &op, b"job");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a.to_bytes(), [0u8; 32]);
    }

    #[test]
    fn content_root_changes_with_bytes_and_binds_domain() {
        let r1 = ContentRoot::from_canonical_plan_bytes(b"plan-a");
        let r2 = ContentRoot::from_canonical_plan_bytes(b"plan-b");
        assert_ne!(r1, r2);
        let mut raw = blake3::Hasher::new();
        raw.update(b"plan-a");
        assert_ne!(r1.to_bytes(), *raw.finalize().as_bytes());
    }

    #[test]
    fn sixteen_byte_ids_reject_zero() {
        assert!(HeapId::from_bytes([0u8; 16]).is_err());
        assert!(CollectionId::from_bytes([0u8; 16]).is_err());
        assert!(VersionId::from_bytes([0u8; 16]).is_err());
    }
}
