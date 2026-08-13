//! Domain separators and canonical-order vocabulary (`ATOMICS_SPEC` §§5–6, 9).
//!
//! Deterministic CBOR field numbers, encode/decode, content-root fixtures, and
//! duplicate-target refusal live in ATM-0.2. This module only freezes the
//! separators so later cards cannot invent new ones.

/// Engine `AtomicId` derivation (`ATOMICS_SPEC` §5).
pub const DOMAIN_ATOMIC_ID: &[u8] = b"RESIDIUUM-ATOMIC-ID-V1";

/// Plan content root (`ATOMICS_SPEC` §5).
pub const DOMAIN_ATOMIC_CONTENT: &[u8] = b"RESIDIUUM-ATOMIC-CONTENT-V1";

/// Prepare evidence (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_PREPARE: &[u8] = b"RESIDIUUM-ATOMIC-PREPARE-V1";

/// Member evidence (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_MEMBER: &[u8] = b"RESIDIUUM-ATOMIC-MEMBER-V1";

/// Decision evidence (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_DECISION: &[u8] = b"RESIDIUUM-ATOMIC-DECISION-V1";

/// Ordered member manifest root (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_MANIFEST: &[u8] = b"RESIDIUUM-ATOMIC-MANIFEST-V1";

/// Read-set root (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_READSET: &[u8] = b"RESIDIUUM-ATOMIC-READSET-V1";

/// Predicate-set root (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_PREDICATES: &[u8] = b"RESIDIUUM-ATOMIC-PREDICATES-V1";

/// Canonical member order components. Encoding of these bytes is ATM-0.2.
///
/// Order is `(heap_id, collection_id, canonical_key_bytes, member_kind, ordinal)`.
pub const CANONICAL_TARGET_ORDER: &str =
    "heap_id, collection_id, canonical_key_bytes, member_kind, ordinal";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_separators_are_v1_and_distinct() {
        let all = [
            DOMAIN_ATOMIC_ID,
            DOMAIN_ATOMIC_CONTENT,
            DOMAIN_ATOMIC_PREPARE,
            DOMAIN_ATOMIC_MEMBER,
            DOMAIN_ATOMIC_DECISION,
            DOMAIN_ATOMIC_MANIFEST,
            DOMAIN_ATOMIC_READSET,
            DOMAIN_ATOMIC_PREDICATES,
        ];
        for (i, a) in all.iter().enumerate() {
            assert!(a.starts_with(b"RESIDIUUM-ATOMIC-"));
            assert!(a.ends_with(b"-V1"));
            for (j, b) in all.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b);
                }
            }
        }
    }
}
