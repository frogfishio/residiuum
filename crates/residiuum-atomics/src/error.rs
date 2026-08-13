//! Construction and identity errors for the protocol crate.
//!
//! These are not Atomic outcomes. Request refusal and durable `not committed`
//! live in [`crate::outcome`].

use thiserror::Error;

use crate::outcome::AtomicRefuseReason;

/// Deterministic CBOR decode/encode failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CborError {
    /// Buffer ended before a complete item.
    #[error("truncated CBOR item")]
    Truncated,
    /// Trailing bytes after the top-level map.
    #[error("trailing bytes after canonical map")]
    TrailingBytes,
    /// Top-level value is not a definite map.
    #[error("canonical item must be a definite-length map")]
    NotMap,
    /// Indefinite-length item encountered.
    #[error("indefinite-length CBOR is not allowed")]
    Indefinite,
    /// Non-shortest integer or length encoding.
    #[error("non-shortest CBOR integer encoding")]
    NonShortest,
    /// Duplicate map key.
    #[error("duplicate CBOR map key")]
    DuplicateKey,
    /// Map keys are not sorted by encoded byte order.
    #[error("CBOR map keys are not sorted")]
    UnsortedKeys,
    /// Map key is not an unsigned integer.
    #[error("CBOR map key must be an unsigned integer")]
    NonUintKey,
    /// Unsupported major type, tag, float, or simple.
    #[error("unsupported CBOR major type or simple value")]
    Unsupported,
    /// CBOR tags are rejected.
    #[error("CBOR tags are not allowed")]
    TagRejected,
}

/// Fail-closed error for identity and constructor validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AtomicsError {
    /// Zero or otherwise illegal identity bytes.
    #[error("invalid atomic identity: {0}")]
    InvalidIdentity(&'static str),
    /// `getrandom` failed while minting a caller `AtomicId`.
    #[error("secure entropy unavailable for AtomicId")]
    EntropyUnavailable,
    /// Canonical CBOR failed.
    #[error(transparent)]
    Cbor(CborError),
    /// Structural refusal before acceptance. No Atomic is issued.
    #[error("atomic plan refused: {0:?}")]
    Refused(AtomicRefuseReason),
}

impl AtomicsError {
    /// Stable snake_case code for tests and later SDK mapping.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidIdentity(_) => "invalid_identity",
            Self::EntropyUnavailable => "entropy_unavailable",
            Self::Cbor(_) => "cbor",
            Self::Refused(reason) => reason.as_str(),
        }
    }
}

impl From<CborError> for AtomicsError {
    fn from(err: CborError) -> Self {
        Self::Cbor(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_snake_case() {
        assert_eq!(
            AtomicsError::EntropyUnavailable.as_str(),
            "entropy_unavailable"
        );
    }
}
