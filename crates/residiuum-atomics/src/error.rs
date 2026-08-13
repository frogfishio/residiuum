//! Construction and identity errors for the protocol crate.
//!
//! These are not Atomic outcomes. Request refusal and durable `not committed`
//! live in [`crate::outcome`].

use thiserror::Error;

/// Fail-closed error for identity and constructor validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AtomicsError {
    /// Zero or otherwise illegal identity bytes.
    #[error("invalid atomic identity: {0}")]
    InvalidIdentity(&'static str),
    /// `getrandom` failed while minting a caller `AtomicId`.
    #[error("secure entropy unavailable for AtomicId")]
    EntropyUnavailable,
}

impl AtomicsError {
    /// Stable snake_case code for tests and later SDK mapping.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidIdentity(_) => "invalid_identity",
            Self::EntropyUnavailable => "entropy_unavailable",
        }
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
