//! Master key provider (`HEAP_SPEC` §35 / HP-005).
//!
//! Concrete providers exist only in `residiuum-authority`. The data service must
//! never hold a `dyn MasterKeyProvider` or signing key.

use crate::error::AuthorityError;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use zeroize::Zeroize;

/// Local master-key provider. Signing stays inside this process.
pub trait MasterKeyProvider {
    /// Current Ed25519 verifying key bytes.
    fn public_key(&self) -> [u8; 32];
    /// Sign a message with the current master.
    fn sign(&self, message: &[u8]) -> Result<[u8; 64], AuthorityError>;
}

/// Ephemeral in-memory master (tests / operator-supplied seed ceremony).
pub struct EphemeralMasterKeyProvider {
    signing: SigningKey,
}

impl EphemeralMasterKeyProvider {
    /// Fresh random master.
    pub fn generate() -> Result<Self, AuthorityError> {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).map_err(|e| AuthorityError::Crypto(e.to_string()))?;
        let signing = SigningKey::from_bytes(&seed);
        seed.zeroize();
        Ok(Self { signing })
    }

    /// From an exact 32-byte seed (tests / development-file ceremony).
    pub fn from_seed(mut seed: [u8; 32]) -> Self {
        let signing = SigningKey::from_bytes(&seed);
        seed.zeroize();
        Self { signing }
    }
}

impl MasterKeyProvider for EphemeralMasterKeyProvider {
    fn public_key(&self) -> [u8; 32] {
        let vk: VerifyingKey = self.signing.verifying_key();
        vk.to_bytes()
    }

    fn sign(&self, message: &[u8]) -> Result<[u8; 64], AuthorityError> {
        Ok(self.signing.sign(message).to_bytes())
    }
}
