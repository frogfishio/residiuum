//! Secret continuation-token key material (DEF-097).
//!
//! Continuation MACs must not be forgeable from public store/cluster IDs.
//! Keys are ≥256 bits of CSPRNG entropy, persisted under `store-info/` (store)
//! or `cluster-meta/` (cluster), never derived solely from public identifiers.
//!
//! Secrets never appear in diagnostics, SDA projections, or operator logs.
//! Display/Debug implementations omit key bytes.

use crate::atomic_file;
use crate::error::StoreError;
use crate::ids::fill_random;
use crate::layout::StorePaths;
use blake3::Hasher;
use std::fs;
use std::path::{Path, PathBuf};

/// Profile tag for token-key material (DEF-097).
pub const TOKEN_KEY_PROFILE: &str = "residiuum-token-key-v1";

/// On-disk filename under `store-info/`.
pub const CURSOR_TOKEN_KEYS_FILE: &str = "cursor_token_keys.v1";

/// Secret size (256 bits).
pub const TOKEN_SECRET_LEN: usize = 32;

const FILE_MAGIC: &[u8; 8] = b"RTK00001";
const FILE_VERSION: u32 = 1;

/// One generation of MAC secret material.
#[derive(Clone)]
pub struct TokenKeyGeneration {
    /// Monotonic generation id (starts at 1).
    pub generation_id: u32,
    /// 32-byte secret (never logged).
    secret: [u8; TOKEN_SECRET_LEN],
}

impl TokenKeyGeneration {
    /// Mint a fresh generation with CSPRNG secret.
    pub fn mint(generation_id: u32) -> Result<Self, StoreError> {
        let mut secret = [0u8; TOKEN_SECRET_LEN];
        fill_random(&mut secret)?;
        Ok(Self {
            generation_id,
            secret,
        })
    }

    /// Construct from known material (tests / restore of retained gens only).
    pub fn from_parts(generation_id: u32, secret: [u8; TOKEN_SECRET_LEN]) -> Self {
        Self {
            generation_id,
            secret,
        }
    }

    /// Generation id.
    pub fn id(&self) -> u32 {
        self.generation_id
    }

    /// Derive a 32-byte BLAKE3 keyed-hash key for domain `domain`.
    ///
    /// The public identifier (store/cluster id) may be mixed into the domain
    /// binding but is **never** sufficient without `secret`.
    pub fn mac_key(&self, domain: &[u8], public_id: &[u8]) -> [u8; 32] {
        let mut h = Hasher::new_keyed(&self.secret);
        h.update(domain);
        h.update(&[0]);
        h.update(public_id);
        h.update(&self.generation_id.to_le_bytes());
        *h.finalize().as_bytes()
    }

    /// Zeroize secret bytes (best-effort; no zeroize crate in residiuum-store).
    pub fn zeroize(&mut self) {
        for b in &mut self.secret {
            *b = 0;
        }
    }
}

impl Drop for TokenKeyGeneration {
    fn drop(&mut self) {
        self.zeroize();
    }
}

impl std::fmt::Debug for TokenKeyGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenKeyGeneration")
            .field("generation_id", &self.generation_id)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Active (+ optional previous) continuation-token keyring (DEF-097).
///
/// Encode uses the active generation. Decode accepts active, then previous
/// during grace (previous retained until rotate retires it or explicit retire).
#[derive(Clone)]
pub struct ContinuationKeyring {
    /// Current signing generation.
    active: TokenKeyGeneration,
    /// Previous generation accepted for verify during grace (`None` if none).
    previous: Option<TokenKeyGeneration>,
}

impl std::fmt::Debug for ContinuationKeyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContinuationKeyring")
            .field("active_generation_id", &self.active.generation_id)
            .field(
                "previous_generation_id",
                &self.previous.as_ref().map(|g| g.generation_id),
            )
            .finish()
    }
}

impl ContinuationKeyring {
    /// Mint a brand-new keyring (store/cluster create).
    pub fn mint_new() -> Result<Self, StoreError> {
        Ok(Self {
            active: TokenKeyGeneration::mint(1)?,
            previous: None,
        })
    }

    /// Active generation id used when encoding new tokens.
    pub fn active_generation_id(&self) -> u32 {
        self.active.generation_id
    }

    /// Previous generation id if any (grace verify).
    pub fn previous_generation_id(&self) -> Option<u32> {
        self.previous.as_ref().map(|g| g.generation_id)
    }

    /// MAC key for the active generation.
    pub fn active_mac_key(&self, domain: &[u8], public_id: &[u8]) -> [u8; 32] {
        self.active.mac_key(domain, public_id)
    }

    /// Resolve MAC key for a token that names `generation_id`.
    ///
    /// Returns `None` if the generation is unknown/retired.
    pub fn mac_key_for(
        &self,
        generation_id: u32,
        domain: &[u8],
        public_id: &[u8],
    ) -> Option<[u8; 32]> {
        if self.active.generation_id == generation_id {
            return Some(self.active.mac_key(domain, public_id));
        }
        if let Some(prev) = &self.previous {
            if prev.generation_id == generation_id {
                return Some(prev.mac_key(domain, public_id));
            }
        }
        None
    }

    /// Rotate: mint a new active generation; previous becomes the old active.
    ///
    /// Tokens signed with the retired (older than previous) generation fail.
    /// Only one previous generation is retained (grace window of one rotation).
    pub fn rotate(&mut self) -> Result<u32, StoreError> {
        let next_id = self.active.generation_id.saturating_add(1).max(1);
        let new_active = TokenKeyGeneration::mint(next_id)?;
        let old = std::mem::replace(&mut self.active, new_active);
        // Drop any older previous (retire beyond grace).
        if let Some(mut p) = self.previous.take() {
            p.zeroize();
        }
        self.previous = Some(old);
        Ok(self.active.generation_id)
    }

    /// Drop previous generation immediately (end grace).
    pub fn retire_previous(&mut self) {
        if let Some(mut p) = self.previous.take() {
            p.zeroize();
        }
    }

    /// Encode durable file bytes (magic + version + gens). No human-readable secrets.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + 4 + 4 + 32 + 1 + 4 + 32);
        out.extend_from_slice(FILE_MAGIC);
        out.extend_from_slice(&FILE_VERSION.to_le_bytes());
        out.extend_from_slice(&self.active.generation_id.to_le_bytes());
        out.extend_from_slice(&self.active.secret);
        match &self.previous {
            None => out.push(0),
            Some(p) => {
                out.push(1);
                out.extend_from_slice(&p.generation_id.to_le_bytes());
                out.extend_from_slice(&p.secret);
            }
        }
        out
    }

    /// Decode keyring from durable bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() < 8 + 4 + 4 + TOKEN_SECRET_LEN + 1 {
            return Err(StoreError::CorruptMeta("cursor token keyring truncated"));
        }
        if &bytes[..8] != FILE_MAGIC {
            return Err(StoreError::CorruptMeta(
                "cursor token keyring magic mismatch",
            ));
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != FILE_VERSION {
            return Err(StoreError::CorruptMeta(
                "cursor token keyring version unsupported",
            ));
        }
        let mut o = 12usize;
        let active_id = u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        o += 4;
        let mut active_secret = [0u8; TOKEN_SECRET_LEN];
        active_secret.copy_from_slice(&bytes[o..o + TOKEN_SECRET_LEN]);
        o += TOKEN_SECRET_LEN;
        let has_prev = bytes[o];
        o += 1;
        let active = TokenKeyGeneration::from_parts(active_id, active_secret);
        let previous = if has_prev == 0 {
            None
        } else {
            if bytes.len() < o + 4 + TOKEN_SECRET_LEN {
                return Err(StoreError::CorruptMeta(
                    "cursor token keyring previous truncated",
                ));
            }
            let prev_id = u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
            o += 4;
            let mut prev_secret = [0u8; TOKEN_SECRET_LEN];
            prev_secret.copy_from_slice(&bytes[o..o + TOKEN_SECRET_LEN]);
            Some(TokenKeyGeneration::from_parts(prev_id, prev_secret))
        };
        Ok(Self { active, previous })
    }

    /// Absolute path of the store-local keyring file.
    pub fn store_path(paths: &StorePaths) -> PathBuf {
        paths.store_info().join(CURSOR_TOKEN_KEYS_FILE)
    }

    /// Persist to `store-info/cursor_token_keys.v1` atomically.
    pub fn save_store(&self, paths: &StorePaths) -> Result<(), StoreError> {
        let path = Self::store_path(paths);
        atomic_file::write_atomic(&path, &self.to_bytes())?;
        // Best-effort restrictive mode (unix).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }

    /// Load from store path, or mint+persist if missing (upgrade path).
    pub fn load_or_mint_store(paths: &StorePaths) -> Result<Self, StoreError> {
        let path = Self::store_path(paths);
        if path.is_file() {
            let bytes = fs::read(&path)?;
            return Self::from_bytes(&bytes);
        }
        let ring = Self::mint_new()?;
        ring.save_store(paths)?;
        Ok(ring)
    }

    /// Load required keyring (no mint) — fails if absent.
    pub fn load_store(paths: &StorePaths) -> Result<Self, StoreError> {
        let path = Self::store_path(paths);
        if !path.is_file() {
            return Err(StoreError::CorruptMeta("cursor token keyring missing"));
        }
        let bytes = fs::read(&path)?;
        Self::from_bytes(&bytes)
    }

    /// Cluster control-plane path: `{root}/cluster_token_keys.v1`.
    pub fn cluster_path(root: &Path) -> PathBuf {
        root.join("cluster_token_keys.v1")
    }

    /// Persist cluster keyring.
    pub fn save_cluster(&self, root: &Path) -> Result<(), StoreError> {
        let path = Self::cluster_path(root);
        atomic_file::write_atomic(&path, &self.to_bytes())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }

    /// Load or mint cluster keyring under cluster root.
    pub fn load_or_mint_cluster(root: &Path) -> Result<Self, StoreError> {
        let path = Self::cluster_path(root);
        if path.is_file() {
            let bytes = fs::read(&path)?;
            return Self::from_bytes(&bytes);
        }
        let ring = Self::mint_new()?;
        ring.save_cluster(root)?;
        Ok(ring)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn public_id_insufficient_without_secret() {
        let a = TokenKeyGeneration::from_parts(1, [1u8; 32]);
        let b = TokenKeyGeneration::from_parts(1, [2u8; 32]);
        let store_id = [9u8; 16];
        let ka = a.mac_key(b"domain", &store_id);
        let kb = b.mac_key(b"domain", &store_id);
        assert_ne!(ka, kb);
    }

    #[test]
    fn rotate_retains_previous_only() {
        let mut ring = ContinuationKeyring::mint_new().unwrap();
        let g1 = ring.active_generation_id();
        ring.rotate().unwrap();
        let g2 = ring.active_generation_id();
        assert_ne!(g1, g2);
        assert_eq!(ring.previous_generation_id(), Some(g1));
        assert!(ring.mac_key_for(g1, b"d", &[0; 16]).is_some());
        assert!(ring.mac_key_for(g2, b"d", &[0; 16]).is_some());
        ring.rotate().unwrap();
        // g1 retired
        assert!(ring.mac_key_for(g1, b"d", &[0; 16]).is_none());
        assert!(ring.mac_key_for(g2, b"d", &[0; 16]).is_some());
    }

    #[test]
    fn store_persist_roundtrip() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let ring = ContinuationKeyring::mint_new().unwrap();
        ring.save_store(&paths).unwrap();
        let loaded = ContinuationKeyring::load_store(&paths).unwrap();
        assert_eq!(loaded.active_generation_id(), ring.active_generation_id());
        assert_eq!(
            loaded.active_mac_key(b"d", &[1; 16]),
            ring.active_mac_key(b"d", &[1; 16])
        );
    }

    #[test]
    fn debug_redacts_secret() {
        let g = TokenKeyGeneration::from_parts(3, [0xab; 32]);
        let s = format!("{g:?}");
        assert!(s.contains("redacted"));
        assert!(!s.contains("ab"));
    }
}
