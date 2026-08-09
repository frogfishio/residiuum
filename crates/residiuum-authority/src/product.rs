//! Restart-safe local product bootstrap over the real Heap authority ceremony.
//!
//! This module intentionally lives in the AGPL authority crate. It is suitable
//! for an embedded application's local composition root, never for the
//! qualified data-service binary. Its concrete file key repository is visibly
//! development-grade; production deployments replace it with an OS keystore,
//! TPM, HSM, or remote signer while retaining the same ceremony.

use crate::ceremony::{commit_prepared_genesis, prepare_genesis, GenesisRequest, PreparedGenesis};
use crate::error::AuthorityError;
use crate::head::AuthorityHead;
use crate::issue::{issue_heap_key, IssueRequest};
use crate::provider::EphemeralMasterKeyProvider;
use crate::slot::{decode_slot_file, encode_slot_file};
use crate::store::{AuthorityPaths, MasterAuthorityStore};
use residiuum_format::{decode_deterministic_uint_map, encode_deterministic_uint_map, CborValue};
use residiuum_heap::{
    decide, inspect_certificate, mint_capability, verify_certificate, AuthorityEpoch,
    AuthorityGeneration, AuthorizationDecision, DeploymentId, HeapCap, HeapId,
    HeapSecuritySnapshot, HeapSlot, Operation, OperationDescriptor, OperationStatus, Rights,
    SecurityRevision, SecurityTimeFloor, VerifiedCertificate, CERT_MAX_LIFETIME_S,
};
use residiuum_store::{
    rebuild_and_persist_all_catalogs, rebuild_heap_entry_from_chain, HeapMetaLayout, StagedGenesis,
    StorePaths,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zeroize::Zeroize;

const MANIFEST_PROFILE: &str = "residiuum-development-file-product-bootstrap-v1";
const DEPLOYMENT_IDENTITY_PROFILE: &str = "residiuum-deployment-identity-v1";
const DEFAULT_VALIDITY: Duration = Duration::from_secs(CERT_MAX_LIFETIME_S);
const DEFAULT_RENEWAL_WINDOW: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// One named Heap requested by a product composition root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductHeapRequest {
    /// Canonical Heap name.
    pub name: String,
    /// Exact authority rights issued to this product.
    pub rights: Rights,
}

impl ProductHeapRequest {
    /// Construct a named Heap request with an explicit rights set.
    pub fn new(name: impl Into<String>, rights: Rights) -> Self {
        Self {
            name: name.into(),
            rights,
        }
    }
}

/// Local embedded product bootstrap configuration.
///
/// `credential_file` contains master seeds and is therefore created with mode
/// `0600` on Unix. The concrete repository is deliberately named development
/// file bootstrap; production key-provider adapters remain a separate gate.
#[derive(Debug, Clone)]
pub struct DevelopmentFileProductBootstrap {
    /// Existing physical deployment / store root.
    pub data_root: PathBuf,
    /// Authority root, separate from the deployment root.
    pub authority_root: PathBuf,
    /// Explicit owner-only authority credential file.
    pub credential_file: PathBuf,
    /// Stable product identity bound into the credential file.
    pub product_identity: String,
    /// Exact set of named Heaps owned by the product composition root.
    pub heaps: Vec<ProductHeapRequest>,
    /// Certificate lifetime; at most the frozen protocol maximum (90 days).
    pub certificate_validity: Duration,
    /// Reissue certificates this close to expiry.
    pub renewal_window: Duration,
}

impl DevelopmentFileProductBootstrap {
    /// Construct with the protocol maximum (90-day) certificate and a 7-day renewal window.
    pub fn new(
        data_root: impl Into<PathBuf>,
        authority_root: impl Into<PathBuf>,
        credential_file: impl Into<PathBuf>,
        product_identity: impl Into<String>,
        heaps: Vec<ProductHeapRequest>,
    ) -> Self {
        Self {
            data_root: data_root.into(),
            authority_root: authority_root.into(),
            credential_file: credential_file.into(),
            product_identity: product_identity.into(),
            heaps,
            certificate_validity: DEFAULT_VALIDITY,
            renewal_window: DEFAULT_RENEWAL_WINDOW,
        }
    }
}

/// How one Heap reached a usable capability during this startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductHeapDisposition {
    /// Heap authority and storage genesis were created now.
    Created,
    /// Existing authority and certificate were validated and loaded.
    Loaded,
    /// An interrupted genesis was validated and completed.
    Resumed,
    /// Existing authority was loaded and its certificate was renewed.
    Renewed,
}

/// One validated, non-serializable Heap capability returned to the composition root.
#[derive(Debug)]
pub struct ProductHeapCapability {
    /// Canonical Heap name.
    pub name: String,
    /// Immutable Heap identity.
    pub heap_id: HeapId,
    /// Capability validated against the anchored authority head.
    pub capability: HeapCap,
    /// Startup disposition.
    pub disposition: ProductHeapDisposition,
}

/// Validated authority outcome for one product deployment.
#[derive(Debug)]
pub struct ProductBootstrapResult {
    /// Stable physical deployment identity shared by every returned Heap.
    pub deployment_id: DeploymentId,
    /// Capabilities in the same order as the request.
    pub heaps: Vec<ProductHeapCapability>,
}

/// Provision missing named Heaps or load their existing validated capabilities.
///
/// This function is process-safe on Unix, fail-closed on manifest/configuration
/// drift, and restart-safe across every persisted boundary it owns. It never
/// manufactures a test capability: every result follows staged genesis,
/// anchored authority, signed HeapKey verification, authorization decision,
/// and kernel capability minting.
pub fn bootstrap_development_file_product(
    config: &DevelopmentFileProductBootstrap,
) -> Result<ProductBootstrapResult, AuthorityError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| AuthorityError::InvalidArgument(format!("system time: {e}")))?
        .as_secs();
    bootstrap_at(config, now)
}

fn bootstrap_at(
    config: &DevelopmentFileProductBootstrap,
    now: u64,
) -> Result<ProductBootstrapResult, AuthorityError> {
    let validated = ValidatedConfig::new(config)?;
    let _deployment_lock = BootstrapLock::acquire_path(
        &validated
            .data_root
            .join("meta")
            .join("product-bootstrap.lock"),
    )?;
    let _lock = BootstrapLock::acquire(&validated.credential_file)?;
    let deployment_id = load_or_create_deployment_identity(&validated.data_root)?;
    let mut manifest = match load_manifest(&validated.credential_file)? {
        Some(manifest) => {
            manifest.validate_config(&validated, deployment_id)?;
            manifest
        }
        None => {
            preflight_names(&validated)?;
            let manifest = Manifest::new(&validated, deployment_id)?;
            persist_manifest(&validated.credential_file, &manifest)?;
            manifest
        }
    };

    let mut results = Vec::with_capacity(manifest.heaps.len());
    for index in 0..manifest.heaps.len() {
        let mut created = false;
        let mut resumed = false;
        if manifest.heaps[index].prepared.is_none() {
            let entry = &manifest.heaps[index];
            let authority = authority_store(&validated, &manifest, entry)?;
            if authority.load_head()?.is_some() {
                return Err(AuthorityError::Refused(format!(
                    "heap '{}' has authority but no persisted prepared genesis",
                    entry.name
                )));
            }
            let prepared = prepare_genesis(GenesisRequest {
                authority_root: validated.authority_root.clone(),
                data_root: validated.data_root.clone(),
                deployment_id: manifest.deployment_id,
                heap_id: entry.heap_id,
                name: entry.name.clone(),
                creation_event_id: entry.creation_event_id,
                effective_at: now.max(1),
            })?;
            manifest.heaps[index].prepared = Some(PreparedRecord::from_prepared(&prepared));
            persist_manifest(&validated.credential_file, &manifest)?;
        }

        let entry = &manifest.heaps[index];
        let prepared = entry
            .prepared
            .as_ref()
            .expect("prepared established above")
            .to_prepared(&validated, manifest.deployment_id, entry);
        let provider = EphemeralMasterKeyProvider::from_seed(entry.master_seed);
        let store = authority_store(&validated, &manifest, entry)?;
        let had_head = store.load_head()?.is_some();
        let had_published = rebuild_heap_entry_from_chain(
            &HeapMetaLayout::new(&validated.data_root),
            &entry.heap_id,
        )
        .map_err(|e| AuthorityError::Provisioning(e.to_string()))?
        .is_some();
        if had_head && had_published {
            validate_established_heap(&validated, &manifest, entry, &prepared)?;
        } else {
            commit_prepared_genesis(&provider, &prepared)?;
            if !had_head {
                created = true;
            } else {
                resumed = true;
            }
        }

        let head = store
            .load_head()?
            .ok_or_else(|| AuthorityError::Refused("authority disappeared after commit".into()))?;
        validate_security_time(&head, now)?;
        let renew = certificate_needs_renewal(entry.certificate.as_deref(), &head, now, config)?;
        if renew {
            let issued = issue_heap_key(
                &store,
                &provider,
                IssueRequest {
                    holder_public_key: entry.holder_public_key,
                    rights: entry.rights,
                    not_before: now,
                    expires_at: now
                        .checked_add(config.certificate_validity.as_secs())
                        .ok_or_else(|| {
                            AuthorityError::InvalidArgument("certificate expiry overflow".into())
                        })?,
                },
            )?;
            manifest.heaps[index].certificate = Some(issued.cose_sign1);
            persist_manifest(&validated.credential_file, &manifest)?;
        }

        let entry = &manifest.heaps[index];
        let certificate = entry.certificate.as_deref().ok_or_else(|| {
            AuthorityError::Refused("certificate issuance was not persisted".into())
        })?;
        let capability = validate_and_mint(&head, certificate, now)?;
        let heap_id =
            HeapId::from_bytes(entry.heap_id).map_err(|e| AuthorityError::Heap(e.to_string()))?;
        let disposition = if created {
            ProductHeapDisposition::Created
        } else if resumed {
            ProductHeapDisposition::Resumed
        } else if renew {
            ProductHeapDisposition::Renewed
        } else {
            ProductHeapDisposition::Loaded
        };
        results.push(ProductHeapCapability {
            name: entry.name.clone(),
            heap_id,
            capability,
            disposition,
        });
    }

    let deployment_id = DeploymentId::from_bytes(manifest.deployment_id)
        .map_err(|e| AuthorityError::Heap(e.to_string()))?;
    Ok(ProductBootstrapResult {
        deployment_id,
        heaps: results,
    })
}

struct ValidatedConfig {
    data_root: PathBuf,
    authority_root: PathBuf,
    credential_file: PathBuf,
    product_identity: String,
    heaps: Vec<ProductHeapRequest>,
}

impl ValidatedConfig {
    fn new(config: &DevelopmentFileProductBootstrap) -> Result<Self, AuthorityError> {
        if config.product_identity.is_empty() || config.product_identity.len() > 256 {
            return Err(AuthorityError::InvalidArgument(
                "product identity must contain 1..=256 bytes".into(),
            ));
        }
        if config.heaps.is_empty() || config.heaps.len() > 64 {
            return Err(AuthorityError::InvalidArgument(
                "product must request 1..=64 heaps".into(),
            ));
        }
        if config.certificate_validity.is_zero()
            || config.certificate_validity > Duration::from_secs(CERT_MAX_LIFETIME_S)
            || config.renewal_window >= config.certificate_validity
        {
            return Err(AuthorityError::InvalidArgument(
                "invalid certificate validity or renewal window".into(),
            ));
        }
        let mut names = BTreeSet::new();
        for heap in &config.heaps {
            if heap.name.is_empty() || heap.name.len() > 255 || !names.insert(heap.name.clone()) {
                return Err(AuthorityError::InvalidArgument(
                    "heap names must be unique and contain 1..=255 bytes".into(),
                ));
            }
            if heap.rights == Rights::EMPTY {
                return Err(AuthorityError::InvalidArgument(
                    "heap rights must not be empty".into(),
                ));
            }
        }
        if !config.data_root.is_dir() {
            return Err(AuthorityError::InvalidArgument(
                "data root must be an existing deployment directory".into(),
            ));
        }
        refuse_symlink(&config.data_root, "data root")?;
        validate_deployment_marker(&config.data_root)?;
        fs::create_dir_all(&config.authority_root)?;
        refuse_symlink(&config.authority_root, "authority root")?;
        let credential_parent = config.credential_file.parent().ok_or_else(|| {
            AuthorityError::InvalidArgument("credential file needs a parent".into())
        })?;
        fs::create_dir_all(credential_parent)?;
        refuse_symlink(credential_parent, "credential parent")?;
        refuse_symlink_if_exists(&config.credential_file, "credential file")?;
        Ok(Self {
            data_root: fs::canonicalize(&config.data_root)?,
            authority_root: fs::canonicalize(&config.authority_root)?,
            credential_file: canonical_child_path(&config.credential_file)?,
            product_identity: config.product_identity.clone(),
            heaps: config.heaps.clone(),
        })
    }
}

fn validate_deployment_marker(data_root: &Path) -> Result<(), AuthorityError> {
    let paths = StorePaths::new(data_root);
    if !paths.looks_like_store()
        || !paths.meta_file().is_file()
        || !paths.store_descriptor_file().is_file()
    {
        return Err(AuthorityError::InvalidArgument(
            "data root is not an initialized Residiuum deployment".into(),
        ));
    }
    let store_id = fs::read(paths.store_id_file())?;
    if store_id.len() != 16 || store_id.iter().all(|byte| *byte == 0) {
        return Err(AuthorityError::Refused(
            "Residiuum deployment store id is malformed".into(),
        ));
    }
    Ok(())
}

struct Manifest {
    product_identity: String,
    data_root: String,
    authority_root: String,
    deployment_id: [u8; 16],
    heaps: Vec<ManifestHeap>,
}

struct ManifestHeap {
    name: String,
    rights: Rights,
    heap_id: [u8; 16],
    creation_event_id: [u8; 16],
    master_seed: [u8; 32],
    holder_public_key: [u8; 32],
    prepared: Option<PreparedRecord>,
    certificate: Option<Vec<u8>>,
}

impl Drop for ManifestHeap {
    fn drop(&mut self) {
        self.master_seed.zeroize();
    }
}

struct PreparedRecord {
    staging_id: [u8; 16],
    descriptor_hash: [u8; 32],
    root_event_id: [u8; 16],
    effective_at: u64,
}

impl PreparedRecord {
    fn from_prepared(prepared: &PreparedGenesis) -> Self {
        Self {
            staging_id: prepared.staged.staging_id,
            descriptor_hash: prepared.staged.descriptor_hash,
            root_event_id: prepared.root_event_id,
            effective_at: prepared.request.effective_at,
        }
    }

    fn to_prepared(
        &self,
        config: &ValidatedConfig,
        deployment_id: [u8; 16],
        heap: &ManifestHeap,
    ) -> PreparedGenesis {
        PreparedGenesis {
            request: GenesisRequest {
                authority_root: config.authority_root.clone(),
                data_root: config.data_root.clone(),
                deployment_id,
                heap_id: heap.heap_id,
                name: heap.name.clone(),
                creation_event_id: heap.creation_event_id,
                effective_at: self.effective_at,
            },
            staged: StagedGenesis {
                staging_id: self.staging_id,
                heap_id: heap.heap_id,
                descriptor_hash: self.descriptor_hash,
                name: heap.name.clone(),
            },
            root_event_id: self.root_event_id,
        }
    }
}

impl Manifest {
    fn new(config: &ValidatedConfig, deployment_id: [u8; 16]) -> Result<Self, AuthorityError> {
        let mut heaps = Vec::with_capacity(config.heaps.len());
        for requested in &config.heaps {
            let mut master_seed = [0u8; 32];
            getrandom::fill(&mut master_seed).map_err(|e| AuthorityError::Crypto(e.to_string()))?;
            let holder_seed = random_bytes32()?;
            let holder = ed25519_dalek::SigningKey::from_bytes(&holder_seed);
            let holder_public_key = holder.verifying_key().to_bytes();
            let mut holder_seed = holder_seed;
            holder_seed.zeroize();
            heaps.push(ManifestHeap {
                name: requested.name.clone(),
                rights: requested.rights,
                heap_id: random_id16()?,
                creation_event_id: random_id16()?,
                master_seed,
                holder_public_key,
                prepared: None,
                certificate: None,
            });
        }
        Ok(Self {
            product_identity: config.product_identity.clone(),
            data_root: path_string(&config.data_root)?,
            authority_root: path_string(&config.authority_root)?,
            deployment_id,
            heaps,
        })
    }

    fn validate_config(
        &self,
        config: &ValidatedConfig,
        deployment_id: [u8; 16],
    ) -> Result<(), AuthorityError> {
        DeploymentId::from_bytes(self.deployment_id)
            .map_err(|e| AuthorityError::Refused(format!("stored deployment identity: {e}")))?;
        if self.product_identity != config.product_identity
            || self.data_root != path_string(&config.data_root)?
            || self.authority_root != path_string(&config.authority_root)?
            || self.deployment_id != deployment_id
            || self.heaps.len() != config.heaps.len()
        {
            return Err(AuthorityError::Refused(
                "product bootstrap configuration does not match credential file".into(),
            ));
        }
        for (stored, requested) in self.heaps.iter().zip(&config.heaps) {
            HeapId::from_bytes(stored.heap_id)
                .map_err(|e| AuthorityError::Refused(format!("stored Heap identity: {e}")))?;
            validate_uuid_v4(stored.creation_event_id, "creation event id")?;
            if stored.master_seed == [0u8; 32] {
                return Err(AuthorityError::Refused("stored master seed is zero".into()));
            }
            ed25519_dalek::VerifyingKey::from_bytes(&stored.holder_public_key)
                .map_err(|e| AuthorityError::Refused(format!("stored holder key: {e}")))?;
            if let Some(prepared) = &stored.prepared {
                if prepared.staging_id == [0u8; 16] {
                    return Err(AuthorityError::Refused("stored staging id is zero".into()));
                }
                validate_uuid_v4(prepared.root_event_id, "root event id")?;
                if prepared.descriptor_hash == [0u8; 32] || prepared.effective_at == 0 {
                    return Err(AuthorityError::Refused(
                        "stored prepared genesis has zero authority fields".into(),
                    ));
                }
            }
            if stored.name != requested.name || stored.rights != requested.rights {
                return Err(AuthorityError::Refused(
                    "product Heap names/order/rights do not match credential file".into(),
                ));
            }
        }
        Ok(())
    }
}

fn preflight_names(config: &ValidatedConfig) -> Result<(), AuthorityError> {
    let layout = HeapMetaLayout::new(&config.data_root);
    let catalog = rebuild_and_persist_all_catalogs(&layout)
        .map_err(|e| AuthorityError::Provisioning(e.to_string()))?
        .0;
    for existing in catalog {
        if config.heaps.iter().any(|requested| {
            requested.name == existing.name || existing.aliases.contains(&requested.name)
        }) {
            return Err(AuthorityError::Refused(format!(
                "named Heap '{}' already exists without this product credential",
                existing.name
            )));
        }
    }
    Ok(())
}

fn authority_store(
    config: &ValidatedConfig,
    manifest: &Manifest,
    heap: &ManifestHeap,
) -> Result<MasterAuthorityStore, AuthorityError> {
    let deployment_dir = config
        .authority_root
        .join(hex::encode(manifest.deployment_id));
    let heap_dir = deployment_dir.join(hex::encode(heap.heap_id));
    refuse_symlink_if_exists(&deployment_dir, "authority deployment directory")?;
    refuse_symlink_if_exists(&heap_dir, "authority Heap directory")?;
    MasterAuthorityStore::open(AuthorityPaths::new(
        &config.authority_root,
        &manifest.deployment_id,
        &heap.heap_id,
    ))
}

fn certificate_needs_renewal(
    certificate: Option<&[u8]>,
    head: &AuthorityHead,
    now: u64,
    config: &DevelopmentFileProductBootstrap,
) -> Result<bool, AuthorityError> {
    let Some(certificate) = certificate else {
        return Ok(true);
    };
    let verified = verify_certificate_against_head(certificate, head)
        .map_err(|e| AuthorityError::Heap(format!("stored certificate: {e}")))?;
    let renewal_at = verified
        .expires_at
        .saturating_sub(config.renewal_window.as_secs());
    Ok(now >= renewal_at)
}

fn validate_and_mint(
    head: &AuthorityHead,
    certificate: &[u8],
    now: u64,
) -> Result<HeapCap, AuthorityError> {
    let snapshot = snapshot_from_head(head)?;
    let trusted = validate_security_time(head, now)?;
    let verified = verify_certificate_against_head(certificate, head)?;
    let operation = Operation::all()
        .iter()
        .find_map(|row| {
            if row.id <= 3 || Operation::status(row.id).ok()? != OperationStatus::Active {
                return None;
            }
            let required = Operation::required_rights(row.id).ok()?;
            verified.rights.contains(required).then_some(row.id)
        })
        .ok_or_else(|| {
            AuthorityError::Refused("certificate authorizes no active Heap operation".into())
        })?;
    match decide(
        &snapshot,
        &verified,
        &OperationDescriptor {
            operation_id: operation,
            request_bytes: 0,
        },
        trusted,
    ) {
        AuthorizationDecision::Allow => {}
        AuthorizationDecision::Deny(cause) => {
            return Err(AuthorityError::Refused(format!(
                "certificate rejected by resident authority: {cause:?}"
            )))
        }
    }
    mint_capability(Arc::new(HeapSlot::new(snapshot)), &verified, trusted)
        .map_err(|e| AuthorityError::Heap(e.to_string()))
}

fn validate_established_heap(
    config: &ValidatedConfig,
    manifest: &Manifest,
    heap: &ManifestHeap,
    prepared: &PreparedGenesis,
) -> Result<(), AuthorityError> {
    let store = authority_store(config, manifest, heap)?;
    let head = store
        .load_head()?
        .ok_or_else(|| AuthorityError::Refused("established Heap lost authority head".into()))?;
    if head.deployment_id != manifest.deployment_id
        || head.heap_id != heap.heap_id
        || head.storage_genesis_hash != prepared.staged.descriptor_hash
    {
        return Err(AuthorityError::Refused(format!(
            "established Heap '{}' conflicts with persisted product authority",
            heap.name
        )));
    }
    let published =
        rebuild_heap_entry_from_chain(&HeapMetaLayout::new(&config.data_root), &heap.heap_id)
            .map_err(|e| AuthorityError::Provisioning(e.to_string()))?
            .ok_or_else(|| AuthorityError::Refused("established Heap is not published".into()))?;
    if published.origin_deployment_id != manifest.deployment_id
        || published.descriptor_hash != head.current_descriptor_hash
        || (published.name != heap.name && !published.aliases.contains(&heap.name))
    {
        return Err(AuthorityError::Refused(format!(
            "published Heap '{}' conflicts with persisted product identity",
            heap.name
        )));
    }
    Ok(())
}

fn verify_certificate_against_head(
    certificate: &[u8],
    head: &AuthorityHead,
) -> Result<VerifiedCertificate, AuthorityError> {
    let inspected = inspect_certificate(certificate)
        .map_err(|e| AuthorityError::Heap(format!("certificate structure: {e}")))?;
    let public_key = if inspected.authority_generation.get() == head.master_generation {
        head.master_public_key
    } else if head.previous_generation == Some(inspected.authority_generation.get()) {
        head.previous_public_key.ok_or_else(|| {
            AuthorityError::Refused("previous authority generation has no public key".into())
        })?
    } else {
        return Err(AuthorityError::Refused(
            "certificate authority generation is not resident".into(),
        ));
    };
    verify_certificate(certificate, &public_key).map_err(|e| AuthorityError::Heap(e.to_string()))
}

fn validate_security_time(
    head: &AuthorityHead,
    now: u64,
) -> Result<residiuum_heap::TrustedInstant, AuthorityError> {
    let mut floor = SecurityTimeFloor::new(head.trusted_time_floor);
    floor
        .observe(now)
        .map_err(|e| AuthorityError::Heap(e.to_string()))
}

fn load_or_create_deployment_identity(data_root: &Path) -> Result<[u8; 16], AuthorityError> {
    let layout = HeapMetaLayout::new(data_root);
    let catalog = rebuild_and_persist_all_catalogs(&layout)
        .map_err(|e| AuthorityError::Provisioning(e.to_string()))?
        .0;
    let existing: BTreeSet<[u8; 16]> = catalog
        .iter()
        .map(|entry| entry.origin_deployment_id)
        .collect();
    if existing.len() > 1 {
        return Err(AuthorityError::Refused(
            "published Heaps disagree on physical deployment identity".into(),
        ));
    }
    let path = layout.meta_dir().join("deployment-identity.v1.cbor");
    refuse_symlink_if_exists(&path, "deployment identity file")?;
    if path.is_file() {
        let payload = decode_slot_file(&fs::read(&path)?)?;
        let map = decode_deterministic_uint_map(&payload)
            .map_err(|e| AuthorityError::Crypto(format!("deployment identity: {e}")))?;
        let mut profile = None;
        let mut deployment = None;
        for (key, value) in map {
            match key {
                1 => profile = Some(expect_text(value, "deployment identity profile")?),
                2 => deployment = Some(expect_b16(value, "deployment id")?),
                _ => {
                    return Err(AuthorityError::Crypto(
                        "unknown deployment identity field".into(),
                    ))
                }
            }
        }
        if profile.as_deref() != Some(DEPLOYMENT_IDENTITY_PROFILE) {
            return Err(AuthorityError::Refused(
                "unsupported deployment identity profile".into(),
            ));
        }
        let deployment =
            deployment.ok_or_else(|| AuthorityError::Crypto("missing deployment id".into()))?;
        DeploymentId::from_bytes(deployment)
            .map_err(|e| AuthorityError::Refused(format!("deployment identity file: {e}")))?;
        if existing
            .iter()
            .next()
            .is_some_and(|published| published != &deployment)
        {
            return Err(AuthorityError::Refused(
                "deployment identity file conflicts with published Heap authority".into(),
            ));
        }
        return Ok(deployment);
    }
    let deployment = match existing.iter().next().copied() {
        Some(deployment) => {
            DeploymentId::from_bytes(deployment).map_err(|e| {
                AuthorityError::Refused(format!("published deployment identity: {e}"))
            })?;
            deployment
        }
        None => random_id16()?,
    };
    let payload = encode_deterministic_uint_map(&[
        (1, CborValue::Text(DEPLOYMENT_IDENTITY_PROFILE.into())),
        (2, CborValue::Bytes(deployment.to_vec())),
    ])
    .map_err(|e| AuthorityError::Crypto(e.to_string()))?;
    crate::slot::write_atomic(&path, &encode_slot_file(&payload)?)?;
    Ok(deployment)
}

fn snapshot_from_head(head: &AuthorityHead) -> Result<HeapSecuritySnapshot, AuthorityError> {
    Ok(HeapSecuritySnapshot {
        deployment_id: DeploymentId::from_bytes(head.deployment_id)
            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        heap_id: HeapId::from_bytes(head.heap_id)
            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        authority_epoch: AuthorityEpoch::new(head.authority_epoch)
            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        authority_generation: AuthorityGeneration::new(head.master_generation)
            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        previous_generation: head
            .previous_generation
            .map(AuthorityGeneration::new)
            .transpose()
            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        grace_deadline_unix_s: head.grace_deadline,
        master_public_key: head.master_public_key,
        previous_master_public_key: head.previous_public_key,
        security_revision: SecurityRevision::new(head.security_revision)
            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        authority_chain_head_hash: head.authority_chain_head_hash,
        administrative_state: head.heap_state,
        blacklist: head.blacklist.clone(),
        policy_rights_ceiling: Some(
            Rights::from_bits_effective(head.access_policy.allowed_rights_mask)
                .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        ),
    })
}

fn persist_manifest(path: &Path, manifest: &Manifest) -> Result<(), AuthorityError> {
    let mut payload = encode_manifest(manifest)?;
    let wrapped = encode_slot_file(&payload);
    payload.zeroize();
    let mut wrapped = wrapped?;
    let result = write_secret_atomic(path, &wrapped);
    wrapped.zeroize();
    result
}

fn load_manifest(path: &Path) -> Result<Option<Manifest>, AuthorityError> {
    if !path.exists() {
        return Ok(None);
    }
    validate_secret_permissions(path)?;
    let mut wrapped = fs::read(path)?;
    let payload = decode_slot_file(&wrapped);
    wrapped.zeroize();
    let mut payload = payload?;
    let result = decode_manifest(&payload).map(Some);
    payload.zeroize();
    result
}

fn encode_manifest(manifest: &Manifest) -> Result<Vec<u8>, AuthorityError> {
    let heaps = manifest
        .heaps
        .iter()
        .map(|heap| {
            let prepared = match &heap.prepared {
                None => CborValue::Null,
                Some(p) => CborValue::Map(vec![
                    (1, CborValue::Bytes(p.staging_id.to_vec())),
                    (2, CborValue::Bytes(p.descriptor_hash.to_vec())),
                    (3, CborValue::Bytes(p.root_event_id.to_vec())),
                    (4, CborValue::Uint(p.effective_at)),
                ]),
            };
            let certificate = heap
                .certificate
                .as_ref()
                .map_or(CborValue::Null, |b| CborValue::Bytes(b.clone()));
            CborValue::Map(vec![
                (1, CborValue::Text(heap.name.clone())),
                (2, CborValue::Uint(heap.rights.bits())),
                (3, CborValue::Bytes(heap.heap_id.to_vec())),
                (4, CborValue::Bytes(heap.creation_event_id.to_vec())),
                (5, CborValue::Bytes(heap.master_seed.to_vec())),
                (6, CborValue::Bytes(heap.holder_public_key.to_vec())),
                (7, prepared),
                (8, certificate),
            ])
        })
        .collect();
    encode_deterministic_uint_map(&[
        (1, CborValue::Text(MANIFEST_PROFILE.into())),
        (2, CborValue::Text(manifest.product_identity.clone())),
        (3, CborValue::Text(manifest.data_root.clone())),
        (4, CborValue::Text(manifest.authority_root.clone())),
        (5, CborValue::Bytes(manifest.deployment_id.to_vec())),
        (6, CborValue::Array(heaps)),
    ])
    .map_err(|e| AuthorityError::Crypto(e.to_string()))
}

fn decode_manifest(bytes: &[u8]) -> Result<Manifest, AuthorityError> {
    let map = decode_deterministic_uint_map(bytes)
        .map_err(|e| AuthorityError::Crypto(format!("product manifest: {e}")))?;
    let mut profile = None;
    let mut product = None;
    let mut data_root = None;
    let mut authority_root = None;
    let mut deployment = None;
    let mut heaps = None;
    for (key, value) in map {
        match key {
            1 => profile = Some(expect_text(value, "profile")?),
            2 => product = Some(expect_text(value, "product identity")?),
            3 => data_root = Some(expect_text(value, "data root")?),
            4 => authority_root = Some(expect_text(value, "authority root")?),
            5 => deployment = Some(expect_b16(value, "deployment id")?),
            6 => heaps = Some(decode_heaps(value)?),
            _ => {
                return Err(AuthorityError::Crypto(
                    "unknown product manifest field".into(),
                ))
            }
        }
    }
    if profile.as_deref() != Some(MANIFEST_PROFILE) {
        return Err(AuthorityError::Refused(
            "unsupported product manifest profile".into(),
        ));
    }
    Ok(Manifest {
        product_identity: product
            .ok_or_else(|| AuthorityError::Crypto("missing product identity".into()))?,
        data_root: data_root.ok_or_else(|| AuthorityError::Crypto("missing data root".into()))?,
        authority_root: authority_root
            .ok_or_else(|| AuthorityError::Crypto("missing authority root".into()))?,
        deployment_id: deployment
            .ok_or_else(|| AuthorityError::Crypto("missing deployment id".into()))?,
        heaps: heaps.ok_or_else(|| AuthorityError::Crypto("missing heaps".into()))?,
    })
}

fn decode_heaps(value: CborValue) -> Result<Vec<ManifestHeap>, AuthorityError> {
    let CborValue::Array(items) = value else {
        return Err(AuthorityError::Crypto("heaps must be an array".into()));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let CborValue::Map(fields) = item else {
            return Err(AuthorityError::Crypto("heap entry must be a map".into()));
        };
        let mut name = None;
        let mut rights = None;
        let mut heap_id = None;
        let mut creation = None;
        let mut master = None;
        let mut holder = None;
        let mut prepared = None;
        let mut certificate = None;
        for (key, value) in fields {
            match key {
                1 => name = Some(expect_text(value, "heap name")?),
                2 => {
                    rights = Some(
                        Rights::from_bits_certificate(expect_uint(value, "rights")?)
                            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
                    )
                }
                3 => heap_id = Some(expect_b16(value, "heap id")?),
                4 => creation = Some(expect_b16(value, "creation event id")?),
                5 => master = Some(expect_b32(value, "master seed")?),
                6 => holder = Some(expect_b32(value, "holder public key")?),
                7 => prepared = Some(decode_prepared(value)?),
                8 => {
                    certificate = Some(match value {
                        CborValue::Null => None,
                        CborValue::Bytes(bytes) => Some(bytes),
                        _ => return Err(AuthorityError::Crypto("certificate field".into())),
                    })
                }
                _ => return Err(AuthorityError::Crypto("unknown heap manifest field".into())),
            }
        }
        out.push(ManifestHeap {
            name: name.ok_or_else(|| AuthorityError::Crypto("missing heap name".into()))?,
            rights: rights.ok_or_else(|| AuthorityError::Crypto("missing rights".into()))?,
            heap_id: heap_id.ok_or_else(|| AuthorityError::Crypto("missing heap id".into()))?,
            creation_event_id: creation
                .ok_or_else(|| AuthorityError::Crypto("missing creation id".into()))?,
            master_seed: master
                .ok_or_else(|| AuthorityError::Crypto("missing master seed".into()))?,
            holder_public_key: holder
                .ok_or_else(|| AuthorityError::Crypto("missing holder key".into()))?,
            prepared: prepared
                .ok_or_else(|| AuthorityError::Crypto("missing prepared field".into()))?,
            certificate: certificate
                .ok_or_else(|| AuthorityError::Crypto("missing certificate field".into()))?,
        });
    }
    Ok(out)
}

fn decode_prepared(value: CborValue) -> Result<Option<PreparedRecord>, AuthorityError> {
    let CborValue::Map(fields) = value else {
        return match value {
            CborValue::Null => Ok(None),
            _ => Err(AuthorityError::Crypto("prepared field".into())),
        };
    };
    let mut staging = None;
    let mut hash = None;
    let mut root = None;
    let mut effective = None;
    for (key, value) in fields {
        match key {
            1 => staging = Some(expect_b16(value, "staging id")?),
            2 => hash = Some(expect_b32(value, "descriptor hash")?),
            3 => root = Some(expect_b16(value, "root event id")?),
            4 => effective = Some(expect_uint(value, "effective at")?),
            _ => return Err(AuthorityError::Crypto("unknown prepared field".into())),
        }
    }
    Ok(Some(PreparedRecord {
        staging_id: staging.ok_or_else(|| AuthorityError::Crypto("missing staging id".into()))?,
        descriptor_hash: hash
            .ok_or_else(|| AuthorityError::Crypto("missing descriptor hash".into()))?,
        root_event_id: root
            .ok_or_else(|| AuthorityError::Crypto("missing root event id".into()))?,
        effective_at: effective
            .ok_or_else(|| AuthorityError::Crypto("missing effective at".into()))?,
    }))
}

fn expect_text(value: CborValue, field: &str) -> Result<String, AuthorityError> {
    match value {
        CborValue::Text(value) => Ok(value),
        _ => Err(AuthorityError::Crypto(format!("{field} must be text"))),
    }
}

fn expect_uint(value: CborValue, field: &str) -> Result<u64, AuthorityError> {
    match value {
        CborValue::Uint(value) => Ok(value),
        _ => Err(AuthorityError::Crypto(format!("{field} must be uint"))),
    }
}

fn expect_b16(value: CborValue, field: &str) -> Result<[u8; 16], AuthorityError> {
    match value {
        CborValue::Bytes(value) if value.len() == 16 => {
            let mut out = [0u8; 16];
            out.copy_from_slice(&value);
            Ok(out)
        }
        _ => Err(AuthorityError::Crypto(format!("{field} must be bstr16"))),
    }
}

fn expect_b32(value: CborValue, field: &str) -> Result<[u8; 32], AuthorityError> {
    match value {
        CborValue::Bytes(value) if value.len() == 32 => {
            let mut out = [0u8; 32];
            out.copy_from_slice(&value);
            Ok(out)
        }
        _ => Err(AuthorityError::Crypto(format!("{field} must be bstr32"))),
    }
}

fn write_secret_atomic(path: &Path, bytes: &[u8]) -> Result<(), AuthorityError> {
    refuse_symlink_if_exists(path, "credential file")?;
    let parent = path
        .parent()
        .ok_or_else(|| AuthorityError::InvalidArgument("credential file needs a parent".into()))?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let mut digest = Sha256::new();
    digest.update(path.as_os_str().as_encoded_bytes());
    digest.update(std::process::id().to_le_bytes());
    digest.update(random_id16()?);
    let tmp = parent.join(format!(
        ".residiuum-bootstrap-{}.tmp",
        hex::encode(&digest.finalize()[..8])
    ));
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| -> Result<(), AuthorityError> {
        let mut file = options.open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&tmp, path)?;
        File::open(parent)?.sync_all()?;
        validate_secret_permissions(path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn validate_secret_permissions(path: &Path) -> Result<(), AuthorityError> {
    refuse_symlink(path, "credential file")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(path)?.permissions().mode() & 0o777;
        if mode != 0o600 {
            return Err(AuthorityError::Refused(format!(
                "credential file permissions must be 0600, found {mode:04o}"
            )));
        }
    }
    Ok(())
}

struct BootstrapLock {
    file: File,
}

impl BootstrapLock {
    fn acquire(credential_file: &Path) -> Result<Self, AuthorityError> {
        let lock_path = credential_file.with_extension("bootstrap.lock");
        Self::acquire_path(&lock_path)
    }

    fn acquire_path(lock_path: &Path) -> Result<Self, AuthorityError> {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)?;
        }
        refuse_symlink_if_exists(lock_path, "bootstrap lock")?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        try_lock_exclusive(&file).map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                AuthorityError::Refused("product bootstrap already in progress".into())
            } else {
                AuthorityError::Io(e)
            }
        })?;
        Ok(Self { file })
    }
}

impl Drop for BootstrapLock {
    fn drop(&mut self) {
        let _ = unlock_exclusive(&self.file);
    }
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn unlock_exclusive(file: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    const LOCK_UN: i32 = 8;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_UN) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}

#[cfg(not(unix))]
fn try_lock_exclusive(_file: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "product bootstrap locking is not implemented on this platform",
    ))
}

#[cfg(not(unix))]
fn unlock_exclusive(_file: &File) -> std::io::Result<()> {
    Ok(())
}

fn refuse_symlink_if_exists(path: &Path, label: &str) -> Result<(), AuthorityError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(AuthorityError::Refused(format!(
            "{label} must not be a symlink"
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn refuse_symlink(path: &Path, label: &str) -> Result<(), AuthorityError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        Err(AuthorityError::Refused(format!(
            "{label} must not be a symlink"
        )))
    } else {
        Ok(())
    }
}

fn canonical_child_path(path: &Path) -> Result<PathBuf, AuthorityError> {
    let name = path
        .file_name()
        .ok_or_else(|| AuthorityError::InvalidArgument("credential file needs a name".into()))?;
    let parent = path
        .parent()
        .ok_or_else(|| AuthorityError::InvalidArgument("credential file needs a parent".into()))?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    Ok(fs::canonicalize(parent)?.join(name))
}

fn path_string(path: &Path) -> Result<String, AuthorityError> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        AuthorityError::InvalidArgument("bootstrap paths must be valid UTF-8".into())
    })
}

fn random_id16() -> Result<[u8; 16], AuthorityError> {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|e| AuthorityError::Crypto(e.to_string()))?;
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    Ok(id)
}

fn validate_uuid_v4(id: [u8; 16], field: &str) -> Result<(), AuthorityError> {
    if id == [0u8; 16] || id[6] & 0xf0 != 0x40 || id[8] & 0xc0 != 0x80 {
        Err(AuthorityError::Refused(format!(
            "stored {field} is not an RFC UUIDv4"
        )))
    } else {
        Ok(())
    }
}

fn random_bytes32() -> Result<[u8; 32], AuthorityError> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|e| AuthorityError::Crypto(e.to_string()))?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::MasterKeyProvider;
    use residiuum_format::encode_deterministic_uint_map;
    use tempfile::TempDir;

    fn rights() -> Rights {
        Rights::from_bits_certificate(
            Rights::READ.bits()
                | Rights::WRITE.bits()
                | Rights::INDEX_ADMIN.bits()
                | Rights::HEAP_ADMIN.bits(),
        )
        .unwrap()
    }

    fn fixture(temp: &TempDir) -> DevelopmentFileProductBootstrap {
        let data_root = temp.path().join("store");
        drop(residiuum_store::StoreHost::create(&data_root).unwrap());
        DevelopmentFileProductBootstrap::new(
            &data_root,
            temp.path().join("authority"),
            temp.path().join("credentials/product.v1.cbor"),
            "gremlin-desktop",
            vec![
                ProductHeapRequest::new("tinker", rights()),
                ProductHeapRequest::new("gremlin", rights()),
            ],
        )
    }

    #[test]
    fn creates_then_loads_two_heaps_under_one_deployment() {
        let temp = TempDir::new().unwrap();
        let config = fixture(&temp);
        let created = bootstrap_at(&config, 1_800_000_000).unwrap();
        assert_eq!(created.heaps.len(), 2);
        assert!(created
            .heaps
            .iter()
            .all(|heap| heap.disposition == ProductHeapDisposition::Created));
        assert_ne!(created.heaps[0].heap_id, created.heaps[1].heap_id);
        assert!(created.heaps.iter().all(|heap| {
            heap.capability.deployment_id() == created.deployment_id
                && heap.capability.heap_id() == heap.heap_id
                && heap.capability.rights() == rights()
        }));
        let ids: Vec<_> = created.heaps.iter().map(|heap| heap.heap_id).collect();
        let deployment = created.deployment_id;
        drop(created);

        let loaded = bootstrap_at(&config, 1_800_000_001).unwrap();
        assert_eq!(loaded.deployment_id, deployment);
        assert_eq!(
            loaded
                .heaps
                .iter()
                .map(|heap| heap.heap_id)
                .collect::<Vec<_>>(),
            ids
        );
        assert!(loaded
            .heaps
            .iter()
            .all(|heap| heap.disposition == ProductHeapDisposition::Loaded));
    }

    #[test]
    fn renews_before_expiry_without_changing_heap_identity() {
        let temp = TempDir::new().unwrap();
        let mut config = fixture(&temp);
        config.certificate_validity = Duration::from_secs(100);
        config.renewal_window = Duration::from_secs(10);
        let first = bootstrap_at(&config, 1_800_000_000).unwrap();
        let ids: Vec<_> = first.heaps.iter().map(|heap| heap.heap_id).collect();
        drop(first);

        let renewed = bootstrap_at(&config, 1_800_000_091).unwrap();
        assert_eq!(
            renewed
                .heaps
                .iter()
                .map(|heap| heap.heap_id)
                .collect::<Vec<_>>(),
            ids
        );
        assert!(renewed
            .heaps
            .iter()
            .all(|heap| heap.disposition == ProductHeapDisposition::Renewed));
    }

    #[test]
    fn separate_products_share_the_physical_deployment_identity() {
        let temp = TempDir::new().unwrap();
        let first_config = fixture(&temp);
        let first = bootstrap_at(&first_config, 1_800_000_000).unwrap();

        let second_config = DevelopmentFileProductBootstrap::new(
            temp.path().join("store"),
            temp.path().join("authority"),
            temp.path().join("credentials/telemetry.v1.cbor"),
            "ringtail",
            vec![ProductHeapRequest::new("telemetry", rights())],
        );
        let second = bootstrap_at(&second_config, 1_800_000_001).unwrap();
        assert_eq!(first.deployment_id, second.deployment_id);
        assert_eq!(
            second.heaps[0].capability.deployment_id(),
            first.deployment_id
        );
    }

    #[test]
    fn restart_accepts_a_previous_generation_certificate_during_rotation_grace() {
        let temp = TempDir::new().unwrap();
        let config = fixture(&temp);
        let first = bootstrap_at(&config, 1_800_000_000).unwrap();
        let manifest = load_manifest(&config.credential_file).unwrap().unwrap();
        let entry = &manifest.heaps[0];
        let store =
            authority_store(&ValidatedConfig::new(&config).unwrap(), &manifest, entry).unwrap();
        let mut head = store.load_head().unwrap().unwrap();
        let old_public_key = head.master_public_key;
        let new_master = EphemeralMasterKeyProvider::generate().unwrap();
        head.previous_generation = Some(head.master_generation);
        head.previous_public_key = Some(old_public_key);
        head.grace_deadline = Some(1_800_001_000);
        head.master_generation += 1;
        head.master_public_key = new_master.public_key();
        commit_test_mutation(&store, &mut head);
        drop(manifest);
        drop(first);

        let restarted = bootstrap_at(&config, 1_800_000_001).unwrap();
        assert!(restarted
            .heaps
            .iter()
            .all(|heap| heap.disposition == ProductHeapDisposition::Loaded));
    }

    #[test]
    fn restart_refuses_authority_descriptor_split_brain() {
        let temp = TempDir::new().unwrap();
        let config = fixture(&temp);
        bootstrap_at(&config, 1_800_000_000).unwrap();
        let manifest = load_manifest(&config.credential_file).unwrap().unwrap();
        let entry = &manifest.heaps[0];
        let store =
            authority_store(&ValidatedConfig::new(&config).unwrap(), &manifest, entry).unwrap();
        let mut head = store.load_head().unwrap().unwrap();
        head.current_descriptor_hash = [0xabu8; 32];
        commit_test_mutation(&store, &mut head);
        drop(manifest);

        assert!(matches!(
            bootstrap_at(&config, 1_800_000_001),
            Err(AuthorityError::Refused(_))
        ));
    }

    #[test]
    fn clock_rollback_refuses_before_reissuing_credentials() {
        let temp = TempDir::new().unwrap();
        let mut config = fixture(&temp);
        config.certificate_validity = Duration::from_secs(100);
        config.renewal_window = Duration::from_secs(10);
        bootstrap_at(&config, 1_800_000_000).unwrap();
        let before = fs::read(&config.credential_file).unwrap();

        assert!(bootstrap_at(&config, 1_799_999_999).is_err());
        assert_eq!(fs::read(&config.credential_file).unwrap(), before);
    }

    fn commit_test_mutation(store: &MasterAuthorityStore, head: &mut AuthorityHead) {
        let previous = head.authority_chain_head_hash;
        let body = b"test-master-rotation";
        head.authority_revision += 1;
        head.security_revision += 1;
        head.file_sequence += 1;
        let event = encode_deterministic_uint_map(&[
            (1, CborValue::Uint(1)),
            (2, CborValue::Uint(2)),
            (3, CborValue::Bytes(body.to_vec())),
            (4, CborValue::Bytes(previous.to_vec())),
        ])
        .unwrap();
        head.authority_chain_head_hash = crate::slot::sha256(&event);
        store.commit_head(head, 2, body, previous).unwrap();
    }

    #[test]
    fn refuses_product_or_rights_drift() {
        let temp = TempDir::new().unwrap();
        let config = fixture(&temp);
        bootstrap_at(&config, 1_800_000_000).unwrap();

        let mut wrong_product = config.clone();
        wrong_product.product_identity = "different-product".into();
        assert!(matches!(
            bootstrap_at(&wrong_product, 1_800_000_001),
            Err(AuthorityError::Refused(_))
        ));

        let mut wrong_rights = config.clone();
        wrong_rights.heaps[0].rights = Rights::READ;
        assert!(matches!(
            bootstrap_at(&wrong_rights, 1_800_000_001),
            Err(AuthorityError::Refused(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn credential_file_is_owner_only_and_broad_permissions_fail_closed() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let config = fixture(&temp);
        bootstrap_at(&config, 1_800_000_000).unwrap();
        assert_eq!(
            fs::metadata(&config.credential_file)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        fs::set_permissions(&config.credential_file, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            bootstrap_at(&config, 1_800_000_001),
            Err(AuthorityError::Refused(_))
        ));
    }
}
