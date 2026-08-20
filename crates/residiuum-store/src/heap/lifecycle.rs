//! Heap lifecycle, purge receipts, and backup/restore gates (`HEAP_SPEC` §6 / §9 / HP-009).
//!
//! Accept focus:
//! - administrative transitions on a resident [`HeapSlot`];
//! - verifiable purge receipts;
//! - payload-only restore never grants access;
//! - damage to one heap's labelled units leaves another heap readable;
//! - permanent identity tombstones + data-key destruction;
//! - disaster-recovery same-identity takeover (§17.4);
//! - incomplete purge with unavailable tier/replica domains (§6.5 / §26.7);
//! - minimum-retention scheduler blocking purge until the window elapses.

use crate::atomic_file::write_atomic;
use crate::error::StoreError;
use crate::failpoint;
use crate::ids::random_id;
use crate::layout::hex16;
use crate::tier::TierClass;
use residiuum_format::{
    decode_deterministic_uint_map, encode_deterministic_uint_map, encode_heap_binding_envelope,
    CborValue,
};
use residiuum_heap::{
    AuthorityEpoch, AuthorityGeneration, DeploymentId, HeapAdministrativeState, HeapId,
    HeapSecuritySnapshot, HeapSlot, SecurityRevision,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Profile for heap lifecycle control documents.
pub const HEAP_LIFECYCLE_PROFILE: &str = "residiuum-heap-lifecycle-v1";

/// Directory under `meta/` for lifecycle receipts and purge plans.
pub const LIFECYCLE_DIR: &str = "lifecycle";

/// Domain for purge coverage hash.
pub const PURGE_COVERAGE_DOMAIN: &[u8] = b"RESIDIUUM-HEAP-PURGE-COVERAGE-V1";

/// Domain for heap backup manifest identity.
pub const BACKUP_MANIFEST_DOMAIN: &[u8] = b"RESIDIUUM-HEAP-BACKUP-MANIFEST-V1";

/// Domain for identity tombstone records.
pub const TOMBSTONE_DOMAIN: &[u8] = b"RESIDIUUM-HEAP-IDENTITY-TOMBSTONE-V1";

/// Domain for data-key destruction receipts.
pub const DATA_KEY_DESTROY_DOMAIN: &[u8] = b"RESIDIUUM-HEAP-DATA-KEY-DESTROY-V1";

/// Domain for incomplete-purge result hashes.
pub const INCOMPLETE_PURGE_DOMAIN: &[u8] = b"RESIDIUUM-HEAP-INCOMPLETE-PURGE-V1";

/// Domain for heap retention policy documents.
pub const RETENTION_POLICY_DOMAIN: &[u8] = b"RESIDIUUM-HEAP-RETENTION-POLICY-V1";

/// Managed media / replica domain that purge coverage must enumerate (§11.7).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MediaDomain {
    /// Local tier class (hot / warm / cold / archive).
    Tier(TierClass),
    /// Named replica copy that must be destroyed for complete coverage.
    Replica {
        /// Replica identity.
        replica_id: [u8; 16],
    },
}

impl MediaDomain {
    fn wire_tag(&self) -> u8 {
        match self {
            Self::Tier(TierClass::Hot) => 1,
            Self::Tier(TierClass::Warm) => 2,
            Self::Tier(TierClass::Cold) => 3,
            Self::Tier(TierClass::Archive) => 4,
            Self::Replica { .. } => 5,
        }
    }

    fn encode_key(&self) -> Vec<u8> {
        let mut out = vec![self.wire_tag()];
        if let Self::Replica { replica_id } = self {
            out.extend_from_slice(replica_id);
        }
        out
    }
}

/// One managed object copy in a purge plan, scoped to a media domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeCoverageUnit {
    /// Object / frame / copy id.
    pub object_id: [u8; 16],
    /// Tier or replica domain holding the copy.
    pub domain: MediaDomain,
    /// Whether the domain is reachable for destruction right now.
    pub available: bool,
}

/// Result when purge cannot complete because managed domains were unavailable.
///
/// Spec: heap MUST remain `retired` and MUST NOT be reported as `purged`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncompletePurgeResult {
    /// Operation id of the aborted purge.
    pub operation_id: [u8; 16],
    /// Heap that remains retired.
    pub heap_id: [u8; 16],
    /// Domains that could not be destroyed.
    pub unavailable_domains: Vec<MediaDomain>,
    /// Coverage units destroyed before abort.
    pub destroyed_ids: Vec<[u8; 16]>,
    /// Coverage units still outstanding.
    pub remaining_ids: Vec<[u8; 16]>,
    /// Domain-separated integrity hash over the result body.
    pub result_hash: [u8; 32],
}

/// Heap-scoped minimum retention (governance; blocks purge until elapsed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapRetentionPolicy {
    /// Heap id.
    pub heap_id: [u8; 16],
    /// Earliest unix seconds at which purge may begin (inclusive).
    pub minimum_retain_until_unix_s: u64,
}

/// Evaluates whether purge is allowed under minimum retention (§12).
#[derive(Debug, Default, Clone)]
pub struct RetentionScheduler {
    policies: BTreeMap<[u8; 16], HeapRetentionPolicy>,
}

impl RetentionScheduler {
    /// Empty scheduler (no retention windows).
    pub fn new() -> Self {
        Self::default()
    }

    /// Install or replace a heap retention policy.
    pub fn set_policy(&mut self, policy: HeapRetentionPolicy) {
        self.policies.insert(policy.heap_id, policy);
    }

    /// Clear retention for a heap.
    pub fn clear_policy(&mut self, heap_id: &[u8; 16]) {
        self.policies.remove(heap_id);
    }

    /// Load durable policy if present under `data_root`.
    pub fn load_policy(
        &mut self,
        data_root: &Path,
        heap_id: &[u8; 16],
    ) -> Result<Option<HeapRetentionPolicy>, StoreError> {
        let path = retention_policy_path(data_root, heap_id);
        if !path.is_file() {
            return Ok(None);
        }
        let bytes = fs::read(&path)?;
        let policy = decode_retention_policy(&bytes)?;
        self.policies.insert(policy.heap_id, policy.clone());
        Ok(Some(policy))
    }

    /// Persist policy and remember it.
    pub fn save_policy(
        &mut self,
        data_root: &Path,
        policy: &HeapRetentionPolicy,
    ) -> Result<(), StoreError> {
        let dir = data_root
            .join("meta")
            .join(LIFECYCLE_DIR)
            .join(hex16(&policy.heap_id));
        fs::create_dir_all(&dir)?;
        let bytes = encode_retention_policy(policy)?;
        write_atomic(&retention_policy_path(data_root, &policy.heap_id), &bytes)?;
        self.policies.insert(policy.heap_id, policy.clone());
        Ok(())
    }

    /// Whether purge may start at `now_unix_s` for `heap_id`.
    pub fn purge_allowed_at(&self, heap_id: &[u8; 16], now_unix_s: u64) -> Result<(), StoreError> {
        if let Some(pol) = self.policies.get(heap_id) {
            if now_unix_s < pol.minimum_retain_until_unix_s {
                return Err(StoreError::HeapAdmit(format!(
                    "purge blocked by minimum retention until {}",
                    pol.minimum_retain_until_unix_s
                )));
            }
        }
        Ok(())
    }

    /// Heaps whose retention window has elapsed by `now_unix_s` (scheduler tick).
    pub fn tick_eligible(&self, now_unix_s: u64) -> Vec<[u8; 16]> {
        self.policies
            .iter()
            .filter(|(_, p)| now_unix_s >= p.minimum_retain_until_unix_s)
            .map(|(id, _)| *id)
            .collect()
    }
}

fn retention_policy_path(data_root: &Path, heap_id: &[u8; 16]) -> PathBuf {
    data_root
        .join("meta")
        .join(LIFECYCLE_DIR)
        .join(hex16(heap_id))
        .join("retention-policy.v1.cbor")
}

fn encode_retention_policy(policy: &HeapRetentionPolicy) -> Result<Vec<u8>, StoreError> {
    let mut body = Vec::new();
    body.extend_from_slice(&policy.heap_id);
    body.extend_from_slice(&policy.minimum_retain_until_unix_s.to_le_bytes());
    let integrity = domain_hash(RETENTION_POLICY_DOMAIN, &body);
    encode_deterministic_uint_map(&[
        (1u64, CborValue::Text(HEAP_LIFECYCLE_PROFILE.into())),
        (2, CborValue::Bytes(policy.heap_id.to_vec())),
        (3, CborValue::Uint(policy.minimum_retain_until_unix_s)),
        (4, CborValue::Bytes(integrity.to_vec())),
    ])
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))
}

fn decode_retention_policy(bytes: &[u8]) -> Result<HeapRetentionPolicy, StoreError> {
    let map =
        decode_deterministic_uint_map(bytes).map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let mut by = BTreeMap::new();
    for (k, v) in map {
        by.insert(k, v);
    }
    let get = |k: u64| {
        by.get(&k)
            .cloned()
            .ok_or_else(|| StoreError::HeapAdmit(format!("missing retention key {k}")))
    };
    match get(1)? {
        CborValue::Text(s) if s == HEAP_LIFECYCLE_PROFILE => {}
        _ => return Err(StoreError::HeapAdmit("bad lifecycle profile".into())),
    }
    Ok(HeapRetentionPolicy {
        heap_id: expect_b16(&get(2)?)?,
        minimum_retain_until_unix_s: expect_u64(&get(3)?)?,
    })
}

/// Permanent identity tombstone kind (§6 / §35).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TombstoneKind {
    /// Heap retired (not purged).
    Retired = 1,
    /// Heap purged; identity never reused.
    Purged = 2,
}

impl TombstoneKind {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Retired),
            2 => Some(Self::Purged),
            _ => None,
        }
    }
}

/// Durable permanent identity tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityTombstone {
    /// Tombstoned heap id.
    pub heap_id: [u8; 16],
    /// Kind.
    pub kind: TombstoneKind,
    /// Authority epoch at tombstone time (if known).
    pub authority_epoch: u64,
    /// Unix seconds.
    pub created_at: u64,
}

/// Data-encryption key handle for one heap (HP-009 / H4 KMS).
///
/// In-process and KMS envelope paths share this type. Plaintext material is
/// wiped on destroy; KMS-backed handles may also carry ciphertext + CMK id.
#[derive(Clone)]
pub struct DataKeyHandle {
    heap_id: [u8; 16],
    key_id: [u8; 16],
    /// Secret bytes; cleared on destroy.
    material: Option<Vec<u8>>,
    /// KMS GenerateDataKey ciphertext blob (envelope DEK), if any.
    ciphertext_blob: Option<Vec<u8>>,
    /// External key id (CMK ARN / alias) when minted via cloud KMS.
    external_key_id: Option<String>,
    /// Provider id that minted this handle (`in-process`, `aws-kms`, …).
    provider_id: Option<String>,
}

impl std::fmt::Debug for DataKeyHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataKeyHandle")
            .field("heap_id", &hex16(&self.heap_id))
            .field("key_id", &hex16(&self.key_id))
            .field("destroyed", &self.material.is_none())
            .field("has_ciphertext", &self.ciphertext_blob.is_some())
            .field("external_key_id", &self.external_key_id)
            .field("provider_id", &self.provider_id)
            .finish()
    }
}

impl DataKeyHandle {
    /// Mint a new data key for `heap_id` (local secret material).
    pub fn generate(heap_id: [u8; 16], secret: &[u8]) -> Result<Self, StoreError> {
        if secret.is_empty() {
            return Err(StoreError::HeapAdmit("empty data key".into()));
        }
        Ok(Self {
            heap_id,
            key_id: random_id()?,
            material: Some(secret.to_vec()),
            ciphertext_blob: None,
            external_key_id: None,
            provider_id: Some("in-process".into()),
        })
    }

    /// Mint an envelope-encrypted data key (plaintext + KMS ciphertext under CMK).
    pub fn generate_envelope(
        heap_id: [u8; 16],
        plaintext: &[u8],
        ciphertext_blob: Vec<u8>,
        external_key_id: impl Into<String>,
        provider_id: impl Into<String>,
    ) -> Result<Self, StoreError> {
        if plaintext.is_empty() {
            return Err(StoreError::HeapAdmit("empty data key plaintext".into()));
        }
        if ciphertext_blob.is_empty() {
            return Err(StoreError::HeapAdmit("empty ciphertext blob".into()));
        }
        Ok(Self {
            heap_id,
            key_id: random_id()?,
            material: Some(plaintext.to_vec()),
            ciphertext_blob: Some(ciphertext_blob),
            external_key_id: Some(external_key_id.into()),
            provider_id: Some(provider_id.into()),
        })
    }

    /// Heap id.
    pub fn heap_id(&self) -> [u8; 16] {
        self.heap_id
    }

    /// Key id.
    pub fn key_id(&self) -> [u8; 16] {
        self.key_id
    }

    /// Whether the key material has been destroyed.
    pub fn is_destroyed(&self) -> bool {
        self.material.is_none()
    }

    /// Borrow secret material if still live.
    pub fn material(&self) -> Option<&[u8]> {
        self.material.as_deref()
    }

    /// KMS ciphertext blob if this is an envelope key.
    pub fn ciphertext_blob(&self) -> Option<&[u8]> {
        self.ciphertext_blob.as_deref()
    }

    /// External CMK / key id when KMS-backed.
    pub fn external_key_id(&self) -> Option<&str> {
        self.external_key_id.as_deref()
    }

    /// Provider that minted the handle.
    pub fn provider_id(&self) -> Option<&str> {
        self.provider_id.as_deref()
    }

    /// Whether this handle was minted via envelope encryption (cloud KMS).
    pub fn is_envelope(&self) -> bool {
        self.ciphertext_blob.is_some()
    }
}

/// Provider surface for heap data-encryption keys (H4 / CPR-003 residual).
///
/// Product path today: [`InProcessDataKeyProvider`]. External HSM / cloud KMS
/// adapters implement this trait; until commissioned they return
/// [`StoreError::HeapAdmit`] with an honest "not configured" reason.
pub trait DataKeyProvider: Send + Sync {
    /// Stable provider id for operator logs (e.g. `in-process`, `hsm-pkcs11`).
    fn provider_id(&self) -> &'static str;

    /// Mint a data key handle for `heap_id`.
    fn generate(&self, heap_id: [u8; 16]) -> Result<DataKeyHandle, StoreError>;

    /// Destroy key material and emit a durable receipt under `data_root`.
    fn destroy(
        &self,
        data_root: &Path,
        handle: &mut DataKeyHandle,
    ) -> Result<DataKeyDestructionReceipt, StoreError>;
}

/// Default in-process provider (Accept / single-node; not an HSM).
#[derive(Debug, Default, Clone, Copy)]
pub struct InProcessDataKeyProvider;

impl DataKeyProvider for InProcessDataKeyProvider {
    fn provider_id(&self) -> &'static str {
        "in-process"
    }

    fn generate(&self, heap_id: [u8; 16]) -> Result<DataKeyHandle, StoreError> {
        // 32-byte secret from OS entropy (not HSM-backed).
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|e| StoreError::HeapAdmit(format!("entropy: {e}")))?;
        DataKeyHandle::generate(heap_id, &secret)
    }

    fn destroy(
        &self,
        data_root: &Path,
        handle: &mut DataKeyHandle,
    ) -> Result<DataKeyDestructionReceipt, StoreError> {
        destroy_data_key(data_root, handle)
    }
}

/// Named external key backends (H4 / CPR-003).
///
/// - [`HsmBackendKind::AwsKms`]: live HTTPS connector via feature `aws-kms`
///   ([`crate::AwsKmsDataKeyProvider`]).
/// - PKCS#11 / GCP / Azure: scaffold refuse until wired.
/// - [`HsmBackendKind::MockInProcess`]: Accept/dev stand-in only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HsmBackendKind {
    /// PKCS#11 shared library (SoftHSM / hardware token).
    Pkcs11,
    /// AWS KMS.
    AwsKms,
    /// Google Cloud KMS.
    GcpKms,
    /// Azure Key Vault.
    AzureKeyVault,
    /// In-process mock for tests only — **not** production HSM.
    MockInProcess,
}

impl HsmBackendKind {
    /// Stable wire / log label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pkcs11 => "pkcs11",
            Self::AwsKms => "aws-kms",
            Self::GcpKms => "gcp-kms",
            Self::AzureKeyVault => "azure-key-vault",
            Self::MockInProcess => "hsm-mock-in-process",
        }
    }
}

/// Operator configuration for an HSM / KMS data-key provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HsmDataKeyConfig {
    /// Backend family.
    pub backend: HsmBackendKind,
    /// PKCS#11 library path, KMS endpoint URL, or vault URI.
    pub library_or_endpoint: Option<String>,
    /// Slot / region / vault name when applicable.
    pub slot_or_region: Option<String>,
    /// Key label / alias inside the backend.
    pub key_label: Option<String>,
    /// When true and backend is [`HsmBackendKind::MockInProcess`], generate/destroy
    /// use OS entropy locally (Accept/dev only).
    pub mock_enabled: bool,
}

impl HsmDataKeyConfig {
    /// Unconfigured real backend (always refuses generate/destroy).
    pub fn unconfigured(backend: HsmBackendKind) -> Self {
        Self {
            backend,
            library_or_endpoint: None,
            slot_or_region: None,
            key_label: None,
            mock_enabled: false,
        }
    }

    /// Mock backend enabled for Accept tests.
    pub fn mock_in_process() -> Self {
        Self {
            backend: HsmBackendKind::MockInProcess,
            library_or_endpoint: Some("mock://local".into()),
            slot_or_region: Some("0".into()),
            key_label: Some("residiuum-heap-test".into()),
            mock_enabled: true,
        }
    }

    /// Whether this config can perform key ops today (mock path only here).
    ///
    /// Live AWS KMS uses [`AwsKmsDataKeyProvider`] (feature `aws-kms`) rather than
    /// this boolean alone.
    pub fn is_operational(&self) -> bool {
        matches!(self.backend, HsmBackendKind::MockInProcess) && self.mock_enabled
    }

    /// AWS KMS configuration: region + CMK id/ARN; optional custom endpoint (LocalStack).
    pub fn aws_kms(
        region: impl Into<String>,
        key_id_or_arn: impl Into<String>,
        endpoint: Option<String>,
    ) -> Self {
        Self {
            backend: HsmBackendKind::AwsKms,
            library_or_endpoint: endpoint,
            slot_or_region: Some(region.into()),
            key_label: Some(key_id_or_arn.into()),
            mock_enabled: false,
        }
    }

    /// Load AWS KMS config from environment when set.
    ///
    /// - `RESIDIUUM_AWS_KMS_KEY_ID` or `RESIDIUUM_KMS_KEY_ARN` — CMK id/ARN (required)
    /// - `AWS_REGION` or `RESIDIUUM_AWS_REGION` — region (default `us-east-1`)
    /// - `RESIDIUUM_AWS_ENDPOINT_URL` or `AWS_ENDPOINT_URL` — optional override (LocalStack)
    pub fn aws_kms_from_env() -> Option<Self> {
        let key = std::env::var("RESIDIUUM_AWS_KMS_KEY_ID")
            .or_else(|_| std::env::var("RESIDIUUM_KMS_KEY_ARN"))
            .ok()
            .filter(|s| !s.is_empty())?;
        let region = std::env::var("RESIDIUUM_AWS_REGION")
            .or_else(|_| std::env::var("AWS_REGION"))
            .unwrap_or_else(|_| "us-east-1".into());
        let endpoint = std::env::var("RESIDIUUM_AWS_ENDPOINT_URL")
            .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
            .ok()
            .filter(|s| !s.is_empty());
        Some(Self::aws_kms(region, key, endpoint))
    }
}

/// Declared capabilities of a data-key backend (honest advertising).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HsmCapabilities {
    /// Can mint heap data keys.
    pub generate: bool,
    /// Can destroy / schedule destroy of keys.
    pub destroy: bool,
    /// Must never return long-lived plaintext key material to callers.
    pub never_export_plaintext: bool,
    /// Whether this is a production external HSM/KMS (false for mock).
    pub production_hsm: bool,
}

/// Scaffold for external HSM / PKCS#11 / cloud KMS adapters.
///
/// - Real backends (`Pkcs11`, `AwsKms`, …): **refuse** until a live connector is wired.
/// - [`HsmBackendKind::MockInProcess`] with `mock_enabled`: Accept/dev path only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HsmDataKeyProvider {
    config: HsmDataKeyConfig,
}

impl HsmDataKeyProvider {
    /// Named backend that is not yet wired (historical constructor).
    pub fn new(backend: &'static str) -> Self {
        let kind = match backend {
            "pkcs11" => HsmBackendKind::Pkcs11,
            "aws-kms" => HsmBackendKind::AwsKms,
            "gcp-kms" => HsmBackendKind::GcpKms,
            "azure-key-vault" => HsmBackendKind::AzureKeyVault,
            "hsm-mock-in-process" | "mock" => HsmBackendKind::MockInProcess,
            _ => HsmBackendKind::Pkcs11,
        };
        Self {
            config: HsmDataKeyConfig::unconfigured(kind),
        }
    }

    /// Build from full config.
    pub fn from_config(config: HsmDataKeyConfig) -> Self {
        Self { config }
    }

    /// Mock provider for Accept / unit tests.
    pub fn mock_for_tests() -> Self {
        Self::from_config(HsmDataKeyConfig::mock_in_process())
    }

    /// Backend configuration.
    pub fn config(&self) -> &HsmDataKeyConfig {
        &self.config
    }

    /// Whether generate/destroy will succeed for this process.
    pub fn is_configured(&self) -> bool {
        self.config.is_operational()
    }

    /// Capability advertisement (never over-claims production HSM).
    pub fn capabilities(&self) -> HsmCapabilities {
        if self.config.is_operational() {
            HsmCapabilities {
                generate: true,
                destroy: true,
                never_export_plaintext: false, // mock holds material in process
                production_hsm: false,
            }
        } else {
            HsmCapabilities {
                generate: false,
                destroy: false,
                never_export_plaintext: true,
                production_hsm: !matches!(self.config.backend, HsmBackendKind::MockInProcess),
            }
        }
    }
}

impl DataKeyProvider for HsmDataKeyProvider {
    fn provider_id(&self) -> &'static str {
        if self.config.is_operational() {
            "hsm-mock-in-process"
        } else {
            "hsm-scaffold"
        }
    }

    fn generate(&self, heap_id: [u8; 16]) -> Result<DataKeyHandle, StoreError> {
        if self.config.is_operational() {
            // Mock path only — production backends never reach here until wired.
            return InProcessDataKeyProvider.generate(heap_id);
        }
        Err(StoreError::HeapAdmit(format!(
            "HSM data-key backend {:?} not configured (library/endpoint={:?} label={:?}); use InProcessDataKeyProvider or HsmDataKeyProvider::mock_for_tests()",
            self.config.backend.as_str(),
            self.config.library_or_endpoint,
            self.config.key_label
        )))
    }

    fn destroy(
        &self,
        data_root: &Path,
        handle: &mut DataKeyHandle,
    ) -> Result<DataKeyDestructionReceipt, StoreError> {
        if self.config.is_operational() {
            return InProcessDataKeyProvider.destroy(data_root, handle);
        }
        Err(StoreError::HeapAdmit(format!(
            "HSM data-key backend {:?} not configured (scaffold only)",
            self.config.backend.as_str()
        )))
    }
}

/// Receipt that a data key was destroyed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataKeyDestructionReceipt {
    /// Receipt id.
    pub receipt_id: [u8; 16],
    /// Heap id.
    pub heap_id: [u8; 16],
    /// Destroyed key id.
    pub key_id: [u8; 16],
    /// Fingerprint of destroyed material (domain-separated hash).
    pub destroyed_fingerprint: [u8; 32],
    /// Unix seconds.
    pub created_at: u64,
}

/// Disaster-recovery ceremony evidence for same-identity restore (§17.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisasterRecoveryCeremony {
    /// Heap identity retained.
    pub heap_id: [u8; 16],
    /// Fenced old deployment.
    pub old_deployment_id: [u8; 16],
    /// Replacement deployment.
    pub new_deployment_id: [u8; 16],
    /// Epoch before takeover.
    pub old_authority_epoch: u64,
    /// Advanced epoch after takeover (must be `old + 1`).
    pub new_authority_epoch: u64,
    /// Fresh master public key for the restored head.
    pub new_master_public_key: [u8; 32],
    /// Opaque recovery-authority evidence hash (§8.9.2 stand-in).
    pub recovery_authority_evidence: [u8; 32],
}

/// Sealed DR restore package (payload + identity; not a live authority head).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisasterRecoveryPackage {
    /// Retained heap id.
    pub heap_id: [u8; 16],
    /// Deployment id recorded in the backup.
    pub backup_deployment_id: [u8; 16],
    /// Authority epoch recorded in the backup.
    pub backup_authority_epoch: u64,
    /// Application payload bytes.
    pub payload: Vec<u8>,
}

/// Result of a successful same-identity DR takeover.
#[derive(Debug, Clone)]
pub struct DisasterRecoveryTakeoverResult {
    /// Installed security snapshot (new deployment + advanced epoch).
    pub snapshot: HeapSecuritySnapshot,
    /// Durable takeover evidence hash.
    pub takeover_evidence_hash: [u8; 32],
    /// Fenced old deployment id.
    pub fenced_deployment_id: [u8; 16],
}

/// Verifiable purge receipt (forensic; not an authorization database).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeReceipt {
    /// Receipt id.
    pub receipt_id: [u8; 16],
    /// Operation id that completed the purge.
    pub operation_id: [u8; 16],
    /// Purged heap.
    pub heap_id: [u8; 16],
    /// Declared coverage hash over managed object ids.
    pub coverage_hash: [u8; 32],
    /// Security revision after transition to purged.
    pub security_revision: u64,
    /// Unix seconds.
    pub created_at: u64,
}

/// Fixed purge plan entered from `retired`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgePlan {
    /// Operation id (resume key).
    pub operation_id: [u8; 16],
    /// Target heap.
    pub heap_id: [u8; 16],
    /// Managed object/frame ids that must be destroyed.
    pub coverage_ids: Vec<[u8; 16]>,
    /// Coverage hash at plan fix time.
    pub coverage_hash: [u8; 32],
    /// Per-unit media domain metadata (empty when using id-only begin_purge).
    pub units: Vec<PurgeCoverageUnit>,
}

/// Heap-scoped backup manifest (lists exact heap IDs; no authority material).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapBackupManifest {
    /// Manifest id.
    pub manifest_id: [u8; 16],
    /// Deployment id the backup was taken from.
    pub deployment_id: [u8; 16],
    /// Exact heap ids included (sorted on encode).
    pub heap_ids: Vec<[u8; 16]>,
    /// BLAKE3 identity of the manifest body.
    pub manifest_hash: [u8; 32],
}

/// Payload bytes restored without authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadOnlyRestore {
    /// Fresh heap id assigned at restore (not the source id).
    pub new_heap_id: [u8; 16],
    /// Source heap id recorded as inert provenance only.
    pub source_heap_id: [u8; 16],
    /// Restored payload path relative marker.
    pub payload_label: String,
}

/// In-memory + durable lifecycle controller for one resident heap.
pub struct HeapLifecycle {
    layout_root: PathBuf,
    slot: Arc<HeapSlot>,
    /// Remembered resume state while suspended.
    resume_state: Option<HeapAdministrativeState>,
    /// Active legal/retention holds (block purge).
    holds: BTreeSet<String>,
    /// Fixed purge plan while purging.
    purge_plan: Option<PurgePlan>,
    /// Destroyed coverage ids during purge.
    destroyed: BTreeSet<[u8; 16]>,
    /// Optional retention scheduler (minimum retention windows).
    retention: RetentionScheduler,
}

impl HeapLifecycle {
    /// Bind to a resident slot under `data_root`.
    pub fn open(data_root: impl Into<PathBuf>, slot: Arc<HeapSlot>) -> Self {
        Self {
            layout_root: data_root.into(),
            slot,
            resume_state: None,
            holds: BTreeSet::new(),
            purge_plan: None,
            destroyed: BTreeSet::new(),
            retention: RetentionScheduler::new(),
        }
    }

    /// Borrow the retention scheduler.
    pub fn retention_mut(&mut self) -> &mut RetentionScheduler {
        &mut self.retention
    }

    /// Current administrative state.
    pub fn state(&self) -> HeapAdministrativeState {
        self.slot.load().administrative_state
    }

    /// Place a retention/legal hold.
    pub fn place_hold(&mut self, hold_id: impl Into<String>) -> Result<(), StoreError> {
        self.holds.insert(hold_id.into());
        Ok(())
    }

    /// Release a hold.
    pub fn release_hold(&mut self, hold_id: &str) -> Result<(), StoreError> {
        self.holds.remove(hold_id);
        Ok(())
    }

    /// Suspend ordinary service (remembers prior active/read_only).
    pub fn suspend(&mut self, operation_id: [u8; 16]) -> Result<(), StoreError> {
        let snap = self.slot.load();
        let remembered = match snap.administrative_state {
            HeapAdministrativeState::Active | HeapAdministrativeState::ReadOnly => {
                snap.administrative_state
            }
            other => {
                return Err(StoreError::HeapAdmit(format!(
                    "invalid transition: cannot suspend from {}",
                    other.wire_name()
                )));
            }
        };
        self.resume_state = Some(remembered);
        self.transition(HeapAdministrativeState::Suspended, operation_id, "suspend")?;
        Ok(())
    }

    /// Resume from suspension to the remembered state.
    pub fn resume(&mut self, operation_id: [u8; 16]) -> Result<(), StoreError> {
        let snap = self.slot.load();
        if snap.administrative_state != HeapAdministrativeState::Suspended {
            return Err(StoreError::HeapAdmit(
                "invalid transition: resume requires suspended".into(),
            ));
        }
        let target = self
            .resume_state
            .take()
            .ok_or_else(|| StoreError::HeapAdmit("missing remembered resume state".into()))?;
        self.transition(target, operation_id, "resume")?;
        Ok(())
    }

    /// Retire the heap (terminal for ordinary discovery).
    pub fn retire(&mut self, operation_id: [u8; 16]) -> Result<(), StoreError> {
        let snap = self.slot.load();
        match snap.administrative_state {
            HeapAdministrativeState::Active
            | HeapAdministrativeState::ReadOnly
            | HeapAdministrativeState::Suspended => {}
            other => {
                return Err(StoreError::HeapAdmit(format!(
                    "invalid transition: cannot retire from {}",
                    other.wire_name()
                )));
            }
        }
        self.resume_state = None;
        self.transition(HeapAdministrativeState::Retired, operation_id, "retire")?;
        let snap = self.slot.load();
        write_identity_tombstone(
            &self.layout_root,
            &IdentityTombstone {
                heap_id: snap.heap_id.to_bytes(),
                kind: TombstoneKind::Retired,
                authority_epoch: snap.authority_epoch.get(),
                created_at: now_secs(),
            },
        )?;
        Ok(())
    }

    fn assert_purge_gates(&self, now_unix_s: u64) -> Result<(), StoreError> {
        if !self.holds.is_empty() {
            return Err(StoreError::HeapAdmit(
                "purge blocked by active retention/legal hold".into(),
            ));
        }
        let heap_id = self.slot.load().heap_id.to_bytes();
        self.retention.purge_allowed_at(&heap_id, now_unix_s)?;
        Ok(())
    }

    /// Begin purge from retired with a fixed plan. Blocked by holds and retention.
    pub fn begin_purge(
        &mut self,
        operation_id: [u8; 16],
        coverage_ids: Vec<[u8; 16]>,
    ) -> Result<PurgePlan, StoreError> {
        self.assert_purge_gates(now_secs())?;
        let snap = self.slot.load();
        if snap.administrative_state != HeapAdministrativeState::Retired {
            return Err(StoreError::HeapAdmit(
                "invalid transition: begin_purge requires retired".into(),
            ));
        }
        let coverage_hash = coverage_hash(&coverage_ids);
        let plan = PurgePlan {
            operation_id,
            heap_id: snap.heap_id.to_bytes(),
            coverage_ids,
            coverage_hash,
            units: Vec::new(),
        };
        self.purge_plan = Some(plan.clone());
        self.destroyed.clear();
        self.transition(
            HeapAdministrativeState::Purging,
            operation_id,
            "begin_purge",
        )?;
        self.persist_plan(&plan)?;
        failpoint::hit("heap_lifecycle.after_purge_plan")?;
        Ok(plan)
    }

    /// Begin purge with explicit tier/replica coverage units (§11.7 / §26.7).
    ///
    /// Unavailable domains are recorded in the fixed plan. Completion is refused
    /// until every unit is destroyed; [`Self::abort_incomplete_purge`] returns a
    /// durable incomplete result and leaves the heap `retired` (never `purged`).
    pub fn begin_purge_media(
        &mut self,
        operation_id: [u8; 16],
        units: Vec<PurgeCoverageUnit>,
        now_unix_s: u64,
    ) -> Result<PurgePlan, StoreError> {
        self.assert_purge_gates(now_unix_s)?;
        let snap = self.slot.load();
        if snap.administrative_state != HeapAdministrativeState::Retired {
            return Err(StoreError::HeapAdmit(
                "invalid transition: begin_purge requires retired".into(),
            ));
        }
        if units.is_empty() {
            return Err(StoreError::HeapAdmit(
                "purge plan must enumerate at least one managed domain unit".into(),
            ));
        }
        let mut coverage_ids: Vec<[u8; 16]> = units.iter().map(|u| u.object_id).collect();
        coverage_ids.sort();
        coverage_ids.dedup();
        let coverage_hash = coverage_hash(&coverage_ids);
        let plan = PurgePlan {
            operation_id,
            heap_id: snap.heap_id.to_bytes(),
            coverage_ids,
            coverage_hash,
            units,
        };
        self.purge_plan = Some(plan.clone());
        self.destroyed.clear();
        self.transition(
            HeapAdministrativeState::Purging,
            operation_id,
            "begin_purge",
        )?;
        self.persist_plan(&plan)?;
        failpoint::hit("heap_lifecycle.after_purge_plan")?;
        Ok(plan)
    }

    /// Domains marked unavailable in the fixed plan (report for operators).
    pub fn unavailable_purge_domains(&self) -> Vec<MediaDomain> {
        let Some(plan) = self.purge_plan.as_ref() else {
            return Vec::new();
        };
        let mut out = BTreeSet::new();
        for u in &plan.units {
            if !u.available {
                out.insert(u.domain.clone());
            }
        }
        out.into_iter().collect()
    }

    /// Record destruction of one coverage id (idempotent).
    ///
    /// Refuses objects whose plan unit is marked unavailable — those domains
    /// must be recovered or the purge aborted as incomplete.
    pub fn destroy_coverage_unit(&mut self, object_id: [u8; 16]) -> Result<(), StoreError> {
        self.destroy_coverage_unit_inner(object_id, None)
    }

    /// Destroy a coverage unit **and wipe live filesystem media** under `media_root`.
    ///
    /// Layout (drill / Accept): `{media_root}/{heap_hex}/{object_hex}/…`.
    /// The entire object directory is removed when present. Missing paths are
    /// treated as already wiped (idempotent). The domain must be available.
    ///
    /// This is the live multi-tier media wipe path for H4 (CPR-003 residual).
    pub fn destroy_coverage_unit_on_media(
        &mut self,
        object_id: [u8; 16],
        media_root: &Path,
    ) -> Result<(), StoreError> {
        self.destroy_coverage_unit_inner(object_id, Some(media_root))
    }

    fn destroy_coverage_unit_inner(
        &mut self,
        object_id: [u8; 16],
        media_root: Option<&Path>,
    ) -> Result<(), StoreError> {
        let plan = self
            .purge_plan
            .as_ref()
            .ok_or_else(|| StoreError::HeapAdmit("no purge plan".into()))?;
        if self.slot.load().administrative_state != HeapAdministrativeState::Purging {
            return Err(StoreError::HeapAdmit("destroy requires purging".into()));
        }
        if !plan.coverage_ids.contains(&object_id) {
            return Err(StoreError::HeapAdmit(
                "object outside purge coverage".into(),
            ));
        }
        if !plan.units.is_empty() {
            let unit = plan
                .units
                .iter()
                .find(|u| u.object_id == object_id)
                .ok_or_else(|| StoreError::HeapAdmit("object outside purge coverage".into()))?;
            if !unit.available {
                return Err(StoreError::HeapAdmit(format!(
                    "managed domain unavailable for object {}",
                    hex16(&object_id)
                )));
            }
        }
        if let Some(root) = media_root {
            wipe_heap_object_media(root, &plan.heap_id, &object_id)?;
        }
        self.destroyed.insert(object_id);
        failpoint::hit("heap_lifecycle.after_coverage_destroy")?;
        Ok(())
    }

    /// Complete purge when coverage is fully destroyed; emits a verifiable receipt.
    pub fn complete_purge(&mut self, operation_id: [u8; 16]) -> Result<PurgeReceipt, StoreError> {
        let plan = self
            .purge_plan
            .clone()
            .ok_or_else(|| StoreError::HeapAdmit("no purge plan".into()))?;
        if operation_id != plan.operation_id {
            return Err(StoreError::HeapAdmit(
                "complete_purge operation_id mismatch".into(),
            ));
        }
        if self.slot.load().administrative_state != HeapAdministrativeState::Purging {
            return Err(StoreError::HeapAdmit(
                "invalid transition: complete_purge requires purging".into(),
            ));
        }
        if !self.unavailable_purge_domains().is_empty() {
            return Err(StoreError::HeapAdmit(
                "incomplete purge: managed replica/tier domain unavailable; remaining retired"
                    .into(),
            ));
        }
        for id in &plan.coverage_ids {
            if !self.destroyed.contains(id) {
                return Err(StoreError::HeapAdmit(
                    "incomplete purge coverage; remaining retired".into(),
                ));
            }
        }
        // Re-hash destroyed set must match plan.
        let mut done: Vec<_> = self.destroyed.iter().copied().collect();
        done.sort();
        let got = coverage_hash(&done);
        if got != plan.coverage_hash {
            return Err(StoreError::HeapAdmit("purge coverage hash mismatch".into()));
        }
        self.transition(
            HeapAdministrativeState::Purged,
            operation_id,
            "complete_purge",
        )?;
        let snap = self.slot.load();
        write_identity_tombstone(
            &self.layout_root,
            &IdentityTombstone {
                heap_id: plan.heap_id,
                kind: TombstoneKind::Purged,
                authority_epoch: snap.authority_epoch.get(),
                created_at: now_secs(),
            },
        )?;
        let receipt = PurgeReceipt {
            receipt_id: random_id()?,
            operation_id,
            heap_id: plan.heap_id,
            coverage_hash: plan.coverage_hash,
            security_revision: snap.security_revision.get(),
            created_at: now_secs(),
        };
        self.persist_purge_receipt(&receipt)?;
        self.purge_plan = None;
        Ok(receipt)
    }

    /// Abort incomplete purge back to retired (no durable incomplete report).
    pub fn abort_purge(&mut self, operation_id: [u8; 16]) -> Result<(), StoreError> {
        if self.slot.load().administrative_state != HeapAdministrativeState::Purging {
            return Err(StoreError::HeapAdmit(
                "invalid transition: abort_purge requires purging".into(),
            ));
        }
        self.purge_plan = None;
        self.destroyed.clear();
        self.transition(
            HeapAdministrativeState::Retired,
            operation_id,
            "abort_purge",
        )?;
        Ok(())
    }

    /// Abort when managed domains are unavailable: durable incomplete result, stay `retired`.
    ///
    /// MUST NOT transition to `purged` (§6.5).
    pub fn abort_incomplete_purge(
        &mut self,
        operation_id: [u8; 16],
    ) -> Result<IncompletePurgeResult, StoreError> {
        let plan = self
            .purge_plan
            .clone()
            .ok_or_else(|| StoreError::HeapAdmit("no purge plan".into()))?;
        if operation_id != plan.operation_id {
            return Err(StoreError::HeapAdmit(
                "abort_incomplete_purge operation_id mismatch".into(),
            ));
        }
        if self.slot.load().administrative_state != HeapAdministrativeState::Purging {
            return Err(StoreError::HeapAdmit(
                "invalid transition: abort_incomplete_purge requires purging".into(),
            ));
        }
        let unavailable = self.unavailable_purge_domains();
        if unavailable.is_empty() {
            // Still allow abort if coverage simply incomplete (operator cancel).
            // Prefer reporting any remaining ids.
        }
        let mut destroyed_ids: Vec<_> = self.destroyed.iter().copied().collect();
        destroyed_ids.sort();
        let remaining_ids: Vec<_> = plan
            .coverage_ids
            .iter()
            .copied()
            .filter(|id| !self.destroyed.contains(id))
            .collect();

        let mut body = Vec::new();
        body.extend_from_slice(&plan.operation_id);
        body.extend_from_slice(&plan.heap_id);
        for d in &unavailable {
            body.extend_from_slice(&d.encode_key());
        }
        for id in &destroyed_ids {
            body.extend_from_slice(id);
        }
        for id in &remaining_ids {
            body.extend_from_slice(id);
        }
        let result_hash = domain_hash(INCOMPLETE_PURGE_DOMAIN, &body);

        let result = IncompletePurgeResult {
            operation_id,
            heap_id: plan.heap_id,
            unavailable_domains: unavailable,
            destroyed_ids,
            remaining_ids,
            result_hash,
        };
        self.persist_incomplete_purge(&result)?;
        self.purge_plan = None;
        self.destroyed.clear();
        self.transition(
            HeapAdministrativeState::Retired,
            operation_id,
            "incomplete_purge",
        )?;
        // Ensure we never leave a purged tombstone from this path.
        debug_assert_ne!(self.state(), HeapAdministrativeState::Purged);
        Ok(result)
    }
}

/// Verify a purge receipt against the expected coverage set.
pub fn verify_purge_receipt(
    receipt: &PurgeReceipt,
    coverage_ids: &[[u8; 16]],
) -> Result<(), StoreError> {
    let expect = coverage_hash(coverage_ids);
    if expect != receipt.coverage_hash {
        return Err(StoreError::HeapAdmit(
            "purge receipt coverage hash does not verify".into(),
        ));
    }
    Ok(())
}

/// Build a heap-aware backup manifest (no authority bytes).
pub fn build_backup_manifest(
    deployment_id: [u8; 16],
    heap_ids: &[[u8; 16]],
) -> Result<HeapBackupManifest, StoreError> {
    let mut ids: Vec<_> = heap_ids.to_vec();
    ids.sort();
    ids.dedup();
    let mut body = Vec::new();
    body.extend_from_slice(&deployment_id);
    for id in &ids {
        body.extend_from_slice(id);
    }
    let manifest_hash = domain_hash(BACKUP_MANIFEST_DOMAIN, &body);
    Ok(HeapBackupManifest {
        manifest_id: random_id()?,
        deployment_id,
        heap_ids: ids,
        manifest_hash,
    })
}

/// Restore payload bytes to a **new** heap id without authority material.
///
/// The returned package is inert provenance + payload only. Callers MUST NOT
/// treat this as access; use [`refuse_access_from_payload_restore`].
pub fn restore_payload_to_new_heap(
    data_root: &Path,
    source_heap_id: [u8; 16],
    payload: &[u8],
    label: &str,
) -> Result<PayloadOnlyRestore, StoreError> {
    let new_heap_id = random_id()?;
    let dir = data_root.join("restore").join(hex16(&new_heap_id));
    fs::create_dir_all(&dir)?;
    write_atomic(&dir.join("payload.bin"), payload)?;
    let meta = encode_deterministic_uint_map(&[
        (1u64, CborValue::Text(HEAP_LIFECYCLE_PROFILE.into())),
        (2, CborValue::Bytes(new_heap_id.to_vec())),
        (3, CborValue::Bytes(source_heap_id.to_vec())),
        (4, CborValue::Text(label.into())),
        // Explicitly absent: no authority head, master key, or HeapKey.
    ])
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    write_atomic(&dir.join("provenance.v1.cbor"), &meta)?;
    Ok(PayloadOnlyRestore {
        new_heap_id,
        source_heap_id,
        payload_label: label.into(),
    })
}

/// Payload-only restore MUST NOT grant ordinary or derived access.
pub fn refuse_access_from_payload_restore(restored: &PayloadOnlyRestore) -> Result<(), StoreError> {
    let _ = restored;
    Err(StoreError::HeapAdmit(
        "payload-only restore cannot grant access".into(),
    ))
}

/// Live filesystem layout for heap object media under a tier/replica root.
///
/// `{media_root}/{heap_id_hex}/{object_id_hex}/`
pub fn heap_object_media_dir(
    media_root: &Path,
    heap_id: &[u8; 16],
    object_id: &[u8; 16],
) -> PathBuf {
    media_root.join(hex16(heap_id)).join(hex16(object_id))
}

/// Remove heap-scoped object media on a live filesystem root (idempotent).
pub fn wipe_heap_object_media(
    media_root: &Path,
    heap_id: &[u8; 16],
    object_id: &[u8; 16],
) -> Result<(), StoreError> {
    if !media_root.is_dir() {
        return Err(StoreError::HeapAdmit(format!(
            "media root unavailable: {}",
            media_root.display()
        )));
    }
    let target = heap_object_media_dir(media_root, heap_id, object_id);
    if target.exists() {
        if target.is_dir() {
            fs::remove_dir_all(&target)?;
        } else {
            fs::remove_file(&target)?;
        }
    }
    // Best-effort: drop empty heap directory when last object is gone.
    let heap_dir = media_root.join(hex16(heap_id));
    if heap_dir.is_dir() && fs::read_dir(&heap_dir)?.next().is_none() {
        let _ = fs::remove_dir(&heap_dir);
    }
    Ok(())
}

/// Destroy data-key material and emit a durable receipt.
///
/// Idempotent: a second destroy returns the existing receipt path semantics by
/// refusing to re-fingerprint empty material.
pub fn destroy_data_key(
    data_root: &Path,
    handle: &mut DataKeyHandle,
) -> Result<DataKeyDestructionReceipt, StoreError> {
    let material = handle
        .material
        .take()
        .ok_or_else(|| StoreError::HeapAdmit("data key already destroyed".into()))?;
    let mut fingerprint_body = Vec::with_capacity(32 + material.len());
    fingerprint_body.extend_from_slice(&handle.heap_id);
    fingerprint_body.extend_from_slice(&handle.key_id);
    fingerprint_body.extend_from_slice(&material);
    if let Some(ext) = handle.external_key_id.as_deref() {
        fingerprint_body.extend_from_slice(ext.as_bytes());
    }
    let destroyed_fingerprint = domain_hash(DATA_KEY_DESTROY_DOMAIN, &fingerprint_body);
    // Best-effort wipe plaintext + envelope ciphertext before drop.
    let mut wiped = material;
    for b in &mut wiped {
        *b = 0;
    }
    drop(wiped);
    if let Some(mut ct) = handle.ciphertext_blob.take() {
        for b in &mut ct {
            *b = 0;
        }
        drop(ct);
    }

    let receipt = DataKeyDestructionReceipt {
        receipt_id: random_id()?,
        heap_id: handle.heap_id,
        key_id: handle.key_id,
        destroyed_fingerprint,
        created_at: now_secs(),
    };
    let dir = data_root
        .join("meta")
        .join(LIFECYCLE_DIR)
        .join(hex16(&receipt.heap_id));
    fs::create_dir_all(&dir)?;
    let bytes = encode_deterministic_uint_map(&[
        (1u64, CborValue::Text(HEAP_LIFECYCLE_PROFILE.into())),
        (2, CborValue::Bytes(receipt.receipt_id.to_vec())),
        (3, CborValue::Bytes(receipt.heap_id.to_vec())),
        (4, CborValue::Bytes(receipt.key_id.to_vec())),
        (5, CborValue::Bytes(receipt.destroyed_fingerprint.to_vec())),
        (6, CborValue::Uint(receipt.created_at)),
    ])
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    write_atomic(
        &dir.join(format!("data-key-destroy-{}.cbor", hex16(&receipt.key_id))),
        &bytes,
    )?;
    Ok(receipt)
}

/// Persist a permanent identity tombstone (never silently cleared by restore).
pub fn write_identity_tombstone(
    data_root: &Path,
    tombstone: &IdentityTombstone,
) -> Result<(), StoreError> {
    let dir = data_root
        .join("meta")
        .join(LIFECYCLE_DIR)
        .join(hex16(&tombstone.heap_id));
    fs::create_dir_all(&dir)?;
    let path = dir.join("identity-tombstone.v1.cbor");
    if path.exists() {
        let existing = load_identity_tombstone(data_root, &tombstone.heap_id)?;
        // Purged wins permanently; retired may upgrade to purged.
        if existing.kind == TombstoneKind::Purged {
            return Ok(());
        }
        if existing.kind == TombstoneKind::Retired && tombstone.kind == TombstoneKind::Retired {
            return Ok(());
        }
    }
    let mut body = Vec::new();
    body.extend_from_slice(&tombstone.heap_id);
    body.push(tombstone.kind as u8);
    body.extend_from_slice(&tombstone.authority_epoch.to_le_bytes());
    let integrity = domain_hash(TOMBSTONE_DOMAIN, &body);
    let bytes = encode_deterministic_uint_map(&[
        (1u64, CborValue::Text(HEAP_LIFECYCLE_PROFILE.into())),
        (2, CborValue::Bytes(tombstone.heap_id.to_vec())),
        (3, CborValue::Uint(tombstone.kind as u64)),
        (4, CborValue::Uint(tombstone.authority_epoch)),
        (5, CborValue::Uint(tombstone.created_at)),
        (6, CborValue::Bytes(integrity.to_vec())),
    ])
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    write_atomic(&path, &bytes)
}

/// Load a permanent identity tombstone if present.
pub fn load_identity_tombstone(
    data_root: &Path,
    heap_id: &[u8; 16],
) -> Result<IdentityTombstone, StoreError> {
    let path = data_root
        .join("meta")
        .join(LIFECYCLE_DIR)
        .join(hex16(heap_id))
        .join("identity-tombstone.v1.cbor");
    let bytes = fs::read(&path)
        .map_err(|e| StoreError::HeapAdmit(format!("identity tombstone missing: {e}")))?;
    let map =
        decode_deterministic_uint_map(&bytes).map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let mut by = BTreeMap::new();
    for (k, v) in map {
        by.insert(k, v);
    }
    let get = |k: u64| {
        by.get(&k)
            .cloned()
            .ok_or_else(|| StoreError::HeapAdmit(format!("missing tombstone key {k}")))
    };
    match get(1)? {
        CborValue::Text(s) if s == HEAP_LIFECYCLE_PROFILE => {}
        _ => return Err(StoreError::HeapAdmit("bad lifecycle profile".into())),
    }
    let kind_u = expect_u64(&get(3)?)? as u8;
    let kind = TombstoneKind::from_u8(kind_u)
        .ok_or_else(|| StoreError::HeapAdmit("bad tombstone kind".into()))?;
    Ok(IdentityTombstone {
        heap_id: expect_b16(&get(2)?)?,
        kind,
        authority_epoch: expect_u64(&get(4)?)?,
        created_at: expect_u64(&get(5)?)?,
    })
}

/// Payload restore MUST NOT clear a purged/retired identity tombstone.
pub fn refuse_clear_tombstone_via_payload_restore(
    data_root: &Path,
    heap_id: &[u8; 16],
) -> Result<(), StoreError> {
    if load_identity_tombstone(data_root, heap_id).is_ok() {
        return Err(StoreError::HeapAdmit(
            "identity tombstone is permanent; payload restore cannot clear it".into(),
        ));
    }
    Ok(())
}

/// Same-identity disaster-recovery restore (§17.4).
///
/// If a live authority for the same `HeapId` exists, restore stops unless an
/// explicit [`DisasterRecoveryCeremony`] fences the old deployment and advances
/// the authority epoch. Without a live conflict, ceremony is still required so
/// ordinary payload restore cannot mint a retained-ID head.
pub fn disaster_recovery_restore_retaining_id(
    data_root: &Path,
    package: &DisasterRecoveryPackage,
    ceremony: &DisasterRecoveryCeremony,
    live_conflict: Option<&HeapSlot>,
) -> Result<DisasterRecoveryTakeoverResult, StoreError> {
    if ceremony.heap_id != package.heap_id {
        return Err(StoreError::HeapAdmit(
            "DR ceremony heap_id does not match package".into(),
        ));
    }
    if ceremony.old_deployment_id != package.backup_deployment_id {
        return Err(StoreError::HeapAdmit(
            "DR ceremony old_deployment_id does not match backup".into(),
        ));
    }
    if ceremony.new_authority_epoch != ceremony.old_authority_epoch.saturating_add(1)
        || ceremony.new_authority_epoch == 0
    {
        return Err(StoreError::HeapAdmit(
            "DR ceremony must advance authority epoch by exactly one".into(),
        ));
    }
    if ceremony.old_authority_epoch != package.backup_authority_epoch {
        return Err(StoreError::HeapAdmit(
            "DR ceremony old epoch does not match backup".into(),
        ));
    }
    if ceremony.new_deployment_id == ceremony.old_deployment_id {
        return Err(StoreError::HeapAdmit(
            "DR takeover requires a distinct replacement DeploymentId".into(),
        ));
    }
    if ceremony.recovery_authority_evidence.iter().all(|b| *b == 0) {
        return Err(StoreError::HeapAdmit(
            "DR ceremony requires non-zero recovery authority evidence".into(),
        ));
    }
    // Purged identities cannot be revived by DR retain-ID.
    if let Ok(ts) = load_identity_tombstone(data_root, &package.heap_id) {
        if ts.kind == TombstoneKind::Purged {
            return Err(StoreError::HeapAdmit(
                "purged HeapId cannot be restored retaining identity".into(),
            ));
        }
    }

    if let Some(live) = live_conflict {
        let snap = live.load();
        if snap.heap_id.to_bytes() != package.heap_id {
            return Err(StoreError::HeapAdmit(
                "live conflict slot HeapId mismatch".into(),
            ));
        }
        if snap.deployment_id.to_bytes() != ceremony.old_deployment_id {
            return Err(StoreError::HeapAdmit(
                "live conflict DeploymentId is not the fenced old deployment".into(),
            ));
        }
        if snap.authority_epoch.get() != ceremony.old_authority_epoch {
            return Err(StoreError::HeapAdmit(
                "live conflict epoch is not the fenced old epoch".into(),
            ));
        }
        // Concurrent writable authority without matching ceremony stop is already
        // enforced by the checks above; install fenced takeover onto the slot.
    }

    let heap_id =
        HeapId::from_bytes(package.heap_id).map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let new_deployment = DeploymentId::from_bytes(ceremony.new_deployment_id)
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let new_epoch = AuthorityEpoch::new(ceremony.new_authority_epoch)
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let prev_rev = live_conflict
        .map(|s| s.load().security_revision.get())
        .unwrap_or(0);
    let security_revision = SecurityRevision::new(prev_rev.saturating_add(1).max(1))
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;

    let snapshot = HeapSecuritySnapshot {
        deployment_id: new_deployment,
        heap_id,
        authority_epoch: new_epoch,
        authority_generation: AuthorityGeneration::new(1)
            .map_err(|e| StoreError::HeapAdmit(e.to_string()))?,
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key: ceremony.new_master_public_key,
        previous_master_public_key: None,
        security_revision,
        authority_chain_head_hash: ceremony.recovery_authority_evidence,
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    };

    if let Some(live) = live_conflict {
        live.store(snapshot.clone());
    }

    // Persist payload under retained id + takeover evidence.
    let dir = data_root
        .join("restore")
        .join("dr")
        .join(hex16(&package.heap_id));
    fs::create_dir_all(&dir)?;
    write_atomic(&dir.join("payload.bin"), &package.payload)?;

    let mut evidence_body = Vec::new();
    evidence_body.extend_from_slice(&package.heap_id);
    evidence_body.extend_from_slice(&ceremony.old_deployment_id);
    evidence_body.extend_from_slice(&ceremony.new_deployment_id);
    evidence_body.extend_from_slice(&ceremony.old_authority_epoch.to_le_bytes());
    evidence_body.extend_from_slice(&ceremony.new_authority_epoch.to_le_bytes());
    evidence_body.extend_from_slice(&ceremony.new_master_public_key);
    evidence_body.extend_from_slice(&ceremony.recovery_authority_evidence);
    let takeover_evidence_hash = domain_hash(b"RESIDIUUM-HEAP-DR-TAKEOVER-V1", &evidence_body);

    let evidence_bytes = encode_deterministic_uint_map(&[
        (1u64, CborValue::Text(HEAP_LIFECYCLE_PROFILE.into())),
        (2, CborValue::Bytes(package.heap_id.to_vec())),
        (3, CborValue::Bytes(ceremony.old_deployment_id.to_vec())),
        (4, CborValue::Bytes(ceremony.new_deployment_id.to_vec())),
        (5, CborValue::Uint(ceremony.old_authority_epoch)),
        (6, CborValue::Uint(ceremony.new_authority_epoch)),
        (7, CborValue::Bytes(ceremony.new_master_public_key.to_vec())),
        (
            8,
            CborValue::Bytes(ceremony.recovery_authority_evidence.to_vec()),
        ),
        (9, CborValue::Bytes(takeover_evidence_hash.to_vec())),
        (10, CborValue::Uint(now_secs())),
    ])
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    write_atomic(&dir.join("takeover-evidence.v1.cbor"), &evidence_bytes)?;

    Ok(DisasterRecoveryTakeoverResult {
        snapshot,
        takeover_evidence_hash,
        fenced_deployment_id: ceremony.old_deployment_id,
    })
}

/// Refuse same-identity restore when a live authority exists and no ceremony is supplied.
pub fn refuse_retain_id_without_ceremony(
    live: &HeapSlot,
    package: &DisasterRecoveryPackage,
) -> Result<(), StoreError> {
    let snap = live.load();
    if snap.heap_id.to_bytes() != package.heap_id {
        return Ok(());
    }
    Err(StoreError::HeapAdmit(
        "same-identity restore stopped: concurrent live authority requires disaster-recovery ceremony".into(),
    ))
}

/// After takeover, credentials bound to the fenced old deployment are invalid.
pub fn old_deployment_credential_invalid(
    result: &DisasterRecoveryTakeoverResult,
    deployment_id: &[u8; 16],
) -> bool {
    *deployment_id == result.fenced_deployment_id
        || *deployment_id != result.snapshot.deployment_id.to_bytes()
}

/// Whether a labelled frame for `heap_id` still admits after local damage.
pub fn labelled_unit_readable(
    heap_id: &[u8; 16],
    frame_envelope: &[u8],
    segment_envelope: &[u8],
) -> bool {
    crate::heap::require_admit(heap_id, segment_envelope, frame_envelope, None).is_ok()
}

/// Encode a heap-binding envelope for damage-isolation fixtures.
pub fn heap_label_envelope(heap_id: &[u8; 16]) -> Result<Vec<u8>, StoreError> {
    encode_heap_binding_envelope(heap_id).map_err(|e| StoreError::HeapAdmit(e.to_string()))
}

impl HeapLifecycle {
    fn transition(
        &mut self,
        to: HeapAdministrativeState,
        operation_id: [u8; 16],
        op: &str,
    ) -> Result<(), StoreError> {
        let authority_guard = self
            .slot
            .lock_authority_frontier()
            .map_err(|_| StoreError::HeapAdmit("Heap authority frontier lock poisoned".into()))?;
        let mut next = (*authority_guard.load()).clone();
        let prev_rev = next.security_revision.get();
        next.administrative_state = to;
        next.security_revision = SecurityRevision::new(prev_rev + 1)
            .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
        authority_guard.store(next);
        failpoint::hit("heap_lifecycle.after_state_store")?;
        self.persist_transition_receipt(operation_id, op, to)?;
        failpoint::hit("heap_lifecycle.after_transition_receipt")?;
        // The transition receipt is durable before an Atomic may validate
        // against the new authority/lifecycle generation.
        drop(authority_guard);
        Ok(())
    }

    fn lifecycle_dir(&self) -> PathBuf {
        self.layout_root.join("meta").join(LIFECYCLE_DIR)
    }

    fn persist_transition_receipt(
        &self,
        operation_id: [u8; 16],
        op: &str,
        to: HeapAdministrativeState,
    ) -> Result<(), StoreError> {
        let snap = self.slot.load();
        let dir = self.lifecycle_dir().join(hex16(snap.heap_id.as_bytes()));
        fs::create_dir_all(&dir)?;
        let bytes = encode_deterministic_uint_map(&[
            (1u64, CborValue::Text(HEAP_LIFECYCLE_PROFILE.into())),
            (2, CborValue::Bytes(operation_id.to_vec())),
            (3, CborValue::Text(op.into())),
            (4, CborValue::Bytes(snap.heap_id.as_bytes().to_vec())),
            (5, CborValue::Uint(to.as_u8() as u64)),
            (6, CborValue::Uint(snap.security_revision.get())),
            (7, CborValue::Uint(now_secs())),
        ])
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
        let name = format!(
            "{}-{}-{}.cbor",
            snap.security_revision.get(),
            op,
            hex16(&operation_id)
        );
        write_atomic(&dir.join(name), &bytes)
    }

    fn persist_plan(&self, plan: &PurgePlan) -> Result<(), StoreError> {
        let dir = self.lifecycle_dir().join(hex16(&plan.heap_id));
        fs::create_dir_all(&dir)?;
        let mut cov = Vec::new();
        for id in &plan.coverage_ids {
            cov.push(CborValue::Bytes(id.to_vec()));
        }
        let mut units = Vec::new();
        for u in &plan.units {
            units.push(CborValue::Array(vec![
                CborValue::Bytes(u.object_id.to_vec()),
                CborValue::Bytes(u.domain.encode_key()),
                CborValue::Uint(if u.available { 1 } else { 0 }),
            ]));
        }
        let bytes = encode_deterministic_uint_map(&[
            (1u64, CborValue::Text(HEAP_LIFECYCLE_PROFILE.into())),
            (2, CborValue::Bytes(plan.operation_id.to_vec())),
            (3, CborValue::Bytes(plan.heap_id.to_vec())),
            (4, CborValue::Bytes(plan.coverage_hash.to_vec())),
            (5, CborValue::Array(cov)),
            (6, CborValue::Array(units)),
        ])
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
        write_atomic(&dir.join("purge-plan.v1.cbor"), &bytes)
    }

    fn persist_incomplete_purge(&self, result: &IncompletePurgeResult) -> Result<(), StoreError> {
        let dir = self.lifecycle_dir().join(hex16(&result.heap_id));
        fs::create_dir_all(&dir)?;
        let mut unavailable = Vec::new();
        for d in &result.unavailable_domains {
            unavailable.push(CborValue::Bytes(d.encode_key()));
        }
        let mut destroyed = Vec::new();
        for id in &result.destroyed_ids {
            destroyed.push(CborValue::Bytes(id.to_vec()));
        }
        let mut remaining = Vec::new();
        for id in &result.remaining_ids {
            remaining.push(CborValue::Bytes(id.to_vec()));
        }
        let bytes = encode_deterministic_uint_map(&[
            (1u64, CborValue::Text(HEAP_LIFECYCLE_PROFILE.into())),
            (2, CborValue::Bytes(result.operation_id.to_vec())),
            (3, CborValue::Bytes(result.heap_id.to_vec())),
            (4, CborValue::Array(unavailable)),
            (5, CborValue::Array(destroyed)),
            (6, CborValue::Array(remaining)),
            (7, CborValue::Bytes(result.result_hash.to_vec())),
            (8, CborValue::Uint(now_secs())),
        ])
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
        write_atomic(
            &dir.join(format!(
                "incomplete-purge-{}.cbor",
                hex16(&result.operation_id)
            )),
            &bytes,
        )
    }

    fn persist_purge_receipt(&self, receipt: &PurgeReceipt) -> Result<(), StoreError> {
        let dir = self.lifecycle_dir().join(hex16(&receipt.heap_id));
        fs::create_dir_all(&dir)?;
        let bytes = encode_purge_receipt(receipt)?;
        write_atomic(
            &dir.join(format!("purge-{}.cbor", hex16(&receipt.receipt_id))),
            &bytes,
        )
    }
}

/// Encode a purge receipt for durable storage / transport.
pub fn encode_purge_receipt(receipt: &PurgeReceipt) -> Result<Vec<u8>, StoreError> {
    encode_deterministic_uint_map(&[
        (1u64, CborValue::Text(HEAP_LIFECYCLE_PROFILE.into())),
        (2, CborValue::Bytes(receipt.receipt_id.to_vec())),
        (3, CborValue::Bytes(receipt.operation_id.to_vec())),
        (4, CborValue::Bytes(receipt.heap_id.to_vec())),
        (5, CborValue::Bytes(receipt.coverage_hash.to_vec())),
        (6, CborValue::Uint(receipt.security_revision)),
        (7, CborValue::Uint(receipt.created_at)),
    ])
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))
}

/// Decode a purge receipt.
pub fn decode_purge_receipt(bytes: &[u8]) -> Result<PurgeReceipt, StoreError> {
    let map =
        decode_deterministic_uint_map(bytes).map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let mut by = BTreeMap::new();
    for (k, v) in map {
        by.insert(k, v);
    }
    let get = |k: u64| {
        by.get(&k)
            .cloned()
            .ok_or_else(|| StoreError::HeapAdmit(format!("missing receipt key {k}")))
    };
    match get(1)? {
        CborValue::Text(s) if s == HEAP_LIFECYCLE_PROFILE => {}
        _ => return Err(StoreError::HeapAdmit("bad lifecycle profile".into())),
    }
    Ok(PurgeReceipt {
        receipt_id: expect_b16(&get(2)?)?,
        operation_id: expect_b16(&get(3)?)?,
        heap_id: expect_b16(&get(4)?)?,
        coverage_hash: expect_b32(&get(5)?)?,
        security_revision: expect_u64(&get(6)?)?,
        created_at: expect_u64(&get(7)?)?,
    })
}

fn coverage_hash(ids: &[[u8; 16]]) -> [u8; 32] {
    let mut sorted = ids.to_vec();
    sorted.sort();
    let mut body = Vec::new();
    for id in sorted {
        body.extend_from_slice(&id);
    }
    domain_hash(PURGE_COVERAGE_DOMAIN, &body)
}

fn domain_hash(domain: &[u8], body: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(domain);
    h.update(&[0u8]);
    h.update(body);
    *h.finalize().as_bytes()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn expect_u64(v: &CborValue) -> Result<u64, StoreError> {
    match v {
        CborValue::Uint(u) => Ok(*u),
        _ => Err(StoreError::HeapAdmit("expected uint".into())),
    }
}
fn expect_b16(v: &CborValue) -> Result<[u8; 16], StoreError> {
    match v {
        CborValue::Bytes(b) if b.len() == 16 => {
            let mut a = [0u8; 16];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(StoreError::HeapAdmit("expected bstr16".into())),
    }
}
fn expect_b32(v: &CborValue) -> Result<[u8; 32], StoreError> {
    match v {
        CborValue::Bytes(b) if b.len() == 32 => {
            let mut a = [0u8; 32];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(StoreError::HeapAdmit("expected bstr32".into())),
    }
}

/// Helper: build a minimal active snapshot for lifecycle tests / reload wiring.
pub fn active_snapshot(
    deployment_id: residiuum_heap::DeploymentId,
    heap_id: HeapId,
    master_public_key: [u8; 32],
) -> Result<HeapSecuritySnapshot, StoreError> {
    Ok(HeapSecuritySnapshot {
        deployment_id,
        heap_id,
        authority_epoch: residiuum_heap::AuthorityEpoch::new(1)
            .map_err(|e| StoreError::HeapAdmit(e.to_string()))?,
        authority_generation: residiuum_heap::AuthorityGeneration::new(1)
            .map_err(|e| StoreError::HeapAdmit(e.to_string()))?,
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key,
        previous_master_public_key: None,
        security_revision: SecurityRevision::new(1)
            .map_err(|e| StoreError::HeapAdmit(e.to_string()))?,
        authority_chain_head_hash: [0u8; 32],
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    })
}
