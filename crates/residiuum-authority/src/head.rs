//! Authority head payload (`HEAP_SPEC` §35.1).

use crate::error::AuthorityStoreError;
use residiuum_format::{decode_deterministic_uint_map, encode_deterministic_uint_map, CborValue};
use residiuum_heap::{BlacklistEntry, BlacklistKind, HeapAdministrativeState, Rights};

/// Recovery profile wire value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecoveryProfile {
    /// No master recovery.
    NoMasterRecovery = 1,
    /// Threshold recovery (not fully exercised in HP-005 cut).
    ThresholdMasterRecovery = 2,
}

/// Resident access policy (§35.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessPolicy {
    /// Allowed rights mask.
    pub allowed_rights_mask: u64,
    /// Policy profile version (exactly 1).
    pub policy_profile_version: u64,
}

impl AccessPolicy {
    /// Default: no additional narrowing (`0x1ffff`, empty constraints, v1).
    pub fn default_open() -> Self {
        Self {
            allowed_rights_mask: 0x1_ffff,
            policy_profile_version: 1,
        }
    }

    fn encode(&self) -> CborValue {
        CborValue::Map(vec![
            (1u64, CborValue::Uint(self.allowed_rights_mask)),
            (2, CborValue::Array(vec![])),
            (3, CborValue::Uint(self.policy_profile_version)),
        ])
    }

    fn decode(v: &CborValue) -> Result<Self, AuthorityStoreError> {
        let CborValue::Map(entries) = v else {
            return Err(AuthorityStoreError::Corrupt("policy map"));
        };
        let mut rights = None;
        let mut version = None;
        for (k, val) in entries {
            match *k {
                1 => match val {
                    CborValue::Uint(u) => rights = Some(*u),
                    _ => return Err(AuthorityStoreError::Corrupt("policy rights")),
                },
                2 => match val {
                    CborValue::Array(a) if a.is_empty() => {}
                    _ => return Err(AuthorityStoreError::Corrupt("policy constraints")),
                },
                3 => match val {
                    CborValue::Uint(u) => version = Some(*u),
                    _ => return Err(AuthorityStoreError::Corrupt("policy version")),
                },
                _ => return Err(AuthorityStoreError::Corrupt("policy unknown")),
            }
        }
        let version = version.ok_or(AuthorityStoreError::Corrupt("policy version"))?;
        if version != 1 {
            return Err(AuthorityStoreError::Corrupt("policy version"));
        }
        Ok(Self {
            allowed_rights_mask: rights.ok_or(AuthorityStoreError::Corrupt("policy rights"))?,
            policy_profile_version: version,
        })
    }
}

/// Validated authority head payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityHead {
    /// Deployment.
    pub deployment_id: [u8; 16],
    /// Heap.
    pub heap_id: [u8; 16],
    /// Authority epoch.
    pub authority_epoch: u64,
    /// Security revision.
    pub security_revision: u64,
    /// Authority revision.
    pub authority_revision: u64,
    /// State revision.
    pub state_revision: u64,
    /// Policy revision.
    pub policy_revision: u64,
    /// Administrative state.
    pub heap_state: HeapAdministrativeState,
    /// Current master generation.
    pub master_generation: u64,
    /// Current Ed25519 public key.
    pub master_public_key: [u8; 32],
    /// Previous generation (during grace).
    pub previous_generation: Option<u64>,
    /// Previous public key.
    pub previous_public_key: Option<[u8; 32]>,
    /// Grace deadline unix seconds.
    pub grace_deadline: Option<u64>,
    /// Blacklist.
    pub blacklist: Vec<BlacklistEntry>,
    /// Trusted time floor seconds.
    pub trusted_time_floor: u64,
    /// Authority-chain head hash.
    pub authority_chain_head_hash: [u8; 32],
    /// Recovery profile.
    pub recovery_profile: RecoveryProfile,
    /// File sequence.
    pub file_sequence: u64,
    /// Access policy.
    pub access_policy: AccessPolicy,
    /// Immutable storage genesis descriptor hash.
    pub storage_genesis_hash: [u8; 32],
    /// Current heap-descriptor hash.
    pub current_descriptor_hash: [u8; 32],
}

impl AuthorityHead {
    /// Encode canonical head payload (without slot wrapper).
    pub fn encode_payload(&self) -> Result<Vec<u8>, AuthorityStoreError> {
        self.validate_shape()?;
        let prev_gen = match self.previous_generation {
            None => CborValue::Null,
            Some(g) => CborValue::Uint(g),
        };
        let prev_pk = match self.previous_public_key {
            None => CborValue::Null,
            Some(pk) => CborValue::Bytes(pk.to_vec()),
        };
        let grace = match self.grace_deadline {
            None => CborValue::Null,
            Some(g) => CborValue::Uint(g),
        };
        let blacklist = CborValue::Array(
            self.blacklist
                .iter()
                .map(|e| {
                    CborValue::Map(vec![
                        (1u64, CborValue::Uint(e.kind as u64)),
                        (2, CborValue::Uint(e.generation)),
                        (3, CborValue::Bytes(e.fingerprint.to_vec())),
                    ])
                })
                .collect(),
        );
        encode_deterministic_uint_map(&[
            (1u64, CborValue::Uint(1)),
            (2, CborValue::Bytes(self.deployment_id.to_vec())),
            (3, CborValue::Bytes(self.heap_id.to_vec())),
            (4, CborValue::Uint(self.authority_epoch)),
            (5, CborValue::Uint(self.security_revision)),
            (6, CborValue::Uint(self.authority_revision)),
            (7, CborValue::Uint(self.state_revision)),
            (8, CborValue::Uint(self.policy_revision)),
            (9, CborValue::Uint(self.heap_state.as_u8() as u64)),
            (10, CborValue::Uint(self.master_generation)),
            (11, CborValue::Bytes(self.master_public_key.to_vec())),
            (12, prev_gen),
            (13, prev_pk),
            (14, grace),
            (15, blacklist),
            (16, CborValue::Uint(self.trusted_time_floor)),
            (
                17,
                CborValue::Bytes(self.authority_chain_head_hash.to_vec()),
            ),
            (18, CborValue::Uint(self.recovery_profile as u64)),
            (19, CborValue::Array(vec![])),
            (20, CborValue::Uint(0)),
            (21, CborValue::Uint(0)),
            (22, CborValue::Uint(self.file_sequence)),
            (23, self.access_policy.encode()),
            (24, CborValue::Null),
            (25, CborValue::Bytes(self.storage_genesis_hash.to_vec())),
            (26, CborValue::Bytes(self.current_descriptor_hash.to_vec())),
        ])
        .map_err(|_| AuthorityStoreError::Corrupt("head encode"))
    }

    /// Decode and validate a head payload.
    pub fn decode_payload(bytes: &[u8]) -> Result<Self, AuthorityStoreError> {
        let map = decode_deterministic_uint_map(bytes)
            .map_err(|_| AuthorityStoreError::Corrupt("head cbor"))?;
        let mut profile = None;
        let mut deployment = None;
        let mut heap = None;
        let mut epoch = None;
        let mut sec_rev = None;
        let mut auth_rev = None;
        let mut state_rev = None;
        let mut pol_rev = None;
        let mut state = None;
        let mut gen = None;
        let mut pk = None;
        let mut prev_gen = None;
        let mut prev_pk = None;
        let mut grace = None;
        let mut blacklist = None;
        let mut floor = None;
        let mut chain = None;
        let mut recovery = None;
        let mut file_seq = None;
        let mut policy = None;
        let mut genesis = None;
        let mut tip = None;
        for (k, v) in map {
            match k {
                1 => profile = Some(expect_uint(&v)?),
                2 => deployment = Some(expect_b16(&v)?),
                3 => heap = Some(expect_b16(&v)?),
                4 => epoch = Some(expect_uint(&v)?),
                5 => sec_rev = Some(expect_uint(&v)?),
                6 => auth_rev = Some(expect_uint(&v)?),
                7 => state_rev = Some(expect_uint(&v)?),
                8 => pol_rev = Some(expect_uint(&v)?),
                9 => {
                    let u = expect_uint(&v)? as u8;
                    state = Some(
                        HeapAdministrativeState::from_u8(u)
                            .ok_or(AuthorityStoreError::Corrupt("heap state"))?,
                    );
                }
                10 => gen = Some(expect_uint(&v)?),
                11 => pk = Some(expect_b32(&v)?),
                12 => {
                    prev_gen = Some(match v {
                        CborValue::Null => None,
                        other => Some(expect_uint(&other)?),
                    })
                }
                13 => {
                    prev_pk = Some(match v {
                        CborValue::Null => None,
                        other => Some(expect_b32(&other)?),
                    })
                }
                14 => {
                    grace = Some(match v {
                        CborValue::Null => None,
                        other => Some(expect_uint(&other)?),
                    })
                }
                15 => blacklist = Some(decode_blacklist(&v)?),
                16 => floor = Some(expect_uint(&v)?),
                17 => chain = Some(expect_b32(&v)?),
                18 => {
                    recovery = Some(match expect_uint(&v)? {
                        1 => RecoveryProfile::NoMasterRecovery,
                        2 => RecoveryProfile::ThresholdMasterRecovery,
                        _ => return Err(AuthorityStoreError::Corrupt("recovery profile")),
                    })
                }
                19 => match v {
                    CborValue::Array(a) if a.is_empty() => {}
                    _ => return Err(AuthorityStoreError::Corrupt("recovery keys")),
                },
                20 => {
                    if expect_uint(&v)? != 0 {
                        return Err(AuthorityStoreError::Corrupt("recovery threshold"));
                    }
                }
                21 => {
                    if expect_uint(&v)? != 0 {
                        return Err(AuthorityStoreError::Corrupt("tombstone"));
                    }
                }
                22 => file_seq = Some(expect_uint(&v)?),
                23 => policy = Some(AccessPolicy::decode(&v)?),
                24 => match v {
                    CborValue::Null => {}
                    _ => return Err(AuthorityStoreError::Corrupt("resume state")),
                },
                25 => genesis = Some(expect_b32(&v)?),
                26 => tip = Some(expect_b32(&v)?),
                _ => return Err(AuthorityStoreError::Corrupt("head unknown key")),
            }
        }
        if profile != Some(1) {
            return Err(AuthorityStoreError::Corrupt("head profile"));
        }
        let head = Self {
            deployment_id: deployment.ok_or(AuthorityStoreError::Corrupt("deployment"))?,
            heap_id: heap.ok_or(AuthorityStoreError::Corrupt("heap"))?,
            authority_epoch: epoch.ok_or(AuthorityStoreError::Corrupt("epoch"))?,
            security_revision: sec_rev.ok_or(AuthorityStoreError::Corrupt("sec rev"))?,
            authority_revision: auth_rev.ok_or(AuthorityStoreError::Corrupt("auth rev"))?,
            state_revision: state_rev.ok_or(AuthorityStoreError::Corrupt("state rev"))?,
            policy_revision: pol_rev.ok_or(AuthorityStoreError::Corrupt("pol rev"))?,
            heap_state: state.ok_or(AuthorityStoreError::Corrupt("state"))?,
            master_generation: gen.ok_or(AuthorityStoreError::Corrupt("generation"))?,
            master_public_key: pk.ok_or(AuthorityStoreError::Corrupt("master pk"))?,
            previous_generation: prev_gen.ok_or(AuthorityStoreError::Corrupt("prev gen"))?,
            previous_public_key: prev_pk.ok_or(AuthorityStoreError::Corrupt("prev pk"))?,
            grace_deadline: grace.ok_or(AuthorityStoreError::Corrupt("grace"))?,
            blacklist: blacklist.ok_or(AuthorityStoreError::Corrupt("blacklist"))?,
            trusted_time_floor: floor.ok_or(AuthorityStoreError::Corrupt("floor"))?,
            authority_chain_head_hash: chain.ok_or(AuthorityStoreError::Corrupt("chain"))?,
            recovery_profile: recovery.ok_or(AuthorityStoreError::Corrupt("recovery"))?,
            file_sequence: file_seq.ok_or(AuthorityStoreError::Corrupt("file seq"))?,
            access_policy: policy.ok_or(AuthorityStoreError::Corrupt("policy"))?,
            storage_genesis_hash: genesis.ok_or(AuthorityStoreError::Corrupt("genesis"))?,
            current_descriptor_hash: tip.ok_or(AuthorityStoreError::Corrupt("tip"))?,
        };
        head.validate_shape()?;
        let _ = Rights::from_bits_certificate(head.access_policy.allowed_rights_mask)
            .map_err(|_| AuthorityStoreError::Corrupt("policy rights bits"))?;
        Ok(head)
    }

    fn validate_shape(&self) -> Result<(), AuthorityStoreError> {
        let prev_present = self.previous_generation.is_some();
        if prev_present != self.previous_public_key.is_some()
            || prev_present != self.grace_deadline.is_some()
        {
            return Err(AuthorityStoreError::Corrupt("prev grace triple"));
        }
        if let Some(pg) = self.previous_generation {
            if pg + 1 != self.master_generation {
                return Err(AuthorityStoreError::Corrupt("prev generation"));
            }
        }
        if self.authority_epoch == 0
            || self.security_revision == 0
            || self.authority_revision == 0
            || self.file_sequence == 0
        {
            return Err(AuthorityStoreError::Corrupt("zero revision"));
        }
        if self.master_generation == 0 {
            return Err(AuthorityStoreError::Corrupt("zero generation"));
        }
        if self.recovery_profile == RecoveryProfile::NoMasterRecovery {
            // keys empty + threshold 0 already enforced at decode
        }
        Ok(())
    }
}

fn decode_blacklist(v: &CborValue) -> Result<Vec<BlacklistEntry>, AuthorityStoreError> {
    let CborValue::Array(items) = v else {
        return Err(AuthorityStoreError::Corrupt("blacklist"));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let CborValue::Map(fields) = item else {
            return Err(AuthorityStoreError::Corrupt("blacklist entry"));
        };
        let mut kind = None;
        let mut gen = None;
        let mut fp = None;
        for (k, val) in fields {
            match *k {
                1 => {
                    kind = Some(match expect_uint(val)? {
                        1 => BlacklistKind::CertificateHash,
                        2 => BlacklistKind::HolderPublicKeyHash,
                        _ => return Err(AuthorityStoreError::Corrupt("blacklist kind")),
                    })
                }
                2 => gen = Some(expect_uint(val)?),
                3 => fp = Some(expect_b32(val)?),
                _ => return Err(AuthorityStoreError::Corrupt("blacklist key")),
            }
        }
        out.push(BlacklistEntry {
            kind: kind.ok_or(AuthorityStoreError::Corrupt("blacklist kind"))?,
            generation: gen.ok_or(AuthorityStoreError::Corrupt("blacklist gen"))?,
            fingerprint: fp.ok_or(AuthorityStoreError::Corrupt("blacklist fp"))?,
        });
    }
    Ok(out)
}

fn expect_uint(v: &CborValue) -> Result<u64, AuthorityStoreError> {
    match v {
        CborValue::Uint(u) => Ok(*u),
        _ => Err(AuthorityStoreError::Corrupt("uint")),
    }
}

fn expect_b16(v: &CborValue) -> Result<[u8; 16], AuthorityStoreError> {
    match v {
        CborValue::Bytes(b) if b.len() == 16 => {
            let mut a = [0u8; 16];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(AuthorityStoreError::Corrupt("bstr16")),
    }
}

fn expect_b32(v: &CborValue) -> Result<[u8; 32], AuthorityStoreError> {
    match v {
        CborValue::Bytes(b) if b.len() == 32 => {
            let mut a = [0u8; 32];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(AuthorityStoreError::Corrupt("bstr32")),
    }
}
