//! Live AWS KMS data-key connector (H4 / CPR-003).
//!
//! Feature `aws-kms`. Talks to **real** KMS over HTTPS using SigV4 (no in-process
//! mock). Optional endpoint override targets LocalStack / VPC endpoints while
//! still using the same wire protocol.
//!
//! ## Ops
//! - **generate**: `GenerateDataKey` (AES_256) under a CMK → envelope DEK
//! - **destroy**: wipe local plaintext + ciphertext; durable receipt  
//!   (does **not** `ScheduleKeyDeletion` on a shared CMK)
//!
//! ## Env (`HsmDataKeyConfig::aws_kms_from_env` / [`AwsKmsDataKeyProvider::from_env`])
//! - `RESIDIUUM_AWS_KMS_KEY_ID` or `RESIDIUUM_KMS_KEY_ARN` — CMK id/ARN (**required**)
//! - `AWS_REGION` / `RESIDIUUM_AWS_REGION` — region (default `us-east-1`)
//! - `RESIDIUUM_AWS_ENDPOINT_URL` / `AWS_ENDPOINT_URL` — optional base URL
//! - Credentials: standard AWS chain via env  
//!   (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`)
//!
//! Live Accept: `RESIDIUUM_KMS_LIVE=1` + creds + CMK, then  
//! `cargo test -p residiuum-store --features aws-kms aws_kms_live -- --ignored --nocapture`.

use crate::error::StoreError;
use crate::heap::lifecycle::{
    destroy_data_key, DataKeyDestructionReceipt, DataKeyHandle, DataKeyProvider, HsmCapabilities,
    HsmDataKeyConfig,
};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Live AWS KMS data-key provider (production cloud path).
#[derive(Clone)]
pub struct AwsKmsDataKeyProvider {
    /// CMK id or ARN used as KEK for GenerateDataKey.
    cmk_key_id: String,
    region: String,
    /// Full service URL, e.g. `https://kms.us-east-1.amazonaws.com/`.
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl std::fmt::Debug for AwsKmsDataKeyProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsKmsDataKeyProvider")
            .field("cmk_key_id", &self.cmk_key_id)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &self.access_key_id)
            .field("has_session_token", &self.session_token.is_some())
            .finish()
    }
}

impl AwsKmsDataKeyProvider {
    /// Build from [`HsmDataKeyConfig`] (backend must be AwsKms with key + region).
    pub fn from_config(config: &HsmDataKeyConfig) -> Result<Self, StoreError> {
        if config.backend != crate::HsmBackendKind::AwsKms {
            return Err(StoreError::HeapAdmit(
                "AwsKmsDataKeyProvider requires HsmBackendKind::AwsKms".into(),
            ));
        }
        let cmk = config
            .key_label
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| StoreError::HeapAdmit("AWS KMS CMK id/ARN missing (key_label)".into()))?
            .to_string();
        let region = config
            .slot_or_region
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("us-east-1")
            .to_string();
        let endpoint = config
            .library_or_endpoint
            .clone()
            .unwrap_or_else(|| format!("https://kms.{region}.amazonaws.com"));
        Self::connect(region, cmk, Some(endpoint))
    }

    /// Connect using region, CMK, optional endpoint override (LocalStack / custom).
    pub fn connect(
        region: impl Into<String>,
        cmk_key_id: impl Into<String>,
        endpoint: Option<String>,
    ) -> Result<Self, StoreError> {
        let region = region.into();
        let cmk_key_id = cmk_key_id.into();
        if cmk_key_id.is_empty() {
            return Err(StoreError::HeapAdmit("empty AWS KMS CMK id".into()));
        }
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").map_err(|_| {
            StoreError::HeapAdmit("AWS_ACCESS_KEY_ID not set (required for live KMS)".into())
        })?;
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| {
            StoreError::HeapAdmit("AWS_SECRET_ACCESS_KEY not set (required for live KMS)".into())
        })?;
        let session_token = std::env::var("AWS_SESSION_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());
        let endpoint = endpoint
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("https://kms.{region}.amazonaws.com"));
        let endpoint = endpoint.trim_end_matches('/').to_string();
        Ok(Self {
            cmk_key_id,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
        })
    }

    /// From environment (see module docs). Fails if CMK or credentials unset.
    pub fn from_env() -> Result<Self, StoreError> {
        let cfg = HsmDataKeyConfig::aws_kms_from_env().ok_or_else(|| {
            StoreError::HeapAdmit(
                "AWS KMS env not set (need RESIDIUUM_AWS_KMS_KEY_ID or RESIDIUUM_KMS_KEY_ARN)"
                    .into(),
            )
        })?;
        Self::from_config(&cfg)
    }

    /// CMK id/ARN.
    pub fn cmk_key_id(&self) -> &str {
        &self.cmk_key_id
    }

    /// Region string.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Service endpoint (may be LocalStack).
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Honest capability advertisement for AWS KMS envelope DEKs.
    pub fn capabilities(&self) -> HsmCapabilities {
        HsmCapabilities {
            generate: true,
            destroy: true,
            never_export_plaintext: false, // GenerateDataKey returns DEK plaintext once
            production_hsm: true,
        }
    }

    fn generate_data_key(&self, heap_id: [u8; 16]) -> Result<DataKeyHandle, StoreError> {
        let heap_hex = crate::layout::hex16(&heap_id);
        let body = serde_json::json!({
            "KeyId": self.cmk_key_id,
            "KeySpec": "AES_256",
            "EncryptionContext": {
                "residiuum-heap-id": heap_hex,
                "residiuum-profile": "residiuum-heap-v1",
            }
        });
        let body_bytes = serde_json::to_vec(&body)
            .map_err(|e| StoreError::HeapAdmit(format!("kms body: {e}")))?;
        let resp = self.signed_post("TrentService.GenerateDataKey", &body_bytes)?;
        let v: serde_json::Value = serde_json::from_str(&resp)
            .map_err(|e| StoreError::HeapAdmit(format!("kms response json: {e}")))?;
        if let Some(err) = v.get("__type").or_else(|| v.get("message")) {
            return Err(StoreError::HeapAdmit(format!("AWS KMS error: {err} / {v}")));
        }
        let plaintext_b64 = v
            .get("Plaintext")
            .and_then(|x| x.as_str())
            .ok_or_else(|| StoreError::HeapAdmit("KMS GenerateDataKey missing Plaintext".into()))?;
        let ciphertext_b64 = v
            .get("CiphertextBlob")
            .and_then(|x| x.as_str())
            .ok_or_else(|| {
                StoreError::HeapAdmit("KMS GenerateDataKey missing CiphertextBlob".into())
            })?;
        let plaintext = b64_decode(plaintext_b64)?;
        let ciphertext = b64_decode(ciphertext_b64)?;
        if plaintext.len() != 32 {
            return Err(StoreError::HeapAdmit(format!(
                "expected 32-byte AES-256 DEK, got {}",
                plaintext.len()
            )));
        }
        let external = v
            .get("KeyId")
            .and_then(|x| x.as_str())
            .unwrap_or(self.cmk_key_id.as_str())
            .to_string();
        DataKeyHandle::generate_envelope(heap_id, &plaintext, ciphertext, external, "aws-kms")
    }

    fn signed_post(&self, target: &str, body: &[u8]) -> Result<String, StoreError> {
        let url = format!("{}/", self.endpoint.trim_end_matches('/'));
        let host = host_from_url(&url)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StoreError::HeapAdmit(format!("clock: {e}")))?;
        let secs = now.as_secs() as i64;
        let amz_date = format_amz_date(secs);
        let date_stamp = &amz_date[..8];
        let payload_hash = hex::encode(Sha256::digest(body));

        let mut headers: Vec<(String, String)> = vec![
            ("content-type".into(), "application/x-amz-json-1.1".into()),
            ("host".into(), host.clone()),
            ("x-amz-date".into(), amz_date.clone()),
            ("x-amz-target".into(), target.into()),
            ("x-amz-content-sha256".into(), payload_hash.clone()),
        ];
        if let Some(ref tok) = self.session_token {
            headers.push(("x-amz-security-token".into(), tok.clone()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));

        let signed_headers = headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers = headers
            .iter()
            .map(|(k, v)| format!("{k}:{}\n", v.trim()))
            .collect::<String>();
        let canonical_request =
            format!("POST\n/\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
        let credential_scope = format!("{date_stamp}/{}/kms/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let signing_key =
            aws4_signing_key(&self.secret_access_key, date_stamp, &self.region, "kms")?;
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes())?);
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key_id
        );

        let mut req = ureq::post(&url)
            .set("Authorization", &auth)
            .set("Content-Type", "application/x-amz-json-1.1")
            .set("X-Amz-Date", &amz_date)
            .set("X-Amz-Target", target)
            .set("X-Amz-Content-Sha256", &payload_hash);
        if let Some(ref tok) = self.session_token {
            req = req.set("X-Amz-Security-Token", tok);
        }
        let resp = req
            .send_bytes(body)
            .map_err(|e| StoreError::HeapAdmit(format!("AWS KMS HTTP: {e}")))?;
        let status = resp.status();
        let text = resp
            .into_string()
            .map_err(|e| StoreError::HeapAdmit(format!("AWS KMS body: {e}")))?;
        if !(200..300).contains(&status) {
            return Err(StoreError::HeapAdmit(format!(
                "AWS KMS HTTP {status}: {text}"
            )));
        }
        Ok(text)
    }
}

impl DataKeyProvider for AwsKmsDataKeyProvider {
    fn provider_id(&self) -> &'static str {
        "aws-kms"
    }

    fn generate(&self, heap_id: [u8; 16]) -> Result<DataKeyHandle, StoreError> {
        self.generate_data_key(heap_id)
    }

    fn destroy(
        &self,
        data_root: &Path,
        handle: &mut DataKeyHandle,
    ) -> Result<DataKeyDestructionReceipt, StoreError> {
        destroy_data_key(data_root, handle)
    }
}

/// Shared handle for optional Arc wrapping (session caches).
pub type SharedAwsKmsDataKeyProvider = Arc<AwsKmsDataKeyProvider>;

fn host_from_url(url: &str) -> Result<String, StoreError> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or_else(|| StoreError::HeapAdmit(format!("invalid KMS endpoint URL: {url}")))?;
    let host = rest.split('/').next().unwrap_or(rest);
    let host = host.split('@').next_back().unwrap_or(host);
    Ok(host.to_string())
}

fn format_amz_date(secs: i64) -> String {
    // UTC YYYYMMDD'T'HHMMSS'Z' without chrono dependency.
    let days = secs.div_euclid(86400);
    let tod = secs.rem_euclid(86400) as u32;
    let (y, m, d) = civil_from_days(days);
    let hh = tod / 3600;
    let mm = (tod % 3600) / 60;
    let ss = tod % 60;
    format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

/// Howard Hinnant civil_from_days (UTC).
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>, StoreError> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|e| StoreError::HeapAdmit(format!("hmac key: {e}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn aws4_signing_key(
    secret: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
) -> Result<Vec<u8>, StoreError> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes())?;
    let k_region = hmac_sha256(&k_date, region.as_bytes())?;
    let k_service = hmac_sha256(&k_region, service.as_bytes())?;
    hmac_sha256(&k_service, b"aws4_request")
}

fn b64_decode(s: &str) -> Result<Vec<u8>, StoreError> {
    // Standard base64 (KMS JSON uses standard alphabet).
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let s = s.trim().as_bytes();
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut buf = 0u32;
    let mut n = 0;
    for &c in s {
        if c == b'=' {
            break;
        }
        if c == b'\n' || c == b'\r' || c == b' ' {
            continue;
        }
        let v = val(c).ok_or_else(|| StoreError::HeapAdmit("invalid base64".into()))?;
        buf = (buf << 6) | u32::from(v);
        n += 6;
        if n >= 8 {
            n -= 8;
            out.push((buf >> n) as u8);
            buf &= (1 << n) - 1;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HsmBackendKind;

    #[test]
    fn from_config_requires_aws_backend_and_cmk() {
        let bad = HsmDataKeyConfig::unconfigured(HsmBackendKind::Pkcs11);
        assert!(AwsKmsDataKeyProvider::from_config(&bad).is_err());
        let missing_key = HsmDataKeyConfig {
            backend: HsmBackendKind::AwsKms,
            library_or_endpoint: None,
            slot_or_region: Some("us-west-2".into()),
            key_label: None,
            mock_enabled: false,
        };
        assert!(AwsKmsDataKeyProvider::from_config(&missing_key).is_err());
    }

    #[test]
    fn aws_kms_config_helpers() {
        let cfg = HsmDataKeyConfig::aws_kms("eu-west-1", "alias/residiuum-test", None);
        assert_eq!(cfg.backend, HsmBackendKind::AwsKms);
        assert_eq!(cfg.slot_or_region.as_deref(), Some("eu-west-1"));
        assert_eq!(cfg.key_label.as_deref(), Some("alias/residiuum-test"));
    }

    #[test]
    fn connect_requires_credentials_in_env() {
        // Clear is hard without mutating process env permanently; empty CMK still fails first.
        let err = AwsKmsDataKeyProvider::connect("us-east-1", "", None).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn civil_date_roundtrip_smoke() {
        // 2024-01-01 00:00:00 UTC ≈ 1704067200
        let s = format_amz_date(1_704_067_200);
        assert!(s.starts_with("2024"), "{s}");
        assert!(s.ends_with('Z'), "{s}");
        assert_eq!(s.len(), 16);
    }

    #[test]
    fn b64_roundtrip_known() {
        // "hello" base64
        let d = b64_decode("aGVsbG8=").unwrap();
        assert_eq!(d, b"hello");
    }
}
