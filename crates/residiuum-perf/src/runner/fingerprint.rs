//! Environment fingerprint for run validity / comparison (SPEC §15).

use super::platform::{detect_adapter, free_space_bytes, free_space_inodes};
use super::BuildMode;
use super::RunnerError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentFingerprint {
    pub schema: String,
    pub platform_adapter: String,
    pub os: String,
    pub arch: String,
    pub family: String,
    pub pointer_width: String,
    pub endian: String,
    pub cpu_count: usize,
    /// Free bytes at capture time (volatile; excluded from stable hash).
    pub free_bytes: Option<u64>,
    pub free_inodes: Option<u64>,
    pub build_mode: String,
    pub debug_assertions: bool,
    pub rustc_opt_level: Option<String>,
    /// SHA-256 hex of the stable identity fields (excludes free_*).
    pub environment_hash: String,
}

pub fn environment_fingerprint(
    work_path: Option<&Path>,
    build_mode: BuildMode,
) -> Result<EnvironmentFingerprint, RunnerError> {
    let adapter = detect_adapter();
    let (free_bytes, free_inodes) = if let Some(p) = work_path {
        (
            free_space_bytes(p).ok(),
            free_space_inodes(p).ok().flatten(),
        )
    } else {
        (None, None)
    };

    let mut fp = EnvironmentFingerprint {
        schema: "residiuum-perf-environment-v1".into(),
        platform_adapter: adapter.as_str().into(),
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        family: std::env::consts::FAMILY.into(),
        pointer_width: (std::mem::size_of::<usize>() * 8).to_string(),
        endian: if cfg!(target_endian = "little") {
            "little".into()
        } else {
            "big".into()
        },
        cpu_count: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        free_bytes,
        free_inodes,
        build_mode: build_mode.as_str().into(),
        debug_assertions: cfg!(debug_assertions),
        rustc_opt_level: option_env!("OPT_LEVEL").map(|s| s.to_string()),
        environment_hash: String::new(),
    };
    fp.environment_hash = stable_environment_hash(&fp);
    let _ = adapter;
    Ok(fp)
}

/// Hash only comparison-stable fields (SPEC: environment class match).
fn stable_environment_hash(fp: &EnvironmentFingerprint) -> String {
    let mut h = Sha256::new();
    h.update(fp.schema.as_bytes());
    h.update(b"|");
    h.update(fp.platform_adapter.as_bytes());
    h.update(b"|");
    h.update(fp.os.as_bytes());
    h.update(b"|");
    h.update(fp.arch.as_bytes());
    h.update(b"|");
    h.update(fp.family.as_bytes());
    h.update(b"|");
    h.update(fp.pointer_width.as_bytes());
    h.update(b"|");
    h.update(fp.endian.as_bytes());
    h.update(b"|");
    h.update(fp.cpu_count.to_string().as_bytes());
    h.update(b"|");
    h.update(fp.build_mode.as_bytes());
    h.update(b"|");
    h.update(if fp.debug_assertions { b"1" } else { b"0" });
    h.update(b"|");
    if let Some(ref o) = fp.rustc_opt_level {
        h.update(o.as_bytes());
    }
    hex::encode(h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_stable_across_calls_excluding_free() {
        let a = environment_fingerprint(None, BuildMode::Diagnostic).unwrap();
        let b = environment_fingerprint(None, BuildMode::Diagnostic).unwrap();
        assert_eq!(a.environment_hash, b.environment_hash);
        assert!(!a.environment_hash.is_empty());
        assert_eq!(a.environment_hash.len(), 64);
    }

    #[test]
    fn free_bytes_do_not_affect_hash() {
        let mut a = environment_fingerprint(None, BuildMode::Diagnostic).unwrap();
        let h1 = a.environment_hash.clone();
        a.free_bytes = Some(1);
        a.free_inodes = Some(2);
        // recompute as function would
        let h2 = stable_environment_hash(&a);
        assert_eq!(h1, h2);
    }

    #[test]
    fn qualification_vs_diagnostic_hash_differs_on_build_mode_field() {
        // BuildMode is part of stable hash; different modes → different hash.
        let d = environment_fingerprint(None, BuildMode::Diagnostic).unwrap();
        let q = environment_fingerprint(None, BuildMode::Qualification).unwrap();
        assert_ne!(d.environment_hash, q.environment_hash);
    }
}
