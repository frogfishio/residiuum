//! Media locators and object-store seam (Stage 9 + product follow-on).
//!
//! Stage 9 places sealed segments on hot/warm/cold/archive **media roots**.
//! Roots may be ordinary filesystem directories (baseline) or object-store
//! style URIs. This module defines the addressing seam, ships a **local object**
//! stand-in, and resolves live **S3/GCS** via operator mirrors (env or
//! explicit [`CloudMirrorConfig`]) so catalogs and placement stay backend-agnostic.
//!
//! ## URI forms (roots.txt third column)
//!
//! | Spec | Meaning |
//! |------|---------|
//! | `/abs/path` or `relative/path` | Filesystem directory |
//! | `file:///abs/path` | Filesystem (URI form) |
//! | `object:local:/abs/or/rel` | Local object layout under a directory |
//! | `s3://bucket/prefix` | Amazon S3 — I/O via `RESIDIUUM_S3_ROOT` mirror |
//! | `gs://bucket/prefix` | Google Cloud Storage — I/O via `RESIDIUUM_GS_ROOT` mirror |
//!
//! ## Live cloud connectors
//!
//! Set `RESIDIUUM_S3_ROOT` / `RESIDIUUM_GS_ROOT` to a directory that holds
//! `{bucket}/{prefix}/…` object keys (rclone mount, s3fs, offline mirror, or
//! MinIO disk layout). Keys are stored as ordinary files under that tree.
//! Without a mirror, cloud locators parse and appear **offline** for coverage
//! honesty (`MediaUnsupported` on put/get).

use crate::error::StoreError;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Object-store scheme for cloud or local stand-in media.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectScheme {
    /// Local filesystem laid out as opaque object keys (in-tree stand-in).
    Local,
    /// Amazon S3 (`s3://`).
    S3,
    /// Google Cloud Storage (`gs://`).
    Gcs,
}

impl ObjectScheme {
    /// Stable ASCII name for catalogs and errors.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::S3 => "s3",
            Self::Gcs => "gs",
        }
    }

    /// Whether this build can read/write objects without an external connector.
    pub fn is_builtin(self) -> bool {
        matches!(self, Self::Local)
    }
}

/// Parsed object-store URI (`s3://bucket/prefix`, `gs://…`, `object:local:…`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMediaUri {
    /// Scheme.
    pub scheme: ObjectScheme,
    /// Bucket / container name (for `object:local`, a logical namespace; may be `_`).
    pub bucket: String,
    /// Key prefix inside the bucket (no leading slash; may be empty).
    pub key_prefix: String,
    /// Local directory for [`ObjectScheme::Local`] (and optional offline cache later).
    pub local_root: Option<PathBuf>,
}

impl ObjectMediaUri {
    /// Canonical string form for roots.txt / logs.
    pub fn to_uri_string(&self) -> String {
        match self.scheme {
            ObjectScheme::Local => {
                let root = self
                    .local_root
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default();
                if self.key_prefix.is_empty() {
                    format!("object:local:{root}")
                } else {
                    format!("object:local:{root}#{}", self.key_prefix)
                }
            }
            ObjectScheme::S3 => {
                if self.key_prefix.is_empty() {
                    format!("s3://{}", self.bucket)
                } else {
                    format!("s3://{}/{}", self.bucket, self.key_prefix)
                }
            }
            ObjectScheme::Gcs => {
                if self.key_prefix.is_empty() {
                    format!("gs://{}", self.bucket)
                } else {
                    format!("gs://{}/{}", self.bucket, self.key_prefix)
                }
            }
        }
    }

    /// Object key for a sealed segment id (hex + `.residiuum`).
    pub fn segment_key(&self, segment_hex: &str) -> String {
        let file = format!("{segment_hex}.residiuum");
        if self.key_prefix.is_empty() {
            file
        } else {
            format!("{}/{}", self.key_prefix.trim_end_matches('/'), file)
        }
    }
}

/// Where tier media lives: filesystem directory or object-store URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaLocator {
    /// Ordinary directory of segment files (Stage 9 baseline).
    Filesystem(PathBuf),
    /// Object-store style addressing.
    Object(ObjectMediaUri),
}

impl MediaLocator {
    /// Parse a roots.txt media root string.
    ///
    /// Empty input is rejected. Unknown schemes that look like `scheme://…`
    /// become a parse error; bare paths remain filesystem.
    pub fn parse(spec: &str) -> Result<Self, StoreError> {
        let spec = spec.trim();
        if spec.is_empty() {
            return Err(StoreError::CorruptMeta("empty media root"));
        }

        if let Some(rest) = spec.strip_prefix("file://") {
            // file:///abs or file://localhost/abs — keep simple: strip host-less form.
            let path = rest.strip_prefix("localhost").unwrap_or(rest);
            return Ok(Self::Filesystem(PathBuf::from(path)));
        }

        if let Some(rest) = spec.strip_prefix("object:local:") {
            let (root, prefix) = match rest.split_once('#') {
                Some((r, p)) => (r, p.trim_matches('/').to_string()),
                None => (rest, String::new()),
            };
            if root.is_empty() {
                return Err(StoreError::CorruptMeta("object:local requires a path"));
            }
            return Ok(Self::Object(ObjectMediaUri {
                scheme: ObjectScheme::Local,
                bucket: "_".into(),
                key_prefix: prefix,
                local_root: Some(PathBuf::from(root)),
            }));
        }

        if let Some(rest) = spec.strip_prefix("s3://") {
            return parse_cloud_uri(ObjectScheme::S3, rest);
        }
        if let Some(rest) = spec.strip_prefix("gs://") {
            return parse_cloud_uri(ObjectScheme::Gcs, rest);
        }

        // Reject other scheme:// forms so typos fail loudly.
        if let Some(idx) = spec.find("://") {
            let scheme = &spec[..idx];
            if scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
                && !scheme.is_empty()
            {
                return Err(StoreError::MediaUnsupported(format!(
                    "unknown media scheme {scheme:?} (supported: filesystem path, file://, object:local:, s3://, gs://)"
                )));
            }
        }

        Ok(Self::Filesystem(PathBuf::from(spec)))
    }

    /// Canonical string for persistence.
    pub fn to_spec_string(&self) -> String {
        match self {
            Self::Filesystem(p) => p.display().to_string(),
            Self::Object(u) => u.to_uri_string(),
        }
    }

    /// Filesystem directory used for built-in I/O, if any.
    ///
    /// Cloud-only locators return `None` (need an HTTP connector).
    pub fn local_directory(&self) -> Option<PathBuf> {
        match self {
            Self::Filesystem(p) => Some(p.clone()),
            Self::Object(u) if u.scheme.is_builtin() => u.local_root.clone(),
            Self::Object(_) => None,
        }
    }

    /// Whether this build can perform put/get without an external connector.
    pub fn is_builtin(&self) -> bool {
        match self {
            Self::Filesystem(_) => true,
            Self::Object(u) => u.scheme.is_builtin(),
        }
    }
}

fn parse_cloud_uri(scheme: ObjectScheme, rest: &str) -> Result<MediaLocator, StoreError> {
    let rest = rest.trim_start_matches('/');
    if rest.is_empty() {
        return Err(StoreError::CorruptMeta("object URI missing bucket"));
    }
    let (bucket, prefix) = match rest.split_once('/') {
        Some((b, p)) => (b.to_string(), p.trim_matches('/').to_string()),
        None => (rest.to_string(), String::new()),
    };
    if bucket.is_empty() {
        return Err(StoreError::CorruptMeta("object URI missing bucket"));
    }
    Ok(MediaLocator::Object(ObjectMediaUri {
        scheme,
        bucket,
        key_prefix: prefix,
        local_root: None,
    }))
}

/// Object put/get used by tier media (and future cloud connectors).
pub trait MediaBackend: Send + Sync {
    /// Store opaque bytes at `key` (segment object key).
    fn put_object(&self, key: &str, bytes: &[u8]) -> Result<(), StoreError>;

    /// Read opaque bytes at `key`.
    fn get_object(&self, key: &str) -> Result<Vec<u8>, StoreError>;

    /// Delete object if present (ok if missing).
    fn delete_object(&self, key: &str) -> Result<(), StoreError>;

    /// Whether the object exists.
    fn object_exists(&self, key: &str) -> Result<bool, StoreError>;

    /// Whether the backend believes media is mounted / reachable.
    fn is_available(&self) -> bool {
        true
    }
}

/// Filesystem directory of segment files (Stage 9 baseline).
#[derive(Debug, Clone)]
pub struct FilesystemMedia {
    root: PathBuf,
}

impl FilesystemMedia {
    /// Media rooted at `root` (created on first put if needed).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Absolute root path.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path_for_key(&self, key: &str) -> PathBuf {
        // Keys may include `/` for prefixes; keep them under root.
        let key = key.trim_start_matches('/');
        self.root.join(key)
    }
}

impl MediaBackend for FilesystemMedia {
    fn put_object(&self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        let dest = self.path_for_key(key);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = dest.with_extension("residiuum.tmp");
        {
            let mut out = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            out.write_all(bytes)?;
            out.sync_all()?;
        }
        fs::rename(&tmp, &dest)?;
        Ok(())
    }

    fn get_object(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        let path = self.path_for_key(key);
        let mut f = File::open(&path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        Ok(buf)
    }

    fn delete_object(&self, key: &str) -> Result<(), StoreError> {
        let path = self.path_for_key(key);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    fn object_exists(&self, key: &str) -> Result<bool, StoreError> {
        Ok(self.path_for_key(key).is_file())
    }
}

/// Local object-store stand-in: keys under a directory (same I/O as filesystem,
/// different addressing contract for catalogs and future cloud adapters).
#[derive(Debug, Clone)]
pub struct LocalObjectMedia {
    inner: FilesystemMedia,
    uri: ObjectMediaUri,
}

impl LocalObjectMedia {
    /// Open a local object root from a parsed URI (scheme must be Local).
    pub fn from_uri(uri: ObjectMediaUri) -> Result<Self, StoreError> {
        if uri.scheme != ObjectScheme::Local {
            return Err(StoreError::MediaUnsupported(format!(
                "LocalObjectMedia requires object:local, got {}",
                uri.scheme.as_str()
            )));
        }
        let root = uri
            .local_root
            .clone()
            .ok_or(StoreError::CorruptMeta("object:local missing path"))?;
        Ok(Self {
            inner: FilesystemMedia::new(root),
            uri,
        })
    }

    /// URI this backend was opened from.
    pub fn uri(&self) -> &ObjectMediaUri {
        &self.uri
    }
}

impl MediaBackend for LocalObjectMedia {
    fn put_object(&self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.inner.put_object(key, bytes)
    }

    fn get_object(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        self.inner.get_object(key)
    }

    fn delete_object(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete_object(key)
    }

    fn object_exists(&self, key: &str) -> Result<bool, StoreError> {
        self.inner.object_exists(key)
    }
}

/// Operator mirror for live S3/GCS object keys on a local filesystem tree.
///
/// Layout: `{root}/{bucket}/{key_prefix}/…` where object keys are relative
/// paths under that directory. Point `RESIDIUUM_S3_ROOT` / `RESIDIUUM_GS_ROOT` at an
/// rclone/s3fs mount or offline copy to enable put/get without an HTTP SDK.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloudMirrorConfig {
    /// Base directory for `s3://` (`RESIDIUUM_S3_ROOT`).
    pub s3_root: Option<PathBuf>,
    /// Base directory for `gs://` (`RESIDIUUM_GS_ROOT`).
    pub gs_root: Option<PathBuf>,
}

impl CloudMirrorConfig {
    /// Empty config (cloud I/O unsupported).
    pub fn new() -> Self {
        Self::default()
    }

    /// Load from process environment.
    ///
    /// | Variable | Scheme |
    /// |----------|--------|
    /// | `RESIDIUUM_S3_ROOT` | `s3://` |
    /// | `RESIDIUUM_GS_ROOT` | `gs://` |
    pub fn from_env() -> Self {
        Self {
            s3_root: std::env::var_os("RESIDIUUM_S3_ROOT").map(PathBuf::from),
            gs_root: std::env::var_os("RESIDIUUM_GS_ROOT").map(PathBuf::from),
        }
    }

    /// Whether any mirror is configured.
    pub fn has_any(&self) -> bool {
        self.s3_root.is_some() || self.gs_root.is_some()
    }

    /// Resolve a cloud URI to a local directory, if a mirror is configured.
    pub fn resolve_directory(&self, uri: &ObjectMediaUri) -> Option<PathBuf> {
        let base = match uri.scheme {
            ObjectScheme::S3 => self.s3_root.as_ref()?,
            ObjectScheme::Gcs => self.gs_root.as_ref()?,
            ObjectScheme::Local => return uri.local_root.clone(),
        };
        let mut path = base.join(&uri.bucket);
        if !uri.key_prefix.is_empty() {
            path = path.join(uri.key_prefix.trim_matches('/'));
        }
        Some(path)
    }
}

/// S3/GCS backend backed by a filesystem mirror (product live-connector path).
#[derive(Debug, Clone)]
pub struct MirroredCloudMedia {
    inner: FilesystemMedia,
    uri: ObjectMediaUri,
}

impl MirroredCloudMedia {
    /// Open from URI + mirror config (scheme must be S3 or Gcs).
    pub fn from_uri(uri: ObjectMediaUri, mirror: &CloudMirrorConfig) -> Result<Self, StoreError> {
        if !matches!(uri.scheme, ObjectScheme::S3 | ObjectScheme::Gcs) {
            return Err(StoreError::MediaUnsupported(format!(
                "MirroredCloudMedia requires s3:// or gs://, got {}",
                uri.scheme.as_str()
            )));
        }
        let root = mirror.resolve_directory(&uri).ok_or_else(|| {
            StoreError::MediaUnsupported(format!(
                "{} mirror not configured for {} (set RESIDIUUM_{}_ROOT)",
                uri.scheme.as_str(),
                uri.to_uri_string(),
                if uri.scheme == ObjectScheme::S3 {
                    "S3"
                } else {
                    "GS"
                }
            ))
        })?;
        Ok(Self {
            inner: FilesystemMedia::new(root),
            uri,
        })
    }

    /// URI this backend was opened from.
    pub fn uri(&self) -> &ObjectMediaUri {
        &self.uri
    }

    /// Local mirror root directory.
    pub fn root(&self) -> &Path {
        self.inner.root()
    }
}

impl MediaBackend for MirroredCloudMedia {
    fn put_object(&self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.inner.put_object(key, bytes)
    }

    fn get_object(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        self.inner.get_object(key)
    }

    fn delete_object(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete_object(key)
    }

    fn object_exists(&self, key: &str) -> Result<bool, StoreError> {
        self.inner.object_exists(key)
    }
}

/// Placeholder when no mirror is configured: locator parses; I/O refuses.
#[derive(Debug, Clone)]
pub struct UnsupportedCloudMedia {
    uri: ObjectMediaUri,
}

impl UnsupportedCloudMedia {
    /// Build from a cloud URI.
    pub fn new(uri: ObjectMediaUri) -> Self {
        Self { uri }
    }
}

impl MediaBackend for UnsupportedCloudMedia {
    fn put_object(&self, _key: &str, _bytes: &[u8]) -> Result<(), StoreError> {
        Err(StoreError::MediaUnsupported(format!(
            "{} connector unavailable ({}); set RESIDIUUM_{}_ROOT or use object:local: / filesystem roots",
            self.uri.scheme.as_str(),
            self.uri.to_uri_string(),
            if self.uri.scheme == ObjectScheme::S3 {
                "S3"
            } else {
                "GS"
            }
        )))
    }

    fn get_object(&self, _key: &str) -> Result<Vec<u8>, StoreError> {
        Err(StoreError::MediaUnsupported(format!(
            "{} connector unavailable ({})",
            self.uri.scheme.as_str(),
            self.uri.to_uri_string()
        )))
    }

    fn delete_object(&self, _key: &str) -> Result<(), StoreError> {
        Err(StoreError::MediaUnsupported(format!(
            "{} connector unavailable ({})",
            self.uri.scheme.as_str(),
            self.uri.to_uri_string()
        )))
    }

    fn object_exists(&self, _key: &str) -> Result<bool, StoreError> {
        Err(StoreError::MediaUnsupported(format!(
            "{} connector unavailable ({})",
            self.uri.scheme.as_str(),
            self.uri.to_uri_string()
        )))
    }

    fn is_available(&self) -> bool {
        false
    }
}

/// Open a media backend using [`CloudMirrorConfig::from_env`] for cloud URIs.
pub fn open_media(locator: &MediaLocator) -> Result<Box<dyn MediaBackend>, StoreError> {
    open_media_with(locator, &CloudMirrorConfig::from_env())
}

/// Open a media backend with an explicit cloud mirror config (tests / operators).
pub fn open_media_with(
    locator: &MediaLocator,
    mirror: &CloudMirrorConfig,
) -> Result<Box<dyn MediaBackend>, StoreError> {
    match locator {
        MediaLocator::Filesystem(p) => Ok(Box::new(FilesystemMedia::new(p.clone()))),
        MediaLocator::Object(u) if u.scheme == ObjectScheme::Local => {
            Ok(Box::new(LocalObjectMedia::from_uri(u.clone())?))
        }
        MediaLocator::Object(u) if matches!(u.scheme, ObjectScheme::S3 | ObjectScheme::Gcs) => {
            if mirror.resolve_directory(u).is_some() {
                Ok(Box::new(MirroredCloudMedia::from_uri(u.clone(), mirror)?))
            } else {
                Ok(Box::new(UnsupportedCloudMedia::new(u.clone())))
            }
        }
        MediaLocator::Object(u) => Ok(Box::new(UnsupportedCloudMedia::new(u.clone()))),
    }
}

/// Resolve a roots.txt media root string into a filesystem directory for the
/// existing placement path logic, when possible.
///
/// Uses [`CloudMirrorConfig::from_env`] for `s3://` / `gs://`.
pub fn media_root_directory(spec: &str) -> Result<PathBuf, StoreError> {
    media_root_directory_with(spec, &CloudMirrorConfig::from_env())
}

/// Resolve a media root string with an explicit mirror config.
pub fn media_root_directory_with(
    spec: &str,
    mirror: &CloudMirrorConfig,
) -> Result<PathBuf, StoreError> {
    let loc = MediaLocator::parse(spec)?;
    if let Some(dir) = loc.local_directory() {
        return Ok(dir);
    }
    match &loc {
        MediaLocator::Object(u) => mirror.resolve_directory(u).ok_or_else(|| {
            StoreError::MediaUnsupported(format!(
                "no local directory for media root {spec:?}; set RESIDIUUM_S3_ROOT / RESIDIUUM_GS_ROOT or use object:local:"
            ))
        }),
        MediaLocator::Filesystem(_) => unreachable!("filesystem always has local_directory"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_filesystem_and_file_uri() {
        let a = MediaLocator::parse("/var/residiuum/cold").unwrap();
        assert_eq!(
            a,
            MediaLocator::Filesystem(PathBuf::from("/var/residiuum/cold"))
        );
        let b = MediaLocator::parse("file:///var/residiuum/cold").unwrap();
        assert_eq!(
            b,
            MediaLocator::Filesystem(PathBuf::from("/var/residiuum/cold"))
        );
    }

    #[test]
    fn parse_object_local_and_roundtrip_uri() {
        let loc = MediaLocator::parse("object:local:/tmp/obj#archive").unwrap();
        match &loc {
            MediaLocator::Object(u) => {
                assert_eq!(u.scheme, ObjectScheme::Local);
                assert_eq!(u.local_root.as_deref(), Some(Path::new("/tmp/obj")));
                assert_eq!(u.key_prefix, "archive");
                assert_eq!(u.segment_key("abcd"), "archive/abcd.residiuum");
            }
            _ => panic!("expected object"),
        }
        assert_eq!(loc.to_spec_string(), "object:local:/tmp/obj#archive");
    }

    #[test]
    fn parse_s3_gs() {
        let s3 = MediaLocator::parse("s3://my-bucket/prefix/path").unwrap();
        match s3 {
            MediaLocator::Object(u) => {
                assert_eq!(u.scheme, ObjectScheme::S3);
                assert_eq!(u.bucket, "my-bucket");
                assert_eq!(u.key_prefix, "prefix/path");
                assert!(!u.scheme.is_builtin());
            }
            _ => panic!("expected object"),
        }
        let gs = MediaLocator::parse("gs://b").unwrap();
        match gs {
            MediaLocator::Object(u) => {
                assert_eq!(u.scheme, ObjectScheme::Gcs);
                assert_eq!(u.bucket, "b");
                assert!(u.key_prefix.is_empty());
            }
            _ => panic!("expected object"),
        }
    }

    #[test]
    fn unknown_scheme_errors() {
        let err = MediaLocator::parse("azure://x").unwrap_err();
        assert!(matches!(err, StoreError::MediaUnsupported(_)));
    }

    #[test]
    fn local_object_put_get_delete() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("objects");
        let loc = MediaLocator::parse(&format!("object:local:{}", root.display())).unwrap();
        let media = open_media(&loc).unwrap();
        media
            .put_object("seg/aa.residiuum", b"hello-object")
            .unwrap();
        assert!(media.object_exists("seg/aa.residiuum").unwrap());
        assert_eq!(
            media.get_object("seg/aa.residiuum").unwrap(),
            b"hello-object"
        );
        media.delete_object("seg/aa.residiuum").unwrap();
        assert!(!media.object_exists("seg/aa.residiuum").unwrap());
    }

    #[test]
    fn s3_backend_refuses_io_without_mirror() {
        let loc = MediaLocator::parse("s3://bucket/p").unwrap();
        let media = open_media_with(&loc, &CloudMirrorConfig::new()).unwrap();
        assert!(!media.is_available());
        let err = media.put_object("x.residiuum", b"nope").unwrap_err();
        assert!(matches!(err, StoreError::MediaUnsupported(_)));
    }

    #[test]
    fn s3_mirror_put_get_delete() {
        let dir = tempfile::tempdir().unwrap();
        let mirror = CloudMirrorConfig {
            s3_root: Some(dir.path().to_path_buf()),
            gs_root: None,
        };
        let loc = MediaLocator::parse("s3://my-bucket/archive").unwrap();
        let media = open_media_with(&loc, &mirror).unwrap();
        assert!(media.is_available());
        media.put_object("seg01.residiuum", b"cloud-bytes").unwrap();
        assert!(media.object_exists("seg01.residiuum").unwrap());
        assert_eq!(media.get_object("seg01.residiuum").unwrap(), b"cloud-bytes");
        // On-disk layout: {root}/{bucket}/{prefix}/{key}
        let on_disk = dir
            .path()
            .join("my-bucket")
            .join("archive")
            .join("seg01.residiuum");
        assert!(on_disk.is_file());
        media.delete_object("seg01.residiuum").unwrap();
        assert!(!media.object_exists("seg01.residiuum").unwrap());
    }

    #[test]
    fn gs_mirror_resolves_media_root_directory() {
        let dir = tempfile::tempdir().unwrap();
        let mirror = CloudMirrorConfig {
            s3_root: None,
            gs_root: Some(dir.path().to_path_buf()),
        };
        let got = media_root_directory_with("gs://cold-bucket/tier", &mirror).unwrap();
        assert_eq!(got, dir.path().join("cold-bucket").join("tier"));
    }

    #[test]
    fn media_root_directory_for_local_object() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("o");
        let got = media_root_directory(&format!("object:local:{}", p.display())).unwrap();
        assert_eq!(got, p);
        let err = media_root_directory_with("s3://b/p", &CloudMirrorConfig::new()).unwrap_err();
        assert!(matches!(err, StoreError::MediaUnsupported(_)));
    }
}
