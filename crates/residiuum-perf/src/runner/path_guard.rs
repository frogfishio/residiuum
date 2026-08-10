//! Destructive-target rejection for dedicated work roots (SPEC §4).

use super::platform::{is_block_device_path, mount_points};
use super::RunnerError;
use std::path::{Component, Path, PathBuf};

/// Why a candidate work path was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathRejectReason {
    Root,
    Home,
    RepositoryRoot,
    MountRoot,
    BlockDevice,
    SymlinkEscape,
    NotAbsolute,
    Empty,
    RelativeDot,
}

impl std::fmt::Display for PathRejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Root => write!(f, "filesystem root"),
            Self::Home => write!(f, "home directory"),
            Self::RepositoryRoot => write!(f, "repository root"),
            Self::MountRoot => write!(f, "mount root"),
            Self::BlockDevice => write!(f, "block/device path"),
            Self::SymlinkEscape => write!(f, "symlink resolves outside requested parent policy"),
            Self::NotAbsolute => write!(f, "path is not absolute"),
            Self::Empty => write!(f, "empty path"),
            Self::RelativeDot => write!(f, "path contains relative components after normalize"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkPathClass {
    Allowed {
        resolved: PathBuf,
    },
    Rejected {
        reason: PathRejectReason,
        path: PathBuf,
    },
}

/// Classify a candidate dedicated work root.
///
/// `repository_root` is the Residiuum workspace root (never a legal work dir).
/// When `repository_root` is `None`, repository-root rejection is skipped
/// (tests that only care about other classes).
pub fn classify_work_path(
    candidate: &Path,
    repository_root: Option<&Path>,
) -> Result<WorkPathClass, RunnerError> {
    if candidate.as_os_str().is_empty() {
        return Ok(WorkPathClass::Rejected {
            reason: PathRejectReason::Empty,
            path: candidate.to_path_buf(),
        });
    }
    if !candidate.is_absolute() {
        return Ok(WorkPathClass::Rejected {
            reason: PathRejectReason::NotAbsolute,
            path: candidate.to_path_buf(),
        });
    }

    // Reject device nodes early (path form), even if they do not exist.
    if is_block_device_path(candidate) {
        return Ok(WorkPathClass::Rejected {
            reason: PathRejectReason::BlockDevice,
            path: candidate.to_path_buf(),
        });
    }

    // Logical root before resolve.
    if is_filesystem_root(candidate) {
        return Ok(WorkPathClass::Rejected {
            reason: PathRejectReason::Root,
            path: PathBuf::from("/"),
        });
    }

    let resolved = resolve_for_guard(candidate)?;

    if is_filesystem_root(&resolved) {
        return Ok(WorkPathClass::Rejected {
            reason: PathRejectReason::Root,
            path: resolved,
        });
    }

    if let Some(home) = home_dir() {
        if paths_equal(&resolved, &home) {
            return Ok(WorkPathClass::Rejected {
                reason: PathRejectReason::Home,
                path: resolved,
            });
        }
    }

    if let Some(repo) = repository_root {
        let repo_resolved = resolve_for_guard(repo).unwrap_or_else(|_| repo.to_path_buf());
        if paths_equal(&resolved, &repo_resolved) {
            return Ok(WorkPathClass::Rejected {
                reason: PathRejectReason::RepositoryRoot,
                path: resolved,
            });
        }
    }

    if is_mount_root(&resolved) {
        return Ok(WorkPathClass::Rejected {
            reason: PathRejectReason::MountRoot,
            path: resolved,
        });
    }

    // Detect path swap via parent symlink: if any prefix of the resolved path
    // differs from the lexical normalize in a way that lands on a forbidden
    // target, reject. resolve_for_guard already canonicalizes existing prefixes.
    if let Some(parent) = candidate.parent() {
        if parent.exists() {
            let parent_canon = fs_canonicalize(parent)?;
            let resolved_parent = resolved.parent().map(|p| p.to_path_buf());
            if let Some(rp) = resolved_parent {
                // If candidate's parent canonical path is not a prefix of resolved, escape.
                if !rp.starts_with(&parent_canon) && parent_canon != rp {
                    // Only flag when symlink redirected outside the stated parent tree.
                    if candidate
                        .components()
                        .any(|c| matches!(c, Component::ParentDir))
                    {
                        return Ok(WorkPathClass::Rejected {
                            reason: PathRejectReason::SymlinkEscape,
                            path: resolved,
                        });
                    }
                }
            }
        }
    }

    Ok(WorkPathClass::Allowed { resolved })
}

fn is_filesystem_root(path: &Path) -> bool {
    let comps: Vec<_> = path.components().collect();
    matches!(comps.as_slice(), [Component::RootDir])
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from).and_then(|p| {
        if p.as_os_str().is_empty() {
            None
        } else {
            Some(fs_canonicalize(&p).unwrap_or(p))
        }
    })
}

fn is_mount_root(path: &Path) -> bool {
    let mounts = mount_points().unwrap_or_default();
    mounts.iter().any(|m| paths_equal(path, m))
}

fn paths_equal(a: &Path, b: &Path) -> bool {
    // Prefer canonical equality; fall back to component equality.
    match (fs_canonicalize(a), fs_canonicalize(b)) {
        (Ok(ca), Ok(cb)) => ca == cb,
        _ => a == b,
    }
}

/// Resolve existing path; for non-existent leaf, resolve parent and join name.
fn resolve_for_guard(path: &Path) -> Result<PathBuf, RunnerError> {
    if path.exists() {
        return fs_canonicalize(path);
    }
    // Walk up until an existing ancestor, then re-append missing components.
    let mut cur = path.to_path_buf();
    let mut missing = Vec::new();
    while !cur.exists() {
        match cur.file_name() {
            Some(name) => {
                missing.push(name.to_os_string());
                cur = cur
                    .parent()
                    .ok_or_else(|| {
                        RunnerError::InvalidPath(format!(
                            "cannot resolve non-existent path {}",
                            path.display()
                        ))
                    })?
                    .to_path_buf();
            }
            None => {
                return Err(RunnerError::InvalidPath(format!(
                    "cannot resolve {}",
                    path.display()
                )));
            }
        }
    }
    let mut resolved = fs_canonicalize(&cur)?;
    for name in missing.into_iter().rev() {
        resolved.push(name);
    }
    Ok(resolved)
}

fn fs_canonicalize(path: &Path) -> Result<PathBuf, RunnerError> {
    std::fs::canonicalize(path)
        .map_err(|e| RunnerError::InvalidPath(format!("canonicalize {}: {e}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn rejects_filesystem_root() {
        let c = classify_work_path(Path::new("/"), None).unwrap();
        assert!(matches!(
            c,
            WorkPathClass::Rejected {
                reason: PathRejectReason::Root,
                ..
            }
        ));
    }

    #[test]
    fn rejects_relative() {
        let c = classify_work_path(Path::new("relative/work"), None).unwrap();
        assert!(matches!(
            c,
            WorkPathClass::Rejected {
                reason: PathRejectReason::NotAbsolute,
                ..
            }
        ));
    }

    #[test]
    fn rejects_home() {
        let home = std::env::var("HOME").expect("HOME");
        let c = classify_work_path(Path::new(&home), None).unwrap();
        assert!(matches!(
            c,
            WorkPathClass::Rejected {
                reason: PathRejectReason::Home,
                ..
            }
        ));
    }

    #[test]
    fn rejects_repo_root() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        fs::create_dir_all(&repo).unwrap();
        let c = classify_work_path(&repo, Some(&repo)).unwrap();
        assert!(matches!(
            c,
            WorkPathClass::Rejected {
                reason: PathRejectReason::RepositoryRoot,
                ..
            }
        ));
    }

    #[test]
    fn rejects_dev_null_as_blockish() {
        let c = classify_work_path(Path::new("/dev/null"), None).unwrap();
        assert!(matches!(
            c,
            WorkPathClass::Rejected {
                reason: PathRejectReason::BlockDevice,
                ..
            }
        ));
    }

    #[test]
    fn allows_nested_temp_path() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("pqh-work");
        fs::create_dir_all(&work).unwrap();
        let c = classify_work_path(&work, None).unwrap();
        assert!(matches!(c, WorkPathClass::Allowed { .. }));
    }
}
