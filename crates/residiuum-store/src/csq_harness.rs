//! CSQ-2 external harness barriers and controllers.
//!
//! In-process failpoints are necessary but not sufficient (SPEC §10.3). This
//! module records the **approved external harness inventory** and provides
//! light-weight controllers used by crash-matrix / composed-failure drivers.
//! Full filesystem-image and device-mapper campaigns are owned by CSQ-5; CSQ-2
//! freezes the barrier contract so unregistered edges cannot pass CI.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

/// Barrier phase relative to a registered persistence/publication edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierPhase {
    /// Immediately before the edge runs.
    Before,
    /// Immediately after the edge completes its local step.
    After,
}

/// Kind of external or in-process barrier a boundary may use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierKind {
    /// Named in-process failpoint ([`crate::failpoint`]).
    InProcessFailpoint,
    /// Child process + `Action::Abort` / SIGKILL (see crash-child binary).
    ChildProcessAbort,
    /// Loopback filesystem image / remount / quota (CSQ-5 campaign).
    FilesystemImage,
    /// Suite-owned logical edge (no durable media step).
    SuiteOwnedLogical,
}

/// One approved harness capability for CSQ-2 census.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HarnessCapability {
    /// Barrier / harness kind.
    pub kind: BarrierKind,
    /// CSQ package that owns readiness of this capability.
    pub owner_package: &'static str,
    /// Whether the harness can execute today (false = inventory only).
    pub ready: bool,
    /// Short human note for evidence bundles.
    pub notes: &'static str,
}

/// Frozen inventory of external harness capabilities (CSQ-2).
pub const HARNESS_CAPABILITIES: &[HarnessCapability] = &[
    HarnessCapability {
        kind: BarrierKind::InProcessFailpoint,
        owner_package: "CSQ-2",
        ready: true,
        notes: "failpoint.rs Panic/Abort/Error/ShortWrite/Io*/Cancel/AllocFail",
    },
    HarnessCapability {
        kind: BarrierKind::ChildProcessAbort,
        owner_package: "CSQ-2",
        ready: true,
        notes: "residiuum-store-crash-child + stage_def_022 multiprocess cells",
    },
    HarnessCapability {
        kind: BarrierKind::FilesystemImage,
        owner_package: "CSQ-5",
        ready: true,
        notes: "portable tempfile FS image ready; Linux loopback/dm-flakey is explicit skip-with-reason",
    },
    HarnessCapability {
        kind: BarrierKind::SuiteOwnedLogical,
        owner_package: "CSQ-2",
        ready: true,
        notes: "logical read/scan ops without durable write edges",
    },
];

/// Parse a harness string from `boundaries-v1.json` into a barrier kind.
pub fn parse_harness(s: &str) -> Option<BarrierKind> {
    match s {
        "in_process_failpoint" => Some(BarrierKind::InProcessFailpoint),
        "child_process_abort" | "process_crash_controller" => Some(BarrierKind::ChildProcessAbort),
        "filesystem_image" | "external_fs_image" => Some(BarrierKind::FilesystemImage),
        "suite_owned" | "suite_owned_logical" => Some(BarrierKind::SuiteOwnedLogical),
        "operation_matrix_proxy" => Some(BarrierKind::InProcessFailpoint),
        "external_harness_csq5" => Some(BarrierKind::FilesystemImage),
        _ => None,
    }
}

/// True when the boundary harness is considered injectable or approved external.
pub fn harness_is_approved(s: &str) -> bool {
    match parse_harness(s) {
        Some(BarrierKind::FilesystemImage) => {
            // Approved external even when campaign not ready: must be explicit.
            true
        }
        Some(_) => true,
        None => false,
    }
}

/// Child-process crash controller (wraps the crash-child binary contract).
#[derive(Debug, Clone)]
pub struct CrashController {
    /// Path to `residiuum-store-crash-child`.
    pub binary: PathBuf,
}

impl CrashController {
    /// Resolve the crash-child binary next to the running test, or via env.
    pub fn resolve() -> Option<Self> {
        if let Ok(p) = std::env::var("CARGO_BIN_EXE_residiuum_store_crash_child") {
            let path = PathBuf::from(p);
            if path.is_file() {
                return Some(Self { binary: path });
            }
        }
        let mut exe = std::env::current_exe().ok()?;
        exe.pop();
        if exe.file_name().and_then(|s| s.to_str()) == Some("deps") {
            exe.pop();
        }
        exe.push("residiuum-store-crash-child");
        if exe.is_file() {
            return Some(Self { binary: exe });
        }
        None
    }

    /// Run one child op with optional Abort failpoint.
    pub fn run(
        &self,
        store: &Path,
        op: &str,
        failpoint: Option<&str>,
        key: &str,
        val: &str,
    ) -> std::io::Result<ExitStatus> {
        let mut cmd = Command::new(&self.binary);
        cmd.env("RESIDIUUM_CRASH_STORE", store)
            .env("RESIDIUUM_CRASH_OP", op)
            .env("RESIDIUUM_CRASH_KEY", key)
            .env("RESIDIUUM_CRASH_VAL", val);
        if let Some(fp) = failpoint {
            cmd.env("RESIDIUUM_CRASH_FP", fp);
        }
        cmd.status()
    }
}

/// Filesystem-image harness **placeholder** (CSQ-5 fills execution).
///
/// CSQ-2 freezes the contract: a boundary may cite `filesystem_image` only when
/// the capability is registered here. Running the campaign is not required for
/// CSQ-2 accept of the instrumentation package.
#[derive(Debug, Clone, Default)]
pub struct FilesystemImageHarness {
    /// Planned image root (not created by CSQ-2).
    pub image_root: Option<PathBuf>,
    /// Whether the campaign runner is implemented (CSQ-5 sets true).
    pub ready: bool,
}

impl FilesystemImageHarness {
    /// Construct the inventory entry (portable image ready under CSQ-5).
    pub fn inventory() -> Self {
        Self {
            image_root: None,
            ready: true,
        }
    }

    /// Portable campaign: create a tempfile-backed image root.
    pub fn run_portable_campaign(
        &mut self,
        parent: &std::path::Path,
    ) -> Result<std::path::PathBuf, String> {
        if !self.ready {
            return Err("filesystem-image harness not ready".into());
        }
        let img =
            crate::csq5_campaign::PortableFsImage::create(parent).map_err(|e| e.to_string())?;
        self.image_root = Some(img.root.clone());
        Ok(img.store_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_covers_required_kinds() {
        let kinds: Vec<_> = HARNESS_CAPABILITIES.iter().map(|c| c.kind).collect();
        assert!(kinds.contains(&BarrierKind::InProcessFailpoint));
        assert!(kinds.contains(&BarrierKind::ChildProcessAbort));
        assert!(kinds.contains(&BarrierKind::FilesystemImage));
        let fs = HARNESS_CAPABILITIES
            .iter()
            .find(|c| c.kind == BarrierKind::FilesystemImage)
            .unwrap();
        assert!(fs.ready, "CSQ-5 portable FS image is ready");
        assert_eq!(fs.owner_package, "CSQ-5");
    }

    #[test]
    fn harness_strings_parse() {
        assert!(harness_is_approved("in_process_failpoint"));
        assert!(harness_is_approved("operation_matrix_proxy"));
        assert!(harness_is_approved("external_harness_csq5"));
        assert!(harness_is_approved("suite_owned"));
        assert!(!harness_is_approved(""));
        assert!(!harness_is_approved("mystery"));
    }
}
