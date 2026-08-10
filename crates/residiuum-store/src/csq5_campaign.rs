//! CSQ-5 crash / filesystem campaign helpers.
//!
//! Complements DEF-022 matrix drivers with:
//! - old/new/unknown outcome classification (CSQ-ACK-003);
//! - campaign evidence capture (platform, mount, skip ledger);
//! - portable filesystem-image harness (loopback/dm remains platform lane);
//! - composed-failure execution status records.
//!
//! Campaign tests live in `tests/csq5_crash_campaign.rs`.

use crate::composed_failure::{
    failure_class_action, schedule, FailureCombinationDoc, ScheduleDecision,
};
use crate::crash_matrix::{all_cells, ci_subset_cells, CrashMatrix, ExpectedReopen};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Registered crash outcome for an unacknowledged or interrupted op.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrashOutcomeClass {
    /// Only pre-op durable state survives.
    Old,
    /// Full post-op durable state applied.
    New,
    /// Bounded uncertainty — never hybrid.
    Unknown,
}

/// Classify reopen observation for a subject key after a crash cell.
///
/// - `acked == true` ⇒ must be New (receipt implies durable visibility on qualified model)
/// - `acked == false` + key absent + prior retained ⇒ Old
/// - `acked == false` + key present without receipt ⇒ New (media durable, no receipt)
/// - otherwise Unknown (must not claim hybrid semantics)
pub fn classify_outcome(
    acked: bool,
    key_visible_after_reopen: bool,
    prior_retained: bool,
) -> CrashOutcomeClass {
    if acked {
        return CrashOutcomeClass::New;
    }
    match (key_visible_after_reopen, prior_retained) {
        (false, true) => CrashOutcomeClass::Old,
        (true, true) => CrashOutcomeClass::New,
        (false, false) => CrashOutcomeClass::Unknown,
        (true, false) => CrashOutcomeClass::Unknown,
    }
}

/// Validate expected matrix reopen fields against observed state.
///
/// Returns `Err` if observation forms an impossible hybrid or contradicts matrix.
pub fn validate_reopen_expectation(
    expected: &ExpectedReopen,
    acked: bool,
    key_visible: bool,
    prior_retained: bool,
) -> Result<CrashOutcomeClass, String> {
    if !expected.no_fabricated_commit && key_visible && !acked {
        // Fabrication of commit without media is forbidden; visibility without
        // ack is allowed only as durable-on-disk New.
    }
    if acked && !key_visible {
        return Err(
            "hybrid: durable receipt claimed but key not visible after reopen (CSQ-ACK-001/002)"
                .into(),
        );
    }
    if expected.prior_durable_retained && !prior_retained {
        return Err("prior durable key lost after reopen".into());
    }
    let class = classify_outcome(acked, key_visible, prior_retained);
    match expected.acknowledged_visible {
        Some(true) if !key_visible => {
            return Err(format!(
                "matrix expected visible key, got absent (outcome={class:?})"
            ));
        }
        Some(false) if !acked && key_visible => {
            // Allowed as New (post-sync kill); not a hybrid if prior retained.
            if !prior_retained {
                return Err("unacked visible without prior retention is hybrid".into());
            }
        }
        Some(false) if !acked && !key_visible => {
            // Old — good
        }
        _ => {}
    }
    // Hybrid detector: acked false AND invent value while prior lost is bad.
    if !acked && key_visible && !prior_retained {
        return Err("impossible hybrid: new key without prior and without ack".into());
    }
    Ok(class)
}

/// Explicit skip/run record — never silent (CSQ-5 exit).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneStatus {
    /// Lane name.
    pub lane: String,
    /// `ran` | `skipped` | `rejected`
    pub status: String,
    /// Required when skipped/rejected.
    pub reason: Option<String>,
    /// Platform tag.
    pub platform: String,
}

/// Filesystem / mount evidence for a campaign run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesystemEvidence {
    /// Store root path used.
    pub store_root: String,
    /// OS family.
    pub os: String,
    /// Arch.
    pub arch: String,
    /// Mount options / notes (best-effort).
    pub mount_notes: String,
    /// Whether a privileged loopback/dm lane was available.
    pub loopback_available: bool,
}

impl FilesystemEvidence {
    /// Capture current host evidence for `store_root`.
    pub fn capture(store_root: &Path) -> Self {
        let loopback = cfg!(target_os = "linux")
            && Path::new("/dev/loop-control").exists()
            && which_exists("losetup");
        Self {
            store_root: store_root.display().to_string(),
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            mount_notes: if cfg!(target_os = "macos") {
                "portable tempfile image (no dm-flakey on this host)".into()
            } else if cfg!(target_os = "linux") {
                if loopback {
                    "linux: loop-control present; dm-flakey campaign optional".into()
                } else {
                    "linux: no loop-control; portable tempfile image only".into()
                }
            } else {
                "portable tempfile image".into()
            },
            loopback_available: loopback,
        }
    }
}

fn which_exists(bin: &str) -> bool {
    std::env::var_os("PATH")
        .map(|p| {
            std::env::split_paths(&p).any(|dir| {
                let cand = dir.join(bin);
                cand.is_file()
            })
        })
        .unwrap_or(false)
}

/// Aggregate campaign report.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CampaignReport {
    /// Matrix cells executed.
    pub matrix_cells_ran: u32,
    /// Matrix cells in document.
    pub matrix_cells_total: u32,
    /// CI subset cells ran.
    pub ci_subset_ran: u32,
    /// Composed-failure runs.
    pub composed_ran: u32,
    /// Composed-failure rejected (with reason).
    pub composed_rejected: u32,
    /// Outcome class histogram.
    pub outcomes: BTreeMap<String, u32>,
    /// Lane statuses (no silent skips).
    pub lanes: Vec<LaneStatus>,
    /// Optional filesystem evidence.
    pub filesystem: Option<FilesystemEvidence>,
}

impl CampaignReport {
    /// Record an outcome class.
    pub fn record_outcome(&mut self, class: CrashOutcomeClass) {
        let k = format!("{class:?}").to_ascii_lowercase();
        *self.outcomes.entry(k).or_insert(0) += 1;
    }

    /// Add a lane that ran.
    pub fn lane_ran(&mut self, name: &str) {
        self.lanes.push(LaneStatus {
            lane: name.into(),
            status: "ran".into(),
            reason: None,
            platform: std::env::consts::OS.into(),
        });
    }

    /// Add an explicit skip (never silent).
    pub fn lane_skipped(&mut self, name: &str, reason: impl Into<String>) {
        self.lanes.push(LaneStatus {
            lane: name.into(),
            status: "skipped".into(),
            reason: Some(reason.into()),
            platform: std::env::consts::OS.into(),
        });
    }

    /// Serialize pretty JSON for evidence bundles.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Assert no lane is missing a reason when skipped.
    pub fn assert_no_silent_skips(&self) {
        for l in &self.lanes {
            if l.status == "skipped" {
                assert!(
                    l.reason
                        .as_ref()
                        .map(|s| !s.trim().is_empty())
                        .unwrap_or(false),
                    "silent skip forbidden for lane {}",
                    l.lane
                );
            }
        }
    }
}

/// Count matrix cells.
pub fn matrix_totals(m: &CrashMatrix) -> (u32, u32) {
    let total = all_cells(m).len() as u32;
    let ci = ci_subset_cells(m).len() as u32;
    (total, ci)
}

/// Load failure combinations from workspace path.
pub fn load_failure_combinations(path: &Path) -> Result<FailureCombinationDoc, String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    serde_json::from_str(&text).map_err(|e| e.to_string())
}

/// Build composed-failure schedule decisions for the campaign.
pub fn composed_schedule(doc: &FailureCombinationDoc) -> Result<Vec<ScheduleDecision>, String> {
    schedule(doc, false)
}

/// Map a failure class string to a failpoint action when injectable in-process.
pub fn action_for_failure_class(class: &str) -> Option<crate::failpoint::Action> {
    failure_class_action(class)
}

/// Portable filesystem-image root under a host temp parent.
///
/// Creates `image_root/store` for Residiuum data. Privileged loopback remains
/// a separate Linux lane recorded in evidence.
#[derive(Debug, Clone)]
pub struct PortableFsImage {
    /// Root directory of the image.
    pub root: PathBuf,
    /// Store directory inside the image.
    pub store_path: PathBuf,
}

impl PortableFsImage {
    /// Create a new image under `parent`.
    pub fn create(parent: &Path) -> std::io::Result<Self> {
        let root = parent.join("csq5-fs-image");
        std::fs::create_dir_all(&root)?;
        let store_path = root.join("store");
        std::fs::create_dir_all(&store_path)?;
        // Marker file for campaign evidence.
        std::fs::write(
            root.join("CSQ5_IMAGE.txt"),
            "residiuum-csq5-portable-fs-image-v1\n",
        )?;
        Ok(Self { root, store_path })
    }

    /// Capture evidence for this image.
    pub fn evidence(&self) -> FilesystemEvidence {
        FilesystemEvidence::capture(&self.store_path)
    }

    /// Path-alias simulation: rename image root and return new store path.
    pub fn rename_alias(&self, new_name: &str) -> std::io::Result<PathBuf> {
        let parent = self.root.parent().unwrap_or_else(|| Path::new("."));
        let new_root = parent.join(new_name);
        std::fs::rename(&self.root, &new_root)?;
        Ok(new_root.join("store"))
    }
}

/// Linux loopback/dm lane probe (does not require root; records availability).
pub fn probe_linux_loopback_lane() -> LaneStatus {
    let os = std::env::consts::OS;
    if os != "linux" {
        return LaneStatus {
            lane: "linux_loopback_dm".into(),
            status: "skipped".into(),
            reason: Some(format!("not linux (os={os})")),
            platform: os.into(),
        };
    }
    let available = Path::new("/dev/loop-control").exists() && which_exists("losetup");
    if available {
        // Privileged dm-flakey still needs root — mark ran probe only.
        LaneStatus {
            lane: "linux_loopback_dm".into(),
            status: "skipped".into(),
            reason: Some(
                "loop-control present but dm-flakey requires privileged setup; portable image used"
                    .into(),
            ),
            platform: os.into(),
        }
    } else {
        LaneStatus {
            lane: "linux_loopback_dm".into(),
            status: "skipped".into(),
            reason: Some("loop-control/losetup unavailable".into()),
            platform: os.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crash_matrix::ExpectedReopen;

    #[test]
    fn classify_old_new_unknown() {
        assert_eq!(classify_outcome(false, false, true), CrashOutcomeClass::Old);
        assert_eq!(classify_outcome(true, true, true), CrashOutcomeClass::New);
        assert_eq!(classify_outcome(false, true, true), CrashOutcomeClass::New);
        assert_eq!(
            classify_outcome(false, false, false),
            CrashOutcomeClass::Unknown
        );
    }

    #[test]
    fn hybrid_receipt_without_visibility_fails() {
        let exp = ExpectedReopen {
            acknowledged_visible: Some(true),
            no_fabricated_commit: true,
            salvageable: true,
            prior_durable_retained: true,
            notes: String::new(),
        };
        assert!(validate_reopen_expectation(&exp, true, false, true).is_err());
    }

    #[test]
    fn silent_skip_assert() {
        let mut r = CampaignReport::default();
        r.lane_skipped("x", "reason");
        r.assert_no_silent_skips();
    }
}
