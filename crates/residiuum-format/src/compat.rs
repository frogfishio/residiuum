//! Wire format reader/writer compatibility matrix (DEF-052 / FORMAT_SPEC §12)
//! and major-1 freeze readiness (DEF-053 / `doc/wip/format/WIRE_MAJOR1_FREEZE.md`).
//!
//! Major versions may change framing semantics. Minor versions may add kinds,
//! flags, or envelope fields while preserving the ability of an older
//! same-major reader to locate, bound, verify, and retain unknown frames.
//!
//! This module is the **declared support window** for the current build. It
//! does not freeze the draft wire (`WIRE_PROFILE_LABEL`); freeze remains DEF-053
//! until every criterion is Met and a principal freeze declaration is recorded.

use crate::frame::{WIRE_MAJOR, WIRE_MINOR};
use crate::WIRE_PROFILE_LABEL;

/// Support status for a wire major generation in this build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireSupportStatus {
    /// This build writes this generation and fully reads it.
    Current,
    /// This build can read (locate/bound/verify/retain) but does not write it.
    ReadOnly,
    /// Still readable but scheduled for removal after a support window.
    Deprecated,
    /// Not supported: frames must be preserved as opaque evidence only.
    Unsupported,
}

/// One entry in the reader/writer matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireSupportEntry {
    /// Wire major version.
    pub major: u8,
    /// Lowest minor this entry covers (inclusive).
    pub min_minor: u8,
    /// Highest minor this entry covers (inclusive). `None` = open-ended.
    pub max_minor: Option<u8>,
    /// Whether this build can fully interpret frames of this major.
    pub can_read: bool,
    /// Whether this build emits this major on encode.
    pub can_write: bool,
    /// Lifecycle status.
    pub status: WireSupportStatus,
}

/// Wire majors this build can fully read (locate, bound, verify, retain).
///
/// Adjacent-generation dual-read is required before a new major is introduced
/// as a writer (FORMAT_SPEC §12, DEF-052).
pub const SUPPORTED_READER_MAJORS: &[u8] = &[WIRE_MAJOR];

/// Wire major this build writes on encode.
pub const WRITER_WIRE_MAJOR: u8 = WIRE_MAJOR;

/// Wire minor this build writes on encode.
pub const WRITER_WIRE_MINOR: u8 = WIRE_MINOR;

/// Policy id for the freeze checklist document (DEF-053 labor cut).
pub const WIRE_FREEZE_POLICY_ID: &str = "residiuum-wire-major1-freeze-v1";

/// Status of one freeze criterion (see `doc/wip/format/WIRE_MAJOR1_FREEZE.md` §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFreezeCriterionStatus {
    /// Implemented evidence exists and is accepted for this criterion.
    Met,
    /// Some evidence shipped; residual still blocks freeze.
    Partial,
    /// Not satisfied; freeze blocked.
    Open,
}

/// One freeze criterion entry for diagnostics and honesty checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireFreezeCriterion {
    /// Stable id (`F1` … `F16`).
    pub id: &'static str,
    /// Short title.
    pub title: &'static str,
    /// Current readiness.
    pub status: WireFreezeCriterionStatus,
}

/// Snapshot of freeze readiness for packaging / doctor / release notes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireFreezeReadiness {
    /// Current profile label (`1.0-draft` until freeze).
    pub profile_label: &'static str,
    /// Policy document id.
    pub policy_id: &'static str,
    /// Whether a freeze has been declared for this build.
    pub is_frozen: bool,
    /// Freeze criteria table (static).
    pub criteria: &'static [WireFreezeCriterion],
}

/// Declared reader/writer matrix for this build.
///
/// Today only major `1` (draft) is current. When a future major is introduced,
/// keep the prior major as [`WireSupportStatus::ReadOnly`] or
/// [`WireSupportStatus::Deprecated`] until the support window ends.
pub fn wire_compat_matrix() -> &'static [WireSupportEntry] {
    // Static table so operators and migration preflight share one source of truth.
    const MATRIX: &[WireSupportEntry] = &[WireSupportEntry {
        major: WIRE_MAJOR,
        min_minor: 0,
        max_minor: None,
        can_read: true,
        can_write: true,
        status: WireSupportStatus::Current,
    }];
    MATRIX
}

/// Whether this build can fully interpret frames of the given major.
pub fn wire_reader_supports(major: u8) -> bool {
    SUPPORTED_READER_MAJORS.contains(&major)
}

/// Whether this build encodes frames with the given major.
pub fn wire_writer_emits(major: u8) -> bool {
    major == WRITER_WIRE_MAJOR
}

/// Look up a matrix entry for `major` (first match).
pub fn wire_support_for(major: u8) -> Option<&'static WireSupportEntry> {
    wire_compat_matrix().iter().find(|e| e.major == major)
}

/// Human-readable summary of the support window (diagnostics / CLI).
pub fn wire_support_summary() -> String {
    format!(
        "writer={}.{} ({}); readers={:?}; matrix_entries={}",
        WRITER_WIRE_MAJOR,
        WRITER_WIRE_MINOR,
        WIRE_PROFILE_LABEL,
        SUPPORTED_READER_MAJORS,
        wire_compat_matrix().len()
    )
}

/// Freeze criteria for wire major 1 (DEF-053). Keep in sync with
/// `doc/wip/format/WIRE_MAJOR1_FREEZE.md` §2. Do **not** mark every row Met without
/// evidence and a principal freeze declaration.
pub fn wire_freeze_criteria() -> &'static [WireFreezeCriterion] {
    const CRITERIA: &[WireFreezeCriterion] = &[
        WireFreezeCriterion {
            id: "F1",
            title: "Framing layout + magics",
            status: WireFreezeCriterionStatus::Met,
        },
        WireFreezeCriterion {
            id: "F2",
            title: "CRC32C + BLAKE3-256 integrity",
            status: WireFreezeCriterionStatus::Met,
        },
        WireFreezeCriterion {
            id: "F3",
            title: "Safety limits / checked lengths",
            status: WireFreezeCriterionStatus::Met,
        },
        WireFreezeCriterion {
            id: "F4",
            title: "Deterministic CBOR envelopes",
            status: WireFreezeCriterionStatus::Met,
        },
        WireFreezeCriterion {
            id: "F5",
            title: "Chunk manifests / generation-exact path",
            status: WireFreezeCriterionStatus::Partial,
        },
        WireFreezeCriterion {
            id: "F6",
            title: "Conflict identity (event_id)",
            status: WireFreezeCriterionStatus::Met,
        },
        WireFreezeCriterion {
            id: "F7",
            title: "Recovery ordering / salvage",
            status: WireFreezeCriterionStatus::Met,
        },
        WireFreezeCriterion {
            id: "F8",
            title: "FORMAT_SPEC §13 automated corpus",
            status: WireFreezeCriterionStatus::Met,
        },
        WireFreezeCriterion {
            id: "F9",
            title: "Fuzzing schedule (long residual)",
            status: WireFreezeCriterionStatus::Partial,
        },
        WireFreezeCriterion {
            id: "F10",
            title: "Multi-implementation fixtures",
            status: WireFreezeCriterionStatus::Partial,
        },
        WireFreezeCriterion {
            id: "F11",
            title: "Production soak + long corruption",
            status: WireFreezeCriterionStatus::Open,
        },
        WireFreezeCriterion {
            id: "F12",
            title: "External review of wire surfaces",
            status: WireFreezeCriterionStatus::Open,
        },
        WireFreezeCriterion {
            id: "F13",
            title: "Canonical encodings inventory",
            status: WireFreezeCriterionStatus::Met,
        },
        WireFreezeCriterion {
            id: "F14",
            title: "Compatibility policy published",
            status: WireFreezeCriterionStatus::Met,
        },
        WireFreezeCriterion {
            id: "F15",
            title: "Golden multi-window upgrade/downgrade",
            status: WireFreezeCriterionStatus::Open,
        },
        WireFreezeCriterion {
            id: "F16",
            title: "Stable WIRE_PROFILE_LABEL declaration",
            status: WireFreezeCriterionStatus::Open,
        },
    ];
    CRITERIA
}

/// Whether every freeze criterion is [`WireFreezeCriterionStatus::Met`].
pub fn wire_freeze_criteria_all_met() -> bool {
    wire_freeze_criteria()
        .iter()
        .all(|c| c.status == WireFreezeCriterionStatus::Met)
}

/// Whether this build has declared a frozen wire profile.
///
/// Guard: while the profile label contains `draft`, freeze is **false** even
/// if criteria are later marked Met — principal must remove the draft suffix
/// and set F16 Met together.
pub fn wire_is_frozen() -> bool {
    !WIRE_PROFILE_LABEL.contains("draft") && wire_freeze_criteria_all_met()
}

/// Full freeze readiness snapshot (diagnostics / release honesty).
pub fn wire_freeze_readiness() -> WireFreezeReadiness {
    WireFreezeReadiness {
        profile_label: WIRE_PROFILE_LABEL,
        policy_id: WIRE_FREEZE_POLICY_ID,
        is_frozen: wire_is_frozen(),
        criteria: wire_freeze_criteria(),
    }
}

/// Human-readable freeze readiness line for doctor / packaging.
pub fn wire_freeze_summary() -> String {
    let r = wire_freeze_readiness();
    let mut met = 0usize;
    let mut partial = 0usize;
    let mut open = 0usize;
    for c in r.criteria {
        match c.status {
            WireFreezeCriterionStatus::Met => met += 1,
            WireFreezeCriterionStatus::Partial => partial += 1,
            WireFreezeCriterionStatus::Open => open += 1,
        }
    }
    format!(
        "profile={}; policy={}; frozen={}; criteria met={} partial={} open={}",
        r.profile_label, r.policy_id, r.is_frozen, met, partial, open
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_major_is_readable_and_writable() {
        assert!(wire_reader_supports(WIRE_MAJOR));
        assert!(wire_writer_emits(WIRE_MAJOR));
        let e = wire_support_for(WIRE_MAJOR).expect("entry");
        assert!(e.can_read && e.can_write);
        assert_eq!(e.status, WireSupportStatus::Current);
    }

    #[test]
    fn future_major_is_not_supported() {
        assert!(!wire_reader_supports(WIRE_MAJOR.saturating_add(1)));
        assert!(!wire_writer_emits(99));
        assert!(wire_support_for(99).is_none());
    }

    #[test]
    fn summary_mentions_draft_profile() {
        let s = wire_support_summary();
        assert!(s.contains(WIRE_PROFILE_LABEL));
        assert!(s.contains("writer="));
    }

    /// DEF-053 guard: do not ship a stable wire label while freeze is incomplete.
    #[test]
    fn def_053_wire_remains_draft_until_freeze() {
        assert_eq!(WIRE_PROFILE_LABEL, "1.0-draft");
        assert!(
            WIRE_PROFILE_LABEL.contains("draft"),
            "relabel only after freeze declaration"
        );
        assert!(!wire_is_frozen());
        assert!(!wire_freeze_criteria_all_met());
        let r = wire_freeze_readiness();
        assert_eq!(r.policy_id, WIRE_FREEZE_POLICY_ID);
        assert_eq!(r.profile_label, WIRE_PROFILE_LABEL);
        assert!(!r.is_frozen);
        assert_eq!(r.criteria.len(), 16);
        // At least one Open residual must remain while draft.
        assert!(r
            .criteria
            .iter()
            .any(|c| c.status == WireFreezeCriterionStatus::Open));
        let s = wire_freeze_summary();
        assert!(s.contains("frozen=false"));
        assert!(s.contains("1.0-draft"));
    }
}
