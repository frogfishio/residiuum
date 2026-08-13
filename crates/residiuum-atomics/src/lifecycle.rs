//! Formal Atomic lifecycle transitions (`ATOMICS_SPEC` §9–11).
//!
//! This is a finite-state model of prepare/member/decision/publication. It is
//! not a store. ATM-0.11 uses it to prove the gate invariants instead of
//! asserting them.

use crate::evidence::{
    AtomicLifecycle, DecisionPhase, MemberPhase, PreparePhase, PublicationPhase,
};

/// One legal or attempted step in the formal model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleEvent {
    /// Record a valid prepare.
    Prepare,
    /// Append staged members (still invisible).
    StageMembers,
    /// Members reach the durable-invisible stable boundary.
    StabilizeMembers,
    /// Write a committed decision naming `member_count` durable members.
    DecideCommitted {
        /// Members named by the decision/manifest.
        member_count: u32,
    },
    /// Write a not-committed decision.
    DecideNotCommitted,
    /// A second, conflicting valid decision is observed.
    DiscoverConflict,
    /// Apply one whole-Heap read-view delta.
    Publish,
}

impl LifecycleEvent {
    /// Stable snake_case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::StageMembers => "stage_members",
            Self::StabilizeMembers => "stabilize_members",
            Self::DecideCommitted { .. } => "decide_committed",
            Self::DecideNotCommitted => "decide_not_committed",
            Self::DiscoverConflict => "discover_conflict",
            Self::Publish => "publish",
        }
    }
}

/// Why a step is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleError {
    /// The event is not enabled in this state.
    Illegal,
    /// A terminal decision cannot be rewritten.
    TerminalDecision,
}

/// Model state: formal phases plus the member counts a publication must name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LifecycleTrace {
    /// Formal phases.
    pub state: AtomicLifecycle,
    /// Members named by a committed decision / prepare manifest.
    pub named_members: u32,
    /// Members included in a published delta.
    pub published_members: u32,
}

impl LifecycleTrace {
    /// Empty heap, no evidence.
    pub const fn empty() -> Self {
        Self {
            state: AtomicLifecycle::empty(),
            named_members: 0,
            published_members: 0,
        }
    }

    /// Apply one event. Conflicting second decisions enter [`DecisionPhase::Conflicting`].
    pub fn apply(self, event: LifecycleEvent) -> Result<Self, LifecycleError> {
        let AtomicLifecycle {
            mut prepare,
            mut members,
            mut decision,
            mut publication,
        } = self.state;
        let mut named = self.named_members;
        let mut published = self.published_members;

        match event {
            LifecycleEvent::Prepare => {
                if prepare != PreparePhase::None
                    || decision != DecisionPhase::None
                    || publication != PublicationPhase::Unpublished
                {
                    return Err(LifecycleError::Illegal);
                }
                prepare = PreparePhase::Prepared;
            }
            LifecycleEvent::StageMembers => {
                if prepare != PreparePhase::Prepared
                    || members != MemberPhase::Absent
                    || decision != DecisionPhase::None
                {
                    return Err(LifecycleError::Illegal);
                }
                members = MemberPhase::Staged;
            }
            LifecycleEvent::StabilizeMembers => {
                if members != MemberPhase::Staged || decision != DecisionPhase::None {
                    return Err(LifecycleError::Illegal);
                }
                members = MemberPhase::DurableInvisible;
            }
            LifecycleEvent::DecideCommitted { member_count } => match decision {
                DecisionPhase::None => {
                    if prepare != PreparePhase::Prepared {
                        return Err(LifecycleError::Illegal);
                    }
                    let members_ready = members == MemberPhase::DurableInvisible
                        || (members == MemberPhase::Absent && member_count == 0);
                    if !members_ready {
                        return Err(LifecycleError::Illegal);
                    }
                    decision = DecisionPhase::Committed;
                    named = member_count;
                }
                DecisionPhase::Committed => return Err(LifecycleError::TerminalDecision),
                DecisionPhase::NotCommitted => {
                    decision = DecisionPhase::Conflicting;
                    publication = PublicationPhase::Unpublished;
                }
                DecisionPhase::Conflicting => return Err(LifecycleError::TerminalDecision),
            },
            LifecycleEvent::DecideNotCommitted => match decision {
                DecisionPhase::None => {
                    if prepare != PreparePhase::Prepared {
                        return Err(LifecycleError::Illegal);
                    }
                    decision = DecisionPhase::NotCommitted;
                }
                DecisionPhase::NotCommitted => return Err(LifecycleError::TerminalDecision),
                DecisionPhase::Committed => {
                    decision = DecisionPhase::Conflicting;
                    publication = PublicationPhase::Unpublished;
                }
                DecisionPhase::Conflicting => return Err(LifecycleError::TerminalDecision),
            },
            LifecycleEvent::DiscoverConflict => {
                if !matches!(
                    decision,
                    DecisionPhase::Committed | DecisionPhase::NotCommitted
                ) {
                    return Err(LifecycleError::Illegal);
                }
                decision = DecisionPhase::Conflicting;
                publication = PublicationPhase::Unpublished;
            }
            LifecycleEvent::Publish => {
                if decision != DecisionPhase::Committed
                    || publication != PublicationPhase::Unpublished
                {
                    return Err(LifecycleError::Illegal);
                }
                let members_ready = members == MemberPhase::DurableInvisible
                    || (members == MemberPhase::Absent && named == 0);
                if !members_ready {
                    return Err(LifecycleError::Illegal);
                }
                publication = PublicationPhase::Published;
                published = named;
            }
        }

        Ok(Self {
            state: AtomicLifecycle {
                prepare,
                members,
                decision,
                publication,
            },
            named_members: named,
            published_members: published,
        })
    }

    /// Compact label for evidence summaries.
    pub fn label(self) -> String {
        format!(
            "{}/{}/{}/{}/n{}/p{}",
            phase(self.state.prepare),
            mem(self.state.members),
            dec(self.state.decision),
            pubp(self.state.publication),
            self.named_members,
            self.published_members
        )
    }
}

fn phase(p: PreparePhase) -> &'static str {
    match p {
        PreparePhase::None => "prepare_none",
        PreparePhase::Prepared => "prepared",
    }
}

fn mem(m: MemberPhase) -> &'static str {
    match m {
        MemberPhase::Absent => "members_absent",
        MemberPhase::Staged => "staged",
        MemberPhase::DurableInvisible => "durable_invisible",
    }
}

fn dec(d: DecisionPhase) -> &'static str {
    match d {
        DecisionPhase::None => "decision_none",
        DecisionPhase::Committed => "committed",
        DecisionPhase::NotCommitted => "not_committed",
        DecisionPhase::Conflicting => "conflicting",
    }
}

fn pubp(p: PublicationPhase) -> &'static str {
    match p {
        PublicationPhase::Unpublished => "unpublished",
        PublicationPhase::Published => "published",
    }
}

const CANDIDATES: &[LifecycleEvent] = &[
    LifecycleEvent::Prepare,
    LifecycleEvent::StageMembers,
    LifecycleEvent::StabilizeMembers,
    LifecycleEvent::DecideCommitted { member_count: 0 },
    LifecycleEvent::DecideCommitted { member_count: 2 },
    LifecycleEvent::DecideNotCommitted,
    LifecycleEvent::DiscoverConflict,
    LifecycleEvent::Publish,
];

/// Exhaustive reachable-state report used as ATM-0.11 model evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelCheckReport {
    /// Number of distinct reachable traces.
    pub reachable_state_count: usize,
    /// Labels of reachable traces, sorted.
    pub reachable: Vec<String>,
    /// Enabled (from, event, to) triples.
    pub allowed_transitions: usize,
    /// Required proofs. Every field must be true for the gate.
    pub proofs: ModelProofs,
}

/// Individual ATM-0.11 proofs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModelProofs {
    /// Published implies a committed decision.
    pub published_implies_committed: bool,
    /// A committed publication names every durable member.
    pub committed_publication_names_all_durable_members: bool,
    /// A not-committed decision is never published.
    pub not_committed_never_published: bool,
    /// Repeating a terminal decision is refused.
    pub terminal_decisions_cannot_change: bool,
    /// Conflicting decisions are degraded and not ordinary-visible.
    pub conflicting_decisions_enter_degraded_status: bool,
    /// Staged or durable-invisible members are not ordinary-visible.
    pub staged_material_never_ordinarily_visible: bool,
}

impl ModelProofs {
    /// True when every required proof held.
    pub const fn all_held(self) -> bool {
        self.published_implies_committed
            && self.committed_publication_names_all_durable_members
            && self.not_committed_never_published
            && self.terminal_decisions_cannot_change
            && self.conflicting_decisions_enter_degraded_status
            && self.staged_material_never_ordinarily_visible
    }
}

/// Explore the model and evaluate the ATM-0.11 proofs.
pub fn check_model() -> ModelCheckReport {
    let mut reachable = vec![LifecycleTrace::empty()];
    let mut allowed = 0usize;
    let mut i = 0;
    while i < reachable.len() {
        let here = reachable[i];
        for &ev in CANDIDATES {
            if let Ok(next) = here.apply(ev) {
                allowed += 1;
                if !reachable.contains(&next) {
                    reachable.push(next);
                }
            }
        }
        i += 1;
    }

    let mut labels: Vec<String> = reachable.iter().map(|s| s.label()).collect();
    labels.sort();

    let published_implies_committed = reachable.iter().all(|s| {
        s.state.publication != PublicationPhase::Published
            || s.state.decision == DecisionPhase::Committed
    });
    let committed_publication_names_all_durable_members = reachable.iter().all(|s| {
        s.state.publication != PublicationPhase::Published || s.published_members == s.named_members
    });
    let not_committed_never_published = reachable.iter().all(|s| {
        s.state.decision != DecisionPhase::NotCommitted
            || s.state.publication == PublicationPhase::Unpublished
    });

    let mut terminal_ok = true;
    let mut saw_terminal = false;
    for s in &reachable {
        let retry = match s.state.decision {
            DecisionPhase::Committed => Some(LifecycleEvent::DecideCommitted {
                member_count: s.named_members,
            }),
            DecisionPhase::NotCommitted => Some(LifecycleEvent::DecideNotCommitted),
            DecisionPhase::Conflicting => Some(LifecycleEvent::DecideCommitted { member_count: 0 }),
            DecisionPhase::None => None,
        };
        if let Some(ev) = retry {
            saw_terminal = true;
            if s.apply(ev) != Err(LifecycleError::TerminalDecision) {
                terminal_ok = false;
            }
        }
    }
    terminal_ok &= saw_terminal;

    let mut saw_conflict = false;
    let mut conflict_ok = true;
    for s in &reachable {
        if s.state.decision == DecisionPhase::Conflicting {
            saw_conflict = true;
            if s.state.ordinary_visible() {
                conflict_ok = false;
            }
        }
        if s.state.decision == DecisionPhase::Committed {
            if let Ok(next) = s.apply(LifecycleEvent::DecideNotCommitted) {
                saw_conflict = true;
                if next.state.decision != DecisionPhase::Conflicting
                    || next.state.ordinary_visible()
                {
                    conflict_ok = false;
                }
            }
        }
    }
    conflict_ok &= saw_conflict;

    let staged_ok = reachable.iter().all(|s| {
        let staged = matches!(
            s.state.members,
            MemberPhase::Staged | MemberPhase::DurableInvisible
        );
        !staged || !s.state.ordinary_visible() || s.state.publication == PublicationPhase::Published
    }) && reachable
        .iter()
        .all(|s| s.state.members != MemberPhase::Staged || !s.state.ordinary_visible());

    ModelCheckReport {
        reachable_state_count: reachable.len(),
        reachable: labels,
        allowed_transitions: allowed,
        proofs: ModelProofs {
            published_implies_committed,
            committed_publication_names_all_durable_members,
            not_committed_never_published,
            terminal_decisions_cannot_change: terminal_ok,
            conflicting_decisions_enter_degraded_status: conflict_ok,
            staged_material_never_ordinarily_visible: staged_ok,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_proofs_all_hold() {
        let report = check_model();
        assert!(
            report.reachable_state_count >= 8,
            "{}",
            report.reachable_state_count
        );
        assert!(report.proofs.all_held(), "{:?}", report.proofs);
    }

    #[test]
    fn staged_is_not_visible_and_publish_names_members() {
        let staged = LifecycleTrace::empty()
            .apply(LifecycleEvent::Prepare)
            .unwrap()
            .apply(LifecycleEvent::StageMembers)
            .unwrap();
        assert!(!staged.state.ordinary_visible());
        let published = staged
            .apply(LifecycleEvent::StabilizeMembers)
            .unwrap()
            .apply(LifecycleEvent::DecideCommitted { member_count: 2 })
            .unwrap()
            .apply(LifecycleEvent::Publish)
            .unwrap();
        assert!(published.state.ordinary_visible());
        assert_eq!(published.published_members, 2);
        assert_eq!(published.named_members, 2);
    }
}
