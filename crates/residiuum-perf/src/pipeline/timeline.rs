//! Sampled timeline consistency — reject omitted/reordered stage timestamps.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub seq: u64,
    pub stage: String,
    pub t_ns: u64,
    pub kind: String, // enter | exit
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimelineViolation {
    ReorderedTimestamp { prev: u64, got: u64 },
    ExitBeforeEnter { stage: String, seq: u64 },
    MissingExit { stage: String, seq: u64 },
    StageOrderBroken { seq: u64, detail: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineReport {
    pub ok: bool,
    pub violations: Vec<TimelineViolation>,
    pub event_count: usize,
}

/// Check monotonicity and enter/exit pairing per seq.
pub fn check_timeline(events: &[TimelineEvent]) -> TimelineReport {
    let mut violations = Vec::new();
    let mut last_t = 0u64;
    // open enters: (seq, stage) -> enter t
    let mut open: Vec<(u64, String, u64)> = Vec::new();

    for e in events {
        if e.t_ns < last_t {
            violations.push(TimelineViolation::ReorderedTimestamp {
                prev: last_t,
                got: e.t_ns,
            });
        }
        last_t = e.t_ns;

        match e.kind.as_str() {
            "enter" => {
                open.push((e.seq, e.stage.clone(), e.t_ns));
            }
            "exit" => {
                if let Some(pos) = open
                    .iter()
                    .rposition(|(s, st, _)| *s == e.seq && st == &e.stage)
                {
                    open.remove(pos);
                } else {
                    violations.push(TimelineViolation::ExitBeforeEnter {
                        stage: e.stage.clone(),
                        seq: e.seq,
                    });
                }
            }
            _ => {}
        }
    }
    for (seq, stage, _) in open {
        violations.push(TimelineViolation::MissingExit { stage, seq });
    }

    TimelineReport {
        ok: violations.is_empty(),
        event_count: events.len(),
        violations,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_monotonic_enter_exit() {
        let ev = vec![
            TimelineEvent {
                seq: 0,
                stage: "validation".into(),
                t_ns: 1,
                kind: "enter".into(),
            },
            TimelineEvent {
                seq: 0,
                stage: "validation".into(),
                t_ns: 5,
                kind: "exit".into(),
            },
        ];
        assert!(check_timeline(&ev).ok);
    }

    #[test]
    fn rejects_reorder() {
        let ev = vec![
            TimelineEvent {
                seq: 0,
                stage: "a".into(),
                t_ns: 10,
                kind: "enter".into(),
            },
            TimelineEvent {
                seq: 0,
                stage: "a".into(),
                t_ns: 5,
                kind: "exit".into(),
            },
        ];
        let r = check_timeline(&ev);
        assert!(!r.ok);
        assert!(r
            .violations
            .iter()
            .any(|v| matches!(v, TimelineViolation::ReorderedTimestamp { .. })));
    }

    #[test]
    fn rejects_missing_exit() {
        let ev = vec![TimelineEvent {
            seq: 1,
            stage: "encoding".into(),
            t_ns: 1,
            kind: "enter".into(),
        }];
        let r = check_timeline(&ev);
        assert!(!r.ok);
    }
}
