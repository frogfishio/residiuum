//! Post-timing writers: result, histograms, timeseries + hashing.
//!
//! These MUST NOT be called from a timed probe path (`ProbeSession::is_timed_path`).

use super::aggregate::ThreadAggregate;
use super::probe::ProbeSession;
use super::result::ResultKernel;
use super::MetricsError;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::Path;

/// Refuse artifact I/O while still on the timed path.
pub fn assert_not_timed(session: &ProbeSession) -> Result<(), MetricsError> {
    if session.is_timed_path() {
        return Err(MetricsError::Msg(
            "artifact I/O forbidden on timed path".into(),
        ));
    }
    Ok(())
}

pub fn hash_bytes(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

pub fn write_result_json(path: &Path, result: &ResultKernel) -> Result<String, MetricsError> {
    let body = serde_json::to_vec_pretty(result)?;
    atomic_write(path, &body)?;
    Ok(hash_bytes(&body))
}

pub fn write_histograms_json(path: &Path, agg: &ThreadAggregate) -> Result<String, MetricsError> {
    #[derive(Serialize)]
    struct HistDoc<'a> {
        schema: &'static str,
        e2e: &'a super::histogram::LatencyHistogram,
        stages: &'a [super::histogram::LatencyHistogram],
        stage_names: &'static [&'static str],
    }
    let doc = HistDoc {
        schema: "residiuum-pqh3-histograms-v1",
        e2e: &agg.e2e_latency,
        stages: &agg.stage_latency,
        stage_names: crate::STAGES,
    };
    let body = serde_json::to_vec_pretty(&doc)?;
    atomic_write(path, &body)?;
    Ok(hash_bytes(&body))
}

/// Append NDJSON timeseries points (already collected — not from timed path).
pub fn write_timeseries_ndjson<T: Serialize>(
    path: &Path,
    points: &[T],
) -> Result<String, MetricsError> {
    let mut body = Vec::new();
    for p in points {
        serde_json::to_writer(&mut body, p)?;
        body.write_all(b"\n")?;
    }
    atomic_write(path, &body)?;
    Ok(hash_bytes(&body))
}

fn atomic_write(path: &Path, body: &[u8]) -> Result<(), MetricsError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, body)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::probe::{InstrumentationBudget, ProbeMode};
    use crate::metrics::result::ResultKernel;
    use serde_json::json;

    #[test]
    fn timed_path_blocks_writers() {
        let s = ProbeSession::new(ProbeMode::Aggregate, 0);
        assert!(assert_not_timed(&s).is_err());
    }

    #[test]
    fn writers_after_end_timed() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = ProbeSession::new(ProbeMode::Aggregate, 0);
        s.on_ack(0, 500, 32);
        s.end_timed_path();
        assert_not_timed(&s).unwrap();

        let result = ResultKernel::from_aggregate(
            "run-w",
            &s.agg,
            ProbeMode::Aggregate,
            &InstrumentationBudget::default(),
            None,
        );
        let h1 = write_result_json(&tmp.path().join("result.json"), &result).unwrap();
        let h2 = write_histograms_json(&tmp.path().join("histograms.json"), &s.agg).unwrap();
        let points = vec![json!({"t": 1, "ops": 1}), json!({"t": 2, "ops": 2})];
        let h3 = write_timeseries_ndjson(&tmp.path().join("timeseries.ndjson"), &points).unwrap();
        assert_eq!(h1.len(), 64);
        assert_eq!(h2.len(), 64);
        assert_eq!(h3.len(), 64);
        assert!(tmp.path().join("result.json").exists());
    }
}
