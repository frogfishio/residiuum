//! Corpus / logical fixture handles for the harness.
//!
//! Q4.1: load + validate corpus metadata; materialisation stays in generators
//! (`tools/rql_q1/materialise_fixture.py`) — harness records handles only.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

use crate::lane::LaneId;

#[derive(Debug, thiserror::Error)]
pub enum FixtureError {
    #[error("io: {0}")]
    Io(String),
    #[error("json: {0}")]
    Json(String),
    #[error("corpus: {0}")]
    Corpus(String),
}

/// Handle to one qualification corpus case (authority: intention + expected rule).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorpusCaseHandle {
    pub case_id: String,
    pub tier: String,
    pub domain: String,
    /// Plain-English intent (from corpus record).
    pub plain_english_intent: Option<String>,
    pub generator_id: Option<String>,
    pub seed: Option<u64>,
    /// Residiuum RQL source when present.
    pub rql_source: Option<String>,
    /// Lane eligibility flags from corpus / Q0.A4.
    pub server_lane_ineligible: bool,
    pub lane_hint: Option<LaneId>,
}

/// Loaded corpus index (not full fixture bytes).
#[derive(Debug, Clone)]
pub struct CorpusIndex {
    pub version: String,
    pub path: PathBuf,
    pub cases: Vec<CorpusCaseHandle>,
}

impl CorpusIndex {
    /// Load `spec/rql/qualification/corpus-v1/corpus-v1.json` (or compatible).
    pub fn load_json(path: impl AsRef<Path>) -> Result<Self, FixtureError> {
        let path = path.as_ref().to_path_buf();
        let raw = fs::read_to_string(&path).map_err(|e| FixtureError::Io(e.to_string()))?;
        let v: Value = serde_json::from_str(&raw).map_err(|e| FixtureError::Json(e.to_string()))?;
        let version = v
            .get("version")
            .or_else(|| v.get("corpus_version"))
            .and_then(|x| x.as_str())
            .unwrap_or("unknown")
            .to_string();
        let arr = v
            .get("cases")
            .and_then(|c| c.as_array())
            .ok_or_else(|| FixtureError::Corpus("missing cases array".into()))?;
        let mut cases = Vec::with_capacity(arr.len());
        for c in arr {
            let case_id = c
                .get("case_id")
                .or_else(|| c.get("id"))
                .and_then(|x| x.as_str())
                .ok_or_else(|| FixtureError::Corpus("case missing case_id".into()))?
                .to_string();
            let tier = c
                .get("tier")
                .and_then(|x| x.as_str())
                .unwrap_or("unknown")
                .to_string();
            let domain = c
                .get("domain")
                .and_then(|x| x.as_str())
                .unwrap_or("unknown")
                .to_string();
            let plain = c
                .get("plain_english_intent")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string());
            let gen = c
                .get("fixture_generator")
                .or_else(|| c.get("generator_id"))
                .and_then(|x| x.as_str())
                .map(|s| s.to_string())
                .or_else(|| {
                    c.get("fixture")
                        .and_then(|f| f.get("generator_id"))
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string())
                });
            let seed = c.get("seed").and_then(|x| x.as_u64()).or_else(|| {
                c.get("fixture")
                    .and_then(|f| f.get("seed"))
                    .and_then(|x| x.as_u64())
            });
            let rql_source = extract_rql_source(c);
            let server_lane_ineligible = c
                .get("server_lane_ineligible")
                .and_then(|x| x.as_bool())
                .unwrap_or(false);
            cases.push(CorpusCaseHandle {
                case_id,
                tier,
                domain,
                plain_english_intent: plain,
                generator_id: gen,
                seed,
                rql_source,
                server_lane_ineligible,
                lane_hint: None,
            });
        }
        Ok(Self {
            version,
            path,
            cases,
        })
    }

    pub fn tier_a_count(&self) -> usize {
        self.cases
            .iter()
            .filter(|c| c.tier.eq_ignore_ascii_case("A") || c.tier.eq_ignore_ascii_case("tier_a"))
            .count()
    }
}

fn extract_rql_source(c: &Value) -> Option<String> {
    if let Some(s) = c.get("source").and_then(|x| x.as_str()) {
        return Some(s.to_string());
    }
    if let Some(s) = c.get("rql").and_then(|x| x.as_str()) {
        return Some(s.to_string());
    }
    if let Some(s) = c
        .get("residiuum")
        .and_then(|r| r.get("source"))
        .and_then(|x| x.as_str())
    {
        return Some(s.to_string());
    }
    if let Some(impls) = c.get("implementations").and_then(|x| x.as_object()) {
        for key in ["residiuum", "rql", "Residiuum"] {
            if let Some(s) = impls
                .get(key)
                .and_then(|v| v.get("source").or_else(|| v.get("rql")))
                .and_then(|x| x.as_str())
            {
                return Some(s.to_string());
            }
        }
    }
    None
}

/// Resolve default corpus path from workspace root.
pub fn default_corpus_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join("spec/rql/qualification/corpus-v1/corpus-v1.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace")
    }

    #[test]
    fn load_live_corpus_index() {
        let path = default_corpus_path(&workspace_root());
        let idx = CorpusIndex::load_json(&path).expect("load corpus");
        assert!(
            idx.cases.len() >= 100,
            "expected ~100–150 corpus cases, got {}",
            idx.cases.len()
        );
        assert!(
            idx.cases.iter().any(|c| c.rql_source.is_some()),
            "at least one case should expose RQL source"
        );
    }
}
