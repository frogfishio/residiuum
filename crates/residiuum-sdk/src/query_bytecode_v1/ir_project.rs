//! Core path-project IR phase (RQL-IR1).
//!
//! Profile: **`residiuum-query-ir-project-v1`**
//! Normative: [QUERY_IR_PROJECT_V1.md](../../../../../doc/todo/rql/QUERY_IR_PROJECT_V1.md)
//!
//! Application Core `project` (path list) evaluates here — not an inline private
//! helper inside the page loop. Still a **Rust IR residual** (not an opcode
//! machine). Decision 0 remains OPEN; RQL-C1 must not be accepted.

use crate::error::Error;
use crate::predicate::{resolve_path, Path, Resolve};
use serde_json::Value as JsonValue;

/// IR profile id for Core path-project.
pub const PROJECT_IR_PROFILE: &str = "residiuum-query-ir-project-v1";

/// Apply Core path-project (identity when `paths` is None).
pub(crate) fn apply_project_paths(
    doc: &JsonValue,
    paths: Option<&Vec<Path>>,
) -> Result<JsonValue, Error> {
    let Some(paths) = paths else {
        return Ok(doc.clone());
    };
    let mut out = serde_json::Map::new();
    for p in paths {
        match resolve_path(doc, p) {
            Resolve::Present(v) => {
                // Flatten single-segment paths as object fields; multi-segment nest shallowly.
                if p.0.len() == 1 {
                    out.insert(p.0[0].clone(), v);
                } else {
                    out.insert(p.dotted(), v);
                }
            }
            Resolve::Absent => {}
        }
    }
    Ok(JsonValue::Object(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::Path;
    use serde_json::json;

    #[test]
    fn project_ir_profile_constant() {
        assert_eq!(PROJECT_IR_PROFILE, "residiuum-query-ir-project-v1");
    }

    #[test]
    fn identity_when_no_project() {
        let doc = json!({"a": 1, "b": 2});
        let out = apply_project_paths(&doc, None).unwrap();
        assert_eq!(out, doc);
    }

    #[test]
    fn single_and_multi_segment_paths() {
        let doc = json!({"a": 1, "nested": {"x": 9}});
        let paths = vec![
            Path::parse_dotted("a").unwrap(),
            Path::parse_dotted("nested.x").unwrap(),
        ];
        let out = apply_project_paths(&doc, Some(&paths)).unwrap();
        // Path project uses dotted keys for multi-segment paths.
        assert_eq!(out, json!({"a": 1, "nested.x": 9}));
    }

    #[test]
    fn absent_paths_omitted() {
        let doc = json!({"a": 1});
        let paths = vec![Path::parse_dotted("missing").unwrap()];
        let out = apply_project_paths(&doc, Some(&paths)).unwrap();
        assert_eq!(out, json!({}));
    }
}
