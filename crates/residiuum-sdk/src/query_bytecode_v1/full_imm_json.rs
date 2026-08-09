//! JSON serde for Full pipeline / project immediates in QVM (from retired isa.rs; Q0.A10).

use crate::error::Error;
use crate::predicate::{Path, Predicate};
use residiuum_heap::CollectionId;
use serde_json::{Map, Value as JsonValue};
use std::collections::BTreeMap;
use std::str::FromStr;

use super::full_attach::{
    EnrichCardinality, EnrichStepV1, FullPipelineStepV1, ProjectExprV1, ProjectItemV1, WithinStepV1,
};

pub(crate) fn pipeline_step_json(step: &FullPipelineStepV1) -> Result<JsonValue, Error> {
    match step {
        FullPipelineStepV1::Enrich(e) => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("enrich".into()));
            m.insert("output".into(), JsonValue::String(e.output.clone()));
            m.insert("using_name".into(), JsonValue::String(e.using_name.clone()));
            m.insert("using_id".into(), JsonValue::String(e.using_id.to_string()));
            m.insert("left".into(), path_json(&e.left));
            m.insert("right".into(), path_json(&e.right));
            m.insert("expect".into(), JsonValue::String(e.expect.as_str().into()));
            match &e.candidate_where {
                None => m.insert("candidate_where".into(), JsonValue::Null),
                Some(p) => m.insert("candidate_where".into(), p.to_canonical_json()),
            };
            Ok(btree_to_obj(m))
        }
        FullPipelineStepV1::Within(w) => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("within".into()));
            m.insert("carrier".into(), path_json(&w.carrier));
            match &w.element_alias {
                None => m.insert("element_alias".into(), JsonValue::Null),
                Some(a) => m.insert("element_alias".into(), JsonValue::String(a.clone())),
            };
            m.insert(
                "steps".into(),
                JsonValue::Array(
                    w.steps
                        .iter()
                        .map(pipeline_step_json)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            );
            Ok(btree_to_obj(m))
        }
        FullPipelineStepV1::Filter(p) => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("filter".into()));
            m.insert("where".into(), p.to_canonical_json());
            Ok(btree_to_obj(m))
        }
    }
}

pub(crate) fn parse_pipeline_step(v: &JsonValue) -> Result<FullPipelineStepV1, Error> {
    let obj = v
        .as_object()
        .ok_or_else(|| Error::QueryInvalid("pipeline step object".into()))?;
    match obj.get("kind").and_then(|k| k.as_str()) {
        Some("enrich") => {
            let output = req_str(obj, "output")?;
            let using_name = req_str(obj, "using_name")?;
            let using_id = CollectionId::from_str(req_str(obj, "using_id")?)
                .map_err(|e| Error::QueryInvalid(format!("using_id: {e}")))?;
            let left = parse_path(obj.get("left"))?;
            let right = parse_path(obj.get("right"))?;
            let expect = parse_expect(req_str(obj, "expect")?)?;
            let candidate_where = match obj.get("candidate_where") {
                None | Some(JsonValue::Null) => None,
                Some(w) => Some(Predicate::from_plan_json(w)?),
            };
            Ok(FullPipelineStepV1::Enrich(EnrichStepV1 {
                output: output.to_string(),
                using_name: using_name.to_string(),
                using_id,
                left,
                right,
                candidate_where,
                expect,
            }))
        }
        Some("within") => {
            let carrier = parse_path(obj.get("carrier"))?;
            let element_alias = match obj.get("element_alias") {
                None | Some(JsonValue::Null) => None,
                Some(JsonValue::String(s)) => Some(s.clone()),
                Some(_) => return Err(Error::QueryInvalid("element_alias".into())),
            };
            let steps = match obj.get("steps") {
                Some(JsonValue::Array(items)) => items
                    .iter()
                    .map(parse_pipeline_step)
                    .collect::<Result<Vec<_>, _>>()?,
                _ => return Err(Error::QueryInvalid("within.steps".into())),
            };
            Ok(FullPipelineStepV1::Within(WithinStepV1 {
                carrier,
                element_alias,
                steps,
            }))
        }
        Some("filter") => {
            let w = obj
                .get("where")
                .ok_or_else(|| Error::QueryInvalid("filter.where".into()))?;
            Ok(FullPipelineStepV1::Filter(Predicate::from_plan_json(w)?))
        }
        other => Err(Error::QueryInvalid(format!(
            "unknown pipeline kind `{other:?}`"
        ))),
    }
}

pub(crate) fn project_item_json(item: &ProjectItemV1) -> Result<JsonValue, Error> {
    match item {
        ProjectItemV1::Leaf { output, source } => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("leaf".into()));
            m.insert("output".into(), JsonValue::String(output.clone()));
            m.insert("source".into(), path_json(source));
            Ok(btree_to_obj(m))
        }
        ProjectItemV1::Nested { output, fields } => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("nested".into()));
            m.insert("output".into(), JsonValue::String(output.clone()));
            m.insert(
                "fields".into(),
                JsonValue::Array(
                    fields
                        .iter()
                        .map(project_item_json)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            );
            Ok(btree_to_obj(m))
        }
        ProjectItemV1::Computed { output, expression } => {
            let mut m = BTreeMap::new();
            m.insert("kind".into(), JsonValue::String("computed".into()));
            m.insert("output".into(), JsonValue::String(output.clone()));
            m.insert("expression".into(), project_expr_json(expression)?);
            Ok(btree_to_obj(m))
        }
    }
}

pub(crate) fn parse_project_item(v: &JsonValue) -> Result<ProjectItemV1, Error> {
    let obj = v
        .as_object()
        .ok_or_else(|| Error::QueryInvalid("project item object".into()))?;
    match obj.get("kind").and_then(|k| k.as_str()) {
        Some("leaf") => Ok(ProjectItemV1::Leaf {
            output: req_str(obj, "output")?.to_string(),
            source: parse_path(obj.get("source"))?,
        }),
        Some("nested") => {
            let fields = match obj.get("fields") {
                Some(JsonValue::Array(items)) => items
                    .iter()
                    .map(parse_project_item)
                    .collect::<Result<Vec<_>, _>>()?,
                _ => return Err(Error::QueryInvalid("nested.fields".into())),
            };
            Ok(ProjectItemV1::Nested {
                output: req_str(obj, "output")?.to_string(),
                fields,
            })
        }
        Some("computed") => Ok(ProjectItemV1::Computed {
            output: req_str(obj, "output")?.to_string(),
            expression: parse_project_expr(
                obj.get("expression")
                    .ok_or_else(|| Error::QueryInvalid("computed.expression".into()))?,
            )?,
        }),
        other => Err(Error::QueryInvalid(format!(
            "unknown project kind `{other:?}`"
        ))),
    }
}

fn project_expr_json(expr: &ProjectExprV1) -> Result<JsonValue, Error> {
    let mut m = BTreeMap::new();
    match expr {
        ProjectExprV1::Literal(value) => {
            m.insert("kind".into(), JsonValue::String("literal".into()));
            m.insert("value".into(), value.clone());
        }
        ProjectExprV1::Path(path) => {
            m.insert("kind".into(), JsonValue::String("path".into()));
            m.insert("path".into(), path_json(path));
        }
        ProjectExprV1::Conditional {
            when,
            then_expr,
            else_expr,
        } => {
            m.insert("kind".into(), JsonValue::String("conditional".into()));
            m.insert("when".into(), when.to_canonical_json());
            m.insert("then".into(), project_expr_json(then_expr)?);
            m.insert("else".into(), project_expr_json(else_expr)?);
        }
    }
    Ok(btree_to_obj(m))
}

fn parse_project_expr(v: &JsonValue) -> Result<ProjectExprV1, Error> {
    let obj = v
        .as_object()
        .ok_or_else(|| Error::QueryInvalid("project expression object".into()))?;
    match obj.get("kind").and_then(|k| k.as_str()) {
        Some("literal") => Ok(ProjectExprV1::Literal(
            obj.get("value").cloned().unwrap_or(JsonValue::Null),
        )),
        Some("path") => Ok(ProjectExprV1::Path(parse_path(obj.get("path"))?)),
        Some("conditional") => Ok(ProjectExprV1::Conditional {
            when: Predicate::from_plan_json(
                obj.get("when")
                    .ok_or_else(|| Error::QueryInvalid("conditional.when".into()))?,
            )?,
            then_expr: Box::new(parse_project_expr(
                obj.get("then")
                    .ok_or_else(|| Error::QueryInvalid("conditional.then".into()))?,
            )?),
            else_expr: Box::new(parse_project_expr(
                obj.get("else")
                    .ok_or_else(|| Error::QueryInvalid("conditional.else".into()))?,
            )?),
        }),
        other => Err(Error::QueryInvalid(format!(
            "unknown project expression kind `{other:?}`"
        ))),
    }
}

fn path_json(p: &Path) -> JsonValue {
    JsonValue::Array(p.0.iter().map(|s| JsonValue::String(s.clone())).collect())
}

fn parse_path(v: Option<&JsonValue>) -> Result<Path, Error> {
    match v {
        Some(JsonValue::Array(arr)) => {
            let segs: Vec<String> = arr
                .iter()
                .map(|x| {
                    x.as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| Error::QueryInvalid("path segment".into()))
                })
                .collect::<Result<_, _>>()?;
            Path::from_segments(segs)
        }
        Some(JsonValue::String(s)) => Path::parse_dotted(s),
        _ => Err(Error::QueryInvalid("path required".into())),
    }
}

fn parse_expect(s: &str) -> Result<EnrichCardinality, Error> {
    match s {
        "exactly_one" => Ok(EnrichCardinality::ExactlyOne),
        "optional" => Ok(EnrichCardinality::Optional),
        "many" => Ok(EnrichCardinality::Many),
        other => Err(Error::QueryInvalid(format!("expect `{other}`"))),
    }
}

fn req_str<'a>(obj: &'a Map<String, JsonValue>, key: &str) -> Result<&'a str, Error> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::QueryInvalid(format!("missing string `{key}`")))
}

fn btree_to_obj(m: BTreeMap<String, JsonValue>) -> JsonValue {
    let mut map = Map::new();
    for (k, v) in m {
        map.insert(k, v);
    }
    JsonValue::Object(map)
}
