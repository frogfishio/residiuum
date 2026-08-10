//! Shared total predicate profile (`residiuum-predicate-v1`) — APP-4.
//!
//! Normative: [RESIDIUUM_PREDICATE_SPEC.md](../../../RESIDIUUM_PREDICATE_SPEC.md).
//! Distinct from the legacy DX Mongo-style [`crate::filter::Filter`]:
//! predicate evaluation is **total** and treats `Absent` vs `Null` per the
//! Residiuum rules (e.g. `Absent != x` is **false**, not true).

use crate::error::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Profile identifier (same string as [`crate::PREDICATE_PROFILE`]).
pub const PREDICATE_PROFILE_V1: &str = "residiuum-predicate-v1";

/// Maximum normalized predicate AST nodes (CORE plan §9.2).
pub const MAX_PREDICATE_NODES: usize = 4_096;

/// Maximum path segments in one path.
pub const MAX_PATH_SEGMENTS: usize = 64;

/// Canonical path: each segment is a decoded map key string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Path(pub Vec<String>);

impl Path {
    /// Parse a dotted path (`a.b.c`). Bracket segments are not yet accepted in
    /// this cut (APP-5 / full surface).
    pub fn parse_dotted(s: &str) -> Result<Self, Error> {
        if s.is_empty() {
            return Err(Error::QueryInvalid("empty predicate path".into()));
        }
        let segs: Vec<String> = s.split('.').map(|p| p.to_string()).collect();
        if segs.iter().any(|p| p.is_empty()) {
            return Err(Error::QueryInvalid(format!(
                "invalid dotted path `{s}` (empty segment)"
            )));
        }
        if segs.len() > MAX_PATH_SEGMENTS {
            return Err(Error::QueryInvalid(format!(
                "path exceeds {MAX_PATH_SEGMENTS} segments"
            )));
        }
        Ok(Self(segs))
    }

    /// Path from already-decoded segments.
    pub fn from_segments(segs: impl IntoIterator<Item = impl Into<String>>) -> Result<Self, Error> {
        let segs: Vec<String> = segs.into_iter().map(Into::into).collect();
        if segs.is_empty() {
            return Err(Error::QueryInvalid("empty predicate path".into()));
        }
        if segs.len() > MAX_PATH_SEGMENTS {
            return Err(Error::QueryInvalid(format!(
                "path exceeds {MAX_PATH_SEGMENTS} segments"
            )));
        }
        Ok(Self(segs))
    }

    /// Dotted display form (segments must not contain `.` for round-trip).
    pub fn dotted(&self) -> String {
        self.0.join(".")
    }
}

/// Right- or left-hand operand of a comparison.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Operand {
    /// Document path.
    Path {
        /// Path segments.
        path: Path,
    },
    /// JSON literal (normalized host JSON value).
    Literal {
        /// Literal value.
        value: JsonValue,
    },
    /// Named query parameter (`$name` / bind map key).
    Param {
        /// Parameter name without `$`.
        name: String,
    },
}

impl Operand {
    /// Path operand.
    pub fn path(p: Path) -> Self {
        Self::Path { path: p }
    }

    /// Literal operand.
    pub fn literal(v: impl Into<JsonValue>) -> Self {
        Self::Literal { value: v.into() }
    }

    /// Parameter operand.
    pub fn param(name: impl Into<String>) -> Self {
        Self::Param { name: name.into() }
    }
}

/// Comparison operators (Residiuum Predicate Profile §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    /// `=`
    Eq,
    /// `!=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Lte,
    /// `>`
    Gt,
    /// `>=`
    Gte,
}

/// Total boolean AST for `residiuum-predicate-v1`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Predicate {
    /// Constant true.
    True,
    /// Constant false.
    False,
    /// Comparison between two operands.
    Cmp {
        /// Operator.
        cmp: CompareOp,
        /// Left operand.
        left: Operand,
        /// Right operand.
        right: Operand,
    },
    /// Membership: `operand in [literals…]`.
    In {
        /// Left operand.
        left: Operand,
        /// Literal list.
        list: Vec<JsonValue>,
        /// When true, `not in`.
        #[serde(default)]
        negated: bool,
    },
    /// `present(path)` — Present including Null.
    Present {
        /// Path.
        path: Path,
    },
    /// `missing(path)` — Absent.
    Missing {
        /// Path.
        path: Path,
    },
    /// `path is null` / `path is not null`.
    IsNull {
        /// Path.
        path: Path,
        /// When true, `is not null`.
        #[serde(default)]
        negated: bool,
    },
    /// `starts_with(path, string)`.
    StartsWith {
        /// Path to string field.
        path: Path,
        /// Required prefix.
        prefix: String,
    },
    /// `contains(path, literal)`.
    Contains {
        /// Path.
        path: Path,
        /// Needle (string substring or array element).
        needle: JsonValue,
    },
    /// Conjunction (empty ⇒ true).
    And {
        /// Children.
        args: Vec<Predicate>,
    },
    /// Disjunction (empty ⇒ false).
    Or {
        /// Children.
        args: Vec<Predicate>,
    },
    /// Negation.
    Not {
        /// Child.
        arg: Box<Predicate>,
    },
}

impl Predicate {
    /// Field equality to a named parameter.
    pub fn field_eq_param(path: Path, param_name: impl Into<String>) -> Self {
        Self::Cmp {
            cmp: CompareOp::Eq,
            left: Operand::path(path),
            right: Operand::param(param_name),
        }
    }

    /// Field equality to a literal.
    pub fn field_eq_literal(path: Path, value: impl Into<JsonValue>) -> Self {
        Self::Cmp {
            cmp: CompareOp::Eq,
            left: Operand::path(path),
            right: Operand::literal(value),
        }
    }

    /// Parse the compact plan-vector `where` shape:
    /// `{ "op": "eq", "path": ["status"], "param": "status" }` or `"value": …`.
    pub fn from_plan_json(v: &JsonValue) -> Result<Self, Error> {
        if v.is_null() {
            return Ok(Self::True);
        }
        let obj = v.as_object().ok_or_else(|| {
            Error::QueryInvalid("predicate plan json must be object or null".into())
        })?;
        let op = obj
            .get("op")
            .and_then(|x| x.as_str())
            .ok_or_else(|| Error::QueryInvalid("predicate missing op".into()))?;
        match op {
            "true" => Ok(Self::True),
            "false" => Ok(Self::False),
            "and" => {
                let args = obj
                    .get("args")
                    .and_then(|a| a.as_array())
                    .ok_or_else(|| Error::QueryInvalid("and requires args".into()))?;
                Ok(Self::And {
                    args: args
                        .iter()
                        .map(Self::from_plan_json)
                        .collect::<Result<Vec<_>, _>>()?,
                })
            }
            "or" => {
                let args = obj
                    .get("args")
                    .and_then(|a| a.as_array())
                    .ok_or_else(|| Error::QueryInvalid("or requires args".into()))?;
                Ok(Self::Or {
                    args: args
                        .iter()
                        .map(Self::from_plan_json)
                        .collect::<Result<Vec<_>, _>>()?,
                })
            }
            "not" => {
                let arg = obj
                    .get("arg")
                    .ok_or_else(|| Error::QueryInvalid("not requires arg".into()))?;
                Ok(Self::Not {
                    arg: Box::new(Self::from_plan_json(arg)?),
                })
            }
            "eq" | "ne" | "lt" | "lte" | "gt" | "gte" => {
                let cmp = match op {
                    "eq" => CompareOp::Eq,
                    "ne" => CompareOp::Ne,
                    "lt" => CompareOp::Lt,
                    "lte" => CompareOp::Lte,
                    "gt" => CompareOp::Gt,
                    "gte" => CompareOp::Gte,
                    _ => unreachable!(),
                };
                if let Some(path_v) = obj.get("path") {
                    let path = path_from_json(path_v)?;
                    let right = if let Some(p) = obj.get("param").and_then(|x| x.as_str()) {
                        Operand::param(p)
                    } else if let Some(val) = obj.get("value") {
                        Operand::literal(val.clone())
                    } else {
                        return Err(Error::QueryInvalid(
                            "field compare requires param or value".into(),
                        ));
                    };
                    return Ok(Self::Cmp {
                        cmp,
                        left: Operand::path(path),
                        right,
                    });
                }
                // Full form: left/right operands
                let left = operand_from_json(
                    obj.get("left")
                        .ok_or_else(|| Error::QueryInvalid("cmp requires path or left".into()))?,
                )?;
                let right = operand_from_json(
                    obj.get("right")
                        .ok_or_else(|| Error::QueryInvalid("cmp requires right".into()))?,
                )?;
                Ok(Self::Cmp { cmp, left, right })
            }
            "present" => Ok(Self::Present {
                path: path_from_json(
                    obj.get("path")
                        .ok_or_else(|| Error::QueryInvalid("present requires path".into()))?,
                )?,
            }),
            "missing" => Ok(Self::Missing {
                path: path_from_json(
                    obj.get("path")
                        .ok_or_else(|| Error::QueryInvalid("missing requires path".into()))?,
                )?,
            }),
            "is_null" => Ok(Self::IsNull {
                path: path_from_json(
                    obj.get("path")
                        .ok_or_else(|| Error::QueryInvalid("is_null requires path".into()))?,
                )?,
                negated: obj
                    .get("negated")
                    .and_then(|b| b.as_bool())
                    .unwrap_or(false),
            }),
            "in" => {
                let left = operand_from_json(
                    obj.get("left")
                        .ok_or_else(|| Error::QueryInvalid("in requires left".into()))?,
                )?;
                let list = obj
                    .get("list")
                    .and_then(|l| l.as_array())
                    .ok_or_else(|| Error::QueryInvalid("in requires list".into()))?
                    .clone();
                let negated = obj
                    .get("negated")
                    .and_then(|b| b.as_bool())
                    .unwrap_or(false);
                Ok(Self::In {
                    left,
                    list,
                    negated,
                })
            }
            "starts_with" => Ok(Self::StartsWith {
                path: path_from_json(
                    obj.get("path")
                        .ok_or_else(|| Error::QueryInvalid("starts_with requires path".into()))?,
                )?,
                prefix: obj
                    .get("prefix")
                    .and_then(|p| p.as_str())
                    .ok_or_else(|| Error::QueryInvalid("starts_with requires prefix".into()))?
                    .to_string(),
            }),
            "contains" => Ok(Self::Contains {
                path: path_from_json(
                    obj.get("path")
                        .ok_or_else(|| Error::QueryInvalid("contains requires path".into()))?,
                )?,
                needle: obj
                    .get("needle")
                    .cloned()
                    .ok_or_else(|| Error::QueryInvalid("contains requires needle".into()))?,
            }),
            other => Err(Error::QueryInvalid(format!(
                "unsupported predicate op `{other}` in plan json"
            ))),
        }
    }

    /// Count AST nodes (for ceiling checks).
    pub fn node_count(&self) -> usize {
        match self {
            Self::True | Self::False => 1,
            Self::Cmp { .. }
            | Self::In { .. }
            | Self::Present { .. }
            | Self::Missing { .. }
            | Self::IsNull { .. }
            | Self::StartsWith { .. }
            | Self::Contains { .. } => 1,
            Self::And { args } | Self::Or { args } => {
                1 + args.iter().map(Self::node_count).sum::<usize>()
            }
            Self::Not { arg } => 1 + arg.node_count(),
        }
    }

    /// Fail if node count exceeds [`MAX_PREDICATE_NODES`].
    pub fn check_node_ceiling(&self) -> Result<(), Error> {
        let n = self.node_count();
        if n > MAX_PREDICATE_NODES {
            return Err(Error::QueryInvalid(format!(
                "predicate has {n} nodes; ceiling is {MAX_PREDICATE_NODES}"
            )));
        }
        Ok(())
    }

    /// Total evaluation against a JSON document and bound parameters.
    ///
    /// Missing parameters are a bind error (not document-Absent).
    pub fn eval(
        &self,
        doc: &JsonValue,
        params: &std::collections::BTreeMap<String, JsonValue>,
    ) -> Result<bool, Error> {
        self.check_node_ceiling()?;
        eval_pred(self, doc, params)
    }

    /// Canonical JSON object for plan hashing (stable key order via BTreeMap).
    pub fn to_canonical_json(&self) -> JsonValue {
        use serde_json::Map;
        match self {
            Self::True => json_obj([("op", JsonValue::String("true".into()))]),
            Self::False => json_obj([("op", JsonValue::String("false".into()))]),
            Self::Cmp { cmp, left, right } => {
                let mut m = Map::new();
                m.insert("op".into(), JsonValue::String(cmp_str(*cmp).into()));
                m.insert("left".into(), operand_canonical(left));
                m.insert("right".into(), operand_canonical(right));
                JsonValue::Object(m)
            }
            Self::In {
                left,
                list,
                negated,
            } => {
                let mut m = Map::new();
                m.insert("op".into(), JsonValue::String("in".into()));
                m.insert("left".into(), operand_canonical(left));
                m.insert("list".into(), JsonValue::Array(list.clone()));
                m.insert("negated".into(), JsonValue::Bool(*negated));
                JsonValue::Object(m)
            }
            Self::Present { path } => {
                let mut m = Map::new();
                m.insert("op".into(), JsonValue::String("present".into()));
                m.insert("path".into(), path_canonical(path));
                JsonValue::Object(m)
            }
            Self::Missing { path } => {
                let mut m = Map::new();
                m.insert("op".into(), JsonValue::String("missing".into()));
                m.insert("path".into(), path_canonical(path));
                JsonValue::Object(m)
            }
            Self::IsNull { path, negated } => {
                let mut m = Map::new();
                m.insert("op".into(), JsonValue::String("is_null".into()));
                m.insert("path".into(), path_canonical(path));
                m.insert("negated".into(), JsonValue::Bool(*negated));
                JsonValue::Object(m)
            }
            Self::StartsWith { path, prefix } => {
                let mut m = Map::new();
                m.insert("op".into(), JsonValue::String("starts_with".into()));
                m.insert("path".into(), path_canonical(path));
                m.insert("prefix".into(), JsonValue::String(prefix.clone()));
                JsonValue::Object(m)
            }
            Self::Contains { path, needle } => {
                let mut m = Map::new();
                m.insert("op".into(), JsonValue::String("contains".into()));
                m.insert("path".into(), path_canonical(path));
                m.insert("needle".into(), needle.clone());
                JsonValue::Object(m)
            }
            Self::And { args } => {
                let mut m = Map::new();
                m.insert("op".into(), JsonValue::String("and".into()));
                m.insert(
                    "args".into(),
                    JsonValue::Array(args.iter().map(Self::to_canonical_json).collect()),
                );
                JsonValue::Object(m)
            }
            Self::Or { args } => {
                let mut m = Map::new();
                m.insert("op".into(), JsonValue::String("or".into()));
                m.insert(
                    "args".into(),
                    JsonValue::Array(args.iter().map(Self::to_canonical_json).collect()),
                );
                JsonValue::Object(m)
            }
            Self::Not { arg } => {
                let mut m = Map::new();
                m.insert("op".into(), JsonValue::String("not".into()));
                m.insert("arg".into(), arg.to_canonical_json());
                JsonValue::Object(m)
            }
        }
    }
}

fn json_obj(pairs: impl IntoIterator<Item = (&'static str, JsonValue)>) -> JsonValue {
    let mut m = serde_json::Map::new();
    for (k, v) in pairs {
        m.insert(k.into(), v);
    }
    JsonValue::Object(m)
}

fn cmp_str(c: CompareOp) -> &'static str {
    match c {
        CompareOp::Eq => "eq",
        CompareOp::Ne => "ne",
        CompareOp::Lt => "lt",
        CompareOp::Lte => "lte",
        CompareOp::Gt => "gt",
        CompareOp::Gte => "gte",
    }
}

fn path_canonical(path: &Path) -> JsonValue {
    JsonValue::Array(
        path.0
            .iter()
            .map(|s| JsonValue::String(s.clone()))
            .collect(),
    )
}

fn operand_canonical(op: &Operand) -> JsonValue {
    match op {
        Operand::Path { path } => json_obj([
            ("kind", JsonValue::String("path".into())),
            ("path", path_canonical(path)),
        ]),
        Operand::Literal { value } => json_obj([
            ("kind", JsonValue::String("literal".into())),
            ("value", value.clone()),
        ]),
        Operand::Param { name } => json_obj([
            ("kind", JsonValue::String("param".into())),
            ("name", JsonValue::String(name.clone())),
        ]),
    }
}

fn path_from_json(v: &JsonValue) -> Result<Path, Error> {
    let arr = v
        .as_array()
        .ok_or_else(|| Error::QueryInvalid("path must be string array".into()))?;
    let segs: Vec<String> = arr
        .iter()
        .map(|x| {
            x.as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| Error::QueryInvalid("path segment must be string".into()))
        })
        .collect::<Result<_, _>>()?;
    Path::from_segments(segs)
}

fn operand_from_json(v: &JsonValue) -> Result<Operand, Error> {
    let obj = v
        .as_object()
        .ok_or_else(|| Error::QueryInvalid("operand must be object".into()))?;
    match obj.get("kind").and_then(|k| k.as_str()) {
        Some("path") => Ok(Operand::path(path_from_json(
            obj.get("path")
                .ok_or_else(|| Error::QueryInvalid("path operand needs path".into()))?,
        )?)),
        Some("literal") => {
            Ok(Operand::literal(obj.get("value").cloned().ok_or_else(
                || Error::QueryInvalid("literal operand needs value".into()),
            )?))
        }
        Some("param") => Ok(Operand::param(
            obj.get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| Error::QueryInvalid("param operand needs name".into()))?,
        )),
        _ => Err(Error::QueryInvalid("unknown operand kind".into())),
    }
}

/// Path resolution result (Residiuum §5).
#[derive(Debug, Clone, PartialEq)]
pub enum Resolve {
    /// Path does not exist.
    Absent,
    /// Path exists (value may be JSON null).
    Present(JsonValue),
}

/// Resolve `path` on `doc`.
pub fn resolve_path(doc: &JsonValue, path: &Path) -> Resolve {
    let mut cur = doc;
    for seg in &path.0 {
        match cur {
            JsonValue::Object(map) => match map.get(seg) {
                Some(next) => cur = next,
                None => return Resolve::Absent,
            },
            _ => return Resolve::Absent,
        }
    }
    Resolve::Present(cur.clone())
}

fn eval_pred(
    p: &Predicate,
    doc: &JsonValue,
    params: &std::collections::BTreeMap<String, JsonValue>,
) -> Result<bool, Error> {
    match p {
        Predicate::True => Ok(true),
        Predicate::False => Ok(false),
        Predicate::Cmp { cmp, left, right } => {
            let l = resolve_operand(left, doc, params)?;
            let r = resolve_operand(right, doc, params)?;
            Ok(compare(*cmp, &l, &r))
        }
        Predicate::In {
            left,
            list,
            negated,
        } => {
            let l = resolve_operand(left, doc, params)?;
            let member = match &l {
                Resolve::Absent => false,
                Resolve::Present(v) => list.iter().any(|x| json_eq(v, x)),
            };
            Ok(if *negated {
                // absent → false for `not in`
                matches!(l, Resolve::Present(_)) && !member
            } else {
                member
            })
        }
        Predicate::Present { path } => Ok(matches!(resolve_path(doc, path), Resolve::Present(_))),
        Predicate::Missing { path } => Ok(matches!(resolve_path(doc, path), Resolve::Absent)),
        Predicate::IsNull { path, negated } => {
            let r = resolve_path(doc, path);
            let is_null = matches!(r, Resolve::Present(JsonValue::Null));
            Ok(if *negated {
                // present and not null
                matches!(r, Resolve::Present(ref v) if !v.is_null())
            } else {
                is_null
            })
        }
        Predicate::StartsWith { path, prefix } => match resolve_path(doc, path) {
            Resolve::Present(JsonValue::String(s)) => Ok(s.starts_with(prefix.as_str())),
            _ => Ok(false),
        },
        Predicate::Contains { path, needle } => match resolve_path(doc, path) {
            Resolve::Present(JsonValue::String(s)) => match needle {
                JsonValue::String(n) => Ok(s.contains(n.as_str())),
                _ => Ok(false),
            },
            Resolve::Present(JsonValue::Array(arr)) => Ok(arr.iter().any(|e| json_eq(e, needle))),
            _ => Ok(false),
        },
        Predicate::And { args } => {
            for a in args {
                if !eval_pred(a, doc, params)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Predicate::Or { args } => {
            for a in args {
                if eval_pred(a, doc, params)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Predicate::Not { arg } => Ok(!eval_pred(arg, doc, params)?),
    }
}

fn resolve_operand(
    op: &Operand,
    doc: &JsonValue,
    params: &std::collections::BTreeMap<String, JsonValue>,
) -> Result<Resolve, Error> {
    match op {
        Operand::Path { path } => Ok(resolve_path(doc, path)),
        Operand::Literal { value } => Ok(Resolve::Present(value.clone())),
        Operand::Param { name } => match params.get(name) {
            Some(v) => Ok(Resolve::Present(v.clone())),
            None => Err(Error::QueryInvalid(format!(
                "unbound predicate parameter `{name}`"
            ))),
        },
    }
}

fn compare(op: CompareOp, left: &Resolve, right: &Resolve) -> bool {
    use CompareOp::*;
    match op {
        Eq => match (left, right) {
            (Resolve::Absent, _) | (_, Resolve::Absent) => false,
            (Resolve::Present(a), Resolve::Present(b)) => json_eq(a, b),
        },
        Ne => match (left, right) {
            // Absent != anything ⇔ false (Residiuum §6.2)
            (Resolve::Absent, _) | (_, Resolve::Absent) => false,
            (Resolve::Present(a), Resolve::Present(b)) => !json_eq(a, b),
        },
        Lt | Lte | Gt | Gte => match (left, right) {
            (Resolve::Present(a), Resolve::Present(b)) => ord_cmp(op, a, b),
            _ => false,
        },
    }
}

fn json_eq(a: &JsonValue, b: &JsonValue) -> bool {
    // Structural equality: serde_json PartialEq is sufficient for JSON SDA-ish eq.
    a == b
}

fn ord_cmp(op: CompareOp, a: &JsonValue, b: &JsonValue) -> bool {
    use CompareOp::*;
    match (a, b) {
        (JsonValue::Number(x), JsonValue::Number(y)) => {
            let (Some(xf), Some(yf)) = (x.as_f64(), y.as_f64()) else {
                // Large integers: compare as strings only if both i64
                return match (x.as_i64(), y.as_i64()) {
                    (Some(xi), Some(yi)) => match op {
                        Lt => xi < yi,
                        Lte => xi <= yi,
                        Gt => xi > yi,
                        Gte => xi >= yi,
                        _ => false,
                    },
                    _ => false,
                };
            };
            match op {
                Lt => xf < yf,
                Lte => xf <= yf,
                Gt => xf > yf,
                Gte => xf >= yf,
                _ => false,
            }
        }
        (JsonValue::String(x), JsonValue::String(y)) => match op {
            Lt => x < y,
            Lte => x <= y,
            Gt => x > y,
            Gte => x >= y,
            _ => false,
        },
        _ => false,
    }
}

/// Fluent field builder for predicates (Application Core builder surface).
#[derive(Debug, Clone)]
pub struct PredField {
    path: Path,
}

/// Start a field path for a predicate.
pub fn field(path: &str) -> Result<PredField, Error> {
    Ok(PredField {
        path: Path::parse_dotted(path)?,
    })
}

/// Named parameter operand helper.
pub fn param(name: impl Into<String>) -> Operand {
    Operand::param(name)
}

impl PredField {
    /// Equality to a parameter or literal operand.
    pub fn eq(self, right: Operand) -> Predicate {
        Predicate::Cmp {
            cmp: CompareOp::Eq,
            left: Operand::path(self.path),
            right,
        }
    }

    /// Inequality.
    pub fn ne(self, right: Operand) -> Predicate {
        Predicate::Cmp {
            cmp: CompareOp::Ne,
            left: Operand::path(self.path),
            right,
        }
    }

    /// Less-than.
    pub fn lt(self, right: Operand) -> Predicate {
        Predicate::Cmp {
            cmp: CompareOp::Lt,
            left: Operand::path(self.path),
            right,
        }
    }

    /// Greater-than.
    pub fn gt(self, right: Operand) -> Predicate {
        Predicate::Cmp {
            cmp: CompareOp::Gt,
            left: Operand::path(self.path),
            right,
        }
    }

    /// Field present (including null).
    pub fn present(self) -> Predicate {
        Predicate::Present { path: self.path }
    }

    /// Field missing.
    pub fn missing(self) -> Predicate {
        Predicate::Missing { path: self.path }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn absent_ne_is_false() {
        let p = field("missing").unwrap().ne(Operand::literal("x"));
        let doc = json!({"status": "open"});
        let params = BTreeMap::new();
        assert!(!p.eval(&doc, &params).unwrap());
    }

    #[test]
    fn present_eq_param() {
        let p = field("status").unwrap().eq(param("status"));
        let doc = json!({"status": "open"});
        let mut params = BTreeMap::new();
        params.insert("status".into(), json!("open"));
        assert!(p.eval(&doc, &params).unwrap());
        params.insert("status".into(), json!("closed"));
        assert!(!p.eval(&doc, &params).unwrap());
    }

    #[test]
    fn unbound_param_errors() {
        let p = field("status").unwrap().eq(param("status"));
        let doc = json!({"status": "open"});
        let err = p.eval(&doc, &BTreeMap::new()).unwrap_err();
        assert!(matches!(err, Error::QueryInvalid(_)));
    }

    #[test]
    fn null_vs_absent() {
        let doc = json!({"a": null});
        let params = BTreeMap::new();
        assert!(Predicate::Present {
            path: Path::parse_dotted("a").unwrap()
        }
        .eval(&doc, &params)
        .unwrap());
        assert!(!Predicate::Missing {
            path: Path::parse_dotted("a").unwrap()
        }
        .eval(&doc, &params)
        .unwrap());
        assert!(Predicate::IsNull {
            path: Path::parse_dotted("a").unwrap(),
            negated: false
        }
        .eval(&doc, &params)
        .unwrap());
        assert!(!Predicate::IsNull {
            path: Path::parse_dotted("b").unwrap(),
            negated: false
        }
        .eval(&doc, &params)
        .unwrap());
    }

    /// Plan-json canonical encode → parse must round-trip every op
    /// (RQL-DQ1: `IsNull`/`In`/`StartsWith`/`Contains` were previously
    /// encodable via [`Predicate::to_canonical_json`] but not decodable via
    /// [`Predicate::from_plan_json`] — a latent gap that broke ISA
    /// encode/decode for `is null`, `in`, `starts_with`, `contains`).
    #[test]
    fn plan_json_round_trips_every_op() {
        let path = || Path::parse_dotted("a.b").unwrap();
        let cases = vec![
            Predicate::True,
            Predicate::False,
            Predicate::Cmp {
                cmp: CompareOp::Eq,
                left: Operand::path(path()),
                right: Operand::literal(json!(1)),
            },
            Predicate::In {
                left: Operand::path(path()),
                list: vec![json!(1), json!("x")],
                negated: true,
            },
            Predicate::Present { path: path() },
            Predicate::Missing { path: path() },
            Predicate::IsNull {
                path: path(),
                negated: true,
            },
            Predicate::StartsWith {
                path: path(),
                prefix: "pre".into(),
            },
            Predicate::Contains {
                path: path(),
                needle: json!("needle"),
            },
            Predicate::And {
                args: vec![Predicate::True, Predicate::False],
            },
            Predicate::Or {
                args: vec![Predicate::True, Predicate::False],
            },
            Predicate::Not {
                arg: Box::new(Predicate::True),
            },
        ];
        for p in cases {
            let j = p.to_canonical_json();
            let back = Predicate::from_plan_json(&j)
                .unwrap_or_else(|e| panic!("round-trip decode failed for {p:?}: {e}"));
            assert_eq!(back, p, "round-trip mismatch for {p:?}");
        }
    }
}
