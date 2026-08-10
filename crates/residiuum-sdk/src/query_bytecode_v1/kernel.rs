//! ENR+SDA kernel substrate for Core predicate meaning (RQL-X4).
//!
//! Profile: **`residiuum-query-kernel-sda-v1`**
//! Normative: [QUERY_KERNEL_SDA_V1.md](../../../../../doc/todo/rql/QUERY_KERNEL_SDA_V1.md)
//!
//! Application Core `where` evaluates through [`sda_core`] boolean programs
//! lowered from [`Predicate`]. Host remains scan/index/get only.
//!
//! [`Predicate::eval`] remains the equivalence oracle in tests — not a second
//! product path once Core page uses this module.

use crate::error::Error;
use crate::predicate::{CompareOp, Operand, Path, Predicate};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

/// Kernel profile id (SDA eval substrate for query meaning).
pub const KERNEL_PROFILE: &str = "residiuum-query-kernel-sda-v1";

/// Compiled Core `where` as a parsed SDA boolean program.
#[derive(Debug)]
pub struct CompiledKernelWhere {
    /// Profile stamp.
    pub profile: &'static str,
    /// SDA source text (params already substituted).
    pub source: String,
    prog: sda_core::Program,
    constant: Option<bool>,
    direct: Option<DirectPredicate>,
    requires_logical_key: bool,
}

#[derive(Debug)]
enum DirectPredicate {
    Cmp {
        op: CompareOp,
        path: Vec<String>,
        literal: JsonValue,
    },
    And(Vec<DirectPredicate>),
}

impl DirectPredicate {
    fn eval(&self, doc: &JsonValue) -> bool {
        match self {
            Self::Cmp { op, path, literal } => {
                resolve_ref(doc, path).is_some_and(|value| direct_compare(*op, value, literal))
            }
            Self::And(args) => args.iter().all(|arg| arg.eval(doc)),
        }
    }

    fn collect_fields(&self, fields: &mut Vec<String>) {
        match self {
            Self::Cmp { path, .. } => {
                let field = path.join(".");
                if !fields.contains(&field) {
                    fields.push(field);
                }
            }
            Self::And(args) => {
                for arg in args {
                    arg.collect_fields(fields);
                }
            }
        }
    }

    fn eval_projected(&self, fields: &[String], values: &[Option<JsonValue>]) -> Option<bool> {
        match self {
            Self::Cmp { op, path, literal } => {
                let field = path.join(".");
                let slot = fields.iter().position(|candidate| candidate == &field)?;
                Some(
                    values
                        .get(slot)?
                        .as_ref()
                        .is_some_and(|value| direct_compare(*op, value, literal)),
                )
            }
            Self::And(args) => {
                let mut result = true;
                for arg in args {
                    result &= arg.eval_projected(fields, values)?;
                }
                Some(result)
            }
        }
    }
}

impl CompiledKernelWhere {
    pub(crate) fn constant_value(&self) -> Option<bool> {
        self.constant
    }

    pub(crate) fn requires_logical_key(&self) -> bool {
        self.requires_logical_key
    }

    /// Exact body fields required by the allocation-light direct predicate.
    /// General SDA programs and predicates over the external logical key do
    /// not claim this shortcut.
    pub(crate) fn direct_fields(&self) -> Option<Vec<String>> {
        if self.requires_logical_key {
            return None;
        }
        let direct = self.direct.as_ref()?;
        let mut fields = Vec::new();
        direct.collect_fields(&mut fields);
        Some(fields)
    }

    /// Evaluate the direct predicate against presence-aware projected fields.
    pub(crate) fn eval_direct_projected(
        &self,
        fields: &[String],
        values: &[Option<JsonValue>],
    ) -> Option<bool> {
        self.direct.as_ref()?.eval_projected(fields, values)
    }

    /// Evaluate against one document (`input` binding).
    pub fn eval_doc(&self, doc: &JsonValue) -> Result<bool, Error> {
        if let Some(value) = self.constant {
            return Ok(value);
        }
        if let Some(direct) = &self.direct {
            return Ok(direct.eval(doc));
        }
        match self.prog.run_json("input", doc.clone()) {
            Ok(JsonValue::Bool(b)) => Ok(b),
            Ok(other) => Err(Error::QueryInvalid(format!(
                "kernel SDA expected bool, got {other}"
            ))),
            Err(e) => Err(Error::QueryInvalid(format!(
                "kernel SDA eval failed: {e}; src={}",
                self.source
            ))),
        }
    }
}

/// Lower Application Core `where` + bound params → SDA boolean program.
pub fn compile_where(
    pred: &Predicate,
    params: &BTreeMap<String, JsonValue>,
) -> Result<CompiledKernelWhere, Error> {
    let source = lower_predicate(pred, params)?;
    let prog = sda_core::Program::parse(&source)
        .map_err(|e| Error::QueryInvalid(format!("kernel SDA parse failed: {e}; src={source}")))?;
    let constant = match source.as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    };
    let direct = compile_direct(pred, params);
    let requires_logical_key = predicate_uses_logical_key(pred);
    Ok(CompiledKernelWhere {
        profile: KERNEL_PROFILE,
        source,
        prog,
        constant,
        direct,
        requires_logical_key,
    })
}

fn predicate_uses_logical_key(predicate: &Predicate) -> bool {
    let operand_uses_key = |operand: &Operand| matches!(operand, Operand::Path { path } if matches!(path.0.first().map(String::as_str), Some("_key" | "$key")));
    match predicate {
        Predicate::Cmp { left, right, .. } => operand_uses_key(left) || operand_uses_key(right),
        Predicate::In { left, .. } => operand_uses_key(left),
        Predicate::Present { path }
        | Predicate::Missing { path }
        | Predicate::IsNull { path, .. }
        | Predicate::StartsWith { path, .. }
        | Predicate::Contains { path, .. } => {
            matches!(path.0.first().map(String::as_str), Some("_key" | "$key"))
        }
        Predicate::And { args } | Predicate::Or { args } => {
            args.iter().any(predicate_uses_logical_key)
        }
        Predicate::Not { arg } => predicate_uses_logical_key(arg),
        Predicate::True | Predicate::False => false,
    }
}

fn compile_direct(
    pred: &Predicate,
    params: &BTreeMap<String, JsonValue>,
) -> Option<DirectPredicate> {
    match pred {
        Predicate::Cmp { cmp, left, right } => match (
            direct_operand(left, params)?,
            direct_operand(right, params)?,
        ) {
            (DirectOperand::Path(path), DirectOperand::Literal(literal)) => {
                Some(DirectPredicate::Cmp {
                    op: *cmp,
                    path,
                    literal,
                })
            }
            (DirectOperand::Literal(literal), DirectOperand::Path(path)) => {
                Some(DirectPredicate::Cmp {
                    op: reverse_compare(*cmp),
                    path,
                    literal,
                })
            }
            _ => None,
        },
        Predicate::And { args } if !args.is_empty() => args
            .iter()
            .map(|arg| compile_direct(arg, params))
            .collect::<Option<Vec<_>>>()
            .map(DirectPredicate::And),
        _ => None,
    }
}

enum DirectOperand {
    Path(Vec<String>),
    Literal(JsonValue),
}

fn direct_operand(
    operand: &Operand,
    params: &BTreeMap<String, JsonValue>,
) -> Option<DirectOperand> {
    match operand {
        Operand::Path { path } => Some(DirectOperand::Path(path.0.clone())),
        Operand::Literal { value } => Some(DirectOperand::Literal(value.clone())),
        Operand::Param { name } => params.get(name).cloned().map(DirectOperand::Literal),
    }
}

fn reverse_compare(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::Ne => CompareOp::Ne,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Lte => CompareOp::Gte,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Gte => CompareOp::Lte,
    }
}

fn resolve_ref<'a>(doc: &'a JsonValue, path: &[String]) -> Option<&'a JsonValue> {
    let mut current = doc;
    for segment in path {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

fn direct_compare(op: CompareOp, left: &JsonValue, right: &JsonValue) -> bool {
    match op {
        CompareOp::Eq => left == right,
        CompareOp::Ne => left != right,
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
            direct_order_compare(op, left, right)
        }
    }
}

fn direct_order_compare(op: CompareOp, left: &JsonValue, right: &JsonValue) -> bool {
    let compare = |ordering: std::cmp::Ordering| match op {
        CompareOp::Lt => ordering.is_lt(),
        CompareOp::Lte => ordering.is_le(),
        CompareOp::Gt => ordering.is_gt(),
        CompareOp::Gte => ordering.is_ge(),
        _ => false,
    };
    match (left, right) {
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            match (left.as_f64(), right.as_f64()) {
                (Some(left), Some(right)) => left.partial_cmp(&right).is_some_and(compare),
                _ => match (left.as_i64(), right.as_i64()) {
                    (Some(left), Some(right)) => compare(left.cmp(&right)),
                    _ => false,
                },
            }
        }
        (JsonValue::String(left), JsonValue::String(right)) => compare(left.cmp(right)),
        _ => false,
    }
}

/// Lower predicate AST → SDA boolean expression (no trailing `;`).
pub fn lower_predicate(
    pred: &Predicate,
    params: &BTreeMap<String, JsonValue>,
) -> Result<String, Error> {
    lower_pred(pred, params)
}

fn lower_pred(pred: &Predicate, params: &BTreeMap<String, JsonValue>) -> Result<String, Error> {
    match pred {
        Predicate::True => Ok("true".into()),
        Predicate::False => Ok("false".into()),
        Predicate::Cmp { cmp, left, right } => lower_cmp(*cmp, left, right, params),
        Predicate::In {
            left,
            list,
            negated,
        } => {
            let gp = match resolve_operand(left, params)? {
                OpLower::Path(p) => path_get(&p)?,
                OpLower::Lit(_) => {
                    return Err(Error::QueryInvalid(
                        "kernel: `in` left operand must be a path".into(),
                    ));
                }
            };
            let items = list
                .iter()
                .map(json_to_sda_literal)
                .collect::<Vec<_>>()
                .join(", ");
            let member = format!("mapOpt({gp}, x => x in Seq[{items}]) = Some(true)");
            if *negated {
                // Residiuum: absent → false for `not in`; present ∩ not member.
                Ok(format!("({gp} != None and not ({member}))"))
            } else {
                Ok(member)
            }
        }
        Predicate::Present { path } => {
            let gp = path_get(path)?;
            Ok(format!("{gp} != None"))
        }
        Predicate::Missing { path } => {
            let gp = path_get(path)?;
            Ok(format!("{gp} = None"))
        }
        Predicate::IsNull { path, negated } => {
            let gp = path_get(path)?;
            if *negated {
                // present and not null
                Ok(format!("mapOpt({gp}, x => x != null) = Some(true)"))
            } else {
                Ok(format!("{gp} = Some(null)"))
            }
        }
        Predicate::StartsWith { path, prefix } => {
            let gp = path_get(path)?;
            Ok(format!(
                "mapOpt({gp}, s => startsWith(s, {})) = Some(true)",
                sda_string_literal(prefix)
            ))
        }
        Predicate::Contains { path, needle } => {
            let gp = path_get(path)?;
            let lit = json_to_sda_literal(needle);
            if needle.is_string() {
                Ok(format!(
                    "((mapOpt({gp}, v => typeOf(v) = \"str\") = Some(true) and mapOpt({gp}, v => strContains(v, {lit})) = Some(true)) or (mapOpt({gp}, v => typeOf(v) = \"seq\") = Some(true) and mapOpt({gp}, v => {lit} in v) = Some(true)))"
                ))
            } else {
                Ok(format!("mapOpt({gp}, v => {lit} in v) = Some(true)"))
            }
        }
        Predicate::And { args } => {
            if args.is_empty() {
                return Ok("true".into());
            }
            let parts = args
                .iter()
                .map(|a| Ok(format!("({})", lower_pred(a, params)?)))
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(parts.join(" and "))
        }
        Predicate::Or { args } => {
            if args.is_empty() {
                return Ok("false".into());
            }
            let parts = args
                .iter()
                .map(|a| Ok(format!("({})", lower_pred(a, params)?)))
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(parts.join(" or "))
        }
        Predicate::Not { arg } => Ok(format!("not ({})", lower_pred(arg, params)?)),
    }
}

enum OpLower {
    Path(Path),
    Lit(JsonValue),
}

fn resolve_operand(op: &Operand, params: &BTreeMap<String, JsonValue>) -> Result<OpLower, Error> {
    match op {
        Operand::Path { path } => Ok(OpLower::Path(path.clone())),
        Operand::Literal { value } => Ok(OpLower::Lit(value.clone())),
        Operand::Param { name } => match params.get(name) {
            Some(v) => Ok(OpLower::Lit(v.clone())),
            None => Err(Error::QueryInvalid(format!(
                "unbound predicate parameter `{name}`"
            ))),
        },
    }
}

fn lower_cmp(
    cmp: CompareOp,
    left: &Operand,
    right: &Operand,
    params: &BTreeMap<String, JsonValue>,
) -> Result<String, Error> {
    let l = resolve_operand(left, params)?;
    let r = resolve_operand(right, params)?;
    match (l, r) {
        (OpLower::Lit(a), OpLower::Lit(b)) => {
            let ok = lit_compare(cmp, &a, &b);
            Ok(if ok { "true" } else { "false" }.into())
        }
        (OpLower::Path(p), OpLower::Lit(v)) => path_lit_cmp(cmp, &p, &v),
        (OpLower::Lit(v), OpLower::Path(p)) => match cmp {
            CompareOp::Eq | CompareOp::Ne => path_lit_cmp(cmp, &p, &v),
            CompareOp::Lt => path_lit_cmp(CompareOp::Gt, &p, &v),
            CompareOp::Lte => path_lit_cmp(CompareOp::Gte, &p, &v),
            CompareOp::Gt => path_lit_cmp(CompareOp::Lt, &p, &v),
            CompareOp::Gte => path_lit_cmp(CompareOp::Lte, &p, &v),
        },
        (OpLower::Path(a), OpLower::Path(b)) => path_path_cmp(cmp, &a, &b),
    }
}

fn path_lit_cmp(cmp: CompareOp, path: &Path, lit: &JsonValue) -> Result<String, Error> {
    let gp = path_get(path)?;
    let lit_s = json_to_sda_literal(lit);
    Ok(match cmp {
        // Absent = x → false (None ≠ Some)
        CompareOp::Eq => format!("{gp} = Some({lit_s})"),
        // Absent != x → false (mapOpt None → None ≠ Some(true))
        CompareOp::Ne => format!("mapOpt({gp}, x => x != {lit_s}) = Some(true)"),
        CompareOp::Lt => format!("mapOpt({gp}, x => x < {lit_s}) = Some(true)"),
        CompareOp::Lte => format!("mapOpt({gp}, x => x <= {lit_s}) = Some(true)"),
        CompareOp::Gt => format!("mapOpt({gp}, x => x > {lit_s}) = Some(true)"),
        CompareOp::Gte => format!("mapOpt({gp}, x => x >= {lit_s}) = Some(true)"),
    })
}

fn path_path_cmp(cmp: CompareOp, a: &Path, b: &Path) -> Result<String, Error> {
    let ga = path_get(a)?;
    let gb = path_get(b)?;
    // Both must be Present; Absent on either side → false (Residiuum §6.2).
    Ok(match cmp {
        CompareOp::Eq => format!("({ga} != None and {gb} != None and {ga} = {gb})"),
        CompareOp::Ne => format!("({ga} != None and {gb} != None and {ga} != {gb})"),
        CompareOp::Lt => {
            format!("mapOpt({ga}, x => mapOpt({gb}, y => x < y)) = Some(Some(true))")
        }
        CompareOp::Lte => {
            format!("mapOpt({ga}, x => mapOpt({gb}, y => x <= y)) = Some(Some(true))")
        }
        CompareOp::Gt => {
            format!("mapOpt({ga}, x => mapOpt({gb}, y => x > y)) = Some(Some(true))")
        }
        CompareOp::Gte => {
            format!("mapOpt({ga}, x => mapOpt({gb}, y => x >= y)) = Some(Some(true))")
        }
    })
}

fn path_get(path: &Path) -> Result<String, Error> {
    if path.0.first().map(|s| s.as_str()) == Some("$key") {
        return Err(Error::QueryInvalid(
            "kernel: `$key` in where is residual (not yet lowered to SDA)".into(),
        ));
    }
    let segs: Vec<String> = path.0.iter().map(|s| sda_string_literal(s)).collect();
    Ok(format!("getPath(input, Seq[{}])", segs.join(", ")))
}

fn lit_compare(cmp: CompareOp, a: &JsonValue, b: &JsonValue) -> bool {
    match cmp {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
            // Mirror predicate ord_cmp for constants.
            match (a, b) {
                (JsonValue::Number(x), JsonValue::Number(y)) => {
                    let (Some(xf), Some(yf)) = (x.as_f64(), y.as_f64()) else {
                        return match (x.as_i64(), y.as_i64()) {
                            (Some(xi), Some(yi)) => match cmp {
                                CompareOp::Lt => xi < yi,
                                CompareOp::Lte => xi <= yi,
                                CompareOp::Gt => xi > yi,
                                CompareOp::Gte => xi >= yi,
                                _ => false,
                            },
                            _ => false,
                        };
                    };
                    match cmp {
                        CompareOp::Lt => xf < yf,
                        CompareOp::Lte => xf <= yf,
                        CompareOp::Gt => xf > yf,
                        CompareOp::Gte => xf >= yf,
                        _ => false,
                    }
                }
                (JsonValue::String(x), JsonValue::String(y)) => match cmp {
                    CompareOp::Lt => x < y,
                    CompareOp::Lte => x <= y,
                    CompareOp::Gt => x > y,
                    CompareOp::Gte => x >= y,
                    _ => false,
                },
                _ => false,
            }
        }
    }
}

fn sda_string_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn json_to_sda_literal(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "null".into(),
        JsonValue::Bool(b) => if *b { "true" } else { "false" }.into(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => sda_string_literal(s),
        JsonValue::Array(items) => {
            let inner = items
                .iter()
                .map(json_to_sda_literal)
                .collect::<Vec<_>>()
                .join(", ");
            format!("Seq[{inner}]")
        }
        JsonValue::Object(map) => {
            let inner = map
                .iter()
                .map(|(k, val)| {
                    format!("{} -> {}", sda_string_literal(k), json_to_sda_literal(val))
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("Map{{{inner}}}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::{CompareOp, Operand, Path};
    use serde_json::json;

    fn eq_oracle(pred: &Predicate, doc: &JsonValue, params: &BTreeMap<String, JsonValue>) {
        let k = compile_where(pred, params).expect("compile");
        let via_kernel = k.eval_doc(doc).expect("kernel");
        let via_pred = pred.eval(doc, params).expect("pred");
        assert_eq!(
            via_kernel, via_pred,
            "mismatch kernel={via_kernel} pred={via_pred}\nsrc={}\ndoc={doc}",
            k.source
        );
    }

    #[test]
    fn kernel_profile_constant() {
        assert_eq!(KERNEL_PROFILE, "residiuum-query-kernel-sda-v1");
    }

    #[test]
    fn eq_absent_null_and_value() {
        let pred = Predicate::Cmp {
            cmp: CompareOp::Eq,
            left: Operand::path(Path::parse_dotted("status").unwrap()),
            right: Operand::literal(json!("active")),
        };
        let params = BTreeMap::new();
        eq_oracle(&pred, &json!({}), &params);
        eq_oracle(&pred, &json!({"status": null}), &params);
        eq_oracle(&pred, &json!({"status": "active"}), &params);
        eq_oracle(&pred, &json!({"status": "paused"}), &params);
    }

    #[test]
    fn ne_absent_is_false() {
        let pred = Predicate::Cmp {
            cmp: CompareOp::Ne,
            left: Operand::path(Path::parse_dotted("status").unwrap()),
            right: Operand::literal(json!("active")),
        };
        let params = BTreeMap::new();
        eq_oracle(&pred, &json!({}), &params);
        eq_oracle(&pred, &json!({"status": "active"}), &params);
        eq_oracle(&pred, &json!({"status": "x"}), &params);
    }

    #[test]
    fn empty_array_eq_and_ne() {
        let path = Path::parse_dotted("tags").unwrap();
        let eq = Predicate::Cmp {
            cmp: CompareOp::Eq,
            left: Operand::path(path.clone()),
            right: Operand::literal(json!([])),
        };
        let ne = Predicate::Cmp {
            cmp: CompareOp::Ne,
            left: Operand::path(path),
            right: Operand::literal(json!([])),
        };
        let params = BTreeMap::new();
        eq_oracle(&eq, &json!({}), &params);
        eq_oracle(&eq, &json!({"tags": null}), &params);
        eq_oracle(&eq, &json!({"tags": []}), &params);
        eq_oracle(&eq, &json!({"tags": ["a"]}), &params);
        eq_oracle(&ne, &json!({}), &params);
        eq_oracle(&ne, &json!({"tags": []}), &params);
        eq_oracle(&ne, &json!({"tags": ["x"]}), &params);
    }

    #[test]
    fn array_contains_element() {
        let pred = Predicate::Contains {
            path: Path::parse_dotted("tags").unwrap(),
            needle: json!("vip"),
        };
        let params = BTreeMap::new();
        eq_oracle(&pred, &json!({}), &params);
        eq_oracle(&pred, &json!({"tags": []}), &params);
        eq_oracle(&pred, &json!({"tags": ["vip"]}), &params);
        eq_oracle(&pred, &json!({"tags": "vip-extra"}), &params);
    }

    #[test]
    fn present_missing_isnull() {
        let params = BTreeMap::new();
        let path = Path::parse_dotted("x").unwrap();
        eq_oracle(
            &Predicate::Present { path: path.clone() },
            &json!({}),
            &params,
        );
        eq_oracle(
            &Predicate::Present { path: path.clone() },
            &json!({"x": null}),
            &params,
        );
        eq_oracle(
            &Predicate::Missing { path: path.clone() },
            &json!({}),
            &params,
        );
        eq_oracle(
            &Predicate::IsNull {
                path: path.clone(),
                negated: false,
            },
            &json!({"x": null}),
            &params,
        );
        eq_oracle(
            &Predicate::IsNull {
                path,
                negated: true,
            },
            &json!({"x": 1}),
            &params,
        );
    }

    #[test]
    fn param_eq() {
        let mut params = BTreeMap::new();
        params.insert("s".into(), json!("active"));
        let pred = Predicate::Cmp {
            cmp: CompareOp::Eq,
            left: Operand::path(Path::parse_dotted("status").unwrap()),
            right: Operand::param("s"),
        };
        eq_oracle(&pred, &json!({"status": "active"}), &params);
        eq_oracle(&pred, &json!({"status": "paused"}), &params);
        eq_oracle(&pred, &json!({}), &params);
    }

    #[test]
    fn starts_with_and_contains() {
        let params = BTreeMap::new();
        let pred = Predicate::StartsWith {
            path: Path::parse_dotted("name").unwrap(),
            prefix: "Ad".into(),
        };
        eq_oracle(&pred, &json!({"name": "Ada"}), &params);
        eq_oracle(&pred, &json!({"name": "Bob"}), &params);
        eq_oracle(&pred, &json!({}), &params);
        let pred = Predicate::Contains {
            path: Path::parse_dotted("tags").unwrap(),
            needle: json!("x"),
        };
        eq_oracle(&pred, &json!({"tags": ["x", "y"]}), &params);
        eq_oracle(&pred, &json!({"tags": ["z"]}), &params);
    }

    #[test]
    fn direct_kernel_admits_nested_compound_and_matches_oracle() {
        let pred = Predicate::And {
            args: vec![
                Predicate::Cmp {
                    cmp: CompareOp::Gte,
                    left: Operand::path(Path::parse_dotted("amount").unwrap()),
                    right: Operand::literal(json!(100)),
                },
                Predicate::Cmp {
                    cmp: CompareOp::Lt,
                    left: Operand::path(Path::parse_dotted("amount").unwrap()),
                    right: Operand::literal(json!(500)),
                },
                Predicate::Cmp {
                    cmp: CompareOp::Eq,
                    left: Operand::path(Path::parse_dotted("nested.l1.flag").unwrap()),
                    right: Operand::literal(json!(true)),
                },
            ],
        };
        let params = BTreeMap::new();
        let kernel = compile_where(&pred, &params).unwrap();
        assert!(kernel.direct.is_some());
        for doc in [
            json!({"amount": 100, "nested": {"l1": {"flag": true}}}),
            json!({"amount": 499.5, "nested": {"l1": {"flag": true}}}),
            json!({"amount": 500, "nested": {"l1": {"flag": true}}}),
            json!({"amount": 200, "nested": {"l1": {}}}),
            json!({}),
        ] {
            eq_oracle(&pred, &doc, &params);
        }
    }

    #[test]
    fn unsupported_predicate_retains_sda_fallback() {
        let pred = Predicate::Contains {
            path: Path::parse_dotted("tags").unwrap(),
            needle: json!("x"),
        };
        let kernel = compile_where(&pred, &BTreeMap::new()).unwrap();
        assert!(kernel.direct.is_none());
    }
}
