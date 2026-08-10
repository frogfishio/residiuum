use crate::ast::*;
use crate::stdlib;
use crate::{Env, Value};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EvalError {
    #[error("Unbound variable: {0}")]
    UnboundVar(String),
    #[error("Type error: {0}")]
    TypeError(String),
    #[error("Missing key: {0}")]
    MissingKey(String),
    #[error("Wrong shape: {0}")]
    WrongShape(String),
    #[error("Duplicate key: {0}")]
    DuplicateKey(String),
    #[error("Not callable: {0}")]
    NotCallable(String),
    #[error("Arity mismatch: expected {expected}, got {got}")]
    ArityMismatch { expected: usize, got: usize },
}

fn fail_value(code: &str, msg: &str) -> Value {
    Value::Fail_(code.to_string(), msg.to_string())
}

fn wrong_shape_value() -> Value {
    fail_value("t_sda_wrong_shape", "wrong shape")
}

fn div_by_zero_value() -> Value {
    fail_value("t_sda_div_by_zero", "division by zero")
}

fn unbound_name_value() -> Value {
    fail_value("t_sda_unbound_name", "unbound name")
}

fn not_callable_value() -> Value {
    fail_value("t_sda_not_callable", "not callable")
}

fn arity_mismatch_value() -> Value {
    fail_value("t_sda_arity_mismatch", "arity mismatch")
}

pub(crate) fn ensure_comparable(value: &Value) -> Result<(), EvalError> {
    match value {
        Value::Null
        | Value::Bool(_)
        | Value::Num(_)
        | Value::Str(_)
        | Value::Bytes(_)
        | Value::None_
        | Value::Fail_(_, _) => Ok(()),
        Value::Seq(items) | Value::Set(items) | Value::Bag(items) => {
            for item in items {
                ensure_comparable(item)?;
            }
            Ok(())
        }
        Value::Map(entries) | Value::Prod(entries) => {
            for (_, value) in entries {
                ensure_comparable(value)?;
            }
            Ok(())
        }
        Value::BagKV(pairs) => {
            for (key, value) in pairs {
                ensure_comparable(key)?;
                ensure_comparable(value)?;
            }
            Ok(())
        }
        Value::Bind(key, value) => {
            ensure_comparable(key)?;
            ensure_comparable(value)
        }
        Value::Some_(inner) | Value::Ok_(inner) => ensure_comparable(inner),
        Value::Lambda(_, _, _) => Err(EvalError::TypeError(
            "function values are not comparable".to_string(),
        )),
    }
}

pub(crate) fn values_equal(a: &Value, b: &Value) -> bool {
    a == b
}

pub fn eval_expr(expr: &Expr, env: &Env) -> Result<Value, EvalError> {
    match expr {
        Expr::Null => Ok(Value::Null),
        Expr::Bool(b) => Ok(Value::Bool(*b)),
        Expr::Num(n) => Ok(Value::Num(n.clone())),
        Expr::Str(s) => Ok(Value::Str(s.clone())),
        Expr::Bytes(bytes) => Ok(Value::Bytes(bytes.clone())),
        Expr::Placeholder => Ok(env.get("_").cloned().unwrap_or_else(|| {
            Value::Fail_(
                "t_sda_unbound_placeholder".to_string(),
                "unbound placeholder".to_string(),
            )
        })),
        Expr::Ident(name) => env
            .get(name)
            .cloned()
            .map_or_else(|| Ok(unbound_name_value()), Ok),
        Expr::Seq(items) => {
            let values: Result<Vec<Value>, EvalError> =
                items.iter().map(|item| eval_expr(item, env)).collect();
            Ok(Value::Seq(values?))
        }
        Expr::Set(items) => {
            let mut values = Vec::new();
            for item in items {
                let value = eval_expr(item, env)?;
                if ensure_comparable(&value).is_err() {
                    return Ok(wrong_shape_value());
                }
                if !values.iter().any(|existing| values_equal(existing, &value)) {
                    values.push(value);
                }
            }
            Ok(Value::Set(values))
        }
        Expr::Bag(items) => {
            let values: Result<Vec<Value>, EvalError> =
                items.iter().map(|item| eval_expr(item, env)).collect();
            Ok(Value::Bag(values?))
        }
        Expr::Map(entries) => {
            let mut result = Vec::new();
            for (k, v) in entries {
                result.push((k.clone(), eval_expr(v, env)?));
            }
            Ok(Value::Map(result))
        }
        Expr::Prod(fields) => {
            let mut result = Vec::new();
            for (k, v) in fields {
                result.push((k.clone(), eval_expr(v, env)?));
            }
            Ok(Value::Prod(result))
        }
        Expr::BagKV(entries) => {
            let mut result = Vec::new();
            for (k, v) in entries {
                result.push((Value::Str(k.clone()), eval_expr(v, env)?));
            }
            Ok(Value::BagKV(result))
        }
        Expr::Some_(inner) => Ok(Value::Some_(Box::new(eval_expr(inner, env)?))),
        Expr::None_ => Ok(Value::None_),
        Expr::Ok_(inner) => Ok(Value::Ok_(Box::new(eval_expr(inner, env)?))),
        Expr::Fail_(code_expr, msg_expr) => {
            let code_value = eval_expr(code_expr, env)?;
            let msg_value = eval_expr(msg_expr, env)?;
            let code = match code_value {
                Value::Str(s) => s,
                other => format!("{other:?}"),
            };
            let msg = match msg_value {
                Value::Str(s) => s,
                other => format!("{other:?}"),
            };
            Ok(Value::Fail_(code, msg))
        }
        Expr::Lambda(param, body) => Ok(Value::Lambda(
            param.clone(),
            body.clone(),
            Box::new(env.clone()),
        )),
        Expr::Call(func_expr, args) => {
            // ENR1 Match(l, R, kL, kR) is a special form: kL/kR close over l and r
            // and must not be evaluated eagerly (see crates/enr-core/ENR1.md §01).
            if let Expr::Ident(name) = func_expr.as_ref() {
                if name == "Match" {
                    return eval_enr_match(args, env);
                }
            }

            let arg_vals: Result<Vec<Value>, EvalError> =
                args.iter().map(|arg| eval_expr(arg, env)).collect();
            let arg_vals = arg_vals?;

            if let Expr::Ident(name) = func_expr.as_ref() {
                if let Some(result) = stdlib::call_stdlib(name, arg_vals.clone()) {
                    return match result {
                        Err(EvalError::ArityMismatch { .. }) => Ok(arity_mismatch_value()),
                        other => other,
                    };
                }
                let func = if let Some(func) = env.get(name).cloned() {
                    func
                } else {
                    return Ok(unbound_name_value());
                };
                return apply_lambda(func, arg_vals);
            }

            let func = eval_expr(func_expr, env)?;
            apply_lambda(func, arg_vals)
        }
        Expr::Enrich(fields) => eval_enr_enrich(fields, env),
        Expr::Pipe(lhs, rhs) => {
            let lhs_value = eval_expr(lhs, env)?;
            let mut child_env = env.clone();
            child_env.insert("_".to_string(), lhs_value);
            eval_expr(rhs, &child_env)
        }
        Expr::Select(obj_expr, field, mode) => {
            let obj = eval_expr(obj_expr, env)?;
            eval_select(obj, field, mode)
        }
        Expr::UnOp(op, expr) => {
            let value = eval_expr(expr, env)?;
            match op {
                UnOpKind::Neg => match value {
                    Value::Num(n) => Ok(Value::Num(n.neg())),
                    _ => Ok(wrong_shape_value()),
                },
                UnOpKind::Not => match value {
                    Value::Bool(b) => Ok(Value::Bool(!b)),
                    _ => Ok(wrong_shape_value()),
                },
            }
        }
        Expr::BinOp(op, lhs_expr, rhs_expr) => {
            let lhs = eval_expr(lhs_expr, env)?;
            let rhs = eval_expr(rhs_expr, env)?;
            eval_binop(op, lhs, rhs)
        }
        Expr::Comprehension {
            yield_expr,
            binding,
            collection,
            pred,
        } => {
            enum Carrier {
                Seq,
                Set,
                Bag,
            }

            let coll_val = eval_expr(collection, env)?;
            let (items, carrier) = match coll_val {
                Value::Seq(items) => (items, Carrier::Seq),
                Value::Set(items) => (items, Carrier::Set),
                Value::Bag(items) => (items, Carrier::Bag),
                Value::BagKV(entries) => (
                    entries
                        .into_iter()
                        .map(|(key, value)| Value::Bind(Box::new(key), Box::new(value)))
                        .collect(),
                    Carrier::Bag,
                ),
                _ => return Ok(wrong_shape_value()),
            };

            let mut results = Vec::new();
            for item in items {
                let mut child_env = env.clone();
                child_env.insert(binding.clone(), item.clone());

                if let Some(pred_expr) = pred {
                    let pred_val = eval_expr(pred_expr, &child_env)?;
                    match pred_val {
                        Value::Bool(false) => continue,
                        Value::Bool(true) => {}
                        _ => return Ok(wrong_shape_value()),
                    }
                }

                let result = if let Some(yield_expr) = yield_expr {
                    eval_expr(yield_expr, &child_env)?
                } else {
                    item
                };
                results.push(result);
            }

            match carrier {
                Carrier::Seq => Ok(Value::Seq(results)),
                Carrier::Bag => Ok(Value::Bag(results)),
                Carrier::Set => {
                    let mut dedup = Vec::new();
                    for value in results {
                        if ensure_comparable(&value).is_err() {
                            return Ok(wrong_shape_value());
                        }
                        if !dedup.iter().any(|existing| values_equal(existing, &value)) {
                            dedup.push(value);
                        }
                    }
                    Ok(Value::Set(dedup))
                }
            }
        }
    }
}

/// `Match(l, R, kL, kR) = { r ∈ R | kR(r) = kL(l) }` as Bag (ENR1 primitive).
///
/// `kL` and `kR` are expressions evaluated with `l` bound to the left value and
/// `r` bound to each right-side candidate (not ordinary eager call args).
fn eval_enr_match(args: &[Expr], env: &Env) -> Result<Value, EvalError> {
    if args.len() != 4 {
        return Ok(arity_mismatch_value());
    }
    let left = eval_expr(&args[0], env)?;
    let right_coll = eval_expr(&args[1], env)?;
    let items = match right_coll {
        Value::Seq(items) | Value::Bag(items) | Value::Set(items) => items,
        _ => return Ok(enr_wrong_shape_value()),
    };
    let mut matched = Vec::new();
    for r in items {
        let mut child = env.clone();
        child.insert("l".to_string(), left.clone());
        child.insert("r".to_string(), r.clone());
        let k_l = eval_expr(&args[2], &child)?;
        let k_r = eval_expr(&args[3], &child)?;
        // ENR1 §09: invalid key type → t_enr_invalid_key (not a silent non-match).
        if ensure_comparable(&k_l).is_err() || ensure_comparable(&k_r).is_err() {
            return Ok(Value::Fail_(
                "t_enr_invalid_key".to_string(),
                "invalid key".to_string(),
            ));
        }
        if values_equal(&k_l, &k_r) {
            matched.push(r);
        }
    }
    Ok(Value::Bag(matched))
}

/// `enrich { field: expr, ... }` over pipe `_`: attach evaluated fields to each left row.
///
/// Each left row is bound as `l` while field expressions evaluate. Result carrier
/// follows the left (Seq/Bag/Set). Attach uses Map keys (JSON-friendly) via `+`.
fn eval_enr_enrich(fields: &[(String, Expr)], env: &Env) -> Result<Value, EvalError> {
    let left = match env.get("_") {
        Some(v) => v.clone(),
        None => {
            return Ok(Value::Fail_(
                "t_sda_unbound_placeholder".to_string(),
                "unbound placeholder".to_string(),
            ))
        }
    };
    enum Carrier {
        Seq,
        Set,
        Bag,
    }
    let (items, carrier) = match left {
        Value::Seq(items) => (items, Carrier::Seq),
        Value::Set(items) => (items, Carrier::Set),
        Value::Bag(items) => (items, Carrier::Bag),
        _ => return Ok(enr_wrong_shape_value()),
    };
    let mut results = Vec::new();
    for item in items {
        let mut child = env.clone();
        child.insert("l".to_string(), item.clone());
        let mut attach = Vec::new();
        for (name, expr) in fields {
            attach.push((name.clone(), eval_expr(expr, &child)?));
        }
        let attached = match item {
            Value::Map(entries) => merge_record_fields(entries, attach, RecordKind::Map),
            Value::Prod(entries) => merge_record_fields(entries, attach, RecordKind::Prod),
            _ => enr_wrong_shape_value(),
        };
        results.push(attached);
    }
    match carrier {
        Carrier::Seq => Ok(Value::Seq(results)),
        Carrier::Bag => Ok(Value::Bag(results)),
        Carrier::Set => {
            let mut dedup = Vec::new();
            for value in results {
                if ensure_comparable(&value).is_err() {
                    return Ok(wrong_shape_value());
                }
                if !dedup.iter().any(|existing| values_equal(existing, &value)) {
                    dedup.push(value);
                }
            }
            Ok(Value::Set(dedup))
        }
    }
}

fn enr_wrong_shape_value() -> Value {
    Value::Fail_("t_enr_wrong_shape".to_string(), "wrong shape".to_string())
}

fn eval_select(obj: Value, field: &str, mode: &SelectMode) -> Result<Value, EvalError> {
    match &obj {
        Value::Map(entries) => {
            let found = entries
                .iter()
                .find(|(k, _)| k == field)
                .map(|(_, v)| v.clone());
            match mode {
                SelectMode::Plain => Ok(Value::Fail_(
                    "t_sda_wrong_shape".to_string(),
                    "wrong shape".to_string(),
                )),
                SelectMode::Optional => Ok(found
                    .map(|v| Value::Some_(Box::new(v)))
                    .unwrap_or(Value::None_)),
                SelectMode::Required => {
                    Ok(found.map(|v| Value::Ok_(Box::new(v))).unwrap_or_else(|| {
                        Value::Fail_("t_sda_missing_key".to_string(), "missing key".to_string())
                    }))
                }
            }
        }
        Value::Prod(fields) => {
            let found = fields
                .iter()
                .find(|(k, _)| k == field)
                .map(|(_, v)| v.clone());
            match mode {
                SelectMode::Plain => Ok(found.unwrap_or_else(|| {
                    Value::Fail_(
                        "t_sda_unknown_field".to_string(),
                        "unknown field".to_string(),
                    )
                })),
                SelectMode::Optional | SelectMode::Required => Ok(Value::Fail_(
                    "t_sda_wrong_shape".to_string(),
                    "wrong shape".to_string(),
                )),
            }
        }
        Value::Bind(key, value) => {
            let found = match field {
                "key" => Some((**key).clone()),
                "val" => Some((**value).clone()),
                _ => None,
            };
            match mode {
                SelectMode::Plain => Ok(found.unwrap_or(Value::Null)),
                SelectMode::Optional => Ok(found
                    .map(|v| Value::Some_(Box::new(v)))
                    .unwrap_or(Value::None_)),
                SelectMode::Required => {
                    Ok(found.map(|v| Value::Ok_(Box::new(v))).unwrap_or_else(|| {
                        Value::Fail_("t_sda_missing_key".to_string(), "missing key".to_string())
                    }))
                }
            }
        }
        Value::BagKV(entries) => {
            let matches: Vec<_> = entries
                .iter()
                .filter(|(k, _)| matches!(k, Value::Str(s) if s == field))
                .collect();
            match mode {
                SelectMode::Plain => Ok(Value::Fail_(
                    "t_sda_wrong_shape".to_string(),
                    "wrong shape".to_string(),
                )),
                SelectMode::Optional => match matches.len() {
                    0 => Ok(Value::None_),
                    1 => Ok(Value::Some_(Box::new(matches[0].1.clone()))),
                    _ => Ok(Value::None_),
                },
                SelectMode::Required => match matches.len() {
                    0 => Ok(Value::Fail_(
                        "t_sda_missing_key".to_string(),
                        "missing key".to_string(),
                    )),
                    1 => Ok(Value::Ok_(Box::new(matches[0].1.clone()))),
                    _ => Ok(Value::Fail_(
                        "t_sda_duplicate_key".to_string(),
                        "duplicate key".to_string(),
                    )),
                },
            }
        }
        _ => match mode {
            SelectMode::Optional => Ok(Value::Fail_(
                "t_sda_wrong_shape".to_string(),
                "wrong shape".to_string(),
            )),
            SelectMode::Required => Ok(Value::Fail_(
                "t_sda_wrong_shape".to_string(),
                "wrong shape".to_string(),
            )),
            SelectMode::Plain => Ok(Value::Fail_(
                "t_sda_wrong_shape".to_string(),
                "wrong shape".to_string(),
            )),
        },
    }
}

fn eval_binop(op: &BinOpKind, lhs: Value, rhs: Value) -> Result<Value, EvalError> {
    match op {
        BinOpKind::Add => match (lhs, rhs) {
            (Value::Num(a), Value::Num(b)) => Ok(Value::Num(a.add(&b))),
            // ENR1 attach / mergeFail via `+` (Prod or Map; field collision → t_enr_field_collision).
            (Value::Prod(left), Value::Prod(right)) => {
                Ok(merge_record_fields(left, right, RecordKind::Prod))
            }
            (Value::Map(left), Value::Map(right)) => {
                Ok(merge_record_fields(left, right, RecordKind::Map))
            }
            (Value::Prod(left), Value::Map(right)) => {
                Ok(merge_record_fields(left, right, RecordKind::Prod))
            }
            (Value::Map(left), Value::Prod(right)) => {
                Ok(merge_record_fields(left, right, RecordKind::Map))
            }
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Sub => match (lhs, rhs) {
            (Value::Num(a), Value::Num(b)) => Ok(Value::Num(a.sub(&b))),
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Mul => match (lhs, rhs) {
            (Value::Num(a), Value::Num(b)) => Ok(Value::Num(a.mul(&b))),
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Div => match (lhs, rhs) {
            (Value::Num(a), Value::Num(b)) => {
                if b.is_zero() {
                    Ok(div_by_zero_value())
                } else {
                    Ok(Value::Num(a.div(&b)))
                }
            }
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Concat => match (lhs, rhs) {
            (Value::Str(a), Value::Str(b)) => Ok(Value::Str(a + &b)),
            (Value::Seq(mut a), Value::Seq(b)) => {
                a.extend(b);
                Ok(Value::Seq(a))
            }
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Eq => {
            if ensure_comparable(&lhs).is_err() || ensure_comparable(&rhs).is_err() {
                return Ok(wrong_shape_value());
            }
            Ok(Value::Bool(values_equal(&lhs, &rhs)))
        }
        BinOpKind::Neq => {
            if ensure_comparable(&lhs).is_err() || ensure_comparable(&rhs).is_err() {
                return Ok(wrong_shape_value());
            }
            Ok(Value::Bool(!values_equal(&lhs, &rhs)))
        }
        BinOpKind::Lt => match (lhs, rhs) {
            (Value::Num(a), Value::Num(b)) => Ok(Value::Bool(a < b)),
            (Value::Str(a), Value::Str(b)) => Ok(Value::Bool(a < b)),
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Le => match (lhs, rhs) {
            (Value::Num(a), Value::Num(b)) => Ok(Value::Bool(a <= b)),
            (Value::Str(a), Value::Str(b)) => Ok(Value::Bool(a <= b)),
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Gt => match (lhs, rhs) {
            (Value::Num(a), Value::Num(b)) => Ok(Value::Bool(a > b)),
            (Value::Str(a), Value::Str(b)) => Ok(Value::Bool(a > b)),
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Ge => match (lhs, rhs) {
            (Value::Num(a), Value::Num(b)) => Ok(Value::Bool(a >= b)),
            (Value::Str(a), Value::Str(b)) => Ok(Value::Bool(a >= b)),
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::And => match (lhs, rhs) {
            (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(a && b)),
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Or => match (lhs, rhs) {
            (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(a || b)),
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Union => match (lhs, rhs) {
            (Value::Set(mut a), Value::Set(b)) => {
                for item in b {
                    if !a.iter().any(|existing| values_equal(existing, &item)) {
                        a.push(item);
                    }
                }
                Ok(Value::Set(a))
            }
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Inter => match (lhs, rhs) {
            (Value::Set(a), Value::Set(b)) => {
                let result = a
                    .into_iter()
                    .filter(|x| b.iter().any(|y| values_equal(x, y)))
                    .collect();
                Ok(Value::Set(result))
            }
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::Diff => match (lhs, rhs) {
            (Value::Set(a), Value::Set(b)) => {
                let result = a
                    .into_iter()
                    .filter(|x| !b.iter().any(|y| values_equal(x, y)))
                    .collect();
                Ok(Value::Set(result))
            }
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::BUnion => match (lhs, rhs) {
            (Value::Bag(mut a), Value::Bag(b)) => {
                a.extend(b);
                Ok(Value::Bag(a))
            }
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::BDiff => match (lhs, rhs) {
            (Value::Bag(a), Value::Bag(b)) => {
                let mut remaining = b.clone();
                let result = a
                    .into_iter()
                    .filter(|x| {
                        if let Some(idx) = remaining.iter().position(|y| values_equal(x, y)) {
                            remaining.remove(idx);
                            false
                        } else {
                            true
                        }
                    })
                    .collect();
                Ok(Value::Bag(result))
            }
            _ => Ok(wrong_shape_value()),
        },
        BinOpKind::In => match rhs {
            Value::Seq(items) => {
                if ensure_comparable(&lhs).is_err() {
                    return Ok(wrong_shape_value());
                }
                for item in &items {
                    if ensure_comparable(item).is_err() {
                        return Ok(wrong_shape_value());
                    }
                }
                Ok(Value::Bool(items.iter().any(|x| values_equal(x, &lhs))))
            }
            Value::Set(items) => {
                if ensure_comparable(&lhs).is_err() {
                    return Ok(wrong_shape_value());
                }
                for item in &items {
                    if ensure_comparable(item).is_err() {
                        return Ok(wrong_shape_value());
                    }
                }
                Ok(Value::Bool(items.iter().any(|x| values_equal(x, &lhs))))
            }
            Value::Bag(items) => {
                if ensure_comparable(&lhs).is_err() {
                    return Ok(wrong_shape_value());
                }
                for item in &items {
                    if ensure_comparable(item).is_err() {
                        return Ok(wrong_shape_value());
                    }
                }
                Ok(Value::Bool(items.iter().any(|x| values_equal(x, &lhs))))
            }
            Value::Map(entries) => {
                if let Value::Str(key) = &lhs {
                    Ok(Value::Bool(entries.iter().any(|(k, _)| k == key)))
                } else {
                    Ok(wrong_shape_value())
                }
            }
            Value::Prod(fields) => {
                if let Value::Str(key) = &lhs {
                    Ok(Value::Bool(fields.iter().any(|(k, _)| k == key)))
                } else {
                    Ok(wrong_shape_value())
                }
            }
            _ => Ok(wrong_shape_value()),
        },
    }
}

enum RecordKind {
    Prod,
    Map,
}

/// ENR1 mergeFail / attach: combine field maps; collide → `t_enr_field_collision`.
fn merge_record_fields(
    left: Vec<(String, Value)>,
    right: Vec<(String, Value)>,
    kind: RecordKind,
) -> Value {
    let mut out = left;
    for (key, value) in right {
        if out.iter().any(|(existing, _)| existing == &key) {
            return fail_value("t_enr_field_collision", "field collision");
        }
        out.push((key, value));
    }
    match kind {
        RecordKind::Prod => Value::Prod(out),
        RecordKind::Map => Value::Map(out),
    }
}

pub(crate) fn apply_lambda(func: Value, args: Vec<Value>) -> Result<Value, EvalError> {
    match func {
        Value::Lambda(param, body, captured_env) => {
            if args.len() != 1 {
                return Ok(arity_mismatch_value());
            }
            let mut new_env = *captured_env;
            new_env.insert(param, args.into_iter().next().unwrap());
            eval_expr(&body, &new_env)
        }
        _ => Ok(not_callable_value()),
    }
}

pub fn eval_program(program: &Program, env: &mut Env) -> Result<Option<Value>, EvalError> {
    let mut last = None;
    for stmt in &program.stmts {
        match stmt {
            Stmt::Let(name, expr) => {
                let value = eval_expr(expr, env)?;
                env.insert(name.clone(), value);
                last = None;
            }
            Stmt::Expr(expr) => {
                last = Some(eval_expr(expr, env)?);
            }
            // ENR1 source declarations are pure semantic annotations; data comes from host binds.
            Stmt::Source { .. } => {
                last = None;
            }
        }
    }
    Ok(last)
}
