use crate::eval::{apply_lambda, ensure_comparable, EvalError};
use crate::number::ExactNum;
use crate::Value;

pub fn call_stdlib(name: &str, args: Vec<Value>) -> Option<Result<Value, EvalError>> {
    match name {
        "typeOf" => Some(stdlib_type_of(args)),
        "keys" => Some(stdlib_keys(args)),
        "values" => Some(stdlib_values(args)),
        "count" => Some(stdlib_count(args)),
        "normalizeUnique" => Some(stdlib_normalize_unique(args)),
        // normalizeFirst / normalizeLast are not core SDA (§7.2 / §14.1).
        "Bind" => Some(stdlib_bind(args)),
        "asBagKV" => Some(stdlib_as_bag_kv(args)),
        "mapOpt" => Some(stdlib_map_opt(args)),
        "bindOpt" => Some(stdlib_bind_opt(args)),
        "orElseOpt" => Some(stdlib_or_else_opt(args)),
        "mapRes" => Some(stdlib_map_res(args)),
        "bindRes" => Some(stdlib_bind_res(args)),
        "orElseRes" => Some(stdlib_or_else_res(args)),
        // Filter/query host helpers (DEF-028). Pure; additive to standalone surface.
        "getPath" => Some(stdlib_get_path(args)),
        "startsWith" => Some(stdlib_starts_with(args)),
        "strContains" => Some(stdlib_str_contains(args)),
        // ENR1 kernel (match-bag cardinality + combine). Same Program::parse compile path as SDA.
        // Spec: crates/enr-core/ENR1.md. Tags t_enr_*. ENR2 not implemented.
        "one?" | "oneOpt" => Some(stdlib_enr_one_opt(args)),
        "one!" | "oneReq" => Some(stdlib_enr_one_req(args)),
        "only" => Some(stdlib_enr_only(args)),
        "first" => Some(stdlib_enr_first(args)),
        "last" => Some(stdlib_enr_last(args)),
        // merge / mergeFail: collision fails. mergeLeft / mergeRight: explicit policies.
        "merge" | "mergeFail" => Some(stdlib_enr_merge(args, MergePolicy::Fail)),
        "mergeLeft" => Some(stdlib_enr_merge(args, MergePolicy::Left)),
        "mergeRight" => Some(stdlib_enr_merge(args, MergePolicy::Right)),
        "asBag" | "matchBag" => Some(stdlib_as_bag(args)),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum MergePolicy {
    Fail,
    Left,
    Right,
}

fn enr_fail(code: &str, msg: &str) -> Value {
    Value::Fail_(code.to_string(), msg.to_string())
}

fn enr_wrong_shape() -> Value {
    enr_fail("t_enr_wrong_shape", "wrong shape")
}

/// Collect multiset items for ENR1 cardinality. Bag is the primitive match carrier;
/// Seq is accepted (order-bearing host datasets). Set is accepted without order claim.
fn enr_match_items(value: Value) -> Result<Vec<Value>, Value> {
    match value {
        Value::Bag(items) | Value::Seq(items) | Value::Set(items) => Ok(items),
        _ => Err(enr_wrong_shape()),
    }
}

/// `one?(B)` — 0 → None, 1 → Some(v), >1 → Fail(t_enr_duplicate).
fn stdlib_enr_one_opt(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("one?", &args, 1)?;
    let items = match enr_match_items(args.into_iter().next().unwrap()) {
        Ok(items) => items,
        Err(fail) => return Ok(fail),
    };
    match items.len() {
        0 => Ok(Value::None_),
        1 => Ok(Value::Some_(Box::new(items.into_iter().next().unwrap()))),
        _ => Ok(enr_fail("t_enr_duplicate", "duplicate match")),
    }
}

/// `one!(B)` — 0 → Fail(t_enr_missing), 1 → v, >1 → Fail(t_enr_duplicate).
fn stdlib_enr_one_req(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("one!", &args, 1)?;
    let items = match enr_match_items(args.into_iter().next().unwrap()) {
        Ok(items) => items,
        Err(fail) => return Ok(fail),
    };
    match items.len() {
        0 => Ok(enr_fail("t_enr_missing", "missing match")),
        1 => Ok(items.into_iter().next().unwrap()),
        _ => Ok(enr_fail("t_enr_duplicate", "duplicate match")),
    }
}

/// `only(B)` — exact uniqueness (same outcomes as `one!` for empty/multi).
fn stdlib_enr_only(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("only", &args, 1)?;
    let items = match enr_match_items(args.into_iter().next().unwrap()) {
        Ok(items) => items,
        Err(fail) => return Ok(fail),
    };
    match items.len() {
        1 => Ok(items.into_iter().next().unwrap()),
        0 => Ok(enr_fail("t_enr_missing", "missing match")),
        _ => Ok(enr_fail("t_enr_duplicate", "duplicate match")),
    }
}

/// `first(B)` — ordered policy only (Seq). Bag/Set → t_enr_unordered_policy.
fn stdlib_enr_first(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("first", &args, 1)?;
    match args.into_iter().next().unwrap() {
        Value::Seq(items) => match items.into_iter().next() {
            Some(v) => Ok(Value::Some_(Box::new(v))),
            None => Ok(Value::None_),
        },
        Value::Bag(_) | Value::Set(_) => Ok(enr_fail("t_enr_unordered_policy", "unordered policy")),
        _ => Ok(enr_wrong_shape()),
    }
}

/// `last(B)` — ordered policy only (Seq).
fn stdlib_enr_last(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("last", &args, 1)?;
    match args.into_iter().next().unwrap() {
        Value::Seq(items) => match items.into_iter().next_back() {
            Some(v) => Ok(Value::Some_(Box::new(v))),
            None => Ok(Value::None_),
        },
        Value::Bag(_) | Value::Set(_) => Ok(enr_fail("t_enr_unordered_policy", "unordered policy")),
        _ => Ok(enr_wrong_shape()),
    }
}

/// `merge` / `mergeFail` / `mergeLeft` / `mergeRight` on Prod/Map.
fn stdlib_enr_merge(args: Vec<Value>, policy: MergePolicy) -> Result<Value, EvalError> {
    let name = match policy {
        MergePolicy::Fail => "merge",
        MergePolicy::Left => "mergeLeft",
        MergePolicy::Right => "mergeRight",
    };
    check_arity(name, &args, 2)?;
    let mut iter = args.into_iter();
    let left = iter.next().unwrap();
    let right = iter.next().unwrap();
    Ok(match (left, right) {
        (Value::Prod(l), Value::Prod(r)) => merge_kv(l, r, true, policy),
        (Value::Map(l), Value::Map(r)) => merge_kv(l, r, false, policy),
        (Value::Prod(l), Value::Map(r)) => merge_kv(l, r, true, policy),
        (Value::Map(l), Value::Prod(r)) => merge_kv(l, r, false, policy),
        _ => enr_wrong_shape(),
    })
}

fn merge_kv(
    left: Vec<(String, Value)>,
    right: Vec<(String, Value)>,
    as_prod: bool,
    policy: MergePolicy,
) -> Value {
    let mut out = left;
    for (key, value) in right {
        if let Some(pos) = out.iter().position(|(existing, _)| existing == &key) {
            match policy {
                MergePolicy::Fail => {
                    return enr_fail("t_enr_field_collision", "field collision");
                }
                MergePolicy::Left => {
                    // keep existing left value; drop right
                    let _ = value;
                }
                MergePolicy::Right => {
                    out[pos].1 = value;
                }
            }
        } else {
            out.push((key, value));
        }
    }
    if as_prod {
        Value::Prod(out)
    } else {
        Value::Map(out)
    }
}

/// `asBag` / `matchBag` — force ENR match-bag carrier (Bag) from Seq/Set/Bag.
fn stdlib_as_bag(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("asBag", &args, 1)?;
    match args.into_iter().next().unwrap() {
        Value::Bag(items) => Ok(Value::Bag(items)),
        Value::Seq(items) | Value::Set(items) => Ok(Value::Bag(items)),
        _ => Ok(enr_wrong_shape()),
    }
}

fn wrong_shape() -> Value {
    Value::Fail_("t_sda_wrong_shape".to_string(), "wrong shape".to_string())
}

fn check_arity(_name: &str, args: &[Value], expected: usize) -> Result<(), EvalError> {
    if args.len() != expected {
        Err(EvalError::ArityMismatch {
            expected,
            got: args.len(),
        })
    } else {
        Ok(())
    }
}

fn stdlib_type_of(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("typeOf", &args, 1)?;
    let type_str = match &args[0] {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Num(_) => "num",
        Value::Str(_) => "str",
        Value::Bytes(_) => "bytes",
        Value::Seq(_) => "seq",
        Value::Set(_) => "set",
        Value::Bag(_) => "bag",
        Value::Map(_) => "map",
        Value::Prod(_) => "prod",
        Value::BagKV(_) => "bagkv",
        Value::Bind(_, _) => "bind",
        Value::Some_(_) => "some",
        Value::None_ => "none",
        Value::Ok_(_) => "ok",
        Value::Fail_(_, _) => "fail",
        Value::Lambda(_, _, _) => "fn",
    };
    Ok(Value::Str(type_str.to_string()))
}

fn stdlib_keys(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("keys", &args, 1)?;
    match &args[0] {
        Value::Map(entries) => Ok(Value::Set(
            entries.iter().map(|(k, _)| Value::Str(k.clone())).collect(),
        )),
        _ => Ok(wrong_shape()),
    }
}

fn stdlib_values(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("values", &args, 1)?;
    match &args[0] {
        Value::Map(entries) => {
            let mut sorted = entries.clone();
            sorted.sort_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));
            Ok(Value::Seq(sorted.into_iter().map(|(_, v)| v).collect()))
        }
        _ => Ok(wrong_shape()),
    }
}

fn stdlib_count(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("count", &args, 2)?;
    let mut iter = args.into_iter();
    let needle = iter.next().unwrap();
    let haystack = iter.next().unwrap();
    if ensure_comparable(&needle).is_err() {
        return Ok(wrong_shape());
    }
    let n = match &haystack {
        Value::Bag(items) => {
            for item in items {
                if ensure_comparable(item).is_err() {
                    return Ok(wrong_shape());
                }
            }
            items.iter().filter(|v| *v == &needle).count()
        }
        _ => return Ok(wrong_shape()),
    };
    Ok(Value::Num(ExactNum::from_usize(n)))
}

fn stdlib_normalize_unique(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("normalizeUnique", &args, 1)?;
    match args.into_iter().next().unwrap() {
        Value::BagKV(pairs) => {
            let mut map: Vec<(String, Value)> = Vec::new();
            for (k, v) in pairs {
                let key_str = match value_as_map_key(&k) {
                    Ok(key) => key,
                    Err(_) => {
                        return Ok(Value::Fail_(
                            "t_sda_wrong_shape".to_string(),
                            "wrong shape".to_string(),
                        ))
                    }
                };
                if map.iter().any(|(existing_key, _)| existing_key == &key_str) {
                    return Ok(Value::Fail_(
                        "t_sda_duplicate_key".to_string(),
                        "duplicate key".to_string(),
                    ));
                }
                map.push((key_str, v));
            }
            Ok(Value::Ok_(Box::new(Value::Map(map))))
        }
        _ => Ok(Value::Fail_(
            "t_sda_wrong_shape".to_string(),
            "wrong shape".to_string(),
        )),
    }
}

fn value_as_map_key(v: &Value) -> Result<String, EvalError> {
    match v {
        Value::Str(s) => Ok(s.clone()),
        other => Err(EvalError::TypeError(format!(
            "Map key must be a string, got {other:?}"
        ))),
    }
}

fn stdlib_bind(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("Bind", &args, 2)?;
    let mut iter = args.into_iter();
    let k = iter.next().unwrap();
    let v = iter.next().unwrap();
    Ok(Value::Bind(Box::new(k), Box::new(v)))
}

fn stdlib_as_bag_kv(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("asBagKV", &args, 1)?;
    match args.into_iter().next().unwrap() {
        Value::Bag(items) => {
            let mut pairs = Vec::new();
            for item in items {
                match item {
                    Value::Bind(k, v) => match *k {
                        Value::Str(s) => pairs.push((Value::Str(s), *v)),
                        _ => {
                            return Ok(Value::Fail_(
                                "t_sda_wrong_shape".to_string(),
                                "wrong shape".to_string(),
                            ))
                        }
                    },
                    _ => {
                        return Ok(Value::Fail_(
                            "t_sda_wrong_shape".to_string(),
                            "wrong shape".to_string(),
                        ))
                    }
                }
            }
            Ok(Value::Ok_(Box::new(Value::BagKV(pairs))))
        }
        _ => Ok(Value::Fail_(
            "t_sda_wrong_shape".to_string(),
            "wrong shape".to_string(),
        )),
    }
}

fn stdlib_map_opt(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("mapOpt", &args, 2)?;
    let mut iter = args.into_iter();
    let opt = iter.next().unwrap();
    let f = iter.next().unwrap();
    match opt {
        Value::Some_(inner) => {
            let result = apply_lambda(f, vec![*inner])?;
            Ok(Value::Some_(Box::new(result)))
        }
        Value::None_ => Ok(Value::None_),
        _ => Ok(wrong_shape()),
    }
}

fn stdlib_bind_opt(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("bindOpt", &args, 2)?;
    let mut iter = args.into_iter();
    let opt = iter.next().unwrap();
    let f = iter.next().unwrap();
    match opt {
        Value::Some_(inner) => {
            let result = apply_lambda(f, vec![*inner])?;
            // §11.3: f MUST return Opt; otherwise Fail(t_sda_wrong_shape).
            match result {
                Value::Some_(_) | Value::None_ => Ok(result),
                _ => Ok(wrong_shape()),
            }
        }
        Value::None_ => Ok(Value::None_),
        _ => Ok(wrong_shape()),
    }
}

fn stdlib_or_else_opt(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("orElseOpt", &args, 2)?;
    let mut iter = args.into_iter();
    let opt = iter.next().unwrap();
    let default = iter.next().unwrap();
    match opt {
        Value::Some_(inner) => Ok(Value::Some_(inner)),
        Value::None_ => Ok(default),
        _ => Ok(wrong_shape()),
    }
}

fn stdlib_map_res(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("mapRes", &args, 2)?;
    let mut iter = args.into_iter();
    let res = iter.next().unwrap();
    let f = iter.next().unwrap();
    match res {
        Value::Ok_(inner) => {
            let result = apply_lambda(f, vec![*inner])?;
            Ok(Value::Ok_(Box::new(result)))
        }
        Value::Fail_(c, m) => Ok(Value::Fail_(c, m)),
        _ => Ok(wrong_shape()),
    }
}

fn stdlib_bind_res(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("bindRes", &args, 2)?;
    let mut iter = args.into_iter();
    let res = iter.next().unwrap();
    let f = iter.next().unwrap();
    match res {
        Value::Ok_(inner) => {
            let result = apply_lambda(f, vec![*inner])?;
            // §11.3: f MUST return Res; otherwise Fail(t_sda_wrong_shape).
            match result {
                Value::Ok_(_) | Value::Fail_(_, _) => Ok(result),
                _ => Ok(wrong_shape()),
            }
        }
        Value::Fail_(c, m) => Ok(Value::Fail_(c, m)),
        _ => Ok(wrong_shape()),
    }
}

fn stdlib_or_else_res(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("orElseRes", &args, 2)?;
    let mut iter = args.into_iter();
    let res = iter.next().unwrap();
    let default = iter.next().unwrap();
    match res {
        Value::Ok_(inner) => Ok(Value::Ok_(inner)),
        Value::Fail_(_, _) => Ok(default),
        _ => Ok(wrong_shape()),
    }
}

/// Optional dotted-path walk over nested `Map` values (JSON objects).
///
/// - Missing key or non-`Map` intermediate → `None` (absence; never `Fail`).
/// - Present value, including `Null`, → `Some(value)`.
/// - Path must be `Seq[Str]` (empty path yields `Some(root)`).
///
/// Aligns native filter path resolution (DX_SPEC §7.1 / DEF-028) with SDA
/// absence-vs-`Null` rules: stored `Null` is `Some(Null)`, not `None`.
fn stdlib_get_path(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("getPath", &args, 2)?;
    let mut iter = args.into_iter();
    let mut cur = iter.next().unwrap();
    let path = iter.next().unwrap();
    let segments = match path {
        Value::Seq(items) => items,
        _ => return Ok(wrong_shape()),
    };
    for seg in segments {
        let key = match seg {
            Value::Str(s) => s,
            _ => return Ok(wrong_shape()),
        };
        match cur {
            Value::Map(entries) => match entries.into_iter().find(|(k, _)| k == &key) {
                Some((_, v)) => cur = v,
                None => return Ok(Value::None_),
            },
            // Intermediate non-object: treat as absence (native filter Missing).
            _ => return Ok(Value::None_),
        }
    }
    Ok(Value::Some_(Box::new(cur)))
}

fn stdlib_starts_with(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("startsWith", &args, 2)?;
    let mut iter = args.into_iter();
    let hay = iter.next().unwrap();
    let prefix = iter.next().unwrap();
    match (hay, prefix) {
        (Value::Str(h), Value::Str(p)) => Ok(Value::Bool(h.starts_with(&p))),
        _ => Ok(wrong_shape()),
    }
}

fn stdlib_str_contains(args: Vec<Value>) -> Result<Value, EvalError> {
    check_arity("strContains", &args, 2)?;
    let mut iter = args.into_iter();
    let hay = iter.next().unwrap();
    let needle = iter.next().unwrap();
    match (hay, needle) {
        (Value::Str(h), Value::Str(n)) => Ok(Value::Bool(h.contains(&n))),
        _ => Ok(wrong_shape()),
    }
}
