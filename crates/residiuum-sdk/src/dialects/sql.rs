//! Tiny SQL → portable Filter / QVM mimicry dialect (**RQL-DQ1**).
//!
//! Intentionally incomplete. Compiles a useful `SELECT` / `WHERE` subset into
//! the portable [`Filter`] vocabulary for Query VM execution. Joins, aggregates,
//! subqueries, `ORDER BY`, and functions are out of scope.
//!
//! See [doc/SDA/DIALECTS.md](../../../../../../doc/SDA/DIALECTS.md).

use super::portable::CompiledPortable;
use crate::error::Error;
use crate::filter::{Filter, Pred};
use serde_json::{Number, Value as JsonValue};

/// Compile a SQL-ish string into a portable Filter (+ optional project).
pub(super) fn compile_sql(source: &str) -> Result<CompiledPortable, Error> {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return Err(Error::QueryInvalid("dialect 'sql': empty query".into()));
    }
    let tokens = tokenize(trimmed)?;
    let mut p = Parser::new(tokens);
    let ast = p.parse_query()?;
    p.expect_end()?;
    lower(ast)
}

#[derive(Debug)]
struct SqlQuery {
    /// `None` means `SELECT *` or WHERE-only (predicate path).
    columns: Option<Vec<String>>,
    /// Optional FROM name (ignored; host binds the collection).
    from: Option<String>,
    where_clause: Option<Filter>,
}

fn lower(q: SqlQuery) -> Result<CompiledPortable, Error> {
    let mut notes = vec![
        "SQL dialect is mimicry, not full SQL (see doc/SDA/DIALECTS.md)".into(),
        "RQL-DQ1: compiles to portable Filter → Query VM (not SDA)".into(),
    ];
    if let Some(ref name) = q.from {
        notes.push(format!(
            "FROM {name} ignored at compile time; host selects the collection"
        ));
    }

    let filter = q.where_clause.unwrap_or(Filter::Always);
    if filter_uses_is_null_style_or(&filter) {
        notes.push(
            "IS NULL / IS NOT NULL maps missing OR stored JSON null — not SDA Null≠absence; \
             use pure SDA when that distinction matters (doc/SDA/DIALECTS.md)"
                .into(),
        );
    }

    match q.columns {
        None => Ok(CompiledPortable::new("sql", filter, None, notes)),
        Some(cols) if cols.is_empty() => Err(Error::QueryInvalid(
            "dialect 'sql': SELECT column list must not be empty".into(),
        )),
        Some(cols) => {
            notes.push(format!(
                "SELECT {} projects via Application Core path project",
                cols.join(", ")
            ));
            Ok(CompiledPortable::new("sql", filter, Some(cols), notes))
        }
    }
}

/// Detect SQL `IS NULL` lowering: `Or(Exists(false), Eq(Null))` (and nesting under
/// And/Not). That map collapses SDA absence vs stored null — comfort only.
fn filter_uses_is_null_style_or(f: &Filter) -> bool {
    match f {
        Filter::Or(parts) if parts.len() == 2 => {
            is_null_pair(&parts[0], &parts[1]) || is_null_pair(&parts[1], &parts[0])
        }
        Filter::And(parts) | Filter::Or(parts) => parts.iter().any(filter_uses_is_null_style_or),
        Filter::Not(inner) => filter_uses_is_null_style_or(inner),
        _ => false,
    }
}

fn is_null_pair(a: &Filter, b: &Filter) -> bool {
    match (a, b) {
        (
            Filter::Field {
                path: p1,
                pred: Pred::Exists(false),
            },
            Filter::Field {
                path: p2,
                pred: Pred::Eq(JsonValue::Null),
            },
        ) => p1 == p2,
        _ => false,
    }
}

// --- lexer -------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    String(String),
    Number(String),
    /// `true` / `false` / `null` as keywords land as Ident; numbers separate.
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    LParen,
    RParen,
    Comma,
    Star,
    Dot,
}

fn tokenize(src: &str) -> Result<Vec<Tok>, Error> {
    let mut out = Vec::new();
    let chars: Vec<char> = src.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        // single-quoted string
        if c == '\'' {
            i += 1;
            let mut s = String::new();
            while i < chars.len() {
                match chars[i] {
                    '\'' => {
                        // SQL '' escape
                        if i + 1 < chars.len() && chars[i + 1] == '\'' {
                            s.push('\'');
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    ch => {
                        s.push(ch);
                        i += 1;
                    }
                }
            }
            out.push(Tok::String(s));
            continue;
        }
        // double-quoted identifier or string → treat as string literal
        if c == '"' {
            i += 1;
            let mut s = String::new();
            while i < chars.len() {
                match chars[i] {
                    '"' => {
                        i += 1;
                        break;
                    }
                    '\\' if i + 1 < chars.len() => {
                        s.push(chars[i + 1]);
                        i += 2;
                    }
                    ch => {
                        s.push(ch);
                        i += 1;
                    }
                }
            }
            out.push(Tok::String(s));
            continue;
        }
        // number
        if c.is_ascii_digit() || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
        {
            let start = i;
            if c == '-' {
                i += 1;
            }
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            out.push(Tok::Number(chars[start..i].iter().collect()));
            continue;
        }
        // multi-char ops
        if i + 1 < chars.len() {
            let two: String = chars[i..i + 2].iter().collect();
            match two.as_str() {
                "!=" | "<>" => {
                    out.push(Tok::Ne);
                    i += 2;
                    continue;
                }
                "<=" => {
                    out.push(Tok::Lte);
                    i += 2;
                    continue;
                }
                ">=" => {
                    out.push(Tok::Gte);
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }
        match c {
            '=' => {
                out.push(Tok::Eq);
                i += 1;
            }
            '<' => {
                out.push(Tok::Lt);
                i += 1;
            }
            '>' => {
                out.push(Tok::Gt);
                i += 1;
            }
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            ',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            '*' => {
                out.push(Tok::Star);
                i += 1;
            }
            '.' => {
                out.push(Tok::Dot);
                i += 1;
            }
            ch if ch.is_ascii_alphabetic() || ch == '_' => {
                let start = i;
                i += 1;
                while i < chars.len()
                    && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == '$')
                {
                    i += 1;
                }
                let ident: String = chars[start..i].iter().collect();
                out.push(Tok::Ident(ident));
            }
            ch => {
                return Err(Error::QueryInvalid(format!(
                    "dialect 'sql': unexpected character {ch:?} at index {i}"
                )));
            }
        }
    }
    Ok(out)
}

// --- parser ------------------------------------------------------------------

struct Parser {
    toks: Vec<Tok>,
    i: usize,
}

impl Parser {
    fn new(toks: Vec<Tok>) -> Self {
        Self { toks, i: 0 }
    }

    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.i)
    }

    fn bump(&mut self) -> Option<Tok> {
        if self.i < self.toks.len() {
            let t = self.toks[self.i].clone();
            self.i += 1;
            Some(t)
        } else {
            None
        }
    }

    fn expect_end(&self) -> Result<(), Error> {
        if self.i < self.toks.len() {
            Err(Error::QueryInvalid(format!(
                "dialect 'sql': unexpected trailing token {:?}",
                self.toks[self.i]
            )))
        } else {
            Ok(())
        }
    }

    fn parse_query(&mut self) -> Result<SqlQuery, Error> {
        // Forms:
        //   SELECT * [FROM name] [WHERE expr]
        //   SELECT a, b [FROM name] [WHERE expr]
        //   WHERE expr
        //   expr   (treated as WHERE body)
        if self.keyword("SELECT") {
            self.bump();
            let columns = if matches!(self.peek(), Some(Tok::Star)) {
                self.bump();
                None
            } else {
                Some(self.parse_column_list()?)
            };
            let from = if self.keyword("FROM") {
                self.bump();
                Some(self.parse_ident_path()?)
            } else {
                None
            };
            let where_clause = if self.keyword("WHERE") {
                self.bump();
                Some(self.parse_or()?)
            } else {
                None
            };
            return Ok(SqlQuery {
                columns,
                from,
                where_clause,
            });
        }
        if self.keyword("WHERE") {
            self.bump();
            return Ok(SqlQuery {
                columns: None,
                from: None,
                where_clause: Some(self.parse_or()?),
            });
        }
        // Bare predicate expression.
        Ok(SqlQuery {
            columns: None,
            from: None,
            where_clause: Some(self.parse_or()?),
        })
    }

    fn parse_column_list(&mut self) -> Result<Vec<String>, Error> {
        let mut cols = vec![self.parse_ident_path()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.bump();
            cols.push(self.parse_ident_path()?);
        }
        Ok(cols)
    }

    fn keyword(&self, kw: &str) -> bool {
        match self.peek() {
            Some(Tok::Ident(s)) => s.eq_ignore_ascii_case(kw),
            _ => false,
        }
    }

    fn parse_ident_path(&mut self) -> Result<String, Error> {
        let mut parts = Vec::new();
        match self.bump() {
            Some(Tok::Ident(s)) => parts.push(s),
            other => {
                return Err(Error::QueryInvalid(format!(
                    "dialect 'sql': expected identifier, got {other:?}"
                )));
            }
        }
        while matches!(self.peek(), Some(Tok::Dot)) {
            self.bump();
            match self.bump() {
                Some(Tok::Ident(s)) => parts.push(s),
                other => {
                    return Err(Error::QueryInvalid(format!(
                        "dialect 'sql': expected identifier after '.', got {other:?}"
                    )));
                }
            }
        }
        Ok(parts.join("."))
    }

    fn parse_or(&mut self) -> Result<Filter, Error> {
        let mut left = self.parse_and()?;
        while self.keyword("OR") {
            self.bump();
            let right = self.parse_and()?;
            left = Filter::or(vec![left, right]);
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Filter, Error> {
        let mut left = self.parse_not()?;
        while self.keyword("AND") {
            self.bump();
            let right = self.parse_not()?;
            left = Filter::and(vec![left, right]);
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Filter, Error> {
        if self.keyword("NOT") {
            self.bump();
            return Ok(Filter::not(self.parse_not()?));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Filter, Error> {
        if matches!(self.peek(), Some(Tok::LParen)) {
            self.bump();
            let inner = self.parse_or()?;
            match self.bump() {
                Some(Tok::RParen) => {}
                other => {
                    return Err(Error::QueryInvalid(format!(
                        "dialect 'sql': expected ')', got {other:?}"
                    )));
                }
            }
            return Ok(inner);
        }

        // field path
        let path = self.parse_ident_path()?;

        // IS [NOT] NULL
        if self.keyword("IS") {
            self.bump();
            let neg = if self.keyword("NOT") {
                self.bump();
                true
            } else {
                false
            };
            if !self.keyword("NULL") {
                return Err(Error::QueryInvalid(
                    "dialect 'sql': expected NULL after IS".into(),
                ));
            }
            self.bump();
            // SQL NULL ≈ missing or JSON null is ambiguous; map IS NULL to
            // exists(false) OR eq(null) is wrong. Prefer: field equals JSON null
            // is Pred::Eq(Null); missing is separate. Portable choice: Eq(Null)
            // does not match missing; Exists(false) is absence.
            // Mimicry: IS NULL → exists(false) OR eq(null) via Or — still not
            // SQL 3VL. Document and use: equals null OR not exists.
            let f = Filter::or(vec![
                Filter::Field {
                    path: path.clone(),
                    pred: Pred::Exists(false),
                },
                Filter::Field {
                    path,
                    pred: Pred::Eq(JsonValue::Null),
                },
            ]);
            return Ok(if neg { Filter::not(f) } else { f });
        }

        // IN ( ... )
        if self.keyword("IN") {
            self.bump();
            match self.bump() {
                Some(Tok::LParen) => {}
                other => {
                    return Err(Error::QueryInvalid(format!(
                        "dialect 'sql': expected '(' after IN, got {other:?}"
                    )));
                }
            }
            let mut vals = Vec::new();
            loop {
                vals.push(self.parse_value()?);
                match self.peek() {
                    Some(Tok::Comma) => {
                        self.bump();
                    }
                    Some(Tok::RParen) => {
                        self.bump();
                        break;
                    }
                    other => {
                        return Err(Error::QueryInvalid(format!(
                            "dialect 'sql': expected ',' or ')' in IN list, got {other:?}"
                        )));
                    }
                }
            }
            return Ok(Filter::Field {
                path,
                pred: Pred::In(vals),
            });
        }

        // comparison
        let pred = match self.bump() {
            Some(Tok::Eq) => Pred::Eq(self.parse_value()?),
            Some(Tok::Ne) => Pred::Ne(self.parse_value()?),
            Some(Tok::Lt) => Pred::Lt(self.parse_value()?),
            Some(Tok::Lte) => Pred::Lte(self.parse_value()?),
            Some(Tok::Gt) => Pred::Gt(self.parse_value()?),
            Some(Tok::Gte) => Pred::Gte(self.parse_value()?),
            other => {
                return Err(Error::QueryInvalid(format!(
                    "dialect 'sql': expected comparison after field {path:?}, got {other:?}"
                )));
            }
        };
        Ok(Filter::Field { path, pred })
    }

    fn parse_value(&mut self) -> Result<JsonValue, Error> {
        match self.bump() {
            Some(Tok::String(s)) => Ok(JsonValue::String(s)),
            Some(Tok::Number(n)) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(JsonValue::Number(i.into()))
                } else if let Ok(f) = n.parse::<f64>() {
                    Number::from_f64(f).map(JsonValue::Number).ok_or_else(|| {
                        Error::QueryInvalid(format!("dialect 'sql': invalid number literal {n}"))
                    })
                } else {
                    Err(Error::QueryInvalid(format!(
                        "dialect 'sql': invalid number literal {n}"
                    )))
                }
            }
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("true") => Ok(JsonValue::Bool(true)),
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("false") => Ok(JsonValue::Bool(false)),
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("null") => Ok(JsonValue::Null),
            other => Err(Error::QueryInvalid(format!(
                "dialect 'sql': expected value literal, got {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bare_predicate() {
        let c = compile_sql("status = 'active'").unwrap();
        assert!(c.project.is_none());
        assert!(c.filter.matches(&json!({"status": "active"})));
        assert!(!c.filter.matches(&json!({"status": "idle"})));
        let pred = c.filter.to_predicate().unwrap();
        let params = std::collections::BTreeMap::new();
        assert!(pred.eval(&json!({"status": "active"}), &params).unwrap());
    }

    #[test]
    fn in_list() {
        let c = compile_sql("country IN ('TH', 'SG')").unwrap();
        assert!(c.filter.matches(&json!({"country": "TH"})));
        assert!(!c.filter.matches(&json!({"country": "US"})));
    }

    #[test]
    fn is_null_missing() {
        let c = compile_sql("nickname IS NULL").unwrap();
        assert!(
            c.notes
                .iter()
                .any(|n| n.contains("Null") || n.contains("absence")),
            "expected null-vs-absence note, got {:?}",
            c.notes
        );
        assert!(c.filter.matches(&json!({})));
        assert!(c.filter.matches(&json!({"nickname": null})));
        assert!(!c.filter.matches(&json!({"nickname": "x"})));
    }
}
