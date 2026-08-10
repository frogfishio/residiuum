//! SQL-ish+ → Application Core RQL (`residiuum-sql-plus-to-rql-v1`).
//!
//! Pure emit / refuse compiler (no IO, no catalog lookup). Normative design:
//! `doc/todo/rql/SQL_TO_RQL_SPEC.md`. This module is a **labor scaffold** for
//! Phase 2 expressiveness de-risk — not a claim that SQL-ish+ is product-ready.
//!
//! Rule: emit equivalent Core RQL, or refuse. Never guess.

use crate::error::Error;
use crate::plan_v1::CollectionBindings;
use crate::rql_app_core::{compile_app_core, CompiledAppCore};

/// Compiler profile label (SQL_TO_RQL_SPEC).
pub const SQL_PLUS_PROFILE: &str = "residiuum-sql-plus-to-rql-v1";

/// Product dialect spelling.
pub const SQL_PLUS_DIALECT: &str = "sql+";

/// Portable alias (shells / URLs).
pub const SQL_PLUS_ALIAS: &str = "sql-plus";

/// Diagnostic prefix for construct refusals.
pub const DIAG_SQL_RQL_CONSTRUCT_UNSUPPORTED: &str = "sql_rql_construct_unsupported";

/// Diagnostic prefix for statement refusals.
pub const DIAG_SQL_RQL_STATEMENT_UNSUPPORTED: &str = "sql_rql_statement_unsupported";

/// Diagnostic prefix for parse failures.
pub const DIAG_SQL_RQL_PARSE_ERROR: &str = "sql_rql_parse_error";

/// Successful emit result (may still carry honesty notes).
#[derive(Debug, Clone, PartialEq)]
pub struct SqlToRqlEmit {
    /// Emitted Application Core RQL source.
    pub rql: String,
    /// Optional compile of that RQL against provided bindings.
    pub compiled: Option<CompiledAppCore>,
    /// Mapping honesty notes (SQL Null↔missing collapse, etc.).
    pub notes: Vec<String>,
}

/// Outcome of [`compile_sql_to_rql`].
#[derive(Debug, Clone, PartialEq)]
pub enum SqlToRqlResult {
    /// Emitted Core RQL (and optional plan).
    Emit(SqlToRqlEmit),
    /// Explicit refuse (safety success).
    Refuse {
        /// Structured diagnostic code.
        diagnostic: String,
        /// Human detail.
        detail: String,
    },
}

impl SqlToRqlResult {
    /// True when the compiler emitted RQL.
    pub fn is_emit(&self) -> bool {
        matches!(self, Self::Emit(_))
    }

    /// True when the compiler refused (fail-closed).
    pub fn is_refuse(&self) -> bool {
        matches!(self, Self::Refuse { .. })
    }
}

/// Compile SQL-ish+ source to Application Core RQL, or refuse.
///
/// Pure after `bindings` are supplied. When `bindings` is `Some`, a successful
/// emit also runs [`compile_app_core`] so plan hashes lock against Core.
pub fn compile_sql_to_rql(
    sql: &str,
    bindings: Option<&CollectionBindings>,
) -> Result<SqlToRqlResult, Error> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Ok(SqlToRqlResult::Refuse {
            diagnostic: DIAG_SQL_RQL_PARSE_ERROR.into(),
            detail: "empty SQL source".into(),
        });
    }
    if trimmed.len() > crate::rql_app_core::MAX_RQL_SOURCE_BYTES {
        return Ok(SqlToRqlResult::Refuse {
            diagnostic: DIAG_SQL_RQL_PARSE_ERROR.into(),
            detail: "SQL source exceeds size limit".into(),
        });
    }

    if let Some(r) = refuse_statement_shape(trimmed) {
        return Ok(r);
    }
    if let Some(r) = refuse_banned_constructs(trimmed) {
        return Ok(r);
    }

    match emit_select(trimmed) {
        Ok((rql, notes)) => {
            let compiled = if let Some(b) = bindings {
                Some(compile_app_core(&rql, b)?)
            } else {
                None
            };
            Ok(SqlToRqlResult::Emit(SqlToRqlEmit {
                rql,
                compiled,
                notes,
            }))
        }
        Err(RefuseErr { diagnostic, detail }) => Ok(SqlToRqlResult::Refuse { diagnostic, detail }),
    }
}

struct RefuseErr {
    diagnostic: String,
    detail: String,
}

fn refuse(diagnostic: &str, detail: impl Into<String>) -> RefuseErr {
    RefuseErr {
        diagnostic: diagnostic.into(),
        detail: detail.into(),
    }
}

fn refuse_statement_shape(sql: &str) -> Option<SqlToRqlResult> {
    let lower = sql.to_ascii_lowercase();
    let first = lower.split_whitespace().next().unwrap_or("");
    let banned_lead = [
        "insert", "update", "delete", "merge", "create", "drop", "alter", "truncate", "grant",
        "revoke", "begin", "commit", "rollback", "with",
    ];
    if banned_lead.contains(&first) {
        return Some(SqlToRqlResult::Refuse {
            diagnostic: DIAG_SQL_RQL_STATEMENT_UNSUPPORTED.into(),
            detail: format!("statement `{first}` is outside sql+ v1 (read-only SELECT)"),
        });
    }
    if first != "select" {
        return Some(SqlToRqlResult::Refuse {
            diagnostic: DIAG_SQL_RQL_PARSE_ERROR.into(),
            detail: format!("expected SELECT, got `{first}`"),
        });
    }
    // Second statement / trailing semicolon body.
    if lower.matches(';').count() > 0 {
        let after = lower.split(';').nth(1).unwrap_or("").trim();
        if !after.is_empty() {
            return Some(SqlToRqlResult::Refuse {
                diagnostic: DIAG_SQL_RQL_STATEMENT_UNSUPPORTED.into(),
                detail: "multiple statements are refused".into(),
            });
        }
    }
    None
}

fn refuse_banned_constructs(sql: &str) -> Option<SqlToRqlResult> {
    let lower = format!(" {} ", sql.to_ascii_lowercase().replace('\n', " "));
    let banned: &[(&str, &str)] = &[
        (" join ", "JOIN"),
        (" inner join ", "JOIN"),
        (" left join ", "JOIN"),
        (" right join ", "JOIN"),
        (" full join ", "JOIN"),
        (" cross join ", "JOIN"),
        (" group by ", "GROUP BY"),
        (" having ", "HAVING"),
        (" union ", "UNION"),
        (" intersect ", "INTERSECT"),
        (" except ", "EXCEPT"),
        (" distinct ", "DISTINCT"),
        (" distinct on ", "DISTINCT ON"),
        (" offset ", "OFFSET"),
        (" limit all ", "LIMIT ALL"),
        (" with ties ", "WITH TIES"),
        (" like ", "LIKE"),
        (" similar to ", "SIMILAR TO"),
        (" between ", "BETWEEN"),
        (" exists ", "EXISTS"),
        (" case ", "CASE"),
        (" cast ", "CAST"),
        (" over ", "window OVER"),
        (" fetch ", "FETCH"),
        (" tablesample ", "TABLESAMPLE"),
        (" for update ", "FOR UPDATE"),
        (" for share ", "FOR SHARE"),
    ];
    for (needle, label) in banned {
        if lower.contains(needle) {
            return Some(SqlToRqlResult::Refuse {
                diagnostic: DIAG_SQL_RQL_CONSTRUCT_UNSUPPORTED.into(),
                detail: format!("`{label}` is refused by sql+ v1"),
            });
        }
    }
    // Aggregate-ish function calls (heuristic; never emit).
    for agg in [" count(", " sum(", " avg(", " min(", " max("] {
        if lower.contains(agg) {
            return Some(SqlToRqlResult::Refuse {
                diagnostic: DIAG_SQL_RQL_CONSTRUCT_UNSUPPORTED.into(),
                detail: "aggregates are refused by sql+ v1 (APB-8 lane)".into(),
            });
        }
    }
    None
}

/// Emit a narrow SELECT … FROM … [WHERE …] [ORDER BY …] [LIMIT …] [PAGE SIZE …].
fn emit_select(sql: &str) -> Result<(String, Vec<String>), RefuseErr> {
    let mut p = SqlParser::new(sql.trim_end_matches(';').trim());
    p.expect_kw("select")?;
    let (project, star) = p.parse_select_list()?;
    p.expect_kw("from")?;
    let table = p.parse_ident()?;
    // Optional AS alias — ignored for Core (single source).
    if p.eat_kw("as") {
        let _alias = p.parse_ident()?;
    } else if p.peek_ident().is_some()
        && !p.peek_is_kw(&["where", "order", "limit", "page", "access", "continue"])
    {
        let _alias = p.parse_ident()?;
    }

    let mut notes = Vec::new();
    let mut where_rql = None;
    if p.eat_kw("where") {
        let (pred, null_notes) = p.parse_predicate()?;
        where_rql = Some(pred);
        notes.extend(null_notes);
    }

    let mut order_parts = Vec::new();
    if p.eat_kw("order") {
        p.expect_kw("by")?;
        loop {
            let col = p.parse_column_ref()?;
            let dir = if p.eat_kw("desc") {
                "desc"
            } else {
                let _ = p.eat_kw("asc");
                "asc"
            };
            let nulls = if p.eat_kw("nulls") {
                if p.eat_kw("first") {
                    Some("nulls first")
                } else if p.eat_kw("last") {
                    Some("nulls last")
                } else {
                    return Err(refuse(
                        DIAG_SQL_RQL_PARSE_ERROR,
                        "ORDER BY … NULLS requires FIRST|LAST",
                    ));
                }
            } else {
                None
            };
            let mut part = format!("{col} {dir}");
            if let Some(n) = nulls {
                part.push(' ');
                part.push_str(n);
            }
            order_parts.push(part);
            if !p.eat_char(',') {
                break;
            }
        }
    }

    let mut limit = None;
    if p.eat_kw("limit") {
        limit = Some(p.parse_u64()?);
    }

    let mut page_size = None;
    if p.eat_kw("page") {
        p.expect_kw("size")?;
        page_size = Some(p.parse_u64()?);
    }

    if p.eat_kw("access") || p.eat_kw("continue") {
        return Err(refuse(
            DIAG_SQL_RQL_CONSTRUCT_UNSUPPORTED,
            "ACCESS/CONTINUE are outside Application Core emit (full RQL / APP-6)",
        ));
    }

    p.skip_ws();
    if !p.is_eof() {
        return Err(refuse(
            DIAG_SQL_RQL_PARSE_ERROR,
            format!("unexpected trailing input near `{}`", p.snippet()),
        ));
    }

    if star && !project.is_empty() {
        return Err(refuse(
            DIAG_SQL_RQL_PARSE_ERROR,
            "SELECT * cannot mix with explicit columns",
        ));
    }

    let mut out = format!("from {table}");
    if let Some(w) = where_rql {
        out.push_str(" where ");
        out.push_str(&w);
    }
    if !star {
        if project.is_empty() {
            return Err(refuse(DIAG_SQL_RQL_PARSE_ERROR, "SELECT list is empty"));
        }
        out.push_str(" project ");
        out.push_str(&project.join(", "));
    }
    if !order_parts.is_empty() {
        out.push_str(" order by ");
        out.push_str(&order_parts.join(", "));
    }
    if let Some(n) = limit {
        out.push_str(&format!(" limit {n}"));
    }
    if let Some(n) = page_size {
        out.push_str(&format!(" page size {n}"));
    }
    Ok((out, notes))
}

// --- minimal SQL lexer/parser (SELECT subset) --------------------------------

struct SqlParser<'a> {
    s: &'a str,
    i: usize,
}

impl<'a> SqlParser<'a> {
    fn new(s: &'a str) -> Self {
        Self { s, i: 0 }
    }

    fn is_eof(&self) -> bool {
        self.skip_ws_immutable();
        self.i >= self.s.len()
    }

    fn skip_ws_immutable(&self) -> usize {
        let bytes = self.s.as_bytes();
        let mut i = self.i;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        i
    }

    fn skip_ws(&mut self) {
        self.i = self.skip_ws_immutable();
    }

    fn rest(&self) -> &'a str {
        let mut i = self.i.min(self.s.len());
        while i > 0 && i < self.s.len() && !self.s.is_char_boundary(i) {
            i -= 1;
        }
        &self.s[i..]
    }

    fn snippet(&self) -> String {
        self.rest().chars().take(24).collect()
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let r = self.rest();
        let mut it = r.char_indices();
        let (_, c) = it.next()?;
        let next = it.next().map(|(o, _)| o).unwrap_or(r.len());
        let base = self.s.len() - r.len();
        self.i = base + next;
        Some(c)
    }

    fn eat_char(&mut self, c: char) -> bool {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        self.skip_ws();
        let r = self.rest();
        let lower = r.to_ascii_lowercase();
        if lower.starts_with(kw)
            && (r.len() == kw.len()
                || !r
                    .as_bytes()
                    .get(kw.len())
                    .copied()
                    .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_'))
        {
            // Advance by keyword byte length (ASCII keywords only).
            self.i += kw.len();
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), RefuseErr> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(refuse(
                DIAG_SQL_RQL_PARSE_ERROR,
                format!("expected `{kw}` near `{}`", self.snippet()),
            ))
        }
    }

    fn peek_is_kw(&self, kws: &[&str]) -> bool {
        let i = self.skip_ws_immutable();
        let r = &self.s[i..];
        let lower = r.to_ascii_lowercase();
        kws.iter().any(|kw| {
            lower.starts_with(kw)
                && (r.len() == kw.len()
                    || !r
                        .as_bytes()
                        .get(kw.len())
                        .copied()
                        .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_'))
        })
    }

    fn peek_ident(&self) -> Option<()> {
        let i = self.skip_ws_immutable();
        let r = &self.s[i..];
        let c = r.chars().next()?;
        if c.is_ascii_alphabetic() || c == '_' || c == '"' {
            Some(())
        } else {
            None
        }
    }

    fn parse_ident(&mut self) -> Result<String, RefuseErr> {
        self.skip_ws();
        if self.peek() == Some('"') {
            self.bump();
            let mut out = String::new();
            loop {
                match self.bump() {
                    Some('"') => {
                        if self.peek() == Some('"') {
                            self.bump();
                            out.push('"');
                        } else {
                            break;
                        }
                    }
                    Some(c) => out.push(c),
                    None => {
                        return Err(refuse(
                            DIAG_SQL_RQL_PARSE_ERROR,
                            "unterminated quoted identifier",
                        ))
                    }
                }
            }
            return Ok(out);
        }
        let r = self.rest();
        let mut n = 0;
        for c in r.chars() {
            if c.is_ascii_alphanumeric() || c == '_' {
                n += c.len_utf8();
            } else {
                break;
            }
        }
        if n == 0 {
            return Err(refuse(
                DIAG_SQL_RQL_PARSE_ERROR,
                format!("expected identifier near `{}`", self.snippet()),
            ));
        }
        let raw = &r[..n];
        self.i += n;
        Ok(raw.to_ascii_lowercase())
    }

    fn parse_column_ref(&mut self) -> Result<String, RefuseErr> {
        let a = self.parse_ident()?;
        self.skip_ws();
        if self.eat_char('.') {
            let b = self.parse_ident()?;
            // table.col → dotted path for Core (drop table qualifier honesty).
            Ok(format!("{a}.{b}"))
        } else {
            Ok(a)
        }
    }

    fn parse_select_list(&mut self) -> Result<(Vec<String>, bool), RefuseErr> {
        self.skip_ws();
        if self.eat_char('*') {
            return Ok((Vec::new(), true));
        }
        let mut cols = Vec::new();
        loop {
            let col = self.parse_column_ref()?;
            // Optional AS alias — Core project uses path spelling, not alias.
            if self.eat_kw("as") {
                let _ = self.parse_ident()?;
            } else if self.peek_ident().is_some() && !self.peek_is_kw(&["from"]) {
                // bare alias
                let _ = self.parse_ident()?;
            }
            cols.push(col);
            if !self.eat_char(',') {
                break;
            }
        }
        Ok((cols, false))
    }

    fn parse_u64(&mut self) -> Result<u64, RefuseErr> {
        self.skip_ws();
        let r = self.rest();
        let mut n = 0;
        for c in r.chars() {
            if c.is_ascii_digit() {
                n += 1;
            } else {
                break;
            }
        }
        if n == 0 {
            return Err(refuse(
                DIAG_SQL_RQL_PARSE_ERROR,
                format!("expected unsigned integer near `{}`", self.snippet()),
            ));
        }
        let v: u64 = r[..n]
            .parse()
            .map_err(|_| refuse(DIAG_SQL_RQL_PARSE_ERROR, "integer out of range"))?;
        self.i += n;
        Ok(v)
    }

    fn parse_string(&mut self) -> Result<String, RefuseErr> {
        self.skip_ws();
        if self.peek() != Some('\'') {
            return Err(refuse(
                DIAG_SQL_RQL_PARSE_ERROR,
                format!("expected string near `{}`", self.snippet()),
            ));
        }
        self.bump();
        let mut out = String::new();
        loop {
            match self.bump() {
                Some('\'') => {
                    if self.peek() == Some('\'') {
                        self.bump();
                        out.push('\'');
                    } else {
                        break;
                    }
                }
                Some(c) => out.push(c),
                None => {
                    return Err(refuse(
                        DIAG_SQL_RQL_PARSE_ERROR,
                        "unterminated string literal",
                    ))
                }
            }
        }
        Ok(out)
    }

    fn parse_predicate(&mut self) -> Result<(String, Vec<String>), RefuseErr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<(String, Vec<String>), RefuseErr> {
        let (mut left, mut notes) = self.parse_and()?;
        while self.eat_kw("or") {
            let (right, n2) = self.parse_and()?;
            left = format!("({left}) or ({right})");
            notes.extend(n2);
        }
        Ok((left, notes))
    }

    fn parse_and(&mut self) -> Result<(String, Vec<String>), RefuseErr> {
        let (mut left, mut notes) = self.parse_not()?;
        while self.eat_kw("and") {
            let (right, n2) = self.parse_not()?;
            left = format!("{left} and {right}");
            notes.extend(n2);
        }
        Ok((left, notes))
    }

    fn parse_not(&mut self) -> Result<(String, Vec<String>), RefuseErr> {
        if self.eat_kw("not") {
            let (inner, notes) = self.parse_primary()?;
            Ok((format!("not ({inner})"), notes))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<(String, Vec<String>), RefuseErr> {
        self.skip_ws();
        if self.eat_char('(') {
            let (inner, notes) = self.parse_or()?;
            if !self.eat_char(')') {
                return Err(refuse(DIAG_SQL_RQL_PARSE_ERROR, "expected `)`"));
            }
            return Ok((format!("({inner})"), notes));
        }
        let col = self.parse_column_ref()?;
        self.skip_ws();
        if self.eat_kw("is") {
            let neg = self.eat_kw("not");
            self.expect_kw("null")?;
            // SQL document view collapses missing and Null → emit OR / AND.
            let notes = vec![
                "sql+ IS NULL maps missing∨Null (SQL document view collapse); Core retains both"
                    .into(),
            ];
            let expr = if neg {
                format!("present({col}) and {col} is not null")
            } else {
                format!("(missing({col}) or {col} is null)")
            };
            return Ok((expr, notes));
        }
        let not_in = self.eat_kw("not");
        if self.eat_kw("in") {
            self.skip_ws();
            if !self.eat_char('(') {
                return Err(refuse(DIAG_SQL_RQL_PARSE_ERROR, "expected `(` after IN"));
            }
            let mut vals = Vec::new();
            loop {
                vals.push(self.parse_literal_rql()?);
                if !self.eat_char(',') {
                    break;
                }
            }
            if !self.eat_char(')') {
                return Err(refuse(
                    DIAG_SQL_RQL_PARSE_ERROR,
                    "expected `)` after IN list",
                ));
            }
            let list = vals.join(", ");
            let expr = if not_in {
                format!("{col} not in [{list}]")
            } else {
                format!("{col} in [{list}]")
            };
            return Ok((expr, Vec::new()));
        }
        if not_in {
            return Err(refuse(
                DIAG_SQL_RQL_PARSE_ERROR,
                "expected `IN` after `NOT`",
            ));
        }
        let op = if self.eat_ascii("<>") || self.eat_ascii("!=") {
            "!="
        } else if self.eat_ascii("<=") {
            "<="
        } else if self.eat_ascii(">=") {
            ">="
        } else if self.eat_char('<') {
            "<"
        } else if self.eat_char('>') {
            ">"
        } else if self.eat_char('=') {
            "="
        } else {
            return Err(refuse(
                DIAG_SQL_RQL_PARSE_ERROR,
                format!("expected comparison after `{col}`"),
            ));
        };
        let right = self.parse_operand_rql()?;
        Ok((format!("{col} {op} {right}"), Vec::new()))
    }

    fn eat_ascii(&mut self, op: &str) -> bool {
        self.skip_ws();
        let r = self.rest();
        if r.starts_with(op) {
            self.i += op.len();
            true
        } else {
            false
        }
    }

    fn parse_operand_rql(&mut self) -> Result<String, RefuseErr> {
        self.skip_ws();
        if self.peek() == Some(':') {
            self.bump();
            let name = self.parse_ident()?;
            return Ok(format!("${name}"));
        }
        if self.peek() == Some('\'') {
            let s = self.parse_string()?;
            // Core uses JSON double-quoted strings.
            let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
            return Ok(format!("\"{escaped}\""));
        }
        if self.eat_kw("true") {
            return Ok("true".into());
        }
        if self.eat_kw("false") {
            return Ok("false".into());
        }
        if self.eat_kw("null") {
            return Err(refuse(
                DIAG_SQL_RQL_CONSTRUCT_UNSUPPORTED,
                "NULL literal comparisons are refused; use IS NULL",
            ));
        }
        // number
        let r = self.rest();
        let mut n = 0;
        let bytes = r.as_bytes();
        if !bytes.is_empty() && (bytes[0] == b'-' || bytes[0].is_ascii_digit()) {
            if bytes[0] == b'-' {
                n = 1;
            }
            let start = n;
            while n < bytes.len() && bytes[n].is_ascii_digit() {
                n += 1;
            }
            if n > start {
                if n < bytes.len() && bytes[n] == b'.' {
                    return Err(refuse(
                        DIAG_SQL_RQL_CONSTRUCT_UNSUPPORTED,
                        "decimal literals are outside sql+ Phase-2 scaffold",
                    ));
                }
                let lit = &r[..n];
                self.i += n;
                return Ok(lit.to_string());
            }
        }
        // column ref as right-hand side
        let col = self.parse_column_ref()?;
        Ok(col)
    }

    fn parse_literal_rql(&mut self) -> Result<String, RefuseErr> {
        self.parse_operand_rql()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use residiuum_heap::CollectionId;
    use std::str::FromStr;

    fn bindings() -> CollectionBindings {
        let mut b = CollectionBindings::default();
        b.bind(
            "orders",
            CollectionId::from_str("00000000-0000-4000-8000-0000000000a1").unwrap(),
        );
        b
    }

    #[test]
    fn refuse_join_and_group_by() {
        for sql in [
            "SELECT * FROM orders JOIN customers ON orders.id = customers.id",
            "SELECT status FROM orders GROUP BY status",
            "SELECT * FROM orders OFFSET 10",
            "INSERT INTO orders VALUES (1)",
            "SELECT COUNT(*) FROM orders",
        ] {
            let r = compile_sql_to_rql(sql, None).unwrap();
            assert!(r.is_refuse(), "expected refuse for {sql:?}, got {r:?}");
        }
    }

    #[test]
    fn emit_simple_select_equals() {
        let r = compile_sql_to_rql(
            "SELECT id, status FROM orders WHERE status = 'open' ORDER BY created_at DESC LIMIT 100",
            Some(&bindings()),
        )
        .unwrap();
        match r {
            SqlToRqlResult::Emit(e) => {
                assert!(e.rql.contains("from orders"));
                assert!(e.rql.contains("status = \"open\""));
                assert!(e.rql.contains("project id, status"));
                assert!(e.rql.contains("order by created_at desc"));
                assert!(e.rql.contains("limit 100"));
                assert!(e.compiled.is_some());
            }
            other => panic!("expected emit, got {other:?}"),
        }
    }

    #[test]
    fn emit_is_null_collapses_with_note() {
        let r = compile_sql_to_rql(
            "SELECT id FROM orders WHERE nickname IS NULL",
            Some(&bindings()),
        )
        .unwrap();
        match r {
            SqlToRqlResult::Emit(e) => {
                assert!(e.rql.contains("missing(nickname)") || e.rql.contains("is null"));
                assert!(!e.notes.is_empty());
                assert!(e.compiled.is_some());
            }
            other => panic!("expected emit, got {other:?}"),
        }
    }

    #[test]
    fn emit_param_and_star() {
        let r = compile_sql_to_rql(
            "SELECT * FROM orders WHERE status = :status PAGE SIZE 16",
            Some(&bindings()),
        )
        .unwrap();
        match r {
            SqlToRqlResult::Emit(e) => {
                assert!(!e.rql.contains("project"));
                assert!(e.rql.contains("$status"));
                assert!(e.rql.contains("page size 16"));
            }
            other => panic!("expected emit, got {other:?}"),
        }
    }

    #[test]
    fn profiles() {
        assert_eq!(SQL_PLUS_PROFILE, "residiuum-sql-plus-to-rql-v1");
        assert_eq!(SQL_PLUS_DIALECT, "sql+");
        assert_eq!(SQL_PLUS_ALIAS, "sql-plus");
    }
}
