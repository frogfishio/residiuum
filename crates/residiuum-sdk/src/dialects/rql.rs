//! RQL (Residiuum Query Language) — official human dialect → pure ENR1 + SDA.
//!
//! Design authority:
//! [doc/wip/query/RQL_SPEC.md](../../../../doc/wip/query/RQL_SPEC.md).
//! Every construct lowers into the same match-bag kernel that pure ENR text uses
//! (`Match` / `one!` / `one?` / bare bag / `enrich` pipe). Execution is shared
//! (`residiuum-sda`); there is no second evaluator.

use crate::dialects::CompiledSda;
use crate::error::Error;

/// Compile RQL source into pure ENR1 + SDA program text.
pub fn compile_rql(source: &str) -> Result<CompiledSda, Error> {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return Err(Error::QueryInvalid("dialect 'rql': empty program".into()));
    }
    let program = parse_rql(trimmed)?;
    let sda = lower_to_sda(&program)?;
    // Proof of compile path: emitted text must parse as ENR1/SDA.
    sda_core::Program::parse(&sda).map_err(|e| {
        Error::QueryInvalid(format!(
            "dialect 'rql': internal SDA compile failed: {e}; sda={sda}"
        ))
    })?;
    Ok(CompiledSda::program(
        "rql",
        sda,
        std::iter::empty::<String>(),
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RqlProgram {
    from: Option<String>,
    enriches: Vec<EnrichStep>,
    project: Option<Vec<ProjectField>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EnrichStep {
    output: String,
    source: String,
    left_key: Vec<String>,
    right_key: Vec<String>,
    cardinality: Cardinality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cardinality {
    ExactlyOne,
    Optional,
    Many,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProjectField {
    /// `order_id` or dotted path `customer.name` (nested Maps).
    Path(Vec<String>),
    /// `items { quantity, product.name }`
    Nested {
        name: String,
        fields: Vec<ProjectField>,
    },
}

fn parse_rql(source: &str) -> Result<RqlProgram, Error> {
    let tokens = tokenize(source)?;
    let mut p = Parser { tokens, pos: 0 };
    p.parse_program()
}

struct Parser {
    tokens: Vec<Tok>,
    pos: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    LBrace,
    RBrace,
    Eq,
    Comma,
    Dot,
    Eof,
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
        // line comments
        if c == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        match c {
            '{' => {
                out.push(Tok::LBrace);
                i += 1;
            }
            '}' => {
                out.push(Tok::RBrace);
                i += 1;
            }
            '=' => {
                out.push(Tok::Eq);
                i += 1;
            }
            ',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            '.' => {
                out.push(Tok::Dot);
                i += 1;
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                i += 1;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                out.push(Tok::Ident(s));
            }
            other => {
                return Err(Error::QueryInvalid(format!(
                    "dialect 'rql': unexpected character {other:?}"
                )));
            }
        }
    }
    out.push(Tok::Eof);
    Ok(out)
}

impl Parser {
    fn peek(&self) -> &Tok {
        self.tokens.get(self.pos).unwrap_or(&Tok::Eof)
    }

    fn advance(&mut self) -> Tok {
        let t = self.tokens.get(self.pos).cloned().unwrap_or(Tok::Eof);
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        t
    }

    fn expect_ident(&mut self) -> Result<String, Error> {
        match self.advance() {
            Tok::Ident(s) => Ok(s),
            other => Err(Error::QueryInvalid(format!(
                "dialect 'rql': expected identifier, got {other:?}"
            ))),
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), Error> {
        match self.advance() {
            Tok::Ident(s) if s.eq_ignore_ascii_case(kw) => Ok(()),
            other => Err(Error::QueryInvalid(format!(
                "dialect 'rql': expected keyword {kw:?}, got {other:?}"
            ))),
        }
    }

    fn parse_program(&mut self) -> Result<RqlProgram, Error> {
        let mut from = None;
        let mut enriches = Vec::new();
        let mut project = None;

        while !matches!(self.peek(), Tok::Eof) {
            match self.peek() {
                Tok::Ident(s) if s.eq_ignore_ascii_case("from") => {
                    self.advance();
                    from = Some(self.expect_ident()?);
                }
                Tok::Ident(s) if s.eq_ignore_ascii_case("enrich") => {
                    enriches.push(self.parse_enrich()?);
                }
                Tok::Ident(s) if s.eq_ignore_ascii_case("project") => {
                    if project.is_some() {
                        return Err(Error::QueryInvalid(
                            "dialect 'rql': only one project clause is allowed".into(),
                        ));
                    }
                    self.advance();
                    project = Some(self.parse_project_block()?);
                }
                other => {
                    return Err(Error::QueryInvalid(format!(
                        "dialect 'rql': unexpected token {other:?}; expected from/enrich/project"
                    )));
                }
            }
        }

        if from.is_none() && enriches.is_empty() {
            return Err(Error::QueryInvalid(
                "dialect 'rql': program needs `from <collection>` or at least one enrich".into(),
            ));
        }

        Ok(RqlProgram {
            from,
            enriches,
            project,
        })
    }

    fn parse_enrich(&mut self) -> Result<EnrichStep, Error> {
        self.expect_kw("enrich")?;
        let output = self.expect_ident()?;
        self.expect_kw("using")?;
        let source = self.expect_ident()?;
        self.expect_kw("matching")?;
        let left_key = self.parse_path()?;
        match self.advance() {
            Tok::Eq => {}
            other => {
                return Err(Error::QueryInvalid(format!(
                    "dialect 'rql': expected '=' in matching clause, got {other:?}"
                )));
            }
        }
        let right_key = self.parse_path()?;
        self.expect_kw("expect")?;
        let card = self.expect_ident()?;
        let cardinality = match card.to_ascii_lowercase().as_str() {
            "exactly_one" | "exactlyone" | "one" => Cardinality::ExactlyOne,
            "optional" | "opt" => Cardinality::Optional,
            "many" => Cardinality::Many,
            other => {
                return Err(Error::QueryInvalid(format!(
                    "dialect 'rql': unknown expect cardinality {other:?}; \
                     use exactly_one, optional, or many"
                )));
            }
        };
        Ok(EnrichStep {
            output,
            source,
            left_key,
            right_key,
            cardinality,
        })
    }

    fn parse_path(&mut self) -> Result<Vec<String>, Error> {
        let mut parts = vec![self.expect_ident()?];
        while matches!(self.peek(), Tok::Dot) {
            self.advance();
            parts.push(self.expect_ident()?);
        }
        Ok(parts)
    }

    fn parse_project_block(&mut self) -> Result<Vec<ProjectField>, Error> {
        match self.advance() {
            Tok::LBrace => {}
            other => {
                return Err(Error::QueryInvalid(format!(
                    "dialect 'rql': expected '{{' after project, got {other:?}"
                )));
            }
        }
        let mut fields = Vec::new();
        while !matches!(self.peek(), Tok::RBrace | Tok::Eof) {
            fields.push(self.parse_project_field()?);
            if matches!(self.peek(), Tok::Comma) {
                self.advance();
            }
        }
        match self.advance() {
            Tok::RBrace => {}
            other => {
                return Err(Error::QueryInvalid(format!(
                    "dialect 'rql': expected '}}' to close project, got {other:?}"
                )));
            }
        }
        Ok(fields)
    }

    fn parse_project_field(&mut self) -> Result<ProjectField, Error> {
        let name = self.expect_ident()?;
        if matches!(self.peek(), Tok::LBrace) {
            let nested = self.parse_project_block()?;
            return Ok(ProjectField::Nested {
                name,
                fields: nested,
            });
        }
        let mut path = vec![name];
        while matches!(self.peek(), Tok::Dot) {
            self.advance();
            path.push(self.expect_ident()?);
        }
        Ok(ProjectField::Path(path))
    }
}

fn lower_to_sda(program: &RqlProgram) -> Result<String, Error> {
    let root = program.from.clone().ok_or_else(|| {
        Error::QueryInvalid(
            "dialect 'rql': `from <collection>` is required for v0.1 lowering".into(),
        )
    })?;

    let mut pipeline = root;
    for step in &program.enriches {
        let match_expr = format!(
            "Match(l, {}, {}, {})",
            step.source,
            path_get("l", &step.left_key),
            path_get("r", &step.right_key),
        );
        let value = match step.cardinality {
            Cardinality::ExactlyOne => format!("one!({match_expr})"),
            Cardinality::Optional => format!("one?({match_expr})"),
            Cardinality::Many => match_expr,
        };
        pipeline = format!(
            "{pipeline}\n|> enrich {{\n    {}: {}\n  }}",
            step.output, value
        );
    }

    if let Some(fields) = &program.project {
        let body = lower_project_fields(fields, "o", 0)?;
        pipeline = format!("{pipeline}\n|> refine {{\n    yield {body}\n    | o in _\n  }}");
    }

    Ok(pipeline)
}

fn path_get(var: &str, path: &[String]) -> String {
    let segs = path
        .iter()
        .map(|s| format!("\"{s}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!("getPath({var}, Seq[{segs}])")
}

fn lower_project_fields(fields: &[ProjectField], row: &str, depth: usize) -> Result<String, Error> {
    if depth > 8 {
        return Err(Error::QueryInvalid(
            "dialect 'rql': project nesting too deep".into(),
        ));
    }
    // Group dotted paths into nested Maps; Nested blocks map over bag fields.
    use std::collections::BTreeMap;
    enum Node {
        /// Keep full root-relative path for getPath on the row.
        Leaf(Vec<String>),
        Nested(Vec<ProjectField>),
        Group(BTreeMap<String, Node>),
    }

    let mut root: BTreeMap<String, Node> = BTreeMap::new();
    for f in fields {
        match f {
            ProjectField::Nested { name, fields } => {
                root.insert(name.clone(), Node::Nested(fields.clone()));
            }
            ProjectField::Path(path) => {
                if path.is_empty() {
                    continue;
                }
                if path.len() == 1 {
                    root.insert(path[0].clone(), Node::Leaf(path.clone()));
                    continue;
                }
                let mut cursor = &mut root;
                for (i, seg) in path.iter().enumerate() {
                    let is_last = i + 1 == path.len();
                    if is_last {
                        cursor.insert(seg.clone(), Node::Leaf(path.clone()));
                    } else {
                        let entry = cursor
                            .entry(seg.clone())
                            .or_insert_with(|| Node::Group(BTreeMap::new()));
                        match entry {
                            Node::Group(g) => cursor = g,
                            Node::Leaf(_) | Node::Nested(_) => {
                                return Err(Error::QueryInvalid(format!(
                                    "dialect 'rql': project path conflict at {seg}"
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    fn emit_map(
        map: &BTreeMap<String, Node>,
        row: &str,
        prefix: &[String],
        depth: usize,
    ) -> Result<String, Error> {
        let mut entries = Vec::new();
        for (key, node) in map {
            let mut path = prefix.to_vec();
            path.push(key.clone());
            let val = match node {
                Node::Leaf(full_path) => path_get(row, full_path),
                Node::Group(g) => emit_map(g, row, &path, depth + 1)?,
                Node::Nested(fields) => {
                    // Map over bag/seq field: bindOpt(getPath(...), bag => Some({…})).
                    let bind = format!("i{depth}");
                    let inner = lower_project_fields(fields, &bind, depth + 1)?;
                    let bag_path = path_get(row, &path);
                    format!("bindOpt({bag_path}, bag => Some({{ yield {inner} | {bind} in bag }}))")
                }
            };
            entries.push(format!("\"{key}\" -> {val}"));
        }
        Ok(format!("Map{{{}}}", entries.join(", ")))
    }

    emit_map(&root, row, &[], depth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::SdaShape;
    use serde_json::json;

    #[test]
    fn enrich_exactly_one_lowers_and_runs() {
        let rql = r#"
            from orders
            enrich customer using customers
              matching customer_id = id
              expect exactly_one
        "#;
        let compiled = compile_rql(rql).unwrap();
        assert_eq!(compiled.dialect, "rql");
        assert_eq!(compiled.shape, SdaShape::Program);
        assert!(compiled.sda.contains("Match("));
        assert!(compiled.sda.contains("one!("));
        assert!(compiled.sda.contains("enrich"));

        let pure = r#"
          orders
          |> enrich {
              customer:
                one!(Match(
                  l,
                  customers,
                  getPath(l, Seq["customer_id"]),
                  getPath(r, Seq["id"])
                ))
            }
        "#;
        let input = json!({
            "orders": [
                {"id": "o1", "customer_id": "c1"},
                {"id": "o2", "customer_id": "c2"}
            ],
            "customers": [
                {"id": "c1", "name": "Ada"},
                {"id": "c2", "name": "Bob"}
            ]
        });
        let bindings = [
            ("orders".into(), input["orders"].clone()),
            ("customers".into(), input["customers"].clone()),
        ];
        let from_rql = sda_core::Program::parse(&compiled.sda)
            .unwrap()
            .run_json_bindings(bindings.clone())
            .unwrap();
        let from_pure = sda_core::Program::parse(pure)
            .unwrap()
            .run_json_bindings(bindings)
            .unwrap();
        assert_eq!(from_rql, from_pure);
        assert_eq!(from_rql[0]["customer"]["name"], json!("Ada"));
    }

    #[test]
    fn optional_and_many_cardinality() {
        let opt = compile_rql(
            r#"
            from orders
            enrich customer using customers
              matching customer_id = id
              expect optional
        "#,
        )
        .unwrap();
        assert!(opt.sda.contains("one?("));

        let many = compile_rql(
            r#"
            from orders
            enrich items using items
              matching id = order_id
              expect many
        "#,
        )
        .unwrap();
        assert!(many.sda.contains("Match("));
        assert!(!many.sda.contains("one!("));
        assert!(!many.sda.contains("one?("));
    }
}
