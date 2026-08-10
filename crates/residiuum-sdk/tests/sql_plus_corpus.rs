//! Phase 2: SQL-ish+ emit/refuse corpus host (`sql_plus_corpus_v1.json`).

use residiuum_heap::CollectionId;
use residiuum_sdk::{
    compile_sql_to_rql, CollectionBindings, SqlToRqlResult, SQL_PLUS_ALIAS, SQL_PLUS_DIALECT,
    SQL_PLUS_PROFILE,
};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn read_json(rel: &str) -> Value {
    let path = workspace_root().join(rel);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("json {}: {e}", path.display()))
}

#[test]
fn sql_plus_profiles() {
    assert_eq!(SQL_PLUS_PROFILE, "residiuum-sql-plus-to-rql-v1");
    assert_eq!(SQL_PLUS_DIALECT, "sql+");
    assert_eq!(SQL_PLUS_ALIAS, "sql-plus");
}

#[test]
fn sql_plus_corpus_emit_and_refuse() {
    let doc = read_json("spec/app/v1/sql_plus_corpus_v1.json");
    assert_eq!(doc["profile"].as_str(), Some(SQL_PLUS_PROFILE));

    let mut bindings = CollectionBindings::default();
    for (name, id) in doc["default_bindings"].as_object().unwrap() {
        bindings.bind(name, CollectionId::from_str(id.as_str().unwrap()).unwrap());
    }

    let emit = doc["emit"].as_array().unwrap();
    assert!(emit.len() >= 3, "emit corpus too small");
    for v in emit {
        let id = v["id"].as_str().unwrap_or("?");
        let sql = v["source_sql"].as_str().unwrap();
        let result = compile_sql_to_rql(sql, Some(&bindings))
            .unwrap_or_else(|e| panic!("{id}: compile error {e}"));
        let SqlToRqlResult::Emit(e) = result else {
            panic!("{id}: expected emit for {sql}");
        };
        for needle in v["expect_rql_contains"].as_array().unwrap() {
            let n = needle.as_str().unwrap();
            assert!(e.rql.contains(n), "{id}: rql `{}` missing `{n}`", e.rql);
        }
        if v.get("expect_notes_nonempty").and_then(|x| x.as_bool()) == Some(true) {
            assert!(!e.notes.is_empty(), "{id}: expected honesty notes");
        }
        assert!(e.compiled.is_some(), "{id}: Core compile must succeed");
    }

    let refuse = doc["refuse"].as_array().unwrap();
    assert!(refuse.len() >= 5, "refuse corpus too small");
    for v in refuse {
        let id = v["id"].as_str().unwrap_or("?");
        let sql = v["source_sql"].as_str().unwrap();
        let result = compile_sql_to_rql(sql, Some(&bindings))
            .unwrap_or_else(|e| panic!("{id}: unexpected Error {e}"));
        let SqlToRqlResult::Refuse { diagnostic, detail } = result else {
            panic!("{id}: expected refuse for {sql}");
        };
        let msg = format!("{diagnostic} {detail}");
        for needle in v["diagnostic_contains"].as_array().unwrap() {
            let n = needle.as_str().unwrap();
            assert!(
                msg.to_ascii_lowercase().contains(&n.to_ascii_lowercase()),
                "{id}: `{msg}` missing `{n}`"
            );
        }
    }
}
