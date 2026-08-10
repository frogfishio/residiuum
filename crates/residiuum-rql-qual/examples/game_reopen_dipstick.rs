use residiuum_rql_qual::residiuum_embedded::run_game_reopen_dipstick;

fn main() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/rql-mongo-dipstick");
    let documents = std::env::var("RQL_DIPSTICK_DOCUMENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5_000);
    let repetitions = std::env::var("RQL_REOPEN_REPETITIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(7);
    let report = run_game_reopen_dipstick(&root, documents, repetitions)
        .expect("run Residiuum store-reopen dipstick");
    let body = serde_json::to_vec_pretty(&report).expect("encode reopen report");
    let path = root.join("residiuum-reopen.json");
    std::fs::write(&path, body).expect("write reopen report");
    println!("{}", path.display());
}
