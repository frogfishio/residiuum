use residiuum_rql_qual::residiuum_embedded::run_game_dipstick;

fn main() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/rql-mongo-dipstick");
    let documents = std::env::var("RQL_DIPSTICK_DOCUMENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000);
    let warmups = std::env::var("RQL_DIPSTICK_WARMUPS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3);
    let iterations = std::env::var("RQL_DIPSTICK_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(12);
    let report = run_game_dipstick(&root, documents, warmups, iterations)
        .expect("run Residiuum game dipstick");
    let body = serde_json::to_vec_pretty(&report).expect("encode report");
    let path = root.join("residiuum.json");
    std::fs::write(&path, body).expect("write report");
    println!("{}", path.display());
}
