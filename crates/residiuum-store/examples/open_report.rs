//! Print one writable store-open report for startup diagnosis.

use residiuum_store::Store;
use std::path::PathBuf;
use std::time::Instant;

fn main() {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: open_report STORE");
    let started = Instant::now();
    match Store::open(&path) {
        Ok(store) => {
            println!("elapsed={:?}", started.elapsed());
            println!("{:#?}", store.open_report());
        }
        Err(error) => {
            eprintln!("open failed after {:?}: {error}", started.elapsed());
            std::process::exit(1);
        }
    }
}
