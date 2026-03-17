use std::fs;
use std::path::PathBuf;

pub fn fixture(path: &str) -> String {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    fs::read_to_string(base.join(path)).expect("fixture should exist")
}
