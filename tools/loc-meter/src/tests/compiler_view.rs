//! The claim "categorised the way the compiler categorises it", checked
//! against the compiler: `cargo check` writes dep-info listing every source
//! it read; the walk must name exactly that set.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command;

use crate::side::{Side, TempDir};
use crate::tests::fixture::Fixture;

#[test]
fn the_walk_names_exactly_the_files_the_compiler_reads() {
    let fx = Fixture::new();
    fx.write(
        "crates/alpha/src/lib.rs",
        "mod a;\n#[path = \"custom.rs\"]\nmod c;\nmod inline {\n    pub mod deep;\n}\n#[cfg(test)]\nmod tests;\n#[cfg(feature = \"never\")]\nmod never;\npub fn alpha() -> u32 {\n    a::b::v() + c::under::w() + inline::deep::d()\n}\n",
    );
    fx.write("crates/alpha/Cargo.toml", "[package]\nname = \"alpha\"\nversion = \"0.0.1\"\nedition = \"2024\"\n\n[features]\nnever = []\n");
    fx.write("crates/alpha/src/a.rs", "pub mod b;\n");
    fx.write("crates/alpha/src/a/b.rs", "pub fn v() -> u32 {\n    1\n}\n");
    fx.write("crates/alpha/src/custom.rs", "pub mod under;\n");
    fx.write(
        "crates/alpha/src/under.rs",
        "pub fn w() -> u32 {\n    2\n}\n",
    );
    fx.write(
        "crates/alpha/src/inline/deep.rs",
        "pub fn d() -> u32 {\n    3\n}\n",
    );
    fx.write("crates/alpha/src/tests.rs", "#[test]\nfn t() {}\n");
    fx.write(
        "crates/alpha/src/never.rs",
        "compile_error!(\"never compiled\");\n",
    );
    fx.commit("module shapes");

    let target = TempDir::new("target").unwrap();
    let status = Command::new("cargo")
        .args(["check", "-q", "--offline", "--manifest-path"])
        .arg(fx.path().join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target.path())
        .status()
        .unwrap();
    assert!(status.success(), "cargo check of the fixture");
    let mut compiler: BTreeSet<PathBuf> = BTreeSet::new();
    for entry in std::fs::read_dir(target.path().join("debug/deps")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "d") {
            // Sources are written relative to the workspace root; targets are absolute.
            for token in std::fs::read_to_string(&path).unwrap().split_whitespace() {
                let token = token.trim_end_matches(':');
                let rel = std::path::Path::new(token);
                if rel.is_relative() && rel.extension().is_some_and(|e| e == "rs") {
                    compiler.insert(rel.to_path_buf());
                }
            }
        }
    }

    let side = Side::load(fx.path(), "HEAD").unwrap();
    let walked: BTreeSet<PathBuf> = side.compiled_files().map(|p| p.to_path_buf()).collect();
    assert_eq!(walked, compiler);
    assert!(
        compiler.contains(std::path::Path::new("crates/alpha/src/under.rs")),
        "{compiler:?}"
    );
    assert!(
        !compiler.contains(std::path::Path::new("crates/alpha/src/tests.rs")),
        "{compiler:?}"
    );
}
