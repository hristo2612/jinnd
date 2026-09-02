//! The round-1 rulings on the M2-K18 card (2026-09-02): a rename across
//! categories is a full delete plus a full add, and a `mod` the walk cannot
//! resolve refuses the run the way a dirty tree does.

use std::process::Command;

use crate::side::TempDir;
use crate::tests::fixture::Fixture;
use crate::{Category, MeterError};

const EXTRA: &str = "pub fn two() -> u32 {\n    2\n}\n";

/// The verifier's fixture, verbatim: an unchanged `tools/extra.rs` moved into
/// the compiled tree. Legacy path readings were crates +5 / tools -3.
#[test]
fn a_cross_category_rename_is_a_full_delete_and_a_full_add() {
    let fx = Fixture::new();
    fx.write("tools/extra.rs", EXTRA);
    fx.commit("tool");
    fx.git(&["checkout", "-q", "main"]);
    fx.git(&["merge", "-q", "--ff-only", "packet"]);
    fx.git(&["checkout", "-q", "packet"]);
    fx.remove("tools/extra.rs");
    fx.write("crates/alpha/src/extra.rs", EXTRA);
    fx.write(
        "crates/alpha/src/lib.rs",
        "mod extra;\n\npub fn alpha() -> u32 {\n    extra::two() - 1\n}\n",
    );
    fx.commit("move");
    let report = fx.measure().unwrap();
    assert_eq!(
        report.total(Category::Production).net(),
        fx.old_meter(&["crates"]),
        "{report:#?}"
    );
    assert_eq!(
        report.total(Category::Tools).net(),
        fx.old_meter(&["tools"]),
        "{report:#?}"
    );
    assert_eq!(report.total(Category::Production).net(), 5);
    assert_eq!(report.total(Category::Tools).net(), -3);
    let moved: Vec<_> = report
        .files
        .iter()
        .filter(|r| r.old.as_deref() == Some("tools/extra.rs"))
        .map(|r| (r.category, r.delta.added, r.delta.deleted))
        .collect();
    assert_eq!(
        moved,
        vec![(Category::Tools, 0, 3), (Category::Production, 3, 0)],
        "a delete row on the old line, an add row on the new: {report:#?}"
    );
}

/// A committed `mod absent;` is E0583 to the compiler. The meter claims the
/// compiler's view, so it refuses before `cargo check` gets to fail.
#[test]
fn a_mod_the_walk_cannot_resolve_refuses_the_run_as_the_compiler_would() {
    let fx = Fixture::new();
    fx.append("crates/alpha/src/lib.rs", "mod absent;\n");
    fx.commit("absent");
    let refusal = fx.measure();
    assert!(refusal.is_err(), "expected a refusal, got {refusal:#?}");
    let target = TempDir::new("target").unwrap();
    let check = Command::new("cargo")
        .args(["check", "-q", "--offline", "--manifest-path"])
        .arg(fx.path().join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target.path())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&check.stderr);
    assert!(!check.status.success(), "cargo check must fail too");
    assert!(stderr.contains("E0583"), "{stderr}");
}

/// The other silent path: `#[path]` names a file the walk cannot read.
#[test]
fn a_path_attribute_to_a_missing_file_refuses_the_run() {
    let fx = Fixture::new();
    fx.append(
        "crates/alpha/src/lib.rs",
        "#[path = \"nowhere.rs\"]\nmod elsewhere;\n",
    );
    fx.commit("path");
    let refusal = fx.measure();
    assert!(refusal.is_err(), "expected a refusal, got {refusal:#?}");
}
