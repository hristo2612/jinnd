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
