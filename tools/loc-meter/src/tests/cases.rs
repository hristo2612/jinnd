//! The card's required cases (M2-K18 §5), one test each, plus the shapes the
//! five historical mis-readings took.

use crate::tests::fixture::{Fixture, LIB};
use crate::{Category, MeterError};

const TESTS_RS: &str = "use super::alpha;\n\n#[test]\nfn one() {\n    assert_eq!(alpha(), 1);\n}\n";

#[test]
fn cfg_test_mod_registration_in_a_production_file_counts_zero() {
    let fx = Fixture::new();
    fx.append("crates/alpha/src/lib.rs", "\n#[cfg(test)]\nmod tests;\n");
    fx.write("crates/alpha/src/tests.rs", TESTS_RS);
    fx.commit("tests");
    let report = fx.measure().unwrap();
    assert_eq!(report.total(Category::Production).net(), 0, "{report:#?}");
    assert_eq!(
        report.total(Category::Tests).net(),
        3 + 6,
        "registration lines + tests.rs, {report:#?}"
    );
}

#[test]
fn tests_directory_under_src_counts_zero() {
    let fx = Fixture::new();
    fx.append("crates/alpha/src/lib.rs", "\n#[cfg(test)]\nmod tests;\n");
    fx.write("crates/alpha/src/tests/mod.rs", "mod cases;\n");
    fx.write("crates/alpha/src/tests/cases.rs", TESTS_RS);
    fx.commit("tests dir");
    let report = fx.measure().unwrap();
    assert_eq!(report.total(Category::Production).net(), 0, "{report:#?}");
    assert_eq!(
        report.total(Category::Tests).net(),
        3 + 1 + 6,
        "{report:#?}"
    );
}

#[test]
fn a_tests_rs_sibling_counts_zero() {
    let fx = Fixture::new();
    fx.append("crates/alpha/src/lib.rs", "mod hostnet;\n");
    fx.write(
        "crates/alpha/src/hostnet.rs",
        "pub fn h() -> u32 {\n    2\n}\n\n#[cfg(test)]\nmod hostnet_tests;\n",
    );
    fx.write(
        "crates/alpha/src/hostnet/hostnet_tests.rs",
        "#[test]\nfn two() {\n    assert_eq!(super::h(), 2);\n}\n",
    );
    fx.commit("sibling");
    let report = fx.measure().unwrap();
    assert_eq!(
        report.total(Category::Production).net(),
        1 + 3,
        "{report:#?}"
    );
    assert_eq!(report.total(Category::Tests).net(), 3 + 4, "{report:#?}");
}

#[test]
fn inline_cfg_test_block_counts_zero() {
    let fx = Fixture::new();
    fx.append("crates/alpha/src/lib.rs", "\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn one() {\n        assert_eq!(super::alpha(), 1);\n    }\n}\n");
    fx.commit("inline");
    let report = fx.measure().unwrap();
    assert_eq!(report.total(Category::Production).net(), 0, "{report:#?}");
    assert_eq!(report.total(Category::Tests).net(), 8, "{report:#?}");
}

#[test]
fn contract_prose_lands_on_the_prose_line_and_contract_files_on_theirs() {
    let fx = Fixture::new();
    fx.write(
        "contracts/jinn-x/README.md",
        "# jinn:x\n\nWhy this contract exists.\n\nIts stated reasons.\n",
    );
    fx.write(
        "contracts/jinn-x/contract.wit",
        "package jinn:x;\n\ninterface x {}\n",
    );
    fx.write(
        "contracts/jinn-x/metadata.toml",
        "[contract]\nname = \"jinn:x\"\n",
    );
    fx.commit("contract");
    let report = fx.measure().unwrap();
    assert_eq!(report.total(Category::Production).net(), 0, "{report:#?}");
    assert_eq!(report.total(Category::Prose).net(), 5, "{report:#?}");
    assert_eq!(report.total(Category::Contracts).net(), 5, "{report:#?}");
}

#[test]
fn an_untracked_file_in_the_tree_is_refused() {
    let fx = Fixture::new();
    fx.append("crates/alpha/src/lib.rs", "pub fn beta() {}\n");
    fx.commit("beta");
    fx.write("crates/alpha/src/admit.rs", "pub fn admit() {}\n");
    let err = fx.measure().unwrap_err();
    match err {
        MeterError::Dirty(paths) => {
            assert_eq!(paths, vec!["?? crates/alpha/src/admit.rs".to_string()])
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn an_uncommitted_edit_is_refused() {
    let fx = Fixture::new();
    fx.append("crates/alpha/src/lib.rs", "pub fn beta() {}\n");
    fx.commit("beta");
    fx.append("crates/alpha/src/lib.rs", "pub fn gamma() {}\n");
    let err = fx.measure().unwrap_err();
    match err {
        MeterError::Dirty(paths) => {
            assert_eq!(paths, vec![" M crates/alpha/src/lib.rs".to_string()])
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn a_clean_tree_reproduces_the_old_meter_where_the_old_meter_was_right() {
    let fx = Fixture::new();
    fx.write(
        "crates/alpha/src/lib.rs",
        "mod extra;\n\npub fn alpha() -> u32 {\n    extra::two() - 1\n}\n",
    );
    fx.write(
        "crates/alpha/src/extra.rs",
        "pub fn two() -> u32 {\n    2\n}\n",
    );
    fx.commit("edit");
    let report = fx.measure().unwrap();
    assert_eq!(
        report.total(Category::Production).net(),
        fx.old_meter(&["crates"])
    );
    assert_eq!(report.total(Category::Production).net(), 2 + 3);
}

#[test]
fn the_old_meter_over_billed_a_cfg_test_registration_by_exactly_its_lines() {
    let fx = Fixture::new();
    fx.append("crates/alpha/src/lib.rs", "\n#[cfg(test)]\nmod tests;\n");
    fx.write("crates/alpha/src/tests.rs", TESTS_RS);
    fx.commit("tests");
    let report = fx.measure().unwrap();
    assert_eq!(
        fx.old_meter(&["crates"]) - report.total(Category::Production).net(),
        3
    );
}

#[test]
fn deleting_a_cfg_test_module_counts_zero_on_the_base_side() {
    let fx = Fixture::new();
    fx.append("crates/alpha/src/lib.rs", "\n#[cfg(test)]\nmod tests;\n");
    fx.write("crates/alpha/src/tests.rs", TESTS_RS);
    fx.commit("tests");
    fx.git(&["checkout", "-q", "main"]);
    fx.git(&["merge", "-q", "--ff-only", "packet"]);
    fx.git(&["checkout", "-q", "packet"]);
    fx.write("crates/alpha/src/lib.rs", LIB);
    fx.remove("crates/alpha/src/tests.rs");
    fx.commit("drop tests");
    let report = fx.measure().unwrap();
    assert_eq!(report.total(Category::Production).net(), 0, "{report:#?}");
    assert_eq!(report.total(Category::Tests).net(), -(3 + 6), "{report:#?}");
}

#[test]
fn features_off_by_default_and_unknown_cfgs_are_dropped_platform_cfgs_are_counted() {
    let fx = Fixture::new();
    fx.write(
        "crates/alpha/Cargo.toml",
        "[package]\nname = \"alpha\"\nversion = \"0.0.1\"\nedition = \"2024\"\n\n[features]\nloom = []\nwire = []\ndefault = [\"wire\"]\n",
    );
    fx.write(
        "crates/alpha/src/lib.rs",
        "#[cfg(feature = \"loom\")]\nmod loom_only;\n#[cfg(not(feature = \"loom\"))]\nmod real;\n#[cfg(feature = \"wire\")]\nmod wire;\n#[cfg(unix)]\nmod unix_only;\n#[cfg(kani)]\nmod kani_only;\n#[cfg(all(test, not(feature = \"loom\")))]\nmod tests;\n",
    );
    for file in [
        "loom_only",
        "real",
        "wire",
        "unix_only",
        "kani_only",
        "tests",
    ] {
        fx.write(&format!("crates/alpha/src/{file}.rs"), "pub fn f() {}\n");
    }
    fx.commit("cfgs");
    let report = fx.measure().unwrap();
    let lib_registrations = 2 + 2 + 2;
    let files = 3;
    assert_eq!(
        report.total(Category::Production).net(),
        lib_registrations + files - 3,
        "{report:#?}"
    );
    assert_eq!(
        report.total(Category::Tests).net(),
        (2 + 2 + 2) + 3,
        "{report:#?}"
    );
}

#[test]
fn facade_crates_land_on_their_own_line() {
    let fx = Fixture::new();
    fx.write(
        "Cargo.toml",
        "[workspace]\nresolver = \"3\"\nmembers = [\"crates/alpha\", \"crates/jinnd-api\"]\n",
    );
    fx.write(
        "crates/jinnd-api/Cargo.toml",
        "[package]\nname = \"jinnd-api\"\nversion = \"0.0.1\"\nedition = \"2024\"\n",
    );
    fx.write("crates/jinnd-api/src/lib.rs", "pub struct Kernel;\n");
    fx.commit("facade");
    let report = fx.measure().unwrap();
    assert_eq!(report.total(Category::Production).net(), 0, "{report:#?}");
    assert_eq!(report.total(Category::Facade).net(), 1, "{report:#?}");
    assert_eq!(
        report.total(Category::Contracts).net(),
        4 + 0,
        "member line edit is 1-1, {report:#?}"
    );
}

#[test]
fn rust_under_tools_and_tests_and_scripts_are_outside_every_budget_line() {
    let fx = Fixture::new();
    fx.write("tools/meter/src/main.rs", "fn main() {}\n");
    fx.write("crates/alpha/tests/smoke.rs", "#[test]\nfn smoke() {}\n");
    fx.write("check.sh", "#!/bin/sh\n");
    fx.commit("outside");
    let report = fx.measure().unwrap();
    for category in Category::BUDGET {
        assert_eq!(report.total(category).net(), 0, "{category:?} {report:#?}");
    }
    assert_eq!(report.total(Category::Tools).net(), 1);
    assert_eq!(report.total(Category::Tests).net(), 2);
    assert_eq!(report.total(Category::Other).net(), 1);
}

#[test]
fn a_rename_with_an_edit_bills_the_edit_only() {
    let fx = Fixture::new();
    fx.write(
        "crates/alpha/src/lib.rs",
        "mod extra;\n\npub fn alpha() -> u32 {\n    extra::two() - 1\n}\n",
    );
    fx.write("crates/alpha/src/extra.rs", "pub fn two() -> u32 {\n    2\n}\n\npub fn three() -> u32 {\n    3\n}\n\npub fn four() -> u32 {\n    4\n}\n");
    fx.commit("extra");
    fx.git(&["checkout", "-q", "main"]);
    fx.git(&["merge", "-q", "--ff-only", "packet"]);
    fx.git(&["checkout", "-q", "packet"]);
    fx.write(
        "crates/alpha/src/lib.rs",
        "mod numbers;\n\npub fn alpha() -> u32 {\n    numbers::two() - 1\n}\n",
    );
    fx.remove("crates/alpha/src/extra.rs");
    fx.write("crates/alpha/src/numbers.rs", "pub fn two() -> u32 {\n    2\n}\n\npub fn three() -> u32 {\n    3\n}\n\npub fn four() -> u32 {\n    4\n}\n\npub fn five() -> u32 {\n    5\n}\n");
    fx.commit("rename");
    let report = fx.measure().unwrap();
    let renamed = report
        .files
        .iter()
        .find(|r| r.new.as_deref() == Some("crates/alpha/src/numbers.rs"))
        .unwrap();
    assert_eq!(renamed.old.as_deref(), Some("crates/alpha/src/extra.rs"));
    assert_eq!(renamed.delta.net(), 4, "{report:#?}");
    assert_eq!(report.total(Category::Production).net(), 4, "{report:#?}");
}
