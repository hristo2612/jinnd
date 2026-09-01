//! M2-K21's NO-BYPASS proof, over the provider's SOURCE rather than its
//! behaviour: the M2-K15 form. A release build carries no seam that the
//! tests cannot see, asserted by scanning every non-test file of the
//! module off disk. Split from `tests.rs` at this seam (R10: the scan is
//! independent of the provider's behaviour matrix, which stays there).

use std::path::Path;

/// Every non-test source file of this provider, off disk — a scan that
/// must be extended by hand stops covering the module the day someone
/// forgets.
fn provider_sources() -> Vec<(String, String)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/daemon");
    let mut sources = vec![("auth_cap.rs".to_owned(), read(&dir.join("auth_cap.rs")))];
    for entry in std::fs::read_dir(dir.join("auth_cap")).unwrap_or_else(|error| panic!("{error}")) {
        let path = entry.unwrap_or_else(|error| panic!("{error}")).path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_owned();
        if name.ends_with("tests.rs") {
            continue;
        }
        sources.push((format!("auth_cap/{name}"), read(&path)));
    }
    sources
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

/// NO BYPASS, asserted over the provider's source rather than inspected:
/// no environment read, no build-flag or debug-only gate, no `cfg(not
/// (test))` twin of a test path, and every `cfg(test)` guards a test
/// MODULE and nothing else — so there is no seam a release build could
/// carry. The needles are assembled from fragments so this file never
/// spells one out, even in prose.
#[test]
fn the_provider_has_no_off_switch_in_its_source() {
    let forbidden = [
        concat!("env::", "var"),
        concat!("var", "_os("),
        concat!("option_", "env!"),
        concat!("env!", "("),
        concat!("cfg(", "feature"),
        concat!("cfg(", "debug_assertions"),
        concat!("cfg(", "not(test"),
        concat!("cfg_", "attr"),
    ];
    let sources = provider_sources();
    assert!(!sources.is_empty(), "the walk found the provider");
    for (path, source) in &sources {
        for needle in forbidden {
            assert!(
                !source.contains(needle),
                "{path} names {needle:?}: the check must have no off switch"
            );
        }
        let lines: Vec<&str> = source.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if line.trim() != concat!("#[cfg(", "test)]") {
                continue;
            }
            let guarded = lines
                .get(index + 1)
                .map(|next| next.trim())
                .unwrap_or_default();
            assert!(
                guarded.starts_with("mod ") && guarded.ends_with("_tests;")
                    || guarded == "mod tests;",
                "{path}:{}: a test guard on {guarded:?} — only test modules are test-only",
                index + 2
            );
        }
    }
}
