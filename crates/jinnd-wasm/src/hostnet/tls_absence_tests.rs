//! M2-K15: verification has no off switch, and the one test-only seam is
//! test-only — ASSERTED over the crate's own source, never inspected.
//!
//! "I checked and there is no bypass" is not evidence; a scan that reads
//! every file in the crate is. Both pins read the source off disk rather
//! than naming files one by one, because a scan that must be extended by
//! hand to cover a new file stops covering the crate the day someone
//! forgets.

/// Every Rust source file in this crate, as `(path, text)`.
///
/// Read off disk rather than named one by one: a scan that must be
/// extended by hand to cover a new file is a scan that stops covering the
/// crate the day someone forgets. Tests run from the source checkout, so
/// `CARGO_MANIFEST_DIR` is that crate.
fn sources() -> Vec<(String, String)> {
    fn walk(directory: &std::path::Path, into: &mut Vec<(String, String)>) {
        let entries = std::fs::read_dir(directory)
            .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|error| panic!("entry: {error}"))
                .path();
            if path.is_dir() {
                walk(&path, into);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
                into.push((path.display().to_string(), text));
            }
        }
    }
    let mut sources = Vec::new();
    walk(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut sources,
    );
    assert!(sources.len() > 20, "the walk found the crate");
    sources
}

/// CERTIFICATE VERIFICATION HAS NO OFF SWITCH, and this asserts it rather
/// than inspecting for it.
///
/// rustls puts every way to weaken verification behind two named doors:
/// the escape-hatch configuration accessor, and a hand-written server
/// certificate verifier. Neither appears in this crate, in production code
/// or in test code, so there is no path from a profile, an environment
/// variable, or a plugin to a client that skips verification: the test
/// certificates are trusted because a test ANCHOR was added, which is a
/// different thing entirely.
///
/// The needles are assembled from fragments, and this file never spells
/// one out even in prose — a scan a comment can defeat is not a scan.
#[test]
fn certificate_verification_has_no_off_switch_anywhere_in_this_crate() {
    // The API tokens, never the English words: a scan that fires on the
    // word "dangerous" in an unrelated doc comment is a scan nobody keeps.
    let forbidden = [
        concat!("dang", "erous()"),
        concat!("Dang", "erousClientConfig"),
        concat!("Server", "CertVerifier"),
        concat!("Server", "CertVerified"),
        concat!("accept_invalid", "_certs"),
        concat!("with_custom_certificate", "_verifier"),
    ];
    for (path, source) in sources() {
        for needle in forbidden {
            assert!(
                !source.contains(needle),
                "{path} names {needle:?}: verification must have no off switch"
            );
        }
    }
}

/// The ONE test-only seam — the extra trust anchor — is `#[cfg(test)]` on
/// every occurrence, so it cannot exist in a release build. Asserted by
/// reading the source, because "I checked" is not evidence.
#[test]
fn the_extra_anchor_seam_is_cfg_test_at_every_occurrence() {
    let seam = concat!("extra_", "anchors");
    let mut found = 0;
    for (path, source) in sources() {
        let lines: Vec<&str> = source.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if !line.contains(seam) || line.trim_start().starts_with("//") {
                continue;
            }
            found += 1;
            let guard = lines[..index]
                .iter()
                .rev()
                .find(|earlier| !earlier.trim().is_empty())
                .map(|earlier| earlier.trim())
                .unwrap_or_default();
            assert_eq!(
                guard,
                "#[cfg(test)]",
                "{path}:{}: the anchor seam is not test-only",
                index + 1
            );
        }
    }
    assert_eq!(found, 2, "the seam is its declaration and its one call");
}
