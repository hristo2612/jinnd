//! Demo-kit plumbing for the headless acceptance test: builds the kit
//! through demo/builder (the same tool the operator runbook drives) into a
//! fresh machine-independent root, cleaned on drop.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

/// Removes the demo root on drop, pass or fail.
pub struct Cleanup(pub PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A fresh, unique, machine-independent demo root under the system temp dir.
pub fn fresh_root() -> (PathBuf, Cleanup) {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let unique = format!(
        "jinnd-demo-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::SeqCst)
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).unwrap_or_else(|error| panic!("demo root: {error:?}"));
    (root.clone(), Cleanup(root))
}

/// Polls `check` until it holds, panicking after a generous deadline — the
/// observation lane for changes that arrive through the real file watcher.
pub async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while !check() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

fn builder_manifest() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../demo/builder/Cargo.toml")
}

fn run_builder(args: &[&str]) {
    let manifest = builder_manifest();
    let output = Command::new("cargo")
        .arg("run")
        .arg("--quiet")
        .arg("--manifest-path")
        .arg(&manifest)
        .arg("--")
        .args(args)
        // The kernel workspace's flags and target dir must not leak into the
        // standalone builder (it is not a workspace member by design).
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_TARGET_DIR")
        .output()
        .unwrap_or_else(|error| panic!("demo builder runs: {error:?}"));
    assert!(
        output.status.success(),
        "demo builder failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Builds all three plugins and the pinned profile into `root`.
pub fn build_kit(root: &Path) {
    run_builder(&["kit", root.to_str().unwrap_or_else(|| panic!("utf-8 root"))]);
}

/// Builds the named clock variant over `<artifacts>/clock.wasm` + sidecar.
pub fn clock_variant(variant: &str, artifacts: &Path) {
    run_builder(&[
        "clock",
        variant,
        artifacts
            .to_str()
            .unwrap_or_else(|| panic!("utf-8 artifacts dir")),
    ]);
}

/// Rewrites one entry's `data` config field in the profile document.
pub fn edit_profile_data(profile: &Path, entry: &str, data: &str) {
    let text = std::fs::read_to_string(profile)
        .unwrap_or_else(|error| panic!("profile readable: {error:?}"));
    let mut document: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|error| panic!("profile is JSON: {error:?}"));
    let entries = document["entries"]
        .as_array_mut()
        .unwrap_or_else(|| panic!("entries array"));
    let found = entries
        .iter_mut()
        .find(|candidate| candidate["id"] == entry)
        .unwrap_or_else(|| panic!("entry present"));
    found["config"]["data"] = serde_json::Value::String(data.to_owned());
    std::fs::write(
        profile,
        serde_json::to_string_pretty(&document)
            .unwrap_or_else(|error| panic!("encodes: {error:?}")),
    )
    .unwrap_or_else(|error| panic!("profile writable: {error:?}"));
}

/// Removes one entry from the profile document.
pub fn remove_profile_entry(profile: &Path, entry: &str) {
    let text = std::fs::read_to_string(profile)
        .unwrap_or_else(|error| panic!("profile readable: {error:?}"));
    let mut document: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|error| panic!("profile is JSON: {error:?}"));
    let entries = document["entries"]
        .as_array_mut()
        .unwrap_or_else(|| panic!("entries array"));
    entries.retain(|candidate| candidate["id"] != entry);
    std::fs::write(
        profile,
        serde_json::to_string_pretty(&document)
            .unwrap_or_else(|error| panic!("encodes: {error:?}")),
    )
    .unwrap_or_else(|error| panic!("profile writable: {error:?}"));
}
