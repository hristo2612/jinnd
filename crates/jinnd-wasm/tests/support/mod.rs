//! Builds the checked-in fixture SOURCE into a component, once per test run.
//! No artifact binary is ever checked in (M1-P8 round protocol): the fixture
//! compiles for wasm32-unknown-unknown and is encoded to a component
//! in-process by `wit-component` — the same encoder family wasmtime's own
//! wit-parser tracks.

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/counter-plugin")
}

/// The build must use a rustc whose wasm32-unknown-unknown std is installed.
/// On CI and plain rustup machines that is `cargo` itself; on a machine whose
/// PATH shadows rustup with a distribution rustc (no wasm std), fall back to
/// the rustup toolchain's own binaries.
fn candidates() -> Vec<(PathBuf, Option<PathBuf>)> {
    let mut found = vec![(PathBuf::from("cargo"), None)];
    if let Ok(output) = Command::new("rustup").args(["which", "rustc"]).output()
        && output.status.success()
        && let Ok(path) = String::from_utf8(output.stdout)
    {
        let rustc = PathBuf::from(path.trim());
        let cargo = rustc.with_file_name("cargo");
        if cargo.exists() {
            found.push((cargo, Some(rustc)));
        }
    }
    found
}

fn build_core_module() -> Vec<u8> {
    let dir = fixture_dir();
    let artifact = dir.join("target/wasm32-unknown-unknown/release/counter_plugin.wasm");
    let mut failures = Vec::new();
    for (cargo, rustc) in candidates() {
        let mut command = Command::new(&cargo);
        command
            .args(["build", "--release", "--target", "wasm32-unknown-unknown"])
            .current_dir(&dir)
            // The kernel workspace's flags and target dir must not leak into
            // the guest build (it is not a workspace member by design).
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_TARGET_DIR");
        if let Some(rustc) = rustc {
            command.env("RUSTC", rustc);
        }
        match command.output() {
            Ok(output) if output.status.success() => {
                return std::fs::read(&artifact).unwrap_or_else(|error| {
                    panic!(
                        "fixture built but {} is unreadable: {error}",
                        artifact.display()
                    )
                });
            }
            Ok(output) => failures.push(format!(
                "{}: {}",
                cargo.display(),
                String::from_utf8_lossy(&output.stderr)
            )),
            Err(error) => failures.push(format!("{}: {error}", cargo.display())),
        }
    }
    panic!(
        "no toolchain could build the fixture:\n{}",
        failures.join("\n---\n")
    );
}

/// The fixture as component bytes, built and encoded once per process.
pub fn fixture_component() -> &'static [u8] {
    static COMPONENT: OnceLock<Vec<u8>> = OnceLock::new();
    COMPONENT.get_or_init(|| {
        let core = build_core_module();
        wit_component::ComponentEncoder::default()
            .module(&core)
            .unwrap_or_else(|error| panic!("core module rejected: {error:#}"))
            .validate(true)
            .encode()
            .unwrap_or_else(|error| panic!("component encoding failed: {error:#}"))
    })
}

/// The component and its true pin (Law 5), computed the honest way.
pub fn pinned_fixture() -> (Vec<u8>, String) {
    let bytes = fixture_component().to_vec();
    let hash = jinnd_wasm::hex_digest(&bytes);
    (bytes, hash)
}
