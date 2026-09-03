//! Builds the checked-in guest fixture source into a component. The
//! verifier owns this driver; no component binary is checked in.

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/counter-plugin")
}

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
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_TARGET_DIR");
        if let Some(rustc) = rustc {
            command.env("RUSTC", rustc);
        }
        match command.output() {
            Ok(output) if output.status.success() => {
                return std::fs::read(&artifact)
                    .unwrap_or_else(|error| panic!("fixture artifact: {error}"));
            }
            Ok(output) => failures.push(format!(
                "{}: {}",
                cargo.display(),
                String::from_utf8_lossy(&output.stderr)
            )),
            Err(error) => failures.push(format!("{}: {error}", cargo.display())),
        }
    }
    panic!("fixture build failed:\n{}", failures.join("\n---\n"));
}

pub(crate) fn pinned() -> (Vec<u8>, String) {
    static COMPONENT: OnceLock<Vec<u8>> = OnceLock::new();
    let bytes = COMPONENT
        .get_or_init(|| {
            wit_component::ComponentEncoder::default()
                .module(&build_core_module())
                .unwrap_or_else(|error| panic!("fixture module: {error:#}"))
                .validate(true)
                .encode()
                .unwrap_or_else(|error| panic!("fixture component: {error:#}"))
        })
        .clone();
    let hash = jinnd_wasm::hex_digest(&bytes);
    (bytes, hash)
}
