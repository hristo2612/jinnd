//! Builds the M1 demo kit from source (no artifact binary is ever checked
//! in): each plugin compiles for wasm32-unknown-unknown, is encoded to a
//! component in-process, hashed, and pinned.
//!
//! Usage:
//!   demo-builder kit <demo-root>              — build all three plugins into
//!       <demo-root>/artifacts/ (with .sha256 sidecars) and write
//!       <demo-root>/profile.json with the computed pins.
//!   demo-builder clock <v1|v2|broken> <artifacts-dir> — build the named
//!       clock variant over <artifacts-dir>/clock.wasm (+ sidecar): the
//!       Mode-1 hot-swap trigger the runbook drives.

use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

fn plugin_dir(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("../plugins/{name}"))
}

/// The build must use a rustc whose wasm32-unknown-unknown std is installed;
/// fall back to the rustup toolchain's own binaries when PATH shadows it
/// (same discipline as the kernel's fixture builder).
fn candidates() -> Vec<(PathBuf, Option<PathBuf>)> {
    let mut found = vec![(PathBuf::from("cargo"), None)];
    if let Ok(output) = Command::new("rustup").args(["which", "rustc"]).output() {
        if output.status.success() {
            if let Ok(path) = String::from_utf8(output.stdout) {
                let rustc = PathBuf::from(path.trim());
                let cargo = rustc.with_file_name("cargo");
                if cargo.exists() {
                    found.push((cargo, Some(rustc)));
                }
            }
        }
    }
    found
}

fn build_core(name: &str, features: &[&str]) -> Vec<u8> {
    let dir = plugin_dir(name);
    let module = format!(
        "target/wasm32-unknown-unknown/release/demo_{}.wasm",
        name.replace('-', "_")
    );
    let artifact = dir.join(module);
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
        for feature in features {
            command.args(["--features", feature]);
        }
        if let Some(rustc) = rustc {
            command.env("RUSTC", rustc);
        }
        match command.output() {
            Ok(output) if output.status.success() => {
                return std::fs::read(&artifact).unwrap_or_else(|error| {
                    panic!("built but {} is unreadable: {error}", artifact.display())
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
    panic!("no toolchain could build {name}:\n{}", failures.join("\n---\n"));
}

fn component(name: &str, features: &[&str]) -> (Vec<u8>, String) {
    let core = build_core(name, features);
    let bytes = wit_component::ComponentEncoder::default()
        .module(&core)
        .unwrap_or_else(|error| panic!("core module rejected: {error:#}"))
        .validate(true)
        .encode()
        .unwrap_or_else(|error| panic!("component encoding failed: {error:#}"));
    let hash = format!("{:x}", Sha256::digest(&bytes));
    (bytes, hash)
}

fn write_artifact(dir: &Path, name: &str, bytes: &[u8], hash: &str) {
    std::fs::create_dir_all(dir).expect("artifacts dir");
    let file = dir.join(format!("{name}.wasm"));
    std::fs::write(&file, bytes).expect("artifact write");
    std::fs::write(dir.join(format!("{name}.wasm.sha256")), hash).expect("sidecar write");
    println!("{} {}", hash, file.display());
}

fn profile(clock: &str, greeter: &str, scribe: &str) -> String {
    let document = serde_json::json!({ "entries": [
        { "id": "clock", "package": "demo/clock", "hash": clock,
          "config": { "grants": ["demo:clock"], "data": "" } },
        { "id": "scribe", "package": "demo/scribe", "hash": scribe,
          "config": { "grants": ["demo:announce", "jinn:fs"], "data": "journal.txt" } },
        { "id": "greeter", "package": "demo/greeter", "hash": greeter,
          "config": { "grants": ["demo:greeting", "demo:clock"], "data": "world" } },
    ]});
    serde_json::to_string_pretty(&document).expect("profile encoding")
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("kit") => {
            let root = PathBuf::from(args.get(2).expect("usage: demo-builder kit <demo-root>"));
            let artifacts = root.join("artifacts");
            let (clock, clock_hash) = component("clock", &[]);
            let (greeter, greeter_hash) = component("greeter", &[]);
            let (scribe, scribe_hash) = component("scribe", &[]);
            write_artifact(&artifacts, "clock", &clock, &clock_hash);
            write_artifact(&artifacts, "greeter", &greeter, &greeter_hash);
            write_artifact(&artifacts, "scribe", &scribe, &scribe_hash);
            std::fs::create_dir_all(&root).expect("demo root");
            std::fs::write(
                root.join("profile.json"),
                profile(&clock_hash, &greeter_hash, &scribe_hash),
            )
            .expect("profile write");
            println!("profile {}", root.join("profile.json").display());
        }
        Some("clock") => {
            let variant = args.get(2).map(String::as_str).unwrap_or("v1");
            let dir = PathBuf::from(
                args.get(3)
                    .expect("usage: demo-builder clock <v1|v2|broken> <artifacts-dir>"),
            );
            let features: &[&str] = match variant {
                "v1" => &[],
                "v2" => &["v2"],
                "broken" => &["v2", "broken-restore"],
                other => panic!("unknown clock variant {other}"),
            };
            let (bytes, hash) = component("clock", features);
            write_artifact(&dir, "clock", &bytes, &hash);
        }
        _ => {
            eprintln!("usage: demo-builder kit <demo-root> | clock <v1|v2|broken> <artifacts-dir>");
            std::process::exit(2);
        }
    }
}
