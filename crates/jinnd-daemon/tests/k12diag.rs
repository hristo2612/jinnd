//! TEMPORARY (M2-K12 round 1): a transcript of the two CI-red keystore
//! scenarios. Prints, never asserts — deleted before the packet lands.

#[path = "support/mod.rs"]
mod support;

use std::path::{Path, PathBuf};

use jinnd_daemon::{Daemon, DaemonPaths, MasterKeySource};

const PASSPHRASE: &[u8] = b"operator-passphrase-0xFEEDFACE";

fn tree(root: &Path, depth: usize) {
    let mut names: Vec<PathBuf> = std::fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .collect();
    names.sort();
    for path in names {
        let pad = "  ".repeat(depth);
        if path.is_dir() {
            println!("{pad}{}/", path.display());
            tree(&path, depth + 1);
        } else {
            let len = std::fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
            println!("{pad}{} ({len} bytes)", path.display());
        }
    }
}

fn paths(home: &Path, grants: serde_json::Value, mode: &str) -> DaemonPaths {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    let profile = serde_json::json!({
        "entries": [{
            "id": "holder",
            "package": "demo/counter-plugin",
            "version": "0.0.1",
            "hash": hash,
            "config": { "grants": grants, "data": mode },
        }]
    });
    let profile_path = home.join("profile.json");
    std::fs::write(
        &profile_path,
        serde_json::to_string_pretty(&profile).unwrap_or_else(|error| panic!("{error}")),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    DaemonPaths {
        profile: profile_path,
        ledger: home.join("ledger.sqlite"),
        artifacts: home.join("artifacts"),
        data: home.join("data"),
    }
}

async fn transcript(label: &str, grants: serde_json::Value) {
    println!("\n================ {label} ================");
    let home = std::env::temp_dir().join(format!("jinnd-k12diag-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(home.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    println!("temp_dir = {}", std::env::temp_dir().display());
    println!("home     = {}", home.display());
    let paths = paths(&home, grants, "keystore");
    let daemon = match Daemon::open_with(paths.clone(), MasterKeySource::Passphrase(PASSPHRASE.to_vec())) {
        Ok(daemon) => daemon,
        Err(error) => {
            println!("open FAILED: {error:?}");
            return;
        }
    };
    match daemon.boot().await {
        Ok(report) => println!("boot report: {report:?}"),
        Err(error) => println!("boot FAILED: {error:?}"),
    }
    let fiber = daemon.entry_fiber("holder");
    println!("entry fiber = {fiber:?}");
    println!("fiber state = {:?}", fiber.and_then(|f| daemon.fiber_state(f)));
    println!("fs_effects       = {:?}", daemon.fs_effects());
    println!("keystore_effects = {:?}", daemon.keystore_effects());
    println!("keystore.out exists = {}", paths.data.join("keystore.out").exists());
    println!("secrets.bin exists  = {}", paths.keystore().join("secrets.bin").exists());
    match daemon.ledger_events().await {
        Ok(records) => {
            println!("---- ledger ({}) ----", records.len());
            for record in records {
                println!("  {:?} entry={:?}", record.kind, record.entry);
            }
        }
        Err(error) => println!("ledger read FAILED: {error:?}"),
    }
    let _ = daemon.shutdown().await;
    drop(daemon);
    println!("---- tree under home ----");
    tree(&home, 1);
    let other = Daemon::open_with(
        paths.clone(),
        MasterKeySource::Passphrase(b"a-different-passphrase".to_vec()),
    );
    println!("wrong-passphrase open is_err = {}", other.is_err());
    let none = Daemon::open_with(paths, MasterKeySource::Absent);
    println!("absent-source  open is_err = {}", none.is_err());
    let _ = std::fs::remove_dir_all(&home);
}

#[tokio::test]
async fn k12_transcript() {
    transcript(
        "fs-first",
        serde_json::json!(["jinn:fs", { "contract": "jinn:keystore", "scope": ["engines/"] }]),
    )
    .await;
    transcript(
        "keystore-first",
        serde_json::json!([{ "contract": "jinn:keystore", "scope": ["engines/"] }, "jinn:fs"]),
    )
    .await;
}
