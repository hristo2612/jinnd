//! M2-K8 acceptance (harness #24 and the round-2 vault ruling): a
//! read-only keystore grant refuses put/delete and a read-only fs grant
//! refuses write/append/remove, each a ledgered scope refusal with the
//! entry Active (the guest saw the typed denial); and the data root alone
//! cannot decrypt the store — a daemon opened with a different passphrase,
//! or none, refuses at assembly, and the passphrase is nowhere on disk.

use jinnd_daemon::{Daemon, MasterKeySource};

use super::{active, booted, bytes_under, contains, events, home, paths, scope_refusals};

#[tokio::test]
async fn a_read_only_keystore_grant_refuses_put_and_delete_on_the_record() {
    let home = home("read-only");
    let paths = paths(
        &home,
        serde_json::json!([{ "contract": "jinn:keystore", "scope": ["engines/"], "ops": ["get", "list"] }]),
        "keystore-readonly",
    );
    let daemon = booted(paths).await;
    assert!(active(&daemon), "the guest saw typed denials, not a fault");
    let refused = scope_refusals(&events(&daemon).await, "jinn:keystore");
    assert_eq!(refused.len(), 2, "{refused:?}");
    assert!(refused[0].contains("put") && refused[1].contains("delete"));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

#[tokio::test]
async fn a_read_only_fs_grant_refuses_write_append_and_remove_on_the_record() {
    let home = home("fs-read-only");
    let paths = paths(
        &home,
        serde_json::json!([{ "contract": "jinn:fs", "ops": ["read", "list", "meta"] }]),
        "fs-readonly",
    );
    let daemon = booted(paths.clone()).await;
    assert!(active(&daemon), "the guest saw typed denials, not a fault");
    assert!(!paths.data.join("doc.txt").exists(), "nothing landed");
    let refused = scope_refusals(&events(&daemon).await, "jinn:fs");
    assert_eq!(refused.len(), 3, "{refused:?}");
    assert!(
        refused[0].contains("write")
            && refused[1].contains("append")
            && refused[2].contains("remove")
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Round-2 vault ruling, red-first: the data root alone cannot decrypt.
/// The store written under one passphrase refuses a daemon assembling
/// under another, or under none; the passphrase bytes are nowhere under
/// the home (no key material beside the ciphertext).
#[tokio::test]
async fn the_data_root_alone_cannot_decrypt_the_store() {
    let home = home("passphrase");
    let paths = paths(
        &home,
        serde_json::json!(["jinn:fs", { "contract": "jinn:keystore", "scope": ["engines/"] }]),
        "keystore",
    );
    let daemon = booted(paths.clone()).await;
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    drop(daemon);
    let on_disk = bytes_under(&home.0);
    assert!(
        !contains(&on_disk, super::PASSPHRASE),
        "the passphrase is never written under the home"
    );
    assert!(
        !paths
            .data
            .with_extension("keystore")
            .join("master.key")
            .exists(),
        "no key material beside the ciphertext"
    );
    // TEMPORARY M2-K12 diagnostic — reverted before the packet lands.
    println!("K12 home = {}", home.0.display());
    println!("K12 keystore dir = {}", paths.keystore().display());
    for entry in walkdir(&home.0) {
        println!("K12   {entry}");
    }
    println!(
        "K12 secrets.bin exists = {}",
        paths.keystore().join("secrets.bin").exists()
    );
    let other = Daemon::open_with(
        paths.clone(),
        MasterKeySource::Passphrase(b"a-different-passphrase".to_vec()),
    );
    println!("K12 wrong-passphrase open = {other:?}");
    assert!(
        other.is_err(),
        "a different passphrase does not open the store"
    );
    let none = Daemon::open_with(paths.clone(), MasterKeySource::Absent);
    assert!(none.is_err(), "no passphrase does not open the store");
    let again = booted(paths).await;
    assert!(active(&again), "the right passphrase reopens the store");
    again
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// TEMPORARY M2-K12 diagnostic — reverted before the packet lands.
fn walkdir(root: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(root).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.push(format!("{}/", path.display()));
            out.extend(walkdir(&path));
        } else {
            let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            out.push(format!("{} ({len} bytes)", path.display()));
        }
    }
    out
}
