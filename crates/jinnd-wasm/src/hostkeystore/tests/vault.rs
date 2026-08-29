//! Round-2 ruling 1, red-first: the data root alone cannot decrypt. The
//! master key is never a file beside the ciphertext — a store sealed under
//! one passphrase refuses another and refuses no source; the passphrase is
//! nowhere on disk; and without a source the first mutation is the typed
//! refusal naming the variables while reads of an absent store answer.

use std::sync::{Arc, Mutex};

use jinnd_api::{ErrorCode, FiberId};

use super::{
    Broker, GrantScope, KEYSTORE_CONTRACT, LedgerSink, MasterKeySource, PASSPHRASE, Recording,
    SECRET, contains, home, open_with, passphrase, put_wire, rig,
};

fn files_under(dir: &std::path::Path) -> Vec<String> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            names.extend(files_under(&path));
        } else {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    names.sort();
    names
}

#[tokio::test]
async fn the_data_root_alone_cannot_decrypt() {
    let rig = rig("vault-root");
    rig.ok("put", put_wire("engines/openai", SECRET)).await;
    let (disk, _) = rig.on_the_record();
    assert!(
        !contains(&disk, PASSPHRASE),
        "the passphrase is never on disk"
    );
    assert!(!contains(&disk, SECRET));
    let files = files_under(&rig.home.0.join("keystore"));
    assert!(
        !files.iter().any(|name| name == "master.key"),
        "no key material beside the ciphertext: {files:?}"
    );
    assert!(files.contains(&"secrets.bin".to_owned()) && files.contains(&"salt".to_owned()));
    drop(rig.keystore);

    let silent = Arc::new(Recording(Mutex::new(Vec::new())));
    let other = open_with(
        &rig.home,
        &silent,
        MasterKeySource::Passphrase(b"another-passphrase".to_vec()),
    );
    assert!(
        other.is_err(),
        "a different passphrase does not open the store"
    );
    let none = open_with(&rig.home, &silent, MasterKeySource::Absent);
    assert!(none.is_err(), "no source does not open the store");
    let reopened = open_with(&rig.home, &silent, passphrase())
        .unwrap_or_else(|error| panic!("reopen: {error:?}"));
    assert_eq!(
        reopened.effects().len(),
        1,
        "the right passphrase reopens store and journal"
    );
}

#[tokio::test]
async fn without_a_source_reads_answer_and_the_first_mutation_refuses_typed() {
    let home = home("vault-absent");
    let ledger = Arc::new(Recording(Mutex::new(Vec::new())));
    let broker = Arc::new(Broker::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>));
    let keystore = open_with(&home, &ledger, MasterKeySource::Absent)
        .unwrap_or_else(|error| panic!("an absent store opens without a key: {error:?}"));
    keystore
        .register(&broker)
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    let guest = broker.register_peer(Some(FiberId(7)));
    broker.grant_with(
        guest,
        KEYSTORE_CONTRACT,
        GrantScope::Keys(vec!["engines/".to_owned()]),
    );
    let call = |op: &'static str, payload: Vec<u8>| {
        let broker = Arc::clone(&broker);
        async move { broker.dispatch(guest, KEYSTORE_CONTRACT, op, payload).await }
    };
    assert_eq!(
        call("get", b"engines/none".to_vec())
            .await
            .map_err(|e| e.code),
        Err(ErrorCode::NotFound)
    );
    let refused = call("put", put_wire("engines/openai", SECRET))
        .await
        .err()
        .unwrap_or_else(|| panic!("no source: the mutation refuses"));
    assert_eq!(refused.code, ErrorCode::EffectFailed);
    assert!(
        refused.message.contains("JINND_KEYSTORE_PASSPHRASE"),
        "{}",
        refused.message
    );
    assert!(!contains(refused.message.as_bytes(), SECRET));
    assert!(
        !home.0.join("keystore/secrets.bin").exists(),
        "nothing sealed without a key"
    );
}
