//! Caller-authored `Serialize` is contained at the kernel boundary (M1-P6c
//! round 2; R11, PLA-270): a panicking or failing serializer is an honest
//! error, run outside the persist permit, never an escape (split from
//! `amend.rs` by the 300-line file cap, R10).

#![cfg(not(feature = "loom"))]

mod common;

use std::sync::Arc;

use common::probe::{Probe, disk_entry, probe_entry as entry, probe_loader, scratch_path, stated};
use common::{Grab, id};
use jinnd_api::{ErrorCode, Profile};
use jinnd_loader::{Document, FileStore};

/// The named entry's typed committed config.
fn committed_entry_of<C: Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static>(
    loader: &jinnd_loader::Loader,
    name: &str,
) -> C {
    let committed: Profile<C> = loader.persisted().grab();
    committed
        .entries
        .iter()
        .find(|entry| entry.id == id(name))
        .cloned()
        .grab()
        .config
}

/// A config whose `Serialize` panics on a designated value: caller-authored
/// code the save path must contain at the kernel boundary (R11) — and must
/// never run inside the persist permit's span (R1, PLA-270).
#[derive(Clone, Debug)]
struct PanickySerialize {
    value: u32,
    panic_on: u32,
}

impl PartialEq for PanickySerialize {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl serde::Serialize for PanickySerialize {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        assert!(
            self.value != self.panic_on,
            "the config panics while rendering"
        );
        serializer.serialize_u32(self.value)
    }
}

fn panicky(value: u32) -> PanickySerialize {
    PanickySerialize { value, panic_on: 9 }
}

/// Round-2 blocker: a panicking caller-authored serializer must surface as an
/// honest per-entry error from the amendment — never escape the kernel
/// boundary as a task panic (R11).
#[tokio::test]
async fn a_caller_authored_serializer_panic_is_contained() {
    let (loader, log) = probe_loader::<PanickySerialize>(|config| config.value, Probe::default());
    let loader = Arc::new(loader);
    let path = scratch_path();
    loader.attach_store::<PanickySerialize>(FileStore::new(path.clone()), Document::default());
    loader
        .reconcile(Profile {
            entries: vec![entry("one", panicky(1))],
        })
        .await
        .grab();

    // Spawned so an escaping panic reads as a JoinError, not a dead suite.
    let amended = tokio::spawn({
        let loader = Arc::clone(&loader);
        async move { loader.update_entry(&id("one"), panicky(9)).await }
    })
    .await;
    let outcome = match amended {
        Ok(outcome) => outcome,
        Err(join) => panic!("caller-authored Serialize escaped the kernel boundary: {join}"),
    };
    let Err(refusal) = outcome else {
        panic!("a panicking serializer must be an honest error");
    };
    assert_eq!(refusal.code, ErrorCode::InvalidProfile);
    assert!(
        refusal.message.contains("panic"),
        "the error names the panic, got: {}",
        refusal.message
    );

    // Nothing was committed or staged anywhere: both views at the prior
    // state, the runtime never offered the change.
    assert_eq!(
        committed_entry_of::<PanickySerialize>(&loader, "one").value,
        1
    );
    assert_eq!(disk_entry(&path, "one").await.config, 1);
    assert!(stated(&log).is_empty(), "the runtime observed nothing");

    // The loader is not poisoned: a serializable amendment still lands.
    loader.update_entry(&id("one"), panicky(2)).await.grab();
    assert_eq!(disk_entry(&path, "one").await.config, 2);
    let _ = std::fs::remove_dir_all(path.parent().grab());
}

/// The reconcile-path twin: a panicking serializer fails the commit honestly
/// — nothing lands on disk, no runtime spawns, no panic escapes (R11).
#[tokio::test]
async fn a_serializer_panic_during_reconcile_commits_nothing() {
    let (loader, _log) = probe_loader::<PanickySerialize>(|config| config.value, Probe::default());
    let loader = Arc::new(loader);
    let path = scratch_path();
    loader.attach_store::<PanickySerialize>(FileStore::new(path.clone()), Document::default());

    let reconciled = tokio::spawn({
        let loader = Arc::clone(&loader);
        async move {
            loader
                .reconcile(Profile {
                    entries: vec![entry("one", panicky(9))],
                })
                .await
        }
    })
    .await;
    let outcome = match reconciled {
        Ok(outcome) => outcome,
        Err(join) => panic!("caller-authored Serialize escaped the kernel boundary: {join}"),
    };
    let Err(refusal) = outcome else {
        panic!("a panicking serializer must fail the commit");
    };
    assert_eq!(refusal.code, ErrorCode::InvalidProfile);
    assert!(FileStore::new(path.clone()).load().await.grab().is_none());
    assert!(loader.entry_fiber(&id("one")).is_none());
    let _ = std::fs::remove_dir_all(path.parent().grab());
}
