//! Runtime-originated amendments: the runtime is offered a change first, and
//! the document follows only after the runtime accepted — a rejected or
//! unpersistable change leaves both views of the one truth at their prior
//! state (LAW §3 bidirectional persistence).

#![cfg(not(feature = "loom"))]

mod common;

use std::fmt;
use std::sync::Arc;

use common::probe::{
    Probe, broken_path, committed_entry, disk_entry, probe_entry as entry, probe_loader,
    scratch_path, stated,
};
use common::{Grab, id};
use jinnd_api::{ErrorCode, Profile};
use jinnd_loader::{Document, FileStore};

#[tokio::test]
async fn a_rejected_update_leaves_both_views_at_the_prior_state() {
    let (loader, log) = probe_loader::<u32>(
        |value| *value,
        Probe {
            reject: Some(9),
            ..Probe::default()
        },
    );
    let path = scratch_path();
    loader.attach_store::<u32>(FileStore::new(path.clone()), Document::default());
    loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();

    let Err(refusal) = loader.update_entry(&id("one"), 9u32).await else {
        panic!("the runtime's rejection must refuse the update");
    };
    assert_eq!(refusal.code, ErrorCode::InvalidProfile);

    // Both views of the one truth stayed at the prior state.
    assert_eq!(committed_entry(&loader, "one").config, 1);
    assert_eq!(disk_entry(&path, "one").await.config, 1);
    // The rejected value was never observed as a stated config.
    assert!(!stated(&log).contains(&9));
    let _ = std::fs::remove_dir_all(path.parent().grab());
}

#[tokio::test]
async fn a_failed_write_back_withdraws_the_staged_config() {
    let (loader, log) = probe_loader::<u32>(|value| *value, Probe::default());
    let path = scratch_path();
    loader.attach_store::<u32>(FileStore::new(path.clone()), Document::default());
    loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();

    // Every save through this store fails: its directory does not exist.
    loader.attach_store::<u32>(FileStore::new(broken_path()), Document::default());

    assert!(loader.update_entry(&id("one"), 2u32).await.is_err());
    // The committed view stayed at the prior state...
    assert_eq!(committed_entry(&loader, "one").config, 1);
    // ...and the staged config was withdrawn: offered 2, restored to 1.
    assert_eq!(stated(&log), vec![2, 1]);
    let _ = std::fs::remove_dir_all(path.parent().grab());
}

#[tokio::test]
async fn a_failed_withdrawal_records_the_divergence() {
    // The probe accepts the staged config but rejects the withdrawal to 1.
    let (loader, log) = probe_loader::<u32>(
        |value| *value,
        Probe {
            reject: Some(1),
            ..Probe::default()
        },
    );
    let path = scratch_path();
    loader.attach_store::<u32>(FileStore::new(path.clone()), Document::default());
    loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();
    loader.attach_store::<u32>(FileStore::new(broken_path()), Document::default());

    // Write-back fails AND the withdrawal fails: the views diverged, and the
    // divergence is loud — an error naming it plus a recorded fault carrying
    // both sides, never a dropped rollback error.
    let Err(divergence) = loader.update_entry(&id("one"), 2u32).await else {
        panic!("a diverged update must fail loudly");
    };
    assert!(divergence.message.contains("diverged"), "{divergence:?}");
    let faults = loader.entry_faults();
    assert_eq!(faults.len(), 1);
    assert_eq!(faults[0].entry, id("one"));
    assert!(faults[0].error.message.contains('2'), "runtime side named");
    assert!(
        faults[0].error.message.contains('1'),
        "committed side named"
    );
    // The document stayed at the prior state; the runtime staged the change.
    assert_eq!(disk_entry(&path, "one").await.config, 1);
    assert_eq!(stated(&log), vec![2]);

    // A faulted entry refuses further amendments until reconverged.
    assert!(loader.update_entry(&id("one"), 3u32).await.is_err());

    // Reconciling the document reconverges (here: the document catches up to
    // the runtime) and surfaces the drained fault in the report.
    loader.attach_store::<u32>(FileStore::new(path.clone()), Document::default());
    let report = loader
        .reconcile(Profile {
            entries: vec![entry("one", 2u32)],
        })
        .await
        .grab();
    assert_eq!(
        report.errors.len(),
        1,
        "the divergence surfaced: {report:?}"
    );
    assert_eq!(report.errors[0].entry, id("one"));
    assert_eq!(committed_entry(&loader, "one").config, 2);
    assert_eq!(disk_entry(&path, "one").await.config, 2);
    assert!(loader.entry_faults().is_empty(), "reconverged");
    let _ = std::fs::remove_dir_all(path.parent().grab());
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

/// A config whose `Debug` rendering never changes, while its value does: only
/// the type's own equality may decide reconcile-by-id, never the rendering.
#[derive(Clone, PartialEq)]
struct Sealed(u32);

impl fmt::Debug for Sealed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("sealed")
    }
}

#[tokio::test]
async fn a_config_change_invisible_to_debug_still_restates() {
    let (loader, log) = probe_loader::<Sealed>(|sealed| sealed.0, Probe::default());
    loader
        .reconcile(Profile {
            entries: vec![entry("one", Sealed(1))],
        })
        .await
        .grab();

    let report = loader
        .reconcile(Profile {
            entries: vec![entry("one", Sealed(2))],
        })
        .await
        .grab();
    assert_eq!(report.restarted, vec![id("one")], "the change must restate");
    assert_eq!(stated(&log), vec![2], "the runtime observed the new config");

    // True equality stays inert: an identical document touches nothing.
    let report = loader
        .reconcile(Profile {
            entries: vec![entry("one", Sealed(2))],
        })
        .await
        .grab();
    assert_eq!(report.unchanged, vec![id("one")]);
    assert!(report.restarted.is_empty());
    assert_eq!(stated(&log), vec![2]);
}
