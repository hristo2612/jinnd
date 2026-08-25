//! Runtime-originated amendments: the runtime is offered a change first, and
//! the document follows only after the runtime accepted — a rejected or
//! unpersistable change leaves both views of the one truth at their prior
//! state (LAW §3 bidirectional persistence).

mod common;

use std::fmt;

use common::probe::{
    Probe, committed_entry, disk_entry, encode, probe_entry as entry, probe_loader, scratch_path,
    stated,
};
use common::{Grab, id};
use jinnd_api::{ErrorCode, Profile};
use jinnd_loader::FileStore;

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
    loader.attach_store(FileStore::new(path.clone()), encode);
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
    loader.attach_store(FileStore::new(path.clone()), encode);
    loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();

    // Every save through this store fails: its directory does not exist.
    let broken = std::env::temp_dir()
        .join(format!("jinnd-loader-amend-missing-{}", std::process::id()))
        .join("profile.json");
    loader.attach_store(FileStore::new(broken), encode);

    assert!(loader.update_entry(&id("one"), 2u32).await.is_err());
    // The committed view stayed at the prior state...
    assert_eq!(committed_entry(&loader, "one").config, 1);
    // ...and the staged config was withdrawn: offered 2, restored to 1.
    assert_eq!(stated(&log), vec![2, 1]);
    let _ = std::fs::remove_dir_all(path.parent().grab());
}

#[tokio::test]
async fn a_refused_disposal_leaves_the_document_at_the_prior_state() {
    let (loader, _log) = probe_loader::<u32>(
        |value| *value,
        Probe {
            refuse_disposal: true,
            ..Probe::default()
        },
    );
    let path = scratch_path();
    loader.attach_store(FileStore::new(path.clone()), encode);
    loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();

    assert!(loader.dispose_entry::<u32>(&id("one")).await.is_err());
    // Neither view records the disposal, and the runtime is still there.
    assert!(!committed_entry(&loader, "one").disabled);
    assert!(!disk_entry(&path, "one").await.disabled);
    assert!(loader.entry_fiber(&id("one")).is_some());
    let _ = std::fs::remove_dir_all(path.parent().grab());
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
