//! Runtime-originated amendments: the runtime is offered a change first, and
//! the document follows only after the runtime accepted — a rejected or
//! unpersistable change leaves both views of the one truth at their prior
//! state (LAW §3 bidirectional persistence).

mod common;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

/// A store path whose directory does not exist, so every save fails.
fn broken_path() -> std::path::PathBuf {
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir()
        .join(format!(
            "jinnd-loader-amend-missing-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ))
        .join("profile.json")
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
    loader.attach_store(FileStore::new(path.clone()), encode);
    loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();
    loader.attach_store(FileStore::new(broken_path()), encode);

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
    loader.attach_store(FileStore::new(path.clone()), encode);
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

#[tokio::test]
async fn a_disposal_whose_write_back_fails_records_the_divergence() {
    let (loader, _log) = probe_loader::<u32>(|value| *value, Probe::default());
    let path = scratch_path();
    loader.attach_store(FileStore::new(path.clone()), encode);
    loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();
    loader.attach_store(FileStore::new(broken_path()), encode);

    // Disposal is irreversible at runtime; the failed write-back leaves the
    // document enabled — the divergence is recorded, never silent.
    let Err(divergence) = loader.dispose_entry::<u32>(&id("one")).await else {
        panic!("a diverged disposal must fail loudly");
    };
    assert!(divergence.message.contains("diverged"), "{divergence:?}");
    assert!(loader.entry_fiber(&id("one")).is_none(), "runtime disposed");
    assert!(!disk_entry(&path, "one").await.disabled, "document enabled");
    let faults = loader.entry_faults();
    assert_eq!(faults.len(), 1);
    assert_eq!(faults[0].entry, id("one"));

    // The next reconcile of the still-enabled document reconverges: the entry
    // respawns, and the drained divergence surfaces in the report.
    loader.attach_store(FileStore::new(path.clone()), encode);
    let report = loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();
    assert!(report.created.contains(&id("one")), "respawned: {report:?}");
    assert_eq!(report.errors.len(), 1, "the divergence surfaced");
    assert!(loader.entry_fiber(&id("one")).is_some());
    assert!(loader.entry_faults().is_empty(), "reconverged");
    let _ = std::fs::remove_dir_all(path.parent().grab());
}

#[tokio::test]
async fn a_disposal_write_back_is_retried_before_recording_divergence() {
    let (loader, _log) = probe_loader::<u32>(|value| *value, Probe::default());
    let path = scratch_path();
    loader.attach_store(FileStore::new(path.clone()), encode);
    loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();

    // A store whose directory appears only after the first failed save: the
    // stateful encoder heals the path on its second call, so the retry lands.
    let healing = broken_path();
    let calls = Arc::new(AtomicU64::new(0));
    let seen = Arc::clone(&calls);
    let heal = healing.parent().grab().to_path_buf();
    loader.attach_store(
        FileStore::new(healing.clone()),
        move |profile: &Profile<u32>| {
            if seen.fetch_add(1, Ordering::SeqCst) == 1 {
                std::fs::create_dir_all(&heal).grab();
            }
            encode(profile)
        },
    );

    loader.dispose_entry::<u32>(&id("one")).await.grab();
    assert_eq!(calls.load(Ordering::SeqCst), 2, "retried exactly once");
    assert!(disk_entry(&healing, "one").await.disabled);
    assert!(loader.entry_faults().is_empty(), "no divergence remains");
    let _ = std::fs::remove_dir_all(path.parent().grab());
    let _ = std::fs::remove_dir_all(healing.parent().grab());
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
