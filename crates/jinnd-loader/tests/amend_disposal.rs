//! Runtime-originated disposal: the runtime moves first, the document
//! persists the entry as disabled, and a failed write-back is loud — retried
//! once, then recorded as a divergence (LAW §3; split from `amend.rs` by
//! responsibility, R10).

#![cfg(not(feature = "loom"))]

mod common;

use common::probe::{
    Probe, broken_path, committed_entry, disk_entry, probe_entry as entry, probe_loader,
    scratch_path,
};
use common::{Grab, id};
use jinnd_api::Profile;
use jinnd_loader::Document;

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
    loader.attach_store::<u32>(path.clone(), Document::default());
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

#[tokio::test]
async fn a_disposal_whose_write_back_fails_records_the_divergence() {
    let (loader, _log) = probe_loader::<u32>(|value| *value, Probe::default());
    let path = scratch_path();
    loader.attach_store::<u32>(path.clone(), Document::default());
    loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();
    loader.attach_store::<u32>(broken_path(), Document::default());

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
    loader.attach_store::<u32>(path.clone(), Document::default());
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

// The disposal retry-once proof lives in `src/dispose.rs`'s own test module
// since M1-P6c round 3: the store seam is sealed — `DocumentStore` is
// crate-internal, so the fail-exactly-once double proving the retry is a
// crate-owned impl, exactly as the permit's no-caller-code guarantee demands.
