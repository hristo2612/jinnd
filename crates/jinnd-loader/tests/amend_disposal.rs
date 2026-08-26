//! Runtime-originated disposal: the runtime moves first, the document
//! persists the entry as disabled, and a failed write-back is loud — retried
//! once, then recorded as a divergence (LAW §3; split from `amend.rs` by
//! responsibility, R10).

#![cfg(not(feature = "loom"))]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common::probe::{
    Probe, broken_path, committed_entry, disk_entry, probe_entry as entry, probe_loader,
    scratch_path,
};
use common::{Grab, id};
use jinnd_api::{ErrorCode, KernelError, KernelFuture, Profile};
use jinnd_loader::{Document, DocumentStore, FileStore};

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
    loader.attach_store::<u32>(FileStore::new(path.clone()), Document::default());
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
    loader.attach_store::<u32>(FileStore::new(path.clone()), Document::default());
    loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();
    loader.attach_store::<u32>(FileStore::new(broken_path()), Document::default());

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
    loader.attach_store::<u32>(FileStore::new(path.clone()), Document::default());
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

/// A store that fails exactly its Nth save: the disposal save path is
/// mechanical since M1-P6c round 2 — no caller-authored serializer runs in
/// it — so the store itself is the seam that proves the retry.
struct FlakyStore {
    inner: FileStore,
    saves: Arc<AtomicU64>,
    fail_on: u64,
}

impl DocumentStore for FlakyStore {
    fn save<'a>(&'a self, document: &'a Document) -> KernelFuture<'a, ()> {
        Box::pin(async move {
            if self.saves.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_on {
                return Err(KernelError {
                    code: ErrorCode::InvalidProfile,
                    message: "the store refuses this save".to_owned(),
                    fiber: None,
                });
            }
            self.inner.save(document).await
        })
    }
}

#[tokio::test]
async fn a_disposal_write_back_is_retried_before_recording_divergence() {
    let (loader, _log) = probe_loader::<u32>(|value| *value, Probe::default());
    let path = scratch_path();
    let saves = Arc::new(AtomicU64::new(0));
    // Save 1 is the reconcile's; 2 is the disposal's first write-back, made
    // to fail; 3 is the retry, which lands.
    loader.attach_store::<u32>(
        FlakyStore {
            inner: FileStore::new(path.clone()),
            saves: Arc::clone(&saves),
            fail_on: 2,
        },
        Document::default(),
    );
    loader
        .reconcile(Profile {
            entries: vec![entry("one", 1u32)],
        })
        .await
        .grab();

    loader.dispose_entry::<u32>(&id("one")).await.grab();
    assert_eq!(saves.load(Ordering::SeqCst), 3, "retried exactly once");
    assert!(disk_entry(&path, "one").await.disabled);
    assert!(loader.entry_faults().is_empty(), "no divergence remains");
    let _ = std::fs::remove_dir_all(path.parent().grab());
}
