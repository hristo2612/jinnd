//! Crate-owned M2-K23 round-2 witnesses (R1, Law 2): an administration's
//! runtime step is SCHEDULED — the call answers before the target's
//! disposal completes, a replacement's successor spawns only after the old
//! incarnation withdrew — and the document the commit will write is readable
//! BEFORE the write, so the caller's row can land first; a failed write-back
//! leaves both views at their prior state with nothing scheduled.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jinnd_api::{
    EntryId, ErrorCode, FiberId, FiberState, KernelError, KernelFuture, PluginRef, Profile,
    ProfileEntry, TransitionCause,
};
use tokio::sync::Notify;

use super::Administration;
use crate::document::Document;
use crate::lanes::{EntryHandle, PackageLane, SpawnRequest};
use crate::loader::Loader;
use crate::store::DocumentStore;

/// A handle whose disposal completes only when the test releases it.
struct GatedHandle {
    id: FiberId,
    release: Arc<Notify>,
    disposed: Arc<AtomicBool>,
}

impl EntryHandle for GatedHandle {
    fn id(&self) -> FiberId {
        self.id
    }
    fn state(&self) -> FiberState {
        FiberState::Active
    }
    fn withdrawing(&self) -> bool {
        false
    }
    fn resting(&self) -> bool {
        true
    }
    fn restart(&self, _cause: TransitionCause) {}
    fn restate(&self, _config: &(dyn std::any::Any + Send + Sync)) -> Result<(), KernelError> {
        Ok(())
    }
    fn rebind(&self, _at: jinnd_context::Context<()>) {}
    fn dispose(&self) -> KernelFuture<'static, ()> {
        let release = Arc::clone(&self.release);
        let disposed = Arc::clone(&self.disposed);
        Box::pin(async move {
            release.notified().await;
            disposed.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
    fn quiesce(&self) -> KernelFuture<'static, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Captures every save; fails the Nth when asked.
struct Store {
    saves: Arc<AtomicU64>,
    fail_on: Option<u64>,
    last: Arc<Mutex<Option<Document>>>,
}

impl DocumentStore for Store {
    fn save<'a>(&'a self, document: &'a Document) -> KernelFuture<'a, ()> {
        Box::pin(async move {
            let nth = self.saves.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_on == Some(nth) {
                return Err(KernelError {
                    code: ErrorCode::InvalidProfile,
                    message: "the store refuses this save".to_owned(),
                    fiber: None,
                });
            }
            *self.last.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(document.clone());
            Ok(())
        })
    }
}

struct Rig {
    loader: Arc<Loader>,
    release: Arc<Notify>,
    disposed: Arc<AtomicBool>,
    spawned: Arc<AtomicU64>,
    last: Arc<Mutex<Option<Document>>>,
}

fn entry(name: &str, package: &str) -> ProfileEntry<u32> {
    ProfileEntry {
        id: EntryId(name.to_owned()),
        plugin: PluginRef {
            package: package.to_owned(),
            version: "1".to_owned(),
            artifact_hash: "aa".to_owned(),
        },
        config: 1,
        disabled: false,
        parent: None,
        isolation: Vec::new(),
    }
}

fn grab<T, E: std::fmt::Debug>(outcome: Result<T, E>) -> T {
    match outcome {
        Ok(value) => value,
        Err(error) => panic!("{error:?}"),
    }
}

async fn rig(fail_on: Option<u64>) -> Rig {
    let tree = jinnd_context::ContextTree::new();
    let loader = Arc::new(Loader::new(
        tree.root(),
        jinnd_registry::Registry::new(),
        |_context| {},
    ));
    let release = Arc::new(Notify::new());
    let disposed = Arc::new(AtomicBool::new(false));
    let spawned = Arc::new(AtomicU64::new(0));
    for package in ["double/plugin", "double/other"] {
        let (release, disposed, spawned) = (
            Arc::clone(&release),
            Arc::clone(&disposed),
            Arc::clone(&spawned),
        );
        grab(loader.register_lane::<u32>(
            package,
            PackageLane {
                injects: Vec::new(),
                provides: None,
                spawn: Box::new(move |_request: SpawnRequest<'_>| {
                    let id = spawned.fetch_add(1, Ordering::SeqCst) + 1;
                    Ok(Arc::new(GatedHandle {
                        id: FiberId(id),
                        release: Arc::clone(&release),
                        disposed: Arc::clone(&disposed),
                    }) as Arc<dyn EntryHandle>)
                }),
            },
        ));
    }
    let last = Arc::new(Mutex::new(None));
    loader.attach_store_with::<u32>(
        Box::new(Store {
            saves: Arc::new(AtomicU64::new(0)),
            fail_on,
            last: Arc::clone(&last),
        }),
        Document::default(),
    );
    grab(
        loader
            .reconcile(Profile {
                entries: vec![entry("one", "double/plugin"), entry("two", "double/plugin")],
            })
            .await,
    );
    Rig {
        loader,
        release,
        disposed,
        spawned,
        last,
    }
}

fn saved_ids(last: &Mutex<Option<Document>>) -> Vec<String> {
    last.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
        .map(|document| document.entries.iter().map(|entry| entry.id.clone()).collect())
        .unwrap_or_default()
}

/// R1 (COO ruling 3): the call answers with both views committed while the
/// target's disposal is still in flight; the disposal lands afterwards.
#[tokio::test]
async fn a_removal_answers_before_the_targets_disposal_completes() {
    let rig = rig(None).await;
    let one = EntryId("one".to_owned());
    let staged = grab(rig.loader.stage(Administration::<u32>::Remove(one.clone())));
    let settled = tokio::time::timeout(
        Duration::from_secs(2),
        rig.loader.commit_administration(staged),
    )
    .await;
    let settled = grab(grab(settled));
    assert!(!rig.disposed.load(Ordering::SeqCst), "disposal not awaited");
    assert_eq!(saved_ids(&rig.last), vec!["two"], "the document committed");
    // The entry is engaged until its step lands: a second write refuses
    // `conflict`, retryably, never parks.
    assert!(
        rig.loader
            .stage(Administration::<u32>::Remove(one.clone()))
            .is_err()
    );
    rig.release.notify_one();
    grab(settled.await);
    assert!(rig.disposed.load(Ordering::SeqCst), "the disposal landed");
    assert!(rig.loader.entry_fiber(&one).is_none(), "the runtime forgot it");
    assert!(rig.loader.stage(Administration::<u32>::Remove(one)).is_err());
}

/// The successor of a replacement spawns only after the old incarnation
/// withdrew (no two fibers for one entry), and never inside the call.
#[tokio::test]
async fn a_replacement_spawns_its_successor_after_the_old_incarnation_withdrew() {
    let rig = rig(None).await;
    let one = EntryId("one".to_owned());
    let before = rig.loader.entry_fiber(&one);
    let staged = grab(rig.loader.stage(Administration::<u32>::Swap(
        one.clone(),
        PluginRef {
            package: "double/other".to_owned(),
            version: "1".to_owned(),
            artifact_hash: "bb".to_owned(),
        },
    )));
    let settled = grab(rig.loader.commit_administration(staged).await);
    assert_eq!(rig.spawned.load(Ordering::SeqCst), 2, "no successor yet");
    rig.release.notify_one();
    grab(settled.await);
    assert_eq!(rig.spawned.load(Ordering::SeqCst), 3, "the successor spawned");
    assert_ne!(rig.loader.entry_fiber(&one), before, "a new incarnation");
    let persisted = grab(rig.loader.applied::<u32>()).unwrap_or(Profile { entries: Vec::new() });
    let swapped = persisted.entries.iter().find(|entry| entry.id == one);
    assert_eq!(
        swapped.map(|entry| entry.plugin.package.as_str()),
        Some("double/other")
    );
}

/// Law 2 (COO ruling 4): the staged document renders byte-for-byte what
/// the commit writes, so a row carrying its digest can land BEFORE the write.
#[tokio::test]
async fn the_staged_document_renders_what_the_commit_writes() {
    let rig = rig(None).await;
    let staged = grab(
        rig.loader
            .stage(Administration::Add(entry("three", "double/plugin"))),
    );
    let rendered = staged.rendered().map(str::to_owned);
    assert!(rendered.is_some(), "a store is attached");
    grab(grab(rig.loader.commit_administration(staged).await).await);
    let written = rig
        .last
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
        .map(Document::render);
    assert_eq!(rendered, written);
    assert!(rig.loader.retire_echo(&written.unwrap_or_default()));
}

/// A failed write-back refuses with both views at their prior state and no
/// runtime step scheduled (save 1 is the reconcile's; 2 the administration's).
#[tokio::test]
async fn a_failed_write_back_leaves_both_views_at_the_prior_state() {
    let rig = rig(Some(2)).await;
    let one = EntryId("one".to_owned());
    let staged = grab(rig.loader.stage(Administration::<u32>::Disable(one.clone())));
    assert!(rig.loader.commit_administration(staged).await.is_err());
    rig.release.notify_one();
    tokio::task::yield_now().await;
    assert!(!rig.disposed.load(Ordering::SeqCst), "nothing was scheduled");
    assert!(rig.loader.entry_fiber(&one).is_some(), "the fiber stays");
    assert_eq!(saved_ids(&rig.last), vec!["one", "two"]);
    assert!(rig.loader.entry_faults().is_empty(), "no divergence");
    // The engagement is released with the refusal: the next write stages.
    assert!(rig.loader.stage(Administration::<u32>::Disable(one)).is_ok());
}
