//! M1-P6c scope 4a (PLA-270 R1 seam): the loader must not hold its state lock
//! across `EntryHandle::rebind`/`restart` — those calls reach lane-owned code,
//! and a handle that consults the loader (a legal observation) would deadlock
//! against the held guard. The probe handle asks `Loader::entry_fiber` from a
//! helper thread with a bounded wait, so the pre-fix hold reads as a failed
//! assertion, never a hung suite.

#![cfg(not(feature = "loom"))]

mod common;

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use common::{Grab, id};
use jinnd_api::{
    EntryId, FiberId, FiberState, IsolationBinding, KernelError, KernelFuture, PluginRef, Profile,
    ProfileEntry, Realm, TransitionCause,
};
use jinnd_context::Context;
use jinnd_loader::{EntryHandle, Loader, PackageLane, SpawnRequest};

/// A handle whose rebind consults the loader from a helper thread, bounded.
struct ProbingHandle {
    fiber: FiberId,
    entry: EntryId,
    loader: Arc<OnceLock<Weak<Loader>>>,
    blocked: Arc<AtomicBool>,
}

impl ProbingHandle {
    /// Asks the loader for this entry's fiber on a helper thread; records
    /// whether the answer arrived while this call was on the stack.
    fn probe(&self) {
        let Some(loader) = self.loader.get().and_then(Weak::upgrade) else {
            return;
        };
        let entry = self.entry.clone();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(loader.entry_fiber(&entry));
        });
        if rx.recv_timeout(Duration::from_secs(1)).is_err() {
            self.blocked.store(true, Ordering::SeqCst);
        }
    }
}

impl EntryHandle for ProbingHandle {
    fn id(&self) -> FiberId {
        self.fiber
    }

    fn state(&self) -> FiberState {
        FiberState::Active
    }

    fn withdrawing(&self) -> bool {
        false
    }

    fn restart(&self, _cause: TransitionCause) {
        self.probe();
    }

    fn restate(&self, _config: &(dyn Any + Send + Sync)) -> Result<(), KernelError> {
        Ok(())
    }

    fn rebind(&self, _at: Context<()>) {
        self.probe();
    }

    fn dispose(&self) -> KernelFuture<'static, ()> {
        Box::pin(async { Ok(()) })
    }

    fn quiesce(&self) -> KernelFuture<'static, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn probing_entry(name: &str, isolation: Vec<IsolationBinding>) -> ProfileEntry<u32> {
    ProfileEntry {
        id: id(name),
        plugin: PluginRef {
            package: "test/probing".to_owned(),
            version: "1".to_owned(),
            artifact_hash: String::new(),
        },
        config: 1,
        disabled: false,
        parent: None,
        isolation,
    }
}

#[tokio::test]
async fn rebind_and_restart_run_with_no_loader_lock_held() {
    let tree = jinnd_context::ContextTree::new();
    let loader = Arc::new(Loader::new(
        tree.root(),
        jinnd_registry::Registry::new(),
        |_context| {},
    ));
    let cell: Arc<OnceLock<Weak<Loader>>> = Arc::new(OnceLock::new());
    let blocked = Arc::new(AtomicBool::new(false));
    let handles = Arc::new(std::sync::atomic::AtomicU64::new(1));

    let captured = Arc::clone(&cell);
    let observed = Arc::clone(&blocked);
    let serial = Arc::clone(&handles);
    loader
        .register_lane::<u32>(
            "test/probing",
            PackageLane {
                injects: Vec::new(),
                provides: None,
                spawn: Box::new(move |request: SpawnRequest<'_>| {
                    Ok(Arc::new(ProbingHandle {
                        fiber: FiberId(serial.fetch_add(1, Ordering::Relaxed) + 1),
                        entry: request.entry.clone(),
                        loader: Arc::clone(&captured),
                        blocked: Arc::clone(&observed),
                    }) as Arc<dyn EntryHandle>)
                }),
            },
        )
        .grab();
    cell.set(Arc::downgrade(&loader)).ok().grab();

    loader
        .reconcile(Profile {
            entries: vec![probing_entry("one", Vec::new())],
        })
        .await
        .grab();

    // Changing the entry's isolation plans a Rebind step, which calls the
    // handle's rebind (and restart when the provided realm moved).
    loader
        .reconcile(Profile {
            entries: vec![probing_entry(
                "one",
                vec![IsolationBinding {
                    service: "svc.fixture".to_owned(),
                    realm: Realm::Local(id("one")),
                }],
            )],
        })
        .await
        .grab();

    assert!(
        !blocked.load(Ordering::SeqCst),
        "the loader held its state lock across a handle call: an observation \
         from lane-owned code deadlocked against the guard (R1)"
    );
}
