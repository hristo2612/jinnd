//! M1-P6c scope 3 (R1, extending the P6b conflict-point refusal): an entry's
//! activation amending its OWN entry would make the loader await the calling
//! task's own fiber — a self-deadlock, not a race. Refused honestly and
//! retryably at the conflict point; a sibling amendment from the same
//! activation stays admissible. The mechanism is the REST gate (round-2 law):
//! a fiber-awaiting amendment begins only when the target fiber is at rest,
//! decided from kernel-owned state — so the refusal holds through any chain
//! of spawned-and-awaited helper tasks, not just on the fiber's own task.

#![cfg(not(feature = "loom"))]

mod common;

use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use common::{Grab, entry, fixture, id, profile};
use jinnd_api::{EntryId, KernelError, KernelFuture, Profile, TransitionCause};
use jinnd_fiber::{FiberBody, Setup};
use jinnd_loader::{Loader, PackageLane, SpawnRequest};

type Observed = Arc<Mutex<Vec<String>>>;

fn describe(label: &str, result: &Result<(), KernelError>) -> String {
    match result {
        Ok(()) => format!("{label}: succeeded"),
        Err(error) => format!("{label}: refused: {}", error.message),
    }
}

/// A body that, when told to probe, amends its own entry and then a sibling
/// from inside its activation — directly on the fiber's own task, or through
/// a spawned-and-awaited helper (the task-local-bypass shape, round 2).
struct SelfAmendBody {
    loader: Weak<Loader>,
    own: EntryId,
    sibling: EntryId,
    via_spawn: bool,
    probe: Arc<std::sync::atomic::AtomicBool>,
    observed: Observed,
}

impl FiberBody for SelfAmendBody {
    fn activate<'a>(&'a self, _setup: Setup<'a>) -> KernelFuture<'a, ()> {
        Box::pin(async move {
            if !self.probe.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }
            self.probe.store(false, std::sync::atomic::Ordering::SeqCst);
            let Some(loader) = self.loader.upgrade() else {
                return Ok(());
            };
            let own = {
                let loader = Arc::clone(&loader);
                let entry = self.own.clone();
                async move {
                    if self.via_spawn {
                        // The awaited helper escapes any task-local: only
                        // kernel-owned state can refuse this shape.
                        let helper = tokio::spawn(async move {
                            loader.update_entry(&entry, 5_u32).await
                        });
                        helper.await.unwrap_or_else(|join| {
                            Err(KernelError {
                                code: jinnd_api::ErrorCode::InvalidProfile,
                                message: format!("the helper panicked: {join}"),
                                fiber: None,
                            })
                        })
                    } else {
                        loader.update_entry(&entry, 5_u32).await
                    }
                }
            };
            // Bounded so the pre-fix self-deadlock reads as a failed
            // assertion, never a hung suite.
            let mine = match tokio::time::timeout(Duration::from_secs(2), own).await {
                Ok(result) => result,
                Err(_elapsed) => Err(KernelError {
                    code: jinnd_api::ErrorCode::InvalidProfile,
                    message: "DEADLOCKED: the amendment awaited its own fiber".to_owned(),
                    fiber: None,
                }),
            };
            let siblings = loader.update_entry(&self.sibling, 9_u32).await;
            let mut log = self
                .observed
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            log.push(describe("own", &mine));
            log.push(describe("sibling", &siblings));
            Ok(())
        })
    }
}

type FiberSlot = Arc<Mutex<Option<Arc<jinnd_fiber::Fiber>>>>;

fn self_amend_fixture(
    via_spawn: bool,
) -> (
    Arc<Loader>,
    Observed,
    Arc<std::sync::atomic::AtomicBool>,
    FiberSlot,
) {
    let (loader, _registry, _log) = fixture();
    let loader = Arc::new(loader);
    let observed: Observed = Arc::new(Mutex::new(Vec::new()));
    let probe = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let slot: FiberSlot = Arc::new(Mutex::new(None));
    let recorded = Arc::clone(&observed);
    let armed = Arc::clone(&probe);
    let stashed = Arc::clone(&slot);
    let captured: Weak<Loader> = Arc::downgrade(&loader);
    loader
        .register_lane::<u32>(
            "test/self-amend",
            PackageLane {
                injects: Vec::new(),
                provides: None,
                spawn: Box::new(move |request: SpawnRequest<'_>| {
                    let body = Arc::new(SelfAmendBody {
                        loader: captured.clone(),
                        own: request.entry.clone(),
                        sibling: id("b"),
                        via_spawn,
                        probe: Arc::clone(&armed),
                        observed: Arc::clone(&recorded),
                    });
                    let fiber = Arc::new(jinnd_fiber::Fiber::spawn(
                        body as Arc<dyn FiberBody>,
                        request.signal,
                    ));
                    *stashed.lock().unwrap_or_else(|poison| poison.into_inner()) =
                        Some(Arc::clone(&fiber));
                    Ok(Arc::new(common::PlainHandle { fiber })
                        as Arc<dyn jinnd_loader::EntryHandle>)
                }),
            },
        )
        .grab();
    (loader, observed, probe, slot)
}

/// Loads both entries, arms the probe, re-activates the probing fiber, and
/// returns what its activation observed.
async fn probe_run(via_spawn: bool) -> (Arc<Loader>, Vec<String>) {
    let (loader, observed, probe, slot) = self_amend_fixture(via_spawn);
    loader
        .reconcile(profile(vec![
            entry("a", "test/self-amend", 1),
            entry("b", "test/count", 1),
        ]))
        .await
        .grab();

    // Re-activate outside any loader operation: the probing activation runs
    // with no engagement held, which is exactly where the self-deadlock lived.
    probe.store(true, std::sync::atomic::Ordering::SeqCst);
    let fiber = slot
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
        .grab();
    let restarted = tokio::time::timeout(Duration::from_secs(5), async {
        fiber.restart(TransitionCause::ExplicitRestart);
        fiber.quiesce().await;
    })
    .await;
    assert!(restarted.is_ok(), "the probing activation must settle");

    let observed = observed
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    (loader, observed)
}

/// The refusal committed nothing anywhere; the sibling amendment did.
fn assert_only_the_sibling_committed(loader: &Loader) {
    let committed: Profile<u32> = loader.persisted().grab();
    let own = committed
        .entries
        .iter()
        .find(|spec| spec.id == id("a"))
        .grab();
    assert_eq!(own.config, 1, "a refused self-amendment commits nothing");
    let sibling = committed
        .entries
        .iter()
        .find(|spec| spec.id == id("b"))
        .grab();
    assert_eq!(sibling.config, 9);
}

#[tokio::test]
async fn an_activation_amending_its_own_entry_is_refused_never_deadlocked() {
    let (loader, observed) = probe_run(false).await;
    assert_eq!(observed.len(), 2, "the activation probed both entries");
    assert!(
        observed[0].starts_with("own: refused")
            && observed[0].contains("own fiber")
            && !observed[0].contains("DEADLOCKED"),
        "amending the entry from its own activation must be refused at the \
         conflict point, got: {}",
        observed[0]
    );
    assert!(
        observed[1].starts_with("sibling: succeeded"),
        "a sibling amendment from an activation is admissible, got: {}",
        observed[1]
    );
    assert_only_the_sibling_committed(&loader);
}

/// Round 2 (the task-local-bypass finding): the same self-amendment issued
/// through a helper task the activation spawns and awaits escapes any
/// task-local — the REST gate must refuse it from kernel-owned state alone.
#[tokio::test]
async fn an_awaited_helper_amending_the_activations_entry_is_refused_never_deadlocked() {
    let (loader, observed) = probe_run(true).await;
    assert_eq!(observed.len(), 2, "the activation probed both entries");
    assert!(
        observed[0].starts_with("own: refused") && !observed[0].contains("DEADLOCKED"),
        "the awaited-helper self-amendment must be refused at the conflict \
         point, got: {}",
        observed[0]
    );
    assert!(
        observed[1].starts_with("sibling: succeeded"),
        "a sibling amendment from an activation is admissible, got: {}",
        observed[1]
    );
    assert_only_the_sibling_committed(&loader);
}
