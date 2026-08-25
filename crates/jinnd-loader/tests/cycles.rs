//! M1-P6c scope 6 (I3): dependency cycles over lane provides/injects
//! declarations are detected statically at plan time. Involved entries land
//! cleanly inactive with `DependencyCycle` recorded; acyclic siblings are
//! untouched (R11) and the reconcile terminates.

#![cfg(not(feature = "loom"))]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{Grab, entry, id, plain_spawn, profile};
use jinnd_api::{ErrorCode, FiberState, KernelFuture, ServiceContract, ServiceType};
use jinnd_fiber::{FiberBody, Setup};
use jinnd_loader::{Loader, PackageLane, SpawnRequest};

/// Three service contracts so alpha → beta → gamma → alpha can be declared.
macro_rules! link_service {
    ($name:ident, $label:literal) => {
        #[derive(Debug)]
        struct $name;

        impl ServiceContract for $name {
            type Observation = ();

            const NAME: &'static str = $label;

            fn observe(&self) {}
        }
    };
}

link_service!(LinkA, "svc.link-a");
link_service!(LinkB, "svc.link-b");
link_service!(LinkC, "svc.link-c");

/// A body that never needs to run: cycle members must stay inactive, and the
/// acyclic sibling injects nothing.
struct InertBody;

impl FiberBody for InertBody {
    fn activate<'a>(&'a self, _setup: Setup<'a>) -> KernelFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Registers a lane declaring `injects` and `provides` with an inert body.
fn declare_lane(loader: &Loader, package: &str, injects: Vec<ServiceType>, provides: ServiceType) {
    loader
        .register_lane::<u32>(
            package,
            PackageLane {
                injects,
                provides: Some(provides),
                spawn: Box::new(move |request: SpawnRequest<'_>| {
                    Ok(plain_spawn(Arc::new(InertBody), request.signal))
                }),
            },
        )
        .grab();
}

/// A loader with the cyclic trio (alpha → beta → gamma → alpha), an inert
/// declaration-free `test/solo`, and a `test/sibling` lane registered.
fn cycle_fixture() -> Loader {
    let tree = jinnd_context::ContextTree::new();
    let loader = Loader::new(tree.root(), jinnd_registry::Registry::new(), |_context| {});
    declare_lane(
        &loader,
        "test/alpha",
        vec![ServiceType::of::<LinkB>()],
        ServiceType::of::<LinkA>(),
    );
    declare_lane(
        &loader,
        "test/beta",
        vec![ServiceType::of::<LinkC>()],
        ServiceType::of::<LinkB>(),
    );
    declare_lane(
        &loader,
        "test/gamma",
        vec![ServiceType::of::<LinkA>()],
        ServiceType::of::<LinkC>(),
    );
    for package in ["test/solo", "test/sibling"] {
        loader
            .register_lane::<u32>(
                package,
                PackageLane {
                    injects: Vec::new(),
                    provides: None,
                    spawn: Box::new(move |request: SpawnRequest<'_>| {
                        Ok(plain_spawn(Arc::new(InertBody), request.signal))
                    }),
                },
            )
            .grab();
    }
    loader
}

#[tokio::test]
async fn a_declared_dependency_cycle_is_detected_and_contained() {
    let loader = cycle_fixture();

    // The bound exists to catch a regression hang; miri interprets ~100x
    // slower, so the guard widens there rather than reporting wall-clock.
    let deadline = Duration::from_secs(if cfg!(miri) { 300 } else { 5 });
    let report = tokio::time::timeout(
        deadline,
        loader.reconcile(profile(vec![
            entry("alpha", "test/alpha", 1),
            entry("beta", "test/beta", 1),
            entry("gamma", "test/gamma", 1),
            entry("sibling", "test/sibling", 1),
        ])),
    )
    .await
    .grab()
    .grab();

    // The cycle is reported statically, per involved entry.
    for name in ["alpha", "beta", "gamma"] {
        let fault = report
            .errors
            .iter()
            .find(|fault| fault.entry == id(name))
            .unwrap_or_else(|| panic!("{name} must carry a recorded cycle fault"));
        assert_eq!(
            fault.error.code,
            ErrorCode::DependencyCycle,
            "the recorded error names the cycle, got {:?}",
            fault.error
        );
        // Cleanly inactive: no fiber was ever spawned for a cycle member.
        assert!(
            loader.entry_fiber(&id(name)).is_none(),
            "{name} must land cleanly inactive"
        );
    }

    // The unrelated sibling is untouched (R11) and reaches Active.
    let sibling = loader.entry_fiber(&id("sibling")).grab();
    assert_eq!(loader.fiber_state(sibling), Some(FiberState::Active));
}

/// Round-2 blocker: live entries REPLACED into a cycle must land cleanly
/// inactive — dropping their plan steps left the old fibers running.
#[tokio::test]
async fn entries_replaced_into_a_cycle_do_not_leave_the_old_fibers_active() {
    let loader = cycle_fixture();
    let deadline = Duration::from_secs(if cfg!(miri) { 300 } else { 5 });

    // The trio starts acyclic — declaration-free solo plugins — and Active.
    tokio::time::timeout(
        deadline,
        loader.reconcile(profile(vec![
            entry("alpha", "test/solo", 1),
            entry("beta", "test/solo", 1),
            entry("gamma", "test/solo", 1),
            entry("sibling", "test/sibling", 1),
        ])),
    )
    .await
    .grab()
    .grab();
    for name in ["alpha", "beta", "gamma"] {
        let fiber = loader.entry_fiber(&id(name)).grab();
        assert_eq!(loader.fiber_state(fiber), Some(FiberState::Active));
    }
    let sibling = loader.entry_fiber(&id("sibling")).grab();

    // The same entries are replaced onto the cyclic trio of packages.
    let report = tokio::time::timeout(
        deadline,
        loader.reconcile(profile(vec![
            entry("alpha", "test/alpha", 1),
            entry("beta", "test/beta", 1),
            entry("gamma", "test/gamma", 1),
            entry("sibling", "test/sibling", 1),
        ])),
    )
    .await
    .grab()
    .grab();

    for name in ["alpha", "beta", "gamma"] {
        let fault = report
            .errors
            .iter()
            .find(|fault| fault.entry == id(name))
            .unwrap_or_else(|| panic!("{name} must carry a recorded cycle fault"));
        assert_eq!(fault.error.code, ErrorCode::DependencyCycle);
        assert!(
            loader.entry_fiber(&id(name)).is_none(),
            "{name} is a cycle member and must land cleanly inactive"
        );
    }
    // The acyclic sibling kept its very fiber: untouched, not merely alive.
    assert_eq!(loader.entry_fiber(&id("sibling")), Some(sibling));
    assert_eq!(loader.fiber_state(sibling), Some(FiberState::Active));
}

/// A new member closing a cycle over entries the diff left untouched: the
/// untouched members must still land cleanly inactive, not stay live.
#[tokio::test]
async fn a_new_member_closing_a_cycle_lands_the_untouched_members_inactive() {
    let loader = cycle_fixture();
    let deadline = Duration::from_secs(if cfg!(miri) { 300 } else { 5 });

    // alpha → beta exists; beta waits on a LinkC provider that never comes.
    tokio::time::timeout(
        deadline,
        loader.reconcile(profile(vec![
            entry("alpha", "test/alpha", 1),
            entry("beta", "test/beta", 1),
        ])),
    )
    .await
    .grab()
    .grab();
    assert!(loader.entry_fiber(&id("alpha")).is_some());
    assert!(loader.entry_fiber(&id("beta")).is_some());

    // gamma arrives and closes alpha → beta → gamma → alpha.
    let report = tokio::time::timeout(
        deadline,
        loader.reconcile(profile(vec![
            entry("alpha", "test/alpha", 1),
            entry("beta", "test/beta", 1),
            entry("gamma", "test/gamma", 1),
        ])),
    )
    .await
    .grab()
    .grab();

    for name in ["alpha", "beta", "gamma"] {
        let fault = report
            .errors
            .iter()
            .find(|fault| fault.entry == id(name))
            .unwrap_or_else(|| panic!("{name} must carry a recorded cycle fault"));
        assert_eq!(fault.error.code, ErrorCode::DependencyCycle);
        assert!(
            loader.entry_fiber(&id(name)).is_none(),
            "{name} is a cycle member and must land cleanly inactive"
        );
    }
}
