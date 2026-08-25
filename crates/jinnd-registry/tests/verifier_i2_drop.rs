//! The I2 value-lifetime pin from the M1-P4 round-1 escalation: a dying
//! provider's VALUE must remain alive until every dependent's unload completes —
//! keeping only the lease cell alive is not I2 (verifier blocker, COO ruling on
//! the packet Todo). This file mirrors the verifier's `verifier_i2_drop` probe so
//! the regression stays pinned in the implementer's own suite.

#![cfg(not(feature = "loom"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use jinnd_api::{FiberId, Realm, ServiceContract};
use jinnd_context::ContextTree;
use jinnd_effects::EffectScope;
use jinnd_registry::Registry;

/// A service whose drop is observable from the outside.
#[derive(Debug)]
struct Sentinel {
    dropped: Arc<AtomicBool>,
}

impl ServiceContract for Sentinel {
    type Observation = ();

    const NAME: &'static str = "jinn.test/sentinel";

    fn observe(&self) {}
}

impl Drop for Sentinel {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

/// Lets the spawned withdrawal reach its drain await.
async fn breathe() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn the_provider_value_outlives_its_dependents_unload() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let mut scope = EffectScope::new();
    let dropped = Arc::new(AtomicBool::new(false));

    let provision = registry
        .provide::<Sentinel, ()>(
            &tree.root(),
            &Realm::Root,
            FiberId(0),
            Arc::new(Sentinel {
                dropped: Arc::clone(&dropped),
            }),
            &registry.vitality(true),
        )
        .unwrap_or_else(|error| panic!("an empty slot must accept a provision: {error:?}"));
    let registered = scope.register_draining("provide sentinel", provision.drain, provision.undo);
    assert!(
        registered.is_ok(),
        "provision must register on a live scope"
    );

    let Ok((handle, guard)) = registry.lease::<Sentinel, ()>(&tree.root()) else {
        unreachable!("a just-provided service must lease");
    };
    // The dependent keeps only its lease: the handle's clone of the value must
    // not be what keeps I2 honest.
    drop(handle);

    let replay = tokio::spawn(async move { scope.replay().await });
    breathe().await;
    assert!(
        !replay.is_finished(),
        "withdrawal must wait for the outstanding dependent lease (I2)"
    );
    assert!(
        !dropped.load(Ordering::SeqCst),
        "I2 requires the provider value to remain alive until every dependent unloads"
    );

    drop(guard);
    let report = match replay.await {
        Ok(report) => report,
        Err(_) => unreachable!("the withdrawal task must not panic"),
    };
    assert!(
        report.is_clean(),
        "the drained withdrawal completes cleanly"
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "with every dependent unloaded, the withdrawn value must drop"
    );
}
