//! The facade's `Inject` surface over the registry: per-activation snapshots
//! built from leased handles (R4), so a dying provider waits for every injected
//! consumer (I2).

#![cfg(not(feature = "loom"))]

mod support;

use jinnd_api::{
    ErrorCode, Inject, KernelError, ServiceContract, ServiceHandle, ServiceResolver, ServiceType,
};
use jinnd_context::ContextTree;
use jinnd_effects::EffectScope;
use jinnd_registry::{ActivationResolver, Registry};
use support::{Counter, provide_counter};

/// One consumer's declared dependencies, as a plugin would state them.
#[derive(Debug)]
struct Deps {
    counter: ServiceHandle<Counter>,
}

impl Inject for Deps {
    fn declare() -> Vec<ServiceType> {
        vec![ServiceType::of::<Counter>()]
    }

    fn inject<R: ServiceResolver + ?Sized>(resolver: &R) -> Result<Self, KernelError> {
        Ok(Self {
            counter: resolver.resolve::<Counter>()?,
        })
    }
}

async fn breathe() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn an_injected_snapshot_leases_each_resolved_provider() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let mut scope = EffectScope::new();
    provide_counter(&registry, &mut scope, &tree.root(), 7);

    let root = tree.root();
    let resolver = ActivationResolver::new(&registry, &root);
    let deps = match Deps::inject(&resolver) {
        Ok(deps) => deps,
        Err(error) => unreachable!("a provided dependency must inject: {error:?}"),
    };
    assert_eq!(deps.counter.service.observe(), 7);
    let guards = resolver.into_guards();
    assert_eq!(
        guards.len(),
        1,
        "one dependent lease per resolved service (I2)"
    );

    // The provider's withdrawal must wait for the activation's guards.
    drop(deps);
    let replay = tokio::spawn(async move { scope.replay().await });
    breathe().await;
    assert!(
        !replay.is_finished(),
        "withdrawal must wait for the injected consumer's leases (I2)"
    );

    drop(guards);
    let report = match replay.await {
        Ok(report) => report,
        Err(_) => unreachable!("the withdrawal task must not panic"),
    };
    assert!(
        report.is_clean(),
        "the drained withdrawal completes cleanly"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn injection_without_a_provider_is_refused_not_faked() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();

    let root = tree.root();
    let resolver = ActivationResolver::new(&registry, &root);
    let refused = match Deps::inject(&resolver) {
        Ok(_) => unreachable!("an absent provider must not inject"),
        Err(error) => error,
    };
    assert_eq!(refused.code, ErrorCode::MissingDependency);
    assert!(
        resolver.into_guards().is_empty(),
        "a refused injection holds no leases"
    );
}
