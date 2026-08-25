//! Shared fixtures for the registry integration tests.

#![allow(dead_code)]

use std::any::TypeId;
use std::sync::Arc;

use jinnd_api::{FiberId, Generation, Realm, ServiceContract, ServiceType};
use jinnd_context::Context;
use jinnd_effects::EffectScope;
use jinnd_registry::Registry;

/// The pseudo-fiber provisions made outside any fiber are charged to.
pub const KERNEL_SCOPE: FiberId = FiberId(0);

/// A tiny observable service: its observation is the value it was built with.
#[derive(Debug)]
pub struct Counter(pub u8);

impl ServiceContract for Counter {
    type Observation = u8;

    const NAME: &'static str = "jinn.test/counter";

    fn observe(&self) -> u8 {
        self.0
    }
}

/// The erased identity of [`Counter`], as a dependency declaration names it.
#[must_use]
pub fn counter_service() -> ServiceType {
    ServiceType {
        type_id: TypeId::of::<Counter>(),
        name: Counter::NAME,
    }
}

/// A second, unrelated service, for asserting that changes to one slot never
/// disturb consumers of another.
#[derive(Debug)]
pub struct Other(pub u8);

impl ServiceContract for Other {
    type Observation = u8;

    const NAME: &'static str = "jinn.test/other";

    fn observe(&self) -> u8 {
        self.0
    }
}

/// Provides an [`Other`] at `at` in the root realm, registered on `scope` (R5).
pub fn provide_other<I>(registry: &Registry, scope: &mut EffectScope, at: &Context<I>, value: u8) {
    let provision = registry.provide::<Other, I>(
        at,
        &Realm::Root,
        KERNEL_SCOPE,
        Arc::new(Other(value)),
        &registry.vitality(true),
    );
    let registered = scope.register(format!("provide {}", Other::NAME), provision.undo);
    assert!(
        registered.is_ok(),
        "the provision undo must register: {registered:?}"
    );
}

/// Provides a [`Counter`] at `at` in the root realm, registering the withdrawal
/// on `scope` (R5), and returns the generation the slot carries.
pub fn provide_counter<I>(
    registry: &Registry,
    scope: &mut EffectScope,
    at: &Context<I>,
    value: u8,
) -> Generation {
    provide_counter_guarded(registry, scope, at, value, &registry.vitality(true))
}

/// As [`provide_counter`], under a caller-owned vitality handle.
pub fn provide_counter_guarded<I>(
    registry: &Registry,
    scope: &mut EffectScope,
    at: &Context<I>,
    value: u8,
    vitality: &jinnd_registry::Vitality,
) -> Generation {
    let provision = registry.provide::<Counter, I>(
        at,
        &Realm::Root,
        KERNEL_SCOPE,
        Arc::new(Counter(value)),
        vitality,
    );
    let registered = scope.register(format!("provide {}", Counter::NAME), provision.undo);
    assert!(
        registered.is_ok(),
        "the provision undo must register: {registered:?}"
    );
    provision.generation
}
