//! The activation-time resolver behind the facade's `Inject` surface.
//!
//! A consumer's dependency snapshot is built once per activation (R4): each
//! resolution takes one dependent lease on the resolved generation (I2), and the
//! collected guards are handed back to the caller, which must hold them until
//! its own unload completes — that hold is what makes a dying provider wait for
//! its consumers.

use std::sync::Mutex;

use jinnd_api::{KernelError, ServiceContract, ServiceHandle, ServiceResolver};
use jinnd_context::Context;

use crate::registry::Registry;
use crate::store::LeaseGuard;

/// Resolves one activation's injected services, leasing each one (I2, R4).
#[derive(Debug)]
pub struct ActivationResolver<'a, I> {
    registry: &'a Registry,
    from: &'a Context<I>,
    guards: Mutex<Vec<LeaseGuard>>,
}

impl<'a, I> ActivationResolver<'a, I> {
    /// A resolver scoped to the consumer at `from`.
    #[must_use]
    pub fn new(registry: &'a Registry, from: &'a Context<I>) -> Self {
        Self {
            registry,
            from,
            guards: Mutex::new(Vec::new()),
        }
    }

    /// The dependent leases this resolver took, in resolution order.
    ///
    /// The caller holds them for its activation's whole life — through its own
    /// teardown — and drops them last, so it may still call a dying provider
    /// while unloading (I2).
    #[must_use]
    pub fn into_guards(self) -> Vec<LeaseGuard> {
        self.guards
            .into_inner()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

impl<I> ServiceResolver for ActivationResolver<'_, I> {
    fn resolve<S: ServiceContract>(&self) -> Result<ServiceHandle<S>, KernelError> {
        let (handle, guard) = self.registry.lease::<S, I>(self.from)?;
        // No await and no plugin code runs under this lock (R1).
        self.guards
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(guard);
        Ok(handle)
    }
}
