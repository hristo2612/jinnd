//! Reactive availability: the registry's implementation of the fiber engine's
//! readiness seam (R1).
//!
//! A consumer declares what it injects; the registry answers with a signal whose
//! value is the epoch the consumer may activate against — the value-identity of
//! every injected provider — or `None` while any is missing. The signal is
//! recomputed only on the store's change edge and published only when the epoch
//! actually moved, so a provider that flickers away and back to the same identity
//! coalesces to no work at all (§3, "Epoch gating").

use std::future::pending;

use jinnd_api::{DependencySnapshot, Epoch, ServiceType};
use jinnd_context::Context;
use jinnd_fiber::{ReadinessSignal, Signal};
use tokio::sync::watch;

use crate::registry::Registry;

/// What one consumer declares it injects, in declaration order.
///
/// Order is part of the epoch's identity; the registry never reorders it.
#[derive(Clone, Debug)]
pub struct Injection {
    pub services: Vec<ServiceType>,
}

/// The registry-backed readiness signal one consumer fiber gates on.
#[derive(Debug)]
pub struct InjectedReadiness {
    epoch: watch::Receiver<Option<Epoch>>,
}

impl Registry {
    /// The readiness signal for a consumer at `from` injecting `injection`.
    ///
    /// Spawns one watcher task on the current tokio runtime; the task ends when
    /// the signal is dropped or the registry is.
    ///
    /// # Panics
    ///
    /// If called outside a tokio runtime, where no consumer fiber can live
    /// anyway (R1).
    #[must_use]
    pub fn readiness<I: Send + Sync + 'static>(
        &self,
        from: &Context<I>,
        injection: Injection,
    ) -> InjectedReadiness {
        let registry = self.clone();
        let from = from.clone();
        let (sender, receiver) = watch::channel(compute(&registry, &from, &injection));
        let mut edge = registry.store().watch();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = sender.closed() => return,
                    changed = edge.changed() => {
                        if changed.is_err() {
                            // The store is gone; the epoch can never move again.
                            return;
                        }
                        let next = compute(&registry, &from, &injection);
                        sender.send_if_modified(|current| {
                            if *current == next {
                                return false;
                            }
                            *current = next;
                            true
                        });
                    }
                }
            }
        });
        InjectedReadiness { epoch: receiver }
    }
}

impl ReadinessSignal for InjectedReadiness {
    fn epoch(&self) -> Option<Epoch> {
        self.epoch.borrow().clone()
    }

    fn changed(&mut self) -> Signal<'_> {
        Box::pin(async move {
            if self.epoch.changed().await.is_err() {
                // The watcher is gone and can never speak again; waiting forever
                // is the honest answer, exactly as the fiber crate's source does.
                pending::<()>().await;
            }
        })
    }
}

/// The epoch `from` may currently activate against, if every injected service
/// resolves: one [`DependencySnapshot`] per declaration, in declaration order.
fn compute<I>(registry: &Registry, from: &Context<I>, injection: &Injection) -> Option<Epoch> {
    let mut dependencies = Vec::with_capacity(injection.services.len());
    for service in &injection.services {
        let key = from.tree().key_for(service);
        let (address, entry) = registry.locate(from, key).ok()?;
        dependencies.push(DependencySnapshot {
            service: *service,
            provider: entry.provider,
            generation: entry.generation,
            realm: from
                .tree()
                .realm_value(address.realm)
                .unwrap_or(jinnd_api::Realm::Root),
        });
    }
    Some(Epoch { dependencies })
}
