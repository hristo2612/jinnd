//! The typed registry surface: provision as an effect, resolution as a walk.
//!
//! Resolution delegates the isolation semantics to `jinnd-context`: the walk, its
//! realm, and its boundary stop are the context crate's; this crate only answers
//! what each frame holds (R3, R10). Provision applies the slot and returns the
//! inverse that withdraws it — registration is an effect on whichever scope the
//! caller owns (R5), and the withdrawal completes only after every dependent's
//! lease has drained (I2).

use std::sync::Arc;

use jinnd_api::{
    ErrorCode, FiberId, Generation, KernelError, Realm, ServiceContract, ServiceHandle,
};
use jinnd_context::{Context, Probe, ServiceKey};
use jinnd_effects::Disposer;

use crate::slots::{Address, SlotEntry};
use crate::store::{LeaseGuard, Store};
use crate::vitality::Vitality;

/// The shared registry handle. Cloning shares one store.
#[derive(Clone, Debug)]
pub struct Registry {
    store: Arc<Store>,
}

/// What one provision installed: the generation the slot carries, and the inverse
/// that withdraws it (R5).
///
/// The inverse removes the slot first — no new resolutions, availability
/// withdrawn — and then waits for every dependent lease to drain before it
/// completes (I2). Registering it on the owning scope is the caller's half of the
/// effect contract.
#[must_use = "provision is an effect: register the undo on the owning scope (R5)"]
pub struct Provision {
    pub generation: Generation,
    pub undo: Disposer,
}

impl Registry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            store: Arc::new(Store::new()),
        }
    }

    /// Mints a vitality handle for one provider: the supervisor owning the
    /// provider reports "Active and passing its check" through it (§3), and every
    /// report wakes availability (R1).
    #[must_use]
    pub fn vitality(&self, initially: bool) -> Vitality {
        self.store.vitality(initially)
    }

    /// Publishes `value` as `S` at `at`, in the realm `realm` interns to.
    ///
    /// The slot is available to consumers only while `vitality` last reported
    /// `true`; resolution and leasing stay answerable regardless, so dependents
    /// can still call a dying provider during teardown (I2).
    pub fn provide<S: ServiceContract, I>(
        &self,
        at: &Context<I>,
        realm: &Realm,
        provider: FiberId,
        value: Arc<S>,
        vitality: &Vitality,
    ) -> Provision {
        let tree = at.tree();
        let realm_id = tree.realm(realm);
        let address = Address {
            // A named realm is the visibility unit itself (LAW §3): its slots
            // are realm-global, anchored at the tree root wherever the
            // provider's context sits. The root realm stays positional.
            context: if realm_id.is_root() {
                at.id()
            } else {
                tree.root().id()
            },
            key: tree.key_of::<S>(),
            realm: realm_id,
        };
        let entry = self
            .store
            .slots
            .insert(address, provider, value, vitality.cell());
        self.store.bump();

        let store = Arc::clone(&self.store);
        let leases = Arc::clone(&entry.leases);
        let generation = entry.generation;
        let value = Arc::clone(&entry.value);
        let undo = Disposer::future(move || async move {
            // Unregister and notify first: no new resolutions, availability
            // withdrawn, dependents told to unload — then wait for them (I2).
            if store.slots.remove_if(&address, generation) {
                store.bump();
            }
            leases.close();
            store.drained(Arc::clone(&leases)).await;
            // Only now may the provider's value die: dependents were entitled to
            // call it up to the moment their last lease returned (I2).
            drop(value);
            Ok(())
        });
        Provision { generation, undo }
    }

    /// Resolves `S` from `from`, honoring isolation boundaries (R3).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::MissingDependency`] when no provider is reachable before the
    /// walk's boundary or root.
    pub fn resolve<S: ServiceContract, I>(
        &self,
        from: &Context<I>,
    ) -> Result<ServiceHandle<S>, KernelError> {
        let (address, entry) = self.locate(from, from.tree().key_of::<S>())?;
        self.handle(from, &address, entry)
    }

    /// Resolves `S` and takes a dependent lease on the resolved generation: the
    /// consumer-activation path (I2). The provider's withdrawal will wait for the
    /// returned guard.
    ///
    /// # Errors
    ///
    /// As [`Registry::resolve`], and [`ErrorCode::MissingDependency`] when the
    /// resolved generation was superseded or closed before the lease landed.
    pub fn lease<S: ServiceContract, I>(
        &self,
        from: &Context<I>,
    ) -> Result<(ServiceHandle<S>, LeaseGuard), KernelError> {
        let (address, entry) = self.locate(from, from.tree().key_of::<S>())?;
        let Some(cell) = self.store.slots.lease(&address, entry.generation) else {
            return Err(error(
                ErrorCode::MissingDependency,
                "the resolved provider generation was withdrawn before the lease landed",
            ));
        };
        let guard = self.store.guard(cell);
        Ok((self.handle(from, &address, entry)?, guard))
    }

    pub(crate) fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// The lookup `resolve` and `lease` share, in the caller's realm for the
    /// key. In the root realm: the nearest frame holding the key (the boundary
    /// semantics are `jinnd-context`'s). In a named realm: the realm-global
    /// slot — realms, not tree positions, are the visibility unit (LAW §3).
    pub(crate) fn locate<I>(
        &self,
        from: &Context<I>,
        key: ServiceKey,
    ) -> Result<(Address, SlotEntry), KernelError> {
        let realm = from.resolution_frames(key).realm();
        if !realm.is_root() {
            let address = Address {
                context: from.tree().root().id(),
                key,
                realm,
            };
            return match self.store.slots.get(&address) {
                Some(entry) => Ok((address, entry)),
                None => Err(error(
                    ErrorCode::MissingDependency,
                    "no provider for the service is reachable in this realm",
                )),
            };
        }
        let resolved = from.resolve(key, |frame| {
            let address = Address {
                context: frame.id(),
                key,
                realm,
            };
            match self.store.slots.get(&address) {
                Some(entry) => Probe::Provided(entry),
                None => Probe::Absent,
            }
        })?;
        let address = Address {
            context: resolved.provider,
            key,
            realm,
        };
        Ok((address, resolved.value))
    }

    /// Builds the facade handle for a located entry (R4: the caller's scope rides
    /// with the value).
    fn handle<S: ServiceContract, I>(
        &self,
        from: &Context<I>,
        address: &Address,
        entry: SlotEntry,
    ) -> Result<ServiceHandle<S>, KernelError> {
        let Ok(service) = entry.value.downcast::<S>() else {
            // Unreachable by construction: the slot's key carries `S`'s `TypeId`,
            // so only an `Arc<S>` can have been stored under it. Answered as an
            // error all the same — no panic crosses this boundary (R11).
            return Err(error(
                ErrorCode::EffectFailed,
                "the stored provider value does not implement the resolved contract",
            ));
        };
        Ok(ServiceHandle {
            service,
            caller: from.id(),
            provider: entry.provider,
            generation: entry.generation,
            realm: from
                .tree()
                .realm_value(address.realm)
                .unwrap_or(Realm::Root),
        })
    }
}

fn error(code: ErrorCode, message: &str) -> KernelError {
    KernelError {
        code,
        message: message.to_owned(),
        fiber: None,
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}
