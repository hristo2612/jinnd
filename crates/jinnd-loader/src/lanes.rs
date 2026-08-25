//! The seam between the loader and whatever hosts plugin fibers.
//!
//! The loader itself never instantiates a plugin: a [`PackageLane`], registered
//! per package name, does (R3's string-keyed lane — profile entries are
//! dynamically referenced by `PluginRef`). What comes back is an opaque
//! [`EntryHandle`] the loader drives through the fiber engine's target
//! calculus. Containment tiers live behind this seam (R7): the conformance
//! harness backs it with in-process bodies, the daemon with hosted ones.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use jinnd_api::{
    EntryId, FiberId, FiberState, KernelError, KernelFuture, ServiceType, TransitionCause,
};
use jinnd_context::Context;
use jinnd_fiber::WatchReadiness;

/// Everything a lane needs to spawn one entry's fiber.
pub struct SpawnRequest<'a> {
    /// The entry being spawned.
    pub entry: &'a EntryId,
    /// The entry's derived context.
    pub at: &'a Context<()>,
    /// The entry's config payload; the lane downcasts it to its config type.
    pub config: &'a (dyn Any + Send + Sync),
    /// The readiness signal the fiber must gate on (R1).
    pub signal: WatchReadiness,
}

/// Instantiates one package's plugin as a fiber.
pub type SpawnFn =
    Box<dyn Fn(SpawnRequest<'_>) -> Result<Arc<dyn EntryHandle>, KernelError> + Send + Sync>;

/// One registered package: what its plugin injects, what it provides, and how
/// to spawn it.
pub struct PackageLane {
    /// The services the plugin declares it injects, in declaration order.
    pub injects: Vec<ServiceType>,
    /// The service the plugin provides on activation, if any. The loader uses
    /// it to reload the provider when a rebind moves the service's realm.
    pub provides: Option<ServiceType>,
    /// The spawner.
    pub spawn: SpawnFn,
}

impl fmt::Debug for PackageLane {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PackageLane")
            .field("injects", &self.injects)
            .field("provides", &self.provides)
            .finish_non_exhaustive()
    }
}

/// The loader's handle on one spawned entry fiber.
///
/// Every method is a target statement or an observation; none blocks a
/// transition (R1). `dispose` and `quiesce` borrow nothing so the loader never
/// holds state across their awaits.
pub trait EntryHandle: Send + Sync {
    /// The fiber's uid (never reused, R3).
    fn id(&self) -> FiberId;

    /// The fiber's last committed state.
    fn state(&self) -> FiberState;

    /// True while the fiber is replaying its withdrawal — plugin-owned
    /// inverses executing, on unload, disposal, and failure cleanup alike.
    ///
    /// The contract is causal (M1-P6b): the answer must already be `true` for
    /// any code the replay reaches, on the fiber's task or on tasks it
    /// spawned — the loader's conflict refusal is built on exactly this.
    fn withdrawing(&self) -> bool;

    /// Asks for a full clean reload, stating why.
    fn restart(&self, cause: TransitionCause);

    /// States a new latest config for the next activation to read.
    ///
    /// # Errors
    ///
    /// [`jinnd_api::ErrorCode::InvalidProfile`] when the payload is not this
    /// lane's config type.
    fn restate(&self, config: &(dyn Any + Send + Sync)) -> Result<(), KernelError>;

    /// Hands the fiber's body its rebuilt context for the next activation.
    fn rebind(&self, at: Context<()>);

    /// Disposes the fiber and resolves once its withdrawal completed.
    fn dispose(&self) -> KernelFuture<'static, ()>;

    /// Resolves once the fiber has settled with nothing left to do.
    fn quiesce(&self) -> KernelFuture<'static, ()>;
}
