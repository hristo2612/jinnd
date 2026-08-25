//! The dependency declaration/injection surface (authorized M1-P4 facade
//! delta): how a plugin names what it injects and how its per-activation
//! snapshot is built. Split from `lib.rs` by responsibility (R10).

use std::any::TypeId;
use std::fmt::Debug;

use crate::{KernelError, ServiceContract, ServiceHandle};

/// Typed identity for a statically linked service contract (R3).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ServiceType {
    pub type_id: TypeId,
    pub name: &'static str,
}

impl ServiceType {
    /// The identity of `S`, coherent by construction: both fields come from the
    /// one contract, so a declaration can never pair a type with a foreign name.
    #[must_use]
    pub fn of<S: ServiceContract>() -> Self {
        Self {
            type_id: TypeId::of::<S>(),
            name: S::NAME,
        }
    }
}

/// Resolves typed service handles for one consumer scope (R4).
///
/// The kernel implements this; a plugin's dependency snapshot is built through
/// it once per activation and never cached across activations.
pub trait ServiceResolver {
    /// Resolves `S` for the consumer this resolver is scoped to.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorCode::MissingDependency`] when no provider is reachable.
    fn resolve<S: ServiceContract>(&self) -> Result<ServiceHandle<S>, KernelError>;
}

/// A plugin's dependency declaration and injection surface.
///
/// [`declare`](Inject::declare) names the injected contracts in declaration
/// order — the kernel activates the consumer only when every one has an Active,
/// checked provider. [`inject`](Inject::inject) builds one owned per-activation
/// snapshot from resolved handles (R4).
pub trait Inject: Debug + Send + Sync + Sized + 'static {
    /// The injected service contracts, in declaration order.
    fn declare() -> Vec<ServiceType>;

    /// Builds this activation's owned dependency snapshot.
    ///
    /// # Errors
    ///
    /// Whatever the resolver answers; the kernel treats any error as a failed
    /// activation, contained to this fiber (R11).
    fn inject<R: ServiceResolver + ?Sized>(resolver: &R) -> Result<Self, KernelError>;
}

/// A plugin that injects nothing is always injectable.
impl Inject for () {
    fn declare() -> Vec<ServiceType> {
        Vec::new()
    }

    fn inject<R: ServiceResolver + ?Sized>(_resolver: &R) -> Result<Self, KernelError> {
        Ok(())
    }
}
