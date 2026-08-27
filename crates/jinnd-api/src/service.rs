//! Typed service contracts, handles, and dependency epochs (pre-work
//! extraction, M1-P8; zero semantic change).

use std::fmt::Debug;
use std::sync::Arc;

use crate::{ContextId, FiberId, Generation, Realm, ServiceType};

/// A typed service contract with its own observational-equivalence witness.
pub trait ServiceContract: Send + Sync + 'static {
    type Observation: Debug + PartialEq + Send + Sync + 'static;

    const NAME: &'static str;

    fn observe(&self) -> Self::Observation;
}

/// A resolved service paired with caller scope and provider generation (R4).
#[derive(Debug)]
pub struct ServiceHandle<S: ServiceContract> {
    pub service: Arc<S>,
    pub caller: ContextId,
    pub provider: FiberId,
    pub generation: Generation,
    pub realm: Realm,
}

/// One dependency generation captured for a single activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySnapshot {
    pub service: ServiceType,
    pub provider: FiberId,
    pub generation: Generation,
    pub realm: Realm,
}

/// Full dependency epoch owned by one activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Epoch {
    pub dependencies: Vec<DependencySnapshot>,
}
