//! The typed resolution walk and its isolation-boundary stop.
//!
//! Ported from the TS original's property-get trap (`packages/core/src/reflect.ts`,
//! lines 80-94), which ascends the fiber chain and stops at the first of: a frame that
//! holds the value, a frame that declares the key but has no value yet (inactive
//! context), the root, or an isolation boundary — a parent that resolves the key in a
//! different realm than the caller does.

use jinnd_api::{ContextId, ErrorCode, KernelError};

use crate::context::Context;
use crate::key::{NameId, RealmId, ServiceKey};

/// What a caller's probe found at one frame of the walk.
///
/// This crate stores no services, so a probe — the registry, in a later packet — is
/// asked what each frame holds. A frame that both provides and declares the key
/// answers [`Probe::Provided`]: the TS original consults the store before the
/// injection list.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Probe<T> {
    /// This frame holds a value for the key.
    Provided(T),
    /// This frame declares the key as an injected dependency but holds no value.
    Declared,
    /// This frame neither provides nor declares the key.
    Absent,
}

/// A resolved value with the scope it was charged to (R4).
///
/// `caller` is the context that asked and `provider` the frame that answered, so an
/// effect registered against this resolution is charged to the caller by construction
/// rather than by attribution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Resolved<T> {
    pub value: T,
    pub key: ServiceKey,
    pub caller: ContextId,
    pub provider: ContextId,
    pub realm: RealmId,
}

/// The frames a resolution may consult, nearest first.
///
/// The iterator ends at the root or at an isolation boundary, whichever comes first;
/// the boundary frame itself is yielded, only its parent is not.
#[derive(Debug)]
pub struct ResolutionFrames<I> {
    cursor: Option<Context<I>>,
    name: NameId,
    realm: RealmId,
}

impl<I> ResolutionFrames<I> {
    /// The realm every frame of this walk resolves the key in.
    ///
    /// Computed once when the walk is built, so a resolution ascends the layer chain
    /// for the realm exactly once rather than once per query of it.
    #[must_use]
    pub fn realm(&self) -> RealmId {
        self.realm
    }
}

impl<I> Iterator for ResolutionFrames<I> {
    type Item = Context<I>;

    fn next(&mut self) -> Option<Context<I>> {
        let current = self.cursor.take()?;
        self.cursor = current
            .parent()
            .filter(|parent| parent.realm_of(self.name) == self.realm);
        Some(current)
    }
}

impl<I> Context<I> {
    /// The frames [`Context::resolve`] would consult for `key`, nearest first.
    ///
    /// Which frames those are depends only on the key's *name*: isolation is
    /// name-keyed, so the two lanes walk the same chain.
    #[must_use]
    pub fn resolution_frames(&self, key: ServiceKey) -> ResolutionFrames<I> {
        let name = key.name();
        ResolutionFrames {
            cursor: Some(self.clone()),
            name,
            realm: self.realm_of(name),
        }
    }

    /// Walks the tree for `key`, asking `probe` what each frame holds.
    ///
    /// Fails with [`ErrorCode::InactiveContext`] when a frame declares the key without
    /// providing it, and with [`ErrorCode::MissingDependency`] when the walk reaches
    /// the root or an isolation boundary without an answer. The walk holds no lock, so
    /// `probe` never runs under one (R1).
    pub fn resolve<T>(
        &self,
        key: ServiceKey,
        mut probe: impl FnMut(&Context<I>) -> Probe<T>,
    ) -> Result<Resolved<T>, KernelError> {
        let caller = self.id();
        let frames = self.resolution_frames(key);
        let realm = frames.realm();
        for frame in frames {
            match probe(&frame) {
                Probe::Provided(value) => {
                    return Ok(Resolved {
                        value,
                        key,
                        caller,
                        provider: frame.id(),
                        realm,
                    });
                }
                Probe::Declared => {
                    return Err(self.error(
                        ErrorCode::InactiveContext,
                        key,
                        "cannot resolve required service {key} in an inactive context",
                    ));
                }
                Probe::Absent => {}
            }
        }
        Err(self.error(
            ErrorCode::MissingDependency,
            key,
            "no provider for service {key} is reachable from this context",
        ))
    }

    fn error(&self, code: ErrorCode, key: ServiceKey, template: &str) -> KernelError {
        let name = self
            .name_text(key.name())
            .unwrap_or_else(|| format!("<name {:?}>", key.name()));
        KernelError {
            code,
            message: template.replace("{key}", &format!("\"{name}\"")),
            fiber: None,
        }
    }
}
