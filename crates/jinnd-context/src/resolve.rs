//! The typed resolution walk and its isolation-boundary stop.
//!
//! Ported from the TS original's property-get trap (`packages/core/src/reflect.ts`,
//! lines 80-94), which ascends the fiber chain and stops at the first of: a frame that
//! holds the value, a frame that declares the key but has no value yet (inactive
//! context), the root, or an isolation boundary — a parent that resolves the key in a
//! different realm than the caller does.

use jinnd_api::{ContextId, ErrorCode, KernelError};

use crate::context::Context;
use crate::key::{KeyId, RealmId};

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
    key: KeyId,
    realm: RealmId,
}

impl<I> Iterator for ResolutionFrames<I> {
    type Item = Context<I>;

    fn next(&mut self) -> Option<Context<I>> {
        let current = self.cursor.take()?;
        self.cursor = current
            .parent()
            .filter(|parent| parent.realm_of(self.key) == self.realm);
        Some(current)
    }
}

impl<I> Context<I> {
    /// The frames [`Context::resolve`] would consult for `key`, nearest first.
    #[must_use]
    pub fn resolution_frames(&self, key: KeyId) -> ResolutionFrames<I> {
        ResolutionFrames {
            cursor: Some(self.clone()),
            key,
            realm: self.realm_of(key),
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
        key: KeyId,
        mut probe: impl FnMut(&Context<I>) -> Probe<T>,
    ) -> Result<Resolved<T>, KernelError> {
        let caller = self.id();
        let realm = self.realm_of(key);
        for frame in self.resolution_frames(key) {
            match probe(&frame) {
                Probe::Provided(value) => {
                    return Ok(Resolved {
                        value,
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

    fn error(&self, code: ErrorCode, key: KeyId, template: &str) -> KernelError {
        let name = self
            .key_name(key)
            .unwrap_or_else(|| format!("<key {key:?}>"));
        KernelError {
            code,
            message: template.replace("{key}", &format!("\"{name}\"")),
            fiber: None,
        }
    }
}
