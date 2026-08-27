//! Forward-effect and teardown-observation surface (authorized M1-P7 additive
//! delta; paper Def 51/52 + Alg 1; the cordis_dispose and invariant_recovery
//! FACADE_GAP citations, and the I2 value-stability IOU).

use crate::{KernelError, KernelFuture, Undo};

/// One forward action: runs when the kernel drives the effect — never at
/// registration (R9) — and returns the inverse of exactly what it did.
pub type ForwardAction = Box<dyn FnOnce() -> KernelFuture<'static, Box<dyn Undo>> + Send + 'static>;

/// A forward effect, per its atomicity contract (paper Def 51/52):
/// a plain effect installs all-or-none; a stepwise effect yields its inverse
/// per step, with the target-staleness guard run at every yield boundary and
/// a divert rolling back exactly the yielded prefix (Alg 1).
pub enum ForwardEffect {
    Plain(ForwardAction),
    Steps(Vec<ForwardAction>),
}

impl std::fmt::Debug for ForwardEffect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("ForwardEffect::Plain"),
            Self::Steps(steps) => write!(f, "ForwardEffect::Steps({})", steps.len()),
        }
    }
}

/// The running activation's effect registrar: teardown effects registered here
/// are charged to the activation's fiber and replay LIFO with it, after any
/// injected-service leases return — so a teardown effect may still observe its
/// dying dependencies (I2). Registration is refused once the activation has
/// settled: a later inverse would never be withdrawn.
pub trait EffectHost: Send + Sync {
    /// Registers `undo` as a teardown effect of the running activation.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InactiveContext`](crate::ErrorCode::InactiveContext) once
    /// the activation has settled.
    fn register(&self, label: String, undo: Box<dyn Undo>) -> Result<(), KernelError>;
}
