//! The reversible-effect surface: descriptors and inverses (pre-work
//! extraction, M1-P8; zero semantic change).

use crate::{EffectId, KernelFuture};

/// Public description of the live reversible-effect tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectDescriptor {
    pub id: EffectId,
    pub label: String,
    pub children: Vec<EffectDescriptor>,
}

/// An inverse registered at the same boundary as its forward effect.
pub trait Undo: Send + 'static {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()>;
}
