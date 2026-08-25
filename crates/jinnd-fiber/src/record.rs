//! What a fiber publishes about its own history.

use jinnd_api::{EffectDescriptor, KernelError, Transition};
use jinnd_effects::ReplayReport;

/// One fiber's observable history, as of the last landed transition.
///
/// This is the ledger's feed (R6): transitions, failures and withdrawal reports as
/// values, never a `last_error` string. This crate keeps them in memory and persists
/// nothing — the ledger packet is what makes them durable.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FiberRecord {
    /// Every state change the fiber committed, in order.
    pub transitions: Vec<Transition>,
    /// Every contained failure, in the order they happened.
    pub failures: Vec<KernelError>,
    /// What each withdrawal actually withdrew, in the order the replays ran.
    pub replays: Vec<ReplayReport>,
    /// The live effect tree as of the last landed transition.
    pub effects: Vec<EffectDescriptor>,
}
