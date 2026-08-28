//! The broker's bookkeeping: peers with their grants, providers with their
//! generations, and caller-scoped handles (R4). Split from `broker.rs` by
//! responsibility (R10 file hygiene); no locking or dispatch lives here.

use std::collections::HashMap;
use std::sync::Arc;

use jinnd_api::{ErrorCode, FiberId, KernelError};

use crate::peer::{HandleId, Peer, PeerId};

pub(crate) struct PeerRecord {
    pub(crate) fiber: Option<FiberId>,
    /// Per granted contract, the path-prefix scopes the grant carries; an
    /// empty list is the root scope (a bare grant, or one that widened a
    /// scoped grant — root is the explicit maximum, never a default).
    pub(crate) grants: HashMap<String, Vec<String>>,
}

/// One live provision. `generation` is the identity of THIS provision of the
/// contract: any later provision carries a higher one, so a handle pinned to
/// this generation goes stale the moment the provider changes (epoch gating
/// at the call site; R9 — no silent replacement).
pub(crate) struct Provider {
    pub(crate) peer: PeerId,
    pub(crate) callable: Arc<dyn Peer>,
    pub(crate) generation: u64,
}

/// A minted capability handle: owner-scoped (R4) and pinned to the provider
/// generation it resolved against.
pub(crate) struct Handle {
    pub(crate) owner: PeerId,
    pub(crate) contract: String,
    pub(crate) generation: u64,
}

#[derive(Default)]
pub(crate) struct State {
    pub(crate) peers: HashMap<PeerId, PeerRecord>,
    pub(crate) providers: HashMap<String, Provider>,
    /// Per-contract provision generation: bumped by every `provide`, never
    /// by a withdrawal (an absent provider is "missing", not "stale").
    pub(crate) generations: HashMap<String, u64>,
    pub(crate) handles: HashMap<HandleId, Handle>,
    pub(crate) next_peer: PeerId,
    pub(crate) next_handle: HandleId,
}

impl State {
    pub(crate) fn fiber_of(&self, peer: PeerId) -> Option<FiberId> {
        self.peers.get(&peer).and_then(|record| record.fiber)
    }

    pub(crate) fn granted(&self, peer: PeerId, contract: &str) -> bool {
        self.peers
            .get(&peer)
            .is_some_and(|record| record.grants.contains_key(contract))
    }

    /// The current generation of `contract`'s provision slot (0 = never
    /// provided).
    pub(crate) fn generation_of(&self, contract: &str) -> u64 {
        self.generations.get(contract).copied().unwrap_or(0)
    }
}

pub(crate) fn refusal(code: ErrorCode, message: String) -> KernelError {
    KernelError {
        code,
        message,
        fiber: None,
    }
}
