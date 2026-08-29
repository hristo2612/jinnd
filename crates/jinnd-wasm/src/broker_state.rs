//! The broker's bookkeeping: peers with their grants, providers with their
//! generations, and caller-scoped handles (R4). Split from `broker.rs` by
//! responsibility (R10 file hygiene); no locking or dispatch lives here.

use std::collections::HashMap;
use std::sync::Arc;

use jinnd_api::{ErrorCode, FiberId, KernelError};

use crate::grants::GrantScope;
use crate::peer::{HandleId, Peer, PeerId};
use crate::topics::EventTarget;

pub(crate) struct PeerRecord {
    pub(crate) fiber: Option<FiberId>,
    /// The profile entry the peer acts for (M2-K4): a host provider keys
    /// the retained inverse to the ENTRY, whose journal outlives fibers.
    pub(crate) entry: Option<String>,
    /// Per granted contract, the typed authority the grant carries
    /// (M2-K6): root, path-prefix subtrees, or a process/net policy.
    /// Root is the explicit maximum, never a default.
    pub(crate) grants: HashMap<String, GrantScope>,
    /// Per granted contract, the operation class the grant is attenuated
    /// to (M2-K8, harness #24; round-2 ruling 2): absent means every
    /// declared operation; several grants of one contract compose by
    /// union in EITHER order, so once any grant is unattenuated the class
    /// is [`OpsClass::All`] for good.
    pub(crate) ops: HashMap<String, OpsClass>,
    /// The peer's own delivery face (M2-K7): where a host provider delivers
    /// a wake for a registration this peer holds. A Mode-1 commit
    /// re-attaches it to the successor instance (R8).
    pub(crate) target: Option<Arc<dyn EventTarget>>,
}

/// The operation class one peer holds a contract under (M2-K8): the union
/// of every grant's declared class. `All` absorbs — attenuation narrows
/// within one grant, never across grants (Law 1: order-independent).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OpsClass {
    All,
    Only(Vec<String>),
}

impl OpsClass {
    /// Widens by the other class.
    pub(crate) fn union(&mut self, other: Self) {
        match (&mut *self, other) {
            (Self::All, _) => {}
            (_, Self::All) => *self = Self::All,
            (Self::Only(held), Self::Only(more)) => {
                for op in more {
                    if !held.contains(&op) {
                        held.push(op);
                    }
                }
            }
        }
    }

    pub(crate) fn admits(&self, operation: &str) -> bool {
        match self {
            Self::All => true,
            Self::Only(ops) => ops.iter().any(|op| op == operation),
        }
    }
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
