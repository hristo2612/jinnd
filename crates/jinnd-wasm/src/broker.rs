//! The transport-agnostic capability broker — the single dispatch point
//! (decision log 2026-08-25, binding on this packet): grant check → ledger
//! append → dispatch, accepting "a contract call from a peer". Whether the
//! peer is a linked WASM instance or the conformance harness is a transport
//! detail behind [`Peer`]; a broker fused to wasmtime linking is a design
//! defect. The harness lane routes through THIS SAME broker (test-harness
//! ruling closure c).

use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{ErrorCode, FiberId, KernelError, LedgerEventKind};

use crate::broker_state::{PeerRecord, Provider, refusal};
use crate::peer::{HandleId, LedgerSink, Peer, PeerId};

use crate::broker_state::{Handle, State};

mod calls;

/// The broker. One per kernel; every contract crossing of every transport
/// goes through [`Broker::call`].
pub struct Broker {
    state: Mutex<State>,
    ledger: Arc<dyn LedgerSink>,
}

impl Broker {
    pub fn new(ledger: Arc<dyn LedgerSink>) -> Self {
        Self {
            state: Mutex::new(State::default()),
            ledger,
        }
    }

    /// No guard is ever held across an await or a call into a peer (R1);
    /// poisoning is recovered because the maps stay valid whatever thread
    /// panicked while holding them (R11).
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Admits a peer, attributed to `fiber` for ledger purposes.
    pub fn register_peer(&self, fiber: Option<FiberId>) -> PeerId {
        let mut state = self.lock();
        state.next_peer += 1;
        let id = state.next_peer;
        state.peers.insert(
            id,
            PeerRecord {
                fiber,
                grants: std::collections::HashSet::new(),
            },
        );
        id
    }

    /// Grants `peer` the named contract (constitution 01: grants arrive from
    /// the profile/policy side; requests are not grants).
    pub fn grant(&self, peer: PeerId, contract: &str) {
        if let Some(record) = self.lock().peers.get_mut(&peer) {
            record.grants.insert(contract.to_owned());
        }
    }

    /// THE grant check, shared by every granted surface — resolve, provide,
    /// listen, and the host-provider dispatch. Authority arrives only as a
    /// grant; an ungranted call is refused and the refusal is a ledger
    /// event, never a default-accept (Law 1 mechanical closure;
    /// constitution 01 §Grants).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] with a grant-refused message.
    pub fn check_grant(&self, peer: PeerId, name: &str) -> Result<(), KernelError> {
        let (granted, fiber) = {
            let state = self.lock();
            (state.granted(peer, name), state.fiber_of(peer))
        };
        if granted {
            return Ok(());
        }
        self.ledger.append(
            LedgerEventKind::GrantRefused {
                contract: name.to_owned(),
            },
            fiber,
        );
        Err(refusal(
            ErrorCode::EffectFailed,
            format!("grant refused: {name}"),
        ))
    }

    /// Provides `contract` from `peer`. Providing is authority: it requires
    /// the contract's grant exactly as calling does (Law 1) — an ungranted
    /// provide is refused and recorded. A second provider for an occupied
    /// slot is refused — replacement is never silent (R9). Every provision
    /// bumps the contract's generation, so handles resolved against the
    /// previous provider go stale rather than silently retargeting.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] when `peer` holds no grant for `contract`;
    /// [`ErrorCode::DuplicateProvision`] when a different peer holds the slot.
    pub fn provide(
        &self,
        peer: PeerId,
        contract: &str,
        callable: Arc<dyn Peer>,
    ) -> Result<(), KernelError> {
        self.check_grant(peer, contract)?;
        let mut state = self.lock();
        if let Some(existing) = state.providers.get(contract)
            && existing.peer != peer
        {
            return Err(refusal(
                ErrorCode::DuplicateProvision,
                format!("{contract} already has a live provider"),
            ));
        }
        let fiber = state.fiber_of(peer);
        let generation = state.generation_of(contract) + 1;
        state.generations.insert(contract.to_owned(), generation);
        state.providers.insert(
            contract.to_owned(),
            Provider {
                peer,
                callable,
                generation,
            },
        );
        drop(state);
        self.ledger.append(
            LedgerEventKind::ServiceProvided {
                service: contract.to_owned(),
            },
            fiber,
        );
        Ok(())
    }

    /// Withdraws `peer`'s provision of `contract`, if it holds the slot.
    pub fn withdraw(&self, peer: PeerId, contract: &str) {
        let mut state = self.lock();
        let held = state
            .providers
            .get(contract)
            .is_some_and(|provider| provider.peer == peer);
        if !held {
            return;
        }
        state.providers.remove(contract);
        let fiber = state.fiber_of(peer);
        drop(state);
        self.ledger.append(
            LedgerEventKind::ServiceWithdrawn {
                service: contract.to_owned(),
            },
            fiber,
        );
    }

    /// Removes a peer entirely: its provisions withdraw and its handles die.
    /// Instance disposal withdraws exactly its contribution (I1).
    pub fn remove_peer(&self, peer: PeerId) {
        let provided: Vec<String> = {
            let state = self.lock();
            state
                .providers
                .iter()
                .filter(|(_, provider)| provider.peer == peer)
                .map(|(contract, _)| contract.clone())
                .collect()
        };
        for contract in provided {
            self.withdraw(peer, &contract);
        }
        let mut state = self.lock();
        state.handles.retain(|_, handle| handle.owner != peer);
        state.peers.remove(&peer);
    }

    /// Resolves `contract` for `caller` under the grant check. A refusal is
    /// a ledger event exactly as an exercise is (constitution 01 §Grants).
    /// The minted handle pins the provider generation it resolved against:
    /// a later provider change refuses the handle instead of retargeting it.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] with a grant-refused message when the
    /// caller holds no grant for `contract`.
    pub fn resolve(&self, caller: PeerId, contract: &str) -> Result<HandleId, KernelError> {
        self.check_grant(caller, contract)?;
        let mut state = self.lock();
        let fiber = state.fiber_of(caller);
        state.next_handle += 1;
        let handle = state.next_handle;
        let generation = state.generation_of(contract);
        state.handles.insert(
            handle,
            Handle {
                owner: caller,
                contract: contract.to_owned(),
                generation,
            },
        );
        drop(state);
        self.ledger.append(
            LedgerEventKind::ContractResolved {
                contract: contract.to_owned(),
            },
            fiber,
        );
        Ok(handle)
    }
}
