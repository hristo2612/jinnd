//! The transport-agnostic capability broker — the single dispatch point
//! (decision log 2026-08-25, binding on this packet): grant check → ledger
//! append → dispatch, accepting "a contract call from a peer". Whether the
//! peer is a linked WASM instance or the conformance harness is a transport
//! detail behind [`Peer`]; a broker fused to wasmtime linking is a design
//! defect. The harness lane routes through THIS SAME broker (test-harness
//! ruling closure c).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind};

use crate::peer::{HandleId, LedgerSink, Peer, PeerId};

struct PeerRecord {
    fiber: Option<FiberId>,
    grants: HashSet<String>,
}

struct Provider {
    peer: PeerId,
    callable: Arc<dyn Peer>,
}

struct Handle {
    owner: PeerId,
    contract: String,
}

#[derive(Default)]
struct State {
    peers: HashMap<PeerId, PeerRecord>,
    providers: HashMap<String, Provider>,
    handles: HashMap<HandleId, Handle>,
    next_peer: PeerId,
    next_handle: HandleId,
}

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
                grants: HashSet::new(),
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

    fn fiber_of(state: &State, peer: PeerId) -> Option<FiberId> {
        state.peers.get(&peer).and_then(|record| record.fiber)
    }

    /// Provides `contract` from `peer`. A second provider for an occupied
    /// slot is refused — replacement is never silent (R9).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::DuplicateProvision`] when a different peer holds the slot.
    pub fn provide(
        &self,
        peer: PeerId,
        contract: &str,
        callable: Arc<dyn Peer>,
    ) -> Result<(), KernelError> {
        let mut state = self.lock();
        if let Some(existing) = state.providers.get(contract)
            && existing.peer != peer
        {
            return Err(refusal(
                ErrorCode::DuplicateProvision,
                format!("{contract} already has a live provider"),
            ));
        }
        let fiber = Self::fiber_of(&state, peer);
        state
            .providers
            .insert(contract.to_owned(), Provider { peer, callable });
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
        let fiber = Self::fiber_of(&state, peer);
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

    /// Resolves `contract` for `caller`: THE grant check. A refusal is a
    /// ledger event exactly as an exercise is (constitution 01 §Grants).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] with a grant-refused message when the
    /// caller holds no grant for `contract`.
    pub fn resolve(&self, caller: PeerId, contract: &str) -> Result<HandleId, KernelError> {
        let mut state = self.lock();
        let fiber = Self::fiber_of(&state, caller);
        let granted = state
            .peers
            .get(&caller)
            .is_some_and(|record| record.grants.contains(contract));
        if !granted {
            drop(state);
            self.ledger.append(
                LedgerEventKind::GrantRefused {
                    contract: contract.to_owned(),
                },
                fiber,
            );
            return Err(refusal(
                ErrorCode::EffectFailed,
                format!("grant refused: {contract}"),
            ));
        }
        state.next_handle += 1;
        let handle = state.next_handle;
        state.handles.insert(
            handle,
            Handle {
                owner: caller,
                contract: contract.to_owned(),
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

    /// One contract call: validate the caller-scoped handle, append the
    /// crossing, then dispatch to the providing peer — with no broker lock
    /// held across the peer (R1).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] for a handle the caller does not own;
    /// [`ErrorCode::MissingDependency`] when the contract has no live
    /// provider; the provider's own contained failure otherwise.
    pub fn call(
        &self,
        caller: PeerId,
        handle: HandleId,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let operation = operation.to_owned();
        let looked_up = {
            let state = self.lock();
            let fiber = Self::fiber_of(&state, caller);
            match state.handles.get(&handle) {
                Some(record) if record.owner == caller => {
                    let contract = record.contract.clone();
                    let provider = state
                        .providers
                        .get(&contract)
                        .map(|provider| Arc::clone(&provider.callable));
                    Ok((contract, provider, fiber))
                }
                _ => Err(refusal(
                    ErrorCode::EffectFailed,
                    "the handle is not the caller's".to_owned(),
                )),
            }
        };
        match looked_up {
            Err(error) => Box::pin(async move { Err(error) }),
            Ok((contract, provider, fiber)) => {
                self.ledger.append(
                    LedgerEventKind::ContractCall {
                        contract: contract.clone(),
                        operation: operation.clone(),
                    },
                    fiber,
                );
                match provider {
                    None => Box::pin(async move {
                        Err(refusal(
                            ErrorCode::MissingDependency,
                            format!("{contract} has no live provider"),
                        ))
                    }),
                    Some(callable) => callable.call(&contract, &operation, payload),
                }
            }
        }
    }

    /// One per-consumer vitality check (C3): routed to the providing peer,
    /// per notify — the seam shape is expressible over the broker, so a WASM
    /// provider answers a check call like any contract crossing.
    pub fn vitality(&self, contract: &str, consumer: PeerId) -> KernelFuture<'static, bool> {
        let provider = {
            let state = self.lock();
            state
                .providers
                .get(contract)
                .map(|provider| Arc::clone(&provider.callable))
        };
        match provider {
            None => Box::pin(async { Ok(false) }),
            Some(callable) => callable.check(consumer),
        }
    }
}

fn refusal(code: ErrorCode, message: String) -> KernelError {
    KernelError {
        code,
        message,
        fiber: None,
    }
}
