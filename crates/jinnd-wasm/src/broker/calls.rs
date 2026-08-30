//! The broker's dispatch lanes — handle calls, the handle-less
//! host-provider dispatch, and per-consumer vitality — split from
//! `broker.rs` by responsibility (R10 file hygiene). Same struct, same
//! single choke point: grant check → ledger append → dispatch (R6).

use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelFuture, LedgerEventKind};

use crate::broker_state::refusal;
use crate::peer::{HandleId, PeerId};
use crate::waits::Cycle;

use super::Broker;

/// The untyped rendering of a wait-cycle refusal, for the lanes whose
/// error type cannot carry a record (M2-K10): the base host-provider
/// bundles and any non-guest caller. The guest surface takes the TYPED
/// [`Cycle`] instead — prose is never the primary channel (R3).
pub(crate) fn cycle_error(cycle: &Cycle) -> jinnd_api::KernelError {
    refusal(
        ErrorCode::DependencyCycle,
        format!(
            "wait cycle: {} cannot call {} on {} — {} is already awaiting it",
            cycle.waiter_name(),
            cycle.on,
            cycle.target_name(),
            cycle.target_name(),
        ),
    )
}

impl Broker {
    /// One contract call: validate the caller-scoped, generation-pinned
    /// handle, append the crossing, then dispatch to the providing peer with
    /// the caller's identity — and no broker lock held across the peer (R1).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] for a handle the caller does not own, or
    /// a stale handle (the provider generation changed since resolve — the
    /// refusal is recorded, never a silent retarget, R9);
    /// [`ErrorCode::MissingDependency`] when the contract has no live
    /// provider; the provider's own contained failure otherwise.
    pub fn call(
        &self,
        caller: PeerId,
        handle: HandleId,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        match self.call_or_refuse(caller, handle, operation, payload) {
            Ok(answer) => answer,
            Err(cycle) => {
                let error = cycle_error(&cycle);
                Box::pin(async move { Err(error) })
            }
        }
    }

    /// [`Broker::call`] with the wait-cycle refusal kept TYPED (M2-K10):
    /// the guest surface answers it with the wire record naming both ends,
    /// because a `kernel-error` whose payload is a record cannot be
    /// reconstructed from a message string (the M2-K9 precedent, R3).
    ///
    /// # Errors
    ///
    /// [`Cycle`] when the provider is, transitively, already awaiting the
    /// caller: nothing crossed, and the refusal is on the ledger.
    pub fn call_or_refuse(
        &self,
        caller: PeerId,
        handle: HandleId,
        operation: &str,
        payload: Vec<u8>,
    ) -> Result<KernelFuture<'static, Vec<u8>>, Cycle> {
        let operation = operation.to_owned();
        let looked_up = {
            let state = self.lock();
            let fiber = state.fiber_of(caller);
            match state.handles.get(&handle) {
                Some(record) if record.owner == caller => {
                    let contract = record.contract.clone();
                    let provider = state.providers.get(&contract).map(|provider| {
                        (
                            Arc::clone(&provider.callable),
                            provider.generation,
                            state.fiber_of(provider.peer),
                        )
                    });
                    Ok((contract, record.generation, provider, fiber))
                }
                _ => Err(refusal(
                    ErrorCode::EffectFailed,
                    "the handle is not the caller's".to_owned(),
                )),
            }
        };
        let (contract, pinned, provider, fiber) = match looked_up {
            Err(error) => return Ok(Box::pin(async move { Err(error) })),
            Ok(looked_up) => looked_up,
        };
        Ok(match provider {
            // The operation class is checked before the crossing is
            // appended (M2-K8): a refused operation never crosses.
            _ if self.check_op(caller, &contract, &operation).is_err() => {
                let error = refusal(
                    ErrorCode::EffectFailed,
                    format!(
                        "grant refused: {contract} {operation} is outside the granted operation class"
                    ),
                );
                Box::pin(async move { Err(error) })
            }
            Some((_, generation, _)) if generation != pinned => {
                self.ledger.append(
                    LedgerEventKind::StaleHandleRefused {
                        contract: contract.clone(),
                    },
                    fiber,
                );
                Box::pin(async move {
                    Err(refusal(
                        ErrorCode::EffectFailed,
                        format!("stale handle: {contract} changed provider since resolve"),
                    ))
                })
            }
            None => {
                self.ledger.append(
                    LedgerEventKind::ContractCall {
                        contract: contract.clone(),
                        operation: operation.clone(),
                    },
                    fiber,
                );
                Box::pin(async move {
                    Err(refusal(
                        ErrorCode::MissingDependency,
                        format!("{contract} has no live provider"),
                    ))
                })
            }
            Some((callable, _, provider)) => {
                // The wait is declared BEFORE the crossing is appended
                // (M2-K10): a call that would close a cycle never crosses,
                // and lands its refusal row instead of a `ContractCall`.
                let ticket = self.park(fiber, provider, &format!("{contract}.{operation}"))?;
                self.ledger.append(
                    LedgerEventKind::ContractCall {
                        contract: contract.clone(),
                        operation: operation.clone(),
                    },
                    fiber,
                );
                let answer = callable.call(caller, &contract, &operation, payload);
                Box::pin(async move {
                    // The ticket lives exactly as long as the wait it
                    // stands for: dropped here however the call ended,
                    // including a cancelled future.
                    let _ticket = ticket;
                    answer.await
                })
            }
        })
    }

    /// One handle-less contract crossing, used by the kernel-supplied base
    /// host-provider imports (R7: fs, process, net, keystore arrive as
    /// granted WIT imports). The same choke point in the same order — grant
    /// check → ledger append → dispatch — without minting a persistent
    /// handle: each dispatch binds to the CURRENT provider, so staleness
    /// cannot arise, and the caller's scope still travels with the call (R4).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] when the caller holds no grant (recorded);
    /// [`ErrorCode::MissingDependency`] when no provider is live; the
    /// provider's own contained failure otherwise.
    pub fn dispatch(
        &self,
        caller: PeerId,
        contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        if let Err(refused) = self
            .check_grant(caller, contract)
            .and_then(|()| self.check_op(caller, contract, operation))
        {
            return Box::pin(async move { Err(refused) });
        }
        let (provider, fiber) = {
            let state = self.lock();
            (
                state.providers.get(contract).map(|provider| {
                    (
                        Arc::clone(&provider.callable),
                        state.fiber_of(provider.peer),
                    )
                }),
                state.fiber_of(caller),
            )
        };
        // Every kernel-supplied base provider registers with no fiber, so
        // this crossing has no far end to close a cycle through and takes
        // no edge (M2-K10). The check is here anyway rather than as an
        // argument: a provider that DOES carry a fiber is refused like any
        // other, and this lane's bundle errors carry prose, not a record.
        let on = format!("{contract}.{operation}");
        let ticket = match self.park(fiber, provider.as_ref().and_then(|(_, at)| *at), &on) {
            Ok(ticket) => ticket,
            Err(cycle) => {
                let error = cycle_error(&cycle);
                return Box::pin(async move { Err(error) });
            }
        };
        self.ledger.append(
            LedgerEventKind::ContractCall {
                contract: contract.to_owned(),
                operation: operation.to_owned(),
            },
            fiber,
        );
        match provider {
            None => {
                let contract = contract.to_owned();
                Box::pin(async move {
                    Err(refusal(
                        ErrorCode::MissingDependency,
                        format!("{contract} has no live provider"),
                    ))
                })
            }
            Some((callable, _)) => {
                let answer = callable.call(caller, contract, operation, payload);
                Box::pin(async move {
                    let _ticket = ticket;
                    answer.await
                })
            }
        }
    }

    /// Withdraws one host-provider effect through its CURRENT provider
    /// (M2-K3 round 2; R5, M1-P9b): the owning seat's LIFO journal replay
    /// lands here for every `Registration::Host`. No ledger line of its own
    /// — the seat appends the withdrawal at the moment it runs (Law 2).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::MissingDependency`] when no provider is live; the
    /// provider's own refusal (an unknown effect, a failing inverse).
    pub fn withdraw_effect(&self, contract: &str, effect: u64) -> KernelFuture<'static, ()> {
        let provider = {
            let state = self.lock();
            state
                .providers
                .get(contract)
                .map(|provider| Arc::clone(&provider.callable))
        };
        match provider {
            None => {
                let contract = contract.to_owned();
                Box::pin(async move {
                    Err(refusal(
                        ErrorCode::MissingDependency,
                        format!("{contract} has no live provider to withdraw effect {effect}"),
                    ))
                })
            }
            Some(callable) => callable.withdraw(effect),
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
