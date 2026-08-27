//! The broker's dispatch lanes — handle calls, the handle-less
//! host-provider dispatch, and per-consumer vitality — split from
//! `broker.rs` by responsibility (R10 file hygiene). Same struct, same
//! single choke point: grant check → ledger append → dispatch (R6).

use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelFuture, LedgerEventKind};

use crate::broker_state::refusal;
use crate::peer::{HandleId, PeerId};

use super::Broker;

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
        let operation = operation.to_owned();
        let looked_up = {
            let state = self.lock();
            let fiber = state.fiber_of(caller);
            match state.handles.get(&handle) {
                Some(record) if record.owner == caller => {
                    let contract = record.contract.clone();
                    let provider = state
                        .providers
                        .get(&contract)
                        .map(|provider| (Arc::clone(&provider.callable), provider.generation));
                    Ok((contract, record.generation, provider, fiber))
                }
                _ => Err(refusal(
                    ErrorCode::EffectFailed,
                    "the handle is not the caller's".to_owned(),
                )),
            }
        };
        match looked_up {
            Err(error) => Box::pin(async move { Err(error) }),
            Ok((contract, pinned, provider, fiber)) => match provider {
                Some((_, generation)) if generation != pinned => {
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
                provider => {
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
                        Some((callable, _)) => {
                            callable.call(caller, &contract, &operation, payload)
                        }
                    }
                }
            },
        }
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
        if let Err(refused) = self.check_grant(caller, contract) {
            return Box::pin(async move { Err(refused) });
        }
        let (provider, fiber) = {
            let state = self.lock();
            (
                state
                    .providers
                    .get(contract)
                    .map(|provider| Arc::clone(&provider.callable)),
                state.fiber_of(caller),
            )
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
            Some(callable) => callable.call(caller, contract, operation, payload),
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
