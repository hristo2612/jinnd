//! The kernel surfaces a guest imports, implemented over the store data:
//! effect registration, broker provide/resolve/call, and the event port —
//! every crossing lands at the broker/topics seams (Law 2, R6; C3/C4).

use std::sync::Arc;

use crate::bindings;
use crate::instance::HostState;

impl bindings::types::Host for HostState {}

impl HostState {
    /// Admits one registration into the seat's journal (M2-K4, FINDINGS
    /// #15): refused, on the record with the fiber's attribution, once the
    /// seat sealed for withdrawal — fail-closed, like grant admission, so a
    /// dispose trail is exactly the fiber's contribution (I1), never a
    /// prefix of a journal something escaped.
    ///
    /// # Errors
    ///
    /// The sealed refusal ([`jinnd_api::ErrorCode::InactiveContext`]).
    pub(crate) fn admit(&self, what: &str) -> Result<(), jinnd_api::KernelError> {
        if self.seat.slot.as_ref().is_none_or(|slot| !slot.sealed()) {
            return Ok(());
        }
        let mut error = crate::instance::sealed_error();
        error.message = format!("{what} {}", error.message);
        self.seat.broker.ledger().append(
            jinnd_api::LedgerEventKind::ErrorRecorded {
                error: error.clone(),
            },
            self.seat.fiber,
        );
        Err(error)
    }
}

impl HostState {
    /// Refuses a guest write to a kernel-reserved topic (M2-K13), on the
    /// record with the fiber's attribution. Only the kernel publishes
    /// there; a guest holding the topic's grant may LISTEN, never emit.
    ///
    /// # Errors
    ///
    /// The reservation refusal ([`jinnd_api::ErrorCode::EffectFailed`]).
    fn reserve(&self, topic: &str) -> Result<(), jinnd_api::KernelError> {
        if !crate::topics::reserved(topic) {
            return Ok(());
        }
        let error = jinnd_api::KernelError {
            code: jinnd_api::ErrorCode::EffectFailed,
            message: format!("{topic} is published by the kernel; a guest may only listen on it"),
            fiber: self.seat.fiber,
        };
        self.seat.broker.ledger().append(
            jinnd_api::LedgerEventKind::ErrorRecorded {
                error: error.clone(),
            },
            self.seat.fiber,
        );
        Err(error)
    }
}

impl bindings::effects::Host for HostState {
    async fn register(
        &mut self,
        label: String,
        token: u64,
    ) -> Result<u64, bindings::types::KernelError> {
        self.admit("effect").map_err(bindings::wire_error)?;
        let index = self.outcome.effects().count() as u64;
        self.outcome
            .registrations
            .push(crate::handle::Registration::Effect { label, token });
        Ok(index)
    }
}

impl bindings::services::Host for HostState {
    async fn provide(&mut self, contract: String) -> Result<u64, bindings::types::KernelError> {
        self.admit("provide").map_err(bindings::wire_error)?;
        let index = self.outcome.provisions().count() as u64;
        if self.seat.staging {
            // A staged provision is recorded, not routed (R8) — but it is
            // grant-checked NOW, exactly as a live one: refusal fails the
            // health gate instead of surfacing at commit (Law 1).
            self.seat
                .broker
                .check_grant(self.seat.peer, &contract)
                .map_err(bindings::wire_error)?;
        } else {
            let face: Arc<dyn crate::peer::Peer> = match &self.seat.slot {
                Some(slot) => Arc::new(Arc::clone(slot)),
                None => self.face.clone(),
            };
            self.seat
                .broker
                .provide(self.seat.peer, &contract, face)
                .map_err(bindings::wire_error)?;
        }
        self.outcome
            .registrations
            .push(crate::handle::Registration::Provision { contract });
        Ok(index)
    }

    async fn resolve(&mut self, contract: String) -> Result<u64, bindings::types::KernelError> {
        self.seat
            .broker
            .resolve(self.seat.peer, &contract)
            .map_err(bindings::wire_error)
    }

    async fn call(
        &mut self,
        handle: u64,
        operation: String,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, bindings::types::KernelError> {
        // A call that would close a wait cycle is refused TYPED (M2-K10):
        // the guest is handed both ends and the wait between them, not a
        // sentence — and not a five-second stall ending in two dead
        // fibers.
        self.seat
            .broker
            .call_or_refuse(self.seat.peer, handle, &operation, payload)
            .map_err(|cycle| bindings::wire_cycle(&cycle))?
            .await
            .map_err(bindings::wire_error)
    }
}

impl bindings::events::Host for HostState {
    async fn emit(
        &mut self,
        topic: String,
        mode: bindings::types::DispatchMode,
        target: bindings::types::Selector,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, bindings::types::KernelError> {
        // A kernel-reserved topic is PUBLISHED, never emitted (M2-K13): a
        // guest that could write to it could hand a catalog a transition
        // the kernel never committed, which is the fabrication class the
        // whole publish path exists to end. The refusal is a ledger event
        // like every other authority refusal (Law 1, Law 2).
        self.reserve(&topic).map_err(bindings::wire_error)?;
        let report = self
            .seat
            .topics
            .emit(
                self.seat.context,
                &topic,
                bindings::api_mode(mode),
                &bindings::api_selector(target),
                payload,
                self.seat.fiber,
                self.seat.oracle.as_ref(),
            )
            .await;
        // The reply-expecting refusal (M2-K9): the walk never dispatched,
        // so the guest is told so — typed, naming the target and its own
        // next move — instead of waiting on an incarnation the kernel is
        // already taking down.
        // The wait-cycle refusal (M2-K10), in every mode: the walk never
        // dispatched because delivering it would have parked the emitter
        // on a listener that is already parked on the emitter.
        if let Some(cycle) = &report.cycle {
            return Err(bindings::wire_cycle(cycle));
        }
        if let Some(refused) = report.refused {
            return Err(bindings::wire_refusal(&topic, &refused));
        }
        Ok(report.outputs)
    }

    async fn listen(
        &mut self,
        topic: String,
        token: u64,
    ) -> Result<u64, bindings::types::KernelError> {
        self.admit("listen").map_err(bindings::wire_error)?;
        // Subscriptions are covered by the contract grant in v0.1
        // (constitution 01 §Grants): listening on a topic requires the grant
        // of the topic's name, and the refusal is a ledger event (Law 1).
        // A kernel-reserved topic belongs to the contract whose authority
        // bounds its payload, and is gated by THAT grant (M2-K13) — the
        // same check, on the name the contract is granted under.
        self.seat
            .broker
            .check_grant(self.seat.peer, crate::topics::grant_for(&topic))
            .map_err(bindings::wire_error)?;
        if self.seat.staging {
            // Recorded, not routed (R8): the registration is committed at
            // swap commit, against the new instance's own delivery face.
            self.outcome
                .registrations
                .push(crate::handle::Registration::Listen(
                    crate::handle::ListenRecord {
                        topic,
                        token,
                        id: None,
                    },
                ));
            return Ok(0);
        }
        // The delivery target is THIS instance's own face: a token pairs
        // with the instance that minted it, never rebound through a slot.
        let id = self.seat.topics.listen(
            &topic,
            self.seat.context,
            token,
            self.seat.fiber,
            self.face.clone() as Arc<dyn crate::topics::EventTarget>,
        );
        self.outcome
            .registrations
            .push(crate::handle::Registration::Listen(
                crate::handle::ListenRecord {
                    topic,
                    token,
                    id: Some(id),
                },
            ));
        Ok(id)
    }
}
