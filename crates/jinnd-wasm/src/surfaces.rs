//! The kernel surfaces a guest imports, implemented over the store data:
//! effect registration, broker provide/resolve/call, and the event port —
//! every crossing lands at the broker/topics seams (Law 2, R6; C3/C4).

use std::sync::Arc;

use crate::bindings;
use crate::instance::HostState;

impl bindings::types::Host for HostState {}

impl bindings::effects::Host for HostState {
    async fn register(
        &mut self,
        label: String,
        token: u64,
    ) -> Result<u64, bindings::types::KernelError> {
        let index = self.outcome.effects.len() as u64;
        self.outcome.effects.push((label, token));
        Ok(index)
    }
}

impl bindings::services::Host for HostState {
    async fn provide(&mut self, contract: String) -> Result<u64, bindings::types::KernelError> {
        let index = self.outcome.provisions.len() as u64;
        if !self.seat.staging {
            let face: Arc<dyn crate::peer::Peer> = match &self.seat.slot {
                Some(slot) => Arc::new(Arc::clone(slot)),
                None => self.face.clone(),
            };
            self.seat
                .broker
                .provide(self.seat.peer, &contract, face)
                .map_err(bindings::wire_error)?;
        }
        self.outcome.provisions.push(contract);
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
        self.seat
            .broker
            .call(self.seat.peer, handle, &operation, payload)
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
        let report = self
            .seat
            .topics
            .emit(
                self.seat.context,
                &topic,
                bindings::api_mode(mode),
                &bindings::api_selector(target),
                payload,
                self.seat.oracle.as_ref(),
            )
            .await;
        Ok(report.outputs)
    }

    async fn listen(
        &mut self,
        topic: String,
        token: u64,
    ) -> Result<u64, bindings::types::KernelError> {
        if self.seat.staging {
            self.outcome.listens.push(0);
            return Ok(0);
        }
        let target: Arc<dyn crate::topics::EventTarget> = match &self.seat.slot {
            Some(slot) => Arc::new(Arc::clone(slot)),
            None => self.face.clone(),
        };
        let id = self
            .seat
            .topics
            .listen(&topic, self.seat.context, token, target);
        self.outcome.listens.push(id);
        Ok(id)
    }
}
