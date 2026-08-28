//! The slot as the broker's provider face: a contract call or vitality
//! check routes to whichever instance is live at that instant — the Mode-1
//! swap commit's atomic redirect (R8). Split from `slot.rs` by the 300-line
//! file cap (R10).

use std::sync::Arc;

use jinnd_api::KernelFuture;

use crate::peer::{Peer, PeerId};

use super::{SharedSlot, empty};

impl Peer for Arc<SharedSlot> {
    fn call(
        &self,
        caller: PeerId,
        contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let current = self.current();
        let (contract, operation) = (contract.to_owned(), operation.to_owned());
        Box::pin(async move {
            match current {
                Some(instance) => {
                    instance
                        .contract_call(caller, &contract, &operation, payload)
                        .await
                }
                None => Err(empty()),
            }
        })
    }

    fn check(&self, consumer: PeerId) -> KernelFuture<'static, bool> {
        let current = self.current();
        Box::pin(async move {
            match current {
                Some(instance) => instance.check(consumer).await,
                None => Ok(false),
            }
        })
    }
}
