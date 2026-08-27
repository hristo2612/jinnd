//! One fiber's instance slot: the level of indirection Mode-1 hot-swap needs
//! (R8). The broker and the topic registry hold the SLOT as the provider/
//! listener face, so committing a swap redirects every route atomically by
//! installing the new handle — the old instance stays warm and fully routed
//! until that instant, and nothing ever re-provides.

use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{ErrorCode, KernelError, KernelFuture};

use crate::handle::InstanceHandle;
use crate::peer::{Peer, PeerId};
use crate::topics::EventTarget;

/// The live instance behind one fiber, swappable in place.
#[derive(Default)]
pub struct SharedSlot {
    current: Mutex<Option<InstanceHandle>>,
}

fn empty() -> KernelError {
    KernelError {
        code: ErrorCode::PluginFailed,
        message: "the fiber has no live instance".to_owned(),
        fiber: None,
    }
}

impl SharedSlot {
    fn lock(&self) -> MutexGuard<'_, Option<InstanceHandle>> {
        self.current
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Installs `handle` as the live instance, returning the displaced one —
    /// the swap-commit primitive. No lock is held across either instance.
    pub fn install(&self, handle: InstanceHandle) -> Option<InstanceHandle> {
        self.lock().replace(handle)
    }

    /// Empties the slot (teardown), returning the instance to dispose.
    pub fn take(&self) -> Option<InstanceHandle> {
        self.lock().take()
    }

    /// The live instance, if any.
    pub fn current(&self) -> Option<InstanceHandle> {
        self.lock().clone()
    }
}

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

impl EventTarget for Arc<SharedSlot> {
    fn deliver(&self, token: u64, topic: &str, payload: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        let current = self.current();
        let topic = topic.to_owned();
        Box::pin(async move {
            match current {
                Some(instance) => instance.deliver(token, &topic, payload).await,
                None => Err(empty()),
            }
        })
    }
}
