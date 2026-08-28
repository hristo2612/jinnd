//! The instance's transport face: [`InstancePeer`] as broker [`Peer`] and
//! [`EventTarget`], plus the channel `pair` constructor. Split from
//! `handle.rs` by responsibility (R10 file hygiene).

use std::sync::Arc;

use jinnd_api::KernelFuture;
use tokio::sync::{mpsc, oneshot};

use crate::peer::{Peer, PeerId};
use crate::topics::EventTarget;

use super::{Command, InstanceHandle};

/// The instance as a broker peer and event target.
pub(crate) struct InstancePeer {
    pub(crate) handle: InstanceHandle,
}

impl Peer for InstancePeer {
    fn call(
        &self,
        caller: PeerId,
        contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let handle = self.handle.clone();
        let (contract, operation) = (contract.to_owned(), operation.to_owned());
        Box::pin(async move {
            let (reply, rx) = oneshot::channel();
            handle
                .send(
                    Command::HandleCall {
                        caller,
                        contract,
                        operation,
                        payload,
                        reply,
                    },
                    rx,
                )
                .await?
        })
    }

    fn check(&self, consumer: PeerId) -> KernelFuture<'static, bool> {
        let handle = self.handle.clone();
        Box::pin(async move {
            let (reply, rx) = oneshot::channel();
            // A dead provider is not vital; the check itself never errors.
            Ok(handle
                .send(Command::Check { consumer, reply }, rx)
                .await
                .unwrap_or(false))
        })
    }
}

impl EventTarget for InstancePeer {
    fn deliver(&self, token: u64, topic: &str, payload: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        let handle = self.handle.clone();
        let topic = topic.to_owned();
        Box::pin(async move {
            let (reply, rx) = oneshot::channel();
            handle
                .send(
                    Command::Deliver {
                        token,
                        topic,
                        payload,
                        reply,
                    },
                    rx,
                )
                .await?
        })
    }
}

pub(crate) fn pair() -> (InstanceHandle, mpsc::Receiver<Command>) {
    let (tx, rx) = mpsc::channel(16);
    (InstanceHandle { tx }, rx)
}

/// The instance's own transport face, handed to the broker on `provide` and
/// to the topic registry on `listen`.
pub(crate) fn peer_face(handle: &InstanceHandle) -> Arc<InstancePeer> {
    Arc::new(InstancePeer {
        handle: handle.clone(),
    })
}
