//! The instance's transport face: [`InstancePeer`] as broker [`Peer`] and
//! [`EventTarget`], plus the channel `pair` constructor. Split from
//! `handle.rs` by responsibility (R10 file hygiene).

use std::num::NonZeroU64;
use std::sync::Arc;

use jinnd_api::{KernelError, KernelFuture};
use tokio::sync::{mpsc, oneshot, watch};

use crate::peer::{Peer, PeerId};
use crate::settle::DeadlineControl;
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
    fn deliver(
        &self,
        token: u64,
        topic: &str,
        payload: Vec<u8>,
        budget: Option<NonZeroU64>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let handle = self.handle.clone();
        let topic = topic.to_owned();
        Box::pin(async move { handle.deliver_within(token, &topic, payload, budget).await })
    }
}

/// The death notice's two ends: the supervisor retains the terminal error
/// on the sender when the instance dies after activation (M2-K25); the
/// handle watches the receiver. The same shape carries the lane's abort.
pub(crate) type NoticeTx = watch::Sender<Option<KernelError>>;
pub(crate) type NoticeRx = watch::Receiver<Option<KernelError>>;

pub(crate) fn pair(
    deadline: std::time::Duration,
    staging: bool,
) -> (
    InstanceHandle,
    NoticeTx,
    NoticeRx,
    mpsc::Receiver<Command>,
    watch::Receiver<bool>,
) {
    let (tx, rx) = mpsc::channel(16);
    let (death_tx, deaths) = watch::channel(None);
    let (abort, aborts) = watch::channel(None);
    let (staging, staged) = watch::channel(staging);
    (
        InstanceHandle {
            tx,
            deaths,
            abort,
            deadline,
            horizon: DeadlineControl::new(),
            staging,
        },
        death_tx,
        aborts,
        rx,
        staged,
    )
}

/// The instance's own transport face, handed to the broker on `provide` and
/// to the topic registry on `listen`.
pub(crate) fn peer_face(handle: &InstanceHandle) -> Arc<InstancePeer> {
    Arc::new(InstancePeer {
        handle: handle.clone(),
    })
}

impl InstanceHandle {
    /// The seat is committed (M2-K26 amendment 2): every registration from
    /// here on routes live, and the supervisor routes the ones recorded
    /// while staging. Sync and infallible — run inside the commit's
    /// critical section (R8).
    pub(crate) fn commit_seat(&self) {
        self.staging.send_replace(false);
    }
}
