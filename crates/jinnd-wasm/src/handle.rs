//! The channel-shaped face of one live component instance (R1): every guest
//! entry crosses an mpsc command into the instance's supervisor task — no
//! lock, no shared `Store`, cancellation by dropping. The same face is the
//! instance's broker transport ([`Peer`]) and event transport
//! ([`EventTarget`]): a contract call from a peer is a command like any
//! other, which is what keeps the broker transport-agnostic.

use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelError, KernelFuture};
use tokio::sync::{mpsc, oneshot};

use crate::peer::{Peer, PeerId};
use crate::topics::EventTarget;

/// What one activation contributed — ONE journal, in registration order:
/// the lane commits it into the fiber's live seat, so teardown withdraws
/// exactly this instance's contribution with this instance's own tokens by
/// replaying the journal in reverse (LIFO, LAW §3; R5, I1). Per-category
/// views are derived, never stored: there is no second list to iterate.
#[derive(Debug, Default)]
pub struct ActivationOutcome {
    /// Everything the activation registered, in the order it happened.
    pub registrations: Vec<Registration>,
}

/// One guest registration in the activation journal.
#[derive(Debug)]
pub enum Registration {
    /// A guest effect: its label and the undo token of THIS instance.
    Effect { label: String, token: u64 },
    /// A contract provided over the broker.
    Provision { contract: String },
    /// A topic listener registration.
    Listen(ListenRecord),
    /// A `jinn:clock` alarm request — an effect whose undo cancels the
    /// host-side alarm (M2-K2; R5).
    Alarm(AlarmRecord),
}

impl ActivationOutcome {
    /// The guest effects `(label, token)`, in registration order.
    pub fn effects(&self) -> impl DoubleEndedIterator<Item = (&str, u64)> {
        self.registrations
            .iter()
            .filter_map(|registration| match registration {
                Registration::Effect { label, token } => Some((label.as_str(), *token)),
                _ => None,
            })
    }

    /// The provided contracts, in registration order.
    pub fn provisions(&self) -> impl Iterator<Item = &str> {
        self.registrations
            .iter()
            .filter_map(|registration| match registration {
                Registration::Provision { contract } => Some(contract.as_str()),
                _ => None,
            })
    }

    /// The listener registrations, in registration order.
    pub fn listens(&self) -> impl Iterator<Item = &ListenRecord> {
        self.registrations
            .iter()
            .filter_map(|registration| match registration {
                Registration::Listen(record) => Some(record),
                _ => None,
            })
    }

    /// The alarm requests, in registration order (M2-K2).
    pub fn alarms(&self) -> impl Iterator<Item = &AlarmRecord> {
        self.registrations
            .iter()
            .filter_map(|registration| match registration {
                Registration::Alarm(record) => Some(record),
                _ => None,
            })
    }
}

/// One guest listener registration. A live activation carries the topic
/// registry id it registered under; a STAGED activation (the uncommitted
/// side of a Mode-1 swap) records topic and token only — the id is minted
/// at commit, against the new instance's own delivery face (R8; tokens
/// never leave the instance that registered them).
#[derive(Clone, Debug)]
pub struct ListenRecord {
    pub topic: String,
    pub token: u64,
    pub id: Option<u64>,
}

/// One guest alarm request (M2-K2). A live activation carries the alarm
/// registry id it was armed under; a STAGED activation records the request
/// only — the id is minted at commit, armed against the new instance's own
/// delivery face (R8; the seat's staged outcome carries alarms exactly
/// like any effect).
#[derive(Clone, Debug)]
pub struct AlarmRecord {
    /// The request's Law-2 label, shared by registration and withdrawal.
    pub label: String,
    pub spec: crate::alarms::AlarmSpec,
    pub token: u64,
    pub id: Option<u64>,
}

pub(crate) enum Command {
    Activate {
        config: Vec<u8>,
        reply: oneshot::Sender<(Result<(), KernelError>, ActivationOutcome)>,
    },
    Check {
        consumer: PeerId,
        reply: oneshot::Sender<bool>,
    },
    Undo {
        token: u64,
        reply: oneshot::Sender<Result<(), KernelError>>,
    },
    HandleCall {
        caller: PeerId,
        contract: String,
        operation: String,
        payload: Vec<u8>,
        reply: oneshot::Sender<Result<Vec<u8>, KernelError>>,
    },
    Deliver {
        token: u64,
        topic: String,
        payload: Vec<u8>,
        reply: oneshot::Sender<Result<Vec<u8>, KernelError>>,
    },
    Snapshot {
        reply: oneshot::Sender<Result<Vec<u8>, KernelError>>,
    },
    Restore {
        blob: Vec<u8>,
        reply: oneshot::Sender<Result<(), KernelError>>,
    },
    Shutdown,
}

pub(crate) fn gone() -> KernelError {
    KernelError {
        code: ErrorCode::PluginFailed,
        message: "the instance is gone".to_owned(),
        fiber: None,
    }
}

/// A handle onto one live instance. Cloneable; the instance dies when its
/// supervisor is told to [`InstanceHandle::dispose`] (or hits its deadline or
/// a trap), and every later call answers "gone" — never a hang.
#[derive(Clone)]
pub struct InstanceHandle {
    pub(crate) tx: mpsc::Sender<Command>,
}

impl InstanceHandle {
    async fn send<T>(&self, command: Command, rx: oneshot::Receiver<T>) -> Result<T, KernelError> {
        self.tx.send(command).await.map_err(|_| gone())?;
        rx.await.map_err(|_| gone())
    }

    /// Runs the plugin body once (LAW §3). The outcome always carries what
    /// the guest registered before settling — a failing activation still
    /// owes its inverses (I1).
    pub async fn activate(&self, config: Vec<u8>) -> (Result<(), KernelError>, ActivationOutcome) {
        let (reply, rx) = oneshot::channel();
        match self.send(Command::Activate { config, reply }, rx).await {
            Ok(settled) => settled,
            Err(error) => (Err(error), ActivationOutcome::default()),
        }
    }

    /// Runs one guest inverse (LIFO teardown replay, Law 3).
    pub async fn undo(&self, token: u64) -> Result<(), KernelError> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::Undo { token, reply }, rx).await?
    }

    /// One direct contract call onto this instance's provider face — the
    /// same entry `Peer::call` uses after broker dispatch, carrying the
    /// caller's identity (R4). Public for lane and observation code that
    /// already holds the instance (the broker path stays the only
    /// grant-checked one).
    pub async fn contract_call(
        &self,
        caller: PeerId,
        contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, KernelError> {
        let (reply, rx) = oneshot::channel();
        self.send(
            Command::HandleCall {
                caller,
                contract: contract.to_owned(),
                operation: operation.to_owned(),
                payload,
                reply,
            },
            rx,
        )
        .await?
    }

    /// One per-consumer vitality answer (C3), asked of this instance.
    pub async fn check(&self, consumer: PeerId) -> Result<bool, KernelError> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::Check { consumer, reply }, rx).await
    }

    /// One event delivery onto this instance's listener face.
    pub async fn deliver(
        &self,
        token: u64,
        topic: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, KernelError> {
        let (reply, rx) = oneshot::channel();
        self.send(
            Command::Deliver {
                token,
                topic: topic.to_owned(),
                payload,
                reply,
            },
            rx,
        )
        .await?
    }

    /// One state-handoff snapshot (R8 Mode 1).
    pub async fn snapshot(&self) -> Result<Vec<u8>, KernelError> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::Snapshot { reply }, rx).await?
    }

    /// Offers the predecessor's snapshot (R8 Mode 1). Refusal fails the
    /// swap's health gate.
    pub async fn restore(&self, blob: Vec<u8>) -> Result<(), KernelError> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::Restore { blob, reply }, rx).await?
    }

    /// Disposes the instance: its store drops inside the supervisor, so its
    /// memory, tables, and pending state vanish at once (R7 instant dispose;
    /// I1). Idempotent; safe on an already-dead instance.
    pub async fn dispose(&self) {
        let _ = self.tx.send(Command::Shutdown).await;
    }
}

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
