//! The channel-shaped face of one live component instance (R1): every guest
//! entry crosses an mpsc command into the instance's supervisor task — no
//! lock, no shared `Store`, cancellation by dropping. The same face is the
//! instance's broker transport ([`Peer`]) and event transport
//! ([`EventTarget`]): a contract call from a peer is a command like any
//! other, which is what keeps the broker transport-agnostic.

use jinnd_api::KernelError;
use std::num::NonZeroU64;
use tokio::sync::{mpsc, oneshot};

use crate::peer::PeerId;

mod command;
mod face;

pub(crate) use command::{Command, gone};
pub(crate) use face::{InstancePeer, pair, peer_face};

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
    /// A host-provider effect registered on this instance's behalf (M2-K3:
    /// a `jinn:fs` write/append/remove) — withdrawn through the broker's
    /// current provider of the contract, LIFO with the rest (R5).
    Host(HostRecord),
    /// A host-provider KERNEL REGISTRATION on this instance's behalf
    /// (M2-K6: a `jinn:process` child, a `jinn:net` listener or
    /// connection) — released through the broker's current provider on
    /// suspend AND on dispose alike, never retained (M2-K4 class).
    Kernel(HostRecord),
}

/// One host-provider effect in the journal: the contract that owns the
/// inverse, the Law-2 label shared with the provider's ledger line, and
/// the provider's effect id.
#[derive(Clone, Debug)]
pub struct HostRecord {
    pub contract: String,
    pub label: String,
    pub effect: u64,
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
    pub budget: Option<NonZeroU64>,
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

/// A handle onto one live instance. Cloneable; the instance dies when its
/// supervisor is told to [`InstanceHandle::dispose`] (or hits its deadline or
/// a trap), and every later call answers "gone" — never a hang.
#[derive(Clone)]
pub struct InstanceHandle {
    pub(crate) tx: mpsc::Sender<Command>,
    pub(crate) deaths: tokio::sync::watch::Receiver<Option<KernelError>>,
}

impl InstanceHandle {
    /// Watches the retained terminal error when this live instance dies
    /// after activation (M2-K25). The lane uses it to fault the owning
    /// fiber without polling.
    pub fn deaths(&self) -> tokio::sync::watch::Receiver<Option<KernelError>> {
        self.deaths.clone()
    }

    pub(crate) fn same_instance(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx)
    }

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
        self.deliver_within(token, topic, payload, None).await
    }

    pub(crate) async fn deliver_within(
        &self,
        token: u64,
        topic: &str,
        payload: Vec<u8>,
        budget: Option<NonZeroU64>,
    ) -> Result<Vec<u8>, KernelError> {
        let (reply, rx) = oneshot::channel();
        self.send(
            Command::Deliver {
                token,
                topic: topic.to_owned(),
                payload,
                budget,
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

    /// Seals the instance ahead of its retirement or suspension (M2-K4,
    /// FINDINGS #15): resolves once every guest entry in flight has
    /// returned and committed its late registrations, after which the
    /// instance refuses new entries — the journal that teardown replays is
    /// then exactly the instance's contribution, never a prefix (I1).
    pub async fn seal(&self) {
        let (reply, rx) = oneshot::channel();
        let _ = self.send(Command::Seal { reply }, rx).await;
    }

    /// Disposes the instance: its store drops inside the supervisor, so its
    /// memory, tables, and pending state vanish at once (R7 instant dispose;
    /// I1). Idempotent; safe on an already-dead instance.
    pub async fn dispose(&self) {
        let _ = self.tx.send(Command::Shutdown).await;
    }
}
