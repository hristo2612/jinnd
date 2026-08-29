//! The shared core of a handle-minting native host provider (M2-K6; R10:
//! ONE table, one attribution path, one refusal shape for `jinn:process`
//! and `jinn:net`): rows keyed by minted handle and owned by the peer that
//! minted them (R4), the broker kept weakly for caller attribution and
//! typed policies, and the ledgered grant refusal (Law 2).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use jinnd_api::{ErrorCode, FiberId, KernelError, LedgerEventKind, RefusalReason};

use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::grants::GrantScope;
use crate::lane::lock;
use crate::peer::{LedgerSink, PeerId};

/// One row a provider holds under a handle: cheap to clone out of the
/// table (no lock is held across an await, R1), owned by one peer.
pub(crate) trait Owned: Clone {
    fn owner(&self) -> PeerId;
}

pub(crate) struct ProviderCore<T> {
    pub(crate) sink: Arc<dyn LedgerSink>,
    contract: &'static str,
    broker: OnceLock<Weak<Broker>>,
    table: Mutex<HashMap<u64, T>>,
    next: AtomicU64,
}

impl<T: Owned> ProviderCore<T> {
    pub(crate) fn new(contract: &'static str, sink: Arc<dyn LedgerSink>) -> Self {
        Self {
            sink,
            contract,
            broker: OnceLock::new(),
            table: Mutex::new(HashMap::new()),
            next: AtomicU64::new(0),
        }
    }

    /// Keeps the broker weakly: it owns the provider, never the reverse.
    pub(crate) fn attach(&self, broker: &Arc<Broker>) {
        let _ = self.broker.set(Arc::downgrade(broker));
    }

    fn broker(&self) -> Option<Arc<Broker>> {
        self.broker.get().and_then(Weak::upgrade)
    }

    /// The fiber attribution of one calling peer, through the broker.
    pub(crate) fn attribution(&self, caller: PeerId) -> Option<FiberId> {
        self.broker().and_then(|broker| broker.attribution(caller))
    }

    /// The calling peer's delivery face, through the broker (M2-K7).
    pub(crate) fn target_of(&self, caller: PeerId) -> Option<Arc<dyn crate::topics::EventTarget>> {
        self.broker().and_then(|broker| broker.target_of(caller))
    }

    /// The typed authority `caller` holds this contract under.
    pub(crate) fn policy(&self, caller: PeerId) -> Option<GrantScope> {
        self.broker()
            .and_then(|broker| broker.policy(caller, self.contract))
    }

    /// One ledgered grant refusal with the caller's attribution (Law 2),
    /// exactly like the broker's own: the typed class on the record, the
    /// prose beside it and on the wire (R3).
    pub(crate) fn refuse(
        &self,
        caller: PeerId,
        reason: RefusalReason,
        message: String,
    ) -> KernelError {
        self.sink.append(
            LedgerEventKind::GrantRefused {
                contract: self.contract.to_owned(),
                reason,
                detail: Some(message.clone()),
            },
            self.attribution(caller),
        );
        refusal(ErrorCode::EffectFailed, message)
    }

    /// A fresh handle (never reused).
    pub(crate) fn mint(&self) -> u64 {
        self.next.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub(crate) fn insert(&self, handle: u64, row: T) {
        lock(&self.table).insert(handle, row);
    }

    pub(crate) fn remove(&self, handle: u64) -> Option<T> {
        lock(&self.table).remove(&handle)
    }

    pub(crate) fn len(&self) -> usize {
        lock(&self.table).len()
    }

    /// The caller's own row, copied out under the lock (R4: a handle is
    /// valid only for the peer that minted it — another peer's use is a
    /// ledgered refusal; an unknown handle is the typed not-found).
    pub(crate) fn row(&self, caller: PeerId, handle: u64) -> Result<T, KernelError> {
        match lock(&self.table).get(&handle) {
            Some(row) if row.owner() == caller => Ok(row.clone()),
            Some(_) => Err(self.refuse(
                caller,
                RefusalReason::ForeignHandle,
                format!("{} handle {handle} is not the caller's", self.contract),
            )),
            None => Err(refusal(
                ErrorCode::NotFound,
                format!("unknown {} handle {handle}", self.contract),
            )),
        }
    }
}
