//! The two guards every guest entry runs behind: the lane's abort notice
//! interrupting a call in flight, and the death that records itself on the
//! fiber that died (M2-K25(c), Law 2). Split from `instance.rs` by
//! responsibility (R10 file hygiene).

use std::future::Future;

use jinnd_api::{KernelError, LedgerEventKind};
use tokio::sync::watch;
use wasmtime::Store;

use crate::settle::hung;

use super::HostState;

pub(super) async fn interrupted<T>(
    aborts: &mut watch::Receiver<Option<KernelError>>,
    work: impl Future<Output = T>,
) -> Result<T, KernelError> {
    if let Some(error) = aborts.borrow_and_update().clone() {
        return Err(error);
    }
    tokio::select! {
        biased;
        _ = aborts.changed() => Err(aborts.borrow_and_update().clone().unwrap_or_else(hung)),
        value = work => Ok(value),
    }
}

pub(super) fn die(
    store: &Store<HostState>,
    deaths: &watch::Sender<Option<KernelError>>,
    active: bool,
    mut error: KernelError,
) -> KernelError {
    if !active {
        return error;
    }
    error.fiber = store.data().seat.fiber;
    store.data().seat.broker.ledger().append(
        LedgerEventKind::ErrorRecorded {
            error: error.clone(),
        },
        store.data().seat.fiber,
    );
    deaths.send_replace(Some(error.clone()));
    error
}
