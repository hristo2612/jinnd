//! The readiness-wake task for one `jinn:net` socket (M2-K7, harness #23;
//! R1): tokio readiness outside every lock, one wake per readiness
//! transition the guest has not yet acted on (`wake.rs` holds the
//! decision), every wake a ledger event with the holder's attribution
//! (Law 2), delivered to the holding instance's own face through the
//! broker's peer target — the instance that owns the handle, as a Mode-1
//! swap re-attaches it.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{FiberId, LedgerEventKind};

use super::{HostNet, Socket, Wake};
use crate::peer::PeerId;

/// The topic every readiness wake is delivered under; the wake's token is
/// the socket handle (wit/plugin.wit `interface net`).
pub const READABLE_TOPIC: &str = "jinn:net/readable";

/// Waits for the socket to be readable: a listener with a pending
/// connection, a connection with bytes or EOF. Readiness is NOT cleared
/// here — the guest's own accept/read does that — so a wake never
/// consumes what it announces.
async fn readable(socket: &Socket) -> std::io::Result<()> {
    match socket {
        Socket::Listener { listener, .. } => listener.readable().await.map(|guard| {
            // Dropping the guard without clearing keeps the readiness.
            drop(guard);
        }),
        Socket::Conn { stream, .. } => stream.readable().await,
    }
}

/// One socket's wake loop: wait until armed, wait for readiness, claim the
/// wake (the ledger append rides the claim), deliver outside every lock.
/// A `take` (close, suspend, dispose) or a re-arm kicks `notify`, so the
/// loop re-reads the row instead of sleeping on a dead socket.
pub(super) async fn run(
    provider: Arc<HostNet>,
    handle: u64,
    socket: Socket,
    wake: Arc<Wake>,
    caller: PeerId,
    fiber: Option<FiberId>,
) {
    loop {
        match provider.wakes.armed(handle) {
            None => return,
            Some(false) => {
                wake.notify.notified().await;
                continue;
            }
            Some(true) => {}
        }
        tokio::select! {
            // An io error reads as readiness: the guest's next read answers it.
            _ = readable(&socket) => {}
            () = wake.notify.notified() => continue,
        }
        // A peer with no delivery face (a native caller) holds no wake
        // shape: disarm until it acts again, nothing is ledgered.
        let Some(target) = provider.core.target_of(caller) else {
            provider.wakes.claim_wake(handle, || {});
            continue;
        };
        let claimed = provider.wakes.claim_wake(handle, || {
            provider
                .core
                .sink
                .append(LedgerEventKind::NetReadable { handle }, fiber);
        });
        if !claimed {
            if !provider.wakes.alive(handle) {
                return;
            }
            continue;
        }
        wake.delivering.store(true, Ordering::SeqCst);
        let delivered = target
            .deliver(handle, READABLE_TOPIC, handle.to_le_bytes().to_vec())
            .await;
        wake.delivering.store(false, Ordering::SeqCst);
        if let Err(error) = delivered
            && provider.wakes.alive(handle)
        {
            // Still live: a real contained wake-handler failure, never the
            // benign race with a release that just took the row (R6, R11).
            provider
                .core
                .sink
                .append(LedgerEventKind::ErrorRecorded { error }, fiber);
        }
    }
}
