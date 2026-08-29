//! The `jinn:net` rows: one held socket with its wake-task handle (split
//! from `hostnet.rs` by responsibility, R10 file hygiene).

use std::sync::Arc;

use jinnd_api::FiberId;
use tokio::io::unix::AsyncFd;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

use crate::hostbase::Owned;
use crate::peer::PeerId;

/// One held socket: a listener or an accepted connection, owned by the
/// peer that minted it (R4), with the kick its wake task listens on.
#[derive(Clone)]
pub(super) enum Socket {
    Listener {
        owner: PeerId,
        fiber: Option<FiberId>,
        /// A non-blocking std listener behind tokio readiness: accept
        /// stays non-blocking, and readiness is observable without
        /// accepting (the wake task's need).
        listener: Arc<AsyncFd<std::net::TcpListener>>,
        wake: Arc<Wake>,
    },
    Conn {
        owner: PeerId,
        fiber: Option<FiberId>,
        stream: Arc<TcpStream>,
        wake: Arc<Wake>,
    },
}

/// One socket's wake task: the kick it listens on, the task itself —
/// aborted and awaited at release, so the released socket's descriptor
/// is closed when `withdraw` returns — and whether it is mid-delivery: a
/// guest closing the socket from INSIDE its own readiness handler is
/// released without awaiting the task that is awaiting that handler (the
/// descriptor then closes as the handler returns), never a deadlock.
#[derive(Default)]
pub(super) struct Wake {
    pub(super) notify: Notify,
    pub(super) task: Mutex<Option<JoinHandle<()>>>,
    pub(super) delivering: std::sync::atomic::AtomicBool,
}

impl Socket {
    pub(super) fn fiber(&self) -> Option<FiberId> {
        match self {
            Self::Listener { fiber, .. } | Self::Conn { fiber, .. } => *fiber,
        }
    }

    pub(super) fn wake(&self) -> &Arc<Wake> {
        match self {
            Self::Listener { wake, .. } | Self::Conn { wake, .. } => wake,
        }
    }
}

impl Owned for Socket {
    fn owner(&self) -> PeerId {
        match self {
            Self::Listener { owner, .. } | Self::Conn { owner, .. } => *owner,
        }
    }
}
