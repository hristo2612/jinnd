//! Topic registry unit tests (crate lane), split by what each half is
//! ABOUT: [`registry`] pins ordinary dispatch and tracing, [`refusal`]
//! pins the M2-K9 rule that a reply-expecting walk into a replaced
//! incarnation refuses whole, [`dispositions`] pins that each refusal is
//! named for the future it actually implies, [`race`] closes the hostile
//! interleaving around the swap commit, and [`cycle_restart`] pins which
//! refusal answers when a wait cycle forms around a peer that is at the
//! same moment restarting (M2-K10).
//!
//! The fixtures every half builds a registry out of live here, so no two
//! halves can drift about what a listener, a sink, or a doomed fiber is.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{EntryId, ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind, Owed};

use super::{EventTarget, RestartOracle, Unserved};
use crate::peer::LedgerSink;

mod cycle_restart;
mod dispositions;
mod race;
mod refusal;
mod registry;

#[derive(Default)]
pub(in crate::topics) struct RecordingSink(Mutex<Vec<(LedgerEventKind, Option<FiberId>)>>);

impl LedgerSink for RecordingSink {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((kind, fiber));
    }
}

impl RecordingSink {
    pub(in crate::topics) fn recorded(&self) -> Vec<(LedgerEventKind, Option<FiberId>)> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

struct Answer(Vec<u8>);

impl EventTarget for Answer {
    fn deliver(&self, _: u64, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        let answer = self.0.clone();
        Box::pin(async move { Ok(answer) })
    }
}

struct Failing;

impl EventTarget for Failing {
    fn deliver(&self, _: u64, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        Box::pin(async {
            Err(KernelError {
                code: ErrorCode::ListenerFailed,
                message: "listener failed".into(),
                fiber: None,
            })
        })
    }
}

/// A counting target: what it answered, and how often it was entered at
/// all — a refused walk must never enter one.
#[derive(Default)]
struct Counted(AtomicUsize);

impl EventTarget for Counted {
    fn deliver(&self, _: u64, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(b"served".to_vec()) })
    }
}

/// A fixed oracle: `doomed` names the one fiber that owes `owed`.
struct Doomed {
    doomed: FiberId,
    owed: Owed,
    asked: AtomicUsize,
}

impl RestartOracle for Doomed {
    fn unserved(&self, fiber: FiberId) -> Option<Unserved> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        (fiber == self.doomed).then(|| Unserved {
            entry: EntryId("consumer".to_owned()),
            incarnation: 7,
            owed: self.owed,
        })
    }
}

fn owing(fiber: FiberId, owed: Owed) -> Arc<Doomed> {
    Arc::new(Doomed {
        doomed: fiber,
        owed,
        asked: AtomicUsize::new(0),
    })
}

fn doomed(fiber: FiberId) -> Arc<Doomed> {
    owing(fiber, Owed::Reload)
}
