//! The byte-lane event port for Tier A guests: topic-keyed listeners,
//! declarative selectors evaluated kernel-side (C4), five dispatch modes over
//! opaque payloads, listener failures contained (R9).
//!
//! This is the wasm boundary's port, not a second event bus: typed events and
//! their full temporal semantics stay in `jinnd-events`; bridging the two is
//! the bus ledger-tap packet's business (out of M1-P8 scope, per card). The
//! byte-lane mode rules are declared in `wit/plugin.wit`: a non-empty output
//! is decisive (bail) and replaces the payload (waterfall).

use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use jinnd_api::{DispatchMode, FiberId, KernelError, KernelFuture, LedgerEventKind};

use crate::peer::LedgerSink;
use crate::selector::{RealmOracle, Selector, selects};

mod restarting;

pub use restarting::{RestartOracle, Restarting};
use restarting::{expects_reply, refusal};

/// One event delivery answered by a listener's host — the transport seam,
/// like [`crate::broker::Peer`] for contract calls.
pub trait EventTarget: Send + Sync + 'static {
    fn deliver(&self, token: u64, topic: &str, payload: Vec<u8>) -> KernelFuture<'static, Vec<u8>>;
}

struct Listener {
    id: u64,
    topic: String,
    context: u64,
    token: u64,
    /// The fiber whose incarnation minted this registration (M2-K9): the
    /// identity a reply-expecting walk asks the restart oracle about.
    fiber: Option<FiberId>,
    target: Arc<dyn EventTarget>,
}

/// One listener registration for an atomic [`LocalTopics::rebind`].
pub struct Rebind {
    pub topic: String,
    pub context: u64,
    pub token: u64,
    /// The fiber the successor incarnation serves (M2-K9).
    pub fiber: Option<FiberId>,
    pub target: Arc<dyn EventTarget>,
}

#[derive(Default)]
struct Inner {
    listeners: Vec<Listener>,
    next: u64,
}

/// The settled outcome of one emit: outputs per the mode's byte-lane rule,
/// contained failures in observation order — never an aborted walk (R9).
#[derive(Debug, Default)]
pub struct EmitReport {
    pub outputs: Vec<Vec<u8>>,
    pub failures: Vec<KernelError>,
    /// The walk was REFUSED before any delivery (M2-K9): a selected
    /// listener's incarnation is already scheduled for replacement.
    /// Outputs and failures are empty — nothing ran.
    pub refused: Option<KernelError>,
}

/// Topic registry + dispatcher.
#[derive(Default)]
pub struct LocalTopics {
    inner: Mutex<Inner>,
    /// The dispatch-trace tap (M2-K2; Law 2): with a sink, every emit that
    /// DISPATCHES lands exactly one `DispatchTrace` after its walk
    /// settled; a walk refused before any delivery lands its
    /// `DispatchRefused` row instead (M2-K9). `None` keeps the registry a
    /// pure port (crate tests, pre-tap callers).
    sink: Option<Arc<dyn LedgerSink>>,
    /// The pending-restart oracle (M2-K9): set once by the assembly that
    /// owns the fibers. Unset, no walk is ever refused — the registry
    /// stays the pure port it is for crate tests.
    restarts: OnceLock<Arc<dyn RestartOracle>>,
}

impl LocalTopics {
    /// A registry whose every emit lands one `DispatchTrace` on `sink`.
    #[must_use]
    pub fn traced(sink: Arc<dyn LedgerSink>) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            sink: Some(sink),
            restarts: OnceLock::new(),
        }
    }

    /// Installs the pending-restart oracle (M2-K9): from here on every
    /// reply-expecting walk is decided against kernel-owned fiber state
    /// before it dispatches. Idempotent — a second install is ignored.
    pub fn watch_restarts(&self, oracle: Arc<dyn RestartOracle>) {
        let _ = self.restarts.set(oracle);
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Registers a listener; the returned id is its withdrawal key (listener
    /// registration is just an effect, LAW §3).
    pub fn listen(
        &self,
        topic: &str,
        context: u64,
        token: u64,
        fiber: Option<FiberId>,
        target: Arc<dyn EventTarget>,
    ) -> u64 {
        let mut inner = self.lock();
        inner.next += 1;
        let id = inner.next;
        inner.listeners.push(Listener {
            id,
            topic: topic.to_owned(),
            context,
            token,
            fiber,
            target,
        });
        id
    }

    /// Withdraws one registration, returning its topic — the caller's
    /// Law-2 withdrawal label. Idempotent: an already-withdrawn id is
    /// `None`, and nothing is appended for it.
    pub fn unlisten(&self, id: u64) -> Option<String> {
        let mut inner = self.lock();
        let index = inner
            .listeners
            .iter()
            .position(|listener| listener.id == id)?;
        Some(inner.listeners.remove(index).topic)
    }

    /// Atomically withdraws `old` and registers `new`, under ONE lock: no
    /// emit ever selects a half-swapped listener set — the Mode-1 commit
    /// shape (R8 atomic replacement).
    pub fn rebind(&self, old: &[u64], new: Vec<Rebind>) -> Vec<u64> {
        let mut inner = self.lock();
        inner
            .listeners
            .retain(|listener| !old.contains(&listener.id));
        new.into_iter()
            .map(|registration| {
                inner.next += 1;
                let id = inner.next;
                inner.listeners.push(Listener {
                    id,
                    topic: registration.topic,
                    context: registration.context,
                    token: registration.token,
                    fiber: registration.fiber,
                    target: registration.target,
                });
                id
            })
            .collect()
    }

    /// The first selected listener whose incarnation is already scheduled
    /// for replacement, in selection order (M2-K9). Answered by the
    /// oracle from kernel-owned state alone; without one, never.
    fn doomed(
        &self,
        selected: &[(Option<FiberId>, u64, Arc<dyn EventTarget>)],
    ) -> Option<Restarting> {
        let oracle = self.restarts.get()?;
        selected
            .iter()
            .filter_map(|(fiber, _, _)| *fiber)
            .find_map(|fiber| oracle.restarting(fiber))
    }

    /// Dispatches one payload: listeners are selected kernel-side from a
    /// snapshot (no lock is held across a delivery, R1), then walked per the
    /// mode. A failing listener is contained and recorded, never aborting a
    /// collecting walk (R9). With a trace sink, exactly one `DispatchTrace`
    /// lands after the walk settled, attributed to `fiber` — the append is
    /// fire-and-forget relative to the walk and never changes the report
    /// (M2-K2; Law 2, R11).
    #[allow(clippy::too_many_arguments)]
    pub async fn emit(
        &self,
        emitter: u64,
        topic: &str,
        mode: DispatchMode,
        selector: &Selector,
        payload: Vec<u8>,
        fiber: Option<FiberId>,
        oracle: &dyn RealmOracle,
    ) -> EmitReport {
        let selected: Vec<(Option<FiberId>, u64, Arc<dyn EventTarget>)> = {
            let inner = self.lock();
            inner
                .listeners
                .iter()
                .filter(|listener| listener.topic == topic)
                .filter(|listener| selects(selector, oracle, emitter, listener.context))
                .map(|listener| (listener.fiber, listener.token, Arc::clone(&listener.target)))
                .collect()
        };
        // A reply-expecting walk is DECIDED before it dispatches (M2-K9):
        // one selected listener in a doomed incarnation refuses the whole
        // walk, so a dispatch is never half-landed and never lands in an
        // incarnation the kernel is already taking down. The refusal is
        // the ledger row; a walk that dispatched nothing traces nothing.
        if expects_reply(mode)
            && let Some(target) = self.doomed(&selected)
        {
            if let Some(sink) = &self.sink {
                sink.append(
                    LedgerEventKind::DispatchRefused {
                        topic: topic.to_owned(),
                        mode,
                        target: target.entry.clone(),
                        incarnation: target.incarnation,
                    },
                    fiber,
                );
            }
            return EmitReport {
                refused: Some(refusal(topic, &target)),
                ..EmitReport::default()
            };
        }
        let listeners = selected.len();
        let mut report = EmitReport::default();
        match mode {
            DispatchMode::Emit | DispatchMode::Parallel | DispatchMode::Serial => {
                for (_, token, target) in selected {
                    match target.deliver(token, topic, payload.clone()).await {
                        Ok(output) => report.outputs.push(output),
                        Err(failure) => report.failures.push(failure),
                    }
                }
                if mode == DispatchMode::Emit {
                    report.outputs.clear();
                }
            }
            DispatchMode::Bail => {
                for (_, token, target) in selected {
                    match target.deliver(token, topic, payload.clone()).await {
                        Ok(output) if !output.is_empty() => {
                            report.outputs.push(output);
                            break;
                        }
                        Ok(_) => {}
                        Err(failure) => report.failures.push(failure),
                    }
                }
            }
            DispatchMode::Waterfall => {
                let mut current = payload;
                for (_, token, target) in selected {
                    match target.deliver(token, topic, current.clone()).await {
                        Ok(output) if !output.is_empty() => current = output,
                        Ok(_) => {}
                        Err(failure) => report.failures.push(failure),
                    }
                }
                report.outputs.push(current);
            }
        }
        if let Some(sink) = &self.sink {
            sink.append(
                LedgerEventKind::DispatchTrace {
                    topic: topic.to_owned(),
                    mode,
                    listeners: u32::try_from(listeners).unwrap_or(u32::MAX),
                    failures: u32::try_from(report.failures.len()).unwrap_or(u32::MAX),
                    emitter,
                },
                fiber,
            );
        }
        report
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests;
