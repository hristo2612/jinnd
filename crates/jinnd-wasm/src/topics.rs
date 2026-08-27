//! The byte-lane event port for Tier A guests: topic-keyed listeners,
//! declarative selectors evaluated kernel-side (C4), five dispatch modes over
//! opaque payloads, listener failures contained (R9).
//!
//! This is the wasm boundary's port, not a second event bus: typed events and
//! their full temporal semantics stay in `jinnd-events`; bridging the two is
//! the bus ledger-tap packet's business (out of M1-P8 scope, per card). The
//! byte-lane mode rules are declared in `wit/plugin.wit`: a non-empty output
//! is decisive (bail) and replaces the payload (waterfall).

use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{DispatchMode, KernelError, KernelFuture};

use crate::selector::{RealmOracle, Selector, selects};

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
    target: Arc<dyn EventTarget>,
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
}

/// Topic registry + dispatcher.
#[derive(Default)]
pub struct LocalTopics {
    inner: Mutex<Inner>,
}

impl LocalTopics {
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
            target,
        });
        id
    }

    /// Withdraws one registration. Idempotent.
    pub fn unlisten(&self, id: u64) {
        self.lock().listeners.retain(|listener| listener.id != id);
    }

    /// Dispatches one payload: listeners are selected kernel-side from a
    /// snapshot (no lock is held across a delivery, R1), then walked per the
    /// mode. A failing listener is contained and recorded, never aborting a
    /// collecting walk (R9).
    pub async fn emit(
        &self,
        emitter: u64,
        topic: &str,
        mode: DispatchMode,
        selector: &Selector,
        payload: Vec<u8>,
        oracle: &dyn RealmOracle,
    ) -> EmitReport {
        let selected: Vec<(u64, Arc<dyn EventTarget>)> = {
            let inner = self.lock();
            inner
                .listeners
                .iter()
                .filter(|listener| listener.topic == topic)
                .filter(|listener| selects(selector, oracle, emitter, listener.context))
                .map(|listener| (listener.token, Arc::clone(&listener.target)))
                .collect()
        };
        let mut report = EmitReport::default();
        match mode {
            DispatchMode::Emit | DispatchMode::Parallel | DispatchMode::Serial => {
                for (token, target) in selected {
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
                for (token, target) in selected {
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
                for (token, target) in selected {
                    match target.deliver(token, topic, current.clone()).await {
                        Ok(output) if !output.is_empty() => current = output,
                        Ok(_) => {}
                        Err(failure) => report.failures.push(failure),
                    }
                }
                report.outputs.push(current);
            }
        }
        report
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::sync::Arc;

    use jinnd_api::{DispatchMode, ErrorCode, KernelError, KernelFuture};

    use super::{EventTarget, LocalTopics};
    use crate::selector::{NoRealms, Selector};

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

    #[tokio::test]
    async fn failing_listener_never_aborts_a_collecting_walk() {
        let topics = LocalTopics::default();
        topics.listen("t", 1, 0, Arc::new(Failing));
        topics.listen("t", 2, 0, Arc::new(Answer(b"ok".to_vec())));
        let report = topics
            .emit(
                0,
                "t",
                DispatchMode::Parallel,
                &Selector::All,
                Vec::new(),
                &NoRealms,
            )
            .await;
        assert_eq!(report.outputs, vec![b"ok".to_vec()]);
        assert_eq!(report.failures.len(), 1, "contained and recorded (R9)");
    }

    #[tokio::test]
    async fn bail_takes_the_first_non_empty_output_only() {
        let topics = LocalTopics::default();
        topics.listen("t", 1, 0, Arc::new(Answer(Vec::new())));
        topics.listen("t", 2, 0, Arc::new(Answer(b"first".to_vec())));
        topics.listen("t", 3, 0, Arc::new(Answer(b"second".to_vec())));
        let report = topics
            .emit(
                0,
                "t",
                DispatchMode::Bail,
                &Selector::All,
                Vec::new(),
                &NoRealms,
            )
            .await;
        assert_eq!(report.outputs, vec![b"first".to_vec()]);
    }

    #[tokio::test]
    async fn waterfall_folds_non_empty_outputs_into_the_payload() {
        let topics = LocalTopics::default();
        topics.listen("t", 1, 0, Arc::new(Answer(b"a".to_vec())));
        topics.listen("t", 2, 0, Arc::new(Answer(Vec::new())));
        topics.listen("t", 3, 0, Arc::new(Answer(b"b".to_vec())));
        let report = topics
            .emit(
                0,
                "t",
                DispatchMode::Waterfall,
                &Selector::All,
                b"seed".to_vec(),
                &NoRealms,
            )
            .await;
        assert_eq!(report.outputs, vec![b"b".to_vec()]);
    }

    #[tokio::test]
    async fn selector_and_unlisten_gate_delivery() {
        let topics = LocalTopics::default();
        let selected = topics.listen("t", 1, 0, Arc::new(Answer(b"in".to_vec())));
        topics.listen("t", 2, 0, Arc::new(Answer(b"out".to_vec())));
        let report = topics
            .emit(
                0,
                "t",
                DispatchMode::Serial,
                &Selector::ContextSet(vec![1]),
                Vec::new(),
                &NoRealms,
            )
            .await;
        assert_eq!(report.outputs, vec![b"in".to_vec()]);

        topics.unlisten(selected);
        topics.unlisten(selected);
        let after = topics
            .emit(
                0,
                "t",
                DispatchMode::Serial,
                &Selector::ContextSet(vec![1]),
                Vec::new(),
                &NoRealms,
            )
            .await;
        assert!(after.outputs.is_empty(), "withdrawn, idempotently");
    }
}
