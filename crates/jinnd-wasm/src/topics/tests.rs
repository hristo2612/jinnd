//! Topic registry unit tests (crate lane).

use std::sync::{Arc, Mutex};

use jinnd_api::{DispatchMode, ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind};

use super::{EventTarget, LocalTopics};
use crate::peer::LedgerSink;
use crate::selector::{NoRealms, Selector};

#[derive(Default)]
struct RecordingSink(Mutex<Vec<(LedgerEventKind, Option<FiberId>)>>);

impl LedgerSink for RecordingSink {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((kind, fiber));
    }
}

impl RecordingSink {
    fn recorded(&self) -> Vec<(LedgerEventKind, Option<FiberId>)> {
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

#[tokio::test]
async fn every_traced_emit_lands_exactly_one_dispatch_trace() {
    let sink = Arc::new(RecordingSink::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    topics.listen("t", 1, 0, Arc::new(Failing));
    topics.listen("t", 2, 0, Arc::new(Answer(b"ok".to_vec())));
    topics.listen("other", 3, 0, Arc::new(Answer(b"off-topic".to_vec())));

    let report = topics
        .emit(
            7,
            "t",
            DispatchMode::Parallel,
            &Selector::All,
            Vec::new(),
            Some(FiberId(3)),
            &NoRealms,
        )
        .await;
    assert_eq!(report.outputs, vec![b"ok".to_vec()]);

    let recorded = sink.recorded();
    assert_eq!(recorded.len(), 1, "exactly one trace per emit");
    let (kind, fiber) = &recorded[0];
    assert_eq!(
        *fiber,
        Some(FiberId(3)),
        "emitter fiber attribution (Law 2)"
    );
    assert_eq!(
        *kind,
        LedgerEventKind::DispatchTrace {
            topic: "t".to_owned(),
            mode: DispatchMode::Parallel,
            listeners: 2,
            failures: 1,
            emitter: 7,
        }
    );
}

#[tokio::test]
async fn a_listenerless_traced_emit_still_traces() {
    let sink = Arc::new(RecordingSink::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    topics
        .emit(
            1,
            "quiet",
            DispatchMode::Emit,
            &Selector::All,
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;
    assert_eq!(
        sink.recorded(),
        vec![(
            LedgerEventKind::DispatchTrace {
                topic: "quiet".to_owned(),
                mode: DispatchMode::Emit,
                listeners: 0,
                failures: 0,
                emitter: 1,
            },
            None
        )]
    );
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
            None,
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
            None,
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
            None,
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
            None,
            &NoRealms,
        )
        .await;
    assert_eq!(report.outputs, vec![b"in".to_vec()]);

    assert_eq!(
        topics.unlisten(selected),
        Some("t".to_owned()),
        "withdrawal returns the topic — the caller's Law-2 label"
    );
    assert_eq!(
        topics.unlisten(selected),
        None,
        "idempotent: the second withdrawal is a no-op"
    );
    let after = topics
        .emit(
            0,
            "t",
            DispatchMode::Serial,
            &Selector::ContextSet(vec![1]),
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;
    assert!(after.outputs.is_empty(), "withdrawn, idempotently");
}
