//! Topic registry unit tests (crate lane).

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
