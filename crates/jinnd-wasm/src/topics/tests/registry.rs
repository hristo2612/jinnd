//! Ordinary dispatch and tracing: what the registry does when nothing is
//! being replaced. The refusal rule next door only makes sense against
//! this baseline — every walk pinned here must still behave identically
//! once the oracle is watching.

use std::sync::Arc;

use jinnd_api::{DispatchMode, FiberId, LedgerEventKind};

use super::{Answer, Failing, RecordingSink};
use crate::peer::LedgerSink;
use crate::selector::{NoRealms, Selector};
use crate::topics::LocalTopics;

#[tokio::test]
async fn every_traced_emit_lands_exactly_one_dispatch_trace() {
    let sink = Arc::new(RecordingSink::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    topics.listen("t", 1, 0, None, Arc::new(Failing));
    topics.listen("t", 2, 0, None, Arc::new(Answer(b"ok".to_vec())));
    topics.listen("other", 3, 0, None, Arc::new(Answer(b"off-topic".to_vec())));

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
    topics.listen("t", 1, 0, None, Arc::new(Failing));
    topics.listen("t", 2, 0, None, Arc::new(Answer(b"ok".to_vec())));
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
    topics.listen("t", 1, 0, None, Arc::new(Answer(Vec::new())));
    topics.listen("t", 2, 0, None, Arc::new(Answer(b"first".to_vec())));
    topics.listen("t", 3, 0, None, Arc::new(Answer(b"second".to_vec())));
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
    topics.listen("t", 1, 0, None, Arc::new(Answer(b"a".to_vec())));
    topics.listen("t", 2, 0, None, Arc::new(Answer(Vec::new())));
    topics.listen("t", 3, 0, None, Arc::new(Answer(b"b".to_vec())));
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
    let selected = topics.listen("t", 1, 0, None, Arc::new(Answer(b"in".to_vec())));
    topics.listen("t", 2, 0, None, Arc::new(Answer(b"out".to_vec())));
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
