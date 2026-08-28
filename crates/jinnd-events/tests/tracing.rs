//! The bus's dispatch-trace tap (M2-K2; Law 2, R6): exactly one trace per
//! emit, carrying topic, mode, selected-listener count, contained-failure
//! count, and the emitting context — and never altering dispatch outcomes.

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::{Arc, Mutex};

use jinnd_api::{ContextId, DispatchMode};
use jinnd_events::{DispatchTraceRecord, EventBus, TraceSink};
use support::{FnListener, Gather, Ping, Routed, boxed, failure};

#[derive(Default)]
struct Recorder {
    traces: Mutex<Vec<DispatchTraceRecord>>,
}

impl TraceSink for Recorder {
    fn trace(&self, record: DispatchTraceRecord) {
        self.traces
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(record);
    }
}

impl Recorder {
    fn recorded(&self) -> Vec<DispatchTraceRecord> {
        self.traces
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

fn traced() -> (EventBus, Arc<Recorder>) {
    let recorder = Arc::new(Recorder::default());
    let bus = EventBus::traced(Arc::clone(&recorder) as Arc<dyn TraceSink>);
    (bus, recorder)
}

#[tokio::test]
async fn every_emit_lands_exactly_one_trace_with_counts() {
    let (bus, recorder) = traced();
    let _kept = bus.listen::<Gather, _>(
        ContextId(1),
        FnListener(|_, _| boxed(async { Ok(7) })),
        false,
    );
    let _grumpy = bus.listen::<Gather, _>(
        ContextId(2),
        FnListener(|_, _| boxed(async { failure("listener refused") })),
        false,
    );

    let report = bus.dispatch(ContextId(9), Gather).await;
    assert_eq!(report.outputs, vec![7]);
    assert_eq!(report.failures.len(), 1);

    let traces = recorder.recorded();
    assert_eq!(traces.len(), 1, "exactly one trace per emit");
    let trace = &traces[0];
    assert_eq!(trace.topic, std::any::type_name::<Gather>());
    assert_eq!(trace.mode, DispatchMode::Parallel);
    assert_eq!(trace.listeners, 2);
    assert_eq!(trace.failures, 1);
    assert_eq!(trace.emitter, ContextId(9));
}

#[tokio::test]
async fn a_listenerless_emit_still_traces() {
    let (bus, recorder) = traced();
    let report = bus.dispatch(ContextId(3), Ping).await;
    assert!(report.failures.is_empty());

    let traces = recorder.recorded();
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].listeners, 0);
    assert_eq!(traces[0].failures, 0);
    assert_eq!(traces[0].mode, DispatchMode::Emit);
}

#[tokio::test]
async fn the_trace_counts_selected_listeners_not_registered_ones() {
    let (bus, recorder) = traced();
    let _selected = bus.listen::<Routed, _>(
        ContextId(1),
        FnListener(|_, _| boxed(async { Ok(()) })),
        false,
    );
    let _unselected = bus.listen::<Routed, _>(
        ContextId(2),
        FnListener(|_, _| boxed(async { Ok(()) })),
        false,
    );

    bus.dispatch(
        ContextId(1),
        Routed {
            target: ContextId(1),
        },
    )
    .await;

    let traces = recorder.recorded();
    assert_eq!(traces.len(), 1);
    assert_eq!(
        traces[0].listeners, 1,
        "inverted routing selected one listener"
    );
}

#[tokio::test]
async fn an_untraced_bus_dispatches_unchanged() {
    let bus = EventBus::new();
    let _kept = bus.listen::<Gather, _>(
        ContextId(1),
        FnListener(|_, _| boxed(async { Ok(4) })),
        false,
    );
    let report = bus.dispatch(ContextId(1), Gather).await;
    assert_eq!(report.outputs, vec![4]);
}
