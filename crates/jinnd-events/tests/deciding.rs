//! The deciding walks — bail and waterfall — stop only on a decisive resolved
//! value or an explicit decline, never on a pending result or an error (R9).

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::Arc;

use jinnd_events::EventBus;
use support::{FnListener, Fold, Log, Probe, ROOT, Step, boxed, failure, record, recorded};

#[tokio::test(flavor = "current_thread")]
async fn bail_stops_at_the_first_decisive_value() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    let listener = |name: &'static str, output: Option<u8>, log: &Log| {
        let log = Arc::clone(log);
        FnListener(move |_, Probe| {
            record(&log, name);
            boxed(async move { Ok(output) })
        })
    };
    bus.listen(ROOT, listener("first", None, &log), false);
    bus.listen(ROOT, listener("second", Some(9), &log), false);
    bus.listen(ROOT, listener("third", Some(10), &log), false);

    let report = bus.dispatch(ROOT, Probe).await;

    assert_eq!(recorded(&log), vec!["first", "second"]);
    assert_eq!(report.outputs, vec![Some(9)], "the decisive output alone");
}

#[tokio::test(flavor = "current_thread")]
async fn a_pending_async_result_is_never_treated_as_bailed() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    {
        let log = Arc::clone(&log);
        bus.listen(
            ROOT,
            FnListener(move |_, Probe| {
                let log = Arc::clone(&log);
                boxed(async move {
                    tokio::task::yield_now().await;
                    record(&log, "async none");
                    Ok(None)
                })
            }),
            false,
        );
    }
    {
        let log = Arc::clone(&log);
        bus.listen(
            ROOT,
            FnListener(move |_, Probe| {
                record(&log, "sync value");
                boxed(async { Ok(Some(4)) })
            }),
            false,
        );
    }

    let report = bus.dispatch(ROOT, Probe).await;

    assert_eq!(recorded(&log), vec!["async none", "sync value"], "R9");
    assert_eq!(report.outputs, vec![Some(4)]);
}

#[tokio::test(flavor = "current_thread")]
async fn a_failing_bail_listener_is_recorded_and_the_walk_continues() {
    let bus = EventBus::new();
    bus.listen(
        ROOT,
        FnListener(|_, Probe| boxed(async { failure("bail failure") })),
        false,
    );
    bus.listen(
        ROOT,
        FnListener(|_, Probe| boxed(async { Ok(Some(2)) })),
        false,
    );

    let report = bus.dispatch(ROOT, Probe).await;

    assert_eq!(report.outputs, vec![Some(2)]);
    assert_eq!(report.failures.len(), 1, "an error is never a bail value");
}

#[tokio::test(flavor = "current_thread")]
async fn waterfall_folds_outputs_in_registration_order() {
    let bus = EventBus::new();
    bus.listen(
        ROOT,
        FnListener(|_, Fold { .. }| boxed(async { Ok(Step::Add(1)) })),
        false,
    );
    bus.listen(
        ROOT,
        FnListener(|_, Fold { .. }| boxed(async { Ok(Step::Add(1)) })),
        false,
    );

    let report = bus.dispatch(ROOT, Fold { acc: 2 }).await;

    assert_eq!(report.event.acc, 4);
    assert!(
        report.outputs.is_empty(),
        "outputs are folded, not gathered"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn waterfall_decline_stops_the_walk() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    bus.listen(
        ROOT,
        FnListener(|_, Fold { .. }| boxed(async { Ok(Step::Add(1)) })),
        false,
    );
    bus.listen(
        ROOT,
        FnListener(|_, Fold { .. }| boxed(async { Ok(Step::Take(3)) })),
        false,
    );
    {
        let log = Arc::clone(&log);
        bus.listen(
            ROOT,
            FnListener(move |_, Fold { .. }| {
                record(&log, "after decline");
                boxed(async { Ok(Step::Add(10)) })
            }),
            false,
        );
    }

    let report = bus.dispatch(ROOT, Fold { acc: 2 }).await;

    assert_eq!(report.event.acc, 3);
    assert!(
        recorded(&log).is_empty(),
        "listeners after the declining middleware never run"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_failing_waterfall_listener_contributes_nothing() {
    let bus = EventBus::new();
    bus.listen(
        ROOT,
        FnListener(|_, Fold { .. }| boxed(async { failure("no contribution") })),
        false,
    );
    bus.listen(
        ROOT,
        FnListener(|_, Fold { .. }| boxed(async { Ok(Step::Add(5)) })),
        false,
    );

    let report = bus.dispatch(ROOT, Fold { acc: 1 }).await;

    assert_eq!(report.event.acc, 6, "R9: the walk continued");
    assert_eq!(report.failures.len(), 1);
}
