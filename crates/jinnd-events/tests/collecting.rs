//! The collecting walks — emit, parallel, serial — never abort on a failing
//! listener (R9) and keep registration order observable.

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::{Arc, Mutex};

use jinnd_api::ErrorCode;
use jinnd_events::EventBus;
use support::{FnListener, Gather, Log, Ordered, Ping, ROOT, boxed, failure, record, recorded};

#[tokio::test(flavor = "current_thread")]
async fn dispatch_without_listeners_reports_nothing() {
    let bus = EventBus::new();
    let report = bus.dispatch(ROOT, Ping).await;
    assert!(report.outputs.is_empty());
    assert!(report.failures.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn emit_notifies_every_listener_and_ignores_outputs() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    for name in ["first", "second"] {
        let log = Arc::clone(&log);
        bus.listen(
            ROOT,
            FnListener(move |_, Ping| {
                record(&log, name);
                boxed(async { Ok(()) })
            }),
            false,
        );
    }

    let report = bus.dispatch(ROOT, Ping).await;

    assert_eq!(recorded(&log), vec!["first", "second"]);
    assert!(report.outputs.is_empty(), "emit ignores outputs");
    assert!(report.failures.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn emit_failure_never_aborts_the_remaining_listeners() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    let ok = |name: &'static str, log: &Log| {
        let log = Arc::clone(log);
        FnListener(move |_, Ping| {
            record(&log, name);
            boxed(async { Ok(()) })
        })
    };
    bus.listen(ROOT, ok("first", &log), false);
    bus.listen(
        ROOT,
        FnListener(|_, Ping| boxed(async { failure("middle failed") })),
        false,
    );
    bus.listen(ROOT, ok("third", &log), false);

    let report = bus.dispatch(ROOT, Ping).await;

    assert_eq!(recorded(&log), vec!["first", "third"], "R9: no abort");
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].message, "middle failed");
}

#[tokio::test(flavor = "current_thread")]
async fn emit_panic_is_contained_and_recorded() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    bus.listen(
        ROOT,
        FnListener(|_, Ping| -> jinnd_api::KernelFuture<'static, ()> {
            panic!("listener exploded")
        }),
        false,
    );
    {
        let log = Arc::clone(&log);
        bus.listen(
            ROOT,
            FnListener(move |_, Ping| {
                record(&log, "after");
                boxed(async { Ok(()) })
            }),
            false,
        );
    }

    let report = bus.dispatch(ROOT, Ping).await;

    assert_eq!(recorded(&log), vec!["after"], "R11: panic is local");
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].code, ErrorCode::ListenerFailed);
    assert!(report.failures[0].message.contains("listener exploded"));
}

#[tokio::test(flavor = "current_thread")]
async fn parallel_gathers_outputs_in_registration_order() {
    let bus = EventBus::new();
    bus.listen(
        ROOT,
        FnListener(|_, Gather| {
            boxed(async {
                tokio::task::yield_now().await;
                Ok(1)
            })
        }),
        false,
    );
    bus.listen(ROOT, FnListener(|_, Gather| boxed(async { Ok(2) })), false);

    let report = bus.dispatch(ROOT, Gather).await;

    assert_eq!(report.outputs, vec![1, 2]);
    assert!(report.failures.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn parallel_listeners_genuinely_overlap() {
    let bus = EventBus::new();
    // Each listener parks on the shared barrier: the dispatch completes only
    // if both run concurrently rather than one after the other.
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    for output in [1, 2] {
        let barrier = Arc::clone(&barrier);
        bus.listen(
            ROOT,
            FnListener(move |_, Gather| {
                let barrier = Arc::clone(&barrier);
                boxed(async move {
                    barrier.wait().await;
                    Ok(output)
                })
            }),
            false,
        );
    }

    let report = bus.dispatch(ROOT, Gather).await;

    assert_eq!(report.outputs, vec![1, 2]);
}

#[tokio::test(flavor = "current_thread")]
async fn parallel_settles_every_listener_and_aggregates_every_failure() {
    let bus = EventBus::new();
    let settled = Arc::new(Mutex::new(false));
    bus.listen(
        ROOT,
        FnListener(|_, Gather| boxed(async { failure("sync failure") })),
        false,
    );
    {
        let settled = Arc::clone(&settled);
        bus.listen(
            ROOT,
            FnListener(move |_, Gather| {
                let settled = Arc::clone(&settled);
                boxed(async move {
                    tokio::task::yield_now().await;
                    *settled.lock().unwrap_or_else(|poison| poison.into_inner()) = true;
                    failure("async failure")
                })
            }),
            false,
        );
    }
    bus.listen(ROOT, FnListener(|_, Gather| boxed(async { Ok(7) })), false);

    let report = bus.dispatch(ROOT, Gather).await;

    assert_eq!(report.outputs, vec![7]);
    let messages: Vec<_> = report
        .failures
        .iter()
        .map(|error| error.message.as_str())
        .collect();
    assert_eq!(messages, vec!["sync failure", "async failure"]);
    assert!(
        *settled.lock().unwrap_or_else(|poison| poison.into_inner()),
        "R9: the delayed listener settled before the aggregate was reported"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn serial_runs_in_registration_order_and_continues_past_failure() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    {
        let log = Arc::clone(&log);
        bus.listen(
            ROOT,
            FnListener(move |_, Ordered| {
                let log = Arc::clone(&log);
                boxed(async move {
                    tokio::task::yield_now().await;
                    record(&log, "first");
                    Ok(1)
                })
            }),
            false,
        );
    }
    bus.listen(
        ROOT,
        FnListener(|_, Ordered| boxed(async { failure("second failed") })),
        false,
    );
    {
        let log = Arc::clone(&log);
        bus.listen(
            ROOT,
            FnListener(move |_, Ordered| {
                record(&log, "third");
                boxed(async { Ok(3) })
            }),
            false,
        );
    }

    let report = bus.dispatch(ROOT, Ordered).await;

    assert_eq!(recorded(&log), vec!["first", "third"]);
    assert_eq!(
        report.outputs,
        vec![1, 3],
        "R9: failure recorded, not fatal"
    );
    assert_eq!(report.failures.len(), 1);
}
