//! Inverted routing, once-delivery, and the snapshot walk's registration
//! semantics (LAW §3; R1).

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::{Arc, Mutex};

use jinnd_api::ContextId;
use jinnd_events::{EventBus, Registration};
use support::{FnListener, Log, Ping, ROOT, Routed, Unroutable, boxed, record, recorded};

#[tokio::test(flavor = "current_thread")]
async fn payload_selects_listeners_by_registration_context() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    for (context, name) in [(ContextId(1), "one"), (ContextId(2), "two")] {
        let log = Arc::clone(&log);
        bus.listen(
            context,
            FnListener(move |_, Routed { .. }| {
                record(&log, name);
                boxed(async { Ok(()) })
            }),
            false,
        );
    }

    let report = bus
        .dispatch(
            ROOT,
            Routed {
                target: ContextId(2),
            },
        )
        .await;

    assert_eq!(recorded(&log), vec!["two"], "inverted routing");
    assert!(report.failures.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn a_panicking_filter_is_contained_and_skips_delivery() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    {
        let log = Arc::clone(&log);
        bus.listen(
            ROOT,
            FnListener(move |_, Unroutable| {
                record(&log, "reached");
                boxed(async { Ok(()) })
            }),
            false,
        );
    }

    let report = bus.dispatch(ROOT, Unroutable).await;

    assert!(recorded(&log).is_empty());
    assert_eq!(report.failures.len(), 1);
    assert!(report.failures[0].message.contains("routing panic"));
}

#[tokio::test(flavor = "current_thread")]
async fn a_once_listener_is_delivered_exactly_once() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    let registration = {
        let log = Arc::clone(&log);
        bus.listen(
            ROOT,
            FnListener(move |_, Ping| {
                record(&log, "once");
                boxed(async { Ok(()) })
            }),
            true,
        )
    };

    bus.dispatch(ROOT, Ping).await;
    bus.dispatch(ROOT, Ping).await;

    assert_eq!(recorded(&log), vec!["once"]);
    assert!(
        !registration.remove(),
        "the kernel already withdrew the delivered once-registration"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn an_unselected_once_listener_stays_registered() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    {
        let log = Arc::clone(&log);
        bus.listen(
            ContextId(1),
            FnListener(move |_, Routed { .. }| {
                record(&log, "once");
                boxed(async { Ok(()) })
            }),
            true,
        );
    }

    bus.dispatch(
        ROOT,
        Routed {
            target: ContextId(2),
        },
    )
    .await;
    assert!(recorded(&log).is_empty(), "not selected, not consumed");

    bus.dispatch(
        ROOT,
        Routed {
            target: ContextId(1),
        },
    )
    .await;
    assert_eq!(recorded(&log), vec!["once"]);
}

#[tokio::test(flavor = "current_thread")]
async fn a_removed_listener_misses_the_next_dispatch() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    let registration = {
        let log = Arc::clone(&log);
        bus.listen(
            ROOT,
            FnListener(move |_, Ping| {
                record(&log, "call");
                boxed(async { Ok(()) })
            }),
            false,
        )
    };

    bus.dispatch(ROOT, Ping).await;
    bus.dispatch(ROOT, Ping).await;
    assert!(registration.remove());
    bus.dispatch(ROOT, Ping).await;

    assert_eq!(recorded(&log), vec!["call", "call"]);
}

#[tokio::test(flavor = "current_thread")]
async fn registration_during_dispatch_misses_the_running_walk() {
    let bus = EventBus::new();
    let log: Log = Log::default();
    let late: Arc<Mutex<Option<Registration>>> = Arc::default();
    let early = {
        let bus = bus.clone();
        let log = Arc::clone(&log);
        let late = Arc::clone(&late);
        bus.clone().listen(
            ROOT,
            FnListener(move |_, Ping| {
                let inner_log = Arc::clone(&log);
                let registration = bus.listen(
                    ROOT,
                    FnListener(move |_, Ping| {
                        record(&inner_log, "late");
                        boxed(async { Ok(()) })
                    }),
                    false,
                );
                *late.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(registration);
                record(&log, "early");
                boxed(async { Ok(()) })
            }),
            false,
        )
    };

    bus.dispatch(ROOT, Ping).await;
    assert_eq!(recorded(&log), vec!["early"], "snapshot: no same-walk join");

    bus.dispatch(ROOT, Ping).await;
    assert_eq!(recorded(&log), vec!["early", "early", "late"]);

    // The early listener's closure captures its own bus, which is a reference
    // cycle through the live table: table → entry → closure → bus → table.
    // Withdrawing the entry breaks it — this is the test fixture's cycle, and
    // exactly what teardown replay does for real plugin listeners (R5).
    assert!(early.remove());
    drop(
        late.lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take(),
    );
}
