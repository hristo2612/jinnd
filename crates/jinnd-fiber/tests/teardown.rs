//! The teardown context marker (M1-P6b): a withdrawal replays plugin-owned
//! inverses inside a marked scope on the fiber's own task, so the profile
//! loader can refuse amendments from teardown context decidably (R1).

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::{Arc, Mutex};

use jinnd_api::FiberState;
use jinnd_effects::Disposer;
use jinnd_fiber::in_teardown;
use support::{body, ready};

/// Where the marker was observed, and what it said there.
type Seen = Arc<Mutex<Vec<(&'static str, bool)>>>;

fn record(seen: &Seen, at: &'static str) {
    seen.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .push((at, in_teardown()));
}

/// The marker is absent during activation and on plain tasks, and present
/// inside the inverse replay — which is exactly where a re-entrant loader
/// call would otherwise deadlock the teardown it runs inside.
#[tokio::test]
async fn the_marker_holds_for_the_withdrawal_replay_and_nowhere_else() {
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&seen);
    let (fiber, _source) = ready(body(move |mut setup| {
        let observed = Arc::clone(&observed);
        Box::pin(async move {
            record(&observed, "activation");
            let inverse = Arc::clone(&observed);
            setup.effect(
                "record the teardown marker",
                Disposer::future(move || async move {
                    record(&inverse, "inverse");
                    Ok(())
                }),
            )?;
            Ok(())
        })
    }));
    fiber.quiesce().await;
    assert!(!in_teardown(), "a plain task is never in teardown");

    fiber.dispose().await;

    assert_eq!(fiber.state(), FiberState::Disposed);
    let seen = seen
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    assert_eq!(
        seen,
        vec![("activation", false), ("inverse", true)],
        "the marker holds exactly for the inverse replay"
    );
}
