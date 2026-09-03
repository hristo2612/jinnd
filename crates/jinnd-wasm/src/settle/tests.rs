//! Red-first witnesses for M2-K25(a): a walk parks the emitter's call
//! horizon, and nested parking extends it exactly once per elapsed span.

use std::time::Duration;

use tokio::time::{Instant, sleep};

use super::{DeadlineControl, within};

#[tokio::test]
async fn a_parked_walk_does_not_spend_the_emitters_deadline() {
    let control = DeadlineControl::new();
    let inside = control.clone();
    let started = Instant::now();
    let answer = within(Duration::from_millis(80), &control, async move {
        sleep(Duration::from_millis(20)).await;
        let parked = inside.park();
        sleep(Duration::from_millis(100)).await;
        drop(parked);
        sleep(Duration::from_millis(20)).await;
        7
    })
    .await;

    assert_eq!(answer, Ok(7));
    assert!(started.elapsed() >= Duration::from_millis(130));
}

#[tokio::test]
async fn nested_walk_parking_counts_elapsed_time_once() {
    let control = DeadlineControl::new();
    let inside = control.clone();
    let answer = within(Duration::from_millis(70), &control, async move {
        let outer = inside.park();
        sleep(Duration::from_millis(45)).await;
        let inner = inside.park();
        sleep(Duration::from_millis(45)).await;
        drop(inner);
        sleep(Duration::from_millis(45)).await;
        drop(outer);
        sleep(Duration::from_millis(20)).await;
        9
    })
    .await;

    assert_eq!(answer, Ok(9));
}

#[tokio::test]
async fn unparked_guest_work_still_dies_at_its_deadline() {
    let control = DeadlineControl::new();
    let answer = within(Duration::from_millis(30), &control, async {
        sleep(Duration::from_millis(100)).await;
        1
    })
    .await;

    assert!(answer.is_err());
}
