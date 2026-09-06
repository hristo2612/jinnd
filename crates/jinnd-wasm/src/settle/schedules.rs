//! K25 clock edges must be charged at the transition, regardless of parent polling.
use super::{DeadlineControl, within};
use std::{future::Future, pin::Pin, task::Poll, time::Duration};
use tokio::time::{Instant, advance, sleep};

async fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut future = future;
    std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

async fn witness(park_observation_delay: u64, resume_observation_delay: u64) -> (bool, bool) {
    let control = DeadlineControl::new();
    let inside = control.clone();
    let started = Instant::now();
    let call = within(Duration::from_secs(5), &control, async move {
        let park = inside.park();
        eprintln!("CLOCK actual park {:?}", started.elapsed());
        sleep(Duration::from_secs(6)).await;
        drop(park);
        eprintln!("CLOCK actual unpark {:?}", started.elapsed());
        sleep(Duration::from_secs(20)).await;
    });
    tokio::pin!(call);
    assert!(poll_once(call.as_mut()).await.is_pending());
    advance(Duration::from_secs(park_observation_delay)).await;
    assert!(poll_once(call.as_mut()).await.is_pending());
    eprintln!("CLOCK park observation at {:?}", started.elapsed());
    advance(Duration::from_secs(6 - park_observation_delay)).await;
    assert!(poll_once(call.as_mut()).await.is_pending());
    advance(Duration::from_secs(resume_observation_delay)).await;
    assert!(poll_once(call.as_mut()).await.is_pending());
    eprintln!("CLOCK resume observation at {:?}", started.elapsed());
    advance(Duration::from_secs(3 - resume_observation_delay)).await;
    let early = poll_once(call.as_mut()).await.is_ready();
    eprintln!(
        "CLOCK elapsed={:?} actual_active=3s expired={early}",
        started.elapsed()
    );
    if early {
        return (true, true);
    }
    advance(Duration::from_secs(2)).await;
    let on_time = poll_once(call.as_mut()).await.is_ready();
    eprintln!(
        "CLOCK elapsed={:?} actual_active=5s expired={on_time}",
        started.elapsed()
    );
    (early, on_time)
}

#[tokio::test(start_paused = true)]
async fn immediate_observation_control() {
    assert_eq!(witness(0, 0).await, (false, true));
}

#[tokio::test(start_paused = true)]
async fn delayed_park_observation_must_not_charge_parked_time() {
    let (early, _) = witness(2, 0).await;
    assert!(
        !early,
        "K25(a): six parked seconds must not consume two of the five active seconds"
    );
}

#[tokio::test(start_paused = true)]
async fn delayed_resume_observation_must_not_extend_active_budget() {
    let (_, on_time) = witness(0, 2).await;
    assert!(
        on_time,
        "R11/K25(a): five active seconds must exhaust the unchanged five-second budget"
    );
}

#[tokio::test(start_paused = true)]
async fn missed_nested_union_preserves_work_before_the_first_park() {
    let control = DeadlineControl::new();
    let call = within(
        Duration::from_secs(5),
        &control,
        std::future::pending::<()>(),
    );
    tokio::pin!(call);
    assert!(poll_once(call.as_mut()).await.is_pending());
    advance(Duration::from_secs(2)).await;
    let outer = control.park();
    advance(Duration::from_secs(1)).await;
    let inner = control.park();
    advance(Duration::from_secs(2)).await;
    drop(outer);
    advance(Duration::from_secs(1)).await;
    drop(inner);
    // No supervisor observation in the entire four-second parked union.
    assert!(poll_once(call.as_mut()).await.is_pending());
    advance(Duration::from_secs(2)).await;
    assert!(poll_once(call.as_mut()).await.is_pending());
    advance(Duration::from_secs(1)).await;
    assert_eq!(
        poll_once(call.as_mut()).await,
        Poll::Ready(Err(super::DeadlineElapsed))
    );
}

#[tokio::test(start_paused = true)]
async fn starting_while_parked_credits_only_this_calls_overlap() {
    let control = DeadlineControl::new();
    advance(Duration::from_secs(7)).await;
    let parked = control.park();
    advance(Duration::from_secs(3)).await;
    let call = within(
        Duration::from_secs(5),
        &control,
        std::future::pending::<()>(),
    );
    tokio::pin!(call);
    assert!(poll_once(call.as_mut()).await.is_pending());
    advance(Duration::from_secs(3)).await;
    drop(parked);
    assert!(poll_once(call.as_mut()).await.is_pending());
    advance(Duration::from_secs(4)).await;
    assert!(poll_once(call.as_mut()).await.is_pending());
    advance(Duration::from_secs(1)).await;
    assert_eq!(
        poll_once(call.as_mut()).await,
        Poll::Ready(Err(super::DeadlineElapsed))
    );
}

#[tokio::test(start_paused = true)]
async fn cancellation_drops_nested_parks_and_the_next_call_gets_its_own_budget() {
    let control = DeadlineControl::new();
    let mut call = Box::pin(within(Duration::from_secs(5), &control, async {
        let _outer = control.park();
        let _inner = control.park();
        std::future::pending::<()>().await;
    }));
    assert!(poll_once(call.as_mut()).await.is_pending());
    advance(Duration::from_secs(6)).await;
    drop(call);
    advance(Duration::from_secs(20)).await;
    let next = super::settle(
        Duration::from_secs(5),
        &control,
        std::future::pending::<wasmtime::Result<Result<(), crate::bindings::lifecycle::GuestFault>>>(
        ),
    );
    tokio::pin!(next);
    assert!(poll_once(next.as_mut()).await.is_pending());
    advance(Duration::from_secs(4)).await;
    assert!(poll_once(next.as_mut()).await.is_pending());
    advance(Duration::from_secs(1)).await;
    match poll_once(next.as_mut()).await {
        Poll::Ready(super::Settled::Dead(error)) => {
            assert_eq!(error.code, jinnd_api::ErrorCode::PluginFailed);
            assert_eq!(error.message, "guest exceeded its call deadline");
        }
        _ => panic!("the next call must end with the contained deadline cause"),
    }
}

#[tokio::test(start_paused = true)]
async fn supervisor_and_caller_observers_keep_independent_baselines() {
    let control = DeadlineControl::new();
    let earlier = within(
        Duration::from_secs(5),
        &control,
        std::future::pending::<()>(),
    );
    tokio::pin!(earlier);
    assert!(poll_once(earlier.as_mut()).await.is_pending());
    advance(Duration::from_secs(2)).await;
    let later = within(
        Duration::from_secs(5),
        &control,
        std::future::pending::<()>(),
    );
    tokio::pin!(later);
    assert!(poll_once(later.as_mut()).await.is_pending());
    advance(Duration::from_secs(1)).await;
    let parked = control.park();
    assert!(poll_once(earlier.as_mut()).await.is_pending());
    advance(Duration::from_secs(6)).await;
    drop(parked);
    // Only the earlier observer saw the park; the later one missed it whole.
    assert!(poll_once(earlier.as_mut()).await.is_pending());
    assert!(poll_once(later.as_mut()).await.is_pending());
    advance(Duration::from_secs(2)).await;
    assert_eq!(
        poll_once(earlier.as_mut()).await,
        Poll::Ready(Err(super::DeadlineElapsed))
    );
    assert!(poll_once(later.as_mut()).await.is_pending());
    advance(Duration::from_secs(2)).await;
    assert_eq!(
        poll_once(later.as_mut()).await,
        Poll::Ready(Err(super::DeadlineElapsed))
    );
}

#[tokio::test(start_paused = true)]
async fn parking_cannot_restore_already_exhausted_active_time() {
    let control = DeadlineControl::new();
    let call = within(
        Duration::from_secs(5),
        &control,
        std::future::pending::<()>(),
    );
    tokio::pin!(call);
    assert!(poll_once(call.as_mut()).await.is_pending());
    advance(Duration::from_secs(6)).await;
    let _parked = control.park();
    assert_eq!(
        poll_once(call.as_mut()).await,
        Poll::Ready(Err(super::DeadlineElapsed))
    );
}
