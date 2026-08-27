//! Forward effect-iterator semantics (paper Def 51/52 + Alg 1; M1-P7).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use jinnd_api::{ErrorCode, KernelError, KernelFuture, Undo};
use tokio_util::sync::CancellationToken;

use jinnd_effects::{ForwardAction, ForwardEffect, ForwardEnd, advance, discharge};

type Log = Arc<Mutex<Vec<u32>>>;

fn log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

fn read(log: &Log) -> Vec<u32> {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

fn mark(log: &Log, value: u32) {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .push(value);
}

struct MarkUndo(Log, u32);

impl Undo for MarkUndo {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
        mark(&self.0, self.1);
        Box::pin(async { Ok(()) })
    }
}

fn step_marking(log: &Log, forward: u32, inverse: u32) -> ForwardAction {
    let log = Arc::clone(log);
    Box::new(move || {
        mark(&log, forward);
        let undo: Box<dyn Undo> = Box::new(MarkUndo(log, inverse));
        Box::pin(async move { Ok(undo) })
    })
}

fn failing_step(message: &'static str) -> ForwardAction {
    Box::new(move || {
        Box::pin(async move {
            Err(KernelError {
                code: ErrorCode::EffectFailed,
                message: message.to_owned(),
                fiber: None,
            })
        })
    })
}

#[tokio::test]
async fn a_plain_action_that_lands_installs_exactly_its_inverse() {
    let log = log();
    let end = advance(
        ForwardEffect::Plain(step_marking(&log, 1, 2)),
        &CancellationToken::new(),
    )
    .await;
    assert_eq!(read(&log), vec![1], "the inverse must not run at install");
    let ForwardEnd::Installed(disposer) = end else {
        panic!("a landed plain action must install");
    };
    discharge(disposer).await;
    assert_eq!(read(&log), vec![1, 2]);
}

#[tokio::test]
async fn a_failed_plain_action_installs_nothing_and_returns_the_original_error() {
    let end = advance(
        ForwardEffect::Plain(failing_step("the forward action refused")),
        &CancellationToken::new(),
    )
    .await;
    let ForwardEnd::Failed { error, unwound } = end else {
        panic!("a failed plain action must fail the effect");
    };
    assert_eq!(error.message, "the forward action refused");
    assert!(unwound.is_empty(), "nothing was yielded, nothing unwinds");
}

#[tokio::test]
async fn a_stale_plain_action_lands_then_undoes_immediately() {
    let log = log();
    let stale = CancellationToken::new();
    stale.cancel();
    let end = advance(ForwardEffect::Plain(step_marking(&log, 1, 2)), &stale).await;
    assert!(matches!(end, ForwardEnd::Diverted { .. }));
    assert_eq!(
        read(&log),
        vec![1, 2],
        "the launched action lands, its undo follows immediately"
    );
}

#[tokio::test]
async fn a_completed_iterator_replays_every_yield_last_first() {
    let log = log();
    let end = advance(
        ForwardEffect::Steps(vec![
            step_marking(&log, 1, 2),
            step_marking(&log, 3, 4),
            step_marking(&log, 5, 6),
        ]),
        &CancellationToken::new(),
    )
    .await;
    assert_eq!(read(&log), vec![1, 3, 5]);
    let ForwardEnd::Installed(disposer) = end else {
        panic!("a completed iterator must install");
    };
    discharge(disposer).await;
    assert_eq!(read(&log), vec![1, 3, 5, 6, 4, 2]);
}

#[tokio::test]
async fn a_failing_step_unwinds_exactly_the_yielded_prefix_immediately() {
    let log = log();
    let end = advance(
        ForwardEffect::Steps(vec![
            step_marking(&log, 1, 2),
            failing_step("the second step refused"),
            step_marking(&log, 7, 8),
        ]),
        &CancellationToken::new(),
    )
    .await;
    let ForwardEnd::Failed { error, unwound } = end else {
        panic!("a failing step must fail the effect");
    };
    assert_eq!(error.message, "the second step refused");
    assert_eq!(unwound.len(), 1);
    assert_eq!(
        read(&log),
        vec![1, 2],
        "inverse 1 runs immediately; the third step never launches"
    );
}

#[tokio::test]
async fn a_panicking_step_is_contained_and_unwinds_the_prefix() {
    let log = log();
    let panicking: ForwardAction = Box::new(|| panic!("the step panicked"));
    let end = advance(
        ForwardEffect::Steps(vec![step_marking(&log, 1, 2), panicking]),
        &CancellationToken::new(),
    )
    .await;
    let ForwardEnd::Failed { error, .. } = end else {
        panic!("a panicking step must fail the effect, never unwind out");
    };
    assert_eq!(error.code, ErrorCode::EffectFailed);
    assert_eq!(read(&log), vec![1, 2]);
}

#[tokio::test]
async fn a_divert_at_the_yield_boundary_rolls_back_only_the_yielded_prefix() {
    let log = log();
    let stale = CancellationToken::new();
    let tripwire = Arc::clone(&log);
    let trip = stale.clone();
    // The first step lands and trips the guard as it does, so the divert is
    // observed at the yield boundary after the landing.
    let tripping: ForwardAction = Box::new(move || {
        mark(&tripwire, 1);
        trip.cancel();
        let undo: Box<dyn Undo> = Box::new(MarkUndo(tripwire, 2));
        Box::pin(async move { Ok(undo) })
    });
    let end = advance(
        ForwardEffect::Steps(vec![tripping, step_marking(&log, 7, 8)]),
        &stale,
    )
    .await;
    let ForwardEnd::Diverted { unwound } = end else {
        panic!("a stale target at the boundary must divert");
    };
    assert_eq!(unwound.len(), 1);
    assert_eq!(
        read(&log),
        vec![1, 2],
        "the launched step lands, only its inverse runs, the next never launches"
    );
}

#[tokio::test]
async fn a_divert_before_the_first_yield_unwinds_nothing() {
    let stale = CancellationToken::new();
    stale.cancel();
    let log = log();
    let end = advance(ForwardEffect::Steps(vec![step_marking(&log, 1, 2)]), &stale).await;
    let ForwardEnd::Diverted { unwound } = end else {
        panic!("a pre-launch stale target must divert");
    };
    assert!(unwound.is_empty());
    assert_eq!(read(&log), Vec::<u32>::new(), "no step ever launches");
}

#[tokio::test]
async fn an_installed_disposer_ignores_the_forward_guard() {
    // The guard belongs to the forward walk: cancelling it after install
    // must not stop the installed inverse from replaying in full.
    let log = log();
    let stale = CancellationToken::new();
    let end = advance(
        ForwardEffect::Steps(vec![step_marking(&log, 1, 2), step_marking(&log, 3, 4)]),
        &stale,
    )
    .await;
    let ForwardEnd::Installed(disposer) = end else {
        panic!("the iterator must install");
    };
    stale.cancel();
    let outcome = discharge(disposer).await;
    assert!(outcome.is_done(), "installed inverses replay in full");
    assert_eq!(read(&log), vec![1, 3, 4, 2]);
}

#[tokio::test]
async fn an_installed_inverse_failure_stops_the_replay_at_that_inverse() {
    let log = log();
    let failing_undo: ForwardAction = Box::new(|| {
        struct RefusingUndo;
        impl Undo for RefusingUndo {
            fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
                Box::pin(async {
                    Err(KernelError {
                        code: ErrorCode::EffectFailed,
                        message: "the inverse refused".to_owned(),
                        fiber: None,
                    })
                })
            }
        }
        let undo: Box<dyn Undo> = Box::new(RefusingUndo);
        Box::pin(async move { Ok(undo) })
    });
    let counted = Arc::new(AtomicUsize::new(0));
    let survivor = Arc::clone(&counted);
    let counting: ForwardAction = Box::new(move || {
        struct CountUndo(Arc<AtomicUsize>);
        impl Undo for CountUndo {
            fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            }
        }
        let undo: Box<dyn Undo> = Box::new(CountUndo(survivor));
        Box::pin(async move { Ok(undo) })
    });
    let end = advance(
        ForwardEffect::Steps(vec![counting, failing_undo]),
        &CancellationToken::new(),
    )
    .await;
    let ForwardEnd::Installed(disposer) = end else {
        panic!("the iterator must install");
    };
    let outcome = discharge(disposer).await;
    assert!(!outcome.is_done(), "the failing inverse is reported");
    assert_eq!(
        counted.load(Ordering::SeqCst),
        0,
        "inverses after a failed one assume it ran and do not run"
    );
    let _ = read(&log);
}
