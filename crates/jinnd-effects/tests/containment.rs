//! Failure is local: a bad inverse never stops the rest of the teardown.

mod support;

use jinnd_api::{ErrorCode, KernelError};
use jinnd_effects::{Disposer, EffectScope, UndoOutcome};
use support::{Trace, error, failing, panicking, recorded, registered};

/// R11: a panicking inverse is contained here. R9: it is not `emit`-style
/// abort-on-first-error — every remaining inverse still runs.
#[tokio::test]
async fn a_panicking_inverse_is_contained_and_the_rest_still_run() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("bottom", recorded(&trace, "bottom")));
    registered(scope.register("bad", panicking(&trace, "bad")));
    registered(scope.register("top", recorded(&trace, "top")));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["top", "bad", "bottom"]);
    assert_eq!(
        report
            .effects
            .iter()
            .map(|effect| effect.label.as_str())
            .collect::<Vec<_>>(),
        vec!["top", "bad", "bottom"]
    );
    assert_eq!(
        report.effects.get(1).map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Panicked("bad could not be undone".to_owned()))
    );
    assert!(!report.is_clean());
    assert_eq!(report.unclean().count(), 1);
}

#[tokio::test]
async fn an_erroring_inverse_is_recorded_and_replay_carries_on() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("bottom", recorded(&trace, "bottom")));
    registered(scope.register("bad", failing(&trace, "bad")));
    registered(scope.register("top", recorded(&trace, "top")));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["top", "bad", "bottom"]);
    assert_eq!(
        report.effects.get(1).map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Failed(error("bad")))
    );
    assert!(!report.is_clean());
}

/// An inverse can also panic while its future is being built, before a single poll.
#[tokio::test]
async fn a_panic_raised_while_building_the_inverse_is_contained() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("survivor", recorded(&trace, "survivor")));
    registered(scope.register(
        "unbuildable",
        Disposer::future(|| -> std::future::Ready<Result<(), KernelError>> {
            panic!("no inverse to build")
        }),
    ));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["survivor"]);
    assert_eq!(
        report.effects.first().map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Panicked("no inverse to build".to_owned()))
    );
}

/// A panicking child does not stop its parent's inverse from running: the cascade is
/// structural, not conditional on success.
#[tokio::test]
async fn a_panicking_child_still_lets_its_parent_be_withdrawn() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    let parent = registered(scope.register("parent", recorded(&trace, "parent")));
    registered(scope.register_child(parent, "child", panicking(&trace, "child")));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["child", "parent"]);
    assert_eq!(
        report.effects.last().map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Done)
    );
}

#[tokio::test]
async fn a_non_string_panic_payload_is_still_reported() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register(
        "structured-panic",
        Disposer::sync(|| std::panic::panic_any(ErrorCode::EffectFailed)),
    ));

    let report = scope.replay().await;

    assert_eq!(
        report.effects.first().map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Panicked(
            "inverse panicked with a non-string payload".to_owned()
        ))
    );
    assert!(trace.entries().is_empty());
}
