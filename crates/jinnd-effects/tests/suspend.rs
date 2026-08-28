//! Suspension (M2-K4): an effect may declare a suspend path distinct from
//! its inverse. A suspend replay runs it LIFO with the rest — an effect
//! without one is a kernel registration and releases through its undo —
//! while an effect that declared one keeps its inverse unrun: a world
//! mutation is retained, never withdrawn, when the fiber merely suspends
//! (SOURCE-OF-TRUTH decision log 2026-08-28; R5, Law 3).

mod support;

use jinnd_effects::EffectScope;
use support::{Trace, recorded};

#[tokio::test]
async fn a_suspend_replay_runs_suspend_paths_and_plain_undos_lifo() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    scope
        .register("listener", recorded(&trace, "undo:listener"))
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    scope
        .register_suspendable(
            "world mutation",
            recorded(&trace, "undo:world"),
            recorded(&trace, "suspend:world"),
        )
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    scope
        .register("alarm", recorded(&trace, "undo:alarm"))
        .unwrap_or_else(|error| panic!("register: {error:?}"));

    let report = scope.suspend().await;

    assert!(report.is_clean());
    assert_eq!(
        trace.entries(),
        vec!["undo:alarm", "suspend:world", "undo:listener"],
        "LIFO; the suspendable effect ran its suspend path, never its inverse"
    );
    assert!(scope.is_empty(), "a suspended scope holds nothing live");
    let labels: Vec<&str> = report
        .effects
        .iter()
        .map(|line| line.label.as_str())
        .collect();
    assert_eq!(labels, vec!["alarm", "world mutation", "listener"]);
}

#[tokio::test]
async fn a_full_replay_runs_the_inverse_of_a_suspendable_effect() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    scope
        .register_suspendable(
            "world mutation",
            recorded(&trace, "undo:world"),
            recorded(&trace, "suspend:world"),
        )
        .unwrap_or_else(|error| panic!("register: {error:?}"));

    let report = scope.replay().await;

    assert!(report.is_clean());
    assert_eq!(trace.entries(), vec!["undo:world"]);
}

#[tokio::test]
async fn a_suspended_scope_refuses_new_registrations() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    scope.suspend().await;
    assert!(
        scope.register("late", recorded(&trace, "late")).is_err(),
        "a suspended scope is sealed exactly as a replayed one"
    );
}
