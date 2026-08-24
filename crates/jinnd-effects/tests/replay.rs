//! Ordering, nesting, and exactly-once withdrawal.

mod support;

use jinnd_api::{EffectDescriptor, ErrorCode};
use jinnd_effects::{Disposer, EffectScope, UndoOutcome};
use support::{Trace, recorded, recorded_async, registered};

/// Cordis origin: `packages/core/src/fiber.ts`, `effect()` — disposers are collected
/// in registration order and replayed reversed.
#[tokio::test]
async fn inverses_replay_in_strict_reverse_registration_order() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    for label in ["first", "second", "third"] {
        registered(scope.register(label, recorded(&trace, label)));
    }

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["third", "second", "first"]);
    assert_eq!(
        report.labels().collect::<Vec<_>>(),
        vec!["third", "second", "first"]
    );
    assert!(report.is_clean());
}

/// A child effect was applied after the effect it nested under, so it is withdrawn
/// first: disposing a parent cascades through its whole subtree (R5).
#[tokio::test]
async fn a_child_effect_is_withdrawn_before_the_effect_it_nested_under() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();

    let outer = registered(scope.register("outer", recorded(&trace, "outer")));
    let inner = registered(scope.register_child(outer, "inner", recorded(&trace, "inner")));
    registered(scope.register_child(inner, "leaf", recorded(&trace, "leaf")));
    registered(scope.register_child(outer, "sibling", recorded(&trace, "sibling")));
    registered(scope.register("later-root", recorded(&trace, "later-root")));

    scope.replay().await;

    assert_eq!(
        trace.entries(),
        vec!["later-root", "sibling", "leaf", "inner", "outer"]
    );
}

/// R5: the live tree is free introspection — labels and nesting, no walk required.
#[test]
fn the_live_tree_publishes_every_effect_with_its_label_and_nesting() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();

    let outer = registered(scope.register("outer", recorded(&trace, "outer")));
    let first = registered(scope.register_child(outer, "first-child", recorded(&trace, "a")));
    let second = registered(scope.register_child(outer, "second-child", recorded(&trace, "b")));
    let leaf = registered(scope.register_child(first, "leaf", recorded(&trace, "c")));

    assert_eq!(
        scope.tree(),
        vec![EffectDescriptor {
            id: outer,
            label: "outer".to_owned(),
            children: vec![
                EffectDescriptor {
                    id: first,
                    label: "first-child".to_owned(),
                    children: vec![EffectDescriptor {
                        id: leaf,
                        label: "leaf".to_owned(),
                        children: Vec::new(),
                    }],
                },
                EffectDescriptor {
                    id: second,
                    label: "second-child".to_owned(),
                    children: Vec::new(),
                },
            ],
        }]
    );
    assert!(trace.entries().is_empty(), "introspection runs no inverse");
}

#[tokio::test]
async fn an_inverse_runs_exactly_once_and_a_second_replay_withdraws_nothing() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    let parent = registered(scope.register("parent", recorded(&trace, "parent")));
    registered(scope.register_child(parent, "child", recorded(&trace, "child")));

    let first = scope.replay().await;
    let second = scope.replay().await;

    assert_eq!(trace.entries(), vec!["child", "parent"]);
    assert_eq!(first.effects.len(), 2);
    assert!(second.effects.is_empty());
    assert!(scope.is_empty());
    assert!(scope.tree().is_empty());
}

/// Cordis origin: `packages/core/src/fiber.ts`, `INACTIVE_EFFECT` — a withdrawn scope
/// takes no new effect, so nothing can be registered that replay would never see.
#[tokio::test]
async fn a_replayed_scope_refuses_new_effects() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    let parent = registered(scope.register("parent", recorded(&trace, "parent")));
    scope.replay().await;

    let root = scope.register("late", recorded(&trace, "late"));
    let child = scope.register_child(parent, "late-child", recorded(&trace, "late-child"));

    assert_eq!(root.err().map(|error| error.code), Some(ErrorCode::InactiveContext));
    assert_eq!(child.err().map(|error| error.code), Some(ErrorCode::InactiveContext));
    assert!(
        trace.entries().is_empty(),
        "a refused registration runs no inverse"
    );
}

#[test]
fn nesting_under_an_unknown_effect_is_refused() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    let parent = registered(scope.register("parent", recorded(&trace, "parent")));
    let mut other = EffectScope::new();
    let foreign = registered(other.register("foreign", recorded(&trace, "foreign")));

    let refused = scope.register_child(foreign, "child", recorded(&trace, "child"));

    assert_eq!(refused.err().map(|error| error.code), Some(ErrorCode::EffectFailed));
    assert_eq!(scope.tree().len(), 1);
    assert_ne!(parent, foreign, "effect identity is unique across scopes");
}

#[tokio::test]
async fn an_awaited_inverse_is_driven_to_completion_in_order() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("sync-first", recorded(&trace, "sync-first")));
    registered(scope.register("awaited", recorded_async(&trace, "awaited")));
    registered(scope.register("sync-last", recorded(&trace, "sync-last")));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["sync-last", "awaited", "sync-first"]);
    assert!(report.is_clean());
}

#[tokio::test]
async fn dropping_a_scope_withdraws_nothing_on_its_own() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    let parent = registered(scope.register("parent", recorded(&trace, "parent")));
    registered(scope.register_child(parent, "child", recorded(&trace, "child")));

    drop(scope);

    assert!(
        trace.entries().is_empty(),
        "withdrawal is explicit: a dropped scope runs no inverse"
    );
}

#[tokio::test]
async fn every_outcome_of_a_clean_replay_is_done() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("only", recorded(&trace, "only")));

    let report = scope.replay().await;

    assert_eq!(
        report.effects.first().map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Done)
    );
    assert_eq!(report.unclean().count(), 0);
}
