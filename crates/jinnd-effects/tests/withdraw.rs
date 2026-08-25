//! Targeted withdrawal: one live effect leaves the tree and runs its inverses
//! in place, without waiting and without a lock held anywhere near plugin code.

mod support;

use jinnd_effects::{Disposer, EffectScope, UndoOutcome};
use support::{Trace, recorded, registered};

#[test]
fn withdrawing_one_effect_runs_its_inverse_and_removes_its_record() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    let kept = registered(scope.register("kept", recorded(&trace, "kept")));
    let withdrawn = registered(scope.register("withdrawn", recorded(&trace, "withdrawn")));

    let Some(detached) = scope.detach(withdrawn) else {
        panic!("a live effect detaches")
    };
    let report = detached.withdraw_now();

    assert_eq!(trace.entries(), vec!["withdrawn"]);
    assert!(report.is_clean());
    assert_eq!(report.effects.len(), 1);
    assert_eq!(report.effects[0].id, withdrawn);
    let tree = scope.tree();
    assert!(tree.iter().any(|entry| entry.id == kept));
    assert!(!tree.iter().any(|entry| entry.id == withdrawn));
}

#[test]
fn a_detached_subtree_withdraws_children_before_their_parent() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    let parent = registered(scope.register("parent", recorded(&trace, "parent")));
    let inner = registered(scope.register_child(parent, "inner", recorded(&trace, "inner")));
    registered(scope.register_child(inner, "leaf", recorded(&trace, "leaf")));

    let Some(detached) = scope.detach(parent) else {
        panic!("a live parent detaches")
    };
    let report = detached.withdraw_now();

    assert_eq!(trace.entries(), vec!["leaf", "inner", "parent"]);
    assert!(report.is_clean());
    assert!(scope.is_empty());
}

#[test]
fn detaching_an_unknown_effect_is_none_and_changes_nothing() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    let kept = registered(scope.register("kept", recorded(&trace, "kept")));

    assert!(scope.detach(jinnd_api::EffectId(u64::MAX)).is_none());
    assert!(scope.detach(kept).is_some(), "the tree is untouched");
    assert!(scope.detach(kept).is_none(), "detaching is exactly-once");
}

#[tokio::test]
async fn a_withdrawn_effect_is_not_replayed_again() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("kept", recorded(&trace, "kept")));
    let withdrawn = registered(scope.register("withdrawn", recorded(&trace, "withdrawn")));

    let Some(detached) = scope.detach(withdrawn) else {
        panic!("a live effect detaches")
    };
    detached.withdraw_now();
    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["withdrawn", "kept"]);
    assert_eq!(report.effects.len(), 1, "the replay owes only what is live");
}

#[test]
fn an_inverse_that_would_wait_is_reported_interrupted() {
    let mut scope = EffectScope::new();
    let waiting = registered(scope.register(
        "waiting",
        Disposer::future(std::future::pending::<Result<(), jinnd_api::KernelError>>),
    ));

    let Some(detached) = scope.detach(waiting) else {
        panic!("a live effect detaches")
    };
    let report = detached.withdraw_now();

    assert_eq!(report.effects.len(), 1);
    assert!(matches!(
        report.effects[0].outcome,
        UndoOutcome::Interrupted { panic: None }
    ));
    assert!(!scope.tree().iter().any(|entry| entry.id == waiting));
}
