mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jinnd_api::{Kernel, KernelFuture, Undo};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use support::{SpecCase, expect_ok, facade_gap_at, spec_case};

const SUBSYSTEM: support::Subsystem = support::Subsystem::Effects;
const FACADE_GAP_REASON: &str = "the facade cannot execute forward effects, dispose effect ids, or observe sibling state after failed activation";

struct MarkerUndo(Arc<AtomicUsize>);

impl Undo for MarkerUndo {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

spec_case! {
    /// Paper origin: recovery exactness theorem; SOURCE-OF-TRUTH §4 invariant I1.
    failed_mid_load_withdraws_exactly_the_partial_contribution,
    origin: "paper: recovery exactness theorem / I1",
    test: "recovery under mid-load failure",
    setup: ["capture observational baseline", "plugin applies two reversible mutations then fails before a third"],
    actions: ["allow failure to settle", "remove failed plugin", "compare all service observations and siblings with baseline"],
    expected: ["both applied inverses run once in LIFO order", "no partial contribution remains", "unrelated state is unchanged"]
}

spec_case! {
    /// Paper origin: Definitions 51-52, a plain effect commits its inverse iff its forward action commits.
    plain_effect_application_is_all_or_none,
    origin: "paper: Definitions 51-52 / plain-effect atomicity",
    test: "plain effect is all-or-none",
    setup: ["capture an observable baseline", "prepare a forward mutation and its inverse"],
    actions: ["apply the effect through the kernel boundary", "force both success and failure outcomes"],
    expected: ["success publishes one contribution and one inverse", "failure publishes neither contribution nor inverse"],
    body: |case| {
        let kernel = jinnd_adapter::kernel();
        let undone = Arc::new(AtomicUsize::new(0));
        let effect = expect_ok(
            kernel.register_effect(
                kernel.root_context(),
                "plain effect inverse".to_owned(),
                Box::new(MarkerUndo(Arc::clone(&undone))),
            ),
            "the closest inverse-only facade operation should register",
        );
        assert!(
            kernel
                .effect_tree(jinnd_adapter::KERNEL_SCOPE)
                .iter()
                .any(|descriptor| descriptor.id == effect)
        );
        assert_eq!(undone.load(Ordering::SeqCst), 0);

        facade_gap_at(
            &case,
            "register_effect accepts an already-created inverse and exposes neither the forward action nor effect-id disposal",
        );
    }
}

spec_case! {
    /// Paper origin: recovery exactness theorem; SOURCE-OF-TRUTH §4 invariant I1.
    removal_after_arbitrary_restart_history_withdraws_only_owned_effects,
    origin: "paper: recovery exactness theorem / I1",
    test: "history-sensitive recovery exactness",
    setup: ["two sibling plugins contribute observationally distinct reversible effects", "target plugin has restarted across provider generations"],
    actions: ["remove only the target and wait for quiescence"],
    expected: ["target contribution is absent", "sibling contribution and generation are unchanged", "result equals assembly without target"]
}

/// Paper origin: Theorem 20 recovery exactness under arbitrary interleaving.
#[test]
fn interleaved_withdrawal_removes_exactly_one_owners_contribution() {
    let case = SpecCase {
        origin: "paper: Theorem 20 / Theorem 61",
        test_name: "interleaved withdrawal preserves the other owner's contribution",
        setup: &["two plugins alternate reversible mutations on one shared observable"],
        actions: &["remove one owner after a generated effect interleaving"],
        expected: &[
            "exactly the removed owner's contribution disappears",
            "the survivor remains observationally unchanged",
        ],
        states: &[],
    };
    support::validate(&case);

    let strategy = prop::collection::vec(any::<bool>(), 1..33);
    let mut runner = proptest::test_runner::TestRunner::deterministic();
    let mut generated = Vec::new();
    for _ in 0..32 {
        let tree = match strategy.new_tree(&mut runner) {
            Ok(tree) => tree,
            Err(reason) => panic!("the interleaving strategy should generate: {reason}"),
        };
        generated.push(tree.current());
    }
    assert!(
        generated
            .iter()
            .all(|interleaving| !interleaving.is_empty())
    );

    let kernel = jinnd_adapter::kernel();
    let marker = Arc::new(AtomicUsize::new(0));
    for owner in ["owner-a inverse", "owner-b inverse"] {
        expect_ok(
            kernel.register_effect(
                kernel.root_context(),
                owner.to_owned(),
                Box::new(MarkerUndo(Arc::clone(&marker))),
            ),
            "the inverse-only effects should register",
        );
    }
    assert_eq!(kernel.effect_tree(jinnd_adapter::KERNEL_SCOPE).len(), 2);

    facade_gap_at(
        &case,
        "the facade exposes neither forward effect application nor per-owner effect withdrawal, so generated interleavings cannot mutate or remove one contribution",
    );
}
