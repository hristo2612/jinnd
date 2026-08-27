mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{
    Activation, ErrorCode, FiberState, ForwardAction, ForwardEffect, Kernel, KernelError,
    KernelFuture, PluginContract, Undo,
};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use support::{SpecCase, expect_ok, spec_case};

struct MarkerUndo(Arc<AtomicUsize>);

impl Undo for MarkerUndo {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

struct OrderedUndo(Arc<Mutex<Vec<u32>>>, u32);

impl Undo for OrderedUndo {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(self.1);
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct RecoveryPlugin {
    log: Arc<Mutex<Vec<u32>>>,
    fail: bool,
}

impl PluginContract for RecoveryPlugin {
    type Config = ();
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/recovery";

    fn activate<'a>(&'a self, activation: Activation<'a, ()>, _config: ()) -> KernelFuture<'a, ()> {
        let log = Arc::clone(&self.log);
        let fail = self.fail;
        Box::pin(async move {
            activation.effects.register(
                "first".to_owned(),
                Box::new(OrderedUndo(Arc::clone(&log), 1)),
            )?;
            activation
                .effects
                .register("second".to_owned(), Box::new(OrderedUndo(log, 2)))?;
            if fail {
                return Err(KernelError {
                    code: ErrorCode::PluginFailed,
                    message: "mid-load failure".to_owned(),
                    fiber: Some(activation.fiber),
                });
            }
            Ok(())
        })
    }
}

fn owned_action(state: &Arc<Mutex<[usize; 2]>>, owner: usize) -> ForwardAction {
    let state = Arc::clone(state);
    Box::new(move || {
        state.lock().unwrap_or_else(|poison| poison.into_inner())[owner] += 1;
        struct OwnedUndo(Arc<Mutex<[usize; 2]>>, usize);
        impl Undo for OwnedUndo {
            fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
                self.0.lock().unwrap_or_else(|poison| poison.into_inner())[self.1] -= 1;
                Box::pin(async { Ok(()) })
            }
        }
        let undo: Box<dyn Undo> = Box::new(OwnedUndo(state, owner));
        Box::pin(async move { Ok(undo) })
    })
}

spec_case! {
    /// Paper origin: recovery exactness theorem; SOURCE-OF-TRUTH §4 invariant I1.
    failed_mid_load_withdraws_exactly_the_partial_contribution,
    origin: "paper: recovery exactness theorem / I1",
    test: "recovery under mid-load failure",
    setup: ["capture observational baseline", "plugin applies two reversible mutations then fails before a third"],
    actions: ["allow failure to settle", "remove failed plugin", "compare all service observations and siblings with baseline"],
    expected: ["both applied inverses run once in LIFO order", "no partial contribution remains", "unrelated state is unchanged"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = Arc::new(Mutex::new(Vec::new()));
        let sibling = Arc::new(AtomicUsize::new(0));
        let sibling_effect = expect_ok(
            kernel.register_effect(
                kernel.root_context(),
                "sibling".to_owned(),
                Box::new(MarkerUndo(Arc::clone(&sibling))),
            ),
            "sibling effect should register",
        );
        let failed = expect_ok(
            kernel.spawn(
                kernel.root_context(),
                RecoveryPlugin { log: Arc::clone(&log), fail: true },
                (),
            ).await,
            "failed activation should remain observable",
        );
        assert_eq!(kernel.state(failed), FiberState::Failed);
        assert_eq!(
            log.lock().unwrap_or_else(|poison| poison.into_inner()).as_slice(),
            [2, 1],
        );
        expect_ok(kernel.dispose(failed).await, "failed plugin should dispose");
        assert_eq!(sibling.load(Ordering::SeqCst), 0);
        assert!(kernel.effect_tree(jinnd_adapter::KERNEL_SCOPE).iter().any(|effect| effect.id == sibling_effect));
    }
}

spec_case! {
    /// Paper origin: Definitions 51-52, a plain effect commits its inverse iff its forward action commits.
    plain_effect_application_is_all_or_none,
    origin: "paper: Definitions 51-52 / plain-effect atomicity",
    test: "plain effect is all-or-none",
    setup: ["capture an observable baseline", "prepare a forward mutation and its inverse"],
    actions: ["apply the effect through the kernel boundary", "force both success and failure outcomes"],
    expected: ["success publishes one contribution and one inverse", "failure publishes neither contribution nor inverse"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let contribution = Arc::new(AtomicUsize::new(0));
        let changed = Arc::clone(&contribution);
        let failing: ForwardAction = Box::new(move || {
            changed.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Err(KernelError {
                    code: ErrorCode::EffectFailed,
                    message: "forward mutation failed".to_owned(),
                    fiber: None,
                })
            })
        });
        let effect = expect_ok(
            kernel.begin_effect(
                kernel.root_context(),
                "plain atomicity".to_owned(),
                ForwardEffect::Plain(failing),
            ),
            "plain effect should begin",
        );
        assert!(kernel.effect_outcome(effect).await.is_err());
        assert_eq!(
            contribution.load(Ordering::SeqCst),
            0,
            "a failed plain effect must publish neither its contribution nor an inverse",
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
    expected: ["target contribution is absent", "sibling contribution and generation are unchanged", "result equals assembly without target"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let target_log = Arc::new(Mutex::new(Vec::new()));
        let sibling_log = Arc::new(Mutex::new(Vec::new()));
        let target = expect_ok(
            kernel.spawn(
                kernel.root_context(),
                RecoveryPlugin { log: Arc::clone(&target_log), fail: false },
                (),
            ).await,
            "target should activate",
        );
        let sibling = expect_ok(
            kernel.spawn(
                kernel.root_context(),
                RecoveryPlugin { log: Arc::clone(&sibling_log), fail: false },
                (),
            ).await,
            "sibling should activate",
        );
        for _ in 0..3 {
            expect_ok(kernel.restart(target).await, "target restart should settle");
        }
        assert_eq!(kernel.state(sibling), FiberState::Active);
        assert!(sibling_log.lock().unwrap_or_else(|poison| poison.into_inner()).is_empty());
        expect_ok(kernel.dispose(target).await, "target should dispose");
        assert_eq!(kernel.state(target), FiberState::Disposed);
        assert_eq!(kernel.state(sibling), FiberState::Active);
        assert!(sibling_log.lock().unwrap_or_else(|poison| poison.into_inner()).is_empty());
        assert_eq!(target_log.lock().unwrap_or_else(|poison| poison.into_inner()).len(), 8);
    }
}

/// Paper origin: Theorem 20 recovery exactness under arbitrary interleaving.
#[tokio::test(flavor = "current_thread")]
async fn interleaved_withdrawal_removes_exactly_one_owners_contribution() {
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

    for interleaving in generated {
        let kernel = jinnd_adapter::kernel();
        let state = Arc::new(Mutex::new([0usize; 2]));
        let mut owned = [Vec::new(), Vec::new()];
        for belongs_to_second in interleaving {
            let owner = usize::from(belongs_to_second);
            let effect = expect_ok(
                kernel.begin_effect(
                    kernel.root_context(),
                    format!("owner-{owner}"),
                    ForwardEffect::Plain(owned_action(&state, owner)),
                ),
                "owned effect should begin",
            );
            expect_ok(
                kernel.effect_outcome(effect).await,
                "owned effect should land",
            );
            owned[owner].push(effect);
        }
        let before = *state.lock().unwrap_or_else(|poison| poison.into_inner());
        for effect in owned[0].drain(..) {
            expect_ok(
                kernel.dispose_effect(effect).await,
                "owner zero effect should withdraw",
            );
        }
        assert_eq!(
            *state.lock().unwrap_or_else(|poison| poison.into_inner()),
            [0, before[1]],
            "withdrawing owner zero must preserve owner one's contribution",
        );
    }
}
