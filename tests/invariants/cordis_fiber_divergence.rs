mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jinnd_api::{
    Activation, ErrorCode, FiberState, Kernel, KernelError, KernelFuture, PluginContract,
};
use support::{expect_ok, spec_case};

#[derive(Debug)]
struct AlwaysFails {
    attempts: Arc<AtomicUsize>,
}

impl PluginContract for AlwaysFails {
    type Config = ();
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/restart-failed";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, ()>,
        _config: (),
    ) -> KernelFuture<'a, ()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(KernelError {
                code: ErrorCode::PluginFailed,
                message: "fixture failure".to_owned(),
                fiber: None,
            })
        })
    }
}

spec_case! {
    /// TS comparison origin: `packages/core/src/fiber.ts`; intentionally diverges from its failed-state restart guard.
    explicit_restart_rearms_a_failed_fiber_once,
    origin: "paper: L-Begin divergence / explicit revision bump",
    test: "explicit restart re-arms a failed fiber exactly once",
    setup: ["plugin records one activation attempt then fails", "environment otherwise remains unchanged"],
    actions: ["issue one explicit restart", "wait repeatedly for quiescence"],
    expected: ["one additional activation is attempted", "fiber returns to failed", "no third attempt occurs without another aim change"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let attempts = Arc::new(AtomicUsize::new(0));
        let fiber = expect_ok(
            kernel
                .spawn(
                    kernel.root_context(),
                    AlwaysFails {
                        attempts: Arc::clone(&attempts),
                    },
                    (),
                )
                .await,
            "the failing fixture should spawn",
        );
        assert_eq!(kernel.state(fiber), FiberState::Failed);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        expect_ok(
            kernel.restart(fiber).await,
            "the explicit revision bump should settle",
        );
        assert_eq!(kernel.state(fiber), FiberState::Failed);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        let after_restart = kernel.transitions(fiber);

        for _ in 0..3 {
            expect_ok(
                kernel.wait_for_quiescence().await,
                "the unchanged failed fiber should remain at rest",
            );
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(kernel.transitions(fiber), after_restart);
    }
}
