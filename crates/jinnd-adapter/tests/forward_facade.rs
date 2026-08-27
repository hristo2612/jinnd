//! Facade-level forward-effect wiring (M1-P7): begin/outcome/dispose against
//! the kernel scope, and the teardown-effect host on activations.

use std::sync::{Arc, Mutex};

use jinnd_api::{
    Activation, ErrorCode, ForwardAction, ForwardEffect, Kernel, KernelError, KernelFuture,
    PluginContract, Undo,
};

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

#[tokio::test]
async fn a_landed_plain_effect_is_visible_and_disposes_exactly_once() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    let effect = kernel
        .begin_effect(
            kernel.root_context(),
            "test forward".to_owned(),
            ForwardEffect::Plain(step_marking(&log, 1, 2)),
        )
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    kernel
        .effect_outcome(effect)
        .await
        .unwrap_or_else(|error| panic!("outcome: {error:?}"));
    assert!(
        kernel
            .effect_tree(jinnd_adapter::KERNEL_SCOPE)
            .iter()
            .any(|descriptor| descriptor.id == effect && descriptor.label == "test forward"),
        "the landed effect is visible in the tree"
    );
    kernel
        .dispose_effect(effect)
        .await
        .unwrap_or_else(|error| panic!("dispose: {error:?}"));
    kernel
        .dispose_effect(effect)
        .await
        .unwrap_or_else(|error| panic!("re-dispose: {error:?}"));
    assert_eq!(read(&log), vec![1, 2], "the undo ran exactly once");
    assert!(
        kernel
            .effect_tree(jinnd_adapter::KERNEL_SCOPE)
            .iter()
            .all(|descriptor| descriptor.id != effect),
        "the withdrawn record left the tree"
    );
}

#[tokio::test]
async fn a_failed_forward_effect_installs_nothing_and_returns_the_original_error() {
    let kernel = jinnd_adapter::kernel();
    let failing: ForwardAction = Box::new(|| {
        Box::pin(async {
            Err(KernelError {
                code: ErrorCode::EffectFailed,
                message: "the forward action refused".to_owned(),
                fiber: None,
            })
        })
    });
    let effect = kernel
        .begin_effect(
            kernel.root_context(),
            "failing forward".to_owned(),
            ForwardEffect::Plain(failing),
        )
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    let outcome = kernel.effect_outcome(effect).await;
    let Err(failure) = outcome else {
        panic!("the original error must surface");
    };
    assert_eq!(failure.message, "the forward action refused");
    assert!(
        kernel
            .effect_tree(jinnd_adapter::KERNEL_SCOPE)
            .iter()
            .all(|descriptor| descriptor.id != effect),
        "all-or-none: a failed effect publishes no record"
    );
}

#[tokio::test]
async fn disposing_an_in_flight_iterator_lands_the_step_then_unwinds_the_prefix() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    let (gate, released) = tokio::sync::oneshot::channel::<()>();
    let gated_log = Arc::clone(&log);
    let gated: ForwardAction = Box::new(move || {
        Box::pin(async move {
            let _ = released.await;
            mark(&gated_log, 1);
            let undo: Box<dyn Undo> = Box::new(MarkUndo(gated_log, 2));
            Ok(undo)
        })
    });
    let effect = kernel
        .begin_effect(
            kernel.root_context(),
            "gated iterator".to_owned(),
            ForwardEffect::Steps(vec![gated, step_marking(&log, 7, 8)]),
        )
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    // Let the driver launch the first step and park at its gate, so the
    // disposal below arrives against a step genuinely in flight.
    tokio::task::yield_now().await;

    let ((), sent) = tokio::join!(
        async {
            kernel
                .dispose_effect(effect)
                .await
                .unwrap_or_else(|error| panic!("dispose: {error:?}"));
        },
        async move { gate.send(()) },
    );
    sent.unwrap_or_else(|()| panic!("the gate must reach the launched step"));
    assert_eq!(
        read(&log),
        vec![1, 2],
        "the launched step lands, only its inverse runs, the next never launches"
    );
}

#[derive(Debug)]
struct TeardownPlugin {
    log: Log,
}

impl PluginContract for TeardownPlugin {
    type Config = u8;
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/teardown-host";

    fn activate<'a>(&'a self, activation: Activation<'a, ()>, _config: u8) -> KernelFuture<'a, ()> {
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            activation
                .effects
                .register("teardown probe".to_owned(), Box::new(MarkUndo(log, 9)))?;
            Ok(())
        })
    }
}

#[tokio::test]
async fn an_activation_registered_teardown_effect_runs_on_disposal() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    let fiber = kernel
        .spawn(
            kernel.root_context(),
            TeardownPlugin {
                log: Arc::clone(&log),
            },
            0,
        )
        .await
        .unwrap_or_else(|error| panic!("spawn: {error:?}"));
    assert!(
        kernel
            .effect_tree(fiber)
            .iter()
            .any(|descriptor| descriptor.label == "teardown probe"),
        "the hosted effect is charged to the activation's fiber"
    );
    assert_eq!(read(&log), Vec::<u32>::new());
    kernel
        .dispose(fiber)
        .await
        .unwrap_or_else(|error| panic!("dispose: {error:?}"));
    assert_eq!(
        read(&log),
        vec![9],
        "the teardown effect ran with the fiber"
    );
}
