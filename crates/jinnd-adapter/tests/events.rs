//! The events subsystem wired behind the stable facade (M1-P5).

use std::future::Future;
use std::sync::{Arc, Mutex};

use jinnd_api::{
    ContextId, DispatchMode, ErrorCode, Event, EventListener, Kernel, KernelError, KernelFuture,
};

const ROOT: ContextId = ContextId(0);
const DEAD: ContextId = ContextId(u64::MAX);

type Log = Arc<Mutex<Vec<&'static str>>>;

fn record(log: &Log, entry: &'static str) {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .push(entry);
}

fn recorded(log: &Log) -> Vec<&'static str> {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

fn boxed<T>(
    future: impl Future<Output = Result<T, KernelError>> + Send + 'static,
) -> KernelFuture<'static, T> {
    Box::pin(future)
}

struct FnListener<F>(F);

impl<E, F> EventListener<E> for FnListener<F>
where
    E: Event,
    F: Fn(ContextId, E) -> KernelFuture<'static, E::Output> + Send + Sync + 'static,
{
    fn call<'a>(&'a self, caller: ContextId, event: E) -> KernelFuture<'a, E::Output> {
        (self.0)(caller, event)
    }
}

fn counting(
    log: &Log,
    name: &'static str,
) -> FnListener<impl Fn(ContextId, Ping) -> KernelFuture<'static, ()> + Send + Sync + 'static> {
    let log = Arc::clone(log);
    FnListener(move |_, Ping| {
        record(&log, name);
        boxed(async { Ok(()) })
    })
}

#[derive(Clone, Debug)]
struct Ping;

impl Event for Ping {
    type Output = ();

    const MODE: DispatchMode = DispatchMode::Emit;
}

#[derive(Clone, Debug)]
struct Routed {
    target: ContextId,
}

impl Event for Routed {
    type Output = u8;

    const MODE: DispatchMode = DispatchMode::Serial;

    fn selects(&self, listener: ContextId) -> bool {
        listener == self.target
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_listener_receives_until_its_effect_is_withdrawn() {
    let kernel = jinnd_adapter::kernel();
    let log: Log = Log::default();
    let Ok(effect) = kernel.listen(ROOT, counting(&log, "call")) else {
        panic!("the root context accepts listeners")
    };

    let Ok(_) = kernel.dispatch(ROOT, Ping).await else {
        panic!("the first dispatch settles")
    };
    let Ok(_) = kernel.dispatch(ROOT, Ping).await else {
        panic!("the second dispatch settles")
    };
    let Ok(()) = kernel.unlisten(effect) else {
        panic!("withdrawal is clean")
    };
    let Ok(_) = kernel.dispatch(ROOT, Ping).await else {
        panic!("the post-withdrawal dispatch settles")
    };

    assert_eq!(recorded(&log), vec!["call", "call"]);
    let Ok(()) = kernel.unlisten(effect) else {
        panic!("withdrawal is idempotent")
    };
}

#[tokio::test(flavor = "current_thread")]
async fn a_once_listener_is_withdrawn_by_its_first_delivery() {
    let kernel = jinnd_adapter::kernel();
    let log: Log = Log::default();
    let Ok(effect) = kernel.listen_once(ROOT, counting(&log, "once")) else {
        panic!("the root context accepts once-listeners")
    };

    let Ok(_) = kernel.dispatch(ROOT, Ping).await else {
        panic!("the first dispatch settles")
    };
    let Ok(_) = kernel.dispatch(ROOT, Ping).await else {
        panic!("the second dispatch settles")
    };

    assert_eq!(recorded(&log), vec!["once"]);
    let Ok(()) = kernel.unlisten(effect) else {
        panic!("withdrawing a consumed once-listener is a no-op")
    };
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_routes_by_interrogating_listener_contexts() {
    let kernel = jinnd_adapter::kernel();
    let isolated = kernel.derive_context(ROOT, Vec::new());
    for context in [ROOT, isolated] {
        let Ok(_) = kernel.listen(
            context,
            FnListener(move |_, Routed { .. }| boxed(async move { Ok(7) })),
        ) else {
            panic!("both contexts accept listeners")
        };
    }

    let Ok(outputs) = kernel.dispatch(ROOT, Routed { target: isolated }).await else {
        panic!("the routed dispatch settles")
    };

    assert_eq!(outputs, vec![7], "only the interrogated match ran");
}

#[tokio::test(flavor = "current_thread")]
async fn a_failing_listener_is_reported_after_the_walk_completes() {
    let kernel = jinnd_adapter::kernel();
    let log: Log = Log::default();
    let Ok(_) = kernel.listen(
        ROOT,
        FnListener(|_, Ping| {
            boxed(async {
                Err(KernelError {
                    code: ErrorCode::ListenerFailed,
                    message: "deliberate".to_owned(),
                    fiber: None,
                })
            })
        }),
    ) else {
        panic!("the failing listener registers")
    };
    let Ok(_) = kernel.listen(ROOT, counting(&log, "trailing")) else {
        panic!("the trailing listener registers")
    };

    let Err(error) = kernel.dispatch(ROOT, Ping).await else {
        panic!("the failure is observable")
    };

    assert_eq!(error.code, ErrorCode::ListenerFailed);
    assert_eq!(
        recorded(&log),
        vec!["trailing"],
        "R9: reported, not aborted"
    );

    let Ok(report) = kernel.dispatch_report(ROOT, Ping).await else {
        panic!("the report lane settles")
    };
    assert_eq!(
        report.failures.len(),
        1,
        "the aggregate keeps every failure"
    );
    assert_eq!(recorded(&log), vec!["trailing", "trailing"]);
}

#[tokio::test(flavor = "current_thread")]
async fn an_unminted_context_is_refused_everywhere() {
    let kernel = jinnd_adapter::kernel();

    let Err(listen) = kernel.listen(DEAD, FnListener(|_, Ping| boxed(async { Ok(()) }))) else {
        panic!("listen refuses a foreign context")
    };
    let Err(once) = kernel.listen_once(DEAD, FnListener(|_, Ping| boxed(async { Ok(()) }))) else {
        panic!("listen_once refuses a foreign context")
    };
    let Err(dispatch) = kernel.dispatch(DEAD, Ping).await else {
        panic!("dispatch refuses a foreign context")
    };
    let Err(report) = kernel.dispatch_report(DEAD, Ping).await else {
        panic!("dispatch_report refuses a foreign context")
    };

    for error in [listen, once, dispatch, report] {
        assert_eq!(error.code, ErrorCode::InactiveContext);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_listener_registration_is_a_visible_effect() {
    let kernel = jinnd_adapter::kernel();
    let Ok(effect) = kernel.listen(ROOT, FnListener(|_, Ping| boxed(async { Ok(()) }))) else {
        panic!("the listener registers")
    };

    let tree = kernel.effect_tree(jinnd_adapter::KERNEL_SCOPE);
    let Some(descriptor) = tree.iter().find(|entry| entry.id == effect) else {
        panic!("the registration effect is introspectable (R5)")
    };
    assert!(
        descriptor.label.contains("listen"),
        "the effect label names the registration"
    );
}
