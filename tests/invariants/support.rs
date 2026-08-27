#![allow(dead_code, unused_imports, unused_macros)]

use jinnd_api::{
    ContextId, DispatchMode, ErrorCode, Event, EventListener, FiberId, FiberState, Kernel,
    KernelError, KernelFuture, Profile, ServiceContract,
};

#[derive(Clone, Copy, Debug)]
pub enum Subsystem {
    Context,
    Fiber,
    Services,
    Effects,
    Events,
    Loader,
}

#[derive(Debug)]
pub struct StateAt {
    pub millis: u64,
    pub state: FiberState,
}

#[derive(Debug)]
pub struct SpecCase<'a> {
    pub origin: &'a str,
    pub test_name: &'a str,
    pub setup: &'a [&'a str],
    pub actions: &'a [&'a str],
    pub expected: &'a [&'a str],
    pub states: &'a [StateAt],
}

#[derive(Debug)]
struct FixtureService;

impl ServiceContract for FixtureService {
    type Observation = ();

    const NAME: &'static str = "jinn.test/fixture-service";

    fn observe(&self) {}
}

#[derive(Clone, Debug)]
struct FixtureEvent;

impl Event for FixtureEvent {
    type Output = ();

    const MODE: DispatchMode = DispatchMode::Emit;
}

#[derive(Debug)]
struct FixtureListener;

impl EventListener<FixtureEvent> for FixtureListener {
    fn call<'a>(&'a self, _caller: ContextId, _event: FixtureEvent) -> KernelFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub(crate) fn validate(case: &SpecCase<'_>) {
    assert!(
        case.origin.ends_with(".spec.ts")
            || case.origin.starts_with("paper:")
            || case.origin.starts_with("rule:"),
        "origin must name a TS spec, paper theorem, or numbered rule"
    );
    assert!(!case.test_name.is_empty(), "TS test name must be recorded");
    assert!(!case.setup.is_empty(), "ported case must define its setup");
    assert!(
        !case.actions.is_empty(),
        "ported case must exercise an action"
    );
    assert!(
        !case.expected.is_empty(),
        "ported case must encode an observable result"
    );
    assert!(
        case.states
            .windows(2)
            .all(|pair| pair[0].millis <= pair[1].millis),
        "state checkpoints must be chronological"
    );
    assert!(
        case.states.iter().all(|state| matches!(
            state.state,
            FiberState::Pending
                | FiberState::Loading
                | FiberState::Active
                | FiberState::Failed
                | FiberState::Unloading
                | FiberState::Disposed
        )),
        "state checkpoints must use facade states"
    );
}

pub struct Listener<F>(pub F);

impl<E, F> EventListener<E> for Listener<F>
where
    E: Event,
    F: Fn(ContextId, E) -> KernelFuture<'static, E::Output> + Send + Sync + 'static,
{
    fn call<'a>(&'a self, caller: ContextId, event: E) -> KernelFuture<'a, E::Output> {
        (self.0)(caller, event)
    }
}

pub fn ready<T: Send + 'static>(result: Result<T, KernelError>) -> KernelFuture<'static, T> {
    Box::pin(async move { result })
}

pub fn listener_error(message: &str) -> KernelError {
    KernelError {
        code: ErrorCode::ListenerFailed,
        message: message.to_owned(),
        fiber: None,
    }
}

pub fn expect_ok<T, E: std::fmt::Debug>(result: Result<T, E>, context: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{context}: {error:?}"),
    }
}

pub fn v02_deferred_at(case: &SpecCase<'_>, bound: &str) -> ! {
    validate(case);
    assert!(
        !bound.is_empty(),
        "v0.2 deferrals require a constitution bound"
    );
    panic!(
        "V02_DEFERRED: {bound}; case={} :: {}",
        case.origin, case.test_name
    )
}

/// Drives the closest v0.1 subsystem before recording the constitutional bound
/// that leaves the full cited behavior for a later contract version.
pub async fn v02_deferred(case: &SpecCase<'_>, subsystem: Subsystem, bound: &str) -> ! {
    validate(case);
    assert!(
        !bound.is_empty(),
        "v0.2 deferrals require a constitution bound"
    );

    let kernel = jinnd_adapter::kernel();
    match subsystem {
        Subsystem::Context => {
            let root = kernel.root_context();
            let child = kernel.derive_context(root, Vec::new());
            assert_ne!(
                child, root,
                "a derived facade context needs its own identity"
            );
        }
        Subsystem::Fiber => {
            let state = kernel.state(FiberId(0));
            assert!(
                matches!(
                    state,
                    FiberState::Pending
                        | FiberState::Loading
                        | FiberState::Active
                        | FiberState::Failed
                        | FiberState::Unloading
                        | FiberState::Disposed
                ),
                "the facade returned an unknown fiber state"
            );
        }
        Subsystem::Services => match kernel.resolve::<FixtureService>(ContextId(0)) {
            Ok(handle) => assert_eq!(handle.caller, ContextId(0)),
            Err(error) => assert!(matches!(
                error.code,
                ErrorCode::InactiveContext | ErrorCode::MissingDependency
            )),
        },
        Subsystem::Effects => {
            let tree = kernel.effect_tree(FiberId(0));
            assert!(
                tree.iter().all(|effect| !effect.label.is_empty()),
                "published effect labels must be non-empty"
            );
        }
        Subsystem::Events => match kernel.listen(ContextId(0), FixtureListener) {
            Ok(effect) => assert_ne!(effect.0, u64::MAX),
            Err(error) => assert!(matches!(
                error.code,
                ErrorCode::InactiveContext | ErrorCode::MissingDependency
            )),
        },
        Subsystem::Loader => {
            let report = kernel
                .reconcile(Profile::<u8> {
                    entries: Vec::new(),
                })
                .await;
            match report {
                Ok(report) => {
                    assert!(report.created.is_empty());
                    assert!(report.restarted.is_empty());
                    assert!(report.disposed.is_empty());
                    assert!(report.unchanged.is_empty());
                }
                Err(error) => assert_eq!(error.code, ErrorCode::InvalidProfile),
            }
        }
    }

    v02_deferred_at(case, bound)
}

macro_rules! spec_case {
    (
        $(#[$meta:meta])*
        $name:ident,
        origin: $origin:literal,
        test: $test_name:literal,
        setup: [$($setup:literal),* $(,)?],
        actions: [$($action:literal),+ $(,)?],
        expected: [$($expected:literal),+ $(,)?],
        body: |$case:ident| $body:block
    ) => {
        $(#[$meta])*
        #[tokio::test(flavor = "current_thread")]
        async fn $name() {
            let $case = $crate::support::SpecCase {
                origin: $origin,
                test_name: $test_name,
                setup: &[$($setup),*],
                actions: &[$($action),+],
                expected: &[$($expected),+],
                states: &[],
            };
            $crate::support::validate(&$case);
            $body
        }
    };
    (
        $(#[$meta:meta])*
        $name:ident,
        origin: $origin:literal,
        test: $test_name:literal,
        setup: [$($setup:literal),* $(,)?],
        actions: [$($action:literal),+ $(,)?],
        expected: [$($expected:literal),+ $(,)?]
    ) => {
        $(#[$meta])*
        #[tokio::test(flavor = "current_thread")]
        async fn $name() {
            $crate::support::v02_deferred(
                &$crate::support::SpecCase {
                    origin: $origin,
                    test_name: $test_name,
                    setup: &[$($setup),*],
                    actions: &[$($action),+],
                    expected: &[$($expected),+],
                    states: &[],
                },
                $crate::SUBSYSTEM,
                $crate::V02_DEFERRED_BOUND,
            )
            .await;
        }
    };
}

pub(crate) use spec_case;
