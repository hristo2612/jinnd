//! The conformance-harness kernel: context + effects + fiber + registry wired
//! behind the stable facade (M1-P4).

use std::sync::{Arc, Mutex};

use jinnd_api::{
    Activation, ErrorCode, FiberState, IsolationBinding, Kernel, KernelFuture, PluginContract,
    Realm, ServiceContract, TransitionCause,
};

#[derive(Clone, Debug)]
struct Recorder {
    seen: Arc<Mutex<Vec<String>>>,
}

impl Recorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn seen(&self) -> Vec<String> {
        self.seen
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

impl PluginContract for Recorder {
    type Config = String;
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/recorder";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, ()>,
        config: String,
    ) -> KernelFuture<'a, ()> {
        self.seen
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(config);
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct Beacon(u8);

impl ServiceContract for Beacon {
    type Observation = u8;

    const NAME: &'static str = "jinn.test/beacon";

    fn observe(&self) -> u8 {
        self.0
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_spawned_plugin_activates_and_rests_active() {
    let kernel = jinnd_adapter::kernel();
    let plugin = Recorder::new();
    let seen = plugin.clone();

    let Ok(fiber) = kernel
        .spawn(kernel.root_context(), plugin, "one".to_owned())
        .await
    else {
        panic!("a dependency-free plugin must spawn");
    };
    assert_eq!(kernel.state(fiber), FiberState::Active);
    assert_eq!(seen.seen(), vec!["one".to_owned()]);

    let transitions = kernel.transitions(fiber);
    let states: Vec<FiberState> = transitions.iter().map(|transition| transition.to).collect();
    assert_eq!(
        states,
        vec![FiberState::Loading, FiberState::Active],
        "the fiber's history is published through the facade (R6)"
    );
    assert!(
        transitions
            .iter()
            .all(|transition| transition.fiber == fiber),
        "every transition is charged to the spawned fiber"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn each_config_update_lands_before_the_next_is_stated() {
    let kernel = jinnd_adapter::kernel();
    let plugin = Recorder::new();
    let seen = plugin.clone();

    let Ok(fiber) = kernel
        .spawn(kernel.root_context(), plugin, "a".to_owned())
        .await
    else {
        panic!("the recorder must spawn");
    };
    let Ok(()) = kernel.update::<Recorder>(fiber, "b".to_owned()).await else {
        panic!("the first update must settle");
    };
    let Ok(()) = kernel.update::<Recorder>(fiber, "c".to_owned()).await else {
        panic!("the second update must settle");
    };
    let Ok(()) = kernel.wait_for_quiescence().await else {
        panic!("quiescence must be reachable");
    };

    assert_eq!(
        seen.seen(),
        vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
        "every stated config lands, in order, none coalesced away (§3 loader contract)"
    );
    assert_eq!(kernel.state(fiber), FiberState::Active);
    assert!(
        kernel
            .transitions(fiber)
            .iter()
            .any(|transition| transition.cause == TransitionCause::ConfigChanged),
        "updates are recorded under their config-change provenance"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn restart_runs_one_full_clean_reload() {
    let kernel = jinnd_adapter::kernel();
    let plugin = Recorder::new();
    let seen = plugin.clone();

    let Ok(fiber) = kernel
        .spawn(kernel.root_context(), plugin, "same".to_owned())
        .await
    else {
        panic!("the recorder must spawn");
    };
    let Ok(()) = kernel.restart(fiber).await else {
        panic!("the restart must settle");
    };

    assert_eq!(seen.seen(), vec!["same".to_owned(), "same".to_owned()]);
    assert_eq!(kernel.state(fiber), FiberState::Active);
}

#[tokio::test(flavor = "current_thread")]
async fn dispose_is_terminal_and_unknown_fibers_read_disposed() {
    let kernel = jinnd_adapter::kernel();

    let Ok(fiber) = kernel
        .spawn(kernel.root_context(), Recorder::new(), "x".to_owned())
        .await
    else {
        panic!("the recorder must spawn");
    };
    let Ok(()) = kernel.dispose(fiber).await else {
        panic!("disposal must settle");
    };
    assert_eq!(kernel.state(fiber), FiberState::Disposed);

    assert_eq!(
        kernel.state(jinnd_api::FiberId(u64::MAX)),
        FiberState::Disposed,
        "a fiber this kernel never spawned is not live (uids are never reused, R3)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn provide_and_resolve_work_through_the_facade() {
    let kernel = jinnd_adapter::kernel();
    let root = kernel.root_context();

    let Ok(effect) = kernel.provide(root, Realm::Root, Arc::new(Beacon(3))).await else {
        panic!("the provision must install");
    };
    let resolved = kernel.resolve::<Beacon>(root);
    let Ok(handle) = resolved else {
        panic!("the provided beacon must resolve: {resolved:?}");
    };
    assert_eq!(handle.service.observe(), 3);
    assert_eq!(handle.caller, root);
    assert_eq!(handle.realm, Realm::Root);

    let labels: Vec<String> = kernel
        .effect_tree(jinnd_api::FiberId(0))
        .iter()
        .map(|descriptor| descriptor.label.clone())
        .collect();
    assert!(
        labels.iter().any(|label| label.contains(Beacon::NAME)),
        "the provision is a labelled effect on the kernel scope (R5): {labels:?}, {effect:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn resolution_honors_an_isolation_boundary_between_contexts() {
    let kernel = jinnd_adapter::kernel();
    let root = kernel.root_context();
    let isolated = kernel.derive_context(
        root,
        vec![IsolationBinding {
            service: Beacon::NAME.to_owned(),
            realm: Realm::Shared("apart".to_owned()),
        }],
    );
    assert_ne!(isolated, root);

    let Ok(_effect) = kernel.provide(root, Realm::Root, Arc::new(Beacon(8))).await else {
        panic!("the provision must install");
    };
    assert!(kernel.resolve::<Beacon>(root).is_ok());
    let error = match kernel.resolve::<Beacon>(isolated) {
        Err(error) => error,
        Ok(handle) => panic!("the boundary must hide the root provider: {handle:?}"),
    };
    assert_eq!(error.code, ErrorCode::MissingDependency);
}

#[tokio::test(flavor = "current_thread")]
async fn a_facade_effect_registers_on_the_kernel_scope() {
    let kernel = jinnd_adapter::kernel();
    struct Noop;
    impl jinnd_api::Undo for Noop {
        fn undo(self: Box<Self>) -> jinnd_api::KernelFuture<'static, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    let Ok(effect) = kernel.register_effect(
        kernel.root_context(),
        "test: marker".to_owned(),
        Box::new(Noop),
    ) else {
        panic!("the effect must register");
    };
    let tree = kernel.effect_tree(jinnd_api::FiberId(0));
    assert!(
        tree.iter().any(|descriptor| descriptor.id == effect),
        "the registered effect is introspectable (R5): {tree:?}"
    );
}
