mod support;

use std::sync::Arc;

use jinnd_api::{
    Activation, EntryId, ErrorCode, Inject, Kernel, KernelFuture, PluginContract, PluginRef,
    Profile, ProfileEntry, ServiceContract, ServiceHandle, ServiceResolver, ServiceType,
};
use support::{expect_ok, facade_gap_at, spec_case};

const SUBSYSTEM: support::Subsystem = support::Subsystem::Fiber;
const FACADE_GAP_REASON: &str = "the facade cannot declare one package lane that both provides and injects services, so it cannot express a dependency cycle";

#[derive(Debug)]
struct LinkService;

impl ServiceContract for LinkService {
    type Observation = ();

    const NAME: &'static str = "jinn.test/progress-link";

    fn observe(&self) {}
}

#[derive(Debug)]
struct NeedsLink {
    _service: ServiceHandle<LinkService>,
}

impl Inject for NeedsLink {
    fn declare() -> Vec<ServiceType> {
        vec![ServiceType::of::<LinkService>()]
    }

    fn inject<R: ServiceResolver + ?Sized>(resolver: &R) -> Result<Self, jinnd_api::KernelError> {
        Ok(Self {
            _service: resolver.resolve::<LinkService>()?,
        })
    }
}

#[derive(Debug)]
struct LinkConsumer;

impl PluginContract for LinkConsumer {
    type Config = u8;
    type Dependencies = NeedsLink;

    const NAME: &'static str = "jinn.test/progress-consumer";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, NeedsLink>,
        _config: u8,
    ) -> KernelFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct Sibling;

impl PluginContract for Sibling {
    type Config = u8;
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/progress-sibling";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, ()>,
        _config: u8,
    ) -> KernelFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn entry(id: &str, package: &str) -> ProfileEntry<u8> {
    ProfileEntry {
        id: EntryId(id.to_owned()),
        plugin: PluginRef {
            package: package.to_owned(),
            version: "1".to_owned(),
            artifact_hash: String::new(),
        },
        config: 0,
        disabled: false,
        parent: None,
        isolation: Vec::new(),
    }
}

spec_case! {
    /// Paper origin: progress theorem; SOURCE-OF-TRUTH §4 invariant I3.
    acyclic_dependency_precedence_always_reaches_quiescence,
    origin: "paper: progress theorem / I3",
    test: "acyclic dependency graph reaches quiescence",
    setup: ["acyclic graph qux -> foo -> bar and qux -> bar starts in arbitrary registration order"],
    actions: ["provide leaves", "wait with a bounded virtual-time deadline"],
    expected: ["wait completes before deadline", "every satisfiable fiber is active", "no transition remains in flight"]
}

spec_case! {
    /// Paper origin: progress theorem; SOURCE-OF-TRUTH §4 invariant I3.
    dependency_cycle_is_detected_and_left_cleanly_inactive,
    origin: "paper: progress theorem / I3",
    test: "cycle yields clean inactivity and quiescence",
    setup: ["fibers alpha, beta, gamma form a dependency cycle", "unrelated sibling is acyclic"],
    actions: ["register graph", "wait with a bounded virtual-time deadline"],
    expected: ["cycle is reported statically", "cycle members are pending or failed with no effects", "unrelated sibling reaches active", "kernel is quiescent"],
    body: |case| {
        const PROVIDER: &str = "jinn.test/progress-provider-package";
        const CONSUMER: &str = "jinn.test/progress-consumer-package";
        const SIBLING: &str = "jinn.test/progress-sibling-package";

        let kernel = jinnd_adapter::kernel();
        expect_ok(
            kernel.register_provider_package(PROVIDER, |_config: u8| {
                Ok(Arc::new(LinkService))
            }),
            "the provider half should register",
        );
        expect_ok(
            kernel.register_package(CONSUMER, |config: u8| Ok((LinkConsumer, config))),
            "the injector half should register",
        );
        expect_ok(
            kernel.register_package(SIBLING, |config: u8| Ok((Sibling, config))),
            "the acyclic sibling should register",
        );

        let report = expect_ok(
            kernel
                .reconcile(Profile {
                    entries: vec![
                        entry("provider-half", PROVIDER),
                        entry("consumer-half", CONSUMER),
                        entry("sibling", SIBLING),
                    ],
                })
                .await,
            "the closest expressible split graph should reconcile",
        );
        expect_ok(
            kernel.wait_for_quiescence().await,
            "the split graph should quiesce",
        );
        assert!(
            report
                .errors
                .iter()
                .all(|fault| fault.error.code != ErrorCode::DependencyCycle),
            "separate provider and injector entries are acyclic"
        );
        for id in ["provider-half", "consumer-half", "sibling"] {
            assert!(
                kernel.entry_fiber(&EntryId(id.to_owned())).is_some(),
                "the expressible acyclic entry {id} should have a fiber"
            );
        }

        facade_gap_at(&case, FACADE_GAP_REASON);
    }
}
