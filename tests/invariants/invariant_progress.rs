mod support;

use std::sync::Arc;

use jinnd_api::{
    Activation, EntryId, ErrorCode, Inject, Kernel, KernelFuture, PluginContract, PluginRef,
    Profile, ProfileEntry, ServiceContract, ServiceHandle, ServiceResolver, ServiceType,
};
use support::{expect_ok, spec_case};

const SUBSYSTEM: support::Subsystem = support::Subsystem::Fiber;
const FACADE_GAP_REASON: &str = "the facade cannot declare one package lane that both provides and injects services, so it cannot express a dependency cycle";

macro_rules! service {
    ($name:ident, $contract:literal) => {
        #[derive(Debug)]
        struct $name;

        impl ServiceContract for $name {
            type Observation = ();
            const NAME: &'static str = $contract;
            fn observe(&self) {}
        }
    };
}

service!(LinkA, "jinn.test/progress-a");
service!(LinkB, "jinn.test/progress-b");
service!(LinkC, "jinn.test/progress-c");

macro_rules! needs {
    ($name:ident, $service:ident) => {
        #[derive(Debug)]
        struct $name {
            _service: ServiceHandle<$service>,
        }

        impl Inject for $name {
            fn declare() -> Vec<ServiceType> {
                vec![ServiceType::of::<$service>()]
            }

            fn inject<R: ServiceResolver + ?Sized>(
                resolver: &R,
            ) -> Result<Self, jinnd_api::KernelError> {
                Ok(Self {
                    _service: resolver.resolve::<$service>()?,
                })
            }
        }
    };
}

needs!(NeedsA, LinkA);
needs!(NeedsB, LinkB);
needs!(NeedsC, LinkC);

macro_rules! plugin {
    ($name:ident, $dependencies:ident, $contract:literal) => {
        #[derive(Debug)]
        struct $name;

        impl PluginContract for $name {
            type Config = u8;
            type Dependencies = $dependencies;

            const NAME: &'static str = $contract;

            fn activate<'a>(
                &'a self,
                _activation: Activation<'a, $dependencies>,
                _config: u8,
            ) -> KernelFuture<'a, ()> {
                Box::pin(async { Ok(()) })
            }
        }
    };
}

plugin!(PluginA, NeedsB, "jinn.test/progress-plugin-a");
plugin!(PluginB, NeedsC, "jinn.test/progress-plugin-b");
plugin!(PluginC, NeedsA, "jinn.test/progress-plugin-c");

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
    body: |_case| {
        const ALPHA: &str = "jinn.test/progress-alpha-package";
        const BETA: &str = "jinn.test/progress-beta-package";
        const GAMMA: &str = "jinn.test/progress-gamma-package";
        const SIBLING: &str = "jinn.test/progress-sibling-package";

        let kernel = jinnd_adapter::kernel();
        expect_ok(
            kernel.register_providing_package(ALPHA, |config: u8| {
                Ok((PluginA, config, Arc::new(LinkA)))
            }),
            "alpha should register",
        );
        expect_ok(
            kernel.register_providing_package(BETA, |config: u8| {
                Ok((PluginB, config, Arc::new(LinkB)))
            }),
            "beta should register",
        );
        expect_ok(
            kernel.register_providing_package(GAMMA, |config: u8| {
                Ok((PluginC, config, Arc::new(LinkC)))
            }),
            "gamma should register",
        );
        expect_ok(
            kernel.register_package(SIBLING, |config: u8| Ok((Sibling, config))),
            "the acyclic sibling should register",
        );

        let report = expect_ok(
            kernel
                .reconcile(Profile {
                    entries: vec![
                        entry("alpha", ALPHA),
                        entry("beta", BETA),
                        entry("gamma", GAMMA),
                        entry("sibling", SIBLING),
                    ],
                })
                .await,
            "the cyclic graph should reconcile with contained faults",
        );
        expect_ok(
            kernel.wait_for_quiescence().await,
            "the cyclic graph should quiesce",
        );
        let mut cycle_entries: Vec<&str> = report
            .errors
            .iter()
            .filter(|fault| fault.error.code == ErrorCode::DependencyCycle)
            .map(|fault| fault.entry.0.as_str())
            .collect();
        cycle_entries.sort_unstable();
        assert_eq!(
            cycle_entries,
            ["alpha", "beta", "gamma"],
            "every cycle member is diagnosed statically",
        );
        for id in ["alpha", "beta", "gamma"] {
            assert!(kernel.entry_fiber(&EntryId(id.to_owned())).is_none());
        }
        let sibling = kernel
            .entry_fiber(&EntryId("sibling".to_owned()))
            .unwrap_or_else(|| panic!("the unrelated sibling should have a fiber"));
        assert_eq!(kernel.state(sibling), jinnd_api::FiberState::Active);
        assert!(kernel.effect_tree(sibling).is_empty());
    }
}
