//! Package-lane registration through the facade: duplicate refusal, effect
//! withdrawal, and containment of plugin-owned declaration code (R5, R9, R11).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jinnd_adapter::kernel;
use jinnd_api::{
    Activation, EntryId, ErrorCode, Inject, Kernel, KernelError, KernelFuture, PluginContract,
    PluginRef, Profile, ProfileEntry, ServiceResolver, ServiceType,
};

/// A declaration that panics before naming anything.
#[derive(Debug)]
struct PanickingDeclaration;

impl Inject for PanickingDeclaration {
    fn declare() -> Vec<ServiceType> {
        panic!("lane dependency-declaration panic");
    }

    fn inject<R: ServiceResolver + ?Sized>(_resolver: &R) -> Result<Self, KernelError> {
        Ok(Self)
    }
}

#[derive(Clone, Debug)]
struct Saboteur;

impl PluginContract for Saboteur {
    type Config = u32;
    type Dependencies = PanickingDeclaration;

    const NAME: &'static str = "jinn.test/saboteur";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, PanickingDeclaration>,
        _config: u32,
    ) -> KernelFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct Counting {
    counter: Arc<AtomicUsize>,
}

impl PluginContract for Counting {
    type Config = u32;
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/counting";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, ()>,
        _config: u32,
    ) -> KernelFuture<'a, ()> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

fn entry(name: &str, package: &str, config: u32) -> ProfileEntry<u32> {
    ProfileEntry {
        id: EntryId(name.to_owned()),
        plugin: PluginRef {
            package: package.to_owned(),
            version: "1".to_owned(),
            artifact_hash: String::new(),
        },
        config,
        disabled: false,
        parent: None,
        isolation: Vec::new(),
    }
}

fn id(name: &str) -> EntryId {
    EntryId(name.to_owned())
}

#[tokio::test(flavor = "current_thread")]
async fn unregistered_packages_fault_per_entry_without_failing_the_reconcile() {
    let kernel = kernel();
    let report = kernel
        .reconcile(Profile {
            entries: vec![entry("ghost", "jinn.test/unregistered", 1)],
        })
        .await
        .grab();
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].entry, id("ghost"));
    assert_eq!(report.errors[0].error.code, ErrorCode::InvalidProfile);
    assert!(kernel.entry_fiber(&id("ghost")).is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn package_registration_is_a_withdrawable_effect() {
    let kernel = kernel();
    let counter = Arc::new(AtomicUsize::new(0));
    let lane_counter = Arc::clone(&counter);
    let registration = kernel
        .register_package("jinn.test/counting", move |config: u32| {
            Ok((
                Counting {
                    counter: Arc::clone(&lane_counter),
                },
                config,
            ))
        })
        .grab();

    // A second registration of the same package is refused (R9).
    let counter_again = Arc::clone(&counter);
    let duplicate = kernel.register_package("jinn.test/counting", move |config: u32| {
        Ok((
            Counting {
                counter: Arc::clone(&counter_again),
            },
            config,
        ))
    });
    let Err(refused) = duplicate.map(|_| ()) else {
        panic!("a duplicate package registration must be refused (R9)");
    };
    assert_eq!(refused.code, ErrorCode::InvalidProfile);
    let _ = registration;
}

#[tokio::test(flavor = "current_thread")]
async fn a_panicking_declaration_refuses_the_package_registration() {
    let kernel = kernel();

    // The declaration is plugin-owned code: its panic is contained at the
    // registration boundary and answered as this plugin's failure — the
    // package never registers, so no entry of it can ever activate (R11).
    let refused = kernel.register_package("jinn.test/saboteur", |config: u32| {
        let _ = config;
        Ok((Saboteur, 0))
    });
    let Err(error) = refused.map(|_| ()) else {
        panic!("a panicking dependency declaration must refuse the registration (R11)");
    };
    assert_eq!(
        error.code,
        ErrorCode::PluginFailed,
        "the panic surfaces as a plugin failure, never as `no dependencies`: {error:?}"
    );

    // The kernel stays fully usable and an entry naming the package is a
    // contained per-entry fault, exactly like any unregistered package.
    let report = kernel
        .reconcile(Profile {
            entries: vec![entry("sab", "jinn.test/saboteur", 1)],
        })
        .await
        .grab();
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].entry, id("sab"));
    assert!(kernel.entry_fiber(&id("sab")).is_none());
    kernel.wait_for_quiescence().await.grab();
}

/// Repo convention: no `unwrap`/`expect`. `grab` is the tests' one panicking
/// accessor, carrying the caller's location.
pub trait Grab<T> {
    fn grab(self) -> T;
}

impl<T, E: std::fmt::Debug> Grab<T> for Result<T, E> {
    #[track_caller]
    fn grab(self) -> T {
        match self {
            Ok(value) => value,
            Err(error) => panic!("unexpected error: {error:?}"),
        }
    }
}

impl<T> Grab<T> for Option<T> {
    #[track_caller]
    fn grab(self) -> T {
        match self {
            Some(value) => value,
            None => panic!("unexpectedly empty"),
        }
    }
}
