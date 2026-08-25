//! R11 pin (M1-P4 round-2 blocker): `Dependencies::declare()` is plugin-owned
//! code; a panic it raises must be contained at the spawn boundary and answered
//! as a `KernelError`, never unwound through the kernel.

use jinnd_api::{
    Activation, ErrorCode, Inject, Kernel, KernelError, KernelFuture, PluginContract,
    ServiceResolver, ServiceType,
};

/// A declaration that panics before naming anything.
#[derive(Debug)]
struct PanickingDeclaration;

impl Inject for PanickingDeclaration {
    fn declare() -> Vec<ServiceType> {
        panic!("verifier dependency-declaration panic");
    }

    fn inject<R: ServiceResolver + ?Sized>(_resolver: &R) -> Result<Self, KernelError> {
        Ok(Self)
    }
}

#[derive(Clone, Debug)]
struct Saboteur;

impl PluginContract for Saboteur {
    type Config = ();
    type Dependencies = PanickingDeclaration;

    const NAME: &'static str = "jinn.test/saboteur";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, PanickingDeclaration>,
        (): (),
    ) -> KernelFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_panicking_dependency_declaration_is_a_contained_kernel_error() {
    let kernel = jinnd_adapter::kernel();
    let root = kernel.root_context();

    let Err(error) = kernel.spawn(root, Saboteur, ()).await else {
        panic!("a panicking declaration must fail the spawn (R11)");
    };
    assert_eq!(
        error.code,
        ErrorCode::PluginFailed,
        "the panic surfaces as a plugin failure, charged to no live fiber: {error:?}"
    );

    // The kernel stays fully usable afterwards: the failure was local (R11).
    let Ok(()) = kernel.wait_for_quiescence().await else {
        panic!("a contained spawn failure must not wedge the kernel");
    };
}
