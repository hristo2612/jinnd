//! The loader's handle onto one wasm entry (M2-K4): the generic lane handle
//! plus the ENTRY's own obligation — a dispose is the entry leaving the
//! composition, so after the fiber's withdrawal the entry's retained
//! journal (what suspended incarnations handed back) withdraws too, LIFO,
//! through each effect's provider. Everything else delegates to the fiber.

use std::any::Any;
use std::sync::Arc;

use jinnd_api::{EntryId, FiberId, FiberState, KernelError, KernelFuture, TransitionCause};
use jinnd_context::Context;
use jinnd_fiber::Fiber;
use jinnd_loader::EntryHandle;
use jinnd_loader::host::{Rebind, config_of};

use crate::lane::{LaneCore, WasmBody};

/// One wasm entry's handle: the fiber, its body, and the lane that holds
/// the entry's journal.
pub(crate) struct WasmHandle<C, R> {
    fiber: Arc<Fiber>,
    body: Arc<WasmBody>,
    core: Arc<LaneCore>,
    entry: EntryId,
    restate: R,
    _config: std::marker::PhantomData<fn(C)>,
}

impl<C, R> WasmHandle<C, R>
where
    C: Clone + 'static,
    R: Fn(&WasmBody, C) -> Result<(), KernelError> + Send + Sync + 'static,
{
    pub(crate) fn new(
        fiber: Arc<Fiber>,
        body: Arc<WasmBody>,
        core: Arc<LaneCore>,
        entry: EntryId,
        restate: R,
    ) -> Self {
        Self {
            fiber,
            body,
            core,
            entry,
            restate,
            _config: std::marker::PhantomData,
        }
    }
}

impl<C, R> EntryHandle for WasmHandle<C, R>
where
    C: Clone + 'static,
    R: Fn(&WasmBody, C) -> Result<(), KernelError> + Send + Sync + 'static,
{
    fn id(&self) -> FiberId {
        self.fiber.id()
    }

    fn state(&self) -> FiberState {
        self.fiber.state()
    }

    fn withdrawing(&self) -> bool {
        self.fiber.withdrawing()
    }

    fn resting(&self) -> bool {
        self.fiber.resting()
    }

    fn restart(&self, cause: TransitionCause) {
        self.fiber.restart(cause);
    }

    fn restate(&self, config: &(dyn Any + Send + Sync)) -> Result<(), KernelError> {
        (self.restate)(&self.body, config_of::<C>(config)?)
    }

    fn rebind(&self, at: Context<()>) {
        self.body.rebind(at);
    }

    /// The fiber's withdrawal, then the entry's retained journal: the
    /// live seat's trail first (newest), the inherited effects after it
    /// (older) — one strictly LIFO trail across incarnations (R5, I1).
    fn dispose(&self) -> KernelFuture<'static, ()> {
        let fiber = Arc::clone(&self.fiber);
        let core = Arc::clone(&self.core);
        let entry = self.entry.clone();
        Box::pin(async move {
            fiber.dispose().await;
            core.withdraw_journal(&entry, Some(fiber.id())).await
        })
    }

    fn quiesce(&self) -> KernelFuture<'static, ()> {
        let fiber = Arc::clone(&self.fiber);
        Box::pin(async move {
            fiber.quiesce().await;
            Ok(())
        })
    }
}
