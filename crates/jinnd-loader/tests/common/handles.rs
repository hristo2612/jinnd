//! The fixture lane's entry handles (split from `mod.rs` by the 300-line
//! file cap, R10): the loader-facing seam over real spawned fibers.

#![allow(dead_code)]

use std::any::Any;
use std::sync::Arc;

use jinnd_api::{ErrorCode, FiberId, KernelError, KernelFuture, TransitionCause};
use jinnd_context::Context;
use jinnd_fiber::{Fiber, FiberBody};
use jinnd_loader::EntryHandle;

use super::FixtureBody;

pub(crate) struct TestHandle {
    pub(crate) fiber: Arc<Fiber>,
    pub(crate) body: Arc<FixtureBody>,
}

impl EntryHandle for TestHandle {
    fn id(&self) -> FiberId {
        self.fiber.id()
    }

    fn state(&self) -> jinnd_api::FiberState {
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
        let Some(config) = config.downcast_ref::<u32>() else {
            return Err(KernelError {
                code: ErrorCode::InvalidProfile,
                message: "fixture config must be u32".to_owned(),
                fiber: Some(self.fiber.id()),
            });
        };
        *self
            .body
            .cell
            .config
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = *config;
        Ok(())
    }

    fn rebind(&self, at: Context<()>) {
        *self
            .body
            .cell
            .at
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = at;
    }

    fn dispose(&self) -> KernelFuture<'static, ()> {
        let fiber = Arc::clone(&self.fiber);
        Box::pin(async move {
            fiber.dispose().await;
            Ok(())
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

/// A plain lane handle over one spawned fiber: restate accepts anything, rebind
/// is a no-op. For conformance tests whose bodies read neither config nor
/// context after spawn.
pub struct PlainHandle {
    pub fiber: Arc<Fiber>,
}

impl EntryHandle for PlainHandle {
    fn id(&self) -> FiberId {
        self.fiber.id()
    }

    fn state(&self) -> jinnd_api::FiberState {
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

    fn restate(&self, _config: &(dyn Any + Send + Sync)) -> Result<(), KernelError> {
        Ok(())
    }

    fn rebind(&self, _at: Context<()>) {}

    fn dispose(&self) -> KernelFuture<'static, ()> {
        let fiber = Arc::clone(&self.fiber);
        Box::pin(async move {
            fiber.dispose().await;
            Ok(())
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

/// Spawns `body` on a lane request's signal and wraps it as a plain handle.
pub fn plain_spawn(
    body: Arc<dyn FiberBody>,
    signal: jinnd_fiber::WatchReadiness,
) -> Arc<dyn EntryHandle> {
    Arc::new(PlainHandle {
        fiber: Arc::new(Fiber::spawn(body, signal)),
    }) as Arc<dyn EntryHandle>
}
