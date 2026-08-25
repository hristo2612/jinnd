//! Panic containment for one plugin body (R11).

use std::any::Any;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

use jinnd_api::{ErrorCode, FiberId, KernelError, KernelFuture};

/// Builds and drives one activation, turning a panic into a recorded failure.
///
/// A body can panic at two boundaries: constructing its future, and polling it. Both
/// are caught here, so no plugin-authored code can unwind past the fiber that owns
/// it and no sibling ever observes the crash (R11). A body that merely *errors* is
/// the same outcome with a better message; either way the fiber fails alone and the
/// effects it had already registered are withdrawn by the caller.
pub(crate) async fn contained<'a, F>(fiber: FiberId, make: F) -> Result<(), KernelError>
where
    F: FnOnce() -> KernelFuture<'a, ()>,
{
    let outcome = match panic::catch_unwind(AssertUnwindSafe(make)) {
        Ok(future) => {
            Contained {
                future: Some(future),
            }
            .await
        }
        Err(payload) => Err(describe(payload)),
    };

    match outcome {
        Ok(Ok(())) => Ok(()),
        Ok(Err(mut error)) => {
            error.fiber.get_or_insert(fiber);
            Err(error)
        }
        Err(panic) => Err(KernelError {
            code: ErrorCode::PluginFailed,
            message: format!("the plugin body panicked: {panic}"),
            fiber: Some(fiber),
        }),
    }
}

/// A boxed activation future plus the guarantee that it is polled at most to
/// completion.
///
/// `KernelFuture` is a `Pin<Box<_>>`, so this wrapper is `Unpin` and needs no pin
/// projection — and therefore no unsafe code — to poll what it holds.
/// `AssertUnwindSafe` is honest here: a future that panicked is dropped at once and
/// never polled again, and nothing this crate keeps is borrowed across the call.
struct Contained<'a> {
    future: Option<KernelFuture<'a, ()>>,
}

impl Future for Contained<'_> {
    type Output = Result<Result<(), KernelError>, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // Polling a completed future is a caller contract violation. Staying pending
        // is the only response that neither panics (R11) nor polls it again.
        let Some(future) = this.future.as_mut() else {
            return Poll::Pending;
        };

        let polled = match panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(result)) => Ok(result),
            Err(payload) => Err(describe(payload)),
        };
        // The future's destructor is plugin code too: it is dropped behind the same
        // containment its poll ran behind, so a panicking Drop fails this one fiber
        // instead of unwinding through its supervisor (R11).
        let dropped = contain_drop(this.future.take());
        Poll::Ready(match (polled, dropped) {
            (Err(panic), _) | (Ok(Ok(())), Err(panic)) => Err(panic),
            // A body that already failed keeps its own failure as the recorded
            // cause; a Drop panic behind it is contained all the same.
            (Ok(result), _) => Ok(result),
        })
    }
}

impl Drop for Contained<'_> {
    /// The wrapper is dropped mid-flight only when its whole task is; even then
    /// the plugin future's destructor stays contained (R11).
    fn drop(&mut self) {
        let _ = contain_drop(self.future.take());
    }
}

/// Drops a plugin future behind `catch_unwind`, reporting a panicking destructor.
fn contain_drop(future: Option<KernelFuture<'_, ()>>) -> Result<(), String> {
    panic::catch_unwind(AssertUnwindSafe(move || drop(future))).map_err(describe)
}

/// Renders a panic payload for the record.
fn describe(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_owned();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "the panic carried a non-string payload".to_owned()
}
