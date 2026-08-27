//! Panic containment for one inverse (R11).

use std::any::Any;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

use jinnd_api::{KernelError, KernelFuture};

/// Builds and drives one inverse, turning a panic into a value.
///
/// An inverse can panic at either of two boundaries: constructing its future
/// (`make`, which runs the caller's closure) and polling it. Both are caught here,
/// so no plugin-authored inverse can unwind past the kernel.
///
/// Its destructor is a third boundary, and just as plugin-authored: a panic raised
/// while the finished future is dropped outranks whatever the inverse returned,
/// because a withdrawal that ends in a panic is not a clean one.
///
/// `AssertUnwindSafe` is honest at every site: a future that panicked is dropped
/// immediately and never polled again, and nothing this crate keeps is left borrowed
/// across the call, so no half-updated state is observable afterwards. Whatever the
/// inverse itself half-did is exactly what the returned outcome reports.
pub(crate) async fn contained<T, F>(make: F) -> Result<Result<T, KernelError>, String>
where
    F: FnOnce() -> KernelFuture<'static, T>,
{
    match panic::catch_unwind(AssertUnwindSafe(make)) {
        Ok(future) => {
            Contained {
                future: Some(future),
            }
            .await
        }
        Err(payload) => Err(describe(payload)),
    }
}

/// Runs `body`, turning a panic it raises into its rendered payload (R11).
pub(crate) fn catching<T, F>(body: F) -> Result<T, String>
where
    F: FnOnce() -> T,
{
    panic::catch_unwind(AssertUnwindSafe(body)).map_err(describe)
}

/// Runs `body` for its effect only, reporting a panic it raised.
pub(crate) fn caught<F>(body: F) -> Option<String>
where
    F: FnOnce(),
{
    catching(body).err()
}

/// A boxed inverse future plus the guarantee that it is polled at most to completion.
///
/// `KernelFuture` is a `Pin<Box<_>>`, so this wrapper is `Unpin` and needs no pin
/// projection — and therefore no unsafe code — to poll what it holds.
struct Contained<T> {
    future: Option<KernelFuture<'static, T>>,
}

impl<T> Future for Contained<T> {
    type Output = Result<Result<T, KernelError>, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // Polling a future after it completed is a caller contract violation. Staying
        // pending is the only response that neither panics (R11) nor polls a future
        // that has already returned `Ready`.
        let Some(future) = this.future.as_mut() else {
            return Poll::Pending;
        };

        let outcome = match catching(|| future.as_mut().poll(cx)) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(result)) => Ok(result),
            Err(panic) => Err(panic),
        };

        // The inverse is finished either way, so drop it here rather than leaving it
        // for a destructor that has nowhere to report. A panic from that destructor
        // is the stronger signal and outranks what the inverse returned.
        Poll::Ready(match caught(|| drop(this.future.take())) {
            Some(panic) => Err(panic),
            None => outcome,
        })
    }
}

/// Renders a panic payload for the report.
fn describe(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_owned();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "inverse panicked with a non-string payload".to_owned()
}
