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
/// `AssertUnwindSafe` is honest at both sites: a future that panicked is dropped
/// immediately and never polled again, and nothing this crate keeps is left borrowed
/// across the call, so no half-updated state is observable afterwards. Whatever the
/// inverse itself half-did is exactly what the returned outcome reports.
pub(crate) async fn contained<F>(make: F) -> Result<Result<(), KernelError>, String>
where
    F: FnOnce() -> KernelFuture<'static, ()>,
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

/// A boxed inverse future plus the guarantee that it is polled at most to completion.
///
/// `KernelFuture` is a `Pin<Box<_>>`, so this wrapper is `Unpin` and needs no pin
/// projection — and therefore no unsafe code — to poll what it holds.
struct Contained {
    future: Option<KernelFuture<'static, ()>>,
}

impl Future for Contained {
    type Output = Result<Result<(), KernelError>, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // Polling a future after it completed is a caller contract violation. Staying
        // pending is the only response that neither panics (R11) nor polls a future
        // that has already returned `Ready`.
        let Some(future) = this.future.as_mut() else {
            return Poll::Pending;
        };

        match panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => {
                drop_contained(&mut this.future);
                Poll::Ready(Ok(result))
            }
            Err(payload) => {
                drop_contained(&mut this.future);
                Poll::Ready(Err(describe(payload)))
            }
        }
    }
}

/// Drops the inverse future, containing a panic raised by its own destructor.
fn drop_contained(future: &mut Option<KernelFuture<'static, ()>>) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| drop(future.take())));
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
