//! Panic containment for one listener call (R11).

use std::any::Any;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

use jinnd_api::{ErrorCode, KernelError, KernelFuture};

/// Builds and drives one listener call, turning a panic into a recorded failure.
///
/// A listener can panic at two boundaries: constructing its future, and polling
/// it. Both are caught here, so no plugin-authored listener can unwind past the
/// dispatch walk — the walk records the failure and the remaining listeners
/// still run (R9). A listener that merely *errors* is the same outcome with its
/// own message; either way the outcome is one value, never an unwind.
pub(crate) async fn contained<'a, T, F>(make: F) -> Result<T, KernelError>
where
    F: FnOnce() -> KernelFuture<'a, T>,
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
        Ok(result) => result,
        Err(panic) => Err(KernelError {
            code: ErrorCode::ListenerFailed,
            message: format!("the listener panicked: {panic}"),
            fiber: None,
        }),
    }
}

/// Drops a plugin-owned value behind containment (R11).
///
/// A listener handle released by a claiming walk may be the final one, so the
/// destructor that runs is plugin code; the same holds for a payload clone the
/// walk discards. A panicking destructor becomes a recorded failure, never an
/// unwind out of the walk (R9).
pub(crate) fn releasing<T>(value: T) -> Result<(), KernelError> {
    panic::catch_unwind(AssertUnwindSafe(move || drop(value))).map_err(|payload| KernelError {
        code: ErrorCode::ListenerFailed,
        message: format!(
            "a plugin-authored destructor panicked: {}",
            describe(payload)
        ),
        fiber: None,
    })
}

/// Runs payload-owned selection or folding code, reporting a panic it raises.
pub(crate) fn catching<T, F>(body: F) -> Result<T, KernelError>
where
    F: FnOnce() -> T,
{
    panic::catch_unwind(AssertUnwindSafe(body)).map_err(|payload| KernelError {
        code: ErrorCode::ListenerFailed,
        message: format!("the payload's routing code panicked: {}", describe(payload)),
        fiber: None,
    })
}

/// A boxed listener future plus the guarantee that it is polled at most to
/// completion.
///
/// `KernelFuture` is a `Pin<Box<_>>`, so this wrapper is `Unpin` and needs no
/// pin projection — and therefore no unsafe code — to poll what it holds.
/// `AssertUnwindSafe` is honest here: a future that panicked is dropped at once
/// and never polled again, and nothing this crate keeps is borrowed across the
/// call.
struct Contained<'a, T> {
    future: Option<KernelFuture<'a, T>>,
}

impl<T> Future for Contained<'_, T> {
    type Output = Result<Result<T, KernelError>, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // Polling a completed future is a caller contract violation. Staying
        // pending is the only response that neither panics (R11) nor polls it
        // again.
        let Some(future) = this.future.as_mut() else {
            return Poll::Pending;
        };

        let polled = match panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(result)) => Ok(result),
            Err(payload) => Err(describe(payload)),
        };
        // The future's destructor is plugin code too: it is dropped behind the
        // same containment its poll ran behind (R11).
        let dropped = contain_drop(this.future.take());
        Poll::Ready(match (polled, dropped) {
            (Err(panic), _) | (Ok(Ok(_)), Err(panic)) => Err(panic),
            // A listener that already failed keeps its own failure as the
            // recorded cause; a Drop panic behind it is contained all the same.
            (Ok(result), _) => Ok(result),
        })
    }
}

impl<T> Drop for Contained<'_, T> {
    /// The wrapper is dropped mid-flight only when its whole walk is; even then
    /// the listener future's destructor stays contained (R11).
    fn drop(&mut self) {
        let _ = contain_drop(self.future.take());
    }
}

/// Drops a listener future behind `catch_unwind`, reporting a panicking
/// destructor.
fn contain_drop<T>(future: Option<KernelFuture<'_, T>>) -> Result<(), String> {
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
