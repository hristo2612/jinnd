//! One child stream between the guest's non-blocking calls and a pipe
//! (M2-K6; R1, R9): a bounded ring plus the signal that resumes the task
//! waiting on it. Output: a pump task fills the ring, a take wakes it
//! when stalled. Stdin: an offer fills the ring, a feeder task drains it
//! into the pipe. No guest call ever touches a pipe.

use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelError};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::ChildStdin;
use tokio::sync::Notify;

use super::ring::{Ring, STREAM_CAP};
use crate::broker_state::refusal;

#[derive(Clone)]
pub(super) struct Stream {
    ring: Arc<Ring>,
    space: Arc<Notify>,
}

impl Stream {
    pub(super) fn new() -> Self {
        Self {
            ring: Arc::new(Ring::new(STREAM_CAP)),
            space: Arc::new(Notify::new()),
        }
    }

    /// One non-blocking read: `(bytes, eof)`; a take that made room wakes
    /// the pump.
    pub(super) fn take(&self, max: usize) -> (Vec<u8>, bool) {
        let (data, eof) = self.ring.take(max);
        if !data.is_empty() {
            self.space.notify_one();
        }
        (data, eof)
    }

    /// Offers bytes without blocking; answers the accepted count (up to the
    /// ring's free space), waking the feeder.
    ///
    /// # Errors
    ///
    /// A closed stream.
    pub(super) fn offer(&self, bytes: &[u8]) -> Result<usize, KernelError> {
        if self.ring.is_closed() {
            return Err(refusal(
                ErrorCode::PluginFailed,
                "process stdin is closed".to_owned(),
            ));
        }
        let accepted = self.ring.offer(bytes);
        self.space.notify_one();
        Ok(accepted)
    }

    /// Ends the stream and frees a stalled pump or feeder.
    pub(super) fn release(&self) {
        self.ring.close();
        self.space.notify_one();
    }

    /// Ends a stream that never had a pipe.
    pub(super) fn close(&self) {
        self.ring.close();
    }

    #[cfg(test)]
    pub(super) fn buffered(&self) -> usize {
        self.ring.len()
    }
}

/// Moves one pipe into its ring; stalls on a full ring until a take makes
/// room (backpressure, R9); closes the ring at the pipe's end.
pub(super) async fn pump(mut pipe: impl AsyncRead + Unpin, stream: Stream) {
    let mut chunk = vec![0u8; 8192];
    loop {
        let read = match pipe.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        let mut offset = 0;
        while offset < read {
            offset += stream.ring.offer(&chunk[offset..read]);
            if offset < read {
                if stream.ring.is_closed() {
                    return;
                }
                stream.space.notified().await;
            }
        }
    }
    stream.ring.close();
}

/// Drains the stdin ring into the pipe; idles until an offer; the ring's
/// close (the guest's `close-stdin`, or the release) is the child's EOF.
pub(super) async fn feed(mut pipe: ChildStdin, stream: Stream) {
    loop {
        let (chunk, eof) = stream.ring.take(8192);
        if !chunk.is_empty() {
            if pipe.write_all(&chunk).await.is_err() {
                stream.ring.close();
                return;
            }
        } else if eof {
            let _ = pipe.shutdown().await;
            return;
        } else {
            stream.space.notified().await;
        }
    }
}
