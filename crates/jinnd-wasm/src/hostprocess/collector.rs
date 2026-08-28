//! The one-shot `run`'s stdout collector (M2-K6 round 3; R9, R11): OWNED
//! by the call, bounded like the long-lived edition. The pipe is pumped
//! into a bounded ring by a task the collector holds; the call drains the
//! ring into a total capped at [`RUN_OUTPUT_CAP`] (bundle metadata
//! `output-cap-bytes`). Past the cap, or at the deadline, the collector is
//! CUT: the pump task is aborted and awaited, so the host's read end is
//! closed before the kill — a descendant that inherited the pipe gets
//! EPIPE, never a live sink. A torn pipe or a dead pump task is a typed
//! error: `run` never answers success with defaulted output.

use std::io;

use jinnd_api::{ErrorCode, KernelError};
use tokio::io::AsyncRead;
use tokio::task::JoinHandle;

use super::stream::{Stream, pump};
use crate::broker_state::refusal;

/// The hard total-output cap of one `run`, declared in the bundle
/// metadata: more than this answers a typed `output-truncated`.
pub(super) const RUN_OUTPUT_CAP: usize = 1 << 20;

/// The collected total passed the cap.
pub(super) struct Overflow;

pub(super) struct Collector {
    stream: Stream,
    pump: JoinHandle<io::Result<()>>,
    bytes: Vec<u8>,
}

impl Collector {
    /// Starts pumping `pipe` into a bounded ring under a task this
    /// collector owns.
    pub(super) fn start(pipe: impl AsyncRead + Unpin + Send + 'static) -> Self {
        let stream = Stream::new();
        let pump = tokio::spawn(pump(pipe, stream.clone()));
        Self::from_parts(stream, pump)
    }

    pub(super) fn from_parts(stream: Stream, pump: JoinHandle<io::Result<()>>) -> Self {
        Self {
            stream,
            pump,
            bytes: Vec::new(),
        }
    }

    /// Drains the ring into the total until the stream ends, or the total
    /// passes the cap. Cancel-safe: the total lives here, a cancelled
    /// wait loses nothing.
    pub(super) async fn drain(&mut self) -> Result<(), Overflow> {
        loop {
            let (data, eof) = self.stream.take(8192);
            self.bytes.extend(data);
            if self.bytes.len() > RUN_OUTPUT_CAP {
                return Err(Overflow);
            }
            if eof {
                return Ok(());
            }
            self.stream.readable().await;
        }
    }

    /// Closes the host's read end NOW: the pump is aborted and awaited, so
    /// no task and no pipe outlive this — whoever still writes gets EPIPE.
    pub(super) async fn cut(&mut self) {
        self.pump.abort();
        let _ = (&mut self.pump).await;
        self.stream.close();
    }

    /// The honest finish after the stream ended: the pump's own verdict.
    ///
    /// # Errors
    ///
    /// A torn pipe (`io::Error`) or a dead pump task, typed — never an
    /// empty success.
    pub(super) async fn finish(self) -> Result<Vec<u8>, KernelError> {
        match self.pump.await {
            Ok(Ok(())) => Ok(self.bytes),
            Ok(Err(error)) => Err(refusal(
                ErrorCode::PluginFailed,
                format!("process run stdout collector: {error}"),
            )),
            Err(error) => Err(refusal(
                ErrorCode::PluginFailed,
                format!("process run stdout collector task: {error}"),
            )),
        }
    }

    #[cfg(test)]
    pub(super) fn buffered(&self) -> usize {
        self.bytes.len()
    }
}
