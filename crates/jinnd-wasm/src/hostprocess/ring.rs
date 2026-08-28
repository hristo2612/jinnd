//! The bounded per-stream buffer between a child's output pump and the
//! guest's non-blocking reads (M2-K6; R9): a guest that never reads gets
//! backpressure — the pump stalls, the child blocks on its pipe — never
//! unbounded host memory. Pure decision core over one lock; it compiles
//! under loom and its offer/take/close interleavings are pinned in
//! `ring_tests.rs` (the stream/suspend loom obligation of the card).

use std::collections::VecDeque;

use crate::sync::Mutex;

/// Per-stream capacity in bytes.
pub(crate) const STREAM_CAP: usize = 64 * 1024;

pub(crate) struct Ring {
    inner: Mutex<Inner>,
    cap: usize,
}

struct Inner {
    data: VecDeque<u8>,
    closed: bool,
}

impl Ring {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                data: VecDeque::new(),
                closed: false,
            }),
            cap,
        }
    }

    fn lock(&self) -> impl std::ops::DerefMut<Target = Inner> + '_ {
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Accepts a prefix of `bytes` up to the free space; 0 when full or
    /// closed (the pump waits for a take, or stops).
    pub(crate) fn offer(&self, bytes: &[u8]) -> usize {
        let mut inner = self.lock();
        if inner.closed {
            return 0;
        }
        let room = self.cap.saturating_sub(inner.data.len());
        let accepted = room.min(bytes.len());
        inner.data.extend(&bytes[..accepted]);
        accepted
    }

    /// Takes up to `max` bytes; the flag is EOF — closed and now drained.
    pub(crate) fn take(&self, max: usize) -> (Vec<u8>, bool) {
        let mut inner = self.lock();
        let count = max.min(inner.data.len());
        let data: Vec<u8> = inner.data.drain(..count).collect();
        let eof = inner.closed && inner.data.is_empty();
        (data, eof)
    }

    /// Ends the stream: no offer lands after this returns; takes drain
    /// what is buffered, then answer EOF.
    pub(crate) fn close(&self) {
        self.lock().closed = true;
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.lock().closed
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().data.len()
    }
}
