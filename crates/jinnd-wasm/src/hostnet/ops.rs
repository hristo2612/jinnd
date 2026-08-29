//! The `jinn:net` handle operations behind the provider's broker face
//! (wit/plugin.wit `interface net`): non-blocking accept / read / write,
//! and the guest's close. Split from `hostnet.rs` by responsibility (R10
//! file hygiene).

use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind};

use tokio::io::ReadBuf;
use tokio::io::unix::AsyncFd;
use tokio::net::TcpStream;

use super::{HostNet, Socket, Wake, failed};
use crate::broker_state::refusal;
use crate::hostwire::{Reader, TAG_DATA, TAG_WOULD_BLOCK, encode_read};
use crate::peer::PeerId;

/// The largest single read a guest may ask for.
const READ_CAP: usize = 64 * 1024;

/// One non-blocking accept behind tokio readiness: `None` when nothing is
/// pending (which CLEARS the readiness), else the accepted stream adopted
/// by tokio. A probe, never a wait (R1): the noop waker registers nothing
/// a re-arm does not re-register.
fn try_accept(listener: &AsyncFd<std::net::TcpListener>) -> Option<std::io::Result<TcpStream>> {
    match listener.poll_read_ready(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(mut guard)) => match guard.try_io(|fd| fd.get_ref().accept()) {
            Ok(Ok((stream, _))) => Some(
                stream
                    .set_nonblocking(true)
                    .and_then(|()| TcpStream::from_std(stream)),
            ),
            Ok(Err(error)) => Some(Err(error)),
            Err(_would_block) => None,
        },
        Poll::Ready(Err(error)) => Some(Err(error)),
        Poll::Pending => None,
    }
}

/// The level probe after a read (M2-K7 round 2): bytes or EOF still
/// pending keep the readiness, so the re-arm wakes — rightly; nothing
/// pending clears it, so the re-arm cannot re-announce what was just read.
fn probe_pending(stream: &TcpStream) {
    let mut byte = [0u8; 1];
    let mut probe = ReadBuf::new(&mut byte);
    let _ = stream.poll_peek(&mut Context::from_waker(Waker::noop()), &mut probe);
}

impl HostNet {
    /// One operation on a caller-owned handle: 8-byte LE handle first,
    /// then the operation's own fields.
    pub(super) async fn handle_op(
        self: &Arc<Self>,
        caller: PeerId,
        operation: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, KernelError> {
        let mut reader = Reader::new(payload, "net handle");
        let handle = reader.u64()?;
        let socket = self.core.row(caller, handle)?;
        match (operation, &socket) {
            (
                "accept",
                Socket::Listener {
                    listener,
                    fiber,
                    pending,
                    ..
                },
            ) => {
                let fiber = *fiber;
                // Consume FIRST, re-arm AFTER (M2-K7 round 2): the stashed
                // connection a previous probe took, else one poll, no wait
                // (R1). A would-block cleared the readiness: the re-arm
                // waits for the next transition.
                let stashed = pending.lock().unwrap_or_else(|p| p.into_inner()).take();
                let accepted = match stashed.map(Ok).or_else(|| try_accept(listener)) {
                    Some(accepted) => accepted,
                    None => {
                        self.rearm(handle, &socket);
                        return Ok(vec![TAG_WOULD_BLOCK]);
                    }
                };
                let stream = accepted.map_err(|error| failed("accept", &error))?;
                // The level probe: a second pending connection is stashed
                // (readiness stays set — the re-arm wakes, rightly, exactly
                // once); none clears the readiness, so the re-arm cannot
                // re-announce the connection just consumed.
                if let Some(Ok(next)) = try_accept(listener) {
                    *pending.lock().unwrap_or_else(|p| p.into_inner()) = Some(next);
                }
                self.rearm(handle, &socket);
                let conn = self.hold(
                    caller,
                    Socket::Conn {
                        owner: caller,
                        fiber,
                        stream: Arc::new(stream),
                        wake: Arc::new(Wake::default()),
                    },
                );
                self.core.sink.append(
                    LedgerEventKind::NetAccepted {
                        listener: handle,
                        handle: conn,
                    },
                    fiber,
                );
                let mut wire = vec![TAG_DATA];
                wire.extend(conn.to_le_bytes());
                Ok(wire)
            }
            ("read", Socket::Conn { stream, .. }) => {
                let max = (reader.u32()? as usize).clamp(1, READ_CAP);
                let mut buffer = vec![0u8; max];
                Ok(match stream.try_read(&mut buffer) {
                    // EOF is final: the peer's close stays readable, so no
                    // re-arm — the guest heard it once (R9).
                    Ok(0) => encode_read(None, true),
                    Ok(count) => {
                        probe_pending(stream);
                        self.rearm(handle, &socket);
                        buffer.truncate(count);
                        encode_read(Some(buffer), false)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        self.rearm(handle, &socket);
                        encode_read(None, false)
                    }
                    Err(error) => return Err(failed("read", &error)),
                })
            }
            ("write", Socket::Conn { stream, .. }) => {
                let count = match stream.try_write(reader.rest()) {
                    Ok(count) => count,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => 0,
                    Err(error) => return Err(failed("write", &error)),
                };
                Ok(u32::try_from(count)
                    .unwrap_or(u32::MAX)
                    .to_le_bytes()
                    .to_vec())
            }
            ("close", _) => {
                self.withdraw(handle).await?;
                Ok(Vec::new())
            }
            (other, _) => Err(refusal(
                ErrorCode::PluginFailed,
                format!("net operation {other:?} does not apply to handle {handle}"),
            )),
        }
    }
}
