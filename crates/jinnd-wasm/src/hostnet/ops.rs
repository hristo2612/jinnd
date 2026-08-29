//! The `jinn:net` handle operations behind the provider's broker face
//! (wit/plugin.wit `interface net`): non-blocking accept / read / write,
//! and the guest's close. Split from `hostnet.rs` by responsibility (R10
//! file hygiene).

use std::sync::Arc;
use std::task::Poll;

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind};

use tokio::net::TcpStream;

use super::{HostNet, Socket, Wake, failed};
use crate::broker_state::refusal;
use crate::hostwire::{Reader, TAG_DATA, TAG_WOULD_BLOCK, encode_read};
use crate::peer::PeerId;

/// The largest single read a guest may ask for.
const READ_CAP: usize = 64 * 1024;

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
                    listener, fiber, ..
                },
            ) => {
                // One poll, no wait (R1): readiness arrives on a later
                // poll — or on the readiness wake (M2-K7). The guest acted
                // on the listener: its next readiness wakes again.
                self.rearm(handle, &socket);
                let fiber = *fiber;
                let pending =
                    std::future::poll_fn(|cx| Poll::Ready(listener.poll_read_ready(cx))).await;
                let accepted = match pending {
                    Poll::Ready(Ok(mut guard)) => {
                        match guard.try_io(|fd| fd.get_ref().accept()) {
                            Ok(Ok((stream, _))) => stream
                                .set_nonblocking(true)
                                .and_then(|()| TcpStream::from_std(stream)),
                            Ok(Err(error)) => Err(error),
                            // Readiness cleared: nothing pending after all.
                            Err(_would_block) => return Ok(vec![TAG_WOULD_BLOCK]),
                        }
                    }
                    Poll::Ready(Err(error)) => Err(error),
                    Poll::Pending => return Ok(vec![TAG_WOULD_BLOCK]),
                };
                let stream = accepted.map_err(|error| failed("accept", &error))?;
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
