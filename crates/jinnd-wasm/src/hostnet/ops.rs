//! The `jinn:net` handle operations behind the provider's broker face
//! (wit/plugin.wit `interface net`): non-blocking accept / read / write,
//! and the guest's close. Split from `hostnet.rs` by responsibility (R10
//! file hygiene).

use std::sync::Arc;
use std::task::Poll;

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind};

use super::{HostNet, Socket, failed};
use crate::broker_state::refusal;
use crate::hostwire::{Reader, TAG_DATA, TAG_WOULD_BLOCK, encode_read};
use crate::peer::PeerId;

/// The largest single read a guest may ask for.
const READ_CAP: usize = 64 * 1024;

impl HostNet {
    /// One operation on a caller-owned handle: 8-byte LE handle first,
    /// then the operation's own fields.
    pub(super) async fn handle_op(
        &self,
        caller: PeerId,
        operation: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, KernelError> {
        let mut reader = Reader::new(payload, "net handle");
        let handle = reader.u64()?;
        let socket = self.core.row(caller, handle)?;
        match (operation, socket) {
            (
                "accept",
                Socket::Listener {
                    listener, fiber, ..
                },
            ) => {
                // One poll, no wait (R1): readiness arrives on a later poll.
                let pending =
                    std::future::poll_fn(|cx| Poll::Ready(listener.poll_accept(cx))).await;
                match pending {
                    Poll::Ready(Ok((stream, _))) => {
                        let conn = self.core.mint();
                        self.core.insert(
                            conn,
                            Socket::Conn {
                                owner: caller,
                                fiber,
                                stream: Arc::new(stream),
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
                    Poll::Ready(Err(error)) => Err(failed("accept", &error)),
                    Poll::Pending => Ok(vec![TAG_WOULD_BLOCK]),
                }
            }
            ("read", Socket::Conn { stream, .. }) => {
                let max = (reader.u32()? as usize).clamp(1, READ_CAP);
                let mut buffer = vec![0u8; max];
                Ok(match stream.try_read(&mut buffer) {
                    Ok(0) => encode_read(None, true),
                    Ok(count) => {
                        buffer.truncate(count);
                        encode_read(Some(buffer), false)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
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
