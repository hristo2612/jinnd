//! The guest-facing `jinn:process` and `jinn:net` imports (M2-K6): each
//! call is one handle-less broker crossing — grant check → ledger append
//! → dispatch to the live provider — under the wire `wit/plugin.wit`
//! declares. A spawn, a listen, and an accept mint KERNEL REGISTRATIONS:
//! the answered handle joins THIS instance's journal so suspend and
//! dispose release it through the provider, LIFO with the rest (R5;
//! M2-K4 lifecycle class). Every answer crosses as the bundle's own
//! error variant (round 4, R3): a guest matches, never parses. Split
//! from `hostcaps.rs` by responsibility (R10 file hygiene).

use crate::bindings::{net, process};
use crate::handle::{HostRecord, Registration};
use crate::hostwire::{
    self, Reader, decode_handle, decode_response, encode_request, encode_spawn, put_segment,
};
use crate::instance::HostState;
use jinnd_api::KernelError;

/// The process provider's contract name.
pub const PROCESS_CONTRACT: &str = "jinn:process";
/// The net provider's contract name.
pub const NET_CONTRACT: &str = "jinn:net";

/// The Law-2 label one kernel registration is journaled and released
/// under, shared by the provider's ledger line and the seat's withdrawal.
#[must_use]
pub fn registration_label(contract: &str, what: &str, handle: u64) -> String {
    format!("{contract} {what} [handle {handle}]")
}

/// One crossing, facade-typed: each import converts at its boundary into
/// its bundle's error (`From` in `bindings`), so the guest sees the variant.
async fn crossing(
    state: &HostState,
    contract: &str,
    operation: &str,
    payload: Vec<u8>,
) -> Result<Vec<u8>, KernelError> {
    state
        .seat
        .broker
        .dispatch(state.seat.peer, contract, operation, payload)
        .await
}

/// One registering crossing: admitted into the journal first (a sealed
/// seat refuses on the record), dispatched, then the answered handle
/// joins the journal under its label.
async fn registering(
    state: &mut HostState,
    contract: &str,
    operation: &str,
    what: &str,
    payload: Vec<u8>,
) -> Result<u64, KernelError> {
    state.admit(&format!("{contract} {operation}"))?;
    let answer = crossing(state, contract, operation, payload).await?;
    let handle = decode_handle(&answer)?;
    state
        .outcome
        .registrations
        .push(Registration::Kernel(HostRecord {
            contract: contract.to_owned(),
            label: registration_label(contract, what, handle),
            effect: handle,
        }));
    Ok(handle)
}

fn handle_payload(handle: u64, tail: &[u8]) -> Vec<u8> {
    let mut wire = handle.to_le_bytes().to_vec();
    wire.extend(tail);
    wire
}

fn read_answer(answer: &[u8]) -> Result<(u8, Vec<u8>), KernelError> {
    let mut reader = Reader::new(answer, "read answer");
    let tag = reader.u8()?;
    Ok((tag, reader.rest().to_vec()))
}

fn count_answer(answer: &[u8]) -> Result<u32, KernelError> {
    Reader::new(answer, "count answer").u32()
}

impl process::Host for HostState {
    async fn run(
        &mut self,
        command: String,
        args: Vec<String>,
    ) -> Result<Vec<u8>, process::ProcessError> {
        let mut wire = Vec::new();
        put_segment(&mut wire, command.as_bytes());
        for arg in &args {
            put_segment(&mut wire, arg.as_bytes());
        }
        let answer = crossing(self, PROCESS_CONTRACT, "run", wire).await?;
        match read_answer(&answer)? {
            (hostwire::TAG_DATA, data) => Ok(data),
            (hostwire::TAG_TRUNCATED, _) => Err(process::ProcessError::OutputTruncated),
            _ => Err(process::ProcessError::Failed(
                "malformed run answer".to_owned(),
            )),
        }
    }

    async fn spawn(
        &mut self,
        command: String,
        args: Vec<String>,
        cwd: Option<String>,
        env: Vec<(String, String)>,
    ) -> Result<u64, process::ProcessError> {
        let payload = encode_spawn(&command, &args, cwd.as_deref(), &env);
        Ok(registering(self, PROCESS_CONTRACT, "spawn", "spawn", payload).await?)
    }

    async fn write_stdin(
        &mut self,
        handle: u64,
        bytes: Vec<u8>,
    ) -> Result<u32, process::ProcessError> {
        let answer = crossing(
            self,
            PROCESS_CONTRACT,
            "write-stdin",
            handle_payload(handle, &bytes),
        )
        .await?;
        Ok(count_answer(&answer)?)
    }

    async fn close_stdin(&mut self, handle: u64) -> Result<(), process::ProcessError> {
        crossing(
            self,
            PROCESS_CONTRACT,
            "close-stdin",
            handle_payload(handle, &[]),
        )
        .await?;
        Ok(())
    }

    async fn read(
        &mut self,
        handle: u64,
        which: process::ChildStream,
        max: u32,
    ) -> Result<process::ReadResult, process::ProcessError> {
        let mut tail = vec![match which {
            process::ChildStream::Stdout => 0,
            process::ChildStream::Stderr => 1,
        }];
        tail.extend(max.to_le_bytes());
        let answer = crossing(
            self,
            PROCESS_CONTRACT,
            "read",
            handle_payload(handle, &tail),
        )
        .await?;
        Ok(match read_answer(&answer)? {
            (hostwire::TAG_DATA, data) => process::ReadResult::Data(data),
            (hostwire::TAG_EOF, _) => process::ReadResult::Eof,
            _ => process::ReadResult::WouldBlock,
        })
    }

    async fn wait(
        &mut self,
        handle: u64,
        timeout_ms: u64,
    ) -> Result<process::WaitResult, process::ProcessError> {
        let answer = crossing(
            self,
            PROCESS_CONTRACT,
            "wait",
            handle_payload(handle, &timeout_ms.to_le_bytes()),
        )
        .await?;
        let mut reader = Reader::new(&answer, "wait answer");
        let tag = reader.u8()?;
        Ok(if tag == hostwire::TAG_DATA {
            process::WaitResult::Exited(reader.i32()?)
        } else {
            process::WaitResult::Running
        })
    }

    async fn kill(
        &mut self,
        handle: u64,
        signal: process::Signal,
    ) -> Result<(), process::ProcessError> {
        let byte = match signal {
            process::Signal::Interrupt => 0,
            process::Signal::Terminate => 1,
            process::Signal::Kill => 2,
        };
        crossing(
            self,
            PROCESS_CONTRACT,
            "kill",
            handle_payload(handle, &[byte]),
        )
        .await?;
        Ok(())
    }
}

impl net::Host for HostState {
    /// The outbound one-shot at its 0.1.0 declaration (M2-K14): admitted
    /// into the journal like any effect (a sealed seat refuses on the
    /// record), then ONE crossing. The call registers NO undo — it is
    /// declared irreversible, and a journal entry claiming otherwise would
    /// be the Law-3 falsehood.
    async fn request(
        &mut self,
        method: String,
        url: String,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, net::NetError> {
        self.admit("jinn:net request")?;
        let mut wire = Vec::new();
        put_segment(&mut wire, method.as_bytes());
        put_segment(&mut wire, url.as_bytes());
        wire.extend(body);
        Ok(crossing(self, NET_CONTRACT, "request", wire).await?)
    }

    /// The whole-response edition (0.2.0, additive): the same door, the
    /// same journal admission, the same irreversible class — the caller
    /// simply sees the headers and the status too.
    async fn send_request(
        &mut self,
        req: net::OutboundRequest,
    ) -> Result<net::OutboundResponse, net::NetError> {
        self.admit("jinn:net send-request")?;
        let wire = encode_request(&req.method, &req.url, &req.headers, &req.body);
        let answer = crossing(self, NET_CONTRACT, "send-request", wire).await?;
        let (status, headers, body) = decode_response(&answer)?;
        Ok(net::OutboundResponse {
            status,
            headers,
            body,
        })
    }

    async fn listen(&mut self, addr: String) -> Result<u64, net::NetError> {
        Ok(registering(self, NET_CONTRACT, "listen", "listen", addr.into_bytes()).await?)
    }

    async fn accept(&mut self, listener: u64) -> Result<net::AcceptResult, net::NetError> {
        self.admit("jinn:net accept")?;
        let answer = crossing(self, NET_CONTRACT, "accept", handle_payload(listener, &[])).await?;
        let mut reader = Reader::new(&answer, "accept answer");
        if reader.u8()? != hostwire::TAG_DATA {
            return Ok(net::AcceptResult::WouldBlock);
        }
        let handle = reader.u64()?;
        self.outcome
            .registrations
            .push(Registration::Kernel(HostRecord {
                contract: NET_CONTRACT.to_owned(),
                label: registration_label(NET_CONTRACT, "accept", handle),
                effect: handle,
            }));
        Ok(net::AcceptResult::Connection(handle))
    }

    async fn read(&mut self, connection: u64, max: u32) -> Result<net::ReadResult, net::NetError> {
        let answer = crossing(
            self,
            NET_CONTRACT,
            "read",
            handle_payload(connection, &max.to_le_bytes()),
        )
        .await?;
        Ok(match read_answer(&answer)? {
            (hostwire::TAG_DATA, data) => net::ReadResult::Data(data),
            (hostwire::TAG_EOF, _) => net::ReadResult::Eof,
            _ => net::ReadResult::WouldBlock,
        })
    }

    async fn write(&mut self, connection: u64, bytes: Vec<u8>) -> Result<u32, net::NetError> {
        let answer = crossing(
            self,
            NET_CONTRACT,
            "write",
            handle_payload(connection, &bytes),
        )
        .await?;
        Ok(count_answer(&answer)?)
    }

    async fn close(&mut self, handle: u64) -> Result<(), net::NetError> {
        crossing(self, NET_CONTRACT, "close", handle_payload(handle, &[])).await?;
        Ok(())
    }
}
