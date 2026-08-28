//! The guest-facing `jinn:process` and `jinn:net` imports (M2-K6): each
//! call is one handle-less broker crossing — grant check → ledger append
//! → dispatch to the live provider — under the wire `wit/plugin.wit`
//! declares. A spawn, a listen, and an accept mint KERNEL REGISTRATIONS:
//! the answered handle joins THIS instance's journal so suspend and
//! dispose release it through the provider, LIFO with the rest (R5;
//! M2-K4 lifecycle class). Split from `hostcaps.rs` by responsibility
//! (R10 file hygiene).

use crate::bindings::{self, net, process, types::KernelError};
use crate::handle::{HostRecord, Registration};
use crate::hostwire::{self, Reader, decode_handle, encode_spawn, put_segment};
use crate::instance::HostState;

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
        .map_err(bindings::wire_error)
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
    state
        .admit(&format!("{contract} {operation}"))
        .map_err(bindings::wire_error)?;
    let answer = crossing(state, contract, operation, payload).await?;
    let handle = decode_handle(&answer).map_err(bindings::wire_error)?;
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
    let tag = reader.u8().map_err(bindings::wire_error)?;
    Ok((tag, reader.rest().to_vec()))
}

fn count_answer(answer: &[u8]) -> Result<u32, KernelError> {
    Reader::new(answer, "count answer")
        .u32()
        .map_err(bindings::wire_error)
}

impl process::Host for HostState {
    async fn run(&mut self, command: String, args: Vec<String>) -> Result<Vec<u8>, KernelError> {
        let mut wire = Vec::new();
        put_segment(&mut wire, command.as_bytes());
        for arg in &args {
            put_segment(&mut wire, arg.as_bytes());
        }
        crossing(self, PROCESS_CONTRACT, "run", wire).await
    }

    async fn spawn(
        &mut self,
        command: String,
        args: Vec<String>,
        cwd: Option<String>,
        env: Vec<(String, String)>,
    ) -> Result<u64, KernelError> {
        let payload = encode_spawn(&command, &args, cwd.as_deref(), &env);
        registering(self, PROCESS_CONTRACT, "spawn", "spawn", payload).await
    }

    async fn write_stdin(&mut self, handle: u64, bytes: Vec<u8>) -> Result<u32, KernelError> {
        let answer = crossing(
            self,
            PROCESS_CONTRACT,
            "write-stdin",
            handle_payload(handle, &bytes),
        )
        .await?;
        count_answer(&answer)
    }

    async fn close_stdin(&mut self, handle: u64) -> Result<(), KernelError> {
        crossing(
            self,
            PROCESS_CONTRACT,
            "close-stdin",
            handle_payload(handle, &[]),
        )
        .await
        .map(|_| ())
    }

    async fn read(
        &mut self,
        handle: u64,
        which: process::ChildStream,
        max: u32,
    ) -> Result<process::ReadResult, KernelError> {
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
    ) -> Result<process::WaitResult, KernelError> {
        let answer = crossing(
            self,
            PROCESS_CONTRACT,
            "wait",
            handle_payload(handle, &timeout_ms.to_le_bytes()),
        )
        .await?;
        let mut reader = Reader::new(&answer, "wait answer");
        let tag = reader.u8().map_err(bindings::wire_error)?;
        Ok(if tag == hostwire::TAG_DATA {
            process::WaitResult::Exited(reader.i32().map_err(bindings::wire_error)?)
        } else {
            process::WaitResult::Running
        })
    }

    async fn kill(&mut self, handle: u64, signal: process::Signal) -> Result<(), KernelError> {
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
        .await
        .map(|_| ())
    }
}

impl net::Host for HostState {
    async fn request(
        &mut self,
        method: String,
        url: String,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, KernelError> {
        let mut wire = Vec::new();
        put_segment(&mut wire, method.as_bytes());
        put_segment(&mut wire, url.as_bytes());
        wire.extend(body);
        crossing(self, NET_CONTRACT, "request", wire).await
    }

    async fn listen(&mut self, addr: String) -> Result<u64, KernelError> {
        registering(self, NET_CONTRACT, "listen", "listen", addr.into_bytes()).await
    }

    async fn accept(&mut self, listener: u64) -> Result<net::AcceptResult, KernelError> {
        self.admit("jinn:net accept")
            .map_err(bindings::wire_error)?;
        let answer = crossing(self, NET_CONTRACT, "accept", handle_payload(listener, &[])).await?;
        let mut reader = Reader::new(&answer, "accept answer");
        if reader.u8().map_err(bindings::wire_error)? != hostwire::TAG_DATA {
            return Ok(net::AcceptResult::WouldBlock);
        }
        let handle = reader.u64().map_err(bindings::wire_error)?;
        self.outcome
            .registrations
            .push(Registration::Kernel(HostRecord {
                contract: NET_CONTRACT.to_owned(),
                label: registration_label(NET_CONTRACT, "accept", handle),
                effect: handle,
            }));
        Ok(net::AcceptResult::Connection(handle))
    }

    async fn read(&mut self, connection: u64, max: u32) -> Result<net::ReadResult, KernelError> {
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

    async fn write(&mut self, connection: u64, bytes: Vec<u8>) -> Result<u32, KernelError> {
        let answer = crossing(
            self,
            NET_CONTRACT,
            "write",
            handle_payload(connection, &bytes),
        )
        .await?;
        count_answer(&answer)
    }

    async fn close(&mut self, handle: u64) -> Result<(), KernelError> {
        crossing(self, NET_CONTRACT, "close", handle_payload(handle, &[]))
            .await
            .map(|_| ())
    }
}
