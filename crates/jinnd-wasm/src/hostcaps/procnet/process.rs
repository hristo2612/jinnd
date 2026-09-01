//! The guest-facing `jinn:process` import (M2-K6). A spawn mints a KERNEL
//! REGISTRATION: the answered handle joins this instance's journal so
//! suspend and dispose release it through the provider, LIFO with the rest
//! (R5; M2-K4 lifecycle class). Every answer crosses as the bundle's own
//! error variant, so a guest matches and never parses (R3).

use crate::bindings::process;
use crate::hostwire::{self, Reader, encode_spawn, put_segment};
use crate::instance::HostState;

use super::{PROCESS_CONTRACT, count_answer, crossing, handle_payload, read_answer, registering};

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
