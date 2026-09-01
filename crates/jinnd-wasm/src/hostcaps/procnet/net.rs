//! The guest-facing `jinn:net` import (M2-K6; outbound M2-K14). A listen
//! and an accept mint KERNEL REGISTRATIONS journaled for LIFO release; the
//! two outbound one-shots mint NONE — they are declared irreversible, and
//! a journal entry claiming otherwise would be the Law-3 falsehood this
//! packet exists to prevent. Every answer crosses as the bundle's own
//! error variant, so a guest matches and never parses (R3).

use crate::bindings::net;
use crate::handle::{HostRecord, Registration};
use crate::hostwire::{self, Reader, decode_response, encode_request, put_segment};
use crate::instance::HostState;

use super::{
    NET_CONTRACT, count_answer, crossing, handle_payload, read_answer, registering,
    registration_label,
};

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
