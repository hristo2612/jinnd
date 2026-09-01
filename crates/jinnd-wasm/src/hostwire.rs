//! The `jinn:process` / `jinn:net` broker wire (wit/plugin.wit `interface
//! process`, `interface net`; M2-K6): one codec the host glue writes and
//! the providers read, and back for answers. The contract files declare
//! the shapes; this module merely binds them (R12). Every decode is
//! bounds-checked and refuses a truncated payload as malformed.

use jinnd_api::{ErrorCode, KernelError};

use crate::broker_state::refusal;

/// Answer tag: bytes follow.
pub(crate) const TAG_DATA: u8 = 0;
/// Answer tag: nothing yet (`would-block`); `running` for `wait`.
pub(crate) const TAG_WOULD_BLOCK: u8 = 1;
/// Answer tag: the stream ended (`eof`).
pub(crate) const TAG_EOF: u8 = 2;
/// `run` answer tag: output past the cap (`output-truncated`, R9) — typed
/// on the broker wire so the guest matches it as the bundle's variant.
pub(crate) const TAG_TRUNCATED: u8 = 3;

/// Appends one u32-LE length-prefixed segment.
pub fn put_segment(wire: &mut Vec<u8>, segment: &[u8]) {
    wire.extend(
        u32::try_from(segment.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    wire.extend(segment);
}

/// A bounds-checked cursor over one payload.
pub(crate) struct Reader<'a> {
    wire: &'a [u8],
    what: &'static str,
}

impl<'a> Reader<'a> {
    #[must_use]
    pub fn new(wire: &'a [u8], what: &'static str) -> Self {
        Self { wire, what }
    }

    fn malformed(&self) -> KernelError {
        refusal(
            ErrorCode::PluginFailed,
            format!("malformed {} payload", self.what),
        )
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], KernelError> {
        let (head, rest) = self
            .wire
            .split_at_checked(count)
            .ok_or_else(|| self.malformed())?;
        self.wire = rest;
        Ok(head)
    }

    /// One byte; a truncated payload is malformed.
    pub fn u8(&mut self) -> Result<u8, KernelError> {
        Ok(self.take(1)?[0])
    }

    /// One u32-LE.
    pub fn u32(&mut self) -> Result<u32, KernelError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    /// One u64-LE (handles, timeouts).
    pub fn u64(&mut self) -> Result<u64, KernelError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    /// One i32-LE (exit codes).
    pub fn i32(&mut self) -> Result<i32, KernelError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(i32::from_le_bytes(bytes))
    }

    /// One length-prefixed segment.
    pub fn segment(&mut self) -> Result<&'a [u8], KernelError> {
        let length = self.u32()? as usize;
        self.take(length)
    }

    /// One length-prefixed UTF-8 segment (non-UTF-8 is malformed).
    pub fn text(&mut self) -> Result<String, KernelError> {
        let bytes = self.segment()?;
        String::from_utf8(bytes.to_vec()).map_err(|_| self.malformed())
    }

    /// Everything left.
    #[must_use]
    pub fn rest(self) -> &'a [u8] {
        self.wire
    }

    /// Nothing left.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.wire.is_empty()
    }
}

/// Decodes a `spawn` request: argc, envc, then command, cwd, args, env.
#[allow(clippy::type_complexity)]
pub fn decode_spawn(
    payload: &[u8],
) -> Result<(String, Vec<String>, Option<String>, Vec<(String, String)>), KernelError> {
    let mut reader = Reader::new(payload, "process spawn");
    let argc = reader.u32()?;
    let envc = reader.u32()?;
    let command = reader.text()?;
    let cwd = reader.text()?;
    let args = (0..argc)
        .map(|_| reader.text())
        .collect::<Result<Vec<_>, _>>()?;
    let env = (0..envc)
        .map(|_| Ok((reader.text()?, reader.text()?)))
        .collect::<Result<Vec<_>, KernelError>>()?;
    let cwd = (!cwd.is_empty()).then_some(cwd);
    Ok((command, args, cwd, env))
}

/// Encodes a `spawn` request (the shape [`decode_spawn`] reads).
#[must_use]
pub fn encode_spawn(
    command: &str,
    args: &[String],
    cwd: Option<&str>,
    env: &[(String, String)],
) -> Vec<u8> {
    let mut wire = Vec::new();
    wire.extend(u32::try_from(args.len()).unwrap_or(u32::MAX).to_le_bytes());
    wire.extend(u32::try_from(env.len()).unwrap_or(u32::MAX).to_le_bytes());
    put_segment(&mut wire, command.as_bytes());
    put_segment(&mut wire, cwd.unwrap_or("").as_bytes());
    for arg in args {
        put_segment(&mut wire, arg.as_bytes());
    }
    for (name, value) in env {
        put_segment(&mut wire, name.as_bytes());
        put_segment(&mut wire, value.as_bytes());
    }
    wire
}

/// Decodes a `run` request: the command then each argument, prefixed.
pub fn decode_run(payload: &[u8]) -> Result<(String, Vec<String>), KernelError> {
    let mut reader = Reader::new(payload, "process run");
    let command = reader.text()?;
    let mut args = Vec::new();
    while !reader.wire.is_empty() {
        args.push(reader.text()?);
    }
    Ok((command, args))
}

/// A tagged read answer: `data` bytes, `would-block`, or `eof`.
#[must_use]
pub fn encode_read(outcome: Option<Vec<u8>>, eof: bool) -> Vec<u8> {
    match outcome {
        Some(data) => {
            let mut wire = vec![TAG_DATA];
            wire.extend(data);
            wire
        }
        None if eof => vec![TAG_EOF],
        None => vec![TAG_WOULD_BLOCK],
    }
}

/// The 8-byte LE handle answer.
#[must_use]
pub fn encode_handle(handle: u64) -> Vec<u8> {
    handle.to_le_bytes().to_vec()
}

/// Decodes an 8-byte LE handle answer.
pub fn decode_handle(answer: &[u8]) -> Result<u64, KernelError> {
    Reader::new(answer, "handle answer").u64()
}

/// Decodes the 0.1.0 `request` wire: prefixed method, prefixed url, then
/// the body — no header count, because that shape carries no header.
///
/// # Errors
///
/// A malformed payload.
pub fn decode_body_request(payload: &[u8]) -> Result<(String, String, Vec<u8>), KernelError> {
    let mut reader = Reader::new(payload, "net request");
    let method = reader.text()?;
    let url = reader.text()?;
    Ok((method, url, reader.rest().to_vec()))
}

/// Encodes a `send-request` (M2-K14): u32-LE header count, then the method, the
/// url, and each header name and value as prefixed segments, then the body.
#[must_use]
pub fn encode_request(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Vec<u8> {
    let mut wire = u32::try_from(headers.len())
        .unwrap_or(u32::MAX)
        .to_le_bytes()
        .to_vec();
    put_segment(&mut wire, method.as_bytes());
    put_segment(&mut wire, url.as_bytes());
    for (name, value) in headers {
        put_segment(&mut wire, name.as_bytes());
        put_segment(&mut wire, value.as_bytes());
    }
    wire.extend(body);
    wire
}

/// Decodes a `request` (the shape [`encode_request`] writes).
#[allow(clippy::type_complexity)]
pub fn decode_request(
    payload: &[u8],
) -> Result<(String, String, Vec<(String, String)>, Vec<u8>), KernelError> {
    let mut reader = Reader::new(payload, "net request");
    let count = reader.u32()?;
    let method = reader.text()?;
    let url = reader.text()?;
    let headers = (0..count)
        .map(|_| Ok((reader.text()?, reader.text()?)))
        .collect::<Result<Vec<_>, KernelError>>()?;
    Ok((method, url, headers, reader.rest().to_vec()))
}

/// Encodes a response: u32-LE status, u32-LE header count, each header
/// name and value prefixed, then the body.
#[must_use]
pub fn encode_response(status: u16, headers: &[(String, String)], body: &[u8]) -> Vec<u8> {
    let mut wire = u32::from(status).to_le_bytes().to_vec();
    wire.extend(
        u32::try_from(headers.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    for (name, value) in headers {
        put_segment(&mut wire, name.as_bytes());
        put_segment(&mut wire, value.as_bytes());
    }
    wire.extend(body);
    wire
}

/// Decodes a response (the shape [`encode_response`] writes).
#[allow(clippy::type_complexity)]
pub fn decode_response(
    answer: &[u8],
) -> Result<(u16, Vec<(String, String)>, Vec<u8>), KernelError> {
    let mut reader = Reader::new(answer, "net response");
    let status = u16::try_from(reader.u32()?).unwrap_or(u16::MAX);
    let count = reader.u32()?;
    let headers = (0..count)
        .map(|_| Ok((reader.text()?, reader.text()?)))
        .collect::<Result<Vec<_>, KernelError>>()?;
    Ok((status, headers, reader.rest().to_vec()))
}
