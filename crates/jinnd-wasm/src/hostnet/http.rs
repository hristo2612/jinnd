//! The plain-HTTP/1.1 shapes the outbound one-shot is made of (M2-K14):
//! reading a URL, building a request head a caller cannot smuggle through,
//! and reading a response head. Pure functions over bytes — no sockets, no
//! authority, no ledger: deciding what a call IS and deciding whether it
//! MAY happen are two jobs (R10 file hygiene).

use jinnd_api::{ErrorCode, KernelError};

use crate::broker_state::refusal;
use crate::grants::normalize_authority;

/// The three headers the kernel owns: they frame the request, and a
/// caller's own copy would either fight ours or silently change what the
/// response parser may assume.
const FRAMING: [&str; 3] = ["host", "connection", "content-length"];

/// A URL this provider cannot make sense of — the `invalid` reading, never
/// blurred with a grant refusal or a network failure.
pub(super) fn invalid(detail: String) -> KernelError {
    refusal(ErrorCode::InvalidProfile, format!("net request: {detail}"))
}

/// One outbound target as the provider reads it off a URL.
pub(super) struct Target {
    /// The normalized `host:port` authority the allowlist is matched on.
    pub(super) authority: String,
    pub(super) host: String,
    pub(super) port: u16,
    /// The origin-form target written on the request line — path AND
    /// query, because the query is part of the call.
    pub(super) target: String,
    /// The path alone, up to `?`: what the ledger may carry (02
    /// §Redaction — a query string routinely carries a credential).
    pub(super) path: String,
}

/// Reads one absolute `http://` URL.
///
/// # Errors
///
/// [`invalid`] for anything this provider cannot make sense of: another
/// scheme (TLS is M2-K15), no authority, userinfo in the authority, or a
/// port that is not a u16.
pub(super) fn parse(url: &str) -> Result<Target, KernelError> {
    let lower = url.to_lowercase();
    if lower.starts_with("https://") {
        return Err(invalid(
            "https is not provided in v0.2 — no TLS stack in the kernel (M2-K15)".to_owned(),
        ));
    }
    let rest = lower
        .starts_with("http://")
        .then(|| &url[7..])
        .ok_or_else(|| invalid(format!("{url:?} is not an absolute http:// url")))?;
    let split = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(split);
    let (authority, host, port) = normalize_authority(authority)
        .ok_or_else(|| invalid(format!("{url:?} has no readable host:port authority")))?;
    let target = match tail.split_once('#') {
        Some((before, _)) => before,
        None => tail,
    };
    let target = if target.is_empty() { "/" } else { target };
    let path = target.split_once('?').map_or(target, |(path, _)| path);
    Ok(Target {
        authority,
        host,
        port,
        target: target.to_owned(),
        path: path.to_owned(),
    })
}

/// Builds the request head. The kernel writes the framing headers; a
/// caller's header may not carry CR or LF (a second request line is not a
/// header value) and may not be one of the framing three.
///
/// # Errors
///
/// [`invalid`] naming the offending header.
pub(super) fn head(
    method: &str,
    target: &Target,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<Vec<u8>, KernelError> {
    if method.is_empty() || method.contains(|byte: char| !byte.is_ascii_graphic()) {
        return Err(invalid(format!("{method:?} is not a method token")));
    }
    let mut head = format!("{method} {} HTTP/1.1\r\n", target.target);
    head.push_str(&format!("host: {}\r\n", target.authority));
    head.push_str("connection: close\r\n");
    head.push_str(&format!("content-length: {}\r\n", body.len()));
    for (name, value) in headers {
        let lowered = name.to_lowercase();
        if name.is_empty() || name.contains([':', '\r', '\n']) || value.contains(['\r', '\n']) {
            return Err(invalid(format!("header {name:?} is not one header line")));
        }
        if FRAMING.contains(&lowered.as_str()) {
            return Err(invalid(format!(
                "header {lowered:?} frames the request and is the kernel's to write"
            )));
        }
        head.push_str(&format!("{lowered}: {value}\r\n"));
    }
    head.push_str("\r\n");
    Ok(head.into_bytes())
}

/// One response head: the status and the headers, lowercased.
pub(super) struct Head {
    pub(super) status: u16,
    pub(super) headers: Vec<(String, String)>,
}

impl Head {
    fn value(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header == name)
            .map(|(_, value)| value.as_str())
    }

    /// How many body bytes to read: `Some(n)` from `content-length`, `None`
    /// to read to EOF (the provider sent `connection: close`).
    ///
    /// # Errors
    ///
    /// A typed failure for chunked framing, which v0.2 does not decode, and
    /// for a `content-length` that is not a number.
    pub(super) fn expected(&self) -> Result<Option<usize>, KernelError> {
        if self
            .value("transfer-encoding")
            .is_some_and(|coding| coding.contains("chunked"))
        {
            return Err(failed(
                "the target answered chunked framing, which v0.2 does not decode".to_owned(),
            ));
        }
        match self.value("content-length") {
            None => Ok(None),
            Some(length) => length
                .trim()
                .parse()
                .map(Some)
                .map_err(|_| failed(format!("content-length {length:?} is not a count"))),
        }
    }
}

/// An authorized call the network or the response failed — the third
/// reading, never confused with a refusal.
pub(super) fn failed(detail: String) -> KernelError {
    refusal(ErrorCode::PluginFailed, format!("net request: {detail}"))
}

/// Reads a response head off `bytes` (everything up to and including the
/// blank line), answering it and the body bytes already in hand.
///
/// # Errors
///
/// A typed failure for a head this provider cannot read.
pub(super) fn response(bytes: &[u8]) -> Result<(Head, Vec<u8>), KernelError> {
    let end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| failed("the target answered no complete response head".to_owned()))?;
    let text = std::str::from_utf8(&bytes[..end])
        .map_err(|_| failed("the response head is not UTF-8".to_owned()))?;
    let mut lines = text.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| failed("the response has no status line".to_owned()))?;
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_lowercase(), value.trim().to_owned()))
        .collect();
    Ok((Head { status, headers }, bytes[end + 4..].to_vec()))
}
