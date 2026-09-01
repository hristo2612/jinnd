# jinn:net 0.2.0

The base host-provider network contract (M2-K6; R7): the server edition an
operator API needs — `listen` / `accept` / `read` / `write` / `close` on
loopback — plus the outbound one-shot, provided from 0.2.0 (M2-K14) for
plain HTTP to a loopback target.

0.1.0 -> 0.2.0 is STRICTLY ADDITIVE (R12). `request(method, url, body)`
keeps its 0.1.0 declaration byte for byte and starts working;
`send-request(outbound-request) -> outbound-response` arrives beside it
with the shape a real call needs. One door, two handles: same authority,
same ledger row, same effect class.

## Grant

Scope type `net-policy`: `{ "bind": [low, high], "outbound": ["host", ...]
}`, both optional. Admission is fail-closed — any other shape or type
refuses the grant on the record; a bare grant holds the empty policy
(nothing may be bound, nothing reached). `listen` refuses a non-loopback
address and a port outside the granted ranges, each a ledgered grant refusal.

Several grants of this contract to one entry compose as the EXACT SET of
their ranges, never the numeric hull: granted `[1000,1000]` and
`[2000,2000]`, an entry binds those two ports and is refused port 1500,
which no grant conferred. The set is normalized (sorted, overlapping and
adjacent ranges coalesced), so grant order never changes what an entry
holds.

## Lifecycle

Listeners and connections are kernel registrations: suspend and dispose
close them, ledgered; the next `activate` re-listens. A handle is valid only
for the peer that minted it (R4).

## Non-blocking shape (v0.1)

`accept` and `read` answer `would-block`; `write` answers the byte count the
socket accepted.

## Readiness wake (M2-K7)

The kernel delivers `lifecycle.handle-event(handle, "jinn:net/readable",
<8-byte LE handle>)` — the token is the socket handle — when a listener
has a pending connection or a connection has bytes or EOF. One wake per
readiness transition the guest has not yet acted on: a flood of bytes is
one wake until the guest reads, EOF wakes once, `accept`/`read` consume
then re-arm (a level probe: what is still pending wakes again exactly
once; what was just consumed never re-announces).
A server holds no alarm. A guest that ignores wakes and polls (from a
`jinn:clock` alarm) still works.

## Outbound `request` / `send-request` (0.2.0, M2-K14)

Authority is the grant's `outbound` allowlist. An allowlist entry and a
request URL are both normalized to `host:port` (lowercased host, port 80
supplied for `http` when omitted) and the entry admits the URL only when
the two normal forms are EQUAL — no wildcards, no suffix match, no "the
host alone means every port". A bare grant reaches nothing.

Three refusals, three distinct answers, so a caller never has to guess
which one it got:

| answer | means |
|---|---|
| `denied(reason)` | off the allowlist, or a target that is not loopback — including the authority a `30x` redirects to |
| `invalid(reason)` | a URL this provider cannot make sense of: not `http://`, no host, userinfo in the authority, a bad port |
| `failed(reason)` | the call was authorized and the network, the bound, or the response failed |

Stated limits of 0.2.0, each with its reason:

- **`http://` only.** TLS is M2-K15 and deliberately not decided here.
- **Loopback only.** The target must be a `127.0.0.0/8` literal, `::1`, or
  `localhost`. No resolver is consulted: name resolution is ambient
  authority, and a name that resolves off-loopback is exactly the hole the
  allowlist exists to close.
- **A `30x` is never followed, and an off-allowlist one is `denied` inside
  the call.** The `location` crosses the same admission the initial target
  crossed; one this provider cannot parse cannot be proven admitted and is
  refused too. A hop to an authority the caller may already reach is
  answered, still unfollowed. The `NetRequested` row lands either way. Full
  reasoning beside `send-request` in `contract.wit`.
- **`transfer-encoding: chunked` is not decoded** — a typed `failed`. The
  provider sends `connection: close` and reads `content-length`, or to EOF.
- **Bounded (R9).** 3s for the whole call — under the 5s guest-call
  deadline, so a slow target answers the guest rather than killing its
  activation — and a 1 MiB response body cap, past which the answer is a
  typed `failed`, never a silent truncation.

A plugin cannot widen its own allowlist: `jinn:net` has no operation that
writes a grant, and `jinn:profile` refuses an entry that patches itself.
Known limit, stated: an entry granted `jinn:profile` over ANOTHER entry can
widen that entry's allowlist — granting profile-edit over an entry is
granting its authority.

## Irreversibility (Law 3)

Both outbound operations are declared `effect = "irreversible"`: a sent
request cannot be un-sent, so there is no inverse and no compensator, and
a revert unit containing one is REJECTED WHOLE with a typed refusal naming
WHICH call it could not revert (03 §51).

The guarantee is DURABLE, not merely live. The `NetRequested` row carries
the effect id, so the ledger IS the register of outbound calls and there
is no second, in-memory one (R5). A daemon reopened over the same ledger
still names an earlier process's request as irreversible, and seeds its
effect counter past the durable high-water mark at boot so an id never
names two calls.

## Ledger

`NetListening { handle, port }`, `NetAccepted { listener, handle }`,
`NetClosed { handle }`, `NetReadable { handle }` (one per delivered wake),
and `NetRequested { effect, method, host, path, status, request_bytes,
response_bytes, duration_ms }` — with the calling entry's attribution.
Bytes are data plane and are not ledgered.

A request's record is its SHAPE, never its content: no body, no header (an
`Authorization` header carries exactly the credential the keystore exists
to protect), and no query string (`?access_token=` carries one just as
readily) — the path is recorded up to `?`. A URL carrying userinfo is
refused as `invalid` rather than recorded with its credential stripped:
the kernel does not quietly accept a call whose authority it had to edit.
