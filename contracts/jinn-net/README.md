# jinn:net 0.1.0

The base host-provider network contract (M2-K6; R7): the server edition an
operator API needs — `listen` / `accept` / `read` / `write` / `close` on
loopback — plus the declared outbound one-shot `request`.

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

## Not provided in v0.1

`request` (outbound HTTP): the kernel carries no HTTP client (R10). The
provider answers a typed `failed`; the outbound host allowlist is
admitted and carried in the grant for the edition that consumes it. TLS,
non-loopback listening, and UDP are out of scope.

## Ledger

`NetListening { handle, port }`, `NetAccepted { listener, handle }`,
`NetClosed { handle }`, `NetReadable { handle }` (one per delivered wake)
— with the calling entry's attribution. Bytes are data plane and are not
ledgered.
