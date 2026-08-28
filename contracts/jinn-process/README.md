# jinn:process 0.1.0

The base host-provider process contract (M2-K6; R7). `run` is the one-shot
convenience; `spawn` / `write-stdin` / `close-stdin` / `read` / `wait` /
`kill` are the long-lived edition an engine needs to drive a CLI with
streams.

## Grant

Scope type `process-policy`: `{ "exec": ["<absolute prefix>", ...], "env":
"inherit-none" | ["NAME", ...] }`. Admission is fail-closed — a scope of any
other shape or type refuses the grant on the record; a bare grant holds the
empty policy (nothing may be executed, nothing is inherited). Each call
authorizes the FULLY RESOLVED executable (post-symlink) against the
allowlist; `command` must be absolute. The child's environment is exactly
the guest's explicit `env` pairs plus, under an allowlist policy, the named
daemon variables — never the daemon's whole environment.

## Lifecycle

A spawned child is a kernel registration: suspend (shutdown, reconcile,
hot-swap) and dispose both kill it (SIGKILL) and reap it, ledgered; the next
`activate` re-spawns. Dispose leaves no zombie.

## Wake shape (v0.1)

Every host call is non-blocking or bounded (R1): `read` answers
`would-block`, `write-stdin` answers the accepted count, `wait` is capped at
1000ms. The guest polls — the honest minimal shape is a `jinn:clock`
periodic alarm whose handler drains the streams. A readiness event is a
future edition, added when a consumer needs it.

## Ledger

`ProcessSpawned { handle, command, pid }`, `ProcessExited { handle, code }`
(a signal termination is the negated signal number), `ProcessKilled {
handle, signal }` — with the calling entry's attribution. A kernel kill is
never half a story: `run` past its bound shows `ProcessKilled` then
`ProcessExited`; a child not reaped inside the guest deadline is
`ProcessReapPending { handle }` until the host task lands its exit. `run`
is bounded in output too: stdout flows through the same bounded ring as
the long-lived edition into a total capped by the metadata's
`output-cap-bytes`; past it `ProcessOutputTruncated { handle, cap }` is
recorded and the call answers `output-truncated` — the read end is cut
(EPIPE for a descendant holding the pipe), the child killed and reaped.
Stream bytes are data plane and are not ledgered.
