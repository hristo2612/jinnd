# jinn:auth 0.1.0

Who may issue a dispatch on the kernel's inbound surface (M2-K21; the
second of the two M3 blockers named in SOURCE-OF-TRUTH §7). Before this
bundle the kernel authenticated nobody: loopback plus the port a `jinn:net`
grant scopes was the entire boundary, and anything on the machine that
reached the port was the operator.

The kernel has no inbound listener of its own — a transport plugin holds
one through `jinn:net` and the kernel sees only bytes — so the kernel
supplies the **authority** rather than the transport: one credential, one
decision point, every decision ledgered, and a refusal that is its own
typed class.

## Scope, ruled

One operator, one credential named `operator`. No accounts, roles,
tenancy, remote authentication, or delegation between plugins — each is a
different design and none is deferred by oversight.

## Grant

No scope type. A grant is the right to ask; a scoped grant refuses at
admission, fail-closed. `verify` is a read: it mutates nothing and
registers no inverse.

## The credential

One launcher-owned token file beside the data root, `<data>.operator-token`
— a sibling of `.keystore` and `.inverses`, never inside a guest's `jinn:fs`
reach. The daemon only reads it. The launcher creates it (mode `0600`) and
hands the same bytes to its clients.

It is read **on every call**, so rotation and revocation need no restart:
overwrite it and the new value grants from the next call on while the old
refuses; delete it and everything refuses from the next call on.

Deny by default. Each of these is a stated precondition and each answers
`unauthenticated`: the file is absent or unreadable; it is group- or
world-accessible; it exceeds 4096 bytes; its value after trimming ASCII
whitespace is shorter than 16 bytes; the presented value does not match.
There is no default credential and no path that grants when the file is
missing. Comparison is constant-time over SHA-256 digests.

## The refusal is its own class

| answer | contract | means |
|---|---|---|
| `unauthenticated(reason)` | `jinn:auth` | the presented credential did not prove the operator; present it, or stop |
| `denied(reason)` | `jinn:net` | off the allowlist — the caller's profile to widen |
| `failed(reason)` | `jinn:net` | the transport — worth retrying |
| `refused(reason)` | `jinn:profile` | the patch — fix the patch |

Four next moves, four cases. The reason on a refusal names the
precondition that failed, for the operator's log; it never carries
credential bytes.

## Ledger (Law 2, redacted)

Every call lands the broker's `ContractCall` line and one
`AuthDecided { name, presented, granted }` row under the calling entry —
the credential **name** on a grant, and the SHA-256 **digest** of what was
presented either way. Never the presented bytes, never the credential (the
M2-K8 keystore and M2-K14 outbound precedents).

## What a transport owes, and what the kernel does not do

A plugin serving an inbound connection issues **no dispatch on that
connection's behalf** before `verify` answers `principal` for a credential
the connection presented. The kernel cannot see the transport's protocol
and does not gate the transport plugin's own granted calls behind this one
— that would be delegation between plugins, which this contract does not
do. So the check is the transport's obligation and the composition suite's
to prove. The kernel's promise is narrower and exact: one decision point,
deny unless proven, every decision on the record.

The daemon's own stdin (`revert` / `status` lines) is the launcher's — a
parent-process relationship, not a socket — and is unchanged here.

## Threat model, with its limit

In model: a process on this machine that is not the launcher reaching the
transport's socket, by accident, misconfiguration, or as a mistaken second
instance; and a future transport added without its author noticing there
was never a check.

**Not in model: a malicious process running as the daemon's own uid.** It
can read whatever the daemon can read, the credential file included, and
no check here holds against it. That limit is written in the contract, the
metadata, and the provider's code comment, because a guarantee that cannot
hold against same-uid is not a guarantee.

## No bypass

No environment variable, no profile field, no build flag, no test seam. A
test in the daemon crate scans the provider's source for each of those; a
test that needs a credential writes the file, which is configuration.
