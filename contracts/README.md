# Capability Contracts

WIT interface files — the constitution's teeth. Every service a plugin provides or
consumes is declared here; the kernel enforces exactly what is written and the ledger
records every crossing (Laws 1–2).

Format, versioning, equality semantics, and irreversibility declarations are governed
by `docs/constitution/01-capability-contracts.md`.

Rules (R12): contracts are versioned; never breaking within a major version; designed
to outlive any particular kernel implementation or host OS.

Bundles: `jinn-fs` (0.2.0), `jinn-clock` (0.1.0), `jinn-process` (0.1.0),
`jinn-net` (0.1.0, readiness wake M2-K7), `jinn-ledger` (0.1.0, finalized
M2-K7), `jinn-introspect` (0.1.0), `jinn-profile` (0.1.0).

## Operator contracts (M2-K7)

`jinn:introspect` (read-only composition), `jinn:ledger` (paged reads with
consumption receipts), and `jinn:profile` (a patch as operator intent,
applied by the loader, no fiber inverse) are kernel-supplied providers
reached over the string-keyed handle lane (`services.resolve` +
`services.call`), granted like any contract and ledgered per call.

## Lifecycle classification (jinn:plugin@0.3.0, M2-K4)

A contribution belongs to the profile ENTRY, not to a fiber incarnation or a
process lifetime. Every revertible operation in a bundle is one of two classes,
declared in its metadata:

- **World mutation** (`jinn:fs` write/append/remove): retained in the entry's
  durable journal across suspend (daemon shutdown) and incarnation replacement
  (reconcile, config edit, hot-swap); the successor inherits it, revertible by
  key; withdrawn LIFO only when the entry leaves the composition (dispose, I1).
- **Kernel registration** (`jinn:clock` alarms; `jinn:process` children;
  `jinn:net` listeners and connections; provisions, listeners): released on
  suspend with no inverse run, re-established by the next `activate`.

Suspend never runs a world-mutation inverse; dispose never leaves a kernel
registration behind. Both are typed ledger events (Law 2).
