# Capability Contracts

WIT interface files — the constitution's teeth. Every service a plugin provides or
consumes is declared here; the kernel enforces exactly what is written and the ledger
records every crossing (Laws 1–2).

Format, versioning, equality semantics, and irreversibility declarations are governed
by `docs/constitution/01-capability-contracts.md`.

Rules (R12): contracts are versioned; never breaking within a major version; designed
to outlive any particular kernel implementation or host OS.

Bundles, as the parser reads them: the table between the two marker lines is
RENDERED by `jinnd-contract-lens` from every bundle's `contract.wit` (identity)
and `metadata.toml` (scope type), and the lens gate refuses a stale, missing,
or hand-edited copy (M2-K22). Each bundle's own header carries its changelog;
nothing in the table is typed by hand.

<!-- contract-index: begin (rendered by jinnd-contract-lens; never edit by hand) -->
| bundle | contract of record | scope type |
|---|---|---|
| `contracts/jinn-auth` | `jinn:auth@0.1.0` | none |
| `contracts/jinn-clock` | `jinn:clock@0.1.0` | rate |
| `contracts/jinn-fs` | `jinn:fs@0.2.0` | path-prefix |
| `contracts/jinn-introspect` | `jinn:introspect@0.6.0` | none |
| `contracts/jinn-keystore` | `jinn:keystore@0.1.0` | key-prefix |
| `contracts/jinn-ledger` | `jinn:ledger@0.1.0` | none |
| `contracts/jinn-net` | `jinn:net@0.3.0` | net-policy |
| `contracts/jinn-process` | `jinn:process@0.1.0` | process-policy |
| `contracts/jinn-profile` | `jinn:profile@0.3.0` | entry-ids |
| `contracts/jinn-profile-admin` | `jinn:profile-admin@0.1.0` | entry-ids |
<!-- contract-index: end -->

## Operation-class attenuation (M2-K8)

Beside its scope, a grant entry may name the operation class it admits:
`{ contract, scope, ops = ["read", "list", "meta"] }`. The names are the
bundle's declared `[operations.*]`; the kernel refuses every other operation
at its single dispatch point as a ledgered scope refusal, on every lane
(host-provider imports and the `services.call` handle lane alike). Absent
means every operation; the predicate is `a.ops ⊆ b.ops`. Admission is
fail-closed: an unknown name, an empty list, or a non-list refuses the grant
on the record. Several grants of one contract on one entry widen by union,
exactly as path scopes accumulate.

## Operator contracts (M2-K7)

`jinn:introspect` (read-only composition), `jinn:ledger` (paged reads with
consumption receipts), `jinn:profile` (a patch as operator intent,
applied by the loader, no fiber inverse) and `jinn:profile-admin` (M2-K23:
the composition's shape — add, remove, `disabled`, grants, plugin
identity — as operator intent applied by reconcile-by-id, one
`ProfileAdministered` row per write naming the caller and both document
digests) are kernel-supplied providers
reached over the string-keyed handle lane (`services.resolve` +
`services.call`), granted like any contract and ledgered per call.

## Lifecycle classification (world 0.3.0, M2-K4)

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
