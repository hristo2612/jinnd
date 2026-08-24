# 02 — The Ledger

**Status: RATIFIED v0.1 — 2026-08-24.** Serves Law 2:
model-visible means logged.

## The invariant

**Anything an agent can see or do passes through a capability contract, and every
contract crossing lands in the ledger.** There is no off-ledger channel. If data
reached a model or an action reached the world, the ledger says so — or the kernel has
a bug of the highest severity class.

## What is recorded

One append-only stream of events, each with: monotonic id, wall + monotonic time,
**actor** (fiber id + plugin identity + the grant used), **verb** (contract + function
or lifecycle transition), **payload** (per sensitivity class, see Redaction), and
**causal parent** — the event that triggered this one. Root events (operator input,
timer fire, external arrival) use `causal_parent = null` plus an explicit root-cause
kind. Revertible effects carry a reference to their registered inverse descriptor (03).

Recorded event families:

1. **Contract calls** — every crossing, both directions (call + result/error).
2. **Consumption receipts** — every delivery that crosses into a model-visible
   context (event subscription, ledger read, stream chunk) appends a receipt
   referencing the exact delivered event ids and payload hashes. Delivery receipts
   are excluded from the originating subscription's feed — recursion is prevented
   without creating an off-ledger channel.
3. **Lifecycle** — fiber state transitions, epoch changes, loads/unloads, failures.
4. **Composition** — profile edits, reconciles, write-backs, grants and revocations,
   plugin installs/updates with their signature envelopes (05).
5. **Reverts** — intent, per-inverse completion, and outcome events (03); reverting
   never erases history.

## Properties

- **Physically append-only in v0.1.** Nothing is updated or deleted in place, ever.
  Summaries may be appended as *derived indexes*; original events are retained
  unchanged. Destructive compaction does not exist in v0.1 — introducing it requires
  a constitutional amendment defining exactly what remains derivable and revertible.
  Storage: SQLite, single writer (the kernel), WAL mode.
- **Causally chained.** "Why did this happen" is a walk, not a hunt.
- **The ledger is the state.** Durable system state (composition, grants, installed
  plugins) is derivable from the ledger; process memory is a cache. This is what makes
  Tier-2 restart (R8) safe.
- **Device-local in v0.1.** Each device owns its ledger; there is no cross-device
  merge. Sync/replication semantics are a v0.2+ amendment.
- **Readable.** The ledger is exposed as a contract (`jinn:ledger@1`, read +
  subscribe) — timeline UI, debugging, and agent self-knowledge are ordinary
  consumers. Model-visible ledger reads produce consumption receipts per family 2.

## Redaction

Payloads are stored per the contract's sensitivity class: `public` verbatim;
`personal` verbatim locally, redacted in any export; `secret` (key material, tokens)
**never stored** — the event records that a secret crossed, its name and hash, never
its value. (Inverse descriptors needing secret pre-state hold opaque keystore
references, never values — 03.)

## Open questions for v0.2

- Cross-device ledger sync (deferred cleanly: v0.1 is device-local, no merging).
- Derived-index formats for fast timeline queries.
