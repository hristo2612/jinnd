# 02 — The Ledger

**Status: DRAFT v0.1.** Serves Law 2: model-visible means logged.

## The invariant

**Anything an agent can see or do passes through a capability contract, and every
contract crossing lands in the ledger.** There is no off-ledger channel. If data
reached a model or an action reached the world, the ledger says so — or the kernel has
a bug of the highest severity class.

## What is recorded

One append-only stream of events, each with: monotonic id, wall + monotonic time,
**actor** (fiber id + plugin identity + the grant used), **verb** (contract + function
or lifecycle transition), **payload** (per sensitivity class, see Redaction),
**causal parent** (the event that triggered this one), and for revertible effects a
reference to the registered inverse (03).

Recorded event families:

1. **Contract calls** — every crossing, both directions (call + result/error).
2. **Lifecycle** — fiber state transitions, epoch changes, loads/unloads, failures.
3. **Composition** — profile edits, reconciles, write-backs, grants and revocations,
   plugin installs/updates with their signatures (05).
4. **Reverts** — every revert is itself a ledger event chain (03); reverting never
   erases history.

## Properties

- **Append-only.** Nothing is ever updated or deleted in place. Storage: SQLite,
  single writer (the kernel), WAL mode. Retention/compaction may *summarize* old
  events but a summarization is itself an event and the pre-image hash is kept.
- **Causally chained.** Every event names its parent; "why did this happen" is a walk,
  not a hunt.
- **The ledger is the state.** Durable system state (composition, grants, installed
  plugins) is derivable from the ledger; process memory is a cache. This is what makes
  Tier-2 restart (R8) safe.
- **Readable.** The ledger is itself exposed as a contract (`jinn:ledger@1`, read +
  subscribe) — the UI's timeline, debugging, and the agent's self-knowledge are
  ordinary consumers. Ledger reads are logged like any other read (yes, recursively:
  reads of the ledger appear in the ledger; subscription delivery does not re-log).

## Redaction

Payloads are stored per the contract's sensitivity class: `public` verbatim;
`personal` verbatim locally, redacted in any export; `secret` (key material, tokens)
**never stored** — the event records that a secret crossed, its name and hash, never
its value.

## Open questions for v0.2

- Cross-device ledger sync semantics (small brain / big brain): single-writer per
  device with merge, or one elected writer?
- Compaction policy defaults (size- vs age-based) and what the summarization event
  must preserve.
