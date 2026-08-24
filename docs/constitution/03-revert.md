# 03 — Revert Semantics

**Status: DRAFT v0.1-rc2** (verifier round 1 blocker B4 applied). Serves Law 3: every
effect revertible, or explicitly declared irreversible.

## The protocol (not an aspiration — an executable, crash-safe sequence)

All mutation flows through the kernel's effect primitive (R5). A revertible operation
proceeds in this order, and no other:

1. **Register:** the operation supplies a **serializable inverse descriptor** —
   captured delta, an **idempotency key**, and an observational witness (what
   equality relation "restored" is judged under, per the contract) — and the kernel
   **durably records intent** (ledger event) *before* the forward mutation may commit.
2. **Act:** the forward operation runs.
3. **Complete:** the kernel records completion (ledger event).

Consequences:

- **Crash safety:** recovery replays from the ledger. An intent without a completion
  is resumed *under the same idempotency key*; the kernel never issues an unkeyed
  retry, so a non-idempotent action can never silently duplicate.
- **Version stability:** the inverse descriptor is self-contained and serializable —
  it remains executable after its originating plugin is unloaded, upgraded, or gone.
  A contract major-version bump must state what happens to outstanding descriptors
  from the previous major (R12).
- **Secrets:** inverse material that needs secret pre-state holds an opaque,
  versioned **keystore reference**, never a value in the ledger (02 §Redaction).

## What revert guarantees

- **Units of revert (v0.1):** an effect, a fiber, a subtree, or **a causal-descendant
  set** (an event and everything it caused). Numeric or time-range ledger revert does
  not exist in v0.1 and is rejected; it returns as a v0.2+ amendment with defined
  independence/conflict semantics.
- **Order:** within a fiber, inverses run LIFO. Across fibers, the kernel orders
  reverts along the dependency graph — consumers drain before providers (I2).
- **Exactness (I1):** reverting a unit withdraws exactly that unit's contribution,
  judged under the contract's declared observational equivalence — never
  bit-identical.
- **Reverts are events, not erasures.** History says "X happened, then X was
  reverted." The ledger never rewrites.
- A unit containing an `irreversible` event **is rejected as a revert**. Compensation
  is a distinct, explicitly confirmed operation (below), never silently substituted.

## Irreversible effects

1. They MUST be declared `irreversible` in the contract (01). An undeclared
   irreversible effect discovered in review is a Law 3 violation, not a bug.
2. The kernel routes them through a **confirmation flow**: policy decides per grant
   whether the call proceeds silently, requires operator confirmation, or is denied.
   Defaults are conservative; profiles may loosen per contract.
3. Where a **compensator** exists (send a correction, refund a charge), the contract
   may declare it. Compensation is its own confirmed operation; the ledger links it
   to the original; the contract must state the (coarser) equivalence it restores.

## Failure during revert

If an inverse fails:

- The kernel records the failure, and **may retry only under the same idempotency
  key** (safe by construction). There is no blind unkeyed retry.
- The affected dependency branch **remains `Unloading`**: it advertises no new
  availability, provider inverses below it do not run until its consumers drain, and
  the kernel never marks the branch disposed or the revert complete while an inverse
  is unresolved. Independent branches continue normally (R11).
- **Resolution state machine (normative in v0.1):** an unresolved branch is
  `pending-revert`. The operator (or policy) resolves it by exactly one of:
  (a) retry with the same key after fixing the environment; (b) invoke a declared
  compensator; (c) **accept-residue** — an explicit, ledgered declaration that the
  remaining delta is accepted as-is, which closes the revert as `unclean-accepted`.
  The kernel never auto-selects (c). UX for this can evolve; the states cannot.

A plugin failing mid-*load* has its partial effects unwound automatically by the same
protocol (I1 covers failure).

## Open questions for v0.2

- Time-range revert with independence checking (paper §3.1.3) — excluded from v0.1.
- Descriptor migration tooling across contract majors.
