# 03 — Revert Semantics

**Status: DRAFT v0.1.** Serves Law 3: every effect revertible, or explicitly declared
irreversible.

## The primitive

All mutation flows through the kernel's effect primitive (R5): an action registers its
inverse **at the moment it acts**. The kernel owns the collected inverses; the acting
plugin cannot lose, skip, or reorder them.

## What revert guarantees

- **Unit of revert:** an effect, a fiber (all its effects), a subtree (a fiber and all
  its descendants), or a ledger range (everything caused by an event and its causal
  descendants).
- **Order:** within a fiber, inverses run LIFO. Across fibers, the kernel orders
  reverts along the dependency graph (consumers before providers — invariant I2).
- **Exactness (I1):** reverting a unit withdraws exactly that unit's contribution.
  Sibling state is untouched; the paper's recovery-exactness theorem is the contract.
- **"Restored" means observationally equal** under the contract's declared equality
  semantics (01) — indistinguishable by the contract's own operations, not
  bit-identical.
- **Reverts are events, not erasures.** History says "X happened, then X was
  reverted." The ledger never rewrites.

## Irreversible effects

Some actions cannot be undone (send a message, charge a card, delete-with-no-trash).
The constitution's stance:

1. They MUST be declared `irreversible` in the contract (01). An undeclared
   irreversible effect discovered in review is a Law 3 violation, not a bug.
2. The kernel routes them through a **confirmation flow**: policy decides per grant
   whether an irreversible call proceeds silently, requires operator confirmation, or
   is denied. Defaults are conservative; profiles can loosen per contract.
3. Where a true inverse is impossible but a **compensation** exists (send a
   correction, refund a charge), the contract may declare a compensator; the ledger
   links compensation to the original. Compensation is honest about being weaker than
   revert: it restores up to a coarser equivalence, and the contract must say which.

## Failure and revert

A plugin failing mid-load has its partial effects unwound automatically (I1 covers
failure). A failing *inverse* is a serious event: logged, retried once, then the fiber
is quarantined (marked unclean) and surfaced to the operator — the kernel never
pretends an unclean revert was clean.

## Open questions for v0.2

- Time-range revert across interleaved unrelated work: the independence discipline
  (paper §3.1.3) — surface conflicts to the operator, or refuse non-independent
  ranges outright?
- Quarantine UX: what the operator sees and what unblocks an unclean fiber.
