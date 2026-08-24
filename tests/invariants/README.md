# Invariant Suite (verifier-owned)

This directory is the kernel's definition of correct. It will hold:

1. **I1–I4 invariant tests** (`invariant_recovery.rs`, `invariant_ordering.rs`,
   `invariant_progress.rs`, `invariant_confluence.rs`) — the four theorems from the
   spatiotemporal-composability paper as executable acceptance criteria
   (SOURCE-OF-TRUTH §4).
2. **The ported Cordis v4 spec suite** — behavioral parity tests translated from
   `cordis/packages/*/tests/*.spec.ts` (reference checkout:
   `the private reference annex (cordis)`). The inertia-lock trio from
   `fiber.spec.ts` is the crown: it pins mid-flight dependency-swap coalescing,
   which is exactly what naive ports lose (see the cordis-rs audit).

## Rules

- **Two-key rule (R2):** implementation agents MUST NOT modify anything here. Test
  changes and implementation changes never travel in the same PR. CI enforces both.
- Tests land **red** (M1 packet 0) before any kernel code exists. Red here is not a
  failure state; it is the backlog.
- A test may be changed only with verifier sign-off (`verifier-approved` label) and
  a rationale referencing the TS original or the paper.

Status: empty — M1 packet 0 delivers the suite.
