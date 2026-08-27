# Packet M1-P9b — Dispose trail is LIFO through the daemon path (R5 patch)

**Milestone:** M1 · **Owner:** kernel-dev · **Status:** ready ·
**Binding rules:** R5, R6, R1, R11 (+ Law 2/3, LAW §3 LIFO) · **LOC ceiling
(card-authoritative):** delta ≤150 across `crates/jinnd-daemon` (+
`crates/jinnd-wasm` if the seat machinery lives there). Surgical patch; no
facade changes.

## Defect (from the delegated M1 acceptance drive, PLA-289 → PLA-253 evidence)

Runbook step 4: a plugin's effects registered at ledger seq 10/11 withdrew in
the SAME order at seq 52/53. The paradigm's teardown is LIFO replay (LAW §3;
the effects engine implements and tests it). The daemon-path dispose trail
must therefore show withdrawals in strictly REVERSE registration order within
each fiber's contribution — the observed forward order is wrong either in
execution (the retire path runs its own forward loop — an R5 violation: a
second mutation path beside the effect primitive) or in recording (execution
is LIFO but ledger appends are emitted forward — a Law 2 fidelity defect).
Diagnose which, then fix at the root:

- Teardown/withdrawal of a seat's contribution MUST route through the effects
  engine's LIFO replay — no parallel iteration of seat-held lists that
  executes or records withdrawals independently (R5: one mutation primitive).
- The ledger withdrawal events are appended at the moment each undo actually
  runs, in that order (Law 2: the ledger records what happened, in the order
  it happened).

## Acceptance

- A crate-owned test drives a multi-effect plugin through daemon-path dispose
  and asserts the ledger withdrawal sequence is strictly reverse of the
  registration sequence within that fiber's trail.
- The headless demo test's dispose step gains the same ordering assertion.
- No seat/retire code path executes a withdrawal outside the effects engine's
  replay (verifier audits for parallel loops).
- Existing greens hold: ratchet unchanged (95/35), all gates + loom lanes
  green, real tails pasted. Two-key: zero touches under tests/invariants/.
- After land: the delegated operator demo re-drive passes ALL five runbook
  steps end-to-end (tracked on PLA-289; the re-drive is the packet's real
  exit).

## Out of scope

Everything else. No runbook rewording to match wrong behavior — the behavior
conforms to the LAW, not the other way around.
