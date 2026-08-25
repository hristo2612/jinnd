# Packet M1-P6b — Loader Gate Release (R1 patch)

**Milestone:** M1 · **Owner:** kernel-dev · **Status:** ready ·
**Binding rules:** R1, R10, R11 · **LOC ceiling:** delta ≤200 across
`crates/jinnd-loader` + `crates/jinnd-adapter` (patch packet). Metric: src LOC
excluding cfg(test), blanks, comments.

## Scope

Fix the scope-locked R1 violation the verifier filed from PLA-266 round 4:
`Loader::reconcile_with` / `update_entry` / `dispose_entry` hold the
`tokio::sync::MutexGuard` from `loader.gate` while plan steps invoke
`PackageLane::spawn` and `EntryHandle::restate` — and the adapter lane calls
caller-supplied package-builder code (`crates/jinnd-adapter/src/wiring.rs:46,55`).
R1: no lock may be held across plugin-facing code, ever.

- Restructure so the gate guard is RELEASED before any lane constructor,
  restater, or other plugin-facing callback runs (stage the plan under the
  gate; execute callbacks outside it; recommit results under the gate — or an
  equivalent single-flight design).
- Concurrent reconcile/update/dispose stay single-flight or otherwise
  race-safe — no interleaved amendments corrupting committed state.
- Preserve ALL semantics proven in P6: three-state honest-failure amend
  outcomes, atomic write-back ordering, reconcile-by-id minimality.

## Acceptance (from PLA-268, verbatim intent)

- No loader lock guard is held when a lane constructor, restater, or other
  plugin-facing callback runs.
- Concurrent reconcile/update/dispose remain single-flight or race-safe; a
  regression test exercises re-entrant / caller-callback behavior (a callback
  that itself calls back into the loader must not deadlock — it may be refused,
  but honestly).
- Loom model for the gate/callback interleaving if the design is
  interleaving-sensitive.
- Gates: fmt, clippy -D warnings, build, test, ratchet (must stay
  cases=122 expected-green=47 expected-red=75), miri on touched crates —
  paste real tails.
- Two-key: zero touches under `tests/invariants/`; LOC within ceiling; files
  under 300 lines.

## Out of scope

Everything else — this is a surgical patch. No facade changes, no new
subsystems, no test-file changes outside the crates' own suites.
