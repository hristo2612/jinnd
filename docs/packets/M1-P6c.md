# Packet M1-P6c — Registry/loader conformance (dual-audit fixes)

**Milestone:** M1 · **Owner:** kernel-dev · **Status:** ready ·
**Binding rules:** R1, R3, R5, R9, R10, R11 (+ I2/I3 conformance) · **LOC ceiling:**
delta ≤500 across `crates/jinnd-registry` + `crates/jinnd-fiber` +
`crates/jinnd-loader` + `crates/jinnd-adapter` (ceiling, not a floor). Metric: src
LOC excluding cfg(test), blanks, comments.

Source findings: 2026-08-25 decision-log entry (dual audits). Fix packet — every
item below is a demonstrated divergence from the paper, the reference
implementation, or R1, with file:line evidence on PLA-269 / PLA-270 / PLA-271.

## Scope

1. **I2 drain ordering (paper Alg 5).** The dependent-drain wait currently sits
   INSIDE the provision effect's undo (`jinnd-registry/src/registry.rs:95-107`), so
   provider effects registered after provision are withdrawn while dependents can
   still call the dying service. Hoist the drain ahead of the provider's ENTIRE
   withdrawal replay (supervisor-level: slot removal + lease close + drain complete
   before `scope.replay()` runs any inverse). No inverse of the provider runs until
   every dependent is gone.
2. **Duplicate provision refused (paper Def 23; R9).** `SlotMap::insert`
   (`jinnd-registry/src/slots.rs:73-93`) silently replaces a live binding. Refuse a
   provision for an occupied (key, realm) unless it supersedes the SAME provider's
   generation (hot-swap path unchanged). Honest recorded error; the second
   provider's activation fails cleanly (R11).
3. **Self-amend from activation refused.** A plugin amending its own entry from its
   activation body self-deadlocks (`jinnd-loader/src/amend.rs:81,104` awaits the
   calling task's own fiber settling). Extend the P6b conflict-point refusal:
   refuse any amendment that would wait on the calling task's own fiber, same
   honest retryable error, no caller analysis beyond fiber identity.
4. **PLA-270 R1 seams.** (a) `Loader::rebind_step` holds the `state` MutexGuard
   across `EntryHandle::rebind`/`restart` (`apply.rs:139-151`) — release before
   handle calls. (b) The persist permit spans `Loader::persist`, whose `encode`
   closure is caller-supplied (`amend.rs:113-116`, `store.rs:118-148`) — encode
   outside the permit span, or make encoding kernel-owned. No guard/permit held
   across ANY caller-supplied code.
5. **Verbatim preservation through the save path (v0.1 bounds).** Raw undecodable
   entries and unknown fields inside decodable entries must survive a runtime
   write-back: the committed document (not the typed profile) is the persistence
   unit — kernel owns the raw-merge (`Document::from_profile` hard-codes
   `raw: Vec::new()`, `document.rs:154-177`; encoder caller-supplied,
   `store.rs:118-135`); unknown entry fields captured via a serde flatten
   catch-all (`document.rs:19-35`) and re-emitted unchanged. No silent erasure,
   ever.
6. **Static dependency-cycle detection (I3).** `ErrorCode::DependencyCycle` is
   constructed nowhere. Detect cycles over lane provides/injects declarations at
   loader plan time; involved entries land cleanly inactive with the recorded
   error; siblings unaffected. Greening candidate:
   `invariant_progress::dependency_cycle_is_detected_and_left_cleanly_inactive`.

## Round protocol (codified P5/P6 pattern)

Round 1 delivers the fixes and lists claimed-greenable cases. The COO then
dispatches the verifier body/adoption session on the packet branch: new cases where
now pinnable — I2 value-stability (dying provider observed EQUAL to pre-teardown
observation), duplicate-provide refusal (hazard lane), positive failed-fiber re-arm
(gen bump ⇒ exactly one attempt), raw-entry + unknown-field write-back round-trip,
restart-of-FAILED pinned as intended divergence, plain-effect all-or-none,
cross-realm non-blocking withdrawal, I1 interleaved-withdrawal proptest — plus the
suite README deliberate-divergence note, catalog + `expected-green.txt` updates
(rev-8 adoption semantics). Cases needing ledger/wasm surfaces stay prose IOUs in
the catalog. Subsequent rounds fix findings against the real bodies.

## Facade

Minimal **additive** delta authorized where the fixes surface new honest errors
(likely: provision-refused error variant; amendment-refused-self variants). Exact
items documented in the report. Prefer zero (R12).

## Acceptance

- Each scope item proven behaviorally by a crate-owned or invariant test the
  verifier validates; the P6b pins (`reenter*`) stay green.
- I2 ordering: a provider's post-provision effect remains observable to a consumer
  teardown that calls the dying service (the new value-stability case red-first,
  then green).
- Duplicate provision: second provider fails cleanly with recorded reason; first
  provider unaffected; hot-swap same-lane path regression-tested.
- Preservation: profile with an undecodable entry + an unknown field on a decodable
  entry survives load → runtime amend → write-back byte-meaningfully intact.
- Cycle detection: cycle entries inactive with `DependencyCycle` recorded;
  acyclic siblings active; greening candidate validated by the verifier.
- Gates: fmt, clippy -D warnings, build, test, ratchet (expected-green may GROW,
  verifier-owned; no red changes reason without adjudication), miri on touched
  crates, loom where interleaving-sensitive — paste real tails.
- Two-key: zero implementer touches under `tests/invariants/`; verifier-lane
  changes reviewed and adopted by the verify node (rev 8). LOC within ceiling;
  files under 300 lines.

## Out of scope

Ledger, wasm host, intercept plumbing (C6 → P7/P8 design), hot-config acceptance
(C5 → P8 design), per-consumer vitality seam + declarative event selectors (bound
to the wasm-host card), any facade change beyond the authorized delta.
