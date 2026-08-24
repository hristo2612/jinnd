# Packet M1-P0b — Suite Bindability (verifier-owned)

**Milestone:** M1 · **Owner:** jinn-verifier (two-key: suite is yours) ·
**Status:** ready · **Binding rules:** R2, R9, two-key rule.

## Why

M1-P1 exposed a P0 defect: the invariant cases are `spec_case!` prose + `todo!()`
and never call the `jinnd-api` facade. Nothing can ever turn green without editing
`tests/invariants/`, which R2 forbids implementers. The suite must become
*bindable* while staying verifier-owned.

## Scope

1. **Adapter seam.** Create `crates/jinnd-adapter`: a single public constructor
   (e.g. `pub fn kernel() -> impl jinnd_api::Kernel`) whose body is per-subsystem
   `todo!("NO_KERNEL: <subsystem>")` stubs. After this packet the adapter is
   **implementer-owned** (it is wiring, not spec); this packet only bootstraps the
   skeleton so the suite compiles against it.
2. **Rewrite the invariant cases to DRIVE the facade** through
   `jinnd_adapter::kernel()`: real calls, real assertions, keeping every TS-origin
   / theorem citation. Red must now come from the adapter's `NO_KERNEL` stubs
   reaching the test, never from a `todo!()` inside the test itself. A case that
   cannot yet be expressed against the facade is a facade gap — list it for a
   COO-approved facade amendment rather than stubbing around it.
3. **Green-ratchet.** Add `tests/invariants/expected-green.txt` (verifier-owned):
   the exact case list currently expected green (initially empty). CI's invariant
   job becomes a ratchet: fail on any listed case red, fail on any unlisted case
   green. Progress = ratchet-file updates with verifier sign-off.
4. **CI split.** Plain `test` job runs unit/crate tests only (exclude the
   invariant targets); the invariant job runs the suite + ratchet check. The
   workspace test gate must be green on a healthy tree from this packet onward.

## Acceptance

- `cargo test --workspace` (minus invariant targets) green; invariant suite runs
  with all cases red via adapter `NO_KERNEL` reasons; ratchet check passes with an
  empty expected-green list; citations preserved; facade-gap list (if any)
  delivered for COO review.

## Out of scope

Wiring any real crate into the adapter (that is M1-P1c, kernel-dev). Any change to
`crates/jinnd-context`.
