# M1-P0b Suite Bindability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use test-driven-development to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Make every verifier-owned invariant case execute the `jinnd-api::Kernel` facade through an implementer-owned adapter, while keeping the suite intentionally red and ratcheted until kernel subsystems land.

**Architecture:** A new `jinnd-adapter` crate exposes only `kernel() -> impl Kernel`; every facade method is a subsystem-labelled `NO_KERNEL` stub. The invariant suite becomes its own workspace package, uses shared real facade fixtures, and marks facade-unexpressible observations with explicit reasons after driving the closest available subsystem. A shell ratchet enumerates every case and verifies its expected green/red state independently.

**Tech Stack:** Rust 2024, Cargo workspace, Tokio current-thread test runtime, Bash, GitHub Actions.

## Global Constraints

- Do not modify `crates/jinnd-context`.
- Preserve every TS-origin and theorem citation under `tests/invariants/`.
- No `todo!()` may remain in a test body or verifier-owned support code.
- Current invariant failures must contain `NO_KERNEL: <subsystem>` from `jinnd-adapter`.
- `tests/invariants/expected-green.txt` starts empty.
- Plain tests exclude the invariant package; the invariant job runs the suite through the ratchet.
- A descendant `IsolationBinding { realm: Realm::Root }` is explicit, not an erase/inherit sentinel; resolution stops at an intervening ancestor whose binding disagrees, per `reflect.ts:80-94`.
- Conventional commits only; no co-author trailers; zero personal data.

---

### Task 1: Bind invariant tests to the facade

**Files:**
- Create: `tests/invariants/Cargo.toml`
- Modify: `tests/invariants/support.rs`
- Modify: `tests/invariants/*.rs`
- Modify: `tests/invariants/README.md`
- Modify: `crates/jinnd-api/Cargo.toml`
- Modify: `Cargo.toml`

**Interfaces:**
- Consumes: `jinnd_api::Kernel` and `jinnd_adapter::kernel()`.
- Produces: independently named Cargo test cases whose current failure is an adapter `NO_KERNEL` panic.

- [x] **Step 1: Rewrite verifier support and cases first**

Use async tests and a shared `facade_gap(case, subsystem, reason)` driver. It validates the preserved citation metadata, invokes the closest real `Kernel` method, and only then emits `FACADE_GAP: <reason>` if the missing facade surface is reached.

- [x] **Step 2: Verify RED before adapter code exists**

Run: `cargo test -p jinnd-invariants --no-run`

Expected: dependency/build failure because `jinnd-adapter` does not exist yet.

- [x] **Step 3: Keep fully expressible scenarios as real assertions**

For facade-complete cases, construct typed fixture plugins/services/events, await the facade call, and assert returned states, reports, handles, or observations. Do not replace missing observations with a fake test-only kernel API.

### Task 2: Bootstrap the adapter skeleton

**Files:**
- Create: `crates/jinnd-adapter/Cargo.toml`
- Create: `crates/jinnd-adapter/src/lib.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Interfaces:**
- Produces: `pub fn kernel() -> impl jinnd_api::Kernel`.
- Every `Kernel` method fails immediately with one of `NO_KERNEL: context`, `fiber`, `services`, `effects`, `events`, or `loader`.

- [x] **Step 1: Add the minimal private adapter type and public constructor**

Implement the full facade trait with no runtime behavior and no other public adapter item.

- [x] **Step 2: Verify the suite compiles and fails through the adapter**

Run: `cargo test -p jinnd-invariants`

Expected: every case fails; every failure contains `NO_KERNEL:`; no test-local `todo!()` appears.

### Task 3: Add the green ratchet and CI split

**Files:**
- Create: `tests/invariants/expected-green.txt`
- Create: `tests/invariants/check-ratchet.sh`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: exact test names discovered with Cargo and the expected-green file.
- Produces: nonzero status for a listed-red case, an unlisted-green case, a stale expected name, or a red reason not containing `NO_KERNEL:`.

- [x] **Step 1: Add an empty expected-green file and checker**

The checker enumerates each test, runs it exactly, and compares its status with `expected-green.txt`.

- [x] **Step 2: Prove both ratchet directions**

Run once with the empty file (must pass), then temporarily list a red case (must fail), and temporarily add one unlisted green probe (must fail). Revert both temporary mutations before committing.

- [x] **Step 3: Split CI jobs**

Use `cargo test --workspace --exclude jinnd-invariants` for the healthy-tree test gate. Run `tests/invariants/check-ratchet.sh` in a separate invariant job. Preserve the two-key rule while allowing this one bootstrap PR to add the adapter skeleton alongside verifier-owned tests only when every adapter path is newly added.

### Task 4: Final verification and commits

**Files:** all files above.

- [x] **Step 1: Run repository gates**

Run:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --exclude jinnd-invariants
tests/invariants/check-ratchet.sh
```

Expected: all commands exit zero; the ratchet reports every invariant case expected red with `NO_KERNEL` reasons.

- [x] **Step 2: Run privacy and scope checks**

Confirm no diff under `crates/jinnd-context`, no `todo!()` under `tests/invariants`, and no names, credentials, emails, Slack ids, or personal paths in the staged diff.

- [x] **Step 3: Commit with conventional messages**

Commit the suite/adapter bootstrap and CI ratchet without co-author trailers.
