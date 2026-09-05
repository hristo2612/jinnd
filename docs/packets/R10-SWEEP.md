# R10-SWEEP — every production `src/` file under the 300-line cap on `main`, and a CI check that refuses a new breach on any file a PR touches (PLA-363)

**Milestone:** M3-entry hygiene · **Owner:** kernel-dev · **Status:** card
(implementation dispatched via `kernel-packet` after review) · **Card
Todo:** PLA-363 · **Binding rules:** R10 (the subject); R1, R3, R11 and every
other rule untouched — this packet changes NO behaviour · **Design decisions
in scope: NONE** · **Facade:** ZERO items (§Facade) · **Size:** ceiling in
§Size, on `tools/loc-meter` (`cargo run -q -p loc-meter -- --base main`);
declare before code · **Standing gates:** all usual + `cargo build
--workspace --locked`; no fiber-engine concurrency is touched, so no loom
model is owed · **Sequencing:** on the kernel lane after M2-K23 (landed
`f8b285b`), whenever the lane is idle · **Invariants lane:** NONE (§Verifier
lane) · **Decision Log:** none — R10's per-file cap note (2026-08-29) already
states the law this packet enforces; nothing is amended.

## The defect

SOURCE-OF-TRUTH R10 (per-file cap note, 2026-08-29): the 300-line per-file
cap is **hard for `src/`** and **soft for coherent test suites** — a test
file over the cap is split where a natural seam exists, reported as a MINOR,
never a Blocker on line count alone, and still a required fix.

`main` at `f8b285b` carries breaches that pre-date the last packets, and the
verifier has charged two packets in a row (M2-K25, M2-K26) a verify round for
R10 on files they touched while a pre-existing breach sat untouched beside
them. Nothing in CI decides the cap, so every card re-litigates it and every
round pays for it. Two things close that: the breaches are split, and the
cap becomes a gate the PR pays for before a verifier ever reads it.

## 1. The breach list at `main f8b285b`

Measured with `find crates -path '*/src/*.rs' | xargs wc -l | awk '$1>300'`
on 2026-09-05, classified the way the meter classifies (a file reached only
under a `#[cfg(test)]` / `#[cfg(all(test, ..))]` `mod` declaration is `tests`,
outside the production build). "At 138fdce" is the count before M2-K23
merged; K23 is the last packet on `main`.

| # | path | lines | at `138fdce` | class | cap |
|---|---|---|---|---|---|
| 1 | `crates/jinnd-wasm/src/topics.rs` | 415 | 415 | **production** | **HARD — the one production breach** |
| 2 | `crates/jinnd-wasm/src/hostfs/tests.rs` | 794 | 794 | tests (`hostfs.rs:39`, cfg(all(test, not(loom)))) | soft |
| 3 | `crates/jinnd-wasm/src/hostnet/tests.rs` | 706 | 706 | tests (`hostnet.rs:55`) | soft |
| 4 | `crates/jinnd-wasm/src/broker_tests.rs` | 544 | 544 | tests (`lib.rs:21`) | soft |
| 5 | `crates/jinnd-wasm/src/bindings/tests.rs` | 487 | 487 | tests (`bindings.rs:166`, cfg(test)) | soft |
| 6 | `crates/jinnd-wasm/src/grants/tests.rs` | 479 | 479 | tests (`grants.rs:21`) | soft |
| 7 | `crates/jinnd-wasm/src/hostprocess/tests.rs` | 402 | 402 | tests (`hostprocess.rs:40`) | soft |
| 8 | `crates/jinnd-daemon/src/daemon/auth_cap/tests.rs` | 326 | 326 | tests (`auth_cap.rs:41`, cfg(test)) | soft |
| 9 | `crates/jinnd-wasm/src/topics/tests/cycle_restart.rs` | 324 | 324 | tests (child of `topics/tests.rs`, itself under cfg) | soft |
| 10 | `crates/jinnd-loader/src/administer/tests.rs` | 310 | — (**added by M2-K23**) | tests (`administer.rs:30`, cfg(all(test, not(loom)))) | soft |
| 11 | `crates/jinnd-wasm/src/hostnet/outbound_rig_tests.rs` | 305 | 305 | tests (`hostnet.rs:40`) | soft |

**What K23 added or grew:** row 10 only (`administer/tests.rs`, new at 310,
a test suite). No production file grew past the cap in K23; `topics.rs` was
not touched by it (415 before and after — the +8 was M2-K26's grant check).

**`crates/jinnd-api/src/ledger.rs` is NOT a breach at `f8b285b`.** PLA-363's
body names it at 310; that was true at `b1dbe8f`. M2-K23 itself split the
ledger row classes out (`6056482`, `crates/jinnd-api/src/ledger/classes.rs`,
53 lines) on top of the earlier `ledger/record.rs` split (`de43d06`), and the
file is 297 on `main`. This card therefore owes no facade move and no
separate ledger follow-up; the fact is recorded so the acceptance ("every
production `src/` file ≤ 300 on `main`") is read against the tree, not the
Todo's opening figure.

**Next to breach (for the record, not this packet's work):** `store.rs`,
`jinnd-api/src/kernel.rs`, `jinnd-adapter/src/facade.rs` at 299; `waits.rs`
298; `ledger.rs` 297; `lane.rs`, `handle.rs`, `effects/scope.rs` 296. The CI
check in §3 is what stops the next packet from paying a round for one of
these.

## 2. The split — behaviour-free, at natural seams, every path unchanged

### 2.1 `crates/jinnd-wasm/src/topics.rs` (415 → ≈ 185)

`topics.rs` already has three responsibility children (`publish.rs`,
`restarting.rs`, `tombstone.rs` — the last "split from `topics.rs` by
responsibility (R10 file hygiene)" at M2-K26, the precedent this packet
follows). Two more, by the seams the file already draws itself:

| moves to | items (verbatim, no edits) | lines |
|---|---|---|
| `topics/registry.rs` (new, `mod registry;`) | `impl LocalTopics { listen, listen_within, unlisten, rebind }` — the registration table's four mutations (`topics.rs` 188–269) | ≈ 85 + header |
| `topics/emit.rs` (new, `mod emit;`) | `impl LocalTopics { emit }` — the dispatch walk: cycle refusal, K9/K26 refusal, the five-mode delivery loop, the trace append (`topics.rs` 279–411) | ≈ 135 + header |
| stays in `topics.rs` | the module doc, `EventTarget`, `Listener`, `Delivery`, `Selected`, `Rebind`, `Inner`, `EmitReport`, `LocalTopics` and its constructor/installers (`traced`, `watch_restarts`, `watch_waits`) and the private helpers every child shares (`lock`, `park`, `doomed`), the `pub use` lines, the `#[cfg] mod tests;` | ≈ 185 |

**Why these seams and no others:** the two new files are the two verbs of
the port — *register* and *walk* — and each is one `impl LocalTopics` block
that touches the private fields only through `self.lock()` / `self.park()` /
`self.doomed()`, exactly as `tombstone.rs::select` and `publish.rs::publish`
already do from a child module. Rust child modules see the parent's private
items, so nothing gains a `pub(super)` it did not have; `Listener`'s
construction moves with `listen_within`/`rebind` and stays private.

**Every path stays what it was.** All moved items are inherent methods on
`LocalTopics`; a method's path is its type's path, so
`crate::topics::LocalTopics::emit` / `::listen` / `::rebind` / `::unlisten`
are unchanged for every caller (`instance.rs`, `lane/state.rs`,
`slot/commit.rs`, `slot/teardown.rs`, `surfaces.rs`, the crate tests). The
crate's public surface is the `pub use topics::{EmitReport, EventTarget,
LocalTopics, Rebind, RestartOracle, TRANSITIONS_TOPIC, Unserved, grant_for,
reserved}` block in `lib.rs:82` — every one of those items stays in
`topics.rs` (or in the child it is already re-exported from), so the block is
untouched and no new `pub use` is needed. Doc links (`[`crate::broker::Peer`]`
etc.) move with their items unchanged. `#[allow(clippy::too_many_arguments)]`
moves with `emit`.

**What the implementer must NOT do:** re-order arms, rename a local, "tidy" a
comment, merge the two delivery loops, or move `EmitReport`/`Rebind` out of
`topics.rs`. The diff is two cut-and-paste blocks plus two `mod` lines and two
`//!` headers naming the seam (as `tombstone.rs` and `publish.rs` do); the
verifier proves exactly that (§8).

### 2.2 The ten test-suite breaches (rows 2–11) — OUT of this packet, named

R10 makes these soft: MINOR, split where a natural seam exists, a suite with
no seam stays whole and is named in the round's report. They are outside the
production build, outside the CI check's failure path (§3), outside PLA-363's
acceptance ("every **production** `src/` file"), and splitting ten suites in
a behaviour-free packet whose whole value is a diff a verifier can prove is a
pure move would triple the diff for zero production effect. They are named
here so no round has to re-discover them, and are carded as one follow-up
(`R10-SWEEP-TESTS`, COO's to open) with the seams already visible: the
`hostfs`, `hostnet`, `grants`, `hostprocess` suites group by op the way
`hostfs/tests/orphans.rs` already does; `broker_tests.rs` by call class;
`auth_cap/tests.rs` by lane; `administer/tests.rs` (K23) by admin verb.
Row 9 (`cycle_restart.rs`) is one scenario family and may be the "no seam"
case the law allows.

## 3. The CI check — the cap becomes a gate, on the files a PR touches

**Shape.** `.github/scripts/check-r10-file-cap.sh <base-sha> <head-sha>`:

1. `touched` = `git diff --name-only --diff-filter=AMR $(git merge-base
   base head) head -- 'crates/*/src/**/*.rs'` — added, modified, or renamed
   in the PR vs its merge-base (deletions are never a breach). Touched
   means the PR's diff, so a pre-existing breach elsewhere never fails a PR
   that did not touch it — the check is a ratchet, not a retroactive gate.
2. For each touched path, the **production line count as the meter counts
   it**: `tools/loc-meter` gains one read-only mode, `loc-meter --count
   <path>...`, that runs the same compiler-view walk it already runs for a
   diff (`cargo metadata` target roots → `mod` declarations by rustc's rules
   → each item's `#[cfg(..)]` evaluated as the default non-test build
   evaluates it; a false item is dropped with the blank line that separates
   it, so `\n#[cfg(test)]\nmod tests;` costs zero and an inline
   `#[cfg(test)] mod tests { .. }` block costs zero) and prints, per path,
   `production <n>` or `tests` (a file no `lib`/`bin` target reaches in the
   production build). No second counter is written in bash: the meter is the
   canonical one (AGENTS.md "LOC budgets"), and a per-file cap decided by a
   different rule than the budget would be one more thing to re-litigate.
   The mode reuses `walk`/`cfg`/`side` unchanged and adds no dependency.
3. Any touched **production** row with `n > 300` → `::error::` naming the
   file and its count, exit 1. A touched **tests** row over 300 → `::warning::`
   (the law's MINOR: reported, never a Blocker on count alone), exit 0. No
   touched Rust under `crates/*/src` → prints that and exits 0 (a docs-only
   PR is not vacuous here; the property is "no touched production file is
   over the cap", and the empty set satisfies it — unlike the platform-gate
   guard, there is no tree state in which "nothing to check" hides a
   defect). The meter's exit 2 (dirty tree / unresolved `mod`) fails the
   check loudly: a count it could not take is never a pass.

**Wiring — a job in the existing `ci.yml`, additive only.** A new job
`r10-file-cap` (`if: github.event_name == 'pull_request'`, `fetch-depth: 0`,
the stable toolchain and cache the other jobs use, `cargo build -q -p
loc-meter`, then the script on `${{ github.event.pull_request.base.sha }}` /
`head.sha`, merge-base computed inside the script). Its fixture suite (below)
runs as one more step in the `gates` job beside `platform-gate heuristic
fixtures` / `test-inventory differential fixtures` / `push two-key tripwire
fixtures`. **No existing step, job, `if`, or condition is changed;** the
verifier diffs `ci.yml` and every existing script against `main` to prove
it.

**Red-first, twice.**

- *Fixtures* (`.github/scripts/test-r10-file-cap.sh`, the shape of
  `test-push-two-key.sh`): a throwaway git repo holding a one-crate workspace
  (`Cargo.toml` + `crates/example/src/lib.rs`), a baseline commit, then one
  head commit per scenario, the built `target/debug/loc-meter` on `PATH`.
  Must REFUSE: (r1) a touched production file at 301 lines; (r2) a renamed
  production file at 301 (rename is touched); (r3) a touched file at 301
  whose test module is an inline `mod tests { .. }` WITHOUT `#[cfg(test)]`
  (it compiles in the production build, so it counts). Must ACCEPT: (a1) a
  touched file at 320 lines that is 290 after its `#[cfg(test)] mod tests {
  .. }` block — the cfg(test) rule stated above, proven not asserted; (a2) a
  pre-existing 400-line file the head commit does not touch; (a3) a touched
  `tests.rs` reached only under `#[cfg(test)] mod tests;` at 400 lines
  (warning, exit 0); (a4) a deleted production file; (a5) a PR touching no
  Rust under `crates/*/src`. Must FAIL LOUDLY: (f1) the meter absent from
  `PATH` — never pass unchecked. Each assertion checks the message, not
  only the exit code, so a bare `exit 1` cannot pass the suite.
- *Live* (the evidence the verifier reads): on the packet branch, one scratch
  commit `test(r10-sweep): red-first — a production file pushed to 301`
  that adds three comment lines to `crates/jinnd-wasm/src/waits.rs`
  (298 → 301) is pushed and the PR's `r10-file-cap` job goes RED naming
  `waits.rs 301`; the run id is recorded on the Todo and in the PR body;
  the commit is then dropped from the branch (`git revert` is fine too —
  the point is the red run's id, not the history), and the job is green on
  the packet head. The verifier cites that run id; a PR whose red-first run
  cannot be shown is not landed.

## 4. Size — ceiling, declared on `tools/loc-meter`

`cargo run -q -p loc-meter -- --base main --files` on the packet head:

| line | estimate | ceiling |
|---|---|---|
| **production** | ≈ +20 net (two `mod` lines, two `//!` headers, the `use` lines the two children need; every moved line is −1 in `topics.rs` and +1 in the child) | **≤ 30 net**; any production line that is not a `mod`, a header, or an import the move requires is a finding |
| **facade** | 0 | **0** — ZERO new items in `jinnd-api`, and no file move there either (§1: `ledger.rs` is under the cap) |
| contracts | 0 | 0 |
| prose | this card | — |
| tests (outside budget) | 0 in `crates/` (no test moves; `topics/tests.rs` untouched) | 0 |
| tools (outside budget) | `loc-meter --count`: ≈ 50 in `tools/loc-meter/src` + its own unit test in the crate's test module | ≤ 80 |
| other (outside budget) | `check-r10-file-cap.sh` ≈ 70, `test-r10-file-cap.sh` ≈ 150, `ci.yml` ≈ +25 | ≈ 250 |

The number on the production line is what the verifier reads first: a pure
move on the meter is a near-zero net with the `--files` rows showing
`topics.rs` at roughly −230 and the two children at roughly +230 between
them. Meter limit, declared here so it is not re-litigated in the round: the
meter bills a moved line as a delete in one file and an add in another on the
same category, so `+/-` are large while `net` is small — quote `net`, and
paste `--files`.

## 5. Rules cited

- **R10 — Small and boring**: the subject. The per-file cap note is the law
  applied; the split follows the `tombstone.rs`/`publish.rs` precedent
  ("split from `topics.rs` by responsibility"); the check makes the cap a
  gate. Nothing is added to the kernel: two file moves and a CI script.
- **R1**, **R3**, **R11**: untouched, by construction — no `async`, no lock
  scope, no type, no panic boundary changes. `emit` keeps its "no lock is
  held across a delivery" shape line for line (R1) because it keeps its
  lines.
- **R2**: `tests/invariants/` untouched (§8). **R9**: no hazard is near this.
- **AGENTS.md "LOC budgets"**: the meter stays canonical; the cap check
  reads the meter instead of growing a rival count.

## 6. Acceptance

PLA-363's acceptance, verbatim: **"Every production src/ file ≤ 300 lines on
main; a CI check refuses new breaches on touched files; behaviour-free
(gates + ratchet unchanged); verify PASS; landed with CI green."** Plus:
**ratchet and all gates unchanged** — `tests/invariants/expected-green.txt`
(125) and `expected-red-reasons.txt` (35) byte-identical to `main`; every
existing `ci.yml` job and every existing `.github/scripts/*.sh` byte-identical
to `main`; `cargo fmt --check && cargo clippy --workspace --all-targets -- -D
warnings && cargo test --workspace` green; `cargo build --workspace --locked`
green.

## 7. Round protocol — 2 rounds

- **Round 1 (build):** red-first for the CI check — the fixture suite is
  committed failing (no `check-r10-file-cap.sh` yet / no `--count` yet)
  before the script and the meter mode, each in its own commit; the meter's
  `--count` gets its unit test in `tools/loc-meter/src/tests` first (a
  fixture file with a cfg(test) block counts to its production lines). Then
  the two moves, ONE commit each (`refactor(topics): move the registration
  table's mutations to topics/registry.rs (R10, pure move)` and the same for
  `emit.rs`), so `git diff --color-moved` reads each as one block. The live
  red-first scratch commit is pushed and its red run id posted before the
  round-1 verify is requested. Meter declared before code.
- **Round 2 (answer):** findings answered with evidence; `pass` = 0
  Blockers.
- **STOP RULE:** 2 rounds; a third is the COO's call. If the pure-move proof
  fails on a line the implementer believes must change (a `pub(super)` a
  child needs, an import), the change is named in the PR body with the
  reason — it is never folded into a move commit.

## 8. Verifier lane — none; what the verifier proves instead

No invariant case: there is no behaviour to pin (R2 lane untouched). The
verifier's job on this packet is three proofs:

1. **The diff is a pure move.** `git diff -M --color-moved=dimmed-zebra
   main..HEAD -- crates/jinnd-wasm/src/topics.rs crates/jinnd-wasm/src/topics/`
   shows every non-header line as moved, and a symbol-list diff — the sorted
   list of `fn` / `struct` / `enum` / `trait` / `type` / `const` / `pub use`
   signatures under `crates/jinnd-wasm/src/topics*` at `main` and at the
   head — is empty apart from the two `mod` lines. Any content change in a
   move commit is a Blocker.
2. **The CI check went red first.** The run id of the scratch commit's red
   `r10-file-cap` job on this PR, naming `waits.rs 301`, and the fixture
   suite green in `gates` on the head.
3. **Nothing else moved.** `ci.yml` and every pre-existing script under
   `.github/scripts/` byte-identical to `main` except the additions; the
   ratchet files byte-identical; the meter's `--files` rows match §4.

## 9. Explicitly OUT

- The ten test-suite splits (§2.2) — `R10-SWEEP-TESTS`, a follow-up.
- Any change to `crates/jinnd-api` (no file there is over the cap).
- Any retroactive gate on untouched breaches — the check ratchets, it does
  not sweep; the sweep is this packet's §2.
- Any change to the kernel-core LOC ceiling, the meter's budget lines, or
  R10's text. A card sets a number; the law sets what is counted.
- The near-cap files in §1 — they are not breaches, and touching them to
  "make room" would be exactly the golfing the R10 metric note forbids.

## Evidence standard — inherited

As every packet since M2-K18: the meter's output pasted in the PR body, the
standing-gates tail pasted, run ids cited, and the verifier's report by rule
number. This card is public-repo prose: neutral placeholders only.
