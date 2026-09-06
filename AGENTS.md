# Agent Instructions — jinnd

You are working on the Jinn kernel. **Read `SOURCE-OF-TRUTH.md` before anything else.**
It is LAW; this file translates it into direct instructions for you.

## Non-negotiables

1. **R1 — Async-first.** All lifecycle code is tokio-native: per-fiber supervisor
   tasks, `watch` channels, cancellation tokens. You MUST NOT introduce a blocking
   executor, call `block_on` inside the kernel, or hold any lock across a call into
   plugin code. If you find yourself wanting to, stop and report the design problem.
2. **R2 — Tests are the spec.** `tests/invariants/` and the ported Cordis spec tests
   define correct behavior. **You MUST NOT edit, weaken, `#[ignore]`, or delete any
   test under `tests/invariants/` to make your code pass.** If you believe a test is
   wrong, stop and report it with your reasoning; the verifier decides. CI enforces
   this, but you comply because it's the point, not because you're caught.
3. **R5 — One mutation primitive.** Every side effect goes through the effect
   primitive and registers its undo. No direct mutation of shared state, ever.
4. **R9 — Hazards stay dead.** Never implement: emit-abort-on-first-error,
   async-counts-as-bailed, side-effectful service constructors, config expression
   evaluation with ambient authority, native dylib loading, silent service
   replacement, auto-retry of failed fibers against an unchanged environment.
5. **R10 — Small and boring.** Kernel core ceiling 8k LOC. Before adding anything to
   the kernel ask: "could this be a plugin?" If yes, it is one. New files stay under
   300 lines; split by responsibility. Honor the named cohesion exceptions in
   SOURCE-OF-TRUTH R10.
6. **R11 — Failure is local.** No panic may cross the kernel boundary. Every
   plugin-facing entry point is panic-contained. A plugin's failure may affect only
   itself and its declared dependents.

## Working rules

- **Cite rules by number** in commits, PR descriptions, and review responses
  ("implements R4 handle model", "rejected: violates R1").
- **TDD**: the failing test exists before your implementation. If you're implementing
  and no test covers your change, write the missing test FIRST in the crate's own
  test module (never in `tests/invariants/` — that's the verifier's territory).
- **Verify before claiming done.** Follow the required CI gates: `cargo fmt --all
  --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace --exclude jinnd-invariants`,
  `tests/invariants/check-ratchet.sh`, and `cargo build --workspace --locked`.
  Expected-red invariant cases are judged by the verifier-owned ratchet, not
  ordinary workspace test success. Run the required loom/miri jobs for affected
  concurrency/unsafe behavior. Record full commit SHA, commands and actual tails.
  Current PR and post-merge Linux CI remain required; skipped proof is not green.
- **Concurrency changes** to the fiber engine require a loom model test exercising
  the interleaving you claim to handle.
- **LOC budgets** are declared and read on the canonical meter
  `tools/loc-meter` (`cargo run -q -p loc-meter -- --base main`): it decides
  cfg(test)-ness the compiler's way and refuses a dirty tree (M2-K18). Never
  quote a `git diff | awk` pipeline in a card.
- **No new dependencies** without justification against R10 in the PR description.
  Preferred set: tokio, tokio-util, serde, serde_json, tracing, rusqlite (ledger),
  wasmtime (host crate only), axum (API crate only), proptest/loom (dev).
- **Docs**: public items get doc comments stating their contract, not their
  implementation. Contract changes touch `contracts/` + version bump, per R12.

## Repo hygiene

- This repo will eventually be public. **Zero personal data**: no real names, keys,
  emails, personal paths, or references to the operator's other projects. Neutral
  placeholders only.
- Conventional commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`). No co-author
  trailers.
- Never commit directly to `main` once CI exists; branch + PR per work packet.

## Where things live

- `SOURCE-OF-TRUTH.md` — the law (Laws, Invariants I1–I4, Rules R1–R12, roadmap)
- `docs/constitution/` — the five constitution documents (M0)
- `contracts/` — WIT capability contracts (versioned; R12)
- `crates/` — kernel crates (workspace)
- `tests/invariants/` — I1–I4 + ported Cordis spec suite (verifier-owned;
  read-only for implementers)
- Reference material (read-only, never depend on): the audit synthesis and the
  reference checkouts (TS Cordis, the paper, cordis-rs) live in the private annex
  outside this repo.

## Worktree discipline (added 2026-08-24 after a branch collision)

The primary checkout `~/Projects/jinnd` is shared and its checked-out branch is
not yours. **Every packet works in its own worktree**:
`git -C ~/Projects/jinnd worktree add ~/Projects/.worktrees/jinnd-<packet> -b <branch> main`.
Never `git checkout`/`git switch` in the primary checkout; never commit there
without `git branch --show-current` proving you are where you think you are.
Remove your worktree after your branch merges (`git worktree remove`, then prune).

## One packet, one live implement session (added 2026-08-24 after a double dispatch)

A Todo comment is itself a dispatch signal: any live session attached to that Todo
may act on it. Before a workflow run (or a new delegation) starts implementing a
packet, every previously delegated session on that Todo must be stopped or
explicitly scope-closed. Never post findings as a comment "for the record" while
another executor is being launched — route findings through exactly one live
implementer. If you discover another agent's uncommitted edits in your tree:
STOP, commit nothing, preserve everything, escalate for an ownership ruling.

**Merges are edits too:** a merge-conflict resolution that touches
`tests/invariants/` is a verifier-key change like any other — an implementer
resolving a conflict there must take the verifier's side byte-for-byte or STOP
and escalate. (Added after merge 36333fe silently deleted a verifier case; caught
and restored in f400acc.)

## Scoped verification evidence

For new work after this amendment, full gates remain the default. An independent
reviewer may reuse a previously successful independent gate result for unchanged
inputs: record both complete SHAs, the full intervening diff, original command and
output, platform/toolchain, dependency locks, build scripts/config and features. For the
harness also record the exact kernel pin and loader/profile/plugin identity.
Prove that the changed files cannot affect that gate's behavior, including generated
assets and indirect inputs. Label the result reused, not newly executed.

A docs/comment-only or web-only delta can qualify; file extension alone is not
proof. Rerun all affected behavior and acceptance checks on the new head, including
live UI checks and real-loader composition for affected integration behavior.
Changes to Rust/runtime/contract/pin, profiles, loaders, dependencies or build
machinery require the relevant full Rust/composition gates. Missing, ambiguous or
non-independent prior evidence requires full validation. Source-of-truth invariants,
verifier ownership and every required PR/post-merge CI gate remain binding.
