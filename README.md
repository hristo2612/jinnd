# jinnd

The Jinn kernel: a small Rust daemon that makes a machine legible, reversible, and
safe for an agent to operate and reshape. Everything above it is a plugin behind a
typed capability contract; a product is a profile (a named plugin tree), not a
codebase.

**Start here: [`SOURCE-OF-TRUTH.md`](SOURCE-OF-TRUTH.md)** — the laws, invariants,
rules, and roadmap that govern this repo.

## Layout

| Path | What |
|---|---|
| `SOURCE-OF-TRUTH.md` | The law: Five Laws, four invariants (I1–I4), rules R1–R12, roadmap M0–M4 |
| `docs/constitution/` | The five constitution documents (capability contracts, ledger, revert, profiles, manifest/signing) |
| `contracts/` | WIT capability contracts (versioned) |
| `crates/` | Kernel crates (Cargo workspace) |
| `tests/invariants/` | Theorem-backed acceptance tests; gate every merge |

## Status

M0 COMPLETE — Constitution v0.1 RATIFIED (2026-08-24). M1 in progress; no kernel code yet — by
design: the tests and contracts land before the implementation (rule R2).
