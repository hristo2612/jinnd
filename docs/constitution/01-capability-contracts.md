# 01 — Capability Contracts

**Status: DRAFT v0.1.** Serves Law 1: everything is a plugin behind a typed capability
contract; no side doors.

## What a contract is

A contract is a WIT (`.wit`) interface file in `contracts/`, plus this document's
rules. It is simultaneously:

1. **An interface** — the typed functions and events a service exposes.
2. **A permission** — a plugin may call only the contracts it was granted; the kernel
   enforces this at the boundary for every hosting mode.
3. **A ledger schema** — every crossing of the contract is a ledger event (Law 2).

There is no other way for a plugin to affect the world. If a power is not expressible
as a contract, the kernel does not offer it.

## Contract anatomy

Every contract declares, beyond its WIT interface:

- **`name` + semver** — e.g. `jinn:fs@1.2.0`. Consumers bind to a major version.
  Within a major, changes are strictly additive (R12).
- **Effect classes** — each function is marked `read`, `revertible`, or
  `irreversible`. `revertible` functions define their inverse obligation (03).
  `irreversible` functions (send an email, make a payment) are callable only through
  the confirmation flow defined in 03 §Irreversible.
- **Equality semantics** — the observational-equivalence relation for the service's
  state: what "restored" means for this contract (03). Example: a registration table
  compares by entries, never by iteration order or timestamps.
- **Sensitivity class** — `public`, `personal`, or `secret`. Controls ledger payload
  redaction (02 §Redaction) and default grant policy.

## Grants

- A grant is (plugin identity, contract, version range, optional scope narrowing —
  e.g. `jinn:fs` limited to a directory subtree).
- **Attenuation law:** a parent plugin may grant a child only capabilities it holds
  itself, same or narrower scope. Authority only shrinks down the tree, never grows.
- Grants are declared in the profile (04) or requested in the manifest (05) and
  approved by policy; every grant and every denial is a ledger event.
- Revoking a grant deactivates dependent fibers through normal epoch gating — no
  special path.

## The first-party rule

Kernel-built-in (InProc) plugins declare and are granted contracts exactly like
everyone else. The kernel ships with a `contracts/` directory that includes its own
providers' declarations. "Our plugins are never special, only pre-installed."

## Open questions for v0.2

- Scope-narrowing grammar (per-contract, or a kernel-level scoping language?).
- Whether event subscriptions need their own grant class or ride the contract grant.
