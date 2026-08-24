# 01 — Capability Contracts

**Status: DRAFT v0.1-rc2** (verifier round 1 blockers B1 applied). Serves Law 1:
everything is a plugin behind a typed capability contract; no side doors.

## What a contract is

A contract is a **bundle** in `contracts/`: a WIT interface file plus canonical
machine-readable metadata covering, for every operation: its effect class, inverse
obligation, per-key equality relation, sensitivity class, scope type, and the scope
**subset predicate** (below). **CI rejects incomplete bundles** — a contract missing
any of these fields cannot merge, so unenforceable contracts cannot exist.

A contract is simultaneously:

1. **An interface** — the typed functions and events a service exposes.
2. **A permission** — a plugin may call only the contracts it was granted; the kernel
   enforces this at the boundary for every hosting mode (see Mechanical closure).
3. **A ledger schema** — every crossing of the contract is a ledger event (Law 2).

There is no other way for a plugin to affect the world. If a power is not expressible
as a contract, the kernel does not offer it.

## Contract anatomy

- **`name` + semver** — e.g. `jinn:fs@1.2.0`. Consumers bind to a major version.
  Within a major, changes are strictly additive (R12).
- **Effect classes** — each function is marked `read`, `revertible`, or
  `irreversible`. `revertible` functions define their inverse obligation (03).
  `irreversible` functions are callable only through the confirmation flow (03).
- **Equality semantics** — the observational-equivalence relation for the service's
  state: what "restored" means for this contract (03).
- **Sensitivity class** — `public`, `personal`, or `secret` (02 §Redaction).
- **Scope type + subset predicate** — a contract that supports scoped grants declares
  its scope type (e.g. a path prefix, an id set, a rate) and a decidable predicate
  `subset(a, b)` the kernel evaluates. This is normative in v0.1, not deferred: a
  grant without a machine-checkable subset predicate cannot be attenuated and
  therefore cannot be granted to children.

## Grants

- A grant is (plugin identity, contract, version range, optional scope).
- **Attenuation law:** a parent may grant a child only capabilities it holds itself.
  The kernel grants a child scope only when the contract's subset predicate proves it
  no broader than the parent's scope. Authority only shrinks down the tree.
- Grants are declared in the profile (04) or requested in the manifest (05); every
  grant and every denial is a ledger event.
- **Event subscriptions are covered by the contract grant** in v0.1 (no separate
  grant class); every model-visible delivery produces a consumption receipt (02).
- Revoking a grant deactivates dependent fibers through normal epoch gating.

## Mechanical closure (per hosting mode)

Law 1 is only real where it is mechanically unavailable to cheat:

- **`wasm`** — closed by construction: imports are exactly the granted contracts;
  no ambient capabilities exist. This is the default and only mode for
  runtime-installed code.
- **`subprocess`** — **disabled in v0.1.** The mode may be enabled only after its
  mandatory OS sandbox (per-platform) is implemented and direct host authority is
  mechanically unavailable. Until then the kernel refuses `host = "subprocess"`.
- **`inproc`** — in-process Rust is *not sandboxable*; pretending otherwise would be
  a side door. Therefore: **InProc code is part of the kernel's trusted computing
  base (TCB)**, first-party only, and the constitution treats it honestly as such:
  (a) the TCB membership list is enumerated, minimized, and visible in every profile;
  (b) InProc crates live in a CI lane that mechanically forbids direct ambient
  authority (`std::fs`, `std::net`, `std::process`, FFI) outside granted provider
  contracts, enforced by deny-lints and import checks; (c) all InProc contract
  crossings are ledger-visible like everyone else's. ⚠️ **Ratification flag:** this
  is an interpretation of Law 1 ("our plugins are never special" → "InProc = TCB,
  enumerated and disciplined, never silently privileged"). The operator must
  explicitly accept this reading at M0 acceptance.

## Open questions for v0.2

- Richer scope-type library (beyond prefix/set/rate) as contracts demand it.
- Finer-grained subscription grants (v0.1: contract grant covers subscription).
