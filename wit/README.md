# The plugin world — `jinn:plugin@0.10.0`

Version history (additive within 0.x, R12): 0.1.0 M1-P8 world; 0.2.0 M2-K3
`fs` finalized to its bundle; 0.3.0 M2-K4 lifecycle semantics (suspend ≠
dispose — kernel registrations release on suspend, world mutations are
entry-scoped and withdrawn only at dispose); 0.4.0 M2-K6 `process`/`net`
long-lived editions; 0.5.0 M2-K8 `keystore` finalized to its bundle
(`delete`, `list`, `keystore-error`), grants gain an `ops` attenuation, and
`jinn:profile` 0.2.0 (non-blocking `patch-entry`, `entry`/`document` reads).

WIT interface files for the Tier A (WASM component) plugin world: lifecycle
entry points, effect registration, service provide/inject, event emit/listen,
and the config surface (M1-P8; constitution 01; R7, R12).

These files are the product (R12): versioned, never breaking within a major
version, designed to outlive the current kernel implementation.

## Layout

- `plugin.wit` — the `jinn:plugin` package: kernel-surface imports
  (`effects`, `services`, `events`), the `lifecycle` guest export, and the
  `plugin` world.
- Capability contract **bundles** (interface + canonical metadata: effect
  class, inverse obligation, equality relation, sensitivity, scope predicate)
  live in `contracts/`, governed by `docs/constitution/01-capability-contracts.md`.
  The base host-provider contracts the kernel supplies — `jinn:fs`,
  `jinn:ledger`, `jinn:process`, `jinn:net`, `jinn:keystore` — are bundles
  there, not copies here; so are the operator contracts reached over the
  handle lane (`jinn:introspect`, `jinn:profile`; M2-K7). The `net`
  import's readiness wake (`jinn:net/readable`, M2-K7) is additive prose
  on the 0.4.0 (M2-K7) world: no signature changed.

## v0.1 binding of grants to imports

A component's imports are exactly the kernel surfaces of the `plugin` world.
Authority over real contracts (`jinn:fs`, ...) arrives as **grants**: the
broker's single dispatch point (grant check → ledger append → dispatch;
decision log 2026-08-25) refuses `services.resolve` for an ungranted
contract, so no handle exists and no call can be made. Closure is mechanical
— it lives at the one choke point every transport shares, which is what makes
the broker transport-agnostic rather than fused to WASM linking.

Typed per-contract import worlds (a component importing `jinn:fs/files`
directly, bindings generated per grant) are the planned v0.2 refinement; they
tighten closure from "no handle without a grant" to "no import without a
grant" without changing the broker's choke point.

## Validation

`crates/jinnd-wasm` validates this package at compile time:
`wasmtime::component::bindgen!` parses `wit/` and generates the host
bindings the Tier A host implements. The fixture component
(`fixtures/counter-plugin`) implements the guest side of the same world via
`wit-bindgen`.
