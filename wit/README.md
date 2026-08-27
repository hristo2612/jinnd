# The plugin world — `jinn:plugin@0.1.0`

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
  there, not copies here.

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
