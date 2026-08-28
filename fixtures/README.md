# Fixture components

Tiny Tier A guests used by the host test suites. Only SOURCE is checked in —
never a `.wasm` artifact (M1-P8 round protocol): the binary is rebuilt from
this source on every test run, and its pin hash is computed then, so the
tests always exercise the real pin-by-hash admission path (Law 5).

## counter-plugin

Implements the `jinn:plugin` world (`wit/plugin.wit`) via `wit-bindgen`.
Its `activate` mode comes from the entry config (UTF-8): `plain`,
`provider`, `picky`, `caller`, `ungranted`, `trap`, `spin`, the clock modes,
and the `jinn:fs` bundle probes `fs-bundle` / `fs-bundle-denied` /
`fs-scope-probe` (M2-K3) — one fixture,
each containment and broker behavior selectable per entry.

## Build

```sh
rustup target add wasm32-unknown-unknown
cd fixtures/counter-plugin
cargo build --release --target wasm32-unknown-unknown
```

The target produces a CORE module; the test helper
(`crates/jinnd-wasm/tests/support/mod.rs`) encodes it to a component
in-process with `wit-component` (dev-dependency) and computes the pin. The
helper runs this exact build itself; if the PATH's rustc lacks the wasm std
(e.g. a distribution rustc shadowing rustup), it falls back to the rustup
toolchain's own `cargo`/`rustc`.

The crate is deliberately NOT a member of the kernel workspace: it compiles
only for wasm32-unknown-unknown, and its guest-side dependencies
(`wit-bindgen`) never enter the kernel's dependency graph (R10).
