# 05 — Manifest & Signing

**Status: DRAFT v0.1.** Serves Law 5: plugins are sandboxed and signed; provenance is
law.

## The manifest

Every plugin ships a `plugin.toml`:

```toml
[plugin]
name    = "jinn:todos"        # namespaced identity
version = "1.4.0"             # semver
host    = "wasm"              # inproc | wasm | subprocess (R7)
entry   = "todos.wasm"        # artifact, content-addressed below

[provides]                     # contracts implemented (name@version)
"jinn:todos" = "1"

[requires]                     # capability REQUESTS — granted by profile/policy (01)
"jinn:ledger" = "1"
"jinn:fs"     = { version = "1", scope-hint = "notes directory" }

[provenance]
author   = "..."               # publisher identity (key id)
origin   = "human | agent"     # machine-written code is marked, always
built-by = "..."               # toolchain fingerprint

[integrity]
artifact-sha256 = "..."
```

Requests are not grants: the manifest asks, the profile + policy grant (01). A plugin
that receives fewer grants than requested must either degrade gracefully or stay
inactive — requesting maximal power is an anti-pattern the registry surfaces.

## Signing

- Artifacts are content-addressed (sha256) and signed (ed25519) by the publisher key
  named in `provenance.author`.
- The kernel holds a **trust store** of publisher keys with per-key policy. Signature
  verification happens at install AND at every load; the ledger records both.
- **Trust tiers:**
  1. `first-party` — our keys. Same rules as everyone (Law 1); the tier only affects
     default grant policy.
  2. `signed` — a known publisher key. Default policy per key.
  3. `local-dev` — unsigned, allowed only when the profile explicitly enables dev
     mode for that entry; loudly marked in UI and ledger.
- **Agent-generated plugins** (`origin = "agent"`): always sandboxed hosts (never
  InProc), signed by the *generating* system's key, marked lower-trust, and gated by
  evals before receiving grants beyond their sandbox — the eval program is the
  immune system of the OS. A human can promote one to a normal signed publisher
  identity after review.

## Sandboxing (per host, enforced by kernel)

- `wasm` — wasmtime component; no ambient capabilities; imports are exactly the
  granted contracts; fuel/memory metering per entry.
- `subprocess` — supervised child; capability access only via the kernel's IPC; OS
  sandboxing profile per platform (TBD v0.2).
- `inproc` — trusted by construction (compiled in), first-party only, still
  contract-bound and ledger-visible.

## Open questions for v0.2

- Key rotation and revocation flow (compromised publisher).
- Subprocess OS-sandbox baseline per platform (macOS sandbox-exec successor, Linux
  namespaces/landlock, Android isolated service).
- Update channels & rollback policy (pin-by-hash vs track-minor).
