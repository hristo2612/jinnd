# 05 — Manifest & Signing

**Status: DRAFT v0.1-rc2** (verifier round 1 blocker B2 applied). Serves Law 5:
plugins are sandboxed and signed; provenance is law.

## The signed envelope

A plugin is distributed as **one canonical signed envelope**. The ed25519 signature
covers, together (never separately): the complete manifest hash, the artifact hash,
the hashes of every contract bundle it provides, the publisher key id, the immutable
**code-origin attestation**, and build provenance. Verification of the whole envelope
happens **at install AND at every load**; both are ledger events. A manifest field
can therefore never drift from what was signed — host mode, requested capabilities,
identity, version, and origin are all inside the signature.

## The manifest (inside the envelope)

```toml
[plugin]
name    = "jinn:todos"        # namespaced identity
version = "1.4.0"             # semver
host    = "wasm"              # inproc | wasm | subprocess (R7; see Hosting)
entry   = "todos.wasm"        # content-addressed artifact

[provides]
"jinn:todos" = "1"            # contract bundles implemented

[requires]                     # capability REQUESTS — granted by profile/policy (01)
"jinn:ledger" = "1"
"jinn:fs"     = { version = "1", scope-hint = "notes directory" }

[provenance]
author   = "<publisher key id>"
origin   = "human | agent"    # IMMUTABLE code-origin attestation
built-by = "<toolchain fingerprint>"
```

Requests are not grants: the manifest asks; the profile + policy grant (01). A plugin
granted less than requested must degrade gracefully or stay inactive; maximal
requests are surfaced by the registry as an anti-pattern.

## Signing & trust

- **There is no unsigned tier.** Local development uses a **local development signing
  key**, generated on the host and explicitly trusted by that profile's trust store;
  dev-key plugins are loudly marked in UI and ledger. An artifact with no valid
  envelope simply does not load, in any mode, ever.
- The kernel holds a **trust store** of publisher keys with per-key policy. Tiers:
  1. `first-party` — our keys. Same rules as everyone (Law 1); tier affects only
     default grant policy.
  2. `signed` — a known publisher key, per-key policy.
  3. `local-dev` — the host's own dev key, profile-scoped trust.
- **Revocation is v0.1, not future work:** the kernel maintains a revocation
  epoch/denylist checked at install and at every load; a revoked key loads nothing —
  including already-cached artifacts. (Rotation *UX* may evolve; load-time
  denylisting is constitutional now.)
- **`origin` is immutable.** Human review of an agent-written plugin may change its
  review status and grant policy; it never rewrites `origin = "agent"`. Machine
  authorship is permanent, queryable provenance.
- **Agent-generated plugins** (`origin = "agent"`): always sandboxed hosts (never
  InProc), signed by the *generating* system's key, lower default trust, and gated by
  evals before receiving grants beyond their sandbox — the eval program is the
  immune system of the OS.

## Hosting (enforcement per mode — normative table in 01 §Mechanical closure)

- `wasm` — wasmtime component; imports are exactly the granted contracts; fuel and
  memory metering per entry. The only mode for runtime-installed code.
- `subprocess` — **disabled in v0.1** until the mandatory per-platform OS sandbox
  exists (01).
- `inproc` — kernel TCB, first-party only, enumerated in the profile, CI-disciplined
  (01). Never available to runtime-installed or agent-generated code.

## Updates

**v0.1 pins exact signed hashes.** A profile references a specific envelope; there is
no automatic channel tracking. Updating = the profile document changing to a new
pinned envelope (a ledgered, revertible composition event). Channels/auto-update
policy are a v0.2+ amendment.

## Open questions for v0.2

- Key rotation ceremony/UX (revocation itself is v0.1, above).
- Subprocess OS-sandbox baseline per platform (gates enabling that mode).
- Update channels and rollback automation (v0.1: pin-by-hash only).
