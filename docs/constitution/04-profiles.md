# 04 — Profiles

**Status: RATIFIED v0.1 — 2026-08-24.** Serves Law 4: a
device is a profile.

## What a profile is

A profile is a named, versioned document declaring a plugin tree: which plugins, in
what nesting, with what config, grants, and isolation. It is the complete description
of a running system. `company`, `personal`, `jinnos-desktop`, `nucleus` are profiles —
one kernel, different documents. **v0.1: profiles are host-owned local documents;
imported/shared profiles are rejected** (profile signing arrives with a v0.2+
amendment — 05).

## Format

YAML (JSON accepted), a tree of **entries**:

```yaml
profile: company
version: 3
extends: personal           # layering: this profile overlays another (id-merged)
vars:                       # the ONLY expression inputs (see Expressions)
  notes-dir: "~/notes"
entries:
  - id: a1b2c3              # stable CONFIG identity — the reconcile key
    name: jinn:todos        # plugin identity (manifest name, 05)
    config:                 # the entry's seat document (the decoder's shape)
      grants:
        - jinn:ledger@1
        - { contract: jinn:fs@1, scope: "${vars.notes-dir}" }
      injects:              # string-lane contracts injected at activation (M2-K24): a gate, never a grant
        - jinn:ui-bundle
      data: { ... }         # the plugin's own payload, serde-validated against its schema
    disabled: false
    entries: [ ... ]        # children: nesting = the fiber tree; grants attenuate (01)
    isolate: { db: tenant-a }
    intercept: { log: { level: debug } }
```

Rules:

- **`id` is configuration identity, not fiber identity.** Reconciliation is by id.
  Moving an entry (new parent, realm, grants, or intercept chain) re-derives its
  context and recomputes its epoch inputs; **the epoch machinery alone decides
  whether the fiber restarts.** A move that changes any epoch input (a provided or
  injected realm binding, grants, intercept chain) forces a full clean unload →
  reload under the new derived context with a fresh, never-reused fiber generation.
  A move that changes none preserves the running fiber — runtime state survives
  exactly when the kernel can prove the environment is unchanged. *(Amended
  2026-08-25 from "always a complete unload/reload": the ratified sentence
  contradicted the kernel, the verifier-green suite, and the reference
  implementation — see SOURCE-OF-TRUTH decision log.)*
- **`injects` gates; `grants` authorize.** A wasm entry may declare, beside its
  grants in `config` (`config.injects` beside `config.grants`, exactly where the
  seat decoder reads them), the string-lane contracts it injects at activation — a
  bare contract name or `{ contract }`, in declaration order; absent means none, and
  a present `injects` that is no list (`null` included) is a per-entry fault. The
  kernel activates
  the entry only once every declared provider is `Active`, reloads it when one is
  replaced, and re-arms it from `Failed` when one lands later — the typed lane's
  epoch gating on the string lane (SOURCE-OF-TRUTH §3). A declaration never widens
  what the entry may call: a declared contract the entry holds no grant for, or an
  element that is no declaration (a scope or ops key, an object naming no contract),
  is a per-entry fault refused on the record at admission (R11). *(Added 2026-09-03,
  M2-K24; a 0.x MINOR of the seat config schema, R12.)*
- **Layering:** `extends` composes documents (base → overlay, id-merged). A device's
  effective profile = its layer stack; small-brain/big-brain is one overlay entry
  swapping a provider.
- **Write-back is confined.** A plugin may write **only its own schema-validated
  `config` subtree**. Grants, identity, nesting, isolation, interception, policy, and
  profile ancestry require a separate operator-authorized control capability
  (`jinn:profile-admin`) (M2-K23: five writes — add, remove, `disabled`, grants,
  plugin identity — each an operator intent applied by reconcile-by-id, each a
  `ProfileAdministered` row with the caller and both digests, each reversible by
  the inverse write the row records); every write-back is a ledger event.
- **Expressions:** config values may reference only `${vars.*}` (declared,
  non-secret, allowlisted in the profile itself) and `${platform}` — a closed,
  side-effect-free grammar with no environment access, no general evaluation, no
  ambient authority (R9). Every expression resolution is deterministic from the
  document plus the kernel-defined platform constant; no other host state is
  readable.
- **No secrets in profiles.** Profiles reference secrets by name; values live in the
  host keystore.

## Reconcile semantics (bound to the fiber calculus)

- **The restart set is: entries directly changed by the diff PLUS everything
  transitively affected** — through provider generation changes, grant changes, realm
  or intercept changes, or parent-context changes. A provider generation change fully
  unloads and reloads every consumer (epoch gating, SOURCE-OF-TRUTH §3); "my entry
  didn't change" never exempts a consumer whose provider did. A string-lane
  declaration (`injects`) is an epoch input exactly as a typed injection is: an edit
  to it restarts the entry by reconcile, and a declared provider's generation change
  reaches the consumer the same way (M2-K24). A plugin-identity change (package
  or pin) is an incarnation replacement of the SAME entry — the M2-K4 ruling:
  the successor inherits the entry's live effect journal and the swap window is
  closed to reply-expecting walks (M2-K23 (e); where a kernel version has not
  yet closed it, its bundle README names the limit — at M2-K23 the loader's
  `Replace` still disposes then spawns, pinned by the daemon suite and carded
  as M2-K27 with its loom model).
- Reconciliation maintains **committed vs target views** per fiber (the paper's
  calculus): a leaving provider stops admitting new consumers *before* its inverses
  run, while committed consumers keep their existing handle until their teardown
  completes (I2). Every launched transition lands before reconciling to the latest
  target (inertia, R1); stale in-flight steps are diverted by epoch; dependency
  cycles stay cleanly inactive (I3); failure is local with no auto-retry against an
  unchanged environment (R9/R11). A restart's window is closed to reply-expecting
  walks: a listener's subscription is replaced at its successor's commit, never
  absent in between, and a walk that selects it meanwhile is refused `restarting`
  (M2-K26).
- A profile document that fails **document-level validation** (unparseable file,
  duplicate ids, malformed tree shape) is rejected whole — the running system never
  enters a state no document describes. **Entry-level faults are contained per entry
  (R11):** a malformed entry loads nothing, is preserved verbatim on disk, and
  surfaces a recorded error while its siblings load normally. I4 guarantees the
  reconciled result equals a fresh boot of the final document. *(Amended 2026-08-25:
  whole-rejection scoped to document shape — the ratified sentence contradicted the
  shipped per-entry containment doctrine; see SOURCE-OF-TRUTH decision log.)*

## Open questions for v0.2

- Profile signing + import/sharing format (v0.1: local-only).
- Richer var typing/templating (v0.1: string vars + platform const only).
