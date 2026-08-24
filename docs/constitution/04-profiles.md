# 04 — Profiles

**Status: DRAFT v0.1.** Serves Law 4: a device is a profile.

## What a profile is

A profile is a named, versioned document declaring a plugin tree: which plugins, in
what nesting, with what config, grants, and isolation. It is the complete description
of a running system. `company`, `personal`, `jinnos-desktop`, `nucleus` are profiles —
one kernel, different documents. Sharing a profile is sharing a configured computer as
a text file.

## Format

YAML (JSON accepted), a tree of **entries**:

```yaml
profile: company
version: 3
extends: personal          # layering: this profile overlays another
entries:
  - id: a1b2c3            # stable identity — the reconcile key
    name: jinn:todos      # plugin identity (manifest name, 05)
    config: { ... }        # serde-validated against the plugin's schema
    grants:                # capability grants for this entry (01)
      - jinn:ledger@1
      - { contract: jinn:fs@1, scope: "~/notes" }
    disabled: false
    entries: [ ... ]       # children: nesting = the fiber tree; parent
                           # disposal unwinds the subtree; grants attenuate (01)
    isolate: { db: tenant-a }   # this subtree resolves `db` in realm tenant-a
    intercept: { log: { level: debug } }  # config overlay for descendants
```

Rules:

- **`id` is identity.** Reconciliation is by id: edits restart exactly the affected
  entries (config change = in-place restart; name/inject change = stop + start; moved
  id = move, not destroy). Invariant I4 guarantees the result equals a fresh boot of
  the final document.
- **Layering:** `extends` composes documents (base → overlay, id-merged). A device's
  effective profile = its layer stack; small-brain/big-brain is one overlay entry
  swapping a provider.
- **Bidirectional:** runtime changes (operator toggles a plugin, a plugin persists
  config) write back to the document atomically. The document and the running system
  are two views of one truth; the ledger records every write-back.
- **Expressions:** config values may use a closed, side-effect-free expression subset
  (platform, env lookup, profile vars). No general evaluation, no ambient authority
  (R9).
- **No secrets in profiles.** Profiles reference secrets by name; values live in the
  host keystore.

## Guarantees

Reconcile is provably safe (I4): entries not touched by a diff are not restarted;
removal is exact (I1); activation order follows dependencies, never document order
(I2/I3). A profile that fails validation is rejected whole — the running system never
enters a state no document describes.

## Open questions for v0.2

- Profile variables & templating scope (per-device values without forking documents).
- Marketplace/profile-sharing format: signing profiles like plugins (05)?
