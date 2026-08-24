# The Constitution — Overview

**Status: RATIFIED v0.1 — operator approval 2026-08-24 (M0 accepted).** The operator
also explicitly confirmed the removal of the InProc plugin host (01 §Mechanical
closure): all plugins, first-party included, run in the sandboxed WASM host; native
Rust is kernel implementation only.

These five documents ARE jinnOS. Everything else — the kernel implementation, every
plugin, every profile, every device — is replaceable; these are what persist. They are
deliberately short: the whole constitution is meant to be read in one sitting.

| Doc | Governs | Law it serves |
|---|---|---|
| [01 — Capability Contracts](01-capability-contracts.md) | How plugins declare and are granted power | Law 1 |
| [02 — The Ledger](02-ledger.md) | What is recorded, immutably | Law 2 |
| [03 — Revert Semantics](03-revert.md) | What "undo" means and guarantees | Law 3 |
| [04 — Profiles](04-profiles.md) | How systems are composed and named | Law 4 |
| [05 — Manifest & Signing](05-manifest-signing.md) | Provenance and trust | Law 5 |

Binding order: the Five Laws (SOURCE-OF-TRUTH §2) > these documents > the WIT files in
`contracts/` > any implementation. A conflict is resolved upward, never patched
downward.

Amendments to any constitution document require operator approval and a dated entry in
the SOURCE-OF-TRUTH Decision Log. Each document carries its own version; breaking
changes bump the major and require a migration note.
