# jinn:profile 0.1.0

The profile-patch contract (M2-K7; harness finding 21): an operator edit
made through a plugin is operator intent applied by the loader, not a
revertible effect of the editing fiber. Retiring the editor rolls nothing
back.

## Grant

Scope type `entry-ids`: `["scheduler", "status"]` names what may be
patched; `["*"]` names every entry, only when written. A bare grant
patches nothing; any other shape refuses the grant on the record.

## Wire

`services.resolve("jinn:profile")`, then `services.call(handle,
"patch-entry", payload)` with payload = u32-LE length + entry id bytes,
then the merge-patch JSON. The answer is one tag byte — `0` applied, `1`
refused — followed by the refusal reason.

## What applies

The merge-patch (RFC 7396) is applied to the entry's `config` subtree;
the result must be an object whose `grants` would admit at activation.
The loader validates, writes the document back atomically, commits the
runtime view, and restarts exactly the patched fiber (`ConfigChanged`).
The daemon treats the write-back as its own echo.

## Ledger

`ProfilePatched { entry, by }` under the editor's attribution on success;
`AmendmentRefused { detail }` for a refused patch; `GrantRefused {
contract, reason: ScopeMismatch, detail }` for an entry outside the scope —
answered `refused` on the wire like every other refusal. No `EffectRegistered`,
no `EffectWithdrawn`: the document is not in any fiber's journal.
