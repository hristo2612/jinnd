# jinn:profile 0.2.0

The profile-patch contract (M2-K7; harness finding 21): an operator edit
made through a plugin is operator intent applied by the loader, not a
revertible effect of the editing fiber. Retiring the editor rolls nothing
back. 0.2.0 (M2-K8): the patch answers `accepted(seq)` without awaiting
the restart (finding 26), and `entry(id)` / `document()` give read-only
viewers the document's authority fields (finding 25).

## Grant

Scope type `entry-ids`: `["scheduler", "status"]` names what may be
patched; `["*"]` names every entry, only when written. A bare grant
patches nothing; any other shape refuses the grant on the record.

## Wire

`services.resolve("jinn:profile")`, then `services.call(handle,
"patch-entry", payload)` with payload = u32-LE length + entry id bytes,
then the merge-patch JSON. The answer is one tag byte — `2` accepted,
followed by the `ProfilePatched` record's u64-LE ledger sequence; `1`
refused, followed by the reason. (`0` applied is the 0.1.0 answer a 0.2.0
provider never gives.)

`entry` (payload = u32-LE length + id bytes) answers the entry's
authority fields as JSON — `{ id, package, version, hash, grants, config,
disabled, parent }` — or `null` for an unknown id; `document` (empty
payload) answers `{ "entries": [...] }` for every entry the scope admits.
A read outside the scope is a ledgered grant refusal, an error on the
wire. A viewer holds `ops = ["entry", "document"]` and cannot patch.

## What applies

The merge-patch (RFC 7396) is applied to the entry's `config` subtree;
the result must be an object whose `grants` would admit at activation.
The loader validates, writes the document back atomically, commits the
runtime view, and SCHEDULES exactly the patched fiber's restart
(`ConfigChanged`) — the call returns before the restart runs, so a
settings provider may patch the entry that resolves it from `activate`
without the two-hop nested-dispatch deadlock. The restart's transitions
land on the ledger after `seq`. The daemon treats the write-back as its
own echo.

## Ledger

`ProfilePatched { entry, by }` under the editor's attribution on success;
`AmendmentRefused { detail }` for a refused patch; `GrantRefused {
contract, reason: ScopeMismatch, detail }` for an entry outside the scope —
answered `refused` on the wire like every other refusal. No `EffectRegistered`,
no `EffectWithdrawn`: the document is not in any fiber's journal.
