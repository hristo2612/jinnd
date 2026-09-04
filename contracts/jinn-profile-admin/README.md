# jinn:profile-admin 0.1.0

The composition-administration contract (M2-K23; harness finding 37; UI
arc KG-1): adding, removing, disabling, re-granting and re-pinning an
entry is a ledgered kernel operation and not a file edit. Constitution 04
§Write-back is confined names it; this is the bundle.

## Authority — the one design decision

A separate `jinn:profile-admin` grant on the CALLING entry. Whoever holds
it is the operator's delegate by the operator's own document. It confines
plugins; it does not authenticate the operator (`jinn:auth` and the
transport do); a same-uid process editing the file is outside every
grant, in `jinn:auth`'s words. Per-call principal propagation from
`jinn:auth` was rejected on the card and is not re-litigated here. No
plugin may administer itself or an ancestor.

## Grant

Scope type `entry-ids`, the `jinn:profile` parser verbatim: `["*"]` only
when written; a bare grant administers nothing. On `add-entry` the scope
must admit the new id and its parent. `ops` attenuates to any subset of
the five.

## Wire

`services.resolve("jinn:profile-admin")`, then `services.call(handle,
<op>, payload)` with payload = u32-LE length-prefixed UTF-8 segments:

| op | segments | plan step |
|---|---|---|
| `add-entry` | the 0.2.0 `entry` record JSON | `Create` / `Track` |
| `remove-entry` | id | `Remove` |
| `set-disabled` | id, `true`/`false` | `Disable` / `Enable` |
| `set-grants` | id, grants JSON | `Restate` (a restart, never live) |
| `swap-plugin` | id, package, version, hash | `Replace` |

Answer: tag `2` accepted + the row's u64-LE sequence; tag `1` refused +
one class byte (`1` unauthorized, `2` malformed, `3` conflict, `4`
irreversible) + the reason.

## Ledger

`ProfileAdministered { entry, by, write, before, after, prior }` under
the caller: `before`/`after` are SHA-256 hex of the rendered document
(`after` equals the file's digest); `prior` is the entry's record before
the write, `None` on add — the inverse write's payload. The row is the
INTENT and lands BEFORE the commit (Law 2: intent recorded, then
applied): `after` names the digest the file WILL have. A commit that then
fails is refused `conflict` on the wire with an `AmendmentRefused` row
naming the intent's sequence (`intent <seq> did not land: …`), so a
recorded intent that did not land says so and an applied write can never
be unrecorded; a `ProfileAdministered` row not followed by such a refusal
is a landed write. Every other typed refusal is an `AmendmentRefused {
detail }` with nothing recorded before it; a scope refusal is the broker's
`GrantRefused` with reason `ScopeMismatch`. No fiber journal entry.

## Settling (R1)

`accepted(seq)` answers once both views committed — the document on disk
and the runtime's record — and the runtime step is STATED: a reload for
`set-grants`, a spawn for `add-entry` / `set-disabled: false`, a disposal
for `remove-entry` / `set-disabled: true` / `swap-plugin`. The disposal's
landing (and, for a swap, the successor's spawn, which waits for the old
incarnation's withdrawal so one entry never has two fibers) runs on a task
of its own with the entry engaged: a second write on that entry meanwhile
is refused `conflict`, retryably. Nothing is awaited inside the caller's
host call.

## Limits of 0.1.0

- `swap-plugin` and `add-entry` name a package a document-led reconcile
  has already admitted under the entry's pin; a brand-new package is
  refused `malformed` with the Law-5 reason (the card's named
  alternative) until the artifact admission moves behind this seam.
- **`swap-plugin` applies the loader's `Replace` step as this kernel
  version has it: dispose then spawn — a STATED LIMIT, carded as
  M2-K27.** The old incarnation rests `Disposed` under `ExplicitDispose`
  (never `Suspend`): its world journal is withdrawn LIFO rather than
  inherited, its listens are released outright rather than entombed, and
  the successor is a new fiber whose activation is not staged, so a
  reply-expecting walk between the two selects nobody (the harness #47
  shape). The M2-K23 (e) ruling — journal inherited, window closed —
  needs the fiber engine to learn a cross-fiber reload (one fiber's
  suspension committed away by another's staged activation; STOP rule
  (e), trigger 1), which is M2-K27's with its loom model. The daemon
  suite pins this limit
  (`a_swap_disposes_the_old_incarnation_a_stated_limit_until_m2_k27`)
  and flips when M2-K27 lands.
- A write lands exactly the target entry's plan step; consequences for
  other entries (a child of an enabled parent) converge at the next
  document-led reconcile (I4).
