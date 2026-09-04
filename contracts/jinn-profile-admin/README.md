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
the write, `None` on add — the inverse write's payload. `AmendmentRefused
{ detail }` for every typed refusal; a scope refusal is the broker's
`GrantRefused` with reason `ScopeMismatch`. No fiber journal entry.

## Limits of 0.1.0

- `swap-plugin` and `add-entry` name a package a document-led reconcile
  has already admitted under the entry's pin; a brand-new package is
  refused `malformed` with the Law-5 reason (the card's named
  alternative) until the artifact admission moves behind this seam.
- `swap-plugin` applies the loader's `Replace` step as this kernel
  version has it: dispose then spawn. The M2-K23 (e) replacement
  semantics — journal inherited, window closed to reply-expecting walks
  — are the card's ruling and are reported against this version where
  not yet landed.
- A write lands exactly the target entry's plan step; consequences for
  other entries (a child of an enabled parent) converge at the next
  document-led reconcile (I4).
