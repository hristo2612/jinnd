# jinn:introspect 0.1.0

The read-only composition contract (M2-K7; harness finding 19). A status
or health plugin answers `fiber`, `state`, `incarnation`, `provisions`,
`registrations`, and `readiness` from the kernel's own knowledge instead
of probing.

## Grant

A bare `"jinn:introspect"` grant; no scope type is declared, so a scoped
grant refuses on the record. Nothing is attenuable below "the whole
composition" in v0.1.

## Wire

Over the string-keyed handle lane: `services.resolve("jinn:introspect")`,
then `services.call(handle, "entries", [])` or `services.call(handle,
"readiness", [])`. Answers are UTF-8 JSON of the WIT records, kebab-case
field names.

## Ledger

Every read is one `ContractCall { contract: "jinn:introspect", operation }`
with the caller's entry and fiber attribution. Riding with this bundle
(finding 19): the ledger's `entry` column is filled for every attributable
event, and `GrantRefused` carries the typed refusal `reason` (the closed
`RefusalReason` class: not-granted, scope-mismatch, not-loopback,
unresolvable, foreign-handle) with the prose `detail` beside it.
