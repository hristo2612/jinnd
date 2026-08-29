# jinn:introspect 0.2.0

The read-only composition contract (M2-K7; harness finding 19). A status
or health plugin answers `fiber`, `state`, `incarnation`, `unserved`,
`provisions`, `registrations`, and `readiness` from the kernel's own
knowledge instead of probing.

## 0.2.0 (M2-K9, harness finding 31)

Additive: `entry` gains `unserved`. It names what the entry's live
incarnation already owes — `restarting` (a replacement is scheduled),
`gone` (disposal is owed, and disposal is terminal), `suspended`, or
`stalled` (a change nothing will schedule from here: a withdrawn
dependency, an activation not retried against an unchanged environment
per R9, or a terminal fiber) — and is absent when the entry can serve.
That is exactly the window in which a reply-expecting `events.emit`
selecting one of its listeners is refused, with the `kernel-error` case
of the same name.

Four answers rather than a bit because they are four different next
moves: only `restarting` promises a replacement, so only `restarting`
means "retry after it lands", and the kernel answers it only where it can
genuinely schedule one. A caller told to wait on a `gone` or `stalled`
target would wait forever. Callers ASK here instead of discovering the state by
stalling on it; the refusal and this field are read from one snapshot
source, so they never disagree.

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
