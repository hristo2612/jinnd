# jinn:introspect 0.6.0

The read-only composition contract (M2-K7; harness finding 19). A status
or health plugin answers `fiber`, `state`, `incarnation`, `unserved`,
`injects`, `unmet`, `provisions`, `registrations`, and `readiness` from
the kernel's own knowledge instead of probing.

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

## 0.3.0 (M2-K10, harness finding 32)

Additive: the `waits` operation, answering the kernel's live WAIT GRAPH —
one record per crossing a fiber is currently parked on, naming both ends
and what is being waited on.

A crossing whose target is, transitively, already awaiting the caller
would deadlock: a Tier A instance serves one guest entry at a time, so a
fiber parked outbound cannot answer an inbound crossing. The kernel
REFUSES such a crossing immediately, with the `cycle` case of
`kernel-error`, rather than letting both ends run to the guest deadline.
This operation is the same graph that refusal is decided against, so an
operator asking why is told what the kernel actually saw.

Waits are a moment, not a composition: edges exist only while somebody is
parked, so two reads of an unchanged composition legitimately differ.
Nothing is reverted or gated on this answer.

## 0.4.0 (M2-K13, harness findings 40 and 41)

Additive: the contract gains a PUSH side. The kernel publishes every fiber
transition it commits on the reserved topic `jinn:introspect/transitions`,
as the UTF-8 JSON of the new `transition` record. No existing operation
changes shape.

Until now this contract was a pair of PULL operations answered from a
snapshot, so a consumer could only ever see a fiber at REST. Three of the
states `entry.state` itself names — `unloading`, `pending`, `loading` — sit
between two rests, and measured through the real daemon a catalog reading in
a tight loop across a whole restart reached none of them: 189 reads, every
one `active`, while the kernel committed
`active → unloading → pending → loading → active`. A catalog built on the
pull therefore announces transitions it did not witness and cannot time.
This delivery is what it witnesses instead.

### Subscribing

`events.listen("jinn:introspect/transitions", token)` from the plugin
world; deliveries arrive as `lifecycle.handle-event(token, topic, payload)`.
The grant checked is this contract's — the topic is kernel-reserved and
belongs to the contract whose authority bounds its payload. A guest
`events.emit` on that topic is REFUSED on the record: only the kernel
publishes there, so a witnessed transition can never be confused with a
forged one.

### Ordering against the ledger

A delivery never precedes its ledger row. The committing path appends the
transition to the ledger's ordered lane and only then hands it to the
publisher, which reads the ledger's high-water mark THROUGH the single
writer before it delivers anything — a read that answers only once every
append sent before it has committed. `committed-by` carries that mark, so
the guarantee is checkable by the listener rather than merely asserted:
the transition's own row sits at or before it. Law 2 holds at the moment it
matters most — model-visible means logged, in that order.

### Back-pressure

The kernel never waits on a listener; the hand-off is a bounded push that
cannot block. A listener slow enough to fill the bound loses transitions,
and the loss is counted TWICE: as a `PublishDropped` ledger event carrying
the count, and as a gap in the `ordinal` that listener receives. Nothing is
ever reordered — deliveries follow the order the kernel committed. The
alternatives were an unbounded queue (the kernel's memory hostage to a
plugin) or a blocking hand-off (a new deadlock surface on top of the
unretired one); losing loudly is the only honest third answer.

### Late join and replay

There is no replay, and a late joiner is told so rather than left to assume
otherwise. `ordinal` counts every transition this kernel process has
published, so a first delivery above 1 states exactly how many preceded the
subscription, and every later gap states exactly how many were lost. A
listener holding `jinn:ledger` recovers them from the stream itself.

### Authority: the demonstration, and where it failed

Required by the card, stated either way. Every delivered field is one this
contract's own pull already admits: `entry`, `fiber` and `incarnation` are
`entry`'s own fields, and `from`/`to` are values of `entry.state`'s
vocabulary — no new subject and no new field enters a holder's reach, and
the grant is already whole-composition and unattenuable, so no fiber
becomes visible that `entries` did not already list.

What the push DOES add is timing fidelity: a listener reaches the transient
readings a poller cannot. That is the card's purpose, and it is a widening
of resolution, not of scope.

The demonstration FAILED for one field. The kernel's `cause` for a
transition — why it happened — has no counterpart anywhere in this
contract, so delivering it would widen the grant. Rather than widen it,
`cause` is not delivered. A consumer that needs it holds `jinn:ledger`,
which already admits the whole `FiberTransition` row verbatim, and reads it
there.

## 0.5.0 (M2-K16)

Re-issued, not additive. 0.4.0 never parsed: `from` is a WIT keyword, and
`record readiness` shared one interface namespace with the `readiness`
operation. Nothing had noticed because no consumer bindgens this file (only
`wit/plugin.wit` is) and the daemon mirrors the shapes by hand — the first
parser to read it was the contract lens. 0.5.0 is the first parseable
edition: the record is now `readiness-report` and the field is spelled
`%from` (its name is still `from`). The wire operation `readiness`, its
JSON answer, and every other operation and record are unchanged, so a
holder of 0.4.0 needs no change; the version moves because a rename at an
unchanged version is not additive (R12).

## 0.6.0 (M2-K24, harness findings 7, 45 and 46)

Additive: `entry` gains `injects` and `unmet`. A wasm entry may declare,
beside its grants, the string-lane contracts it injects at activation
(`config.injects`, constitution 04 §Format). The kernel then keeps §3's
promises on the string lane exactly as on the typed one: the entry's fiber
rests `pending` until every declared provider is `active` — a provision
landing while the provider is still `loading` is not readiness — reloads
under `DependencyChanged` when a declared provider is replaced, and
re-arms from `failed` when one moves (and never before; R9). `injects`
reports that declaration as the document of record states it, in order —
a disabled entry's included, since it is the document's fact and not a
live gate's; `unmet` names which declared contracts the entry's gate
currently finds unmet (empty for an entry that is not seated), so an
operator reading a `pending` entry learns WHY from the record instead of
inferring it — the K9/K10 precedent that a refusal is observable (Law 2).

Two entries that declare each other rest `pending` for the daemon's life
and each shows the other's contract in `unmet`: the recorded limit (the
string lane has no static cycle chart, because a wasm entry declares what
it injects, not what it provides), cleanly inactive per I3. A declared
contract the entry holds no grant for is a per-entry fault refused on the
record at admission — the entry rests `failed`, never `pending` on it —
and an entry that declares nothing reports two empty lists and behaves
exactly as before.

No existing operation changes shape; the `transition` delivery is
unchanged.

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

A publish that reaches at least one listener lands one `DispatchTrace` for
the reserved topic, attributed to no fiber and to emitter `0` — the kernel
itself (0.4.0). With no listener nothing is delivered and nothing is
logged: no model-visible thing happened. Losses land as `PublishDropped`.

Every read is one `ContractCall { contract: "jinn:introspect", operation }`
with the caller's entry and fiber attribution. Riding with this bundle
(finding 19): the ledger's `entry` column is filled for every attributable
event, and `GrantRefused` carries the typed refusal `reason` (the closed
`RefusalReason` class: not-granted, scope-mismatch, not-loopback,
unresolvable, foreign-handle) with the prose `detail` beside it.
