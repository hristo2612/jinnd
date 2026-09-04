# The Jinn Kernel — Source of Truth

**Status:** LAW. This is the canonical copy; private mirrors point here. Every design decision, every delegation brief, every review of kernel
work checks against this file. If work contradicts this file, the work is wrong or this
file gets amended first — never both drifting silently.

**Amendment rule:** changes to Laws (§2) and Invariants (§4) require Hristo's explicit
approval. Rules (§5) and Roadmap (§7) may be amended by the COO with a dated entry in the
Decision Log (§9). Nothing is deleted from the log.

---

## 1. North Star

One small Rust kernel that makes a machine **legible, reversible, and safe for an agent
to operate and reshape**. Everything above it — engines, todos, workflows, connectors,
UI, and one day the desktop and the phone — is a plugin behind a typed contract. A
product is a profile (a named plugin tree), not a codebase. The kernel is the landlord:
it never forgets what any tenant did, and it can evict anyone without leaving a mark.

The kernel implements the **spatiotemporal composability paradigm** (Cordis / the
Shi-Zhang-Cui paper), reimplemented natively in Rust — not a binding to, port of, or
dependency on any existing implementation.

Working name: **`jinnd`**.

## 2. The Five Laws (the constitution)

These are permanent. They hold for our own plugins exactly as for anyone else's.

1. **Everything is a plugin behind a typed capability contract.** No side doors, not
   even for us. Our plugins are never special, only pre-installed.
2. **Model-visible means logged.** Anything an agent can see or do passes through the
   kernel and lands in the append-only ledger. No off-ledger channels.
3. **Every effect is revertible, or explicitly declared irreversible at the contract
   level.** Revert is a kernel property, not a plugin courtesy.
4. **A device is a profile.** Company, personal, desktop, phone — same kernel, different
   named plugin tree. Placement (small brain / big brain) is config, not architecture.
5. **Plugins are sandboxed and signed.** Provenance is law. Machine-written code gets
   structural containment, not trust.

Opinions live in contracts and safety — never in content. Default plugins are heavily
opinionated in taste and hold zero privilege.

## 3. The Paradigm (what the kernel implements)

- **Context** — a cheap, layered view over shared state. Carries the isolation map
  (which realm each service name resolves in) and the intercept chain (per-subtree
  config overlays). Deriving a child context is O(1).
- **Fiber** — one instantiation of one plugin: its lifecycle cell. States:
  `Pending / Loading / Active / Failed / Unloading / Disposed`. A plugin body runs once
  per fiber.
- **Reversible effects** — the single mutation primitive. Every side effect registers
  its undo at the point of action; teardown replays undos in reverse (LIFO). Child
  plugin registration is itself an effect on the parent fiber, so disposal cascades
  structurally.
- **Services (coeffects)** — plugins declare what they provide and what they inject.
  A fiber activates only when every injected service's provider is Active and passes its
  check. Availability is managed reactively by the kernel, never polled by plugins.
- **Epoch gating** — a fiber's epoch encodes the identity (generation) of every
  provider it depends on. Any provider change forces consumers through a full clean
  unload → reload. There is no silent replace, ever.
- **Events** — one bus, five dispatch modes (emit / parallel / serial / bail /
  waterfall), typed payloads, inverted routing: the payload's filter selects listeners
  by interrogating each listener's context. Listener registration is just an effect.
- **Profiles & loader** — a config document is an entry tree; the loader reconciles by
  id: edit the file, only affected fibers restart. Persistence is bidirectional:
  runtime changes write back atomically. The running system and the config file are two
  views of one truth.

## 4. The Four Invariants (theorem-backed, test-enforced)

From the paper. These are the kernel's acceptance criteria — encoded as integration
tests in `tests/invariants/` that gate every merge. An optimization that breaks one of
these is not an optimization.

- **I1 — Recovery exactness.** Removing a plugin withdraws exactly its contribution and
  nothing else, whatever its history (including failure mid-load).
- **I2 — Ordering.** A provider outlives its consumers' teardown: dependents finish
  unloading, and can still call the dying service while doing so, before the provider's
  value disappears.
- **I3 — Progress.** With an acyclic dependency precedence, the system never deadlocks
  and always reaches quiescence. Dependency cycles leave the involved plugins cleanly
  inactive (and are detected statically).
- **I4 — Confluence.** After any history of loads, unloads, crashes, hot-swaps, and
  config edits, the quiescent state is indistinguishable from a fresh boot of the final
  configuration. The mess leaves no trace.

Note: "recovery" means indistinguishable under each key's published operations
(observational equivalence), never bit-identical. Every service contract declares its
own equality semantics.

## 5. Design Rules

Hard-won doctrine from the audits (TS Cordis v4, the paper, and the cordis-rs
cautionary tale). Numbered so briefs and reviews can cite them.

**R1 — Async-first is law.** The lifecycle engine is built on tokio from line one:
per-fiber supervisor tasks, `watch` channels for state and availability, cooperative
cancellation tokens for in-flight setup, single-flight inertia loops per fiber (a
launched transition always lands, then reconciles to the latest desired state). No
blocking executors, no lock held across plugin code, ever. *(cordis-rs collapsed
exactly this and is disqualified by it — R1 exists so our agents can't repeat that.)*

**R2 — Port the temporal semantics exactly, tests first.** The TS v4 spec suite
(especially the inertia-lock trio pinning mid-flight dependency swaps) is ported to
Rust integration tests **before** the code that passes them. AI agents demonstrably
nail structure and silently simplify temporal subtlety; the tests are the guard.

**R3 — Typed, not stringly.** Services resolve by type (TypeId + realm) with a
string-keyed lane only for dynamically loaded plugins. Events are typed with their
dispatch mode declared. Config is serde-typed at the contract boundary. No
`Arc<dyn Any>` as the primary API surface.

**R4 — Handles, not magic.** No caller-attribution proxies. A resolved service is a
scope-carrying handle pairing the implementation with the caller's context; effects are
charged to the caller by construction. Proc-macros (`#[derive(Plugin)]`, `#[inject]`)
generate owned per-activation dependency snapshots — plain field access at runtime.

**R5 — One mutation primitive.** All context mutation flows through the effect
primitive. If a plugin can change shared state without registering an undo, the kernel
has a hole, not a feature. Effects carry labels and children: the live effect tree is
free introspection.

**R6 — The ledger sits at the capability boundary.** Every contract call crosses the
kernel; the kernel appends it to the ledger (SQLite, append-only). Revert is built on
the ledger + effect inverses. Errors, transitions, write-backs, and provenance are
ledger events, not a `last_error` string.

**R7 — One contract, tiered containment.** Every plugin runs behind the same typed
capability contract (WIT); what differs per plugin is its **containment tier**:

- **Tier A — WASM components** (wasmtime; the default, and the only live tier in
  v0.1). Logic and orchestration plugins and *all machine-written code*,
  first-party included. Provable confinement, instant dispose, one portable
  signed artifact.
- **Tier B — sandboxed native processes** (`Subprocess`: supervised process over
  IPC; any language). For native-heavy and data-plane plugins. Disabled until its
  mandatory per-OS sandbox exists (macOS Seatbelt; Linux namespaces + seccomp +
  Landlock). An unsandboxed subprocess tier never ships.
- **Tier C — GUI environments** (rung 3, post-M4). Tier B processes whose
  capability grants include the display protocol: a compositor plugin holds the
  hardware capability and mediates surfaces, input, and GPU (the Wayland/Fuchsia
  pattern).

The **capability broker is transport-agnostic**: grant check, ledger append, and
dispatch accept "a contract call from a peer" — whether the peer is a linked WASM
instance or a socket-connected sandboxed process is a transport detail. A broker
fused to WASM linking is a design defect (binding on the wasm-host packet).
Ledger granularity for Tier B/C is grants + lifecycle + contract-level effects,
never data-plane traffic — per-frame mediation is rejected as a scaling hazard.

**There is no in-process plugin host**: native Rust is kernel implementation, never
a plugin — it implements only the broker/runtime and the base host-provider
contracts (fs, process, net, keystore), exposed to plugins solely as contracts.
Capability grants, metering, and signing are enforced per-tier by the kernel.
Native dylib loading is banned. *(Amended 2026-08-24 from "three modes incl.
InProc" — verifier round 2: lint discipline is not mechanical closure; an InProc
plugin tier is a Law-1 side door. Amended 2026-08-25: containment tier model
codified — see Decision Log.)*

**R8 — Hot reload has three honest modes.** Mode 0: config reconcile (most operator
value, always available). Mode 1: WASM instance swap — old instance stays warm until
the new one is healthy, auto-rollback on failure, optional state-handoff blob from old
to new. Mode 2: supervised kernel restart with state in the ledger, not process memory.
No in-process native code patching, ever. (Renamed tiers→modes 2026-08-25 so "tier"
means containment tier, R7, unambiguously.)

**R9 — Known hazards stay dead.** Do not port: emit aborting remaining listeners on
first error; async results counting as "bailed"; side-effectful service constructors;
config eval with ambient authority (any expression language in config is a closed,
side-effect-free subset). Do not invent: unload of native libraries; silent service
replacement; auto-retry of failed fibers against an unchanged environment; unbounded
self-registration (a plugin registering instances of itself without bound — the
runaway class the progress theorem's finiteness assumption excludes, and precisely
the failure mode of machine-written plugins; added 2026-08-25).

**R10 — Small and boring.** Kernel core budget ~5k LOC (hard ceiling 8k), wasmtime host
~3k. The kernel has no features — anything that can be a plugin is a plugin. When in
doubt, the code goes above the line, not below it. Metric note (2026-08-25): the
kernel-core count excludes the conformance-harness lane (`jinnd-api` facade +
`jinnd-adapter`), loom-only models, and cfg(test) code — the ceiling exists to keep
the kernel small, never to incentivize golfing containment or honesty code.
A packet card's restatement of the meter NEVER narrows this exclusion list
(promoted 2026-08-30 after M2-K10 restated it as "files under tests/ or named
tests.rs" and a correct loom-only model was counted against the ceiling): a card
may set the ceiling number, the law sets what is counted.
Per-file cap note (2026-08-29, promoted from the M2-K8 card so it stops being
re-litigated card by card): the 300-line per-file cap is **hard for `src/`** and
**soft for coherent test suites** — a test file over the cap is split where a natural
seam exists and is reported as a MINOR, never a Blocker on line count alone. It is
still a required fix: "soft" means the severity is lower, not that the split is
optional. A test file with no natural seam stays whole and is named in the round's
report.

**R11 — Failure is local.** A failing plugin deactivates itself and its dependents,
cleanly, and touches nothing else. Panics never cross the kernel boundary. Sibling
fibers never observe another's crash except through declared dependencies.

**R12 — The contracts outlive everything.** WIT interface files + prose law are the
product. Design every contract as if it will outlive Arch, Android, and this year's
kernel implementation. Contract changes are versioned, reviewed, and never breaking
within a major version.

## 6. Architecture (high overview)

```
┌────────────────────────────────────────────────────────────┐
│ surfaces & habits (plugins): UI, todos, workflows, dreams  │
├────────────────────────────────────────────────────────────┤
│ services (plugins): engines, brain, sync, registry, cron   │
├────────────────────────────────────────────────────────────┤
│ providers (plugins): fs, net, browser, slack, os-specific  │
├════════════ typed capability contracts (WIT) ══════════════┤
│ KERNEL (jinnd): fiber runtime · effect/undo engine ·       │
│ service registry & epochs · event bus · profile loader ·   │
│ ledger + revert · transport-agnostic capability broker ·   │
│ base host providers (fs/process/net/keystore, exposed as   │
│ contracts) · plugin hosts: Tier A WASM | Tier B sandboxed  │
│ process (R7) · HTTP/WS API (axum)                          │
├────────────────────────────────────────────────────────────┤
│ host OS: macOS launchd · Linux systemd · Android service   │
└────────────────────────────────────────────────────────────┘
```

- Every plugin above the contract line runs in a containment tier per R7: WASM by
  default (Tier A); sandboxed native processes for native-heavy work (Tier B);
  GUI environments as Tier B processes with display-protocol grants (Tier C,
  post-M4). One contract surface, one broker, swappable transports.
- The existing React web UI becomes a client of the kernel's API plugin — no UI rewrite
  required for parity.
- The old Node gateway keeps running ALL production until the new kernel proves parity;
  instances cut over one at a time. (The cutover rule — non-negotiable.)
- Component budgets: context tree 0.5–0.7k · fiber lifecycle 0.8–1.2k · effects
  0.3–0.5k · registry+gating 0.5–0.7k · event bus 0.4–0.6k · loader 1–1.5k ·
  proc-macros 0.3–0.6k · tracing bridge 0.15–0.3k.

## 7. Roadmap (each milestone gated by a demo, not a claim)

- **M0 — Constitution v0.1.** The five crown-jewel documents in `docs/constitution/`:
  capability contract format (WIT), ledger invariant, revert semantics, profile format,
  plugin manifest/signing. Small enough to review in one sitting. *Acceptance: Hristo
  reads and approves it in one pass.*
- **M1 — Kernel spike.** `jinnd` on the host machine beside production, ported test suite
  green. *Acceptance demo: three toy plugins from a profile file; edit config → one
  restarts, siblings untouched; hot-swap one WASM plugin with rollback; dispose one and
  watch the ledger show exactly what was undone; revert works.* Packet 0 of M1 is the
  test port — the suite lands red before any kernel code.
- **M2 — Dogfood slice, widened to a core port (amended 2026-08-30, see Decision Log).**
  Originally: one real capability (cron or todos) rebuilt as plugins, running for real.
  Hristo redirected on 2026-08-28 to port the gateway's core pieces FIRST — API,
  settings, engines, sessions, todos, workflows, plugins UX — each as a seam-triple whose
  provider is swappable by profile edit, "so we can switch and extend any piece".
  *Acceptance, BOTH halves required and neither retired:* (a) every named seam proven by
  the real-composition suite against the pinned daemon, provider swappable by profile
  alone; and (b) **the original bar, unchanged — a slice does its production job for a
  week without touching the old gateway's data.** (b) was carried by the cron soak
  (PLA-297). Widening the port does not lower the bar for calling M2 done; it only
  changes what is built before the week is run.
  **ACCEPTED 2026-09-04 (the COO's decision on the criterion above; Decision Log
  entry of the same date).** (a) was met on 2026-09-01. (b) was paid by the cron soak
  on its own data root beside the old gateway, which kept all production throughout,
  and audited on 2026-09-04 (PLA-297; the full audit is on the Todo): fires 653
  expected / 653 recorded / 0 missing over the 163-hour window, the only two
  non-fires being day-1 boot catch-ups of the FINDINGS #13 shape; duty 7 d 01 h 20 m
  over a 7 d 01 h 54 m span with 33 m 51 s of gaps, every gap accounted for (a 24 m
  host reboot, the unproven 08-29 SIGTERM at 8 m, supervised bumps under a minute
  each); the ledger flat at exactly 2,112 rows/day (96 wakes × 22 rows) for three
  consecutive days; RSS 9.8 MB after 3 d 14 h at the audit; undo retention growing
  2.6 MB/day. **Caveat, stated as the audit stated it:** no single pin carried the
  week. The soak ran pin `3a8e5c03` (M2-K9) throughout while the harness shipped
  `a53a352` and then `b1dbe8f` under it, so the week proves the SLICE — which is
  what the criterion asks — and not the shipping kernel. Mid-week bumps were
  anticipated as soak events; that they were never made is the defect, and the
  soak rule below exists so it cannot recur. Recorded by kernel-dev on the PLA-297
  dispatch in the form the M2-K24 landing used; the operator's veto window is open,
  as it was for M1.
  **Soak rule (law, 2026-09-04): the soak tracks the harness pin.** A pin-bump land
  is not complete until the soak has been bumped, supervised, to the new pin and
  its `jinnd.build` records it. Until then the pin-bump is a landing in progress,
  the harness and the soak are on different kernels, and no duty the soak accrues
  counts for the pin the harness ships. The bump is a soak event, logged in the
  soak's duty record like any restart, so the next audit can read duty per pin.
- **M3 — Profiles & parity.** The `company` profile takes over slices of production
  one at a time; web UI runs against the kernel API. *Acceptance: an instance runs a
  full day on jinnd alone.*
  **Two structural blockers, discovered by the M2 port and recorded 2026-09-01 (see
  Decision Log). Both CLOSED 2026-09-02 — kept here as the record of what M3 needed
  and where each was delivered. A THIRD, discovered by the harness UI packet and
  recorded 2026-09-02, LANDED 2026-09-03 (item 3). A FOURTH and a FIFTH, discovered
  by the harness extension-tier packet and recorded 2026-09-03, LANDED 2026-09-03
  and 2026-09-04 (items 4 and 5; the CLOSED markings are the COO's to confirm). A
  SIXTH, discovered by the harness plugins seam (FINDINGS #37) and carded from the
  UI arc's KG-1, is OPEN (item 6).**
  1. **No outbound anything.** `jinn:net` v0.1 has no `request`, no TLS and no
     non-loopback listen, at every pin so far. Slack, Stripe, Linear, GitHub, the
     vendor engine APIs and every webhook are therefore *structurally impossible* in
     the harness today. An instance cannot run "a full day on jinnd alone" while it
     cannot reach anything, so M3 either gets an outbound capability first or its
     acceptance means a day of purely local work — and those are different milestones.
     **Closed 2026-09-02:** M2-K14 (outbound `request`/`send-request` behind the
     profile allowlist, declared irreversible, PLA-336) and M2-K15 (rustls TLS with no
     off switch, typed `untrusted` refusal, PLA-341); harness pin-bump 6 (PLA-344)
     adopted them at world 0.10.0 / `jinn:net` 0.3.0.
  2. **No authentication or authorization, anywhere.** Loopback plus the port scope
     the `jinn:net` grant carries is the entire boundary; anything on the machine that
     reaches the port is an operator. Acceptable for a soak beside production,
     unacceptable for a profile that holds production. Hristo delegated the shape to the
     COO on 2026-09-01. **Closed 2026-09-02, across the pin:** M2-K21 supplies the
     AUTHORITY (`jinn:auth@0.1.0` — one `verify`, deny by default, launcher-owned
     credential re-read per call, every decision an `AuthDecided` row, no bypass;
     PLA-342) and harness 2.8 supplies the DOOR (`jinn-api-http` verifies the bearer
     credential once per request before any dispatch, typed 401, provisioning at boot;
     PLA-343). One operator, one credential; no accounts, roles or delegation — the
     same-uid limit is stated in the contract, not papered over.
  3. **No readiness gating between plugins on the lane plugins actually use.** §3
     promises that a fiber activates only when every injected provider is Active and
     that a provider change forces its consumers through unload → reload. The kernel
     keeps both promises on the typed lane and neither on the R3 string lane — the
     only lane a Tier A wasm plugin has, so in production it is THE lane. A wasm
     entry that injects a sibling's contract at activation therefore boots on a coin
     toss (sibling order is unspecified), rests `Failed` for the daemon's life when it
     loses, and is never restarted when the sibling is replaced. Harness FINDINGS #7
     predicted it; UI-1 (PLA-349) hit it in production shape at pin `85d36b4`, four
     boots out of five, with a transcript (FINDINGS #45, #46), and the provider's own
     "provided" announcement cannot repair it — the kernel refuses that emit as the
     wait cycle it is (M2-K10). An instance whose UI transport boots on a coin toss
     cannot run "a full day on jinnd alone"; the harness workaround (a
     `jinn:introspect/transitions` subscription and a re-probe in every
     activation-time consumer) is a plugin polling the kernel's lifecycle by another
     name and stands only until the kernel makes it unnecessary. **OPEN — carded as
     M2-K24 (`docs/packets/M2-K24.md`, PLA-350):** a per-entry `injects` declaration
     for wasm entries carrying the typed lane's semantics to the string lane —
     activate only once every declared provider is Active; unload → reload when one
     is replaced (epoch gating, R9); re-arm rather than rest `Failed` when one lands
     later (ruling 2, 2026-08-25: retry only against a CHANGED environment, and the
     provider landing is the change). Undeclared entries are unchanged. Harness
     pin-bump 7 adopts it and retires the workaround. Sequenced before M2-K23.
     **Landed 2026-09-03:** M2-K24 merged (implementation at `e263fd8`, the
     verifier's invariants lane at `a53a352`); harness pin-bump 7 adopted it and
     retired the workaround. Recorded by kernel-dev on the PLA-355 dispatch; the
     item's CLOSED marking is the COO's to confirm.
  4. **A listener's delivery spends the EMITTER's clock, and an instance the kernel
     ends on a deadline leaves its fiber `Active` on the record.** At every pin so
     far a guest call is one `settle(deadline)` and `events.emit` awaits every
     delivery INSIDE the emitter's call, so a listener that never returns kills the
     emitter's instance at the 5 s guest deadline — the transport, in the harness's
     case — and the kernel records no transition for either: the dead instance's
     fiber rests `Active`, its `jinn:net` listener keeps accepting for an instance
     that cannot answer, and the operator API is down until the daemon restarts
     (harness FINDINGS #48, UI-2 proof 7 at pin `a53a352`, with a transcript). R11
     is broken in both halves — the wrong fiber died, and no fiber was recorded
     failing — and an instance whose operator API a single looping extension can
     take down for good cannot run "a full day on jinnd alone" once extensions are
     machine-written (§2, Law 5). **OPEN — carded as M2-K25
     (`docs/packets/M2-K25.md`, PLA-355):** the emitter's clock is paused for the
     whole walk and every delivery is bounded on the LISTENER's side — by a
     per-delivery fuel budget declared at `listen` (deterministic, R9) or by the
     guest deadline — and any instance the kernel ends after activation fails its
     OWN fiber on the record (`Failed`, its own error row, its kernel registrations
     released), resting there per R9 until a declared input moves (M2-K24 (c)).
     Sequenced first of the two cards from UI-2 round 1, before M2-K26 and M2-K23.
     **Landed 2026-09-03:** M2-K25 merged (`b1dbe8f` — the implementation and the
     verifier's `invariants/m2-k25` lane, PR #21); harness pin-bump 8 adopted it and
     FINDINGS #48 closed. Recorded by kernel-dev on the PLA-348 dispatch; the item's
     CLOSED marking is the COO's to confirm.
  5. **A listener's config restart withdraws its listen BEFORE the replacement
     commits, so a reply-expecting walk inside the window selects nobody and
     answers the payload UNMODIFIED.** A `ConfigChanged` restart suspends the
     listener's seat — its `listen` is withdrawn at the start of the window — and
     the replacement's `listen` lands only when its activation reaches that call;
     between the two the topic has no registration, M2-K9's `restarting` refusal
     has no selected listener to key on, and a `before-*` waterfall "succeeds" with
     the payload untouched: 53 sends unvalidated across one source edit, a
     1,492 ms window, not one `503` (harness FINDINGS #47, UI-2 proof 5 at pin
     `a53a352`, with a transcript; Blocker-class for every walk that means
     "validate before you act"). The Mode-1 swap already commits its listener
     replacement atomically under one lock (R8); the config restart — Mode 0 — has
     no commit, so the kernel breaks its own `events.emit` text for the window. A
     second half, found by the card's reading: M2-K9's oracle answers `stalled`
     for a replacement whose load is IN FLIGHT. And the sibling authority gap
     FINDINGS #49: `events.emit` is not covered by the topic's grant while
     `listen` is (constitution 01 §Grants). **OPEN — carded as M2-K26
     (`docs/packets/M2-K26.md`, PLA-355):** a listen registration outlives its
     instance's suspension as a selectable, refusing TOMBSTONE until the
     replacement commits atomically in the Mode-1 shape (`rebind`), tombstones
     live exactly as long as the fiber owes a transition (I4), the oracle answers
     `restarting` for an in-flight load at the one source, and #49 rides with it
     (`emit` covered by the topic's grant, with the reasons stated on the card).
     Sequenced after M2-K25 and before M2-K23.
     **Landed 2026-09-04:** the M2-K26 implementation merged (`655f07e`, PLA-360)
     and the verifier's `invariants/m2-k26` lane merged after it (`138fdce`, PR
     #23; PLA-360 closed); harness pin-bump 9 (PLA-364) adopts `138fdce` and is in
     flight at the M2 acceptance — it flips UI-2 proof 5 and closes FINDINGS #47
     and #49. Recorded by kernel-dev on the PLA-348 and PLA-297 dispatches; the
     item's CLOSED marking is the COO's to confirm.
  6. **The composition's SHAPE is reachable through no kernel contract: adding,
     removing, disabling, re-granting or re-pinning an entry is a file edit.**
     `jinn:profile.patch-entry` (M2-K7/K8) writes ONE entry's `config` subtree;
     `package`, `hash`, `disabled`, `parent` and presence are its siblings, so
     through the surface a person or an agent actually uses an operator can change
     what a plugin is CONFIGURED with and never what a plugin IS, whether it is
     present, or whether it runs (harness FINDINGS #37, with a transcript at pin
     `3a8e5c0`; UI arc KG-1). Constitution 04 names the capability — *"grants,
     identity, nesting … require a separate operator-authorized control capability
     (`jinn:profile-admin`)"* — and no such contract exists at any pin; the loader
     already plans every one of those moves (`Create`/`Remove`/`Disable`/`Enable`/
     `Replace`) and can only be reached by editing the file behind the daemon. An
     instance whose composition only a text editor can reshape cannot run "a full
     day on jinnd alone" once an agent operates it (§1), and "install an extension
     from the UI" — UI-7's headline — is impossible without it. A second half,
     found by the card's reading of `655f07e`: `patch-entry`'s validator checks
     that a patched `config.grants` would ADMIT and not that it is UNCHANGED, so a
     grants WIDENING is reachable through the one contract 04 says may never carry
     one — a Law-1 side door, to be confirmed by the packet's first red test. And a
     third: the loader's document-led `Replace` step DISPOSES the old fiber (world
     journal withdrawn, listens released, no tombstone) where the M2-K4 ruling
     says an incarnation replacement hands the successor the entry's journal, so a
     provider swap loses the entry's contribution and reopens the FINDINGS #47
     window for the one swap every seam proves. **OPEN — carded as M2-K23
     (`docs/packets/M2-K23.md`, PLA-348):** a `jinn:profile-admin` contract of
     five writes (add, remove, `disabled`, grants, plugin identity), authorized by
     a separate grant on the calling entry (the R12-small answer; per-call
     principal propagation from `jinn:auth` rejected with reasons, the same-uid
     limit stated in the contract), each an operator intent applied by
     reconcile-by-id (the M2-K7 precedent), each a `ProfileAdministered` row with
     the caller entry and the before/after document digest, each reversible by the
     inverse write the row records, refused typed for a missing grant, a malformed
     record or an unrecordable inverse, no bypass; `patch-entry` closes to grants
     changes; a plugin-identity swap becomes a replacement of the same entry with
     M2-K26's window semantics, or STOPs with the fiber-engine cost priced.
     Sequenced third, after M2-K25 and M2-K26, on the kernel lane after the
     2026-09-04 audit; harness pin-bump 10 adopts it and flips the plugins page's
     five disabled pills. **Carded and approved 2026-09-04:** the card at
     `fa70399` (`aa1b89a` plus the COO's rulings folded in, PLA-348) is approved
     for dispatch after pin-bump 9 lands; nothing of it is implemented yet.
  **State of the six at M2's acceptance (2026-09-04):** 1 CLOSED (M2-K14/K15,
  pin-bump 6); 2 CLOSED (M2-K21 plus harness 2.8, PR #20); 3 LANDED (M2-K24
  `a53a352`, pin-bump 7); 4 LANDED (M2-K25 `b1dbe8f`, pin-bump 8); 5 LANDED
  (M2-K26 `655f07e` plus lane `138fdce`, pin-bump 9 in flight); 6 OPEN, carded
  (M2-K23 `fa70399`, dispatch after pin-bump 9). The CLOSED markings on 3–5 remain
  the COO's to confirm.
  The M2 port delivered the SHAPES faithfully; the two capabilities that separate a
  faithful shape from a thing that can run a business followed as seven kernel packets
  and two harness packets in the two days after, and the third blocker surfaced the
  moment a real UI was composed on the result; the fourth and fifth surfaced the
  moment a machine-written extension was composed on top of that UI. M3 planning
  starts from an instance that can reach out and can refuse a stranger, whose plugins
  can depend on each other without a coin toss, and waits on one whose walks are
  bounded by the listener that spends them and whose restarts are closed to
  unvalidated traffic — and whose composition an agent can reshape through a
  contract, on the record, and not through a text editor. §7(b)'s week of duty was
  paid and audited on 2026-09-04; M2 is accepted with the pin-drift caveat stated
  under it, and the soak now follows the harness pin.
- **M4 — Cutover.** Old gateway retired per the cutover rule. jinnOS work (desktop
  session, mobile ROM) begins only after M4 — as bigger profiles, not new projects.

Ordering rule: never touch a layer until the layer above it is already useful.

## 8. Non-goals

- No TS/Node in the kernel. No dependency on cordis (TS) or cordis-rs (reference only).
- No in-process native plugin loading (dylibs). No function-level live patching.
- No org-chart metaphors in the kernel. Company semantics are a profile's business.
- No feature work in the kernel that a plugin could own. No second UI framework.
- No big-bang cutover. No production risk before M4.

## 9. Decision Log

- **2026-09-04** — **M2 ACCEPTED on the §7(b) soak audit; the soak now follows the
  harness pin (COO decision, option (c) of three; recorded by kernel-dev on the
  PLA-297 dispatch; operator veto window open).** The 2026-09-04 audit (cron
  `jinnd-program-watch`; the full audit is a comment on PLA-297) passed every
  criterion the Todo states: 653 fires expected, 653 recorded, 0 missing, the two
  day-1 catch-ups of the FINDINGS #13 shape; 7 d 01 h 20 m of duty over a
  7 d 01 h 54 m span, 33 m 51 s of gaps each attributed (host reboot 24 m, the
  unproven 08-29 SIGTERM 8 m, supervised bumps under a minute); the ledger at
  exactly 2,112 rows/day for three days; RSS 9.8 MB at 3 d 14 h; undo retention
  2.6 MB/day. One defect, and it is named here rather than smoothed: the soak ran
  pin `3a8e5c03` (M2-K9) for the whole week while the harness shipped `a53a352`
  and then `b1dbe8f`, so no single pin carried the week — the week proves the
  slice, not the shipping kernel. Of the three options the COO weighed, (c) was
  chosen: accept on the criterion as written — "the slice does its production job
  for a week" — because mid-week bumps were anticipated as soak events when the
  soak was planned, the slice never stopped doing its job across them, and the
  defect is procedural (the bumps were not made), not evidential. The procedural
  defect is closed by LAW rather than by a rerun: §7 gains the
  soak rule — the soak tracks the harness pin; a pin-bump land is not complete
  until the soak has been bumped, supervised, to the new pin and its
  `jinnd.build` records it. First application: the soak is bumped to `138fdce`
  once pin-bump 9 (PLA-364) lands, as a separate task. The M1 acceptance's form
  is followed (dated, numbered, caveated, veto window open); §7 records the six
  M3 blockers' state at the moment of acceptance. The cutover rule is untouched —
  the old gateway keeps all production until M4.

- **2026-09-04** — **A sixth M3 structural blocker is named in §7: the
  composition's shape is reachable through no kernel contract (roadmap amendment,
  recorded by kernel-dev on the COO's dispatch, PLA-348; COO veto window open).**
  Found by the harness plugins seam at pin `3a8e5c0` with a transcript (FINDINGS
  #37) and carded from the UI malleability arc's KG-1: `jinn:profile.patch-entry`
  writes one entry's `config` and nothing else, so adding, removing, disabling,
  re-granting or re-pinning an entry is a file edit behind the daemon — the one
  composition change the ledger sees only as consequences and never as an intent
  with an author. The direction is carded, not decided here: M2-K23
  (`docs/packets/M2-K23.md`) supplies `jinn:profile-admin`, the capability
  constitution 04 already names — five writes as operator intents applied by
  reconcile-by-id, authorized by a separate grant on the calling entry (per-call
  principal propagation from `jinn:auth` rejected with reasons; the same-uid limit
  stated as `jinn:auth` states it), each a ledger row with the caller and both
  document digests, each reversible by the inverse write the row records. Two
  readings of `655f07e` ride on the card, each to be confirmed by a red test before
  it is acted on: `patch-entry` admits a `config.grants` widening (a Law-1 side
  door closed on the same seam), and the document-led `Replace` step disposes
  where the M2-K4 ruling says an incarnation replacement inherits (ruled a
  replacement, with a STOP rule if the fiber engine must move). §7's item 6
  records the blocker and points at the card; items 4 and 5 are marked landed the
  same day. Sequenced after M2-K25 and M2-K26. Nothing about M2's acceptance
  changes — §7(a) is met, §7(b) is still owed.

- **2026-09-03** — **A fifth M3 structural blocker is named in §7: a listener's
  config restart opens a fail-open window to every reply-expecting walk (roadmap
  amendment, recorded by kernel-dev on the COO's dispatch, PLA-355; COO veto
  window open).** Found by harness packet UI-2 (PLA-353) at pin `a53a352`, proof
  5, with a transcript (FINDINGS #47): the listener's `listen` is withdrawn at the
  restart's suspension and the replacement's lands at the end of its activation,
  so for the window (1,492 ms measured) a `before-send` waterfall selects nobody
  and answers the payload unmodified — 53 unvalidated sends, no `503` — while
  M2-K9's `restarting` never fires because it is keyed on a SELECTED listener. The
  direction is carded, not decided here: M2-K26 (`docs/packets/M2-K26.md`) keeps
  the registration as a refusing tombstone until the replacement commits under the
  Mode-1 `rebind` lock, clears it exactly when the fiber rests (I4), closes M2-K9's
  in-flight-load `stalled` answer at the one source, and carries FINDINGS #49
  (`emit` covered by the topic's grant) with reasons stated on the card. §7's item
  5 records the blocker and points at the card. Sequenced after M2-K25, before
  M2-K23. Nothing about M2's acceptance changes — §7(a) is met, §7(b) is still
  owed.

- **2026-09-03** — **A fourth M3 structural blocker is named in §7: a listener's
  delivery spends the emitter's clock, and a dead instance is not a failed fiber
  (roadmap amendment, recorded by kernel-dev on the COO's dispatch, PLA-355; COO
  veto window open).** Found by harness packet UI-2 (PLA-353) at pin `a53a352`,
  proof 7, with a transcript (FINDINGS #48): a `while (true) {}` extension on a
  `before-send` waterfall killed the TRANSPORT's instance at the guest deadline,
  the transport's fiber stayed `Active` on the record, its port kept accepting for
  a dead instance, and the operator API was gone until a daemon restart. The
  direction is carded, not decided here: M2-K25 (`docs/packets/M2-K25.md`) pauses
  the emitter's clock for the walk, bounds every delivery on the listener's side
  with a fuel budget declared at `listen`, and makes any post-activation instance
  death fail its own fiber on the record (one additive `TransitionCause`, the
  fiber engine's one new input, loom owed). §7's item 4 records the blocker and
  points at the card; item 3 is marked landed the same day. Nothing about M2's
  acceptance changes — §7(a) is met, §7(b) is still owed.

- **2026-09-02** — **A third M3 structural blocker is named in §7: no readiness
  gating between plugins on the string lane (roadmap amendment, recorded by
  kernel-dev on the COO's dispatch, PLA-350; COO veto window open).** Found by
  harness packet UI-1 (PLA-349) at pin `85d36b4`, with a transcript (FINDINGS #45,
  #46) of the shape FINDINGS #7 predicted: a wasm entry injecting a sibling's
  contract at activation activates on a coin toss and is never re-armed or restarted
  by the kernel. The provider-announces-itself repair is refused by M2-K10 as the
  wait cycle it is, which proves readiness is the kernel's fact to publish (§3). The
  direction is carded, not decided here: M2-K24 (`docs/packets/M2-K24.md`) carries
  the typed lane's `injects` semantics to the string lane as an additive per-entry
  declaration; §7's item 3 records the blocker and points at the card. Nothing about
  M2's acceptance changes — §7(a) is met, §7(b) is still owed.

- **2026-09-01** — **M3's acceptance is not reachable as written; the two blockers are
  named in §7 (Jimbo, roadmap amendment).** Found in a step-back review Hristo asked
  for, not by any gate. The completed core port (2.1–2.7, all landed) ships no outbound
  HTTP capability and no authentication boundary, so "an instance runs a full day on
  jinnd alone" describes an instance that can neither reach a vendor API nor tell an
  operator from anyone else with access to the port. §7 M3 now records both. This
  amendment records a discovered FACT and chooses no direction: whether M3 proceeds,
  and in what order those two are closed, is Hristo's decision and is explicitly open.
  M2 is unchanged by this — §7(a) is met, §7(b) (the week of real duty) is still owed.

- **2026-08-30** — **M2 widened to a core port; recorded LATE and flagged as such
  (Jimbo).** Hristo directed on 2026-08-28: "push reliability wait after we port most
  core pieces including API, sessions, engines, workflows, todos, settings, plugins…
  we want to build new jinn from ground up with malleability in mind. we should be able
  to switch and extend any piece." The soak stopped gating packet dispatch from that
  point. **This should have been logged the day it was given.** It was not, so §7 and
  the actual work diverged silently for two days — the precise failure this file's
  preamble forbids. Recorded now with the delay named rather than back-dated. The
  week-of-duty acceptance is explicitly NOT retired by the widening: M2 needs both the
  seams and the week. As of this entry: seams 2.1 API, 2.2 settings, 2.3 engines, 2.4
  sessions, 2.5 todos landed; 2.6 workflows in verify; 2.7 plugins UX queued. Kernel
  K1–K10 landed, K11 carded. Cutover rule untouched — the old gateway keeps ALL
  production.

- **2026-08-23** — All-in ground-up rewrite in Rust; Cordis paradigm reimplemented, not
  wrapped; one plugin contract, static-native + WASM hosting; old gateway keeps
  production until parity (Hristo).
- **2026-08-23** — 7-agent audit of Cordis v4 + paper complete; Rust verdict GO
  (~4–6k LOC kernel). Synthesis lives in the private audit annex.
- **2026-08-24** — cordis-rs (dshbox) audited by 4-agent fleet: rejected as foundation
  (sync-only engine, dylib tier, no WASM/capabilities/ledger, bus factor 1); kept as
  vendored reference/quarry. R1/R2 codified from its failure mode (Jimbo, approved
  direction by Hristo: "we need to create our own kernel").
- **2026-08-24** — This file created as the kernel's source of truth (Hristo's
  mandate: "the plan and rules are the most important thing").
- **2026-08-24** — Green light: `jinnd` repo scaffolded with CI gates from commit
  zero; two-key rule (test-writer ≠ implementer, `tests/invariants/` write-protected
  from implementers); `kernel-dev` employee hired; M0 drafting started (Hristo).
- **2026-08-24** — Constitution v0.1 round 1: verifier returned NOT-READY with 5
  blockers (capability closure, signed envelope, ledger receipts/compaction, revert
  protocol, profile/calculus binding). All 5 applied in rc2; v0.1 scope narrowed:
  subprocess hosting disabled, no unsigned tier, device-local ledger, no destructive
  compaction, no time-range revert, local-only profiles, pin-by-hash updates.
  ⚠️ One Law-1 interpretation flagged for operator ratification: InProc = enumerated,
  CI-disciplined kernel TCB (01 §Mechanical closure).
- **2026-08-24** — Constitution round 2 (RATIFIABLE-WITH-FIXES, 2 blockers): rc3
  applied. **InProc plugin host removed entirely** — first-party plugins run in the
  same WASM host as everyone; native Rust = kernel implementation only (R7 amended;
  supersedes the round-1 TCB interpretation; ⚠️ reverses part of the 2026-08-23
  two-hosting-modes decision — flagged for operator confirmation at M0 read). Revert
  hardened to keyed exactly-once provider protocol + executable witnesses;
  accept-residue terminal state removed (`pending-revert` until reverted or
  honestly `compensated`).

- **2026-08-24** — **Constitution v0.1 RATIFIED** (operator approval, M0 accepted).
  Verifier trail: round 1 NOT-READY (5 blockers) → round 2 RATIFIABLE-WITH-FIXES
  (2 blockers, 1 minor) → round 3 RATIFIABLE-AS-IS at HEAD af30693. Operator
  explicitly confirmed the InProc removal: all plugins WASM-hosted, first-party
  included; native Rust = kernel implementation only. M1 begins: packet 0 = test
  port (verifier-owned, lands red).

- **2026-08-25** — **Containment tier model codified** (operator-approved). Law 5
  mandates *sandboxed and signed*, not a technology; R7's two backends are tiers of
  one contract. **Tier A (default): WASM components** — logic/orchestration plugins
  and all machine-written code (provable confinement, instant dispose, one portable
  signed artifact). **Tier B: sandboxed native processes** (`Subprocess` backend) —
  any language, native-heavy/data-plane plugins; containment via per-OS sandbox
  (macOS Seatbelt; Linux namespaces + seccomp + Landlock); enabled only when that
  sandbox ships, per R7. **Tier C: GUI environments** — Tier B processes whose
  capability grants include the display protocol (Wayland/Fuchsia pattern): a
  compositor plugin holds the hardware capability and mediates surfaces, input, and
  GPU. Ledger granularity for Tier B/C is grants + lifecycle + contract-level
  effects, never data-plane traffic — per-frame mediation is rejected as a scaling
  hazard. **Binding design requirement on the wasm-host packet: the capability
  broker is transport-agnostic** — grant check, ledger append, and dispatch accept
  "a contract call from a peer"; whether the peer is a linked WASM instance or a
  socket-connected sandboxed process is a transport detail. A broker fused to WASM
  linking is a design defect, not an implementation choice.

- **2026-08-25** — M1-P5 round-1 adjudication (COO): invariant-suite prose IOUs are
  converted to executable bodies BY THE VERIFIER inside the packet that wires their
  subsystem (starting with the 14 events cases), before the implement rework round —
  suite-level TDD, two-key preserved. This prices the audit's "hidden verifier
  packet" into every remaining subsystem packet.

- **2026-08-25** — M1-P4 round-1 adjudication (COO, with verifier escalation): first
  3 invariant cases GREEN (verifier-validated). Rulings: additive dependency-
  declaration/injection facade surface authorized; FACADE_GAP red-reasons admitted
  as the designed post-wiring state, verifier owns the per-case recorded-reason
  ratchet catalog (completes the PLA-255 ruling); component LOC budgets are
  ceilings, not floors (R10).

- **2026-08-25** — Body refactored to carry the tier model natively (operator-directed):
  R7 rewritten as "One contract, tiered containment" (Tiers A/B/C + transport-agnostic
  broker inline); R8's reload "tiers" renamed to *modes* so "tier" unambiguously means
  containment; §6 diagram and notes updated. Semantics identical to the entry above.

- **2026-08-25** — M1-P2 budget amended 300–400 → 300–500 (COO; §6 effects estimate
  synced to 0.3–0.5k). Round-1 verify blockers mandated containment machinery the
  estimate didn't price in; verifier confirmed all else passes at 479 LOC. Rule for
  future cards: budgets are estimates priced before adversarial findings — the COO
  re-prices on evidence rather than incentivizing containment-code golf.

- **2026-08-25** — **Test-harness lane ruling** (COO, from the independent audit): the
  in-proc, statically-typed `jinnd-api::Kernel` facade is the CONFORMANCE HARNESS lane —
  it exists so the invariant suite can drive kernel semantics; it is never a plugin host
  and never ships in the daemon binary (Law 1 closure). The wasm-host packet card MUST
  carry as acceptance: (a) the production plugin path is WIT/broker only, (b) the harness
  lane is compile-gated out of the daemon build, (c) the transport-agnostic broker (R7)
  is the single dispatch point for both lanes. Same date: machine-specific references
  (private paths, host-machine names, session ids) scrubbed from prose and history by
  operator directive; authorship deliberately remains the creator's personal identity.

- **2026-08-25** — **Dual independent audits adjudicated** (formal paper audit →
  PLA-269; cordis-v4/dsh implementation audit → PLA-271; verifier R1-seam
  observations → PLA-270; code fixes packeted as **M1-P6c** registry/loader
  conformance). Rulings codified:
  1. **Refusal-vs-defer recorded.** M1-P6b's refuse-not-wait amendment gate is
     deliberately STRONGER than the paper's Algorithm 5, which never refuses and
     never waits — it stages desired state and lets the landed transition chain to
     the latest target. Consequences: I4's history generator must include
     amendment-during-withdrawal operations so the refusal path is itself
     confluence-tested; constitution 04's self-write-back right is denied from
     teardown/withdrawal contexts by this ruling. Migrating to paper-shaped
     non-blocking staged amends is a recorded post-M1 candidate.
  2. **Failed-fiber re-arm dated.** Re-arm when the aim changes (epoch/revision
     bump) diverges from the paper's no-re-entry-from-failure rule; deliberate —
     TS-v4-faithful and R9-compliant (retry only against a CHANGED environment).
  3. **Root-realm positional ruling dated.** An explicit descendant root binding
     stops at an intervening isolated ancestor (TS-faithful, pinned green);
     diverges from the paper's flat-realm reassignment (Def 29); kept per R2.
  4. **Constitution 04 amended twice** — entry move = epoch-decides-reload (not
     always unload/reload); whole-rejection scoped to document-level shape with
     per-entry faults contained per R11. In both places the ratified text
     contradicted the kernel AND the verifier-green suite; code adjudicated
     correct. Operator veto window open.
  5. **R9 gains unbounded self-registration; R10 gains the kernel-core metric
     note** (harness lane, loom models, cfg(test) excluded).
  6. **Wasm-host packet card requirements extended** (joining the
     transport-agnostic broker and harness-lane closure): per-consumer, per-notify
     vitality-check evaluation seam; DECLARATIVE WIT event selectors (closures
     cannot cross the component boundary; realm queries evaluated kernel-side);
     Mode-1 swap batches all entries sharing an artifact hash.

- **2026-08-29** — **C5 and C6 DECIDED on measured evidence (harness FINDINGS #27,
  settings seam, pin 57360cc).** Measured on the real daemon: a `jinn:profile`
  `patch-entry` on an entry's own config = reconcile-by-id restart, 21 ledger rows,
  28 ms, state retained (K4 entry-scoped effects); the seam-level hot path (a
  separate overlay-store entry + `changed` event, owner untouched) = 33 rows, 56 ms.
  **C5 (hot-config acceptance): REJECTED as a kernel feature.** Reconcile-by-id
  restart stays the ONLY kernel apply path for an entry's own config — cheap,
  honest, and state-preserving since K4. Hot-config is a seam pattern above the
  kernel (a distinct store entry that the owner reads), never an in-place mutation of
  the owner's entry. **C6 (per-entry config layering / intercept plumbing): REJECTED
  in the kernel.** The profile stays the single document of record; layering is the
  settings provider's concern under a harness-side consistency law (a patch reports
  and emits exactly the value the next read resolves, or refuses typed). The kernel's
  only obligations arising from this evidence go to M2-K8: (a) FINDINGS #26 — a
  non-blocking `patch-entry` answer (`accepted` once the document is committed and
  the restart scheduled — the Algorithm-5 deferred-amendment shape already named on
  2026-08-25) so a patched owner may call anything in `activate` without the two-hop
  nested-dispatch deadlock; (b) FINDINGS #25 — a read-only document view
  (`jinn:profile.entry(id)`/`document()` or authority fields on `jinn:introspect`)
  so viewers need no `jinn:fs` write authority and the data-root coupling ends.

- **2026-08-28** — **Suspend ≠ dispose adjudicated (harness FINDINGS #14/#15 →
  M2-K4).** At 41cb2f4, clean SIGINT withdrew every fs mutation (state/history
  reverted to activation-time content) while SIGKILL preserved them — crash kept
  state, graceful shutdown erased it. Ruled a CONFLATION, not a LAW consequence:
  §3 LIFO teardown governs removal from the composition; effect withdrawal is
  bound to the profile ENTRY, never to a fiber incarnation or a process
  lifetime. Codified: dispose (entry removal) withdraws the full trail LIFO
  unchanged; daemon shutdown SUSPENDS (quiescence + ledger flush + typed
  suspension events, zero world-mutation undos); incarnation replacement
  (reconcile/config-edit) hands the successor the entry's live effect journal —
  the durable retention store exists precisely to span incarnations; kernel
  registrations (alarms, listeners, services) release on suspend and re-arm on
  activate, distinct from world mutations. FINDINGS #15 (registrations escaping
  a sealed journal, torn dispose trail) joins the same card: post-seal
  registrations refuse with a ledgered error — a dispose trail is exactly the
  fiber's contribution (I1), never a prefix. Shape (i) (a jinn:ledger read
  import for guest-driven record replay) was considered and DEFERRED to v0.2 —
  the record lane may yet be the ledger, but not before a consumer needs it.
  Card: docs/packets/M2-K4.md. Harness interim: soak planned stops use the hard
  path (SOAK.md ruling 48a44f5) so audit evidence cannot be reverted.

- **2026-08-28** — **M1 ACCEPTED AND CLOSED.** All 9 packets + 3 patch packets
  landed (final main cb9b3c9, 183 commits). Acceptance demonstrated per the
  operator-delegated protocol: a fresh operator-role session drove
  docs/demo/M1-DEMO.md end-to-end against the real `jinnd` binary — three wasm
  plugins Active, config edit restarting exactly the edited fiber, Mode-1
  hot-swap healthy + broken-artifact rollback, strict-LIFO ledger-visible
  dispose, keyed exactly-once revert (duplicate replayed safely, distinct key
  refused), clean SIGINT quiescence — SHIP, zero findings. Suite final:
  95/130 expected-green, 35 V02_DEFERRED each citing its v0.1 constitution
  bound, FACADE_GAP and NO_KERNEL extinct. Known-gap note: the PTY transport
  does not expose the daemon's shell exit code separately (observed at
  acceptance; harmless, logged shutdown is authoritative). M2-entry debt
  register (carried on the program, not lost): daemon lane/slot lift into
  jinnd-wasm; hot-config acceptance decision (C5); intercept plumbing (C6);
  refusal→defer-to-latest-desired amendment migration; Thm 66 bound proptest
  + remaining paper-audit test opportunities. Next per the operator-locked
  plan: **jinn-harness** — ground-up reconstruction of the production gateway
  on this kernel, two-way iteration, kernel changes packet-gated as ever.

- **2026-08-27** — **M1-P7 escalation adjudicated (plain-effect atomicity).** The
  implementer proved joint unsatisfiability between "the kernel erases a failing
  plain closure's partial mutations" and the pinned no-inverse-on-Err semantics;
  accepted. The paper's "a plain effect installs all or none" (p.36) is a claim
  about effect REGISTRATION in a model whose actions are atomic transformers —
  not a mandate to undo hostile closure side effects, which is impossible
  without an inverse the failing action never registered. Codified obligation:
  **a plain-effect action must be internally atomic; the kernel guarantees
  no-registration and no-inverse-run on Err; partial mutation inside a failing
  plain action is a provider contract violation**, in the same class as the
  commutativity obligation (Thm 42). Kernel-enforceable atomicity lives in
  Steps/iterator effects (prefix unwind, Alg 1); the invariant case was
  reshaped accordingly (verifier-lane). Same packet: crash-safe revert claims
  resume under their durable intent (constitution 03), and the harness-lane
  LOC ceilings were re-priced on evidence (M1-P2 precedent).

- **2026-08-24** — M1-P1 delivered (jinnd-context, 468 LOC, miri clean) but exposed
  a P0 suite defect: cases never call the facade, so no packet can green a case
  without breaking the two-key rule. Implementer correctly BLOCKED instead of
  cheating. COO adjudication: P1 acceptance amended; M1-P0b (verifier) restores
  bindability via an adapter seam + green-ratchet + CI split; M1-P1c (kernel-dev)
  wires context into the adapter. Process note: 'red by design' must mean red
  through the adapter, never red inside the test body.

## 10. Sources

- Full audits (TS v4 + paper + cordis-rs addendum) live in the private audit annex.
  Read before any kernel work; do not re-audit.
- Reference checkouts (TS Cordis v4, the 88pp paper, cordis-rs) live in the private
  reference annex, outside this repo.
- dsh cordis-primer (vocabulary for plugin authors):
  `deepseek-ai/deepseek-harness/docs/cordis-primer.md`.
- Origin: operator sessions (private annex).
