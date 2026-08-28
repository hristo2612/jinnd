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
- **M2 — Dogfood slice.** One real capability (cron or todos) rebuilt as plugins on the
  kernel, running for real, ledger-visible. *Acceptance: the slice does its production
  job for a week without touching the old gateway's data.*
- **M3 — Profiles & parity.** The `company` profile takes over slices of production
  one at a time; web UI runs against the kernel API. *Acceptance: an instance runs a
  full day on jinnd alone.*
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
