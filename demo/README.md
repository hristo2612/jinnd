# M1 demo kit

Fixtures for the M1 acceptance demo (`docs/demo/M1-DEMO.md`; headless twin
in `tests/demo/`). Nothing here is a kernel workspace member and no built
artifact is ever checked in — `demo/builder` compiles the three plugins
from source for `wasm32-unknown-unknown`, encodes each to a component, and
pins it by its true sha-256 (Law 5).

- `plugins/clock` — provides `demo:clock`; snapshot/restore hands its tick
  across Mode-1 swaps. Features: `v2` (byte-distinct healthy swap target),
  `broken-restore` (refuses the handoff → rollback demonstrator).
- `plugins/greeter` — consumes `demo:clock`, provides `demo:greeting`,
  announces on `demo:announce`.
- `plugins/scribe` — listens on `demo:announce`, journals through its
  granted `jinn:fs` host contract (the revertible-effect demo).
- `builder` — `kit <root>` builds everything and generates the pinned
  `profile.json`; `clock <v1|v2|broken> <artifacts-dir>` rebuilds the clock
  artifact (+ `.sha256` pin sidecar) in place: the hot-swap trigger.
