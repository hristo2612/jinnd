# M1 Acceptance Demo — the `jinnd` daemon

The §7 M1 acceptance, driven by hand (SOURCE-OF-TRUTH: "each milestone gated
by a demo, not a claim"). Every step names the command to run and what to
observe. The same five steps run headlessly in `tests/demo/` — if that test
is green, this script reproduces it.

Prerequisites: a Rust toolchain with the `wasm32-unknown-unknown` target
(`rustup target add wasm32-unknown-unknown`), and `sqlite3` (only for
reading the ledger; any SQLite reader works).

Every path below lives in one scratch directory:

```sh
DEMO="$(mktemp -d)"
echo "$DEMO"
```

## 0. Build the demo kit

Three toy plugins, built from source (`demo/plugins/`) and pinned by hash —
no artifact binary is checked in (Law 5):

```sh
cargo run --manifest-path demo/builder/Cargo.toml -- kit "$DEMO"
cat "$DEMO/profile.json"
```

Observe: `artifacts/{clock,greeter,scribe}.wasm` with `.sha256` sidecars,
and `profile.json` pinning each entry to its artifact's true hash. The
entries also carry their **grants** — the capability names the profile side
grants each instance (constitution 01: requests are not grants):

- `clock` provides `demo:clock` (a monotonic tick; snapshot/restore carries
  it across hot-swaps),
- `greeter` consumes `demo:clock`, provides `demo:greeting`, announces on
  the `demo:announce` topic,
- `scribe` listens on `demo:announce` and journals through its granted
  `jinn:fs` host contract.

## 1. Boot: three plugins Active

```sh
cargo run -p jinnd-daemon -- --profile "$DEMO/profile.json" --ledger "$DEMO/ledger.sqlite"
```

Observe (stderr): `reconciled` with `created=["clock", "scribe", "greeter"]`,
then one `entry` line per plugin with its **fiber uid** and `state=Active`.
Note the uids — they are the evidence in the next steps.

The ledger already shows the boot: artifact admissions, provisions, guest
effect registrations, contract calls — every crossing of the broker
(Law 2). In a second terminal:

```sh
sqlite3 "$DEMO/ledger.sqlite" 'SELECT seq, fiber, kind FROM events ORDER BY seq'
```

## 2. Edit one entry's config → exactly that fiber restarts

In the second terminal, change the greeter's `data` from `"world"` to
`"kernel"` (edit the file however you like):

```sh
python3 - "$DEMO/profile.json" <<'EOF'
import json, sys
path = sys.argv[1]
doc = json.load(open(path))
next(e for e in doc["entries"] if e["id"] == "greeter")["config"]["data"] = "kernel"
json.dump(doc, open(path, "w"), indent=2)
EOF
```

Observe (daemon stderr): `reconciled` with `restarted=["greeter"]` and the
entry lines again — **clock's and scribe's uids are unchanged**; greeter's
uid is also unchanged (a config restart is the same fiber, one clean
unload/reload through its cell). The fresh greeter activation greeted
again, the clock ticked, and the scribe journalled it:

```sh
cat "$DEMO/data/journal.txt"        # …hello, kernel (tick N)
```

## 3. Hot-swap one plugin's wasm artifact

**Healthy swap.** Build the byte-distinct v2 clock over the artifact file
(the builder writes the artifact AND its `.sha256` pin sidecar — the
operator states the pin, the kernel verifies it):

```sh
cargo run --manifest-path demo/builder/Cargo.toml -- clock v2 "$DEMO/artifacts"
```

Observe (daemon stderr): `hot-swap committed` with `swapped=["clock"]`. The
fiber uid did NOT change — the seat swapped warm under the live fiber; the
old instance answered until the new one was healthy, and the tick continued
where it left off (snapshot → restore). The ledger shows the phase trail:

```sh
sqlite3 "$DEMO/ledger.sqlite" \
  "SELECT seq, kind FROM events WHERE kind LIKE '%SwapPhase%' ORDER BY seq"
# Began → InstanceHealthy → Committed
```

**Broken artifact → automatic rollback.** The `broken` variant refuses the
state handoff, failing the health gate:

```sh
cargo run --manifest-path demo/builder/Cargo.toml -- clock broken "$DEMO/artifacts"
```

Observe: `hot-swap rolled back; old instances serving`, and the ledger's
new phases end in `RolledBack`. Repeat the step-2 config edit with another
name — the greeting still lands in the journal: the old clock never
stopped serving.

> The profile's `hash` stays the BOOT pin. A live swap is runtime-led (R8);
> to make it durable across a restart, update the entry's `hash` to the new
> artifact's (that document-led pin change is a cold Replace, by design).

## 4. Dispose one plugin → the ledger shows exactly what was undone

Remove the `scribe` entry from `profile.json` (same editing pattern as
step 2, deleting the object), then observe: `reconciled` with
`disposed=["scribe"]`, and the ledger's withdrawal trail — the guest's
inverses replayed LIFO, each one recorded:

```sh
sqlite3 "$DEMO/ledger.sqlite" \
  "SELECT seq, kind FROM events WHERE kind LIKE '%EffectWithdrawn%' OR kind LIKE '%ServiceWithdrawn%' ORDER BY seq"
# … {"EffectWithdrawn":{"label":"scribe on duty","clean":true}} …
```

## 5. Revert a recorded effect by key

Every journal write was a **revertible effect**: the fs provider registered
the inverse (the file's prior content) at the point of action (Law 3), and
logged `fs write effect registered` with the effect id (also visible in the
ledger's `EffectRegistered` events). Revert the last one **by typing into
the daemon's stdin** (first terminal):

```
revert <effect-id> demo-revert
```

Observe: `revert resolution=Reverted`, the journal's last line is gone
(`cat "$DEMO/data/journal.txt"`), and the ledger shows the receipt trail of
the keyed exactly-once protocol:

```sh
sqlite3 "$DEMO/ledger.sqlite" \
  "SELECT seq, kind FROM events WHERE kind LIKE '%Revert%' ORDER BY seq"
# RevertIntent → RevertCompleted → RevertResolved {"resolution":"Reverted"}
```

Typing the same command again answers `Reverted` from the record — the
inverse does not run twice. A different key for the same effect is refused.

## 6. Shutdown

`Ctrl-C` in the daemon terminal. Observe: `SIGINT: disposing all…`, every
fiber's withdrawal lands in the ledger, then `quiescent; ledger flushed;
bye` and exit code 0.
