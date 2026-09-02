# loc-meter

The canonical LOC meter (M2-K18). Cards cite it by path; nothing quotes a
`git diff | awk` pipeline any more.

```
cargo run -q -p loc-meter -- [--base main] [--head HEAD] [--files]
```

It measures the commits `merge-base(base, head)..head`, reads both sides from
git objects (never the working tree), and prints every changed line under
exactly one category. Exit 0 measured, 2 refused, 1 failed.

## The four budget lines

| line | what it bills |
|---|---|
| `production` | `.rs` files a `lib`/`bin` target of a workspace package under `crates/` compiles in the default non-test build, minus the harness lane, minus every item whose `#[cfg(..)]` is false in that build |
| `facade` | the same rule for `crates/jinnd-api` and `crates/jinnd-adapter` (the conformance-harness lane, R10 metric note) |
| `contracts` | `.wit` and `.toml`, wherever they live (the card's reading; `Cargo.toml` included) |
| `prose` | `.md`, wherever it lives. Markdown is EXCLUDED from production and reported here, not given a separate budget: a prose budget needs a ceiling nobody can price, a visible line needs nothing |

Everything else is printed under `outside every budget line`: `tests` (Rust
the non-test kernel build does not compile: `cfg`-off items, `tests/` trees,
integration tests, fixtures, demo guests), `tools` (Rust under `tools/`),
`other` (lockfiles, scripts, CI, binaries). Nothing vanishes: a production
file's `cfg`-off items are billed as a companion `[cfg-off items]` row under
`tests`, so the lines sum to the raw `git diff --numstat`.

## How cfg(test)-ness is decided

Not by filename. `cargo metadata --no-deps` names the target roots; the walk
follows `mod` declarations by rustc's rules (`x.rs`, `x/mod.rs`, `#[path]`,
inline blocks) and is proven equal to cargo's own dep-info on a fixture
(`tests/compiler_view.rs`). Each item's `#[cfg(..)]` is evaluated as the
default build evaluates it: `test`, unknown bare cfgs (`loom`, `kani`, ...)
and features nobody enables are false; platform and profile cfgs (`unix`,
`target_os`, `debug_assertions`, ...) count, because a size meter must count
what some build compiles. Features come from each package's defaults plus what
other workspace members request; `--features` flags on the command line are
not modelled. Blank lines that only separate a dropped item from what precedes
it are dropped with it, so `\n#[cfg(test)]\nmod tests;` costs zero.

## Refusal

Any uncommitted or untracked path (ignored paths excepted) refuses the run and
names the paths. The number describes commits; a dirty tree is the one state in
which a reader would take it for the tree, which is how M2-K14 read 929 for a
real 1065. A number nobody can misread beats a number with a footnote.

## Recorded re-measure: M2-K14 at `de43d06`

Old meter (the M2-K3 pipeline over the card's surfaces, paths containing
`tests.rs` excluded): `1355 254 1101`. This meter, same range:

| line | +/- | net |
|---|---|---|
| production | +1082 / -217 | 865 |
| facade | +77 / -31 | 46 |
| contracts | +188 / -27 | 161 |
| prose | +75 / -10 | 65 |

The two agree to the line: 865 + 161 + 65 + 10 (`cfg(test) mod` registration
lines the old meter billed) = 1101. Prose exclusion moves the production figure
by 65 (6% of 1101); contract files by 161; the compiler's view of `cfg(test)`
by 10.

## Known limits

Macro-generated modules, `include!`, and `cfg_attr` are not followed; a file
reached only that way is billed as `tests`. A `.rs` that no target reaches is
billed as `tests` too, which is what the compiler does with it.
