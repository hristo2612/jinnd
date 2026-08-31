#!/usr/bin/env bash
# M2-K12 — the test inventory: what libtest ACTUALLY has on this platform.
#
# This is the measured half of the platform-gating guard, and it is the half
# that carries the guarantee. Rounds 1 and 2 both tried to decide from source
# text which tests could vanish on a platform, and each was defeated by a form
# its author had not imagined -- an inner `#![cfg]`, then token whitespace in
# `#! [cfg(...)]`. The claim behind that shape, "a source scanner can enumerate
# every way a test is silently gated", has no enforcer and is retired.
#
# So stop reading the source and measure the outcome. This lists the tests that
# were compiled in on this platform at this commit; `check-test-inventory.sh`
# compares that list against another platform's list at the same commit, and a
# divergence is the finding WHATEVER produced it: a cfg attribute in any
# spelling, a `cfg_attr` that silences, a feature flag, a build script, an
# absent target, or a dependency captured under a platform-only table -- which
# is precisely the defect that opened this packet and that no test scanner
# would ever have modelled.
#
# What it decides: the presence and the silenced-state of every compiled-in
# test, per package and target. That is TEST PARITY, and test parity is the
# whole of the guarantee. A test that VANISHES or is SILENCED on a platform is
# a platform-gating defect, and this catches it whatever produced it.
#
# What it does NOT decide, written down here because a limit nobody wrote down
# is the exact shape this packet is named after: a test that is present,
# enabled and GREEN on both platforms while its body does nothing -- an early
# return on `std::env::consts::OS`, a loop over an empty set, an assertion
# checked against a store that was never populated. Such a test has the same
# name and the same state in both inventories, so no inventory, no scanner and
# no name comparison can distinguish it. That is a VACUITY defect: a different
# class from platform gating, with a different remedy.
#
# The remedy for vacuity is the non-vacuous precondition discipline this packet
# established in `crates/jinnd-daemon/tests/keystore/authority.rs` — a test
# states and asserts the precondition that makes its main assertion mean
# something. This packet's own founding finding was precisely a vacuity defect:
# a keystore authority assertion that was correct, compiled, executing and
# passing, over an empty store.
#
# Emits TSV, sorted, and free of absolute paths (they differ per runner, and
# this repo is destined to be public): package, kind, target, test, state.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/../.." && pwd)
cd "$repo_dir"

out=${1:?usage: test-inventory.sh <output-file>}

for tool in cargo jq; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "inventory: $tool is required to decide this and is missing — refusing to emit an inventory nobody can trust" >&2
    exit 1
  }
done

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM

# Build every test binary in the workspace. A platform where this does not
# build has no inventory, and the job is red — which is the correct answer.
cargo test --workspace --no-run --message-format=json >"$work/build.json"
cargo metadata --no-deps --format-version 1 >"$work/meta.json"

# package_id carries an absolute path, so resolve it to the package NAME via
# cargo metadata rather than parsing it. Nothing machine-specific survives.
jq -r --slurpfile meta "$work/meta.json" '
  select(.reason == "compiler-artifact")
  | select(.executable != null)
  | select(.profile.test == true)
  | . as $artifact
  | ($meta[0].packages[] | select(.id == $artifact.package_id) | .name) as $package
  | [$package, ($artifact.target.kind | join("+")), $artifact.target.name, $artifact.executable]
  | @tsv
' "$work/build.json" | LC_ALL=C sort -u >"$work/targets.tsv"

if [[ ! -s "$work/targets.tsv" ]]; then
  echo "inventory: the workspace produced no test binaries — refusing to emit a vacuous inventory" >&2
  exit 1
fi

: >"$work/inventory.tsv"
while IFS=$'\t' read -r package kind target executable; do
  [[ -x "$executable" ]] || {
    echo "inventory: $package/$target reported an executable that is not runnable: $executable" >&2
    exit 1
  }

  # `--list --ignored` lists exactly the silenced tests, so a test that is
  # `#[ignore]`d on one platform only is a state divergence, not an absence.
  "$executable" --list --ignored | sed -n 's/^\(.*\): test$/\1/p' | LC_ALL=C sort -u >"$work/ignored.txt"

  while IFS= read -r name; do
    state=test
    grep -qxF -- "$name" "$work/ignored.txt" && state=ignored
    printf '%s\t%s\t%s\t%s\t%s\n' "$package" "$kind" "$target" "$name" "$state" >>"$work/inventory.tsv"
  done < <("$executable" --list | sed -n 's/^\(.*\): test$/\1/p' | LC_ALL=C sort -u)
done <"$work/targets.tsv"

LC_ALL=C sort -u "$work/inventory.tsv" >"$out"

targets=$(wc -l <"$work/targets.tsv" | tr -d ' ')
tests=$(wc -l <"$out" | tr -d ' ')
echo "inventory: $tests tests across $targets test binaries on $(uname -s) -> $out"
