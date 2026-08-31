#!/usr/bin/env bash
# M2-K12 — the FAST HEURISTIC half of the platform-gating guard. It is not the
# guarantee, and its exit code must never be quoted as one; the guarantee is
# `check-test-inventory.sh`, which compares the tests two platforms actually
# compiled in at the same commit.
#
# The property both halves serve: a security or acceptance property that holds
# only where we happen to develop is not a property, it is a hope. The
# forbidden move when such a test goes red on one platform is to gate it to
# another (`#[cfg(target_os = ...)]`) or to silence it (`#[ignore]`), which
# converts an unverified property into an INVISIBLE one. Platform-conditional
# code stays legal in `src/` — the platform default IS platform-dependent by
# design; the integration suites, where the acceptance and invariant properties
# live, must run everywhere.
#
# What this half buys: seconds, on one platform, naming the offending line — so
# the common case is caught in review rather than in CI. What it cannot decide
# is stated in full in `scan-conditional-tests.pl`; read it before treating a
# green here as an answer. Every form it does claim has a fixture in
# `test-no-platform-gated-tests.sh` demonstrated to make this script exit
# non-zero, because a guard nobody has watched fail is not a guard.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/../.." && pwd)
cd "$repo_dir"

directories=()
for candidate in crates/*/tests tests/invariants; do
  [[ -d "$candidate" ]] && directories+=("$candidate")
done

if [[ ${#directories[@]} -eq 0 ]]; then
  echo "guard: no integration test directories found — refusing to pass vacuously"
  exit 1
fi

sources=()
while IFS= read -r file; do
  sources+=("$file")
done < <(find "${directories[@]}" -name '*.rs' -type f | sort)

if [[ ${#sources[@]} -eq 0 ]]; then
  echo "guard: integration test directories hold no Rust sources — refusing to pass vacuously"
  exit 1
fi

if ! command -v perl >/dev/null 2>&1; then
  echo "guard: perl is required to decide this and is missing — refusing to pass unchecked"
  exit 1
fi

set +e
hits=$(perl "$script_dir/scan-conditional-tests.pl" "${sources[@]}")
status=$?
set -e

if [[ $status -gt 1 ]]; then
  echo "guard: the scanner failed to run — refusing to pass unchecked"
  echo "$hits"
  exit 1
fi

if [[ $status -eq 1 ]]; then
  echo "heuristic: an integration test is gated to a platform or silenced:"
  echo "$hits"
  echo
  echo "A property that cannot be verified on every platform is not verified."
  echo "Fix the property, or implement the platform's honest path — never gate"
  echo "the test. Changes under tests/invariants/ are the verifier's key (R2)."
  exit 1
fi

echo "heuristic: ok; ${#sources[@]} integration test sources in ${directories[*]}," \
     "nothing obvious — the guarantee is the test-inventory differential"
