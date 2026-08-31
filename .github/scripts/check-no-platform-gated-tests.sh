#!/usr/bin/env bash
# M2-K12 regression guard. A security or acceptance property that holds only
# where we happen to develop is not a property, it is a hope. The forbidden
# move when such a test goes red on one platform is to gate it to another
# (`#[cfg(target_os = ...)]`) or to silence it (`#[ignore]`): that converts an
# unverified property into an INVISIBLE one. Platform-conditional code stays
# legal in `src/` — the platform default IS platform-dependent by design; the
# integration suites, where the acceptance and invariant properties live, must
# run everywhere.
#
# The forms this recognises, and the one it cannot, are stated in
# `scan-conditional-tests.pl`. Every form named there has a fixture in
# `test-no-platform-gated-tests.sh` that is demonstrated to make this script
# exit non-zero — a guard nobody has watched fail is not a guard.
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
  echo "guard: an integration test is gated to a platform or silenced:"
  echo "$hits"
  echo
  echo "A property that cannot be verified on every platform is not verified."
  echo "Fix the property, or implement the platform's honest path — never gate"
  echo "the test. Changes under tests/invariants/ are the verifier's key (R2)."
  exit 1
fi

echo "guard: ok; ${#sources[@]} integration test sources in ${directories[*]}," \
     "no platform gate and nothing silenced"
