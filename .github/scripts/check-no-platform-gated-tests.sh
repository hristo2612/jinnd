#!/usr/bin/env bash
# M2-K12 regression guard. A security or acceptance property that holds only
# where we happen to develop is not a property, it is a hope. The forbidden
# move when such a test goes red on one platform is to gate it to another
# (`#[cfg(target_os = ...)]`) or to silence it (`#[ignore]`): that converts an
# unverified property into an INVISIBLE one. Platform-conditional code stays
# legal in `src/` — the platform default IS platform-dependent by design; the
# integration suites, where the acceptance and invariant properties live, must
# run everywhere.
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

if hits=$(grep -rnE '#\[(cfg\(\s*(not\(\s*)?target_os|cfg_attr\(\s*target_os|ignore)' \
  "${directories[@]}" --include='*.rs'); then
  echo "guard: an integration test is gated to a platform or silenced:"
  echo "$hits"
  echo
  echo "A property that cannot be verified on every platform is not verified."
  echo "Fix the property, or implement the platform's honest path — never gate"
  echo "the test. Changes under tests/invariants/ are the verifier's key (R2)."
  exit 1
fi

echo "guard: ok; no platform-gated or silenced integration test in ${directories[*]}"
