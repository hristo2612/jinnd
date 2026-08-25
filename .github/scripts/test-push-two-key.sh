#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
checker="$script_dir/check-push-two-key.sh"
fixture_root=$(mktemp -d)

cleanup() {
  rm -rf "$fixture_root"
}
trap cleanup EXIT HUP INT TERM

new_fixture() {
  local name=$1
  local repo="$fixture_root/$name"

  git init --quiet --initial-branch=main "$repo"
  git -C "$repo" config user.name fixture
  git -C "$repo" config user.email fixture@example.invalid
  printf 'baseline\n' >"$repo/README.md"
  mkdir -p "$repo/crates/jinnd-adapter/src"
  printf 'baseline\n' >"$repo/crates/jinnd-adapter/src/lib.rs"
  git -C "$repo" add .
  git -C "$repo" commit --quiet -m "chore: seed fixture"
  printf '%s\n' "$repo"
}

commit_as() {
  local repo=$1
  local name=$2
  local email=$3
  local message=$4

  git -C "$repo" \
    -c "user.name=$name" \
    -c "user.email=$email" \
    commit --quiet -am "$message"
}

commit_all_as() {
  local repo=$1
  local name=$2
  local email=$3
  local message=$4

  git -C "$repo" add .
  commit_as "$repo" "$name" "$email" "$message"
}

assert_passes() {
  local repo=$1
  local base=$2
  local head=$3
  local scenario=$4

  if ! output=$(cd "$repo" && bash "$checker" "$base" "$head" 2>&1); then
    printf 'FAIL: expected pass: %s\n%s\n' "$scenario" "$output" >&2
    exit 1
  fi
}

assert_fails() {
  local repo=$1
  local base=$2
  local head=$3
  local scenario=$4

  if output=$(cd "$repo" && bash "$checker" "$base" "$head" 2>&1); then
    printf 'FAIL: expected failure: %s\n%s\n' "$scenario" "$output" >&2
    exit 1
  fi
  if [[ "$output" != *"two-key tripwire"* ]]; then
    printf 'FAIL: expected loud tripwire error: %s\n%s\n' "$scenario" "$output" >&2
    exit 1
  fi
}

repo=$(new_fixture same-author)
base=$(git -C "$repo" rev-parse HEAD)
mkdir -p "$repo/tests/invariants" "$repo/crates/jinnd-context/src"
printf 'test\n' >"$repo/tests/invariants/new.rs"
printf 'impl\n' >"$repo/crates/jinnd-context/src/lib.rs"
commit_all_as "$repo" implementer implementer@example.invalid "feat: mix test and implementation"
assert_fails "$repo" "$base" "$(git -C "$repo" rev-parse HEAD)" \
  "one author changes protected tests and implementation"

repo=$(new_fixture distinct-authors)
base=$(git -C "$repo" rev-parse HEAD)
mkdir -p "$repo/tests/invariants"
printf 'test\n' >"$repo/tests/invariants/new.rs"
commit_all_as "$repo" verifier verifier@example.invalid "test: add invariant"
mkdir -p "$repo/crates/jinnd-context/src"
printf 'impl\n' >"$repo/crates/jinnd-context/src/lib.rs"
commit_all_as "$repo" implementer implementer@example.invalid "feat: add implementation"
assert_passes "$repo" "$base" "$(git -C "$repo" rev-parse HEAD)" \
  "separate verifier and implementation authors"

repo=$(new_fixture adapter-bootstrap)
base=$(git -C "$repo" rev-parse HEAD)
mkdir -p "$repo/tests/invariants"
printf 'test\n' >"$repo/tests/invariants/new.rs"
printf 'new adapter module\n' >"$repo/crates/jinnd-adapter/src/bootstrap.rs"
commit_all_as "$repo" bootstrap bootstrap@example.invalid "test: bootstrap adapter"
assert_passes "$repo" "$base" "$(git -C "$repo" rev-parse HEAD)" \
  "one-time adapter bootstrap only adds adapter files"

repo=$(new_fixture adapter-modification)
base=$(git -C "$repo" rev-parse HEAD)
mkdir -p "$repo/tests/invariants"
printf 'test\n' >"$repo/tests/invariants/new.rs"
printf 'modified\n' >"$repo/crates/jinnd-adapter/src/lib.rs"
commit_all_as "$repo" implementer implementer@example.invalid "fix: mix test and adapter modification"
assert_fails "$repo" "$base" "$(git -C "$repo" rev-parse HEAD)" \
  "adapter modifications are not bootstrap additions"

repo=$(new_fixture facade-exclusion)
base=$(git -C "$repo" rev-parse HEAD)
mkdir -p "$repo/tests/invariants" "$repo/crates/jinnd-api/src"
printf 'test\n' >"$repo/tests/invariants/new.rs"
printf 'facade\n' >"$repo/crates/jinnd-api/src/lib.rs"
commit_all_as "$repo" verifier verifier@example.invalid "test: extend facade and invariants"
assert_passes "$repo" "$base" "$(git -C "$repo" rev-parse HEAD)" \
  "jinnd-api remains excluded from implementation detection"

printf 'push two-key fixtures: ok\n'
