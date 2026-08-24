#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/../.." && pwd)
manifest="$script_dir/Cargo.toml"
expected_source="$script_dir/expected-green.txt"

actual_cases=$(mktemp)
expected_cases=$(mktemp)
case_output=$(mktemp)
targets=$(mktemp)

cleanup() {
  unlink "$actual_cases" 2>/dev/null || true
  unlink "$expected_cases" 2>/dev/null || true
  unlink "$case_output" 2>/dev/null || true
  unlink "$targets" 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

cd "$repo_dir"

awk '
  /^\[\[test\]\]$/ { in_test = 1; next }
  in_test && /^name = "/ {
    name = $0
    sub(/^name = "/, "", name)
    sub(/"$/, "", name)
    print name
    in_test = 0
  }
' "$manifest" >"$targets"

while IFS= read -r target; do
  cargo test --quiet -p jinnd-invariants --test "$target" -- --list --format terse \
    | sed -n 's/: test$//p' \
    | while IFS= read -r case_name; do
        printf '%s::%s\n' "$target" "$case_name"
      done >>"$actual_cases"
done <"$targets"

sort -u -o "$actual_cases" "$actual_cases"
sed -e 's/#.*$//' -e '/^[[:space:]]*$/d' "$expected_source" \
  | sort -u >"$expected_cases"

stale=$(comm -23 "$expected_cases" "$actual_cases")
if [[ -n "$stale" ]]; then
  echo "ratchet: expected-green contains unknown cases:"
  echo "$stale"
  exit 1
fi

case_count=0
green_count=0
red_count=0

while IFS= read -r case_id; do
  target=${case_id%%::*}
  case_name=${case_id#*::}
  case_count=$((case_count + 1))

  if grep -Fqx "$case_id" "$expected_cases"; then
    if ! cargo test --quiet -p jinnd-invariants --test "$target" "$case_name" \
      -- --exact --nocapture >"$case_output" 2>&1; then
      echo "ratchet: listed case is red: $case_id"
      tail -n 20 "$case_output"
      exit 1
    fi
    green_count=$((green_count + 1))
    continue
  fi

  if cargo test --quiet -p jinnd-invariants --test "$target" "$case_name" \
    -- --exact --nocapture >"$case_output" 2>&1; then
    echo "ratchet: unlisted case is green: $case_id"
    tail -n 20 "$case_output"
    exit 1
  fi

  if ! grep -Fq 'NO_KERNEL:' "$case_output"; then
    echo "ratchet: unlisted case is red without an adapter NO_KERNEL reason: $case_id"
    tail -n 20 "$case_output"
    exit 1
  fi
  red_count=$((red_count + 1))
done <"$actual_cases"

echo "ratchet: ok; cases=$case_count expected-green=$green_count expected-red=$red_count"
