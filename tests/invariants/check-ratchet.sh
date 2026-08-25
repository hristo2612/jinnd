#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/../.." && pwd)
manifest="$script_dir/Cargo.toml"
expected_source="$script_dir/expected-green.txt"
expected_red_source="$script_dir/expected-red-reasons.txt"

actual_cases=$(mktemp)
expected_cases=$(mktemp)
expected_red=$(mktemp)
case_output=$(mktemp)
targets=$(mktemp)

cleanup() {
  unlink "$actual_cases" 2>/dev/null || true
  unlink "$expected_cases" 2>/dev/null || true
  unlink "$expected_red" 2>/dev/null || true
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
sed -e 's/#.*$//' -e '/^[[:space:]]*$/d' "$expected_red_source" \
  | sort >"$expected_red"

stale=$(comm -23 "$expected_cases" "$actual_cases")
if [[ -n "$stale" ]]; then
  echo "ratchet: expected-green contains unknown cases:"
  echo "$stale"
  exit 1
fi

malformed=$(awk -F '|' 'NF != 2 || ($2 != "NO_KERNEL" && $2 != "FACADE_GAP")' "$expected_red")
if [[ -n "$malformed" ]]; then
  echo "ratchet: expected-red-reasons contains malformed entries:"
  echo "$malformed"
  exit 1
fi

duplicates=$(cut -d '|' -f 1 "$expected_red" | uniq -d)
if [[ -n "$duplicates" ]]; then
  echo "ratchet: expected-red-reasons contains duplicate cases:"
  echo "$duplicates"
  exit 1
fi

red_cases=$(cut -d '|' -f 1 "$expected_red")
stale_red=$(comm -23 <(printf '%s\n' "$red_cases") "$actual_cases")
if [[ -n "$stale_red" ]]; then
  echo "ratchet: expected-red-reasons contains unknown cases:"
  echo "$stale_red"
  exit 1
fi

overlap=$(comm -12 "$expected_cases" <(printf '%s\n' "$red_cases"))
if [[ -n "$overlap" ]]; then
  echo "ratchet: cases cannot be both expected green and expected red:"
  echo "$overlap"
  exit 1
fi

catalogued=$(printf '%s\n%s\n' "$(cat "$expected_cases")" "$red_cases" | sed '/^$/d' | sort -u)
unclassified=$(comm -23 "$actual_cases" <(printf '%s\n' "$catalogued"))
if [[ -n "$unclassified" ]]; then
  echo "ratchet: cases missing from the green/red catalog:"
  echo "$unclassified"
  exit 1
fi

case_count=0
green_count=0
red_count=0
no_kernel_count=0
facade_gap_count=0

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

  expected_reason=$(awk -F '|' -v case_id="$case_id" '$1 == case_id { print $2 }' "$expected_red")
  if cargo test --quiet -p jinnd-invariants --test "$target" "$case_name" \
    -- --exact --nocapture >"$case_output" 2>&1; then
    echo "ratchet: unlisted case is green: $case_id"
    tail -n 20 "$case_output"
    exit 1
  fi

  case "$expected_reason" in
    NO_KERNEL)
      reason_needle='NO_KERNEL:'
      no_kernel_count=$((no_kernel_count + 1))
      ;;
    FACADE_GAP)
      reason_needle='FACADE_GAP:'
      facade_gap_count=$((facade_gap_count + 1))
      ;;
  esac

  if ! grep -Fq "$reason_needle" "$case_output"; then
    echo "ratchet: red reason drift for $case_id; expected $expected_reason"
    tail -n 20 "$case_output"
    exit 1
  fi
  red_count=$((red_count + 1))
done <"$actual_cases"

echo "ratchet: ok; cases=$case_count expected-green=$green_count expected-red=$red_count no-kernel=$no_kernel_count facade-gap=$facade_gap_count"
