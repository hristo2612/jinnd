#!/usr/bin/env bash
# M2-K12 — the platform-gating GUARANTEE: a differential over what ran.
#
# Compares two `test-inventory.sh` outputs taken from the same commit on two
# platforms. Any test present on one and absent on the other, and any test
# silenced on one and live on the other, is a finding.
#
# The point of the shape. A scanner has to imagine the syntax that hides a
# test, so it is only ever as complete as its author's imagination, and both
# earlier rounds of this guard were defeated by a form nobody had listed. A
# differential imagines nothing: it asks each platform what it has and reports
# the difference. It cannot be walked past by a spelling, and it catches causes
# no scanner models -- a feature flag, a build script, an absent target, a
# dependency captured under a platform-only table.
#
# A divergence that is genuinely intended is DECLARED, in the allowlist file,
# with a reason, in the diff, where a reviewer sees it. That is the whole
# difference between a property that is verified elsewhere and one that has
# quietly become invisible.
#
# The guarantee is TEST PARITY and no more, and a green run here says so in its
# own output line. A test that runs on BOTH platforms and asserts nothing has
# the same name and the same state in both inventories; that is a VACUITY
# defect, and nothing in a differential can see it. The class and its remedy
# are stated in `test-inventory.sh`'s header.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/../.." && pwd)

label_a=${1:?usage: check-test-inventory.sh <labelA> <fileA> <labelB> <fileB> [allowlist]}
file_a=${2:?usage: check-test-inventory.sh <labelA> <fileA> <labelB> <fileB> [allowlist]}
label_b=${3:?usage: check-test-inventory.sh <labelA> <fileA> <labelB> <fileB> [allowlist]}
file_b=${4:?usage: check-test-inventory.sh <labelA> <fileA> <labelB> <fileB> [allowlist]}
allowlist=${5:-$repo_dir/.github/test-inventory-divergences.tsv}

# The floor exists because an empty inventory diffs clean against another empty
# one. A guard that passes hardest when it has measured nothing is the failure
# this packet is named after.
FLOOR=${TEST_INVENTORY_FLOOR:-100}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM

for pair in "$label_a:$file_a" "$label_b:$file_b"; do
  label=${pair%%:*}
  file=${pair#*:}
  if [[ ! -s "$file" ]]; then
    echo "differential: the $label inventory is missing or empty ($file) — refusing to pass unchecked" >&2
    exit 1
  fi
  count=$(wc -l <"$file" | tr -d ' ')
  if (( count < FLOOR )); then
    echo "differential: the $label inventory holds $count tests, below the floor of $FLOOR — refusing to pass vacuously" >&2
    exit 1
  fi
  if LC_ALL=C awk -F'\t' 'NF != 5 { exit 1 }' "$file"; then :; else
    echo "differential: the $label inventory is not the five-column form test-inventory.sh emits — refusing to pass unchecked" >&2
    exit 1
  fi
done

# An allowlist entry is `package<TAB>kind<TAB>target<TAB>test<TAB># reason`.
# The reason is mandatory: an undocumented exemption is the invisible gate
# wearing a different hat.
: >"$work/allowed.txt"
if [[ -f "$allowlist" ]]; then
  line_no=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line_no=$((line_no + 1))
    [[ -z "${line// /}" || "$line" == \#* ]] && continue
    key=$(printf '%s' "$line" | cut -f1-4)
    reason=$(printf '%s' "$line" | cut -f5-)
    if [[ "$(printf '%s' "$line" | awk -F'\t' '{print NF}')" -lt 5 || -z "${reason//[#[:space:]]/}" ]]; then
      echo "differential: $allowlist:$line_no declares an exemption with no reason — refusing" >&2
      exit 1
    fi
    printf '%s\n' "$key" >>"$work/allowed.txt"
  done <"$allowlist"
fi

key_state() { LC_ALL=C sort -u "$1" | LC_ALL=C awk -F'\t' '{ printf "%s\t%s\t%s\t%s\t%s\n", $1, $2, $3, $4, $5 }'; }
keys() { LC_ALL=C cut -f1-4 "$1" | LC_ALL=C sort -u; }

key_state "$file_a" >"$work/a.tsv"
key_state "$file_b" >"$work/b.tsv"
keys "$file_a" >"$work/a.keys"
keys "$file_b" >"$work/b.keys"

report() { # <headline> <file of keys>
  local headline=$1 file=$2
  [[ -s "$file" ]] || return 0
  while IFS= read -r key; do
    grep -qxF -- "$key" "$work/allowed.txt" && continue
    printf '  %s\t%s\n' "$headline" "$key" >>"$work/findings.txt"
  done <"$file"
}

: >"$work/findings.txt"
LC_ALL=C comm -23 "$work/a.keys" "$work/b.keys" >"$work/only_a.keys"
LC_ALL=C comm -13 "$work/a.keys" "$work/b.keys" >"$work/only_b.keys"
report "present on $label_a, absent on $label_b:" "$work/only_a.keys"
report "present on $label_b, absent on $label_a:" "$work/only_b.keys"

# Same test on both, silenced on one of them.
LC_ALL=C join -t$'\t' -j 1 \
  <(LC_ALL=C awk -F'\t' '{ printf "%s\x1f%s\x1f%s\x1f%s\t%s\n", $1, $2, $3, $4, $5 }' "$work/a.tsv" | LC_ALL=C sort) \
  <(LC_ALL=C awk -F'\t' '{ printf "%s\x1f%s\x1f%s\x1f%s\t%s\n", $1, $2, $3, $4, $5 }' "$work/b.tsv" | LC_ALL=C sort) \
  | LC_ALL=C awk -F'\t' '$2 != $3 { gsub(/\x1f/, "\t", $1); printf "%s\t%s\t%s\n", $1, $2, $3 }' >"$work/state.tsv"

while IFS=$'\t' read -r package kind target name state_a state_b; do
  [[ -n "${package:-}" ]] || continue
  key=$(printf '%s\t%s\t%s\t%s' "$package" "$kind" "$target" "$name")
  grep -qxF -- "$key" "$work/allowed.txt" && continue
  printf '  %s on %s but %s on %s:\t%s\n' "$state_a" "$label_a" "$state_b" "$label_b" "$key" >>"$work/findings.txt"
done <"$work/state.tsv"

a_count=$(wc -l <"$file_a" | tr -d ' ')
b_count=$(wc -l <"$file_b" | tr -d ' ')

if [[ -s "$work/findings.txt" ]]; then
  echo "differential: a test exists on one platform and not the other:"
  LC_ALL=C sort "$work/findings.txt"
  echo
  echo "A property that does not run on every platform we ship is not verified"
  echo "there. Fix the property or implement that platform's honest path — never"
  echo "gate the test. If the divergence is genuinely intended, DECLARE it in"
  echo "${allowlist#"$repo_dir"/} with a reason, so it is visible in review."
  exit 1
fi

allowed=$(wc -l <"$work/allowed.txt" | tr -d ' ')
echo "differential: ok; $label_a $a_count tests, $label_b $b_count tests," \
     "no undeclared divergence ($allowed declared) — this decides test PARITY only;" \
     "a body that runs on both and asserts nothing is vacuity, which no inventory can see"
