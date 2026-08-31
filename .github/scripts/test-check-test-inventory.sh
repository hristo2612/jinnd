#!/usr/bin/env bash
# Red-first proof for check-test-inventory.sh (M2-K12 round 3).
#
# A guard nobody has watched FAIL is not a guard, it is a script that exits 0 —
# and this packet exists because two guards in a row were exactly that. So the
# differential gets the same treatment its predecessor got: every divergence it
# claims to catch is demonstrated here to make it exit non-zero, every shape it
# must NOT catch is demonstrated to leave it green (without which a bare
# `exit 1` would satisfy the file), and its vacuity refusals are proven, since
# an empty inventory diffs clean against another empty one.
#
# The last two cases are the ones that matter most: they show the differential
# catching divergences that carry NO conditional-compilation syntax at all, and
# that the shipped scanner therefore passes. That is the argument for inverting
# the guard rather than teaching the scanner one more form.
#
# What no case here proves, because no case CAN: a test that runs on both
# platforms and asserts nothing. It is present and green in both inventories,
# identical in name and state, and the differential is right to accept it. That
# is a vacuity defect, a separate class, and the acceptance cases below assert
# only that a passing differential says so out loud.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
check="$script_dir/check-test-inventory.sh"
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT HUP INT TERM

failures=0
export TEST_INVENTORY_FLOOR=3

# A realistic small inventory: two packages, three targets, five tests.
baseline() {
  cat <<'TSV'
jinnd-daemon	test	keystore	authority::the_data_root_alone_cannot_decrypt_the_store	test
jinnd-daemon	test	keystore	journal::an_fs_effect_and_a_keystore_effect_both_survive	test
jinnd-daemon	test	suspend	a_suspended_fiber_keeps_its_effects	test
jinnd-wasm	lib	jinnd_wasm	hostkeystore::tests::source::a_readable_source_derives	test
jinnd-wasm	lib	jinnd_wasm	hostprocess::tests::a_child_is_reaped	test
TSV
}

# `mktemp` gives each case its own pair so a case cannot leak into the next.
new_case() {
  local name=$1
  local dir="$root/$name"
  mkdir -p "$dir"
  baseline >"$dir/linux.tsv"
  baseline >"$dir/macos.tsv"
  : >"$dir/allow.tsv"
  printf '%s\n' "$dir"
}

run_check() { # <dir>
  bash "$check" linux "$1/linux.tsv" macos "$1/macos.tsv" "$1/allow.tsv" 2>&1
}

assert_refuses() { # <dir> <scenario> <expected substring>
  local dir=$1 scenario=$2 expect=$3 output
  if output=$(run_check "$dir"); then
    printf 'FAIL: differential accepted a divergence it claims to catch: %s\n%s\n' "$scenario" "$output" >&2
    failures=$((failures + 1))
    return
  fi
  if [[ "$output" != *"$expect"* ]]; then
    printf 'FAIL: differential refused but not for the stated reason: %s\n  wanted: %s\n%s\n' "$scenario" "$expect" "$output" >&2
    failures=$((failures + 1))
    return
  fi
  printf '  refuses: %s\n' "$scenario"
}

assert_accepts() { # <dir> <scenario>
  local dir=$1 scenario=$2 output
  if ! output=$(run_check "$dir"); then
    printf 'FAIL: differential refused a legitimate inventory pair: %s\n%s\n' "$scenario" "$output" >&2
    failures=$((failures + 1))
    return
  fi
  # A pass must say what it did NOT decide. The limit is load-bearing here: a
  # green differential is quoted as evidence, and a reader who takes it for
  # more than test parity repeats this packet's founding mistake.
  if [[ "$output" != *"vacuity, which no inventory can see"* ]]; then
    printf 'FAIL: the differential passed without naming its vacuity limit: %s\n%s\n' "$scenario" "$output" >&2
    failures=$((failures + 1))
    return
  fi
  printf '  accepts: %s\n' "$scenario"
}

echo "differential fixtures — divergences that must be REFUSED:"

# 1. The classic: a test gated away on one platform.
dir=$(new_case gated-away)
grep -v 'the_data_root_alone_cannot_decrypt_the_store' "$dir/linux.tsv" >"$dir/tmp" && mv "$dir/tmp" "$dir/linux.tsv"
assert_refuses "$dir" 'a test compiled in on macOS and absent on Linux' 'present on macos, absent on linux'

# 2. The same, mirrored — the guard must not be one-directional.
dir=$(new_case gated-away-mirrored)
grep -v 'a_suspended_fiber_keeps_its_effects' "$dir/macos.tsv" >"$dir/tmp" && mv "$dir/tmp" "$dir/macos.tsv"
assert_refuses "$dir" 'a test compiled in on Linux and absent on macOS' 'present on linux, absent on macos'

# 3. This packet's own defect: a dependency captured under a platform-only
#    table, so a whole test binary stops existing. No cfg attribute anywhere.
dir=$(new_case whole-target-vanishes)
grep -v $'\tkeystore\t' "$dir/linux.tsv" >"$dir/tmp" && mv "$dir/tmp" "$dir/linux.tsv"
assert_refuses "$dir" 'an entire test binary missing on Linux (the M2-K12 defect itself)' 'present on macos, absent on linux'

# 4. Silenced on one platform only — `cfg_attr(target_os = ..., ignore)`.
dir=$(new_case silenced-on-one)
sed 's/\(journal::an_fs_effect.*\)\ttest$/\1\tignored/' "$dir/linux.tsv" >"$dir/tmp" && mv "$dir/tmp" "$dir/linux.tsv"
assert_refuses "$dir" 'a test silenced with #[ignore] on Linux only' 'ignored on linux but test on macos'

# 5. Vacuity: an empty inventory diffs clean against anything.
dir=$(new_case empty-side)
: >"$dir/linux.tsv"
assert_refuses "$dir" 'an empty inventory on one side' 'missing or empty'

# 6. Vacuity: a build that produced almost nothing must not read as agreement.
dir=$(new_case below-floor)
head -2 "$dir/linux.tsv" >"$dir/tmp" && mv "$dir/tmp" "$dir/linux.tsv"
head -2 "$dir/macos.tsv" >"$dir/tmp" && mv "$dir/tmp" "$dir/macos.tsv"
assert_refuses "$dir" 'both inventories below the vacuity floor' 'below the floor'

# 7. A truncated or reshaped inventory must not be diffed as if it were whole.
dir=$(new_case malformed)
printf 'jinnd-daemon\ttest\tkeystore\n' >>"$dir/linux.tsv"
assert_refuses "$dir" 'an inventory that is not the five-column form' 'not the five-column form'

# 8. An exemption with no reason is the invisible gate wearing a different hat.
dir=$(new_case exemption-without-reason)
grep -v 'a_suspended_fiber_keeps_its_effects' "$dir/macos.tsv" >"$dir/tmp" && mv "$dir/tmp" "$dir/macos.tsv"
printf 'jinnd-daemon\ttest\tsuspend\ta_suspended_fiber_keeps_its_effects\n' >"$dir/allow.tsv"
assert_refuses "$dir" 'a declared divergence carrying no reason' 'no reason'

# 9. A test NAME absent from one platform's inventory with NO conditional-
#    compilation syntax behind it — deleted there, or never built there. Name
#    DELETION is exactly what this proves, and only that: the scanner has
#    nothing to report and the differential still refuses.
#
#    It does NOT demonstrate a body that returns early on one platform. Such a
#    test is present, enabled and green in BOTH inventories, so no fixture in
#    this file can catch it and none claims to — that is the vacuity limit
#    stated in `test-inventory.sh`'s header.
dir=$(new_case no-syntax-to-scan)
grep -v 'hostprocess::tests::a_child_is_reaped' "$dir/linux.tsv" >"$dir/tmp" && mv "$dir/tmp" "$dir/linux.tsv"
assert_refuses "$dir" 'a test name deleted on one platform, with no cfg syntax for any scanner to find' 'present on macos, absent on linux'

echo "differential fixtures — shapes that must be ACCEPTED:"

# 10. Identical inventories. Without this, `exit 1` passes every case above.
dir=$(new_case identical)
assert_accepts "$dir" 'two identical inventories'

# 11. A declared divergence WITH a reason is visible in review, so it passes.
dir=$(new_case declared-divergence)
grep -v 'a_suspended_fiber_keeps_its_effects' "$dir/macos.tsv" >"$dir/tmp" && mv "$dir/tmp" "$dir/macos.tsv"
printf 'jinnd-daemon\ttest\tsuspend\ta_suspended_fiber_keeps_its_effects\t# fixture: an intended divergence, stated\n' >"$dir/allow.tsv"
assert_accepts "$dir" 'a divergence declared with a reason'

# 12. Ordering and duplicates are an artefact of collection, not a divergence.
dir=$(new_case order-and-duplicates)
{ tac "$dir/macos.tsv" 2>/dev/null || tail -r "$dir/macos.tsv"; } >"$dir/tmp"
head -1 "$dir/macos.tsv" >>"$dir/tmp"
mv "$dir/tmp" "$dir/macos.tsv"
assert_accepts "$dir" 'the same tests in a different order, with a duplicate line'

# 13. Comments and blank lines in the allowlist are not exemptions-without-reason.
dir=$(new_case allowlist-comments)
printf '# a comment\n\n   \n' >"$dir/allow.tsv"
assert_accepts "$dir" 'an allowlist of only comments and blank lines'

if (( failures > 0 )); then
  echo "differential fixtures: FAILED ($failures)" >&2
  exit 1
fi

echo "differential fixtures: ok            (9 refusals, 4 acceptances)"
