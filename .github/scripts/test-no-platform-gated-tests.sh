#!/usr/bin/env bash
# Red-first proof for check-no-platform-gated-tests.sh, which is a HEURISTIC.
#
# A guard nobody has watched FAIL is not a guard, it is a script that exits 0.
# So every form the heuristic claims to catch gets a fixture here demonstrated
# to make it exit non-zero, and every form it must NOT catch gets one
# demonstrated to leave it green — otherwise a bare `exit 1` would pass this
# file just as well as a real guard does.
#
# Read what this file is NOT. Green here means the heuristic behaves as
# specified over the forms listed here; it has never meant, and after round 3
# no longer pretends to mean, that no test can silently vanish on a platform.
# Round 2 shipped this suite green while `#! [cfg(target_os = "macos")]` walked
# past the scanner, because no fixture had token whitespace in it — a fixture
# suite can only be as complete as the same imagination that wrote the scanner.
# The guarantee is `test-check-test-inventory.sh` and the differential it
# proves, which measures what each platform actually compiled in.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
fixture_root=$(mktemp -d)

cleanup() {
  rm -rf "$fixture_root"
}
trap cleanup EXIT HUP INT TERM

failures=0

# A fixture is a miniature repo: the guard scripts as they ship, plus one
# integration test file. The guard resolves its own repo root from its own
# location, so a copy scans the copy — the way the round-1 defect was found.
new_fixture() {
  local name=$1
  local repo="$fixture_root/$name"

  mkdir -p "$repo/.github/scripts" "$repo/crates/example/tests"
  cp "$script_dir"/check-no-platform-gated-tests.sh "$repo/.github/scripts/"
  cp "$script_dir"/scan-conditional-tests.pl "$repo/.github/scripts/"
  printf '%s\n' "$repo"
}

case_file() {
  printf '%s/crates/example/tests/case.rs' "$1"
}

# The guard must REFUSE: this form can make a test vanish on a platform we ship.
assert_refuses() {
  local repo=$1
  local scenario=$2

  if output=$(bash "$repo/.github/scripts/check-no-platform-gated-tests.sh" 2>&1); then
    printf 'FAIL: guard accepted a form it claims to catch: %s\n%s\n' "$scenario" "$output" >&2
    failures=$((failures + 1))
    return
  fi
  if [[ "$output" != *"gated to a platform or silenced"* ]]; then
    printf 'FAIL: guard refused but not loudly: %s\n%s\n' "$scenario" "$output" >&2
    failures=$((failures + 1))
    return
  fi
  printf '  refuses: %s\n' "$scenario"
}

# The guard must ACCEPT: without these, `exit 1` satisfies every case above.
assert_accepts() {
  local repo=$1
  local scenario=$2

  if ! output=$(bash "$repo/.github/scripts/check-no-platform-gated-tests.sh" 2>&1); then
    printf 'FAIL: guard refused a legitimate test: %s\n%s\n' "$scenario" "$output" >&2
    failures=$((failures + 1))
    return
  fi
  printf '  accepts: %s\n' "$scenario"
}

printf 'forms the guard must refuse:\n'

repo=$(new_fixture outer-item)
cat >"$(case_file "$repo")" <<'RS'
#[test]
#[cfg(target_os = "macos")]
fn the_property_holds() {}
RS
assert_refuses "$repo" 'outer item-level #[cfg(target_os = ...)]'

repo=$(new_fixture inner-crate)
cat >"$(case_file "$repo")" <<'RS'
#![cfg(target_os = "macos")]

#[test]
fn the_property_holds() {}
RS
assert_refuses "$repo" 'inner crate-level #![cfg(target_os = ...)] — the round-1 blocker'

repo=$(new_fixture inner-module)
cat >"$(case_file "$repo")" <<'RS'
mod authority {
    #![cfg(target_os = "linux")]

    #[test]
    fn the_property_holds() {}
}
RS
assert_refuses "$repo" 'inner module-level #![cfg(target_os = ...)]'

repo=$(new_fixture multi-line)
cat >"$(case_file "$repo")" <<'RS'
#[test]
#[cfg(
    target_os = "macos"
)]
fn the_property_holds() {}
RS
assert_refuses "$repo" 'attribute spread across lines'

repo=$(new_fixture nested-any)
cat >"$(case_file "$repo")" <<'RS'
#[test]
#[cfg(any(target_os = "macos", target_os = "ios"))]
fn the_property_holds() {}
RS
assert_refuses "$repo" 'platform predicate nested inside any()'

repo=$(new_fixture negated)
cat >"$(case_file "$repo")" <<'RS'
#[test]
#[cfg(not(target_os = "linux"))]
fn the_property_holds() {}
RS
assert_refuses "$repo" 'negated platform predicate'

repo=$(new_fixture cfg-attr-ignore)
cat >"$(case_file "$repo")" <<'RS'
#[test]
#[cfg_attr(target_os = "linux", ignore)]
fn the_property_holds() {}
RS
assert_refuses "$repo" 'cfg_attr silencing on one platform'

repo=$(new_fixture bare-ignore)
cat >"$(case_file "$repo")" <<'RS'
#[test]
#[ignore]
fn the_property_holds() {}
RS
assert_refuses "$repo" 'bare #[ignore]'

repo=$(new_fixture ignore-reason)
cat >"$(case_file "$repo")" <<'RS'
#[test]
#[ignore = "slow on the runner"]
fn the_property_holds() {}
RS
assert_refuses "$repo" '#[ignore = "..."] carrying a reason'

repo=$(new_fixture windows-only)
cat >"$(case_file "$repo")" <<'RS'
#[test]
#[cfg(windows)]
fn the_property_holds() {}
RS
assert_refuses "$repo" '#[cfg(windows)] — runs on no platform we ship'

repo=$(new_fixture target-family)
cat >"$(case_file "$repo")" <<'RS'
#[test]
#[cfg(target_family = "wasm")]
fn the_property_holds() {}
RS
assert_refuses "$repo" 'target_family predicate'

repo=$(new_fixture target-arch)
cat >"$(case_file "$repo")" <<'RS'
#[test]
#[cfg(target_arch = "aarch64")]
fn the_property_holds() {}
RS
assert_refuses "$repo" 'target_arch predicate'

repo=$(new_fixture runtime-cfg-macro)
cat >"$(case_file "$repo")" <<'RS'
#[test]
fn the_property_holds() {
    if cfg!(target_os = "linux") {
        return;
    }
}
RS
assert_refuses "$repo" 'cfg!() runtime skip — the same move without an attribute'

repo=$(new_fixture whitespace-inner)
cat >"$(case_file "$repo")" <<'RS'
#! [cfg(target_os = "macos")]

#[test]
fn the_property_holds() {}
RS
assert_refuses "$repo" 'token whitespace in `#! [cfg(...)]` — the round-2 blocker'

repo=$(new_fixture whitespace-outer)
cat >"$(case_file "$repo")" <<'RS'
#[test]
# [cfg(target_os = "linux")]
fn the_property_holds() {}
RS
assert_refuses "$repo" 'token whitespace in `# [cfg(...)]`'

repo=$(new_fixture whitespace-macro)
cat >"$(case_file "$repo")" <<'RS'
#[test]
fn the_property_holds() {
    if cfg ! (target_os = "macos") {
        return;
    }
}
RS
assert_refuses "$repo" 'token whitespace in `cfg ! (...)`'

printf 'forms the guard must accept:\n'

repo=$(new_fixture feature-gate)
cat >"$(case_file "$repo")" <<'RS'
#![cfg(not(feature = "loom"))]

#[test]
fn the_property_holds() {}
RS
assert_accepts "$repo" 'feature gate — the suite-wide loom exclusion is not a platform gate'

repo=$(new_fixture unix-gate)
cat >"$(case_file "$repo")" <<'RS'
#[test]
#[cfg(unix)]
fn the_property_holds() {}
RS
assert_accepts "$repo" '#[cfg(unix)] — constant-true on every platform we ship'

repo=$(new_fixture comment-mention)
cat >"$(case_file "$repo")" <<'RS'
// Never write #[cfg(target_os = "macos")] here, and never #[ignore].
/* #![cfg(target_os = "linux")] would hide this whole file. */
#[test]
fn the_property_holds() {
    let forbidden = "#[cfg(target_os = \"macos\")]";
    assert!(!forbidden.is_empty());
}
RS
assert_accepts "$repo" 'the forms named only in comments and string literals'

repo=$(new_fixture ordinary)
cat >"$(case_file "$repo")" <<'RS'
#[test]
fn the_property_holds() {}
RS
assert_accepts "$repo" 'an ordinary integration test'

repo=$(new_fixture empty-tree)
rm -rf "$repo/crates/example/tests"
if bash "$repo/.github/scripts/check-no-platform-gated-tests.sh" >/dev/null 2>&1; then
  printf 'FAIL: guard passed with nothing to scan\n' >&2
  failures=$((failures + 1))
else
  printf '  refuses: a tree with no integration tests — never pass vacuously\n'
fi

if [[ $failures -ne 0 ]]; then
  printf 'heuristic fixtures: %d FAILED\n' "$failures" >&2
  exit 1
fi

printf 'heuristic fixtures: ok            (16 refusals, 4 acceptances, 1 vacuity)\n'
