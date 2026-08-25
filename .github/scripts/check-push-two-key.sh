#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <base-sha> <head-sha>" >&2
  exit 2
fi

base=$1
head=$2

changed=$(git diff --name-only "$base" "$head" -- tests/invariants/ | wc -l)
implementation=$(
  git diff --name-only "$base" "$head" -- crates/ \
    ':(exclude)crates/jinnd-api/**' | wc -l
)

if [[ "$changed" -eq 0 || "$implementation" -eq 0 ]]; then
  exit 0
fi

# Preserve the one-time adapter-bootstrap carve-out: alongside invariant changes,
# adapter files may only be added, and no other implementation crate may change.
non_bootstrap=$(
  git diff --name-only --diff-filter=AMDR "$base" "$head" -- crates/ \
    ':(exclude)crates/jinnd-api/**' \
    ':(exclude)crates/jinnd-adapter/**' | wc -l
)
adapter_not_added=$(
  git diff --name-only --diff-filter=MDR "$base" "$head" -- \
    crates/jinnd-adapter/ | wc -l
)

if [[ "$non_bootstrap" -eq 0 && "$adapter_not_added" -eq 0 ]]; then
  exit 0
fi

test_authors=$(
  git log --no-merges --format='%ae' "$base..$head" -- tests/invariants/ | sort -u
)
implementation_authors=$(
  git log --no-merges --format='%ae' "$base..$head" -- crates/ \
    ':(exclude)crates/jinnd-api/**' | sort -u
)

while IFS= read -r test_author; do
  [[ -n "$test_author" ]] || continue
  if ! printf '%s\n' "$implementation_authors" | grep -Fqx "$test_author"; then
    exit 0
  fi
done <<<"$test_authors"

echo "::error::two-key tripwire: pushed range touches BOTH tests/invariants/ and implementation crates without an independent verifier author in the commit trail (SOURCE-OF-TRUTH R2). Main is already updated; red CI requires investigation."
exit 1
