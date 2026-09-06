# R10-SWEEP — superseded (PLA-363)

**Status: withdrawn by COO, 2026-09-05.** The first-principles audit found no
interface improvement in splitting the existing `LocalTopics` module solely
for its line count. [SOURCE-OF-TRUTH R10](../../SOURCE-OF-TRUTH.md#5-design-rules)
now records the scoped cohesion exception and its unchanged obligations.

The former file-split and LOC-automation plan is preserved in Git history.
This card authorizes no implementation, new CI quota gate or blanket test-suite
splitting. Reconsider a split when an actual responsibility boundary earns it.
