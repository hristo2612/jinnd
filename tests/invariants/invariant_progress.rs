mod support;

use support::spec_case;

const SUBSYSTEM: support::Subsystem = support::Subsystem::Fiber;
const FACADE_GAP_REASON: &str = "the facade cannot declare a dependency graph or observe static cycle diagnostics and quiescent inactivity";

spec_case! {
    /// Paper origin: progress theorem; SOURCE-OF-TRUTH §4 invariant I3.
    acyclic_dependency_precedence_always_reaches_quiescence,
    origin: "paper: progress theorem / I3",
    test: "acyclic dependency graph reaches quiescence",
    setup: ["acyclic graph qux -> foo -> bar and qux -> bar starts in arbitrary registration order"],
    actions: ["provide leaves", "wait with a bounded virtual-time deadline"],
    expected: ["wait completes before deadline", "every satisfiable fiber is active", "no transition remains in flight"]
}

spec_case! {
    /// Paper origin: progress theorem; SOURCE-OF-TRUTH §4 invariant I3.
    dependency_cycle_is_detected_and_left_cleanly_inactive,
    origin: "paper: progress theorem / I3",
    test: "cycle yields clean inactivity and quiescence",
    setup: ["fibers alpha, beta, gamma form a dependency cycle", "unrelated sibling is acyclic"],
    actions: ["register graph", "wait with a bounded virtual-time deadline"],
    expected: ["cycle is reported statically", "cycle members are pending or failed with no effects", "unrelated sibling reaches active", "kernel is quiescent"]
}
