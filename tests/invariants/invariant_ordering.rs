mod support;

use support::spec_case;

spec_case! {
    /// Paper origin: ordering and resolution-coherence theorem; SOURCE-OF-TRUTH §4 invariant I2.
    consumer_can_call_dying_provider_during_its_teardown,
    origin: "paper: ordering and resolution-coherence theorem / I2",
    test: "consumer may call dying provider during teardown",
    setup: ["active provider and consumer with one owned dependency snapshot", "consumer undo calls provider method"],
    actions: ["dispose provider and wait for quiescence"],
    expected: ["provider slot stops accepting new resolutions before teardown", "existing consumer handle remains callable during consumer undo", "consumer finishes before provider value disappears"]
}

spec_case! {
    /// Paper origin: ordering and resolution-coherence theorem; SOURCE-OF-TRUTH §4 invariant I2.
    one_activation_never_observes_mixed_provider_generations,
    origin: "paper: ordering and resolution-coherence theorem / I2",
    test: "one resolution per transition",
    setup: ["consumer owns two calls through one provider dependency snapshot"],
    actions: ["hot-swap provider between the two scheduled calls"],
    expected: ["first activation sees one generation only", "consumer fully unloads before a new activation captures replacement generation"]
}
