//! M1-P6c scope 2 (paper Def 23, R9): a provision for an occupied (key, realm)
//! from a DIFFERENT provider is refused — never a silent replacement. The
//! refused provider's activation fails cleanly (R11) and the first provider is
//! untouched; the same provider superseding its own generation (the hot-swap
//! lane) stays allowed.

#![cfg(not(feature = "loom"))]

mod common;

use common::{Grab, activations, entry, fixture, id, observations, profile};
use jinnd_api::FiberState;

#[tokio::test]
async fn a_second_provider_for_an_occupied_slot_fails_cleanly() {
    let (loader, _registry, log) = fixture();
    loader
        .reconcile(profile(vec![
            entry("first", "test/provider", 42),
            entry("second", "test/provider", 7),
            entry("watcher", "test/consumer", 1),
        ]))
        .await
        .grab();

    // The first provider is live; the duplicate rests failed, contained.
    let first = loader.entry_fiber(&id("first")).grab();
    let second = loader.entry_fiber(&id("second")).grab();
    assert_eq!(loader.fiber_state(first), Some(FiberState::Active));
    assert_eq!(
        loader.fiber_state(second),
        Some(FiberState::Failed),
        "a duplicate provision must fail its own activation, never replace (R9)"
    );

    // The consumer still observes the FIRST provider's value: the refused
    // provision never touched the occupied slot.
    assert_eq!(activations(&log, "first"), 1);
    let observed = observations(&log, "watcher");
    assert_eq!(
        observed.first().map(|(value, _)| *value),
        Some(42),
        "the occupied slot's value must be untouched by the refused duplicate"
    );
}

#[tokio::test]
async fn a_provider_reload_still_reprovides_after_its_own_withdrawal() {
    let (loader, _registry, log) = fixture();
    loader
        .reconcile(profile(vec![entry("only", "test/provider", 1)]))
        .await
        .grab();
    let fiber = loader.entry_fiber(&id("only")).grab();

    // A full clean reload withdraws the provider's own slot and provides
    // again: refusal must never close the ordinary reload lane.
    loader.update_entry(&id("only"), 2u32).await.grab();
    assert_eq!(
        loader.fiber_state(fiber),
        Some(FiberState::Active),
        "a provider reloading over its own withdrawn slot must stay providable"
    );
    assert_eq!(activations(&log, "only"), 2);
}
