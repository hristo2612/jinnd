//! Realm isolation through the loader: relevance is decided by epoch identity,
//! never by loader guessing (LAW §3 epoch gating; R9 no silent replacement).

mod common;

use common::Grab;
use common::{activations, deactivations, entry, fixture, id, observations, profile};
use jinnd_api::{FiberState, IsolationBinding, ProfileEntry, Realm};

fn isolate(mut e: ProfileEntry<u32>, service: &str, realm: Realm) -> ProfileEntry<u32> {
    e.isolation.push(IsolationBinding {
        service: service.to_owned(),
        realm,
    });
    e
}

const SVC: &str = "svc.fixture";

#[tokio::test(flavor = "current_thread")]
async fn injector_isolation_relevance_is_epoch_decided() {
    let (loader, _registry, log) = fixture();
    let provider = entry("bar", "test/provider", 7);
    let consumer = entry("foo", "test/consumer", 0);

    // Root provider and consumer connect.
    loader
        .reconcile(profile(vec![provider.clone(), consumer.clone()]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 1);
    assert_eq!(observations(&log, "foo").len(), 1);
    let consumer_fiber = loader.entry_fiber(&id("foo")).grab();

    // Relevant injector isolation: consumer maps the service to its own local
    // realm — it unloads once and becomes pending; the provider stays active.
    let isolated = isolate(consumer.clone(), SVC, Realm::Local(id("foo")));
    loader
        .reconcile(profile(vec![provider.clone(), isolated.clone()]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(deactivations(&log, "foo"), 1);
    assert_eq!(activations(&log, "foo"), 1);
    assert_eq!(loader.entry_fiber(&id("foo")).grab(), consumer_fiber);
    assert_eq!(
        loader.fiber_state(consumer_fiber),
        Some(FiberState::Pending)
    );
    assert_eq!(deactivations(&log, "bar"), 0);

    // Irrelevant injector isolation on top: nothing moves.
    let doubly = isolate(
        isolated.clone(),
        "svc.unrelated",
        Realm::Shared("q".to_owned()),
    );
    loader
        .reconcile(profile(vec![provider.clone(), doubly.clone()]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 1);
    assert_eq!(deactivations(&log, "foo"), 1);

    // Removing the relevant mapping (keeping the irrelevant one) reactivates.
    let relieved = isolate(
        consumer.clone(),
        "svc.unrelated",
        Realm::Shared("q".to_owned()),
    );
    loader
        .reconcile(profile(vec![provider.clone(), relieved.clone()]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 2);
    assert_eq!(deactivations(&log, "foo"), 1);

    // Removing the last irrelevant mapping is inert.
    loader
        .reconcile(profile(vec![provider, consumer]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 2);
    assert_eq!(deactivations(&log, "foo"), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn provider_isolation_relevance_reaches_only_matching_consumers() {
    let (loader, _registry, log) = fixture();
    let provider = entry("bar", "test/provider", 7);
    let consumer = entry("foo", "test/consumer", 0);
    loader
        .reconcile(profile(vec![provider.clone(), consumer.clone()]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 1);

    // Relevant provider isolation: provider moves its service to a local realm;
    // the root consumer unloads once and waits.
    let local = isolate(provider.clone(), SVC, Realm::Local(id("bar")));
    loader
        .reconcile(profile(vec![local.clone(), consumer.clone()]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(deactivations(&log, "foo"), 1);
    assert_eq!(activations(&log, "foo"), 1);

    // Irrelevant provider isolation on top: nothing moves.
    let doubly = isolate(
        local.clone(),
        "svc.unrelated",
        Realm::Shared("q".to_owned()),
    );
    loader
        .reconcile(profile(vec![doubly, consumer.clone()]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 1);
    assert_eq!(deactivations(&log, "foo"), 1);

    // Returning the provider to the root realm reactivates the consumer.
    let relieved = isolate(
        provider.clone(),
        "svc.unrelated",
        Realm::Shared("q".to_owned()),
    );
    loader
        .reconcile(profile(vec![relieved.clone(), consumer.clone()]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 2);

    // Removing the last irrelevant provider mapping is inert.
    loader
        .reconcile(profile(vec![provider, consumer]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 2);
    assert_eq!(deactivations(&log, "foo"), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn groups_partition_providers_into_distinct_realms() {
    let (loader, _registry, log) = fixture();

    // alpha isolates the service locally; beta maps it to a shared realm.
    let alpha = isolate(
        entry("alpha", jinnd_api::GROUP_PACKAGE, 0),
        SVC,
        Realm::Local(id("alpha")),
    );
    let beta = isolate(
        entry("beta", jinnd_api::GROUP_PACKAGE, 0),
        SVC,
        Realm::Shared("beta-realm".to_owned()),
    );
    let mut alpha_provider = entry("alpha-bar", "test/provider", 1);
    alpha_provider.parent = Some(id("alpha"));
    let mut beta_provider = entry("beta-bar", "test/provider", 2);
    beta_provider.parent = Some(id("beta"));
    let outside = entry("outside", "test/consumer", 0);

    loader
        .reconcile(profile(vec![
            alpha.clone(),
            beta.clone(),
            alpha_provider.clone(),
            beta_provider.clone(),
            outside.clone(),
        ]))
        .await
        .grab();
    loader.quiesce().await;

    // Two independent provider fibers; the root consumer sees neither realm.
    assert_eq!(activations(&log, "alpha-bar"), 1);
    assert_eq!(activations(&log, "beta-bar"), 1);
    assert_eq!(activations(&log, "outside"), 0);

    // Consumers under alpha: inherited realm sees alpha's provider; an explicit
    // shared binding sees beta's; a fresh local realm sees nothing.
    let mut inherited = entry("inherited", "test/consumer", 0);
    inherited.parent = Some(id("alpha"));
    let mut explicit = isolate(
        entry("explicit", "test/consumer", 0),
        SVC,
        Realm::Shared("beta-realm".to_owned()),
    );
    explicit.parent = Some(id("alpha"));
    let mut fresh = isolate(
        entry("fresh", "test/consumer", 0),
        SVC,
        Realm::Local(id("fresh")),
    );
    fresh.parent = Some(id("alpha"));

    loader
        .reconcile(profile(vec![
            alpha,
            beta,
            alpha_provider,
            beta_provider,
            outside,
            inherited,
            explicit,
            fresh,
        ]))
        .await
        .grab();
    loader.quiesce().await;

    assert_eq!(
        observations(&log, "inherited"),
        vec![(1, observations(&log, "inherited")[0].1)]
    );
    assert_eq!(observations(&log, "explicit").len(), 1);
    assert_eq!(observations(&log, "explicit")[0].0, 2);
    assert_eq!(activations(&log, "fresh"), 0);
    let fresh_fiber = loader.entry_fiber(&id("fresh")).grab();
    assert_eq!(loader.fiber_state(fresh_fiber), Some(FiberState::Pending));
}
