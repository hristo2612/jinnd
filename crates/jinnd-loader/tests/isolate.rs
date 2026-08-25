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

#[tokio::test(flavor = "current_thread")]
async fn identical_realm_restatement_is_inert() {
    let (loader, _registry, log) = fixture();
    let alpha = isolate(
        entry("alpha", jinnd_api::GROUP_PACKAGE, 0),
        SVC,
        Realm::Local(id("alpha")),
    );
    let mut provider = entry("bar", "test/provider", 1);
    provider.parent = Some(id("alpha"));
    let mut consumer = entry("foo", "test/consumer", 0);
    consumer.parent = Some(id("alpha"));

    loader
        .reconcile(profile(vec![
            alpha.clone(),
            provider.clone(),
            consumer.clone(),
        ]))
        .await
        .grab();
    loader.quiesce().await;
    let generation = observations(&log, "foo")[0].1;

    // Writing the same isolation mapping again changes no generation and moves
    // no fiber.
    loader
        .reconcile(profile(vec![alpha, provider, consumer]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 1);
    assert_eq!(activations(&log, "bar"), 1);
    assert_eq!(observations(&log, "foo"), vec![(1, generation)]);
}

#[tokio::test(flavor = "current_thread")]
async fn changing_a_group_realm_switches_the_consumer_provider() {
    let (loader, _registry, log) = fixture();
    // Two shared realms each hold a provider.
    let alpha_realm = Realm::Shared("alpha".to_owned());
    let beta_realm = Realm::Shared("beta".to_owned());
    let alpha_provider = isolate(
        entry("alpha-bar", "test/provider", 1),
        SVC,
        alpha_realm.clone(),
    );
    let beta_provider = isolate(
        entry("beta-bar", "test/provider", 2),
        SVC,
        beta_realm.clone(),
    );
    // A group maps the service to alpha; its consumer follows the mapping.
    let group = isolate(
        entry("group", jinnd_api::GROUP_PACKAGE, 0),
        SVC,
        alpha_realm,
    );
    let mut consumer = entry("foo", "test/consumer", 0);
    consumer.parent = Some(id("group"));

    loader
        .reconcile(profile(vec![
            alpha_provider.clone(),
            beta_provider.clone(),
            group.clone(),
            consumer.clone(),
        ]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(observations(&log, "foo").len(), 1);
    assert_eq!(observations(&log, "foo")[0].0, 1);

    // Remap the group to beta: the consumer unloads once, reloads once, and the
    // new activation observes beta's provider.
    let regrouped = isolate(entry("group", jinnd_api::GROUP_PACKAGE, 0), SVC, beta_realm);
    loader
        .reconcile(profile(vec![
            alpha_provider,
            beta_provider,
            regrouped,
            consumer,
        ]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(deactivations(&log, "foo"), 1);
    assert_eq!(activations(&log, "foo"), 2);
    assert_eq!(observations(&log, "foo")[1].0, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn transfers_across_an_isolated_group_retarget_consumers() {
    let (loader, _registry, log) = fixture();
    let group = isolate(
        entry("group", jinnd_api::GROUP_PACKAGE, 0),
        SVC,
        Realm::Local(id("group")),
    );
    let provider = entry("bar", "test/provider", 7);
    let consumer = entry("foo", "test/consumer", 0);

    // Everything at root: connected.
    loader
        .reconcile(profile(vec![
            group.clone(),
            provider.clone(),
            consumer.clone(),
        ]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 1);

    // Consumer moves into the isolated group: unloads once, pending.
    let mut inside = consumer.clone();
    inside.parent = Some(id("group"));
    loader
        .reconcile(profile(vec![
            group.clone(),
            provider.clone(),
            inside.clone(),
        ]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(deactivations(&log, "foo"), 1);
    let consumer_fiber = loader.entry_fiber(&id("foo")).grab();
    assert_eq!(
        loader.fiber_state(consumer_fiber),
        Some(FiberState::Pending)
    );

    // Provider follows into the group: consumer reactivates against it.
    let mut provider_inside = provider.clone();
    provider_inside.parent = Some(id("group"));
    loader
        .reconcile(profile(vec![
            group.clone(),
            provider_inside.clone(),
            inside.clone(),
        ]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 2);

    // Consumer moves back out: unloads once, pending again.
    loader
        .reconcile(profile(vec![
            group.clone(),
            provider_inside,
            consumer.clone(),
        ]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(deactivations(&log, "foo"), 2);

    // Provider moves out too: consumer reactivates at root.
    loader
        .reconcile(profile(vec![group, provider, consumer]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "foo"), 3);
}
