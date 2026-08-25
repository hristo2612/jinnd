use jinnd_api::{FiberState, Kernel, Realm};

use crate::loader_fixture::{
    CONSUMER, PROVIDER, SERVICE, child, entry, fiber, group, id, isolated, log, observations,
    reconcile, register, state,
};

fn shared(name: &str) -> Realm {
    Realm::Shared(name.to_owned())
}

pub async fn partitioned_providers() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    let alpha = isolated(group("alpha"), SERVICE, Realm::Local(id("alpha")));
    let beta = isolated(group("beta"), SERVICE, shared("beta"));
    reconcile(
        &kernel,
        vec![
            alpha,
            beta,
            child(entry("alpha-bar", PROVIDER, 1), "alpha"),
            child(entry("beta-bar", PROVIDER, 2), "beta"),
            entry("outside", CONSUMER, 0),
        ],
    )
    .await;
    assert_eq!(state(&kernel, "alpha-bar"), Some(FiberState::Active));
    assert_eq!(state(&kernel, "beta-bar"), Some(FiberState::Active));
    assert_eq!(state(&kernel, "outside"), Some(FiberState::Pending));
}

pub async fn identical_realm_is_inert() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    let alpha = isolated(group("alpha"), SERVICE, Realm::Local(id("alpha")));
    let provider = child(entry("bar", PROVIDER, 1), "alpha");
    let consumer = child(entry("foo", CONSUMER, 0), "alpha");
    reconcile(
        &kernel,
        vec![alpha.clone(), provider.clone(), consumer.clone()],
    )
    .await;
    let provider_fiber = fiber(&kernel, "bar");
    let consumer_fiber = fiber(&kernel, "foo");
    let first = observations(&log, "foo");
    reconcile(&kernel, vec![alpha, provider, consumer]).await;
    assert_eq!(kernel.entry_fiber(&id("bar")), Some(provider_fiber));
    assert_eq!(kernel.entry_fiber(&id("foo")), Some(consumer_fiber));
    assert_eq!(observations(&log, "foo"), first);
}

pub async fn consumer_selects_realms() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    let alpha = isolated(group("alpha"), SERVICE, Realm::Local(id("alpha")));
    let beta = isolated(group("beta"), SERVICE, shared("beta"));
    let inherited = child(entry("inherited", CONSUMER, 0), "alpha");
    let explicit = child(
        isolated(entry("explicit", CONSUMER, 0), SERVICE, shared("beta")),
        "alpha",
    );
    let fresh = child(
        isolated(
            entry("fresh", CONSUMER, 0),
            SERVICE,
            Realm::Local(id("fresh")),
        ),
        "alpha",
    );
    reconcile(
        &kernel,
        vec![
            alpha,
            beta,
            child(entry("alpha-bar", PROVIDER, 1), "alpha"),
            child(entry("beta-bar", PROVIDER, 2), "beta"),
            inherited,
            explicit,
            fresh,
        ],
    )
    .await;
    assert_eq!(observations(&log, "inherited")[0].0, 1);
    assert_eq!(observations(&log, "explicit")[0].0, 2);
    assert_eq!(state(&kernel, "fresh"), Some(FiberState::Pending));
}

pub async fn redundant_ancestor_is_inert() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    let outer = group("outer");
    let inner = child(isolated(group("inner"), SERVICE, shared("custom")), "outer");
    let provider = child(entry("bar", PROVIDER, 7), "inner");
    let first = child(entry("first", CONSUMER, 0), "inner");
    let second = child(entry("second", CONSUMER, 0), "inner");
    reconcile(
        &kernel,
        vec![
            outer.clone(),
            inner.clone(),
            provider.clone(),
            first.clone(),
            second.clone(),
        ],
    )
    .await;
    let before_first = observations(&log, "first");
    let before_second = observations(&log, "second");
    let redundant = isolated(group("outer"), SERVICE, shared("custom"));
    reconcile(
        &kernel,
        vec![
            redundant,
            inner.clone(),
            provider.clone(),
            first.clone(),
            second.clone(),
        ],
    )
    .await;
    reconcile(&kernel, vec![outer, inner, provider, first, second]).await;
    assert_eq!(observations(&log, "first"), before_first);
    assert_eq!(observations(&log, "second"), before_second);
}

pub async fn changing_group_switches_provider() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    let alpha_provider = isolated(entry("alpha-bar", PROVIDER, 1), SERVICE, shared("alpha"));
    let beta_provider = isolated(entry("beta-bar", PROVIDER, 2), SERVICE, shared("beta"));
    let alpha_group = isolated(group("group"), SERVICE, shared("alpha"));
    let consumer = child(entry("foo", CONSUMER, 0), "group");
    reconcile(
        &kernel,
        vec![
            alpha_provider.clone(),
            beta_provider.clone(),
            alpha_group,
            consumer.clone(),
        ],
    )
    .await;
    let consumer_fiber = fiber(&kernel, "foo");
    assert_eq!(observations(&log, "foo")[0].0, 1);
    let beta_group = isolated(group("group"), SERVICE, shared("beta"));
    reconcile(
        &kernel,
        vec![alpha_provider, beta_provider, beta_group, consumer],
    )
    .await;
    let seen = observations(&log, "foo");
    assert_eq!(kernel.entry_fiber(&id("foo")), Some(consumer_fiber));
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[1].0, 2);
}

pub async fn moving_provider_retargets_external_consumers() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    let alpha_group = isolated(group("group"), SERVICE, shared("alpha"));
    let provider = child(entry("bar", PROVIDER, 7), "group");
    let alpha = isolated(entry("alpha", CONSUMER, 0), SERVICE, shared("alpha"));
    let beta = isolated(entry("beta", CONSUMER, 0), SERVICE, shared("beta"));
    reconcile(
        &kernel,
        vec![alpha_group, provider.clone(), alpha.clone(), beta.clone()],
    )
    .await;
    assert_eq!(observations(&log, "alpha").len(), 1);
    assert_eq!(state(&kernel, "beta"), Some(FiberState::Pending));
    let beta_group = isolated(group("group"), SERVICE, shared("beta"));
    reconcile(&kernel, vec![beta_group, provider, alpha, beta]).await;
    assert_eq!(state(&kernel, "alpha"), Some(FiberState::Pending));
    assert_eq!(state(&kernel, "beta"), Some(FiberState::Active));
    assert_eq!(observations(&log, "beta").len(), 1);
}
