use jinnd_api::{FiberState, Kernel, Realm};

use crate::loader_fixture::{
    CONSUMER, PROVIDER, SERVICE, activations, entry, fiber, id, isolated, log, reconcile, register,
    state,
};

pub async fn run(stage: u8) {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    let provider = entry("bar", PROVIDER, 7);
    let consumer = entry("foo", CONSUMER, 0);
    reconcile(&kernel, vec![provider.clone(), consumer.clone()]).await;
    let consumer_fiber = fiber(&kernel, "foo");
    assert_eq!(activations(&log, "foo"), 0);
    assert_eq!(crate::loader_fixture::observations(&log, "foo").len(), 1);
    if stage == 0 {
        return;
    }

    let consumer_local = isolated(consumer.clone(), SERVICE, Realm::Local(id("foo")));
    reconcile(&kernel, vec![provider.clone(), consumer_local.clone()]).await;
    assert_eq!(kernel.entry_fiber(&id("foo")), Some(consumer_fiber));
    assert_eq!(kernel.state(consumer_fiber), FiberState::Pending);
    assert_eq!(state(&kernel, "bar"), Some(FiberState::Active));
    if stage == 1 {
        return;
    }

    let consumer_double = isolated(
        consumer_local,
        "jinn.test/unrelated",
        Realm::Shared("irrelevant".to_owned()),
    );
    reconcile(&kernel, vec![provider.clone(), consumer_double]).await;
    assert_eq!(kernel.state(consumer_fiber), FiberState::Pending);
    assert_eq!(crate::loader_fixture::observations(&log, "foo").len(), 1);
    if stage == 2 {
        return;
    }

    let consumer_irrelevant = isolated(
        consumer.clone(),
        "jinn.test/unrelated",
        Realm::Shared("irrelevant".to_owned()),
    );
    reconcile(&kernel, vec![provider.clone(), consumer_irrelevant.clone()]).await;
    assert_eq!(crate::loader_fixture::observations(&log, "foo").len(), 2);
    assert_eq!(kernel.state(consumer_fiber), FiberState::Active);
    if stage == 3 {
        return;
    }

    reconcile(&kernel, vec![provider.clone(), consumer.clone()]).await;
    assert_eq!(crate::loader_fixture::observations(&log, "foo").len(), 2);
    assert_eq!(kernel.state(consumer_fiber), FiberState::Active);
    if stage == 4 {
        return;
    }

    let provider_local = isolated(provider.clone(), SERVICE, Realm::Local(id("bar")));
    reconcile(&kernel, vec![provider_local.clone(), consumer.clone()]).await;
    assert_eq!(kernel.state(consumer_fiber), FiberState::Pending);
    assert_eq!(crate::loader_fixture::observations(&log, "foo").len(), 2);
    if stage == 5 {
        return;
    }

    let provider_double = isolated(
        provider_local,
        "jinn.test/unrelated",
        Realm::Shared("irrelevant".to_owned()),
    );
    reconcile(&kernel, vec![provider_double, consumer.clone()]).await;
    assert_eq!(kernel.state(consumer_fiber), FiberState::Pending);
    assert_eq!(crate::loader_fixture::observations(&log, "foo").len(), 2);
    if stage == 6 {
        return;
    }

    let provider_irrelevant = isolated(
        provider.clone(),
        "jinn.test/unrelated",
        Realm::Shared("irrelevant".to_owned()),
    );
    reconcile(&kernel, vec![provider_irrelevant, consumer.clone()]).await;
    assert_eq!(kernel.state(consumer_fiber), FiberState::Active);
    assert_eq!(crate::loader_fixture::observations(&log, "foo").len(), 3);
    if stage == 7 {
        return;
    }

    reconcile(&kernel, vec![provider, consumer]).await;
    assert_eq!(kernel.state(consumer_fiber), FiberState::Active);
    assert_eq!(crate::loader_fixture::observations(&log, "foo").len(), 3);
}
