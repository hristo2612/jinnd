use jinnd_api::{FiberState, Kernel, Realm};

use crate::loader_fixture::{
    CONSUMER, PROVIDER, SERVICE, child, entry, fiber, group, id, isolated, log, observations,
    reconcile, register,
};

pub async fn run(stage: u8) {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    let group = isolated(group("group"), SERVICE, Realm::Local(id("group")));
    let provider = entry("bar", PROVIDER, 7);
    let consumer = entry("foo", CONSUMER, 0);
    reconcile(
        &kernel,
        vec![group.clone(), provider.clone(), consumer.clone()],
    )
    .await;
    let consumer_fiber = fiber(&kernel, "foo");
    assert_eq!(observations(&log, "foo").len(), 1);
    if stage == 0 {
        return;
    }

    let inside_consumer = child(consumer.clone(), "group");
    reconcile(
        &kernel,
        vec![group.clone(), provider.clone(), inside_consumer.clone()],
    )
    .await;
    assert_eq!(kernel.entry_fiber(&id("foo")), Some(consumer_fiber));
    assert_eq!(kernel.state(consumer_fiber), FiberState::Pending);
    if stage == 1 {
        return;
    }

    let inside_provider = child(provider.clone(), "group");
    reconcile(
        &kernel,
        vec![group.clone(), inside_provider.clone(), inside_consumer],
    )
    .await;
    assert_eq!(kernel.state(consumer_fiber), FiberState::Active);
    assert_eq!(observations(&log, "foo").len(), 2);
    if stage == 2 {
        return;
    }

    reconcile(
        &kernel,
        vec![group.clone(), inside_provider, consumer.clone()],
    )
    .await;
    assert_eq!(kernel.state(consumer_fiber), FiberState::Pending);
    if stage == 3 {
        return;
    }

    reconcile(&kernel, vec![group, provider, consumer]).await;
    assert_eq!(kernel.state(consumer_fiber), FiberState::Active);
    assert_eq!(observations(&log, "foo").len(), 3);
}
