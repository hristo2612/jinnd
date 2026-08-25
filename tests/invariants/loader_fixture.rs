#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use jinnd_api::{
    Activation, EntryId, FiberId, FiberState, Inject, IsolationBinding, Kernel, KernelFuture,
    PluginContract, PluginRef, Profile, ProfileEntry, Realm, ServiceContract, ServiceHandle,
    ServiceResolver, ServiceType,
};

use crate::support::expect_ok;

pub const COUNT: &str = "jinn.test/count";
pub const PROVIDER: &str = "jinn.test/provider";
pub const CONSUMER: &str = "jinn.test/consumer";
pub const SERVICE: &str = "jinn.test/loader-service";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub entry: String,
    pub value: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Moment {
    Activated(String, u32),
    Observed(String, u32, u64),
}

pub type Log = Arc<Mutex<Vec<Moment>>>;

#[derive(Debug)]
pub struct FixtureService(pub u32);

impl ServiceContract for FixtureService {
    type Observation = u32;

    const NAME: &'static str = SERVICE;

    fn observe(&self) -> u32 {
        self.0
    }
}

#[derive(Debug)]
struct NeedsFixture {
    service: ServiceHandle<FixtureService>,
}

impl Inject for NeedsFixture {
    fn declare() -> Vec<ServiceType> {
        vec![ServiceType::of::<FixtureService>()]
    }

    fn inject<R: ServiceResolver + ?Sized>(resolver: &R) -> Result<Self, jinnd_api::KernelError> {
        Ok(Self {
            service: resolver.resolve::<FixtureService>()?,
        })
    }
}

#[derive(Debug)]
struct Counting {
    entry: String,
    log: Log,
}

impl PluginContract for Counting {
    type Config = u32;
    type Dependencies = ();

    const NAME: &'static str = COUNT;

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, ()>,
        config: u32,
    ) -> KernelFuture<'a, ()> {
        push(&self.log, Moment::Activated(self.entry.clone(), config));
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct Observing {
    entry: String,
    log: Log,
}

impl PluginContract for Observing {
    type Config = u32;
    type Dependencies = NeedsFixture;

    const NAME: &'static str = CONSUMER;

    fn activate<'a>(
        &'a self,
        activation: Activation<'a, NeedsFixture>,
        _config: u32,
    ) -> KernelFuture<'a, ()> {
        let handle = &activation.dependencies.service;
        push(
            &self.log,
            Moment::Observed(
                self.entry.clone(),
                handle.service.observe(),
                handle.generation.0,
            ),
        );
        Box::pin(async { Ok(()) })
    }
}

pub fn log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

pub fn register(kernel: &impl Kernel, log: &Log) {
    let count_log = Arc::clone(log);
    expect_ok(
        kernel.register_package(COUNT, move |config: Config| {
            Ok((
                Counting {
                    entry: config.entry,
                    log: Arc::clone(&count_log),
                },
                config.value,
            ))
        }),
        "counting package should register",
    );
    let observe_log = Arc::clone(log);
    expect_ok(
        kernel.register_package(CONSUMER, move |config: Config| {
            Ok((
                Observing {
                    entry: config.entry,
                    log: Arc::clone(&observe_log),
                },
                config.value,
            ))
        }),
        "consumer package should register",
    );
    expect_ok(
        kernel.register_provider_package(PROVIDER, |config: Config| {
            Ok(Arc::new(FixtureService(config.value)))
        }),
        "provider package should register",
    );
}

pub fn id(value: &str) -> EntryId {
    EntryId(value.to_owned())
}

pub fn entry(name: &str, package: &str, value: u32) -> ProfileEntry<Config> {
    ProfileEntry {
        id: id(name),
        plugin: PluginRef {
            package: package.to_owned(),
            version: "1".to_owned(),
            artifact_hash: String::new(),
        },
        config: Config {
            entry: name.to_owned(),
            value,
        },
        disabled: false,
        parent: None,
        isolation: Vec::new(),
    }
}

pub fn group(name: &str) -> ProfileEntry<Config> {
    entry(name, jinnd_api::GROUP_PACKAGE, 0)
}

pub fn child(mut entry: ProfileEntry<Config>, parent: &str) -> ProfileEntry<Config> {
    entry.parent = Some(id(parent));
    entry
}

pub fn disabled(mut entry: ProfileEntry<Config>) -> ProfileEntry<Config> {
    entry.disabled = true;
    entry
}

pub fn isolated(
    mut entry: ProfileEntry<Config>,
    service: &str,
    realm: Realm,
) -> ProfileEntry<Config> {
    entry.isolation.push(IsolationBinding {
        service: service.to_owned(),
        realm,
    });
    entry
}

pub fn profile(entries: Vec<ProfileEntry<Config>>) -> Profile<Config> {
    Profile { entries }
}

pub async fn reconcile(kernel: &impl Kernel, entries: Vec<ProfileEntry<Config>>) {
    let report = expect_ok(
        kernel.reconcile(profile(entries)).await,
        "profile reconcile should settle",
    );
    assert!(
        report.errors.is_empty(),
        "unexpected entry faults: {report:?}"
    );
    expect_ok(
        kernel.wait_for_quiescence().await,
        "loader fibers should quiesce",
    );
}

pub fn fiber(kernel: &impl Kernel, entry: &str) -> FiberId {
    kernel
        .entry_fiber(&id(entry))
        .unwrap_or_else(|| panic!("entry {entry:?} should have a fiber"))
}

pub fn state(kernel: &impl Kernel, entry: &str) -> Option<FiberState> {
    kernel
        .entry_fiber(&id(entry))
        .map(|fiber| kernel.state(fiber))
}

pub fn activations(log: &Log, entry: &str) -> usize {
    moments(log)
        .iter()
        .filter(|moment| matches!(moment, Moment::Activated(id, _) if id == entry))
        .count()
}

pub fn observations(log: &Log, entry: &str) -> Vec<(u32, u64)> {
    moments(log)
        .iter()
        .filter_map(|moment| match moment {
            Moment::Observed(id, value, generation) if id == entry => Some((*value, *generation)),
            _ => None,
        })
        .collect()
}

fn push(log: &Log, moment: Moment) {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .push(moment);
}

fn moments(log: &Log) -> Vec<Moment> {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}
