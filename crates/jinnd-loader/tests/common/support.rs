//! Log, moment, and entry-tree helpers shared by the loader's integration
//! tests (split from `mod.rs` by responsibility, R10).

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use jinnd_api::EntryId;

/// One recorded lifecycle moment of a fixture plugin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Moment {
    /// Entry activated with this config marker.
    Activated(String, u32),
    /// Entry's activation was withdrawn.
    Deactivated(String),
    /// Consumer observed a provider value and generation.
    Observed(String, u32, u64),
}

/// The shared log every fixture body appends to.
pub type Log = Arc<Mutex<Vec<Moment>>>;

pub fn log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

pub fn moments(log: &Log) -> Vec<Moment> {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

pub fn activations(log: &Log, entry: &str) -> usize {
    moments(log)
        .iter()
        .filter(|moment| matches!(moment, Moment::Activated(id, _) if id == entry))
        .count()
}

pub fn deactivations(log: &Log, entry: &str) -> usize {
    moments(log)
        .iter()
        .filter(|moment| matches!(moment, Moment::Deactivated(id) if id == entry))
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

/// Entry-tree building helpers shared by the integration tests.
pub fn id(text: &str) -> EntryId {
    EntryId(text.to_owned())
}

pub fn entry(name: &str, package: &str, config: u32) -> jinnd_api::ProfileEntry<u32> {
    jinnd_api::ProfileEntry {
        id: id(name),
        plugin: jinnd_api::PluginRef {
            package: package.to_owned(),
            version: "1".to_owned(),
            artifact_hash: String::new(),
        },
        config,
        disabled: false,
        parent: None,
        isolation: Vec::new(),
    }
}

pub fn profile(entries: Vec<jinnd_api::ProfileEntry<u32>>) -> jinnd_api::Profile<u32> {
    jinnd_api::Profile { entries }
}

/// Repo convention: no `unwrap`/`expect`. `grab` is the tests' one panicking
/// accessor, carrying the caller's location.
pub trait Grab<T> {
    fn grab(self) -> T;
}

impl<T, E: std::fmt::Debug> Grab<T> for Result<T, E> {
    #[track_caller]
    fn grab(self) -> T {
        match self {
            Ok(value) => value,
            Err(error) => panic!("unexpected error: {error:?}"),
        }
    }
}

impl<T> Grab<T> for Option<T> {
    #[track_caller]
    fn grab(self) -> T {
        match self {
            Some(value) => value,
            None => panic!("unexpectedly empty"),
        }
    }
}
