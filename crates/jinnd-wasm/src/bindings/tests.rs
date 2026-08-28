//! R12 pin: a guest-visible semantic change to the plugin world ships
//! with its version (M2-K4 round-2 ruling: the suspend/dispose
//! lifecycle is contract, so the world is 0.3.0 and the bundles under
//! `contracts/` mirror the classification).

const WORLD: &str = include_str!("../../../../wit/plugin.wit");
const FS_META: &str = include_str!("../../../../contracts/jinn-fs/metadata.toml");
const CLOCK_META: &str = include_str!("../../../../contracts/jinn-clock/metadata.toml");

#[test]
fn world_is_versioned_for_suspend_semantics() {
    assert!(WORLD.contains("package jinn:plugin@0.3.0;"));
    assert!(WORLD.contains("Suspend ≠ dispose"));
}

#[test]
fn bundles_mirror_lifecycle_classification() {
    // Durable world mutations: retained across suspend/incarnations.
    assert!(FS_META.contains("suspend"));
    assert!(FS_META.contains("dispose"));
    // Kernel registrations: released on suspend, re-armed on activate.
    assert!(CLOCK_META.contains("suspend"));
}
