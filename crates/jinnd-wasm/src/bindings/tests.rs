//! R12 pin: a guest-visible semantic change to the plugin world ships
//! with its version (M2-K4 round-2 ruling: the suspend/dispose
//! lifecycle is contract, so the world is 0.4.0 and the bundles under
//! `contracts/` mirror the classification).

const WORLD: &str = include_str!("../../../../wit/plugin.wit");
const FS_META: &str = include_str!("../../../../contracts/jinn-fs/metadata.toml");
const CLOCK_META: &str = include_str!("../../../../contracts/jinn-clock/metadata.toml");

#[test]
fn world_is_versioned_for_suspend_semantics() {
    assert!(WORLD.contains("package jinn:plugin@0.4.0;"));
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

/// M2-K6 round 4 (R3; the world mirrors its bundles): the `process` and
/// `net` imports answer the bundle-declared errors on their own wire —
/// `output-truncated` is a variant a guest matches, never a string.
#[test]
fn process_and_net_imports_return_their_bundles_errors() {
    const PROCESS: &str = include_str!("../../../../contracts/jinn-process/contract.wit");
    const NET: &str = include_str!("../../../../contracts/jinn-net/contract.wit");
    for (bundle, error) in [(PROCESS, "process-error"), (NET, "net-error")] {
        let declared = bundle
            .lines()
            .find(|line| line.trim_start().starts_with(&format!("variant {error}")))
            .unwrap_or_else(|| panic!("{error} declared in the bundle"))
            .trim();
        assert!(declared.contains("not-found"), "{declared}");
        assert!(WORLD.contains(declared), "the world carries {declared} verbatim");
        assert!(WORLD.contains(&format!("result<list<u8>, {error}>")));
    }
    assert!(WORLD.contains("output-truncated }"));
}
