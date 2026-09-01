//! M2-K19 round 2: the boot sweep is ABSOLUTE, not best-effort.
//!
//! Round 1 swallowed every `read_dir`, `file_type` and `remove_file`
//! error and threw the sweep's result away at all three call sites, so
//! `open` could succeed with the orphan still sitting in the guest's
//! directory. A sweep that silently does nothing leaves exactly the I4
//! trace it exists to remove — and leaves it unobserved, which is the
//! failure nobody would ever notice.

use super::{HostFs, home, plant, sink};

/// The sweep is ABSOLUTE, not best-effort (round-2 ruling; the bundle's
/// `[recovery] on-failure = "refuse-open"`). Round 1 swallowed every
/// `read_dir`, `file_type` and `remove_file` error and threw the result
/// away, so `open` could succeed with the orphan still sitting there —
/// a sweep that silently does nothing leaves exactly the I4 trace it
/// exists to remove, and leaves it unobserved. Here the orphan is FOUND
/// and cannot go.
#[cfg(unix)]
#[test]
fn an_orphan_the_sweep_cannot_remove_refuses_the_open() {
    use std::os::unix::fs::PermissionsExt;

    let home = home("sweep-unremovable");
    let data = home.0.join("data");
    let locked = data.join("log");
    plant(&locked.join("b.txt.jinnd-stage"), b"partial");
    // Readable so the walk still finds the orphan; not writable, so
    // removing it fails.
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500))
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(
        std::fs::File::create(locked.join("probe")).is_err(),
        "this case needs a non-root user: a 0o500 directory must refuse a write"
    );

    let refused = HostFs::open(data.clone(), home.0.join("inverses"), sink());

    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(
        refused.is_err(),
        "an orphan that cannot be swept refuses the open instead of passing silently"
    );
    assert!(
        locked.join("b.txt.jinnd-stage").exists(),
        "the refusal is honest about why: the orphan is still there"
    );
}

/// The other half of absolute: a directory the walk cannot READ might
/// hold an orphan, and a sweep that cannot prove its scope clean has not
/// swept it. Silence there is the same unobservable failure.
#[cfg(unix)]
#[test]
fn a_directory_the_sweep_cannot_read_refuses_the_open() {
    use std::os::unix::fs::PermissionsExt;

    let home = home("sweep-unreadable");
    let data = home.0.join("data");
    let opaque = data.join("log");
    plant(&opaque.join("a.txt"), b"kept");
    std::fs::set_permissions(&opaque, std::fs::Permissions::from_mode(0o000))
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(
        std::fs::read_dir(&opaque).is_err(),
        "this case needs a non-root user: a 0o000 directory must refuse a read"
    );

    let refused = HostFs::open(data.clone(), home.0.join("inverses"), sink());

    std::fs::set_permissions(&opaque, std::fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(
        refused.is_err(),
        "a scope the sweep cannot inspect refuses the open"
    );
}
