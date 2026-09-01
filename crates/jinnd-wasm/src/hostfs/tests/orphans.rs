//! M2-K19: a crash between `create` and `rename` leaves an orphan
//! `.jinnd-stage` file, and until this packet NOTHING swept it — the
//! orphan landed in the guest's own directory and stayed there forever.
//!
//! I4 says the quiescent state is indistinguishable from a fresh boot of
//! the final configuration: permanent litter a clean shutdown does not
//! leave IS a trace. These pins force the crash for real — a child
//! process killed inside the real window, running the production commit
//! path, no fault injection — then prove the next boot sweeps what it
//! left.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::super::retention::{Header, Prior, Record, commit_atomic};
use super::super::effect_label;
use super::{FS_CONTRACT, HostFs, Home, Recording, home};
use crate::peer::LedgerSink;

/// Names the target the child process must commit to; its presence is what
/// puts the re-executed test binary in the child role.
const CHILD_ENV: &str = "JINND_K19_STAGE_CRASH_TARGET";

/// Big enough that the stage window (write + fsync) dominates the create
/// that opens it, so a 1 ms poll lands the kill inside the window.
const PAYLOAD: usize = 32 << 20;

fn sink() -> Arc<dyn LedgerSink> {
    Arc::new(Recording(Mutex::new(Vec::new()))) as Arc<dyn LedgerSink>
}

/// Reboots the provider over `home` — the sweep runs at open, before the
/// provider is a broker peer, so no guest of this incarnation ever sees an
/// orphan.
fn reboot(home: &Home) -> HostFs {
    HostFs::open(home.0.join("data"), home.0.join("inverses"), sink())
        .unwrap_or_else(|error| panic!("open: {error:?}"))
}

/// Every file under `dir`, relative, sorted — the disk outcome.
fn tree(dir: &Path) -> Vec<String> {
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(relative) = path.strip_prefix(dir) {
                files.push(relative.to_string_lossy().into_owned());
            }
        }
    }
    files.sort();
    files
}

fn plant(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap_or_else(|error| panic!("{error}"));
    }
    std::fs::write(path, bytes).unwrap_or_else(|error| panic!("{error}"));
}

/// The child role: runs the REAL commit path (`commit_atomic`, the one
/// shape `write`/`append`, the retention spill, and the keystore share)
/// and is killed inside its window by the parent.
fn child_role(target: &str) -> ! {
    let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|error| panic!("{error}"));
    let _ = runtime.block_on(commit_atomic(Path::new(target), &vec![b'K'; PAYLOAD]));
    std::process::exit(0)
}

/// The libtest filter that re-runs exactly this case in the child.
fn case(name: &str) -> String {
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    format!("{module}::{name}")
}

/// Spawns this test binary in the child role and SIGKILLs it inside the
/// stage window. Answers whether the crash left the orphan and nothing
/// else.
fn crash_inside_the_window(target: &Path, staged: &Path) -> bool {
    let Ok(binary) = std::env::current_exe() else {
        return false;
    };
    let mut child = std::process::Command::new(binary)
        .args([
            "--exact",
            "--test-threads=1",
            &case("a_crash_inside_the_stage_window_leaves_an_orphan_the_next_boot_sweeps"),
        ])
        .env(CHILD_ENV, target)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap_or_else(|error| panic!("spawn the crashing child: {error}"));
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if staged.exists() || matches!(child.try_wait(), Ok(Some(_))) || Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let _ = child.kill();
    let _ = child.wait();
    staged.exists() && !target.exists()
}

/// The packet's runtime repro. A real process dies inside the real stage
/// window; the orphan it leaves survives in the guest's own directory —
/// that is the defect — and the next boot of the provider removes it
/// without adopting its (possibly torn) bytes.
#[test]
fn a_crash_inside_the_stage_window_leaves_an_orphan_the_next_boot_sweeps() {
    if let Ok(target) = std::env::var(CHILD_ENV) {
        child_role(&target);
    }
    let home = home("stage-crash");
    let data = home.0.join("data");
    let target = data.join("log/b.txt");
    let staged = data.join("log/b.txt.jinnd-stage");
    std::fs::create_dir_all(data.join("log")).unwrap_or_else(|error| panic!("{error}"));

    let mut forced = false;
    for _ in 0..8 {
        if crash_inside_the_window(&target, &staged) {
            forced = true;
            break;
        }
        let _ = std::fs::remove_file(&staged);
        let _ = std::fs::remove_file(&target);
    }
    assert!(forced, "a crash was forced inside the stage window");
    assert!(staged.exists(), "the crash left the staged file behind");
    assert!(!target.exists(), "the commit never landed: no target");

    let _provider = reboot(&home);
    assert!(
        !staged.exists(),
        "the next boot sweeps the orphan the crash left (I4)"
    );
    assert!(
        !target.exists(),
        "the sweep never adopts staged bytes that may be a torn prefix"
    );
    assert!(
        tree(&data).is_empty(),
        "the quiescent tree carries no trace of the crash: {:?}",
        tree(&data)
    );
}

/// The sweep reaches every depth of the guest's own tree — an orphan is a
/// sibling of its target and guests own subdirectories — and takes
/// nothing else: not a guest file whose name merely contains the suffix,
/// not the target it was staging for.
#[test]
fn the_boot_sweep_reaches_every_depth_and_removes_only_staged_files() {
    let home = home("sweep-depth");
    let data = home.0.join("data");
    for orphan in [
        "top.txt.jinnd-stage",
        "log/b.txt.jinnd-stage",
        "log/deep/nested/c.bin.jinnd-stage",
    ] {
        plant(&data.join(orphan), b"partial");
    }
    for kept in [
        "log/a.txt",
        "log/b.txt",
        "log/notes.jinnd-stage.txt",
        "log/notes.jinnd-staged",
        "log/deep/nested/c.bin",
    ] {
        plant(&data.join(kept), b"kept");
    }

    let _provider = reboot(&home);

    assert_eq!(
        tree(&data),
        vec![
            "log/a.txt".to_owned(),
            "log/b.txt".to_owned(),
            "log/deep/nested/c.bin".to_owned(),
            "log/notes.jinnd-stage.txt".to_owned(),
            "log/notes.jinnd-staged".to_owned(),
        ],
        "exactly the staged files went"
    );
}

/// The same crash inside the retention spill's own commit — the inverse
/// store writes through the identical protocol — is swept when the store
/// opens, and the spilled inverses it indexes are untouched.
#[test]
fn the_retention_spill_sweeps_its_own_orphans_without_losing_an_inverse() {
    let home = home("sweep-spill");
    let inverses = home.0.join("inverses");
    let record = Record {
        header: Header {
            label: "log/a.txt".to_owned(),
            key: String::new(),
            owner: 7,
            entry: "scribe".to_owned(),
            operation: "write".to_owned(),
        },
        prior: Prior::Absent,
    };
    plant(&inverses.join("42.inverse"), &record.encode());
    plant(&inverses.join("43.inverse.jinnd-stage"), b"half a record");
    plant(&inverses.join("epoch.jinnd-stage"), b"9");

    let provider = HostFs::open(home.0.join("data"), inverses.clone(), sink())
        .unwrap_or_else(|error| panic!("open: {error:?}"));

    let mut left = tree(&inverses);
    left.retain(|name| name != "epoch");
    assert_eq!(
        left,
        vec!["42.inverse".to_owned()],
        "the orphans went, the spilled inverse stayed"
    );
    let journals = provider.journals();
    let entries: Vec<(String, Vec<(String, String, u64)>)> = journals
        .into_iter()
        .map(|(entry, records)| {
            let records = records
                .into_iter()
                .map(|record| (record.contract, record.label, record.effect))
                .collect();
            (entry, records)
        })
        .collect();
    assert_eq!(
        entries,
        vec![(
            "scribe".to_owned(),
            vec![(
                FS_CONTRACT.to_owned(),
                effect_label("write", "log/a.txt", 42),
                42
            )]
        )],
        "the rehydrated journal is exactly the surviving inverse"
    );
}

/// A sweep is only sound if the suffix is the kernel's alone. The bundle
/// declares `<name>.jinnd-stage` as the staging name of `<name>`
/// (`contracts/jinn-fs/metadata.toml`, `commit = "stage-fsync-rename"`),
/// so the name is reserved by contract, not by this code's convenience —
/// and the survivors above pin how narrow that reservation is.
#[test]
fn the_bundle_states_what_becomes_of_a_stage_file_whose_rename_never_came() {
    let bundle = include_str!("../../../../../contracts/jinn-fs/metadata.toml");
    assert!(
        bundle.contains("<name>.jinnd-stage"),
        "the bundle declares the reserved staging name"
    );
    assert!(
        bundle.contains("[recovery]"),
        "the bundle states the boot sweep of a staged file whose rename never came"
    );
}
