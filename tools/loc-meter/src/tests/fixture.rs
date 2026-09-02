//! A throwaway git repository holding a one-crate workspace, so every case
//! measures a real `merge-base(main, HEAD)..HEAD` through real git and cargo.

use std::path::Path;
use std::process::Command;

use crate::side::TempDir;
use crate::{MeterError, Options, Report};

pub struct Fixture {
    pub dir: TempDir,
}

pub const LIB: &str = "pub fn alpha() -> u32 {\n    1\n}\n";

impl Fixture {
    /// `main` holds one committed crate `crates/alpha`; `packet` is checked out for the changes.
    pub fn new() -> Self {
        let dir = TempDir::new("fixture").unwrap();
        let fx = Fixture { dir };
        fx.git(&["init", "-q", "-b", "main"]);
        fx.write(
            "Cargo.toml",
            "[workspace]\nresolver = \"3\"\nmembers = [\"crates/alpha\"]\n",
        );
        fx.write(
            "crates/alpha/Cargo.toml",
            "[package]\nname = \"alpha\"\nversion = \"0.0.1\"\nedition = \"2024\"\n",
        );
        fx.write("crates/alpha/src/lib.rs", LIB);
        fx.commit("base");
        fx.git(&["checkout", "-q", "-b", "packet"]);
        fx
    }

    pub fn git(&self, args: &[&str]) -> String {
        let out = Command::new("git")
            .arg("-C")
            .arg(self.dir.path())
            .args([
                "-c",
                "user.name=meter",
                "-c",
                "user.email=meter@example.invalid",
            ])
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    pub fn write(&self, rel: &str, content: &str) {
        let path = self.dir.path().join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    pub fn append(&self, rel: &str, content: &str) {
        let path = self.dir.path().join(rel);
        let mut current = std::fs::read_to_string(&path).unwrap();
        current.push_str(content);
        std::fs::write(path, current).unwrap();
    }

    pub fn remove(&self, rel: &str) {
        std::fs::remove_file(self.dir.path().join(rel)).unwrap();
    }

    pub fn commit(&self, message: &str) {
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "-m", message]);
    }

    pub fn measure(&self) -> Result<Report, MeterError> {
        crate::measure(&Options {
            repo: self.dir.path().to_path_buf(),
            base: "main".to_string(),
            head: "HEAD".to_string(),
        })
    }

    /// The meter every card quoted before M2-K18, over `paths`: `git diff
    /// --numstat main...HEAD -- <paths> | awk 'index($3,"tests.rs")==0 {a+=$1; d+=$2} END {print a-d}'`.
    pub fn old_meter(&self, paths: &[&str]) -> i64 {
        let mut args = vec!["diff", "--numstat", "main...HEAD", "--"];
        args.extend_from_slice(paths);
        self.git(&args)
            .lines()
            .filter_map(|l| {
                let mut cols = l.split('\t');
                let a: i64 = cols.next()?.parse().ok()?;
                let d: i64 = cols.next()?.parse().ok()?;
                let path = cols.next()?;
                (!path.contains("tests.rs")).then_some(a - d)
            })
            .sum()
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }
}
