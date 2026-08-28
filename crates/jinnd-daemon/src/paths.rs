//! The daemon's whole configuration (M1-P9 card: ledger path and profile
//! path are the only required config; the rest defaults beside the profile)
//! and its canonical form (M2-K5 #18). Split from `daemon.rs` by
//! responsibility (R10 file hygiene).

use std::path::{Path, PathBuf};

use jinnd_api::{ErrorCode, KernelError};

use crate::support::error;

/// The daemon's whole configuration.
#[derive(Clone, Debug)]
pub struct DaemonPaths {
    /// The profile document of record (LAW §3).
    pub profile: PathBuf,
    /// The append-only ledger's SQLite file (R6).
    pub ledger: PathBuf,
    /// Where `<package-basename>.wasm` artifacts (and `.sha256` pin
    /// sidecars) live.
    pub artifacts: PathBuf,
    /// The `jinn:fs` provider's containment root.
    pub data: PathBuf,
}

impl DaemonPaths {
    /// The `jinn:fs` inverse spill (M2-K3): beside the root, never inside.
    #[must_use]
    pub fn inverses(&self) -> PathBuf {
        self.data.with_extension("inverses")
    }

    /// The canonical form the watcher needs (M2-K5 #18): every path
    /// resolved against `cwd`, the profile's directory canonicalized — a
    /// bare `profile.json` names the working directory, never an empty
    /// parent — and the rest canonicalized where they already exist.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] when the profile's directory does not
    /// resolve: the watcher could never arm there (honest refusal, before
    /// any evidence).
    pub fn canonical(self, cwd: &Path) -> Result<Self, KernelError> {
        let absolute = |path: PathBuf| {
            if path.is_absolute() {
                path
            } else {
                cwd.join(path)
            }
        };
        let existing = |path: PathBuf| {
            let path = absolute(path);
            path.canonicalize().unwrap_or(path)
        };
        let profile = absolute(self.profile);
        let name = profile
            .file_name()
            .ok_or_else(|| refuse("the profile path names no file"))?
            .to_owned();
        let directory = profile
            .parent()
            .ok_or_else(|| refuse("the profile path has no directory"))?
            .canonicalize()
            .map_err(|failed| refuse(&format!("the profile's directory: {failed}")))?;
        Ok(Self {
            profile: directory.join(name),
            ledger: existing(self.ledger),
            artifacts: existing(self.artifacts),
            data: existing(self.data),
        })
    }
}

fn refuse(message: &str) -> KernelError {
    error(ErrorCode::InvalidProfile, message.to_owned())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::DaemonPaths;

    fn relative() -> DaemonPaths {
        DaemonPaths {
            profile: PathBuf::from("profile.json"),
            ledger: PathBuf::from("ledger.sqlite"),
            artifacts: PathBuf::from("artifacts"),
            data: PathBuf::from("data"),
        }
    }

    /// FINDINGS #18: a bare `--profile profile.json` resolves against the
    /// working directory — the watcher gets a real directory, not `""`.
    #[test]
    fn relative_paths_resolve_against_the_working_directory() {
        let cwd = std::env::temp_dir()
            .canonicalize()
            .unwrap_or_else(|error| panic!("{error}"));
        let paths = relative()
            .canonical(&cwd)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(paths.profile, cwd.join("profile.json"));
        assert_eq!(paths.ledger, cwd.join("ledger.sqlite"));
        assert_eq!(paths.artifacts, cwd.join("artifacts"));
        assert_eq!(paths.data, cwd.join("data"));
        assert_eq!(paths.profile.parent(), Some(cwd.as_path()));
    }

    /// A profile whose directory does not exist refuses honestly — the
    /// watcher could never arm there.
    #[test]
    fn a_missing_profile_directory_refuses() {
        let cwd = std::env::temp_dir().join(format!("jinnd-paths-{}", std::process::id()));
        let refused = relative().canonical(&cwd);
        assert!(refused.is_err(), "{refused:?}");
    }
}
