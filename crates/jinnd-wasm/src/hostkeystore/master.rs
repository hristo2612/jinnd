//! Where the `jinn:keystore` master key comes from (M2-K8 round-2 ruling
//! 1): NEVER a file beside the ciphertext. On macOS the key lives in the
//! platform keychain (Security framework); elsewhere — or wherever an
//! operator says so — it is DERIVED (scrypt) from a passphrase supplied
//! outside the data root at daemon start. The data root alone cannot
//! decrypt: it holds the sealed document, the (public) derivation salt,
//! and sealed inverses, nothing more.

use std::ffi::OsString;
use std::path::Path;

use jinnd_api::{ErrorCode, KernelError};

use crate::broker_state::refusal;

/// The passphrase itself, from the daemon's environment.
pub const PASSPHRASE_ENV: &str = "JINND_KEYSTORE_PASSPHRASE";
/// A file holding the passphrase (trailing newline ignored) — a path an
/// operator hands the daemon at start, outside the data root.
pub const PASSPHRASE_FILE_ENV: &str = "JINND_KEYSTORE_PASSPHRASE_FILE";
const SALT_FILE: &str = "salt";
const SALT_LEN: usize = 16;
/// scrypt N = 2^15, r = 8, p = 1 (~32 MiB, tens of milliseconds): a
/// one-time cost per boot or first mutation, off the async threads.
const SCRYPT_LOG_N: u8 = 15;
#[cfg(target_os = "macos")]
const KEYCHAIN_SERVICE: &str = "jinnd keystore";
#[cfg(target_os = "macos")]
const ERR_SEC_ITEM_NOT_FOUND: i32 = -25300;

/// The operator-facing choice of master-key source. Debug never renders
/// the passphrase.
#[derive(Clone)]
pub enum MasterKeySource {
    /// Derived from this passphrase with the store's salt.
    Passphrase(Vec<u8>),
    /// The platform keychain, one generic-password item per store path.
    #[cfg(target_os = "macos")]
    Keychain,
    /// No source: an existing store cannot open and the first mutation
    /// refuses typed; reads of an absent store still answer not-found.
    Absent,
}

impl std::fmt::Debug for MasterKeySource {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str(match self {
            Self::Passphrase(_) => "Passphrase(<redacted>)",
            #[cfg(target_os = "macos")]
            Self::Keychain => "Keychain",
            Self::Absent => "Absent",
        })
    }
}

impl MasterKeySource {
    /// The daemon's source, from its environment: the passphrase variable,
    /// else the passphrase file, else the platform default (the keychain on
    /// macOS, no source elsewhere).
    ///
    /// # Errors
    ///
    /// A CONFIGURED source that cannot be read fails closed (round-3
    /// Major): a variable set empty, or a file that is missing, unreadable,
    /// or empty, is a typed error NAMING the source — it never degrades to
    /// [`Self::Absent`] or to another backend.
    pub fn from_env() -> Result<Self, KernelError> {
        Self::from_vars(
            std::env::var_os(PASSPHRASE_ENV),
            std::env::var_os(PASSPHRASE_FILE_ENV),
        )
    }

    /// [`Self::from_env`] over explicit variable values (its whole rule,
    /// testable without mutating the process environment).
    ///
    /// # Errors
    ///
    /// As [`Self::from_env`].
    pub(crate) fn from_vars(
        passphrase: Option<OsString>,
        file: Option<OsString>,
    ) -> Result<Self, KernelError> {
        if let Some(value) = passphrase {
            let bytes = trimmed(os_bytes(&value, PASSPHRASE_ENV)?);
            if bytes.is_empty() {
                return Err(unreadable(PASSPHRASE_ENV, None, &"it is set but empty"));
            }
            return Ok(Self::Passphrase(bytes));
        }
        if let Some(path) = file {
            let path = std::path::PathBuf::from(path);
            let read = std::fs::read(&path)
                .map_err(|error| unreadable(PASSPHRASE_FILE_ENV, Some(&path), &error))?;
            let bytes = trimmed(read);
            if bytes.is_empty() {
                return Err(unreadable(
                    PASSPHRASE_FILE_ENV,
                    Some(&path),
                    &"the passphrase file is empty",
                ));
            }
            return Ok(Self::Passphrase(bytes));
        }
        #[cfg(target_os = "macos")]
        {
            Ok(Self::Keychain)
        }
        #[cfg(not(target_os = "macos"))]
        {
            Ok(Self::Absent)
        }
    }

    /// The 32-byte master key for the store under `dir`. Blocking (the
    /// derivation is CPU-bound by design; the keychain is a system call).
    ///
    /// # Errors
    ///
    /// No source, an unreadable salt, or a keychain refusal — each typed,
    /// none carrying key material.
    pub(crate) fn resolve(&self, dir: &Path) -> Result<[u8; 32], KernelError> {
        match self {
            Self::Passphrase(passphrase) => derive(passphrase, dir),
            #[cfg(target_os = "macos")]
            Self::Keychain => keychain(dir),
            Self::Absent => Err(refusal(
                ErrorCode::EffectFailed,
                format!(
                    "keystore master key unavailable: set {PASSPHRASE_ENV} or {PASSPHRASE_FILE_ENV}"
                ),
            )),
        }
    }
}

/// The configured source is named, with why it could not be read and
/// never any of its bytes; the daemon refuses to start rather than fall
/// through to a different key.
fn unreadable(variable: &str, path: Option<&Path>, why: &dyn std::fmt::Display) -> KernelError {
    let named = path.map_or_else(String::new, |path| format!(" ({})", path.display()));
    refusal(
        ErrorCode::InvalidProfile,
        format!("keystore master key source {variable}{named} cannot be read: {why}"),
    )
}

/// A variable's raw bytes; a value the platform cannot render as text is
/// unreadable, not absent.
fn os_bytes(value: &OsString, variable: &str) -> Result<Vec<u8>, KernelError> {
    value
        .clone()
        .into_string()
        .map(String::into_bytes)
        .map_err(|_| unreadable(variable, None, &"it is not valid text"))
}

/// Trailing newlines are the shell's, not the operator's.
fn trimmed(mut bytes: Vec<u8>) -> Vec<u8> {
    while bytes
        .last()
        .is_some_and(|byte| matches!(byte, b'\n' | b'\r'))
    {
        bytes.pop();
    }
    bytes
}

/// OS entropy into `into`, or a typed refusal.
pub(super) fn entropy(into: &mut [u8]) -> Result<(), KernelError> {
    getrandom::fill(into).map_err(|error| {
        refusal(
            ErrorCode::EffectFailed,
            format!("keystore entropy: {error}"),
        )
    })
}

fn failed(detail: &str, error: &dyn std::fmt::Display) -> KernelError {
    refusal(
        ErrorCode::EffectFailed,
        format!("keystore {detail}: {error}"),
    )
}

/// The store's derivation salt (not secret): read, or created from OS
/// entropy once and synced.
fn salt(dir: &Path) -> Result<[u8; SALT_LEN], KernelError> {
    let path = dir.join(SALT_FILE);
    let mut salt = [0u8; SALT_LEN];
    match std::fs::read(&path) {
        Ok(bytes) if bytes.len() == SALT_LEN => {
            salt.copy_from_slice(&bytes);
            return Ok(salt);
        }
        Ok(_) => return Err(failed("salt", &"not 16 bytes")),
        Err(absent) if absent.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(failed("salt", &error)),
    }
    entropy(&mut salt)?;
    std::fs::write(&path, salt)
        .and_then(|()| std::fs::File::open(&path)?.sync_all())
        .map_err(|error| failed("salt", &error))?;
    Ok(salt)
}

fn derive(passphrase: &[u8], dir: &Path) -> Result<[u8; 32], KernelError> {
    let salt = salt(dir)?;
    let params =
        scrypt::Params::new(SCRYPT_LOG_N, 8, 1, 32).map_err(|error| failed("kdf", &error))?;
    let mut key = [0u8; 32];
    scrypt::scrypt(passphrase, &salt, &params, &mut key).map_err(|error| failed("kdf", &error))?;
    Ok(key)
}

/// The keychain item for this store, created from OS entropy on first
/// need. The item's ACL is the platform's: another binary reading it
/// prompts or is refused by the OS, never silently served.
#[cfg(target_os = "macos")]
fn keychain(dir: &Path) -> Result<[u8; 32], KernelError> {
    use security_framework::passwords::{get_generic_password, set_generic_password};
    let account = dir.display().to_string();
    let mut key = [0u8; 32];
    match get_generic_password(KEYCHAIN_SERVICE, &account) {
        Ok(held) if held.len() == key.len() => {
            key.copy_from_slice(&held);
            Ok(key)
        }
        Ok(_) => Err(failed("keychain item", &"not 32 bytes")),
        Err(error) if error.code() == ERR_SEC_ITEM_NOT_FOUND => {
            entropy(&mut key)?;
            set_generic_password(KEYCHAIN_SERVICE, &account, &key)
                .map_err(|error| failed("keychain", &error))?;
            Ok(key)
        }
        Err(error) => Err(failed("keychain", &error)),
    }
}
