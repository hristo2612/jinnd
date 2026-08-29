//! Round-3 Major, red-first: a CONFIGURED master-key source that cannot be
//! read FAILS CLOSED. `JINND_KEYSTORE_PASSPHRASE_FILE` naming a missing,
//! unreadable, or empty file is a typed error naming the source — never a
//! silent degrade to `Absent` or to the platform keychain. Only an
//! UNCONFIGURED environment falls through to the platform default.

use std::ffi::OsString;

use super::{MasterKeySource, PASSPHRASE, contains, home};
use crate::hostkeystore::master::{PASSPHRASE_ENV, PASSPHRASE_FILE_ENV};

fn source(passphrase: Option<&str>, file: Option<&std::path::Path>) -> MasterKeySource {
    from(passphrase, file).unwrap_or_else(|error| panic!("source: {error:?}"))
}

fn from(
    passphrase: Option<&str>,
    file: Option<&std::path::Path>,
) -> Result<MasterKeySource, jinnd_api::KernelError> {
    MasterKeySource::from_vars(
        passphrase.map(OsString::from),
        file.map(|path| OsString::from(path.as_os_str())),
    )
}

fn refusal(passphrase: Option<&str>, file: Option<&std::path::Path>, naming: &str) -> String {
    let error = from(passphrase, file).err().unwrap_or_else(|| {
        panic!("a configured source that cannot be read must fail closed, not degrade")
    });
    assert_eq!(error.code, jinnd_api::ErrorCode::InvalidProfile);
    assert!(error.message.contains(naming), "{}", error.message);
    error.message
}

#[test]
fn an_unset_environment_falls_through_to_the_platform_default() {
    let held = source(None, None);
    #[cfg(target_os = "macos")]
    assert!(matches!(held, MasterKeySource::Keychain), "{held:?}");
    #[cfg(not(target_os = "macos"))]
    assert!(matches!(held, MasterKeySource::Absent), "{held:?}");
}

#[test]
fn a_readable_source_derives_and_trims_its_trailing_newline() {
    let home = home("source-read");
    let path = home.0.join("passphrase");
    let text = String::from_utf8_lossy(PASSPHRASE).into_owned();
    std::fs::write(&path, format!("{text}\n")).unwrap_or_else(|error| panic!("{error}"));
    for held in [
        source(Some(&format!("{text}\n")), None),
        source(None, Some(&path)),
        // The variable wins over the file when both are configured.
        source(Some(&text), Some(&home.0.join("absent"))),
    ] {
        match held {
            MasterKeySource::Passphrase(bytes) => assert_eq!(bytes, PASSPHRASE),
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn a_configured_file_that_cannot_be_read_fails_closed_naming_the_source() {
    let home = home("source-closed");
    let missing = home.0.join("nowhere/passphrase");
    let message = refusal(None, Some(&missing), PASSPHRASE_FILE_ENV);
    assert!(message.contains("passphrase"), "{message}");

    let empty = home.0.join("empty");
    std::fs::write(&empty, b"\n").unwrap_or_else(|error| panic!("{error}"));
    refusal(None, Some(&empty), PASSPHRASE_FILE_ENV);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let denied = home.0.join("denied");
        std::fs::write(&denied, PASSPHRASE).unwrap_or_else(|error| panic!("{error}"));
        std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o000))
            .unwrap_or_else(|error| panic!("{error}"));
        // A privileged test runner reads it anyway; then there is no
        // permission failure to assert.
        if std::fs::read(&denied).is_err() {
            refusal(None, Some(&denied), PASSPHRASE_FILE_ENV);
        }
        let _ = std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o600));
    }
}

#[test]
fn a_configured_but_empty_variable_fails_closed_and_never_echoes_the_secret() {
    let message = refusal(Some("\n"), None, PASSPHRASE_ENV);
    assert!(!contains(message.as_bytes(), PASSPHRASE), "{message}");
    refusal(Some(""), None, PASSPHRASE_ENV);
}
