//! Path containment for `jinn:fs` (M2-K3 round 2; contract bundle §scope:
//! "after symlink and dot-segment resolution"). Containment is decided on
//! the FULLY RESOLVED path, never lexically: the deepest existing ancestor
//! is canonicalized (every symlink followed), the absent tail — which
//! cannot be a link — is appended, and the result must sit under the
//! canonical root and under one of the caller's granted prefixes. An
//! unresolvable path is REFUSED; there is no lexical fallback
//! (security-grade, COO ruling).

use std::path::{Component, Path, PathBuf};

use jinnd_api::{ErrorCode, KernelError, RefusalReason};

use crate::broker_state::refusal;

/// One containment refusal: the typed class the ledger records (R3) and
/// the error the caller receives.
#[derive(Debug)]
pub(crate) struct Refused {
    pub(crate) reason: RefusalReason,
    pub(crate) error: KernelError,
}

impl From<Refused> for KernelError {
    fn from(refused: Refused) -> Self {
        refused.error
    }
}

fn escapes(path: &str) -> Refused {
    Refused {
        reason: RefusalReason::ScopeMismatch,
        error: refusal(
            ErrorCode::EffectFailed,
            format!("fs path escapes its scope: {path:?}"),
        ),
    }
}

/// The lexical shape every scoped path and every scope must have: rooted
/// at the scope (a leading `/` is the root), normal components only — no
/// parent traversal, no absolute escape, no `.`. The empty path is the
/// root itself.
///
/// # Errors
///
/// A path with any non-normal component.
pub(crate) fn lexical(path: &str) -> Result<PathBuf, Refused> {
    let relative = Path::new(path.trim_start_matches('/'));
    if relative
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(escapes(path));
    }
    Ok(relative.to_path_buf())
}

/// Resolves one scoped path under the canonical `root`: post-symlink, or
/// refused. Blocking (canonicalize is a metadata walk) — callers on an
/// async path run it on the blocking pool.
///
/// # Errors
///
/// A lexically escaping path, a resolved path outside `root` (a link out),
/// or an unresolvable existing ancestor (refused, never lexically checked).
pub(crate) fn resolve(root: &Path, path: &str) -> Result<PathBuf, Refused> {
    let candidate = root.join(lexical(path)?);
    let mut existing = candidate.as_path();
    let mut tail = Vec::new();
    loop {
        match std::fs::canonicalize(existing) {
            Ok(mut resolved) => {
                for part in tail.iter().rev() {
                    resolved.push(part);
                }
                return if resolved.starts_with(root) {
                    Ok(resolved)
                } else {
                    Err(escapes(path))
                };
            }
            Err(missing) if missing.kind() == std::io::ErrorKind::NotFound => {
                // An absent component cannot be a link; its parent decides.
                let (name, parent) = match (existing.file_name(), existing.parent()) {
                    (Some(name), Some(parent)) if existing != root => (name.to_owned(), parent),
                    _ => return Err(escapes(path)),
                };
                tail.push(name);
                existing = parent;
            }
            Err(unresolvable) => {
                return Err(Refused {
                    reason: RefusalReason::Unresolvable,
                    error: refusal(
                        ErrorCode::EffectFailed,
                        format!("fs path unresolvable, refused: {path:?}: {unresolvable}"),
                    ),
                });
            }
        }
    }
}

/// Authorizes `path` for a caller holding `scopes` (empty = the root
/// scope): the resolved path must sit under one resolved granted prefix.
///
/// # Errors
///
/// As [`resolve`], or a resolved path beside every granted scope.
pub(crate) fn authorized(root: &Path, scopes: &[String], path: &str) -> Result<PathBuf, Refused> {
    let resolved = resolve(root, path)?;
    if scopes.is_empty() {
        return Ok(resolved);
    }
    for scope in scopes {
        if resolved.starts_with(resolve(root, scope)?) {
            return Ok(resolved);
        }
    }
    Err(Refused {
        reason: RefusalReason::ScopeMismatch,
        error: refusal(
            ErrorCode::EffectFailed,
            format!("fs path outside the caller's granted scope: {path:?}"),
        ),
    })
}
