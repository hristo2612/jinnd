//! Host-side bindings of the `jinn:plugin` world, generated from `wit/` at
//! compile time — compiling this crate IS the WIT validation the packet's
//! acceptance names (R12: the contract files are the product; this module
//! merely binds them).

#[allow(clippy::indexing_slicing, clippy::missing_safety_doc)]
mod generated {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "plugin",
        async: true,
    });
}

pub use generated::exports::jinn::plugin::lifecycle;
pub use generated::jinn::plugin::{
    clock, effects, events, fs, keystore, net, process, services, types,
};
pub use generated::{Plugin, PluginPre};

use jinnd_api::{ErrorCode, Owed};

/// Maps a facade error onto the wire, losing nothing the guest may act on.
pub fn wire_error(error: jinnd_api::KernelError) -> types::KernelError {
    match error.code {
        ErrorCode::InactiveContext => types::KernelError::InactiveContext,
        ErrorCode::MissingDependency => types::KernelError::MissingDependency(error.message),
        ErrorCode::PluginFailed | ErrorCode::ListenerFailed => {
            types::KernelError::ProviderFailed(error.message)
        }
        ErrorCode::EffectFailed => types::KernelError::GrantRefused(error.message),
        ErrorCode::NotFound => types::KernelError::ProviderFailed(error.message),
        ErrorCode::DependencyCycle | ErrorCode::InvalidProfile | ErrorCode::DuplicateProvision => {
            types::KernelError::Invalid(error.message)
        }
    }
}

/// The reply-expecting dispatch refusal, typed onto the wire (M2-K9): the
/// case NAMES the caller's next move and the record names who refused it,
/// so nothing about a refusal has to be read out of a sentence (R3).
/// Deliberately not routed through [`wire_error`] — a `kernel-error` whose
/// payload is a record cannot be reconstructed from a message string.
pub fn wire_refusal(topic: &str, refused: &crate::topics::Unserved) -> types::KernelError {
    let target = types::RefusedTarget {
        entry: refused.entry.0.clone(),
        incarnation: refused.incarnation,
        topic: topic.to_owned(),
    };
    match refused.owed {
        Owed::Reload => types::KernelError::Restarting(target),
        Owed::Disposal => types::KernelError::Gone(target),
        Owed::Suspension => types::KernelError::Suspended(target),
        Owed::Stalled => types::KernelError::Stalled(target),
    }
}

/// The wait-cycle refusal, typed onto the wire (M2-K10): both ends and
/// the wait path between them, so a caller acts on identity rather than on
/// prose (R3). Deliberately not routed through [`wire_error`], for the
/// M2-K9 reason — a `kernel-error` whose payload is a record cannot be
/// reconstructed from a message string.
pub fn wire_cycle(cycle: &crate::waits::Cycle) -> types::KernelError {
    types::KernelError::Cycle(types::WaitCycle {
        on: cycle.on.clone(),
        waiter: cycle.waiter_name(),
        target: cycle.target_name(),
        through: cycle.through.iter().map(|edge| edge.on.clone()).collect(),
    })
}

/// Maps a facade error onto the `jinn:fs` bundle's own error (M2-K3, R12):
/// typed absence, a grant or scope denial, or the contained `io` failure —
/// the provider's own or the kernel boundary's.
pub fn fs_error(error: jinnd_api::KernelError) -> fs::FsError {
    match error.code {
        ErrorCode::NotFound => fs::FsError::NotFound,
        ErrorCode::EffectFailed => fs::FsError::Denied,
        _ => fs::FsError::Io(error.message),
    }
}

/// Maps a facade error onto the `jinn:process` bundle's own error (M2-K6
/// round 4, R3/R12): typed absence, a grant denial, a malformed request,
/// or the contained failure; `?` converts at the import boundary.
/// `output-truncated` never arrives here — it is a tagged answer on the
/// broker wire (`hostwire::TAG_TRUNCATED`).
impl From<jinnd_api::KernelError> for process::ProcessError {
    fn from(error: jinnd_api::KernelError) -> Self {
        match error.code {
            ErrorCode::NotFound => Self::NotFound,
            ErrorCode::EffectFailed => Self::Denied(error.message),
            ErrorCode::DependencyCycle
            | ErrorCode::InvalidProfile
            | ErrorCode::DuplicateProvision => Self::Invalid(error.message),
            _ => Self::Failed(error.message),
        }
    }
}

/// Maps a facade error onto the `jinn:net` bundle's own error (the same
/// classes as the process mapping; a malformed address is `invalid`).
impl From<jinnd_api::KernelError> for net::NetError {
    fn from(error: jinnd_api::KernelError) -> Self {
        match error.code {
            ErrorCode::NotFound => Self::NotFound,
            ErrorCode::EffectFailed => Self::Denied(error.message),
            ErrorCode::DependencyCycle
            | ErrorCode::InvalidProfile
            | ErrorCode::DuplicateProvision => Self::Invalid(error.message),
            _ => Self::Failed(error.message),
        }
    }
}

/// Maps a facade error onto the `jinn:keystore` bundle's own error
/// (M2-K8, R3/R12): typed absence, a grant or scope denial, a malformed
/// key, or the contained failure. Messages name keys, never values.
impl From<jinnd_api::KernelError> for keystore::KeystoreError {
    fn from(error: jinnd_api::KernelError) -> Self {
        match error.code {
            ErrorCode::NotFound => Self::NotFound,
            ErrorCode::EffectFailed => Self::Denied(error.message),
            ErrorCode::DependencyCycle
            | ErrorCode::InvalidProfile
            | ErrorCode::DuplicateProvision => Self::Invalid(error.message),
            _ => Self::Failed(error.message),
        }
    }
}

/// The wire selector, evaluated kernel-side only (C4).
pub fn api_selector(selector: types::Selector) -> crate::selector::Selector {
    match selector {
        types::Selector::All => crate::selector::Selector::All,
        types::Selector::ContextSet(contexts) => crate::selector::Selector::ContextSet(contexts),
        types::Selector::RealmOf(service) => crate::selector::Selector::RealmOf(service),
    }
}

/// The wire dispatch mode.
pub fn api_mode(mode: types::DispatchMode) -> jinnd_api::DispatchMode {
    match mode {
        types::DispatchMode::Emit => jinnd_api::DispatchMode::Emit,
        types::DispatchMode::Parallel => jinnd_api::DispatchMode::Parallel,
        types::DispatchMode::Serial => jinnd_api::DispatchMode::Serial,
        types::DispatchMode::Bail => jinnd_api::DispatchMode::Bail,
        types::DispatchMode::Waterfall => jinnd_api::DispatchMode::Waterfall,
    }
}

#[cfg(test)]
mod tests;
