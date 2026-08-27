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
pub use generated::{Plugin, PluginPre};
pub use generated::jinn::plugin::{effects, events, fs, keystore, net, process, services, types};

use jinnd_api::ErrorCode;

/// Maps a facade error onto the wire, losing nothing the guest may act on.
pub fn wire_error(error: jinnd_api::KernelError) -> types::KernelError {
    match error.code {
        ErrorCode::InactiveContext => types::KernelError::InactiveContext,
        ErrorCode::MissingDependency => types::KernelError::MissingDependency(error.message),
        ErrorCode::PluginFailed | ErrorCode::ListenerFailed => {
            types::KernelError::ProviderFailed(error.message)
        }
        ErrorCode::EffectFailed => types::KernelError::GrantRefused(error.message),
        ErrorCode::DependencyCycle | ErrorCode::InvalidProfile | ErrorCode::DuplicateProvision => {
            types::KernelError::Invalid(error.message)
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
