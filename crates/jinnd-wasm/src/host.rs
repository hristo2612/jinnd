//! The Tier A host (R7): wasmtime engine configuration, pinned-artifact
//! compilation, and per-fiber instantiation. Fuel metering is on for every
//! guest (constitution 05 §Hosting); async support is on so guest execution
//! yields cooperatively and never captures an executor thread (R1).

use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind};
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine};

use crate::artifact::{self, PinnedArtifact};
use crate::bindings::Plugin;
use crate::handle::InstanceHandle;
use crate::instance::{HostState, Seat, spawn};
use crate::peer::LedgerSink;

/// One compiled component, still carrying the pin it was admitted under.
#[derive(Clone)]
pub struct LoadedComponent {
    component: Component,
    hash: String,
}

impl LoadedComponent {
    /// The lower-hex SHA-256 the artifact was admitted under (Law 5).
    pub fn hash(&self) -> &str {
        &self.hash
    }
}

/// The engine and the one linker every instance shares. The linker binds the
/// `jinn:plugin` world's kernel surfaces; nothing else is linkable, so a
/// component importing anything beyond the world fails to instantiate —
/// mechanical closure at the imports (constitution 01).
pub struct WasmHost {
    engine: Engine,
    linker: Arc<Linker<HostState>>,
}

impl WasmHost {
    /// # Errors
    ///
    /// [`ErrorCode::PluginFailed`] when the engine refuses the configuration
    /// (a build/platform defect, not a caller state).
    pub fn new() -> Result<Self, KernelError> {
        let mut config = Config::new();
        config
            .async_support(true)
            .consume_fuel(true)
            .wasm_component_model(true);
        let engine = Engine::new(&config).map_err(|error| KernelError {
            code: ErrorCode::PluginFailed,
            message: format!("engine configuration refused: {error:#}"),
            fiber: None,
        })?;
        let mut linker: Linker<HostState> = Linker::new(&engine);
        Plugin::add_to_linker::<_, wasmtime::component::HasSelf<HostState>>(&mut linker, |state| {
            state
        })
        .map_err(|error| KernelError {
            code: ErrorCode::PluginFailed,
            message: format!("world linking refused: {error:#}"),
            fiber: None,
        })?;
        Ok(Self {
            engine,
            linker: Arc::new(linker),
        })
    }

    /// Admits `bytes` under `expected_hash` (Law 5 pin-by-hash — a mismatch
    /// refuses to load, recorded) and compiles the component. A malformed
    /// component is refused and recorded the same way.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] on a hash mismatch or an artifact that
    /// is not a valid component of this world.
    pub fn load(
        &self,
        bytes: Vec<u8>,
        expected_hash: &str,
        ledger: &dyn LedgerSink,
    ) -> Result<LoadedComponent, KernelError> {
        let pinned: PinnedArtifact = artifact::admit(bytes, expected_hash, ledger)?;
        match Component::new(&self.engine, pinned.bytes()) {
            Ok(component) => Ok(LoadedComponent {
                component,
                hash: pinned.hash().to_owned(),
            }),
            Err(error) => {
                ledger.append(
                    LedgerEventKind::ArtifactRefused {
                        detail: format!("not a loadable component: {error:#}"),
                    },
                    None,
                );
                Err(KernelError {
                    code: ErrorCode::InvalidProfile,
                    message: "artifact is not a loadable component".to_owned(),
                    fiber: None,
                })
            }
        }
    }

    /// Instantiates one supervised instance of `component` — one per fiber,
    /// disposed instantly and completely with it (R7, I1).
    pub fn instantiate(&self, component: &LoadedComponent, seat: Seat) -> InstanceHandle {
        spawn(
            self.engine.clone(),
            component.component.clone(),
            Arc::clone(&self.linker),
            seat,
        )
    }
}
