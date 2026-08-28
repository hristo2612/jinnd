//! The package-lane builder: [`wasm_lane`] spawns one [`WasmBody`] per entry
//! over the loader's package-lane seam. Split from `lane.rs` by
//! responsibility (R10 file hygiene).

use std::sync::{Arc, Mutex};

use jinnd_fiber::{Fiber, WatchReadiness};
use jinnd_loader::host::config_of;
use jinnd_loader::{PackageLane, SpawnRequest};

use crate::entry::WasmHandle;
use crate::grants::SeatSpec;
use crate::host::LoadedComponent;
use crate::slot::SharedSlot;

use super::{LaneCore, WasmBody};

/// The package lane for one wasm package over `core`: entries spawn a
/// [`WasmBody`] fiber over the package's pinned component cell; a config
/// edit restates the seat through `decode` (the next activation reads the
/// new grants and payload). `track` is the assembly's fiber-tracking seam:
/// it spawns the body — [`Fiber::spawn`] gated on the request's signal —
/// and records the fiber wherever the assembly answers for it.
pub fn wasm_lane<C, D>(
    core: Arc<LaneCore>,
    component: Arc<Mutex<LoadedComponent>>,
    guest_trail: bool,
    decode: D,
    track: impl Fn(Arc<WasmBody>, WatchReadiness) -> Arc<Fiber> + Send + Sync + 'static,
) -> PackageLane
where
    C: Clone + 'static,
    D: Fn(&C) -> SeatSpec + Clone + Send + Sync + 'static,
{
    PackageLane {
        injects: Vec::new(),
        provides: None,
        spawn: Box::new(move |request: SpawnRequest<'_>| {
            let config = config_of::<C>(request.config)?;
            let body = Arc::new(WasmBody {
                core: Arc::clone(&core),
                entry: request.entry.clone(),
                component: Arc::clone(&component),
                seat: Mutex::new(decode(&config)),
                at: Mutex::new(request.at.clone()),
                slot: Arc::new(SharedSlot::default()),
                guest_trail,
            });
            let fiber = track(Arc::clone(&body), request.signal);
            let decode = decode.clone();
            let restate = move |body: &WasmBody, config: C| {
                body.restate_seat(decode(&config));
                Ok(())
            };
            Ok(Arc::new(WasmHandle::new(
                fiber,
                body,
                Arc::clone(&core),
                request.entry.clone(),
                restate,
            )))
        }),
    }
}
