//! The package-lane builder: [`wasm_lane`] spawns one [`WasmBody`] per entry
//! over the loader's package-lane seam. Split from `lane.rs` by
//! responsibility (R10 file hygiene).

use std::sync::{Arc, Mutex};

use jinnd_fiber::{Fiber, WatchReadiness};
use jinnd_loader::host::config_of;
use jinnd_loader::{PackageLane, SpawnRequest};
use tokio::sync::watch;

use crate::entry::WasmHandle;
use crate::grants::SeatSpec;
use crate::host::LoadedComponent;
use crate::slot::SharedSlot;

use super::injects::{Declaration, Edges, Gate, gated};
use super::{LaneCore, WasmBody, lock};

/// The package lane for one wasm package over `core`: entries spawn a
/// [`WasmBody`] fiber over the package's pinned component cell; a config
/// edit restates the seat through `decode` (the next activation reads the
/// new grants and payload). `track` is the assembly's fiber-tracking seam:
/// it spawns the body — [`Fiber::spawn`] gated on the request's signal —
/// and records the fiber wherever the assembly answers for it. Entries
/// under this builder declare nothing on the string lane; see
/// [`wasm_lane_declaring`].
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
    wasm_lane_declaring(
        core,
        component,
        guest_trail,
        decode,
        |_: &C| Declaration::default(),
        track,
    )
}

/// [`wasm_lane`] with the entry's string-lane dependency declaration
/// read beside its seat (M2-K24): `declare` decodes the config's
/// `injects`; the fiber then gates on the loader's signal AND the lane's
/// gate — activating only once every declared provider is `Active`,
/// reloading when one is replaced, re-arming when one lands later. A
/// config edit restates both the seat and the declaration.
pub fn wasm_lane_declaring<C, D, I>(
    core: Arc<LaneCore>,
    component: Arc<Mutex<LoadedComponent>>,
    guest_trail: bool,
    decode: D,
    declare: I,
    track: impl Fn(Arc<WasmBody>, WatchReadiness) -> Arc<Fiber> + Send + Sync + 'static,
) -> PackageLane
where
    C: Clone + 'static,
    D: Fn(&C) -> SeatSpec + Clone + Send + Sync + 'static,
    I: Fn(&C) -> Declaration + Clone + Send + Sync + 'static,
{
    PackageLane {
        injects: Vec::new(),
        provides: None,
        spawn: Box::new(move |request: SpawnRequest<'_>| {
            let config = config_of::<C>(request.config)?;
            let seat = decode(&config);
            let gate = Arc::new(Gate::new(&declare(&config), &seat.grants));
            lock(&core.gates).insert(request.entry.clone(), Arc::clone(&gate));
            // The gate's watcher lives as long as the entry's handle.
            let (stop, stopped) = watch::channel(());
            let states = Arc::clone(&core);
            let signal = gated(
                Edges {
                    broker: Arc::clone(&core.broker),
                    provisions: core.broker.provisions(),
                    transitions: core.transitions.subscribe(),
                },
                move |fiber| states.state_of(fiber),
                Arc::clone(&gate),
                request.signal,
                stopped,
            );
            let body = Arc::new(WasmBody {
                core: Arc::clone(&core),
                entry: request.entry.clone(),
                component: Arc::clone(&component),
                seat: Mutex::new(seat),
                gate,
                at: Mutex::new(request.at.clone()),
                slot: Arc::new(SharedSlot::default()),
                guest_trail,
            });
            let fiber = track(Arc::clone(&body), signal);
            core.track_states(
                fiber.id(),
                fiber.states(),
                request.entry.clone(),
                guest_trail,
            );
            let decode = decode.clone();
            let declare = declare.clone();
            let restate = move |body: &WasmBody, config: C| {
                let seat = decode(&config);
                body.gate.restate(&declare(&config), &seat.grants);
                body.restate_seat(seat);
                Ok(())
            };
            Ok(Arc::new(WasmHandle::new(
                fiber,
                body,
                Arc::clone(&core),
                request.entry.clone(),
                restate,
                stop,
            )))
        }),
    }
}
