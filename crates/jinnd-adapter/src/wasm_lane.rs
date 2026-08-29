//! The wasm-backed package lane and the [`WasmLane`] facade impl (authorized
//! M1-P8 adapter delta; de-duplicated against the lifted production lane at
//! M2-K1): a profile entry naming a wasm package instantiates the pinned
//! artifact behind the SAME broker the harness lane calls — one choke point,
//! two transports (decision log 2026-08-25; R6, R7). The generic machinery
//! lives in `jinnd_wasm` (`LaneCore`, `WasmBody`, `wasm_lane`,
//! `swap_pinned`); what stays here is harness policy — the facade's
//! String-config seat decode, the harness peer, and the shared fiber map.

use std::sync::{Arc, Mutex};

use jinnd_api::{
    EffectId, FiberId, KernelError, KernelFuture, LedgerEventKind, SwapReport, WasmArtifact,
    WasmLane,
};
use jinnd_wasm::{Grant, LaneCore, LedgerSink, PeerId, SeatSpec, WasmBody, swap_pinned, wasm_lane};

use crate::{Adapter, KERNEL_SCOPE, lock};

/// Broker crossings land on the kernel ledger's ordered record lane (R6).
struct Sink(jinnd_ledger::Ledger);

impl LedgerSink for Sink {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.0.record(kind, None, fiber);
    }
}

/// Adapter-held wasm-lane state: the lifted [`LaneCore`] (ONE broker, one
/// topic registry, one host, one loom-modeled swap phase machine — the
/// [`jinnd_wasm::SwapCore`] IS the production path, round-2 blocker-3) plus
/// the harness's own broker peer.
pub(crate) struct WasmState {
    core: Arc<LaneCore>,
    harness: Mutex<Option<PeerId>>,
}

impl WasmState {
    pub(crate) fn new(ledger: jinnd_ledger::Ledger) -> Result<Self, KernelError> {
        Ok(Self {
            core: Arc::new(LaneCore::new(Arc::new(Sink(ledger)))?),
            harness: Mutex::new(None),
        })
    }

    fn harness_peer(&self) -> PeerId {
        *lock(&self.harness)
            .get_or_insert_with(|| self.core.broker.register_peer(Some(KERNEL_SCOPE)))
    }
}

impl WasmLane for Adapter {
    fn register_wasm_package(
        &self,
        package: &str,
        artifact: WasmArtifact,
        grants: Vec<String>,
    ) -> Result<EffectId, KernelError> {
        let state = Arc::clone(&self.wasm);
        let component = state.core.host.load(
            artifact.bytes,
            &artifact.expected_hash,
            state.core.sink.as_ref(),
        )?;
        let shared = Arc::new(Mutex::new(component));
        // The facade grants by name; harness-lane grants carry no scope
        // (scoped grants are the profile document's syntax, daemon-side).
        let grants: Arc<Vec<Grant>> = Arc::new(
            grants
                .into_iter()
                .map(|contract| Grant {
                    contract,
                    scope: None,
                    ops: None,
                })
                .collect(),
        );
        let fibers = Arc::clone(&self.fibers);
        // Registration-time grants restate unchanged on every config edit
        // (the facade fixes them per package); the payload is the entry's
        // String config, verbatim.
        let decode = move |config: &String| SeatSpec {
            grants: (*grants).clone(),
            faults: Vec::new(),
            payload: config.clone().into_bytes(),
        };
        let track = move |body: Arc<WasmBody>, signal| {
            Arc::clone(&crate::wiring::track(&fibers, body, signal).fiber)
        };
        // No Law-2 guest trail: the harness lane keeps its own pinned
        // observables (the daemon's ledger obligation is the daemon's).
        let lane = wasm_lane::<String, _>(
            Arc::clone(&state.core),
            Arc::clone(&shared),
            false,
            decode,
            track,
        );
        let effect = self.register_lane_effect::<String>(package, lane)?;
        lock(&state.core.packages).insert(package.to_owned(), shared);
        Ok(effect)
    }

    fn broker_grant(&self, contract: &str) {
        self.wasm
            .core
            .broker
            .grant(self.wasm.harness_peer(), contract);
    }

    fn broker_resolve(&self, contract: &str) -> Result<u64, KernelError> {
        self.wasm
            .core
            .broker
            .resolve(self.wasm.harness_peer(), contract)
    }

    fn broker_call(
        &self,
        handle: u64,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'_, Vec<u8>> {
        self.wasm
            .core
            .broker
            .call(self.wasm.harness_peer(), handle, operation, payload)
    }

    fn swap_wasm_artifact(
        &self,
        old_hash: &str,
        artifact: WasmArtifact,
    ) -> KernelFuture<'_, SwapReport> {
        let state = Arc::clone(&self.wasm);
        let old_hash = old_hash.to_owned();
        Box::pin(async move {
            let fresh = state.core.host.load(
                artifact.bytes,
                &artifact.expected_hash,
                state.core.sink.as_ref(),
            )?;
            let outcome = swap_pinned(&state.core, &old_hash, fresh).await?;
            Ok(SwapReport {
                swapped: outcome.swapped,
                rolled_back: outcome.rolled_back,
            })
        })
    }
}
