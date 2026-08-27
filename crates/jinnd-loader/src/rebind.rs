//! The Rebind step: rebuild an entry's context and let epoch identity decide
//! what reloads (split from `apply.rs` by the 300-line file cap, R10). No
//! loader lock is held across the handle calls (R1, M1-P6c).

use std::sync::Arc;

use jinnd_api::{EntryId, ErrorCode, KernelError, TransitionCause};
use jinnd_registry::Injection;

use crate::apply::Bucket;
use crate::loader::{LaneConfig, Loader};
use crate::state::{error, lock};
use crate::tree::EntryIndex;

impl Loader {
    /// Rebuilds an entry's context and lets epoch identity decide the rest;
    /// a provider whose provided realm moved reloads to provide in it.
    pub(crate) fn rebind_step<C: LaneConfig>(
        &self,
        entry: &EntryId,
        index: &EntryIndex<'_, C>,
    ) -> Result<Bucket, KernelError> {
        let spec = index
            .get(entry)
            .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry left the document"))?;
        let (old_context, lane) = {
            let state = lock(&self.state);
            let runtime = state
                .entries
                .get(entry)
                .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry has no runtime"))?;
            (
                runtime.context.clone(),
                runtime.live.as_ref().map(|live| Arc::clone(&live.lane)),
            )
        };
        let context = self.derive_context(spec)?;
        // The new registry watch is created before any state changes; attaching
        // it publishes the new epoch immediately (R1: reactive, never polled).
        let readiness = lane.as_ref().map(|lane| {
            self.registry.readiness(
                &context,
                Injection {
                    services: lane.injects.clone(),
                },
            )
        });
        let realm_moved = lane.as_ref().is_some_and(|lane| {
            lane.provides.as_ref().is_some_and(|service| {
                let name = self.root.tree().key_for(service).name();
                old_context.as_ref().map(|old| old.realm_of(name)) != Some(context.realm_of(name))
            })
        });

        // State moves under the lock; the handle calls run with it released —
        // they reach lane-owned code, and no loader lock is ever held across
        // a handle call (R1, M1-P6c; the proxy attach is kernel-owned).
        let handle = {
            let mut state = lock(&self.state);
            let runtime = state
                .entries
                .get_mut(entry)
                .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry has no runtime"))?;
            runtime.spec = Arc::new(spec.clone());
            runtime.context = Some(context.clone());
            match (runtime.live.as_mut(), readiness) {
                (Some(live), Some(readiness)) => {
                    live.proxy.attach(readiness);
                    Some(Arc::clone(&live.handle))
                }
                _ => None,
            }
        };
        if let Some(handle) = handle {
            handle.rebind(context);
            if realm_moved {
                handle.restart(TransitionCause::DependencyChanged);
                return Ok(Bucket::Restarted);
            }
        }
        Ok(Bucket::Unchanged)
    }
}
