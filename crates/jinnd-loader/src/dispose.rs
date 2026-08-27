//! Runtime-originated disposal of one entry (split from `amend` by
//! responsibility, R10): the runtime moves first — the fiber is withdrawn —
//! then the document persists the entry as disabled, config retained.

use jinnd_api::{EntryId, ErrorCode, KernelError, ProfileEntry};

use crate::loader::{LaneConfig, Loader};
use crate::state::{error, lock};

impl Loader {
    /// A runtime-originated disposal: the entry's fiber is withdrawn first,
    /// then the document persists the entry as disabled, config retained.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] for an unknown or faulted entry, a
    /// foreign config type, an operation already in flight for this entry or
    /// the document, a call from within a fiber's teardown context, or —
    /// when the entry is live — a target fiber not at REST (M1-P6c round 2)
    /// or any tracked fiber's withdrawal replay in flight (refused retryably
    /// at the conflict point, never parked, from any task; R1, M1-P6b);
    /// whatever the handle answers for a failed disposal (nothing is
    /// persisted or committed then). Disposal is irreversible at runtime, so
    /// a failed write-back is retried once; failing again, the divergence —
    /// runtime disposed, document enabled — is recorded in
    /// [`Loader::entry_faults`] and returned, so the next reconcile of the
    /// document reconverges the two views (LAW §3: never swallowed).
    pub async fn dispose_entry<C: LaneConfig>(&self, entry: &EntryId) -> Result<(), KernelError> {
        crate::refuse::refuse_teardown_context("the disposal")?;
        let _engaged = self.gate.engage_entry(entry)?;
        // Validation and the reality snapshot for a recorded divergence.
        let staged = self.amended::<C>(entry, |persisted| persisted.disabled = true)?;
        let handle = self.live_handle(entry);
        // A live entry's disposal awaits its fiber's withdrawal: it never
        // begins while that fiber is mid-transition (the REST gate, M1-P6c
        // round 2) nor amid another withdrawal already in flight (round-4
        // law).
        if let Some(handle) = &handle {
            crate::refuse::refuse_own_fiber(handle.as_ref(), "the disposal")?;
            crate::refuse::refuse_unrested(handle.as_ref(), "the disposal")?;
            self.refuse_amid_withdrawal("the disposal")?;
        }
        // The runtime moves first: a refused disposal commits nothing. The
        // teardown replays plugin-owned inverses on the fiber's own task with
        // only the engagement marker held — a teardown calling back into the
        // loader is refused honestly, never deadlocked (R1, M1-P6b).
        if let Some(handle) = &handle {
            handle.dispose().await?;
        }
        if let Err(fault) = self.persist_disposal::<C>(entry).await {
            return Err(self.record_divergence(
                entry,
                staged.spec,
                &format!(
                    "the two views diverged: the runtime is disposed, the document stays \
                     enabled (write-back failed: {})",
                    fault.message
                ),
            ));
        }
        Ok(())
    }

    /// The disposal's write-back and commit, under the persist permit. The
    /// amended document is re-derived inside the permit, so amendments
    /// another task landed meanwhile are never overwritten; the permit's
    /// span is mechanical — the disabled flag is a plain-value rewrite, the
    /// config is retained as persisted, no caller-supplied code anywhere
    /// (R1, PLA-270). The disposal cannot be taken back, so a failed
    /// write-back is retried once — and the applied view moves to the
    /// disposed reality whatever the write-back said.
    async fn persist_disposal<C: LaneConfig>(&self, entry: &EntryId) -> Result<(), KernelError> {
        let persistence = self.persistence();
        let _permit = self.gate.persist_permit().await?;
        let amendment = self.amended::<C>(entry, |persisted: &mut ProfileEntry<C>| {
            persisted.disabled = true;
        })?;
        let save = || async {
            match &persistence {
                None => Ok(()),
                Some(persistence) => persistence.save_amendment(entry, None, Some(true)).await,
            }
        };
        let mut persisted = save().await;
        if persisted.is_err() {
            persisted = save().await;
        }
        let mut state = lock(&self.state);
        {
            let runtime = state
                .entries
                .get_mut(entry)
                .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry has no runtime"))?;
            runtime.context = None;
            runtime.live = None;
            runtime.spec = std::sync::Arc::clone(&amendment.spec);
        }
        persisted?;
        state.committed = Some(amendment.committed);
        Ok(())
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use jinnd_api::{
        EntryId, ErrorCode, FiberId, FiberState, KernelError, KernelFuture, PluginRef, Profile,
        ProfileEntry, TransitionCause,
    };

    use crate::document::Document;
    use crate::lanes::{EntryHandle, PackageLane, SpawnRequest};
    use crate::loader::Loader;
    use crate::store::DocumentStore;

    /// The crate-owned fail-exactly-Nth-save double (M1-P6c round 3): the
    /// disposal save path is mechanical — no caller-authored code runs in it
    /// — so the sealed store seam itself is what proves the retry.
    struct FlakyStore {
        saves: Arc<AtomicU64>,
        fail_on: u64,
        last: Arc<Mutex<Option<Document>>>,
    }

    impl DocumentStore for FlakyStore {
        fn save<'a>(&'a self, document: &'a Document) -> KernelFuture<'a, ()> {
            Box::pin(async move {
                if self.saves.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_on {
                    return Err(KernelError {
                        code: ErrorCode::InvalidProfile,
                        message: "the store refuses this save".to_owned(),
                        fiber: None,
                    });
                }
                *self
                    .last
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner()) = Some(document.clone());
                Ok(())
            })
        }
    }

    /// A handle that never transitions: honestly at rest, disposes cleanly.
    struct RestingHandle;

    impl EntryHandle for RestingHandle {
        fn id(&self) -> FiberId {
            FiberId(1)
        }
        fn state(&self) -> FiberState {
            FiberState::Active
        }
        fn withdrawing(&self) -> bool {
            false
        }
        fn resting(&self) -> bool {
            true
        }
        fn restart(&self, _cause: TransitionCause) {}
        fn restate(&self, _config: &(dyn std::any::Any + Send + Sync)) -> Result<(), KernelError> {
            Ok(())
        }
        fn rebind(&self, _at: jinnd_context::Context<()>) {}
        fn dispose(&self) -> KernelFuture<'static, ()> {
            Box::pin(async { Ok(()) })
        }
        fn quiesce(&self) -> KernelFuture<'static, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    fn entry(name: &str) -> ProfileEntry<u32> {
        ProfileEntry {
            id: EntryId(name.to_owned()),
            plugin: PluginRef {
                package: "double/plugin".to_owned(),
                version: "1".to_owned(),
                artifact_hash: String::new(),
            },
            config: 1,
            disabled: false,
            parent: None,
            isolation: Vec::new(),
        }
    }

    fn grab<T, E: std::fmt::Debug>(outcome: Result<T, E>) -> T {
        match outcome {
            Ok(value) => value,
            Err(error) => panic!("{error:?}"),
        }
    }

    /// Disposal is irreversible at runtime, so a failed write-back is retried
    /// exactly once before any divergence is recorded (LAW §3).
    #[tokio::test]
    async fn a_disposal_write_back_is_retried_before_recording_divergence() {
        let tree = jinnd_context::ContextTree::new();
        let loader = Loader::new(tree.root(), jinnd_registry::Registry::new(), |_context| {});
        grab(loader.register_lane::<u32>(
            "double/plugin",
            PackageLane {
                injects: Vec::new(),
                provides: None,
                spawn: Box::new(|_request: SpawnRequest<'_>| {
                    Ok(Arc::new(RestingHandle) as Arc<dyn EntryHandle>)
                }),
            },
        ));
        let saves = Arc::new(AtomicU64::new(0));
        let last = Arc::new(Mutex::new(None));
        // Save 1 is the reconcile's; 2 is the disposal's first write-back,
        // made to fail; 3 is the retry, which lands.
        loader.attach_store_with::<u32>(
            Box::new(FlakyStore {
                saves: Arc::clone(&saves),
                fail_on: 2,
                last: Arc::clone(&last),
            }),
            Document::default(),
        );
        grab(
            loader
                .reconcile(Profile {
                    entries: vec![entry("one")],
                })
                .await,
        );

        grab(
            loader
                .dispose_entry::<u32>(&EntryId("one".to_owned()))
                .await,
        );
        assert_eq!(saves.load(Ordering::SeqCst), 3, "retried exactly once");
        let saved = last
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let Some(saved) = saved else {
            panic!("the retry saved no document");
        };
        let Some(persisted) = saved.entries.iter().find(|entry| entry.id == "one") else {
            panic!("the entry did not persist");
        };
        assert!(persisted.disabled, "the retried save landed the disposal");
        assert!(loader.entry_faults().is_empty(), "no divergence remains");
    }
}
