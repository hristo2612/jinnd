//! Verifier-owned M2-K25 in-flight fault case against the fiber engine's
//! public post-activation input.

use std::sync::{Arc, Mutex};

use jinnd_api::{ErrorCode, FiberState, KernelError, KernelFuture, TransitionCause};
use jinnd_fiber::{FaultSink, Fiber, FiberBody, ReadinessSource, Setup};
use tokio::sync::Notify;

#[derive(Default)]
struct Gate {
    entered: Notify,
    release: Notify,
}

struct Faultable {
    sink: Arc<Mutex<Option<FaultSink>>>,
    gate: Arc<Gate>,
    undos: Arc<Mutex<usize>>,
}

impl FiberBody for Faultable {
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        let sink = Arc::clone(&self.sink);
        let gate = Arc::clone(&self.gate);
        let undos = Arc::clone(&self.undos);
        Box::pin(async move {
            *sink.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(setup.faults());
            setup.effect(
                "gated seat",
                Box::new(move || {
                    Box::pin(async move {
                        gate.entered.notify_one();
                        gate.release.notified().await;
                        *undos.lock().unwrap_or_else(|poison| poison.into_inner()) += 1;
                        Ok(())
                    })
                }),
            )?;
            Ok(())
        })
    }
}

fn failure(message: &str) -> KernelError {
    KernelError {
        code: ErrorCode::PluginFailed,
        message: message.to_owned(),
        fiber: None,
    }
}

async fn until(fiber: &Fiber, state: FiberState) {
    let mut states = fiber.states();
    states
        .wait_for(|current| *current == state)
        .await
        .unwrap_or_else(|error| panic!("state watch: {error}"));
}

#[tokio::test]
async fn a_death_during_an_in_flight_transition_lands_exactly_once() {
    for disposing in [false, true] {
        let sink = Arc::new(Mutex::new(None));
        let gate = Arc::new(Gate::default());
        let undos = Arc::new(Mutex::new(0));
        let readiness = ReadinessSource::independent();
        let fiber = Arc::new(Fiber::spawn(
            Arc::new(Faultable {
                sink: Arc::clone(&sink),
                gate: Arc::clone(&gate),
                undos: Arc::clone(&undos),
            }),
            readiness.signal(),
        ));
        fiber.quiesce().await;
        assert_eq!(fiber.state(), FiberState::Active);

        if disposing {
            let finishing = tokio::spawn({
                let fiber = Arc::clone(&fiber);
                async move { fiber.dispose().await }
            });
            until(&fiber, FiberState::Unloading).await;
            gate.entered.notified().await;
            sink.lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
                .unwrap_or_else(|| panic!("fault sink"))
                .fault(failure("death during disposal"));
            gate.release.notify_one();
            finishing
                .await
                .unwrap_or_else(|error| panic!("dispose task: {error}"));
            assert_eq!(fiber.state(), FiberState::Disposed);
        } else {
            fiber.restart(TransitionCause::ConfigChanged);
            until(&fiber, FiberState::Unloading).await;
            gate.entered.notified().await;
            sink.lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
                .unwrap_or_else(|| panic!("fault sink"))
                .fault(failure("death during restart"));
            gate.release.notify_one();
            fiber.quiesce().await;
            assert_eq!(fiber.state(), FiberState::Active);
        }

        assert_eq!(
            *undos.lock().unwrap_or_else(|poison| poison.into_inner()),
            1
        );
        assert_eq!(fiber.record().failures.len(), 1);
        let states: Vec<_> = fiber
            .record()
            .transitions
            .iter()
            .map(|transition| transition.to)
            .collect();
        assert_eq!(
            states
                .iter()
                .filter(|state| matches!(state, FiberState::Failed | FiberState::Disposed))
                .count(),
            usize::from(disposing)
        );
        assert!(
            !states
                .windows(2)
                .any(|pair| pair == [FiberState::Disposed, FiberState::Failed])
        );
    }
}
