//! Shared test lane: a miniature host over `jinnd-fiber`/`jinnd-registry`,
//! shaped exactly like the adapter wiring the loader serves in production.

#![allow(dead_code)]

pub mod probe;
pub mod support;

pub use support::*;

use std::any::Any;
use std::sync::{Arc, Mutex};

use jinnd_api::{
    ErrorCode, FiberId, KernelError, KernelFuture, ServiceContract, ServiceType, TransitionCause,
};
use jinnd_context::Context;
use jinnd_effects::Disposer;
use jinnd_fiber::{Fiber, FiberBody, Setup};
use jinnd_loader::{EntryHandle, Loader, PackageLane, SpawnRequest};
use jinnd_registry::Registry;

/// The service the provider/consumer fixtures share.
#[derive(Debug)]
pub struct FixtureService(pub u32);

impl ServiceContract for FixtureService {
    type Observation = u32;

    const NAME: &'static str = "svc.fixture";

    fn observe(&self) -> u32 {
        self.0
    }
}

/// What a fixture body needs to run and to be rebound/restated later.
struct Cell {
    entry: String,
    log: Log,
    registry: Registry,
    root: Context<()>,
    at: Mutex<Context<()>>,
    config: Mutex<u32>,
}

impl Cell {
    fn context(&self) -> Context<()> {
        self.at
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn config(&self) -> u32 {
        *self
            .config
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

enum Role {
    Counter,
    Provider,
    Consumer,
}

struct FixtureBody {
    cell: Cell,
    role: Role,
}

impl FiberBody for FixtureBody {
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        let fiber = setup.fiber();
        Box::pin(async move {
            let cell = &self.cell;
            let config = cell.config();
            {
                let mut log = cell.log.lock().unwrap_or_else(|poison| poison.into_inner());
                log.push(Moment::Activated(cell.entry.clone(), config));
            }
            let entry = cell.entry.clone();
            let log = Arc::clone(&cell.log);
            setup.effect(
                "fixture activation",
                Disposer::sync(move || {
                    log.lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push(Moment::Deactivated(entry));
                    Ok(())
                }),
            )?;
            match self.role {
                Role::Counter => {}
                Role::Provider => {
                    let at = cell.context();
                    let tree = at.tree();
                    let name = tree.key_of::<FixtureService>().name();
                    let realm = tree
                        .realm_value(at.realm_of(name))
                        .unwrap_or(jinnd_api::Realm::Root);
                    let vitality = cell.registry.vitality(true);
                    let provision = cell.registry.provide::<FixtureService, ()>(
                        &cell.root,
                        &realm,
                        fiber,
                        Arc::new(FixtureService(config)),
                        &vitality,
                    );
                    setup.effect("provide svc.fixture", provision.undo)?;
                }
                Role::Consumer => {
                    let at = cell.context();
                    let (handle, guard) = cell.registry.lease::<FixtureService, ()>(&at)?;
                    cell.log
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push(Moment::Observed(
                            cell.entry.clone(),
                            handle.service.observe(),
                            handle.generation.0,
                        ));
                    setup.effect(
                        "fixture lease",
                        Disposer::sync(move || {
                            drop(guard);
                            Ok(())
                        }),
                    )?;
                }
            }
            Ok(())
        })
    }
}

struct TestHandle {
    fiber: Arc<Fiber>,
    body: Arc<FixtureBody>,
}

impl EntryHandle for TestHandle {
    fn id(&self) -> FiberId {
        self.fiber.id()
    }

    fn state(&self) -> jinnd_api::FiberState {
        self.fiber.state()
    }

    fn withdrawing(&self) -> bool {
        self.fiber.withdrawing()
    }

    fn restart(&self, cause: TransitionCause) {
        self.fiber.restart(cause);
    }

    fn restate(&self, config: &(dyn Any + Send + Sync)) -> Result<(), KernelError> {
        let Some(config) = config.downcast_ref::<u32>() else {
            return Err(KernelError {
                code: ErrorCode::InvalidProfile,
                message: "fixture config must be u32".to_owned(),
                fiber: Some(self.fiber.id()),
            });
        };
        *self
            .body
            .cell
            .config
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = *config;
        Ok(())
    }

    fn rebind(&self, at: Context<()>) {
        *self
            .body
            .cell
            .at
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = at;
    }

    fn dispose(&self) -> KernelFuture<'static, ()> {
        let fiber = Arc::clone(&self.fiber);
        Box::pin(async move {
            fiber.dispose().await;
            Ok(())
        })
    }

    fn quiesce(&self) -> KernelFuture<'static, ()> {
        let fiber = Arc::clone(&self.fiber);
        Box::pin(async move {
            fiber.quiesce().await;
            Ok(())
        })
    }
}

fn lane(
    role: fn() -> Role,
    injects: Vec<ServiceType>,
    provides: Option<ServiceType>,
    registry: &Registry,
    root: &Context<()>,
    log: &Log,
) -> PackageLane {
    let registry = registry.clone();
    let root = root.clone();
    let log = Arc::clone(log);
    PackageLane {
        injects,
        provides,
        spawn: Box::new(move |request: SpawnRequest<'_>| {
            let Some(config) = request.config.downcast_ref::<u32>() else {
                return Err(KernelError {
                    code: ErrorCode::InvalidProfile,
                    message: "fixture config must be u32".to_owned(),
                    fiber: None,
                });
            };
            let body = Arc::new(FixtureBody {
                cell: Cell {
                    entry: request.entry.0.clone(),
                    log: Arc::clone(&log),
                    registry: registry.clone(),
                    root: root.clone(),
                    at: Mutex::new(request.at.clone()),
                    config: Mutex::new(*config),
                },
                role: role(),
            });
            let fiber = Fiber::spawn(Arc::clone(&body) as Arc<dyn FiberBody>, request.signal);
            Ok(Arc::new(TestHandle {
                fiber: Arc::new(fiber),
                body,
            }) as Arc<dyn EntryHandle>)
        }),
    }
}

/// A loader wired with the three fixture packages: `test/count`,
/// `test/provider`, and `test/consumer`.
pub fn fixture() -> (Loader, Registry, Log) {
    let tree: jinnd_context::ContextTree = jinnd_context::ContextTree::new();
    let root = tree.root();
    let registry = Registry::new();
    let log = log();
    let loader = Loader::new(root.clone(), registry.clone(), |_context| {});
    let service = ServiceType::of::<FixtureService>();
    loader
        .register_lane::<u32>(
            "test/count",
            lane(|| Role::Counter, Vec::new(), None, &registry, &root, &log),
        )
        .grab();
    loader
        .register_lane::<u32>(
            "test/provider",
            lane(
                || Role::Provider,
                Vec::new(),
                Some(service),
                &registry,
                &root,
                &log,
            ),
        )
        .grab();
    loader
        .register_lane::<u32>(
            "test/consumer",
            lane(
                || Role::Consumer,
                vec![service],
                None,
                &registry,
                &root,
                &log,
            ),
        )
        .grab();
    (loader, registry, log)
}
