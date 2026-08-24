//! The typed resolution walk and its isolation-boundary stop.
//!
//! Ported from `packages/core/src/reflect.ts:80-94` (the `get` trap's ascent loop).

use std::cell::RefCell;

use jinnd_api::{ErrorCode, IsolationBinding, Realm};
use jinnd_context::{Context, ContextTree, Probe, RealmId};

fn binding(service: &str, realm: Realm) -> IsolationBinding {
    IsolationBinding {
        service: service.to_owned(),
        realm,
    }
}

/// A stand-in for the future registry: a fixed set of frames that provide or declare
/// the key. This crate stores no services, so the walk asks a probe.
fn probe_over<'a>(
    provided: &'a [(jinnd_api::ContextId, u8)],
    declared: &'a [jinnd_api::ContextId],
    visited: &'a RefCell<Vec<jinnd_api::ContextId>>,
) -> impl FnMut(&Context<()>) -> Probe<u8> + 'a {
    move |frame: &Context<()>| {
        visited.borrow_mut().push(frame.id());
        if let Some((_, value)) = provided.iter().find(|(id, _)| *id == frame.id()) {
            return Probe::Provided(*value);
        }
        if declared.contains(&frame.id()) {
            return Probe::Declared;
        }
        Probe::Absent
    }
}

#[test]
fn a_provider_in_the_calling_context_is_charged_to_the_caller() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let ctx = tree.root().derive().build();
    let visited = RefCell::new(Vec::new());

    let Ok(resolved) = ctx.resolve(key, probe_over(&[(ctx.id(), 41)], &[], &visited)) else {
        panic!("a provider in the calling context must resolve")
    };

    assert_eq!(resolved.value, 41);
    assert_eq!(resolved.caller, ctx.id());
    assert_eq!(resolved.provider, ctx.id());
    assert_eq!(resolved.realm, ctx.realm_of(key.name()));
    assert_eq!(visited.into_inner(), vec![ctx.id()]);
}

#[test]
fn the_walk_ascends_to_an_ancestor_provider_in_the_same_realm() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let root = tree.root();
    let consumer = root.derive().build().derive().build();
    let visited = RefCell::new(Vec::new());

    let Ok(resolved) = consumer.resolve(key, probe_over(&[(root.id(), 7)], &[], &visited)) else {
        panic!("a root provider must be visible from an un-isolated descendant")
    };

    assert_eq!(resolved.value, 7);
    assert_eq!(resolved.caller, consumer.id());
    assert_eq!(resolved.provider, root.id());
    assert_eq!(visited.into_inner().len(), 3);
}

/// TS origin: `reflect.ts:92` — `fiber.parent[isolate][prop] !== key` stops the ascent.
/// Behavioral half of `adding_relevant_injector_isolation_unloads_consumer`.
#[test]
fn an_isolation_boundary_hides_an_ancestor_provider() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let root = tree.root();
    let isolated = root
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let consumer = isolated.derive().build();
    let visited = RefCell::new(Vec::new());

    let Err(error) = consumer.resolve(key, probe_over(&[(root.id(), 7)], &[], &visited)) else {
        panic!("a root-realm provider must be out of an isolated subtree's reach")
    };

    assert_eq!(error.code, ErrorCode::MissingDependency);
    assert!(
        error.message.contains("bar"),
        "message names the key: {}",
        error.message
    );
    assert_eq!(error.fiber, None);
    assert_eq!(visited.into_inner(), vec![consumer.id(), isolated.id()]);
}

#[test]
fn a_provider_inside_the_isolated_subtree_is_reachable() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let isolated = tree
        .root()
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let consumer = isolated.derive().build();
    let visited = RefCell::new(Vec::new());

    let Ok(resolved) = consumer.resolve(key, probe_over(&[(isolated.id(), 9)], &[], &visited))
    else {
        panic!("the boundary frame itself must be probed")
    };

    assert_eq!(resolved.provider, isolated.id());
    assert_eq!(resolved.realm, tree.realm(&Realm::Shared("beta".into())));
}

/// An isolation binding for one key never narrows the walk for another (loader case
/// `adding_irrelevant_injector_isolation_does_not_restart`).
#[test]
fn an_unrelated_isolation_binding_does_not_narrow_the_walk() {
    let tree = ContextTree::<()>::new();
    let bar = tree.dynamic_key("bar");
    let root = tree.root();
    let consumer = root
        .derive()
        .bind_all(&[binding("qux", Realm::Shared("beta".into()))])
        .build();
    let visited = RefCell::new(Vec::new());

    let Ok(resolved) = consumer.resolve(bar, probe_over(&[(root.id(), 3)], &[], &visited)) else {
        panic!("isolating qux must leave bar resolving through the root realm")
    };

    assert_eq!(resolved.provider, root.id());
}

/// TS origin: `reflect.ts:84-87` — a frame that declares the key but holds no value
/// ends the walk with `InactiveContext`; it never falls through to an ancestor.
#[test]
fn a_declared_but_unprovided_key_ends_the_walk_as_inactive() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let root = tree.root();
    let consumer = root.derive().build();
    let visited = RefCell::new(Vec::new());

    let Err(error) = consumer.resolve(
        key,
        probe_over(&[(root.id(), 7)], &[consumer.id()], &visited),
    ) else {
        panic!("an inactive declaration must not fall through to an ancestor")
    };

    assert_eq!(error.code, ErrorCode::InactiveContext);
    assert!(
        error.message.contains("bar"),
        "message names the key: {}",
        error.message
    );
    assert_eq!(visited.into_inner(), vec![consumer.id()]);
}

/// TS origin: `reflect.ts:82-86` — the store is consulted before the injection list,
/// so a frame that both provides and declares the key resolves.
#[test]
fn a_frame_that_provides_and_declares_the_key_resolves_it() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let consumer = tree.root().derive().build();
    let visited = RefCell::new(Vec::new());

    let Ok(resolved) = consumer.resolve(
        key,
        probe_over(&[(consumer.id(), 5)], &[consumer.id()], &visited),
    ) else {
        panic!("provision must win over declaration at one frame")
    };

    assert_eq!(resolved.value, 5);
}

#[test]
fn a_key_no_frame_holds_ends_the_walk_at_the_root() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let consumer = tree.root().derive().build();
    let visited = RefCell::new(Vec::new());

    let Err(error) = consumer.resolve(key, probe_over(&[], &[], &visited)) else {
        panic!("nothing provides the key, so the walk must fail")
    };

    assert_eq!(error.code, ErrorCode::MissingDependency);
    assert_eq!(visited.into_inner(), vec![consumer.id(), tree.root().id()]);
}

#[test]
fn the_frame_walk_is_the_in_boundary_chain_nearest_first() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let root = tree.root();
    let outer = root.derive().build();
    let isolated = outer
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let inner = isolated.derive().build();

    let frames: Vec<_> = inner.resolution_frames(key).map(|ctx| ctx.id()).collect();
    assert_eq!(frames, vec![inner.id(), isolated.id()]);

    let unbounded: Vec<_> = outer.resolution_frames(key).map(|ctx| ctx.id()).collect();
    assert_eq!(unbounded, vec![outer.id(), root.id()]);

    assert_eq!(
        root.resolution_frames(key)
            .map(|ctx| ctx.id())
            .collect::<Vec<_>>(),
        vec![root.id()],
    );
}

/// Two subtrees mapped to one shared realm walk to the same boundary depth; two local
/// realms do not see each other at all (`shared label` / `isolated context`).
#[test]
fn shared_realm_siblings_stop_at_their_own_boundary() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let root = tree.root();
    let left = root
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let right = root
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let visited = RefCell::new(Vec::new());

    let Err(error) = right.resolve(key, probe_over(&[(left.id(), 1)], &[], &visited)) else {
        panic!("a sibling subtree is not on the ascent path, shared realm or not")
    };

    assert_eq!(error.code, ErrorCode::MissingDependency);
    assert_eq!(left.realm_of(key.name()), right.realm_of(key.name()));
}

/// The frame that introduces the boundary is the caller itself: the walk is that one
/// frame, and an ancestor provider stays out of reach.
#[test]
fn a_caller_that_binds_the_key_itself_is_the_whole_walk() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let root = tree.root();
    let caller = root
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let visited = RefCell::new(Vec::new());

    let Err(error) = caller.resolve(key, probe_over(&[(root.id(), 7)], &[], &visited)) else {
        panic!("the caller's own binding must cut it off from the root realm")
    };

    assert_eq!(error.code, ErrorCode::MissingDependency);
    assert_eq!(visited.into_inner(), vec![caller.id()]);
}

/// A descendant may bind a key back to [`Realm::Root`] — a mapping the TS original
/// cannot express, since there removing isolation means dropping the own key and
/// inheriting the ancestor's realm.
///
/// The boundary rule is applied unchanged (`reflect.ts:92`): the descendant resolves in
/// the root realm, but the ancestor between it and the root resolves the key
/// differently, so the walk still stops there. Re-binding to the root realm is
/// therefore not a way to reach across an isolating ancestor.
#[test]
fn rebinding_to_the_root_realm_still_does_not_cross_an_isolating_ancestor() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let root = tree.root();
    let isolating = root
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let rebound = isolating
        .derive()
        .bind_all(&[binding("bar", Realm::Root)])
        .build();
    let visited = RefCell::new(Vec::new());

    assert!(rebound.realm_of(key.name()).is_root());

    let Err(error) = rebound.resolve(key, probe_over(&[(root.id(), 7)], &[], &visited)) else {
        panic!("an isolating ancestor is not crossed, whatever realm the descendant names")
    };

    assert_eq!(error.code, ErrorCode::MissingDependency);
    assert_eq!(visited.into_inner(), vec![rebound.id()]);
}

/// Dropping the binding instead — deriving with no isolation — inherits the ancestor's
/// realm, which is how the TS original reconnects a consumer.
#[test]
fn a_descendant_without_its_own_binding_stays_inside_the_ancestors_realm() {
    let tree = ContextTree::<()>::new();
    let key = tree.dynamic_key("bar");
    let isolating = tree
        .root()
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let inherited = isolating.derive().build();
    let visited = RefCell::new(Vec::new());

    let Ok(resolved) = inherited.resolve(key, probe_over(&[(isolating.id(), 4)], &[], &visited))
    else {
        panic!("an inheriting descendant reaches the provider inside its realm")
    };

    assert_eq!(resolved.provider, isolating.id());
    assert_eq!(resolved.realm, isolating.realm_of(key.name()));
}

/// A contract whose name a profile can also address through the dynamic lane.
struct Bar(u8);

impl jinnd_api::ServiceContract for Bar {
    type Observation = u8;

    const NAME: &'static str = "bar";

    fn observe(&self) -> u8 {
        self.0
    }
}

/// R3: the walk's output names the slot it resolved, so the registry receives the
/// typed identity rather than inferring it from the caller's turn.
#[test]
fn a_resolution_carries_the_slot_it_resolved() {
    let tree = ContextTree::<()>::new();
    let key = tree.key_of::<Bar>();
    let ctx = tree.root().derive().build();
    let visited = RefCell::new(Vec::new());

    let Ok(resolved) = ctx.resolve(key, probe_over(&[(ctx.id(), 7)], &[], &visited)) else {
        panic!("a provider in the calling context must resolve")
    };

    assert_eq!(resolved.key, key);
    assert_ne!(resolved.key, tree.dynamic_key("bar"));
}

/// A profile binds isolation by string only, so a dynamic binding must stop the walk
/// for the typed contract of that name exactly as it does for the dynamic key.
#[test]
fn a_dynamic_isolation_binding_bounds_the_typed_walk_of_that_name() {
    let tree = ContextTree::<()>::new();
    let key = tree.key_of::<Bar>();
    let root = tree.root();
    let isolated = root
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let consumer = isolated.derive().build();
    let visited = RefCell::new(Vec::new());

    let outcome = consumer.resolve(key, probe_over(&[(root.id(), 5)], &[], &visited));

    assert!(outcome.is_err(), "the root provider is across the boundary");
    assert_eq!(visited.into_inner(), vec![consumer.id(), isolated.id()]);
}

/// The walk fixes its realm once, when it is built: every frame is compared against
/// that one realm, so a resolution ascends the layer chain for the realm exactly once.
#[test]
fn a_walk_reports_the_one_realm_it_was_built_for() {
    let tree = ContextTree::<()>::new();
    let key = tree.key_of::<Bar>();
    let realm = tree.realm(&Realm::Shared("beta".into()));
    let isolated = tree
        .root()
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let consumer = isolated.derive().build();

    assert_eq!(consumer.resolution_frames(key).realm(), realm);
    assert_eq!(
        consumer.resolution_frames(key).realm(),
        consumer.realm_of(key.name())
    );
    assert_eq!(tree.root().resolution_frames(key).realm(), RealmId::ROOT);
}
