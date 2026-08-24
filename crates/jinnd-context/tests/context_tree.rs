//! Tree structure: O(1) derivation, frozen layers, nearest-wins lookup.

use jinnd_api::{EntryId, IsolationBinding, Realm};
use jinnd_context::{ContextTree, RealmId};

fn binding(service: &str, realm: Realm) -> IsolationBinding {
    IsolationBinding {
        service: service.to_owned(),
        realm,
    }
}

fn local(entry: &str) -> Realm {
    Realm::Local(EntryId(entry.to_owned()))
}

#[test]
fn root_is_the_only_context_without_a_parent() {
    let tree = ContextTree::<()>::new();
    let root = tree.root();

    assert!(root.is_root());
    assert_eq!(root.depth(), 0);
    assert_eq!(root.parent(), None);
}

/// TS origin: `packages/core/tests/reflect.spec.ts`, `Context.is()` — structural half
/// of `derived_context_retains_kernel_context_identity`.
#[test]
fn derived_context_gets_a_fresh_identity_and_keeps_its_lineage() {
    let tree = ContextTree::<()>::new();
    let root = tree.root();
    let child = root.derive().build();
    let grandchild = child.derive().build();

    assert_ne!(child.id(), root.id());
    assert_ne!(grandchild.id(), child.id());
    assert_eq!(child.depth(), 1);
    assert_eq!(grandchild.depth(), 2);
    assert_eq!(child.parent().map(|parent| parent.id()), Some(root.id()));
    assert!(root.is_ancestor_of(&grandchild));
    assert!(!grandchild.is_ancestor_of(&root));
    assert!(!root.is_ancestor_of(&root));
    assert_eq!(
        grandchild
            .ancestors()
            .map(|ctx| ctx.id())
            .collect::<Vec<_>>(),
        vec![child.id(), root.id()],
    );
}

#[test]
fn a_cloned_handle_is_the_same_context_and_a_foreign_tree_is_never_an_ancestor() {
    let tree = ContextTree::<()>::new();
    let child = tree.root().derive().build();

    assert_eq!(child.clone(), child);
    assert!(!ContextTree::<()>::new().root().is_ancestor_of(&child));
}

#[test]
fn an_unbound_key_resolves_in_the_root_realm() {
    let tree = ContextTree::<()>::new();
    let key = tree.key("bar");

    assert_eq!(tree.root().realm_of(key), RealmId::ROOT);
    assert_eq!(tree.root().own_realm(key), None);
}

/// TS origin: `packages/core/src/context.ts` `isolate()` — the derived layer holds the
/// binding, descendants inherit it.
#[test]
fn descendants_inherit_an_ancestors_realm_binding() {
    let tree = ContextTree::<()>::new();
    let key = tree.key("bar");
    let alpha = tree.realm(&local("alpha"));

    let isolated = tree
        .root()
        .derive()
        .bind_all(&[binding("bar", local("alpha"))])
        .build();
    let nested = isolated.derive().build();

    assert_eq!(isolated.own_realm(key), Some(alpha));
    assert_eq!(isolated.realm_of(key), alpha);
    assert_eq!(nested.realm_of(key), alpha);
    assert_eq!(nested.own_realm(key), None);
    assert_eq!(tree.root().realm_of(key), RealmId::ROOT);
}

#[test]
fn the_nearest_binding_wins_over_an_ancestors() {
    let tree = ContextTree::<()>::new();
    let key = tree.key("bar");
    let beta = tree.realm(&local("beta"));

    let inner = tree
        .root()
        .derive()
        .bind_all(&[binding("bar", local("alpha"))])
        .build()
        .derive()
        .bind_all(&[binding("bar", local("beta"))])
        .build();

    assert_eq!(inner.realm_of(key), beta);
}

/// TS origin: `packages/loader/tests/isolate.spec.ts`, `special case: nested realms` —
/// structural half: an ancestor edit for an already-overridden key changes no
/// descendant's realm, so no consumer has a reason to restart.
#[test]
fn a_redundant_ancestor_binding_does_not_move_a_nested_consumer() {
    let tree = ContextTree::<()>::new();
    let key = tree.key("bar");
    let custom = Realm::Shared("custom".into());

    let outer = tree.root().derive().build();
    let inner = outer
        .derive()
        .bind_all(&[binding("bar", custom.clone())])
        .build();
    let consumer = inner.derive().build();

    let before = consumer.realm_of(key);
    let edited_outer = tree
        .root()
        .derive()
        .bind_all(&[binding("bar", custom.clone())])
        .build();
    let after = edited_outer
        .derive()
        .bind_all(&[binding("bar", custom)])
        .build()
        .derive()
        .build()
        .realm_of(key);

    assert_eq!(before, tree.realm(&Realm::Shared("custom".into())));
    assert_eq!(after, before);
}

/// TS origin: `packages/core/tests/isolate.spec.ts`, `shared label` — structural half:
/// a shared realm gives two separate subtrees one slot, a local realm gives two.
#[test]
fn a_shared_realm_joins_sibling_subtrees_that_local_realms_keep_apart() {
    let tree = ContextTree::<()>::new();
    let key = tree.key("bar");
    let root = tree.root();

    let shared_left = root
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let shared_right = root
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let local_left = root
        .derive()
        .bind_all(&[binding("bar", local("one"))])
        .build();
    let local_right = root
        .derive()
        .bind_all(&[binding("bar", local("two"))])
        .build();

    assert_eq!(shared_left.realm_of(key), shared_right.realm_of(key));
    assert_ne!(local_left.realm_of(key), local_right.realm_of(key));
    assert_ne!(shared_left.realm_of(key), root.realm_of(key));
}

#[test]
fn one_derivation_binds_many_keys_and_the_last_binding_of_a_key_wins() {
    let tree = ContextTree::<()>::new();
    let (bar, qux) = (tree.key("bar"), tree.key("qux"));

    let ctx = tree
        .root()
        .derive()
        .bind_all(&[
            binding("bar", local("alpha")),
            binding("qux", local("alpha")),
            binding("bar", local("beta")),
        ])
        .build();

    assert_eq!(ctx.realm_of(bar), tree.realm(&local("beta")));
    assert_eq!(ctx.realm_of(qux), tree.realm(&local("alpha")));
}

#[test]
fn intercept_overlays_read_nearest_first_and_stop_at_the_root() {
    let tree = ContextTree::<u32>::new();
    let (bar, qux) = (tree.key("bar"), tree.key("qux"));

    let outer = tree.root().derive().intercept(bar, 1).build();
    let inner = outer.derive().intercept(bar, 2).intercept(qux, 7).build();

    assert_eq!(inner.intercept_of(bar), Some(&2));
    assert_eq!(outer.intercept_of(bar), Some(&1));
    assert_eq!(tree.root().intercept_of(bar), None);
    assert_eq!(
        inner.intercept_chain(bar).copied().collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert_eq!(
        inner.intercept_chain(qux).copied().collect::<Vec<_>>(),
        vec![7]
    );
    assert_eq!(inner.intercept_chain(tree.key("none")).count(), 0);
}

/// Isolation and interception are independent lookups over the same layer chain.
#[test]
fn an_intercept_overlay_does_not_move_a_realm_binding() {
    let tree = ContextTree::<u32>::new();
    let key = tree.key("bar");

    let isolated = tree
        .root()
        .derive()
        .bind_all(&[binding("bar", local("alpha"))])
        .build();
    let intercepted = isolated.derive().intercept(key, 5).build();

    assert_eq!(intercepted.realm_of(key), tree.realm(&local("alpha")));
    assert_eq!(intercepted.intercept_of(key), Some(&5));
    assert_eq!(isolated.intercept_of(key), None);
}

/// A context chain is only as deep as the plugin tree that derived it, but freeing one
/// must not recurse: a stack overflow aborts the process, so it is not a failure any
/// kernel boundary can contain (R11). Beyond roughly 5k layers a recursive drop
/// overflows a 2 MiB thread stack.
#[test]
fn freeing_a_deep_chain_does_not_recurse() {
    // Miri interprets every allocation, so it walks a shorter chain: it is checking the
    // unlink for undefined behaviour, not the host's stack depth.
    const DEPTH: u32 = if cfg!(miri) { 512 } else { 50_000 };

    let tree = ContextTree::<()>::new();
    let mut ctx = tree.root();
    for _ in 0..DEPTH {
        ctx = ctx.derive().build();
    }

    assert_eq!(ctx.depth(), DEPTH);
    assert_eq!(ctx.realm_of(tree.key("bar")), RealmId::ROOT);
    drop(ctx);
}

/// Deriving never copies an ancestor's bindings: the child owns only what it bound.
#[test]
fn a_derived_layer_owns_only_its_own_bindings() {
    let tree = ContextTree::<()>::new();
    let (bar, qux) = (tree.key("bar"), tree.key("qux"));

    let ancestor = tree
        .root()
        .derive()
        .bind_all(&[binding("bar", local("alpha"))])
        .build();
    let child = ancestor
        .derive()
        .isolate(qux, tree.realm(&local("beta")))
        .build();

    assert_eq!(child.own_realm(bar), None);
    assert_eq!(child.own_realm(qux), Some(tree.realm(&local("beta"))));
    assert_eq!(child.realm_of(bar), tree.realm(&local("alpha")));
}

/// Interception is a chain of its own: unlike resolution, it is not cut by an
/// isolation boundary, matching the TS original's separate intercept prototype chain.
#[test]
fn an_isolation_boundary_does_not_cut_the_intercept_chain() {
    let tree = ContextTree::<u32>::new();
    let key = tree.key("bar");

    let outer = tree.root().derive().intercept(key, 1).build();
    let isolated = outer
        .derive()
        .bind_all(&[binding("bar", Realm::Shared("beta".into()))])
        .build();
    let inner = isolated.derive().intercept(key, 2).build();

    assert_eq!(
        inner.intercept_chain(key).copied().collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert_eq!(isolated.intercept_of(key), Some(&1));
}

#[test]
fn the_root_has_no_ancestors_and_every_handle_names_its_tree() {
    let tree = ContextTree::<()>::new();
    let root = tree.root();

    assert_eq!(root.ancestors().count(), 0);
    assert_eq!(tree.root(), root);
    assert_eq!(root.derive().build().tree().root(), root);
}

/// The interners are shared state; concurrent derivation must agree on identities and
/// hand out distinct context ids.
#[test]
fn concurrent_derivation_agrees_on_identities() {
    use std::collections::HashSet;
    use std::thread;

    let tree = ContextTree::<()>::new();
    let ids: HashSet<_> = thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let tree = tree.clone();
                scope.spawn(move || {
                    let key = tree.key("bar");
                    let realm = tree.realm(&local("alpha"));
                    let ctx = tree.root().derive().isolate(key, realm).build();
                    (ctx.id(), key, ctx.realm_of(key))
                })
            })
            .collect();
        handles
            .into_iter()
            .filter_map(|handle| handle.join().ok())
            .collect::<Vec<_>>()
    })
    .into_iter()
    .collect();

    assert_eq!(ids.len(), 8, "each derivation gets a distinct context id");
    let tree_key = tree.key("bar");
    assert!(
        ids.iter()
            .all(|(_, key, realm)| { *key == tree_key && *realm == tree.realm(&local("alpha")) })
    );
}
