//! Nested-effect registration (M1-P7 round 2: the authorized surface for the
//! `cordis_dispose` "yield dispose" gap): parent-child tree shape, LIFO
//! subtree withdrawal, idempotent disposal.

use std::sync::{Arc, Mutex};

use jinnd_api::{Kernel, KernelFuture, Undo};

struct MarkUndo(Arc<Mutex<Vec<u32>>>, u32);

impl Undo for MarkUndo {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(self.1);
        Box::pin(async { Ok(()) })
    }
}

fn read(log: &Arc<Mutex<Vec<u32>>>) -> Vec<u32> {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

#[tokio::test]
async fn child_effects_nest_in_the_tree_and_unwind_before_their_parent() {
    let kernel = jinnd_adapter::kernel();
    let log = Arc::new(Mutex::new(Vec::new()));
    let parent = kernel
        .register_effect(
            kernel.root_context(),
            "parent".to_owned(),
            Box::new(MarkUndo(Arc::clone(&log), 1)),
        )
        .unwrap_or_else(|error| panic!("parent: {error:?}"));
    let child = kernel
        .register_child_effect(
            parent,
            "child".to_owned(),
            Box::new(MarkUndo(Arc::clone(&log), 2)),
        )
        .unwrap_or_else(|error| panic!("child: {error:?}"));
    kernel
        .register_child_effect(
            child,
            "grandchild".to_owned(),
            Box::new(MarkUndo(Arc::clone(&log), 3)),
        )
        .unwrap_or_else(|error| panic!("grandchild: {error:?}"));

    let tree = kernel.effect_tree(jinnd_adapter::KERNEL_SCOPE);
    let root = tree
        .iter()
        .find(|effect| effect.id == parent)
        .unwrap_or_else(|| panic!("the parent must be live in the tree"));
    assert_eq!(root.children.len(), 1, "the tree keeps parent-child shape");
    assert_eq!(root.children[0].label, "child");
    assert_eq!(root.children[0].children[0].label, "grandchild");

    kernel
        .dispose_effect(parent)
        .await
        .unwrap_or_else(|error| panic!("dispose: {error:?}"));
    assert_eq!(
        read(&log),
        vec![3, 2, 1],
        "children unwind before their parent, LIFO"
    );
    kernel
        .dispose_effect(parent)
        .await
        .unwrap_or_else(|error| panic!("re-dispose: {error:?}"));
    assert_eq!(read(&log), vec![3, 2, 1], "disposal is idempotent");
}

#[tokio::test]
async fn a_child_under_a_dead_parent_is_refused() {
    let kernel = jinnd_adapter::kernel();
    let log = Arc::new(Mutex::new(Vec::new()));
    assert!(
        kernel
            .register_child_effect(
                jinnd_api::EffectId(u64::MAX),
                "orphan".to_owned(),
                Box::new(MarkUndo(log, 9)),
            )
            .is_err(),
        "a parent that is not live refuses the child"
    );
}
