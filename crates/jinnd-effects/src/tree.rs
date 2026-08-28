//! The record forest behind a scope, and the two walks over it.
//!
//! Every walk here is a loop. A record owns its children, so anything recursive —
//! including the destructor Rust would derive — overflows the stack on a deeply
//! nested tree, and a stack overflow aborts the process rather than failing locally
//! (R11).

use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};

use jinnd_api::{EffectDescriptor, EffectId};

use crate::disposer::Disposer;

/// Effect identity is process-wide, so an [`EffectId`] means the same effect wherever
/// it is quoted — a report, a ledger entry, or another scope's records.
static NEXT_EFFECT: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_id() -> EffectId {
    EffectId(NEXT_EFFECT.fetch_add(1, Ordering::Relaxed))
}

/// One applied effect: what it was called, how to withdraw it, and what was applied
/// underneath it.
pub(crate) struct Record {
    pub(crate) id: EffectId,
    pub(crate) label: String,
    pub(crate) disposer: Disposer,
    /// A drain phase, run before ANY inverse of a full withdrawal (I2): the
    /// effect stops accepting dependents and waits them out, its inverse still
    /// owed afterwards. `None` for ordinary effects.
    pub(crate) drain: Option<Disposer>,
    /// A suspend path (M2-K4): what a SUSPEND replay runs instead of the
    /// inverse — a world mutation's "retain, release nothing". `None` for a
    /// kernel registration, whose inverse IS its release.
    pub(crate) suspend: Option<Disposer>,
    pub(crate) children: Vec<Record>,
}

/// Takes every pending drain phase, in replay order (children before the
/// effect they nested under, last registration first). The records stay live —
/// only their drain phase moves out, so a drain runs at most once.
pub(crate) fn drains(roots: &mut [Record]) -> Vec<(EffectId, String, Disposer)> {
    let mut stack: Vec<&mut Record> = roots.iter_mut().rev().collect();
    let mut collected = Vec::new();
    while let Some(record) = stack.pop() {
        if let Some(drain) = record.drain.take() {
            collected.push((record.id, record.label.clone(), drain));
        }
        stack.extend(record.children.iter_mut().rev());
    }
    collected.reverse();
    collected
}

/// The live record `id` names.
pub(crate) fn find(roots: &mut [Record], id: EffectId) -> Option<&mut Record> {
    let mut stack: Vec<&mut Record> = roots.iter_mut().collect();
    while let Some(record) = stack.pop() {
        if record.id == id {
            return Some(record);
        }
        stack.extend(record.children.iter_mut());
    }
    None
}

/// Removes the live record `id` names, with its whole subtree, from the forest.
pub(crate) fn take(roots: &mut Vec<Record>, id: EffectId) -> Option<Record> {
    let mut stack: Vec<&mut Vec<Record>> = vec![roots];
    while let Some(level) = stack.pop() {
        if let Some(index) = level.iter().position(|record| record.id == id) {
            return Some(level.remove(index));
        }
        for record in level.iter_mut() {
            stack.push(&mut record.children);
        }
    }
    None
}

/// Flattens a forest into replay order: children before the effect they nested under,
/// last registration first.
///
/// Pre-order in registration order, reversed, is exactly that order. Every returned
/// record is childless, so dropping the result does not recurse either.
pub(crate) fn flatten(roots: Vec<Record>) -> Vec<Record> {
    let mut stack: Vec<Record> = roots.into_iter().rev().collect();
    let mut order = Vec::new();
    while let Some(mut record) = stack.pop() {
        let children = mem::take(&mut record.children);
        order.push(record);
        stack.extend(children.into_iter().rev());
    }
    order.reverse();
    order
}

/// Removes the next record to withdraw: the deepest last-registered live effect.
///
/// Repeated calls yield exactly the order [`flatten`] produces, one record at a
/// time, so a teardown that stops early leaves the rest of the forest standing —
/// still nested, still replayable. The returned record is always childless.
///
/// One call walks the deepest path, so a whole teardown costs the forest's size
/// times its depth. Effect nesting mirrors plugin structure and is shallow; the
/// pathologically deep forest is the destructor's problem, and that one is
/// [`flatten`]'s single pass.
pub(crate) fn take_next(roots: &mut Vec<Record>) -> Option<Record> {
    let mut path = Vec::new();
    let mut level: &[Record] = roots;
    while let Some(last) = level.len().checked_sub(1) {
        path.push(last);
        level = &level[last].children;
    }
    path.pop()?;

    let mut siblings = roots;
    for index in path {
        siblings = &mut siblings[index].children;
    }
    siblings.pop()
}

/// Publishes a forest as facade descriptors, in registration order.
pub(crate) fn describe(roots: &[Record]) -> Vec<EffectDescriptor> {
    let mut nodes: Vec<(&Record, Option<usize>)> = Vec::new();
    let mut stack: Vec<(&Record, Option<usize>)> =
        roots.iter().rev().map(|record| (record, None)).collect();
    while let Some((record, parent)) = stack.pop() {
        let index = nodes.len();
        nodes.push((record, parent));
        stack.extend(
            record
                .children
                .iter()
                .rev()
                .map(|child| (child, Some(index))),
        );
    }

    // A child always holds a higher index than its parent, so building from the back
    // finishes a descriptor's children before the descriptor itself.
    let mut built: Vec<Vec<EffectDescriptor>> = vec![Vec::new(); nodes.len()];
    let mut roots = Vec::new();
    for index in (0..nodes.len()).rev() {
        let (record, parent) = nodes[index];
        let mut children = mem::take(&mut built[index]);
        children.reverse();
        let descriptor = EffectDescriptor {
            id: record.id,
            label: record.label.clone(),
            children,
        };
        match parent {
            Some(parent) => built[parent].push(descriptor),
            None => roots.push(descriptor),
        }
    }
    roots.reverse();
    roots
}
