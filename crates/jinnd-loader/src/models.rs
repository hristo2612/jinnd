//! Loom models for the engagement cell (M1-P6b): the refuse-not-wait
//! exclusion that keeps loader operations race-safe with no lock held across
//! plugin-facing code (R1).
//!
//! The tokio semaphore half of the gate cannot be expressed in loom's
//! primitives; its safety is structural (no permit holder runs plugin-facing
//! code) and regression-tested in `tests/reenter.rs`. What loom checks here is
//! the one piece of racy shared state: engagement claims from concurrent
//! tasks — every claim is exclusive, refusal never deadlocks or wedges the
//! cell, and entry claims and the document claim exclude each other.

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;

use jinnd_api::EntryId;

use crate::gate::Engagement;

fn entry(name: &str) -> EntryId {
    EntryId(name.to_owned())
}

/// Two tasks racing to engage the same entry: claims never overlap, refusal
/// is the only alternative to winning, and the cell is clean afterwards.
#[test]
fn one_entry_is_engaged_by_at_most_one_operation() {
    loom::model(|| {
        let cell = Arc::new(Engagement::default());
        let active = Arc::new(AtomicBool::new(false));
        let contend = |cell: Arc<Engagement>, active: Arc<AtomicBool>| {
            thread::spawn(move || {
                if !cell.engage_entry(&entry("e")) {
                    return false;
                }
                assert!(
                    !active.swap(true, Ordering::SeqCst),
                    "two operations engaged one entry at once"
                );
                active.store(false, Ordering::SeqCst);
                cell.release_entry(&entry("e"));
                true
            })
        };
        let first = contend(Arc::clone(&cell), Arc::clone(&active));
        let second = contend(Arc::clone(&cell), Arc::clone(&active));
        let first = first.join().unwrap_or_else(|_| unreachable!());
        let second = second.join().unwrap_or_else(|_| unreachable!());
        assert!(first || second, "both racers were refused a free entry");
        assert!(
            cell.engage_entry(&entry("e")),
            "a released entry engages again"
        );
    });
}

/// An entry claim racing the document claim: the two are mutually exclusive
/// in every interleaving, and neither refusal wedges the other side.
#[test]
fn the_document_and_any_entry_exclude_each_other() {
    loom::model(|| {
        let cell = Arc::new(Engagement::default());
        let entry_active = Arc::new(AtomicBool::new(false));
        let document_active = Arc::new(AtomicBool::new(false));

        let amender = {
            let cell = Arc::clone(&cell);
            let entry_active = Arc::clone(&entry_active);
            let document_active = Arc::clone(&document_active);
            thread::spawn(move || {
                if !cell.engage_entry(&entry("e")) {
                    return false;
                }
                entry_active.store(true, Ordering::SeqCst);
                assert!(
                    !document_active.load(Ordering::SeqCst),
                    "an entry engaged while the document was engaged"
                );
                entry_active.store(false, Ordering::SeqCst);
                cell.release_entry(&entry("e"));
                true
            })
        };
        let reconciler = {
            let cell = Arc::clone(&cell);
            let entry_active = Arc::clone(&entry_active);
            let document_active = Arc::clone(&document_active);
            thread::spawn(move || {
                if !cell.engage_document() {
                    return false;
                }
                document_active.store(true, Ordering::SeqCst);
                assert!(
                    !entry_active.load(Ordering::SeqCst),
                    "the document engaged while an entry was engaged"
                );
                document_active.store(false, Ordering::SeqCst);
                cell.release_document();
                true
            })
        };

        let amended = amender.join().unwrap_or_else(|_| unreachable!());
        let reconciled = reconciler.join().unwrap_or_else(|_| unreachable!());
        assert!(amended || reconciled, "both sides were refused a free cell");
        assert!(cell.engage_document(), "a released cell engages again");
    });
}

/// Claims on distinct entries never exclude each other: refusal is only ever
/// answered to a real conflict.
#[test]
fn distinct_entries_engage_independently() {
    loom::model(|| {
        let cell = Arc::new(Engagement::default());
        let other = {
            let cell = Arc::clone(&cell);
            thread::spawn(move || {
                assert!(
                    cell.engage_entry(&entry("left")),
                    "a free entry was refused"
                );
                cell.release_entry(&entry("left"));
            })
        };
        assert!(
            cell.engage_entry(&entry("right")),
            "a distinct entry was refused"
        );
        cell.release_entry(&entry("right"));
        other.join().unwrap_or_else(|_| unreachable!());
    });
}
