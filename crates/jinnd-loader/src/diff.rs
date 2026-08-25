//! Reconcile-by-id: the pure diff between the applied document and the desired
//! one. Only affected entries appear in the plan (LAW §3; I1/I4 seeds); realm
//! relevance is NOT decided here — a Rebind hands the entry's new context to
//! the epoch machinery, which alone decides whether anything reloads (R9: no
//! silent replacement, no loader guessing).

use std::collections::HashSet;

use jinnd_api::{EntryFault, EntryId, Profile};

use crate::tree::EntryIndex;

/// One planned operation on one entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StepKind {
    /// New effectively-enabled entry: build its context and spawn its fiber.
    Create,
    /// New entry that is effectively disabled: track it, spawn nothing.
    Track,
    /// Existing entry became effectively enabled: spawn.
    Enable,
    /// Existing entry became effectively disabled: dispose its fiber, keep it.
    Disable,
    /// Entry left the document: dispose and forget it.
    Remove,
    /// The plugin reference changed: dispose the old fiber, spawn a new one.
    Replace,
    /// Only the config changed: restate it and reload the same fiber.
    Restate,
    /// The binding environment changed: rebuild the entry's context and let
    /// epoch identity decide whether its fiber moves.
    Rebind,
}

/// One plan entry, in application order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Step {
    pub entry: EntryId,
    pub kind: StepKind,
}

/// The full outcome of one diff.
#[derive(Clone, Debug, Default)]
pub struct Plan {
    /// Steps in application order: disposals deepest-first, then rebinds,
    /// replaces and restates, then creations shallowest-first.
    pub steps: Vec<Step>,
    /// Structural per-entry faults; the faulted entries are planned around
    /// (R11) and their previous runtime, if any, is left untouched.
    pub faults: Vec<EntryFault>,
    /// Valid entries the plan does not need to touch.
    pub unchanged: Vec<EntryId>,
}

/// Diffs the applied document (`old`, `None` before the first reconcile)
/// against the desired one.
///
/// Entry configs are compared in their canonical `Debug` rendering: config is
/// plain data, never behavior (R9), so its rendering is a faithful canonical
/// form — and the facade's `reconcile` bound stays exactly
/// `Clone + Debug + Send + Sync` (R12).
#[must_use]
pub fn plan<C: std::fmt::Debug>(old: Option<&Profile<C>>, new: &Profile<C>) -> Plan {
    let new_index = EntryIndex::new(new);
    let old_index = old.map(EntryIndex::new);
    let mut outcome = Plan {
        faults: new_index.faults.clone(),
        ..Plan::default()
    };

    let mut disposals = Vec::new();
    let mut rebinds = Vec::new();
    let mut swaps = Vec::new();
    let mut restates = Vec::new();
    let mut creations = Vec::new();

    for entry in new_index.entries() {
        let enabled = !new_index.effectively_disabled(&entry.id);
        let previous = old_index
            .as_ref()
            .and_then(|old_index| old_index.get(&entry.id));
        let Some(previous) = previous else {
            let kind = if enabled {
                StepKind::Create
            } else {
                StepKind::Track
            };
            creations.push((new_index.depth(&entry.id), kind, &entry.id));
            continue;
        };
        // Unwrap is shaped away: `previous` exists only when old_index does.
        let Some(old_index) = old_index.as_ref() else {
            continue;
        };
        let was_enabled = !old_index.effectively_disabled(&entry.id);
        match (was_enabled, enabled) {
            (true, false) => {
                disposals.push((new_index.depth(&entry.id), StepKind::Disable, &entry.id))
            }
            (false, true) => {
                creations.push((new_index.depth(&entry.id), StepKind::Enable, &entry.id))
            }
            (false, false) => outcome.unchanged.push(entry.id.clone()),
            (true, true) => {
                if previous.plugin != entry.plugin {
                    swaps.push(&entry.id);
                    continue;
                }
                let rebind = old_index.environment(&entry.id) != new_index.environment(&entry.id);
                let restate = canonical(&previous.config) != canonical(&entry.config);
                if rebind {
                    rebinds.push((new_index.depth(&entry.id), StepKind::Rebind, &entry.id));
                }
                if restate {
                    restates.push(&entry.id);
                }
                if !rebind && !restate {
                    outcome.unchanged.push(entry.id.clone());
                }
            }
        }
    }

    // Entries that left the document entirely. An entry that is merely faulted
    // in the new document is NOT removed: its runtime is left untouched (R11).
    let desired: HashSet<&EntryId> = new.entries.iter().map(|entry| &entry.id).collect();
    if let Some(old_index) = old_index.as_ref() {
        for entry in old_index.entries() {
            if !desired.contains(&entry.id) {
                disposals.push((old_index.depth(&entry.id), StepKind::Remove, &entry.id));
            }
        }
    }

    // Disposals run deepest-first; everything else shallowest-first.
    disposals.sort_by_key(|step| std::cmp::Reverse(step.0));
    rebinds.sort_by_key(|step| step.0);
    creations.sort_by_key(|step| step.0);

    let steps = &mut outcome.steps;
    steps.extend(step_of(disposals));
    steps.extend(step_of(rebinds));
    steps.extend(swaps.into_iter().map(|entry| Step {
        entry: entry.clone(),
        kind: StepKind::Replace,
    }));
    steps.extend(restates.into_iter().map(|entry| Step {
        entry: entry.clone(),
        kind: StepKind::Restate,
    }));
    steps.extend(step_of(creations));
    outcome
}

/// One config's canonical comparison form at this boundary.
fn canonical<C: std::fmt::Debug>(config: &C) -> String {
    format!("{config:?}")
}

fn step_of<'a>(ranked: Vec<(usize, StepKind, &'a EntryId)>) -> impl Iterator<Item = Step> + 'a {
    ranked.into_iter().map(|(_, kind, entry)| Step {
        entry: entry.clone(),
        kind,
    })
}
