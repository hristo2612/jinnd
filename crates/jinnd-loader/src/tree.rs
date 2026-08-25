//! Entry-tree indexing over a typed profile: structural validation with
//! per-entry faults (R11), effective disablement, depth ordering, and the
//! binding environment used to decide when a context must be rebuilt.

use std::collections::HashMap;

use jinnd_api::{EntryFault, EntryId, ErrorCode, IsolationBinding, KernelError, Profile};

/// A validated view over one profile. Faulted entries are excluded from every
/// derived answer; the rest stand on their own (R11: failure is local).
pub(crate) struct EntryIndex<'a, C> {
    profile: &'a Profile<C>,
    by_id: HashMap<&'a EntryId, usize>,
    /// Entry position → depth from the root (0 for root entries).
    depth: HashMap<&'a EntryId, usize>,
    pub(crate) faults: Vec<EntryFault>,
}

impl<'a, C> EntryIndex<'a, C> {
    pub(crate) fn new(profile: &'a Profile<C>) -> Self {
        let mut by_id: HashMap<&EntryId, usize> = HashMap::new();
        let mut faults = Vec::new();
        for (position, entry) in profile.entries.iter().enumerate() {
            // First occurrence wins; the duplicate is the faulted one (R11).
            if by_id.contains_key(&entry.id) {
                faults.push(fault(&entry.id, "the entry id appears more than once"));
            } else {
                by_id.insert(&entry.id, position);
            }
        }
        let mut index = Self {
            profile,
            by_id,
            depth: HashMap::new(),
            faults,
        };
        index.chart();
        index
    }

    /// Walks every entry's ancestry once, recording depth and faulting entries
    /// whose ancestry is missing, faulted, or cyclic.
    fn chart(&mut self) {
        let ids: Vec<&EntryId> = self.profile.entries.iter().map(|entry| &entry.id).collect();
        for id in ids {
            if !self.by_id.contains_key(id) || self.depth.contains_key(id) {
                continue;
            }
            let mut trail = vec![id];
            let broken = loop {
                let Some(current) = trail.last().copied() else {
                    break None;
                };
                let Some(&position) = self.by_id.get(current) else {
                    break Some(format!("ancestor {current:?} is missing or itself faulted"));
                };
                match &self.profile.entries[position].parent {
                    None => break None,
                    Some(parent) if self.depth.contains_key(parent) => break None,
                    Some(parent) if trail.contains(&parent) => {
                        break Some(format!("the parent chain through {parent:?} is a cycle"));
                    }
                    Some(parent) => trail.push(parent),
                }
            };
            match broken {
                Some(reason) => {
                    for id in trail {
                        if self.by_id.remove(id).is_some() {
                            self.faults.push(fault(id, &reason));
                        }
                    }
                }
                None => {
                    let above = trail
                        .last()
                        .and_then(|deepest| self.parent_of(deepest))
                        .and_then(|parent| self.depth.get(parent).copied())
                        .map_or(0, |parent_depth| parent_depth + 1);
                    for (steps, id) in trail.into_iter().rev().enumerate() {
                        self.depth.insert(id, above + steps);
                    }
                }
            }
        }
    }

    fn parent_of(&self, id: &EntryId) -> Option<&'a EntryId> {
        let position = *self.by_id.get(id)?;
        self.profile.entries[position].parent.as_ref()
    }

    /// The valid entries in document order (a duplicated id yields only its
    /// first, winning occurrence).
    pub(crate) fn entries(&self) -> impl Iterator<Item = &'a jinnd_api::ProfileEntry<C>> {
        self.profile
            .entries
            .iter()
            .enumerate()
            .filter_map(|(position, entry)| {
                (self.by_id.get(&entry.id) == Some(&position)).then_some(entry)
            })
    }

    pub(crate) fn get(&self, id: &EntryId) -> Option<&'a jinnd_api::ProfileEntry<C>> {
        self.by_id
            .get(id)
            .map(|&position| &self.profile.entries[position])
    }

    /// Depth from the root; 0 for root entries.
    pub(crate) fn depth(&self, id: &EntryId) -> usize {
        self.depth.get(id).copied().unwrap_or(0)
    }

    /// Whether the entry or any ancestor is disabled.
    pub(crate) fn effectively_disabled(&self, id: &EntryId) -> bool {
        let mut current = self.get(id);
        while let Some(entry) = current {
            if entry.disabled {
                return true;
            }
            current = entry.parent.as_ref().and_then(|parent| self.get(parent));
        }
        false
    }

    /// The entry's binding environment: every ancestor's identity and isolation
    /// from the root down to the entry's own. Two equal environments derive
    /// observably equal contexts, so comparing them decides Rebind steps.
    pub(crate) fn environment(&self, id: &EntryId) -> Vec<(&'a EntryId, &'a [IsolationBinding])> {
        let mut chain = Vec::new();
        let mut current = self.get(id);
        while let Some(entry) = current {
            chain.push((&entry.id, entry.isolation.as_slice()));
            current = entry.parent.as_ref().and_then(|parent| self.get(parent));
        }
        chain.reverse();
        chain
    }
}

fn fault(entry: &EntryId, reason: &str) -> EntryFault {
    EntryFault {
        entry: entry.clone(),
        error: KernelError {
            code: ErrorCode::InvalidProfile,
            message: reason.to_owned(),
            fiber: None,
        },
    }
}
