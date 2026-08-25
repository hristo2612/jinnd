//! Static dependency-cycle detection at plan time (I3, M1-P6c).
//!
//! The paper's progress theorem holds for acyclic dependency precedence;
//! cycles are required to be detected statically, their members left cleanly
//! inactive. The declarations are already static — each registered lane names
//! what its package injects and provides — so the loader can chart the
//! consumer→provider graph over the desired document before spawning anything.
//!
//! Realm resolution here is the plan-time approximation of the runtime walk:
//! the nearest isolation binding on the entry's ancestry decides a service
//! name's realm (root when unbound), and an edge requires the consumer and
//! provider to resolve the service in the SAME realm. Named realms are
//! realm-global and root-realm provisions are anchored at the tree root by
//! the lanes, so realm equality is exactly the visibility the runtime grants.

use std::any::TypeId;
use std::collections::HashMap;

use jinnd_api::{EntryFault, EntryId, ErrorCode, KernelError, Realm};

use crate::lanes::PackageLane;
use crate::loader::LaneConfig;
use crate::tree::EntryIndex;

/// The faults for every entry involved in a declared dependency cycle: the
/// plan drops these entries' steps, so cycle members are never spawned —
/// cleanly inactive with the recorded error (I3, R11) — and their acyclic
/// siblings load untouched.
pub(crate) fn cycle_faults<C: LaneConfig>(
    index: &EntryIndex<'_, C>,
    lanes: &HashMap<(String, TypeId), std::sync::Arc<PackageLane>>,
) -> Vec<EntryFault> {
    // Nodes: effectively enabled plugin entries with a registered lane.
    let mut nodes: Vec<&EntryId> = Vec::new();
    let mut declarations: HashMap<&EntryId, &PackageLane> = HashMap::new();
    for entry in index.entries() {
        if index.effectively_disabled(&entry.id) {
            continue;
        }
        let Some(lane) = lanes.get(&(entry.plugin.package.clone(), TypeId::of::<C>())) else {
            continue;
        };
        nodes.push(&entry.id);
        declarations.insert(&entry.id, lane.as_ref());
    }

    // Providers by (service name, resolved realm).
    let mut providers: HashMap<(&str, Realm), Vec<usize>> = HashMap::new();
    for (position, id) in nodes.iter().enumerate() {
        if let Some(service) = &declarations[id].provides {
            providers
                .entry((service.name, effective_realm(index, id, service.name)))
                .or_default()
                .push(position);
        }
    }

    // Edges: consumer → every provider visible in the consumer's realm.
    let edges: Vec<Vec<usize>> = nodes
        .iter()
        .map(|id| {
            declarations[id]
                .injects
                .iter()
                .flat_map(|service| {
                    providers
                        .get(&(service.name, effective_realm(index, id, service.name)))
                        .into_iter()
                        .flatten()
                        .copied()
                })
                .collect()
        })
        .collect();

    cyclic_members(&edges)
        .into_iter()
        .map(|position| EntryFault {
            entry: nodes[position].clone(),
            error: KernelError {
                code: ErrorCode::DependencyCycle,
                message: "the entry's dependency declarations form a cycle: detected \
                          statically, the entry is left cleanly inactive (I3)"
                    .to_owned(),
                fiber: None,
            },
        })
        .collect()
}

/// The realm the entry's context will resolve `name` in: the nearest binding
/// on its ancestry, root when unbound. Mirrors context derivation, statically.
fn effective_realm<C>(index: &EntryIndex<'_, C>, id: &EntryId, name: &str) -> Realm {
    for (_, bindings) in index.environment(id).iter().rev() {
        if let Some(binding) = bindings.iter().find(|binding| binding.service == name) {
            return binding.realm.clone();
        }
    }
    Realm::Root
}

/// The nodes on at least one directed cycle: members of any strongly
/// connected component with more than one node, or with a self-edge.
/// Iterative Tarjan — no recursion, whatever the document's shape (R11).
fn cyclic_members(edges: &[Vec<usize>]) -> Vec<usize> {
    let n = edges.len();
    let (mut order, mut low) = (vec![usize::MAX; n], vec![0_usize; n]);
    let mut on_stack = vec![false; n];
    let (mut stack, mut next_order) = (Vec::new(), 0_usize);
    let mut members = Vec::new();

    for root in 0..n {
        if order[root] != usize::MAX {
            continue;
        }
        // Explicit DFS frames: (node, next child index).
        let mut frames: Vec<(usize, usize)> = vec![(root, 0)];
        while let Some(&mut (node, ref mut child)) = frames.last_mut() {
            if *child == 0 {
                order[node] = next_order;
                low[node] = next_order;
                next_order += 1;
                stack.push(node);
                on_stack[node] = true;
            }
            if let Some(&target) = edges[node].get(*child) {
                *child += 1;
                if order[target] == usize::MAX {
                    frames.push((target, 0));
                } else if on_stack[target] {
                    low[node] = low[node].min(order[target]);
                }
                continue;
            }
            frames.pop();
            if let Some(&(parent, _)) = frames.last() {
                low[parent] = low[parent].min(low[node]);
            }
            if low[node] == order[node] {
                let mut component = Vec::new();
                while let Some(member) = stack.pop() {
                    on_stack[member] = false;
                    component.push(member);
                    if member == node {
                        break;
                    }
                }
                let looped = component.len() > 1
                    || component
                        .first()
                        .is_some_and(|&only| edges[only].contains(&only));
                if looped {
                    members.extend(component);
                }
            }
        }
    }
    members.sort_unstable();
    members
}

#[cfg(test)]
mod tests {
    use super::cyclic_members;

    #[test]
    fn an_acyclic_graph_has_no_cyclic_members() {
        // 0 -> 1 -> 2, 0 -> 2: the paper's qux/foo/bar shape.
        let edges = vec![vec![1, 2], vec![2], vec![]];
        assert!(cyclic_members(&edges).is_empty());
    }

    #[test]
    fn a_three_cycle_is_reported_in_full_and_its_tail_is_not() {
        // 0 -> 1 -> 2 -> 0, with 3 -> 0 hanging off the cycle.
        let edges = vec![vec![1], vec![2], vec![0], vec![0]];
        assert_eq!(cyclic_members(&edges), vec![0, 1, 2]);
    }

    #[test]
    fn a_self_edge_is_a_cycle() {
        let edges = vec![vec![0], vec![]];
        assert_eq!(cyclic_members(&edges), vec![0]);
    }

    #[test]
    fn two_disjoint_cycles_are_both_reported() {
        let edges = vec![vec![1], vec![0], vec![3], vec![2], vec![]];
        assert_eq!(cyclic_members(&edges), vec![0, 1, 2, 3]);
    }
}
