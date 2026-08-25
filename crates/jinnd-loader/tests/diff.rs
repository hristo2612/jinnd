//! The pure reconcile-by-id calculus: current document + desired document →
//! minimal plan; only affected entries appear in it (LAW §3, I1/I4 seeds).

use jinnd_api::{EntryId, ErrorCode, IsolationBinding, PluginRef, Profile, ProfileEntry, Realm};
use jinnd_loader::{Step, StepKind, plan};

fn id(text: &str) -> EntryId {
    EntryId(text.to_owned())
}

fn entry(name: &str, config: u32) -> ProfileEntry<u32> {
    ProfileEntry {
        id: id(name),
        plugin: PluginRef {
            package: format!("test/{name}"),
            version: "1".to_owned(),
            artifact_hash: String::new(),
        },
        config,
        disabled: false,
        parent: None,
        isolation: Vec::new(),
    }
}

fn profile(entries: Vec<ProfileEntry<u32>>) -> Profile<u32> {
    Profile { entries }
}

fn kinds(steps: &[Step], entry: &str) -> Vec<StepKind> {
    steps
        .iter()
        .filter(|step| step.entry == id(entry))
        .map(|step| step.kind)
        .collect()
}

#[test]
fn first_reconcile_creates_every_enabled_entry_and_skips_disabled_ones() {
    let mut qux = entry("qux", 4);
    qux.disabled = true;
    let outcome = plan(None, &profile(vec![entry("foo", 1), entry("bar", 2), qux]));

    assert!(outcome.faults.is_empty());
    assert_eq!(kinds(&outcome.steps, "foo"), vec![StepKind::Create]);
    assert_eq!(kinds(&outcome.steps, "bar"), vec![StepKind::Create]);
    // The disabled entry is tracked but spawns nothing.
    assert_eq!(kinds(&outcome.steps, "qux"), vec![StepKind::Track]);
}

#[test]
fn unchanged_entries_produce_no_steps() {
    let old = profile(vec![entry("foo", 1), entry("bar", 2)]);
    let new = profile(vec![entry("foo", 1), entry("bar", 3)]);
    let outcome = plan(Some(&old), &new);

    assert!(kinds(&outcome.steps, "foo").is_empty());
    assert_eq!(kinds(&outcome.steps, "bar"), vec![StepKind::Restate]);
    assert_eq!(outcome.unchanged, vec![id("foo")]);
}

#[test]
fn removal_disable_and_enable_map_to_their_steps() {
    let mut qux = entry("qux", 4);
    qux.disabled = true;
    let old = profile(vec![entry("foo", 1), entry("bar", 2), qux]);

    let mut bar = entry("bar", 2);
    bar.disabled = true;
    let qux = entry("qux", 4);
    let new = profile(vec![entry("foo", 1), bar, qux]);

    let outcome = plan(Some(&old), &new);
    assert!(kinds(&outcome.steps, "foo").is_empty());
    assert_eq!(kinds(&outcome.steps, "bar"), vec![StepKind::Disable]);
    assert_eq!(kinds(&outcome.steps, "qux"), vec![StepKind::Enable]);

    let gone = profile(vec![entry("foo", 1)]);
    let outcome = plan(Some(&new), &gone);
    assert_eq!(kinds(&outcome.steps, "bar"), vec![StepKind::Remove]);
    assert_eq!(kinds(&outcome.steps, "qux"), vec![StepKind::Remove]);
}

#[test]
fn effective_disablement_is_inherited_from_ancestors() {
    let mut outer = entry("outer", 0);
    outer.disabled = true;
    let mut inner = entry("inner", 0);
    inner.parent = Some(id("outer"));
    let mut leaf = entry("leaf", 1);
    leaf.parent = Some(id("inner"));

    // Everything under the disabled outer is effectively disabled: no Create.
    let outcome = plan(
        None,
        &profile(vec![outer.clone(), inner.clone(), leaf.clone()]),
    );
    assert_eq!(kinds(&outcome.steps, "leaf"), vec![StepKind::Track]);

    // Disabling inner under the disabled outer changes nothing effective.
    let old = profile(vec![outer.clone(), inner.clone(), leaf.clone()]);
    inner.disabled = true;
    let mid = profile(vec![outer.clone(), inner.clone(), leaf.clone()]);
    let outcome = plan(Some(&old), &mid);
    assert!(outcome.steps.is_empty());

    // Re-enabling inner while outer stays disabled is equally inert.
    inner.disabled = false;
    let back = profile(vec![outer.clone(), inner.clone(), leaf.clone()]);
    let outcome = plan(Some(&mid), &back);
    assert!(outcome.steps.is_empty());

    // Enabling outer activates the whole effectively-enabled subtree.
    outer.disabled = false;
    let new = profile(vec![outer, inner, leaf]);
    let outcome = plan(Some(&back), &new);
    assert_eq!(kinds(&outcome.steps, "inner"), vec![StepKind::Enable]);
    assert_eq!(kinds(&outcome.steps, "leaf"), vec![StepKind::Enable]);
}

#[test]
fn moving_an_entry_between_equal_binding_environments_is_a_rebind_only() {
    let alpha = entry("alpha", 0);
    let mut plugin = entry("plugin", 1);
    let old = profile(vec![alpha.clone(), plugin.clone()]);

    plugin.parent = Some(id("alpha"));
    let new = profile(vec![alpha, plugin]);
    let outcome = plan(Some(&old), &new);
    // The move rebinds the entry's context; activation survival is the epoch
    // machinery's decision, never a loader-forced restart.
    assert_eq!(kinds(&outcome.steps, "plugin"), vec![StepKind::Rebind]);
}

#[test]
fn ancestor_isolation_edits_rebind_every_descendant() {
    let mut group = entry("group", 0);
    let mut inner = entry("inner", 1);
    inner.parent = Some(id("group"));
    let old = profile(vec![group.clone(), inner.clone()]);

    group.isolation.push(IsolationBinding {
        service: "svc.bar".to_owned(),
        realm: Realm::Shared("beta".to_owned()),
    });
    let new = profile(vec![group, inner.clone()]);
    let outcome = plan(Some(&old), &new);
    assert_eq!(kinds(&outcome.steps, "group"), vec![StepKind::Rebind]);
    assert_eq!(kinds(&outcome.steps, "inner"), vec![StepKind::Rebind]);
}

#[test]
fn plugin_ref_change_replaces_the_fiber() {
    let old = profile(vec![entry("foo", 1)]);
    let mut changed = entry("foo", 1);
    changed.plugin.version = "2".to_owned();
    let outcome = plan(Some(&old), &profile(vec![changed]));
    assert_eq!(kinds(&outcome.steps, "foo"), vec![StepKind::Replace]);
}

#[test]
fn structural_faults_are_contained_per_entry() {
    let orphan = {
        let mut orphan = entry("orphan", 1);
        orphan.parent = Some(id("missing"));
        orphan
    };
    let duplicate = entry("foo", 2);
    let outcome = plan(
        None,
        &profile(vec![entry("foo", 1), duplicate, orphan, entry("ok", 3)]),
    );

    let faulted: Vec<&EntryId> = outcome.faults.iter().map(|fault| &fault.entry).collect();
    assert_eq!(faulted, vec![&id("foo"), &id("orphan")]);
    assert!(
        outcome
            .faults
            .iter()
            .all(|fault| fault.error.code == ErrorCode::InvalidProfile)
    );
    // The good entries still plan (R11: failure is local).
    assert_eq!(kinds(&outcome.steps, "foo"), vec![StepKind::Create]);
    assert_eq!(kinds(&outcome.steps, "ok"), vec![StepKind::Create]);
}

#[test]
fn parent_cycles_fault_every_involved_entry() {
    let mut a = entry("a", 1);
    a.parent = Some(id("b"));
    let mut b = entry("b", 2);
    b.parent = Some(id("a"));
    let outcome = plan(None, &profile(vec![a, b, entry("ok", 3)]));

    assert_eq!(outcome.faults.len(), 2);
    assert_eq!(kinds(&outcome.steps, "ok"), vec![StepKind::Create]);
}

#[test]
fn steps_order_disposals_deepest_first_and_creations_shallowest_first() {
    let group = entry("group", 0);
    let mut child = entry("child", 1);
    child.parent = Some(id("group"));
    let old = profile(vec![group.clone(), child.clone()]);

    let outcome = plan(Some(&old), &profile(vec![]));
    let removes: Vec<&EntryId> = outcome.steps.iter().map(|step| &step.entry).collect();
    assert_eq!(removes, vec![&id("child"), &id("group")]);

    let outcome = plan(None, &old);
    let creates: Vec<&EntryId> = outcome.steps.iter().map(|step| &step.entry).collect();
    assert_eq!(creates, vec![&id("group"), &id("child")]);
}
