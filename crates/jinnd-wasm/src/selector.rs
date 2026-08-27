//! Declarative event selectors, evaluated kernel-side (decision log
//! 2026-08-25, C4 binding): closures never cross the component boundary —
//! the payload's routing choice is data, and realm queries are answered by
//! the kernel against the isolation map, never by the guest.

/// The wire-level selector of `jinn:plugin/types.selector`. Routing stays
/// inverted (LAW §3): the payload selects listeners; listeners never filter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Selector {
    /// Every registered listener of the topic.
    All,
    /// Listeners whose registration context is in this set.
    ContextSet(Vec<u64>),
    /// Listeners whose registration context resolves the named service in
    /// the same realm as the emitter's context.
    RealmOf(String),
}

/// The kernel-side realm oracle: answers whether two contexts resolve a
/// service name in the same realm. The harness lane wires this to the
/// context tree's isolation map; tests pin the seam with a table.
pub trait RealmOracle: Send + Sync + 'static {
    fn same_realm(&self, service: &str, emitter: u64, listener: u64) -> bool;
}

/// An oracle for realm-less deployments: nothing shares a realm, so
/// `realm-of` selects nobody rather than everybody (fail closed).
pub struct NoRealms;

impl RealmOracle for NoRealms {
    fn same_realm(&self, _: &str, _: u64, _: u64) -> bool {
        false
    }
}

/// Evaluates `selector` for one listener, kernel-side.
pub fn selects(selector: &Selector, oracle: &dyn RealmOracle, emitter: u64, listener: u64) -> bool {
    match selector {
        Selector::All => true,
        Selector::ContextSet(contexts) => contexts.contains(&listener),
        Selector::RealmOf(service) => oracle.same_realm(service, emitter, listener),
    }
}

#[cfg(test)]
mod tests {
    use super::{NoRealms, RealmOracle, Selector, selects};

    struct Table;

    impl RealmOracle for Table {
        fn same_realm(&self, service: &str, emitter: u64, listener: u64) -> bool {
            service == "jinn:shared" && emitter == 1 && listener == 2
        }
    }

    #[test]
    fn all_selects_everyone() {
        assert!(selects(&Selector::All, &NoRealms, 1, 99));
    }

    #[test]
    fn context_set_selects_exactly_its_members() {
        let selector = Selector::ContextSet(vec![2, 3]);
        assert!(selects(&selector, &NoRealms, 1, 2));
        assert!(!selects(&selector, &NoRealms, 1, 4));
    }

    #[test]
    fn realm_of_asks_the_kernel_oracle_never_the_guest() {
        let selector = Selector::RealmOf("jinn:shared".to_owned());
        assert!(selects(&selector, &Table, 1, 2));
        assert!(!selects(&selector, &Table, 1, 3));
        assert!(
            !selects(&selector, &NoRealms, 1, 2),
            "no realm knowledge fails closed"
        );
    }
}
