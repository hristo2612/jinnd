#![allow(dead_code, unused_imports, unused_macros)]

use jinnd_api::FiberState;

pub const NO_KERNEL_REASON: &str =
    "M1-P0 defines the contract only; bind this case to the kernel implementation";

#[derive(Debug)]
pub struct StateAt {
    pub millis: u64,
    pub state: FiberState,
}

#[derive(Debug)]
pub struct SpecCase<'a> {
    pub origin: &'a str,
    pub test_name: &'a str,
    pub setup: &'a [&'a str],
    pub actions: &'a [&'a str],
    pub expected: &'a [&'a str],
    pub states: &'a [StateAt],
}

#[track_caller]
pub fn pending(case: &SpecCase<'_>) -> ! {
    assert!(
        case.origin.ends_with(".spec.ts")
            || case.origin.starts_with("paper:")
            || case.origin.starts_with("rule:"),
        "origin must name a TS spec, paper theorem, or numbered rule"
    );
    assert!(!case.test_name.is_empty(), "TS test name must be recorded");
    assert!(
        !case.actions.is_empty(),
        "ported case must exercise an action"
    );
    assert!(
        !case.expected.is_empty(),
        "ported case must encode an observable result"
    );
    todo!(
        "{NO_KERNEL_REASON}: {} :: {}; expected={:?}; states={:?}",
        case.origin,
        case.test_name,
        case.expected,
        case.states,
    )
}

macro_rules! spec_case {
    (
        $(#[$meta:meta])*
        $name:ident,
        origin: $origin:literal,
        test: $test_name:literal,
        setup: [$($setup:literal),* $(,)?],
        actions: [$($action:literal),+ $(,)?],
        expected: [$($expected:literal),+ $(,)?]
    ) => {
        $(#[$meta])*
        #[test]
        fn $name() {
            $crate::support::pending(&$crate::support::SpecCase {
                origin: $origin,
                test_name: $test_name,
                setup: &[$($setup),*],
                actions: &[$($action),+],
                expected: &[$($expected),+],
                states: &[],
            });
        }
    };
}

pub(crate) use spec_case;
