#![allow(dead_code)]

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
        case.origin.ends_with(".spec.ts"),
        "TS origin must name a spec file"
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
