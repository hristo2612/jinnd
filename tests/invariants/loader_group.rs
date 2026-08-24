mod support;

use support::spec_case;

spec_case! {
    /// TS origin: `packages/loader/tests/group.spec.ts`, test `Group: basic support / initialize`.
    nested_groups_initialize_all_enabled_children,
    origin: "packages/loader/tests/group.spec.ts",
    test: "Group: basic support / initialize",
    setup: ["outer group contains foo", "inner group nested under outer also contains foo"],
    actions: ["create both groups and settle"],
    expected: ["both foo entries activate", "four profile entries remain addressable"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/group.spec.ts`, test `Group: basic support / disable inner`.
    disabling_inner_group_disposes_only_inner_subtree,
    origin: "packages/loader/tests/group.spec.ts",
    test: "Group: basic support / disable inner",
    setup: ["outer and nested inner groups are active with one child each"],
    actions: ["disable inner group"],
    expected: ["one child disposes", "outer child stays active", "all four entries remain in profile"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/group.spec.ts`, test `Group: basic support / disable outer`.
    disabling_outer_group_disposes_remaining_enabled_subtree,
    origin: "packages/loader/tests/group.spec.ts",
    test: "Group: basic support / disable outer",
    setup: ["inner group is already disabled", "outer child remains active"],
    actions: ["disable outer group"],
    expected: ["remaining outer child disposes once", "entry tree is retained"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/group.spec.ts`, test `Group: basic support / enable inner`.
    enabling_inner_under_disabled_outer_does_not_activate_children,
    origin: "packages/loader/tests/group.spec.ts",
    test: "Group: basic support / enable inner",
    setup: ["outer and inner groups are disabled"],
    actions: ["enable inner only"],
    expected: ["no child activates or disposes", "effective disabled state is inherited"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/group.spec.ts`, test `Group: basic support / enable outer`.
    enabling_outer_reactivates_all_effectively_enabled_descendants,
    origin: "packages/loader/tests/group.spec.ts",
    test: "Group: basic support / enable outer",
    setup: ["outer disabled while inner is locally enabled"],
    actions: ["enable outer and settle"],
    expected: ["both child plugins activate once", "entry tree identity is unchanged"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/group.spec.ts`, test `Group: transfer / initialize`.
    transfer_fixture_tracks_effective_group_state,
    origin: "packages/loader/tests/group.spec.ts",
    test: "Group: transfer / initialize",
    setup: ["active standalone plugin", "active alpha group", "disabled beta under alpha", "enabled gamma under beta"],
    actions: ["settle initial profile"],
    expected: ["standalone plugin activates once", "four entries are addressable"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/group.spec.ts`, test `Group: transfer / enabled -> enabled`.
    moving_entry_between_enabled_parents_preserves_activation,
    origin: "packages/loader/tests/group.spec.ts",
    test: "Group: transfer / enabled -> enabled",
    setup: ["plugin is active at root", "alpha parent is enabled"],
    actions: ["move plugin under alpha without config changes"],
    expected: ["plugin neither restarts nor disposes", "entry identity is preserved"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/group.spec.ts`, test `Group: transfer / enabled -> disabled`.
    moving_entry_into_disabled_parent_disposes_once,
    origin: "packages/loader/tests/group.spec.ts",
    test: "Group: transfer / enabled -> disabled",
    setup: ["plugin is active under alpha", "beta parent is disabled"],
    actions: ["move plugin under beta"],
    expected: ["plugin disposes once and remains addressable"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/group.spec.ts`, test `Group: transfer / disabled -> disabled`.
    moving_entry_between_effectively_disabled_parents_is_inert,
    origin: "packages/loader/tests/group.spec.ts",
    test: "Group: transfer / disabled -> disabled",
    setup: ["plugin is inactive under beta", "gamma is enabled but nested under disabled beta"],
    actions: ["move plugin under gamma"],
    expected: ["plugin neither activates nor disposes", "effective disabled state remains true"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/group.spec.ts`, test `Group: transfer / disabled -> enabled`.
    moving_entry_to_enabled_root_activates_once,
    origin: "packages/loader/tests/group.spec.ts",
    test: "Group: transfer / disabled -> enabled",
    setup: ["plugin is inactive under effectively disabled gamma"],
    actions: ["move plugin to root"],
    expected: ["plugin activates once without a redundant disposal"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/group.spec.ts`, test `Group: intercept / initialize`.
    nested_intercept_layers_preserve_right_biased_ancestry,
    origin: "packages/loader/tests/group.spec.ts",
    test: "Group: intercept / initialize",
    setup: ["outer, inner, and entry layers define a, b, and c for the same intercept key"],
    actions: ["activate entry and inspect its immutable intercept chain"],
    expected: ["nearest layer is c=3", "parent layer is b=2", "grandparent layer is a=1"]
}
