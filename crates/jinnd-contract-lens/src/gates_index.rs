//! The two readers of M2-K22 (harness FINDINGS #44), each run as a SWEEP
//! over the contracts of record on disk and each proven to FIRE against the
//! instance that shipped: the contract index naming versions the bundles
//! no longer carry, and a bare world mention citing an edition the world
//! has moved past. Every sweep prints what it swept.

use crate::{Contract, contract_files, index, mentions, world};

/// INDEX sweep: `contracts/README.md` carries the block the lens renders
/// from the parsed bundles, byte for byte, and no undated bare version
/// outside it.
#[test]
fn the_contract_index_carries_the_derived_block_and_nothing_stale() {
    let expected = index::render(&index::rows());
    let readme = Contract::load(index::PATH);
    let found = index::disagreements(readme.path(), readme.text(), &expected);
    println!(
        "index: {} bundle rows derived; {} disagreements",
        index::rows().len(),
        found.len()
    );
    for disagreement in &found {
        println!("  {disagreement}");
    }
    assert!(found.is_empty(), "{found:#?}");
}

/// INDEX red-first: the hand-kept list that shipped (#44), verbatim — no
/// derived block at all, so the file cannot be read; and a stray bare
/// version beside a fresh block is the same copy in a new place.
#[test]
fn the_index_reader_fires_on_the_hand_kept_list_that_shipped() {
    let shipped = "Bundles: `jinn-fs` (0.2.0; atomic commits M2-K8), `jinn-clock` (0.1.0),\n\
                   `jinn-process` (0.1.0), `jinn-net` (0.1.0, readiness wake M2-K7),\n\
                   `jinn-ledger` (0.1.0, finalized M2-K7), `jinn-introspect` (0.1.0),\n";
    let found = index::disagreements(index::PATH, shipped, "unused");
    assert!(
        matches!(found.as_slice(), [index::Disagreement::Malformed { .. }]),
        "{found:#?}"
    );
    let fresh = index::render(&[index::Row::new("jinn-net", "jinn:net", "0.3.0", None)]);
    let beside = format!("{fresh}\nand `jinn-net` (0.1.0, readiness wake M2-K7) in prose\n");
    let found = index::disagreements(index::PATH, &beside, &fresh);
    assert_eq!(
        found,
        vec![index::Disagreement::Stray {
            path: index::PATH.to_owned(),
            line: 7,
            version: "0.1.0".to_owned(),
        }]
    );
}

/// INDEX red-first: a block that exists but is STALE (one row behind) is
/// reported at the block's line with the fresh rendering attached; a
/// second block, or an end before a begin, cannot be read.
#[test]
fn the_index_reader_fires_on_a_stale_block_and_refuses_a_malformed_one() {
    let rows = vec![
        index::Row::new("jinn-net", "jinn:net", "0.3.0", Some("net-policy")),
        index::Row::new("jinn-auth", "jinn:auth", "0.1.0", None),
    ];
    let fresh = index::render(&rows);
    let stale = fresh.replace("0.3.0", "0.1.0");
    let text = format!("# Index\n\n{stale}\ntrailing prose\n");
    let found = index::disagreements("x.md", &text, &fresh);
    let [index::Disagreement::Stale { line, expected, .. }] = found.as_slice() else {
        panic!("{found:#?}");
    };
    assert_eq!((*line, expected.as_str()), (3, fresh.as_str()));
    // The rendering itself agrees with what refs.rs reads: `jinn:net@0.3.0`.
    assert_eq!(
        index::disagreements("x.md", &format!("a\n{fresh}"), &fresh),
        vec![]
    );

    for malformed in [
        format!("{fresh}\n{fresh}"),
        format!("{}\n{}\n", index::END, index::BEGIN),
        format!("{}\n| a | b | c |\n", index::BEGIN),
    ] {
        let found = index::disagreements("x.md", &malformed, &fresh);
        assert!(
            matches!(found.as_slice(), [index::Disagreement::Malformed { .. }]),
            "{malformed}\n{found:#?}"
        );
    }
}

/// WORLD-MENTION sweep: every undated bare version on a world-anchored
/// line under `wit/` and `contracts/` names the edition `wit/plugin.wit`
/// declares.
#[test]
fn every_bare_world_mention_in_the_contracts_names_the_declared_edition() {
    let declared = world().wit().version();
    let (mut swept, mut dated, mut claims) = (0, 0, 0);
    let mut found = Vec::new();
    for file in contract_files() {
        for candidate in mentions::candidates(file.text()) {
            swept += 1;
            match candidate {
                mentions::Candidate::Dated { line, version, tag } => {
                    dated += 1;
                    println!("  {}:{line}: world {version} ({tag}), dated", file.path());
                }
                mentions::Candidate::Claim { line, version } => {
                    claims += 1;
                    println!("  {}:{line}: world {version}, a claim", file.path());
                }
                mentions::Candidate::Unreadable { .. } => {}
            }
        }
        found.extend(mentions::disagreements(file.path(), file.text(), &declared));
    }
    println!(
        "world mentions: swept {swept} candidates ({dated} dated, {claims} claims) across {} files; declared {declared}",
        contract_files().len()
    );
    println!("world mentions: {} disagreements", found.len());
    for disagreement in &found {
        println!("  {disagreement}");
    }
    assert!(found.is_empty(), "{found:#?}");
}

/// WORLD-MENTION red-first: `contracts/jinn-net/contract.wit:9` as it
/// shipped (#44), verbatim.
#[test]
fn the_mention_reader_fires_on_the_world_citation_that_shipped() {
    let shipped = "// re-listens in `activate`. Every call is non-blocking (R1). The plugin\n\
                   // world's `net` import (wit/plugin.wit, 0.9.0) carries this interface\n\
                   // verbatim with the broker wire beside each operation; this bundle is the\n";
    let found = mentions::disagreements("contracts/jinn-net/contract.wit", shipped, "0.10.0");
    assert_eq!(
        found,
        vec![mentions::Disagreement::Edition {
            path: "contracts/jinn-net/contract.wit".to_owned(),
            line: 2,
            found: "0.9.0".to_owned(),
            declared: "0.10.0".to_owned(),
        }]
    );
}

/// WORLD-MENTION red-first, round 2 (the verifier's fixture, verbatim): a
/// stale claim wearing a MALFORMED packet tag is not dated by it. The tag
/// grammar is exact — `M<d>-K<d+>` or `M<d>-P<d+>`, then end-of-clause —
/// and an attempt that reads as anything else, or anything but a tag after
/// a separator, FAILS CLOSED at its line: never a claim, never dated.
#[test]
fn malformed_packet_suffix_does_not_exempt_a_stale_world_claim() {
    let found =
        mentions::disagreements("fixture.md", "plugin world 0.9.0, M2-K16-extra\n", "0.10.0");
    assert_eq!(
        found,
        vec![mentions::Disagreement::Unreadable {
            path: "fixture.md".to_owned(),
            line: 1,
            token: "0.9.0, M2-K16-extra".to_owned(),
        }],
        "a suffix outside the stated packet-tag grammar must not date the claim: {found:#?}"
    );

    let boundary = "world 0.9.0, M2-K16extra\n\
                    world 0.9.0 M2-K16_x\n\
                    world 0.9.0 (M2-X16)\n\
                    world 0.9.0, see M2-K16\n\
                    world 0.9.0 M2-K16.1 and more\n\
                    world 0.9.0 Mode-1 swap\n\
                    world 0.9.0, M2-K16.\n\
                    world 0.9.0 M1-P8 and 0.8.0 (M2-K13): dated\n";
    let unreadable = |line: usize, token: &str| mentions::Disagreement::Unreadable {
        path: "b.md".to_owned(),
        line,
        token: token.to_owned(),
    };
    assert_eq!(
        mentions::disagreements("b.md", boundary, "0.10.0"),
        vec![
            unreadable(1, "0.9.0, M2-K16extra"),
            unreadable(2, "0.9.0 M2-K16_x"),
            unreadable(3, "0.9.0 (M2-X16"),
            unreadable(4, "0.9.0, see"),
            unreadable(5, "0.9.0 M2-K16.1"),
            mentions::Disagreement::Edition {
                path: "b.md".to_owned(),
                line: 6,
                found: "0.9.0".to_owned(),
                declared: "0.10.0".to_owned(),
            },
        ]
    );
    let dated = mentions::candidates(boundary)
        .into_iter()
        .filter(|c| matches!(c, mentions::Candidate::Dated { .. }))
        .count();
    assert_eq!(dated, 3);
}

/// WORLD-MENTION grammar: a dated edition is read and never checked, a
/// package reference is refs.rs's and not a mention, an unanchored line
/// holds no candidate, and a dotted run that is not `X.Y.Z` FAILS CLOSED.
#[test]
fn the_mention_reader_reads_dated_editions_and_refuses_what_it_cannot_read() {
    let text = "# Lifecycle classification (world 0.3.0, M2-K4)\n\
                Version history: 0.1.0 M1-P8 world; 0.2.0 M2-K3 `fs`\n\
                /// 0.8.0 (M2-K13): a publisher on this world's own bus\n\
                /// jinn:plugin@0.9.0 — the Tier A plugin world\n\
                the bundle (`contracts/jinn-fs`, 0.2.0) with no anchor\n\
                a World with a trailing-junk edition 0.10.0-rc1\n\
                the world at v0.10.0 and a loopback 127.0.0.1 world\n\
                on the 0.10.0 world: agrees\n";
    let found = mentions::disagreements("x.md", text, "0.10.0");
    let lines: Vec<_> = found
        .iter()
        .map(|d| match d {
            mentions::Disagreement::Unreadable { line, token, .. } => (*line, token.clone()),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        lines,
        [
            (6, "0.10.0-rc1".to_owned()),
            (7, "v0.10.0".to_owned()),
            (7, "127.0.0.1".to_owned())
        ]
    );
    let dated = mentions::candidates(text)
        .into_iter()
        .filter(|c| matches!(c, mentions::Candidate::Dated { .. }))
        .count();
    assert_eq!(dated, 4);
}
