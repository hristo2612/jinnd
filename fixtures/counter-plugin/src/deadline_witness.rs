//! Verifier-only behavioral witness. Existing modes and the runtime stay
//! unchanged. Guest work reaches named effects, whose occurrence survives
//! asynchronous ledger writes and cleanup; no INSERT timestamp is a clock.
use super::*;

pub(super) fn arm() -> Result<(), GuestFault> {
    let now = clock::now().map_err(fault)?;
    clock::alarm_at(now + 100, WAKE_TOKEN).map_err(fault)?;
    Ok(())
}

pub(super) fn run() -> Result<(), GuestFault> {
    // These fs phases only locate a writer-delay intervention in an owned
    // test home. The test never starts a stopwatch when it observes them.
    fs::write("/deadline-walk-begin", b"begin", "").map_err(fs_fault)?;
    let before = clock::now().map_err(fault)?;
    jinn::plugin::events::emit(
        TOPIC,
        jinn::plugin::types::DispatchMode::Emit,
        &jinn::plugin::types::Selector::All,
        b"ping",
    )
    .map_err(fault)?;
    let walk_ms = clock::now()
        .map_err(fault)?
        .checked_sub(before)
        .ok_or_else(|| GuestFault::Failed("clock moved backwards during walk".into()))?;
    effects::register(&format!("deadline walk ms {walk_ms}"), 0).map_err(fault)?;
    fs::write("/deadline-walk-end", b"end", "").map_err(fs_fault)?;

    // The unchanged guest clock read drives actual work, not an observer's
    // delayed notification. Keep the original 4500ms lower bound; add an
    // over-credit control before the final infinite spin. The correct
    // remaining five-second horizon kills this call inside the second spin.
    dawdle(4_500)?;
    effects::register("deadline survived 4500ms", 0).map_err(fault)?;
    dawdle(1_000)?;
    effects::register("deadline exceeded 5500ms", 0).map_err(fault)?;
    loop {
        std::hint::black_box(());
    }
}
