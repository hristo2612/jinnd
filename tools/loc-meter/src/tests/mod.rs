//! The meter's own tests. Registered under `#[cfg(test)]` from a `tests/`
//! directory under `src/` on purpose: the shape the meter must count as zero.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod cases;
mod compiler_view;
mod fixture;
