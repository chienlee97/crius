//! Server test entrypoints.
//!
//! Keep this file as a dispatcher only. Workflow-specific regression tests live in
//! `tests_*` modules so new server coverage does not accumulate in one file.

pub(super) use super::*;

#[path = "tests_lifecycle.rs"]
mod lifecycle;

#[path = "tests_metrics.rs"]
mod tests_metrics;
#[path = "tests_recovery.rs"]
mod tests_recovery;
#[path = "tests_status.rs"]
mod tests_status;
#[path = "tests_streaming.rs"]
mod tests_streaming;
