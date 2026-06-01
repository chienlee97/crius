//! Server lifecycle regression test dispatcher.
//!
//! Keep shared fixtures and workflow-specific lifecycle coverage split into
//! focused include files. `include!` keeps the existing test module scope so
//! helper functions remain private without broad `pub(super)` plumbing.

include!("tests_lifecycle_common.rs");
include!("tests_lifecycle_nri.rs");
include!("tests_lifecycle_state_model.rs");
include!("tests_lifecycle_create_pod.rs");
include!("tests_lifecycle_exec_attach.rs");
include!("tests_lifecycle_status_config.rs");
include!("tests_lifecycle_checkpoint.rs");
include!("tests_lifecycle_container_runtime.rs");
include!("tests_lifecycle_nri_domain.rs");
include!("tests_lifecycle_stats_events.rs");
include!("tests_lifecycle_port_forward.rs");
include!("tests_lifecycle_recovery.rs");
include!("tests_lifecycle_mounts.rs");
