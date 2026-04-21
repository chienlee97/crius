pub mod adjust;
pub mod api;
pub mod config;
pub mod convert;
pub mod domain;
pub mod error;
pub mod manager;
pub mod merge;
pub mod transport;

pub use adjust::{
    apply_annotation_adjustments, apply_container_adjustment, validate_container_adjustment,
    validate_container_update, validate_update_linux_resources,
};
pub use api::{
    NopNri, NriApi, NriContainerEvent, NriCreateContainerResult, NriDomain, NriPodEvent,
    NriUpdateContainerResult,
};
pub use config::NriManagerConfig;
pub use convert::{
    cri_linux_resources_from_nri, external_annotations, linux_resources_from_cri, oci_args, oci_env, oci_hooks,
    oci_linux_container, oci_mounts, oci_rlimits, oci_user,
};
pub use domain::RuntimeSnapshot;
pub use error::{NriError, Result};
pub use manager::NriManager;
pub use merge::{
    merge_annotation_adjustments, merge_container_adjustments, merge_container_updates,
    MergeResult, MergedContainerUpdates,
};
pub use transport::{multiplex_connection, PluginTtrpcClient, RuntimeTtrpcServer};
